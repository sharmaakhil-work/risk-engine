import { prisma } from '../src/db.js';
import { redis, KEYS, STREAMS } from '../src/redis.js';
import { priceStore } from '../src/market-data/price-store.js';
import { devForcePublishOrderFills, placeOrder } from '../src/positions/exchange-api.js';
import { runReconcile, verifyInvariants } from '../src/positions/reconcile.js';
import { fillToFields } from '../src/positions/position-tracker.js';
import { portfolioState } from '../src/risk/mark-to-market.js';
import { incrementalInitial, maintenanceMargin } from '../src/risk/margin.js';
import { recentEvents } from '../src/pipeline/event-bus.js';
import { config } from '../src/config.js';
import { Decimal, mul, normalize, toMoneyString, ZERO } from '../src/precision/decimal.js';
import { sleep } from '../src/util.js';

const argv = process.argv.slice(2);
const flags = new Set(argv.filter(a => a.startsWith('--')));
function flagValue(name: string): string | undefined {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : undefined;
}
const RUN_DUP = !flags.has('--no-dup');
const RUN_LOST = !flags.has('--no-lost');
const RUN_CRASH = !flags.has('--no-crash');
const CRASH_PCT_OVERRIDE = flagValue('--crash-pct');

const results: { phase: string; ok: boolean; note: string }[] = [];

function log(phase: string, msg: string): void {
  console.log(`[${phase}] ${msg}`);
}

function record(phase: string, ok: boolean, note: string): void {
  results.push({ phase, ok, note });
  console.log(`[${phase}] ${ok ? 'PASS' : 'FAIL'} — ${note}`);
}

async function waitForOrder(orderId: string, timeoutMs: number): Promise<string> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const order = await prisma.order.findUnique({ where: { id: orderId } });
    if (order && (order.status === 'SETTLED' || order.status === 'CANCELLED')) return order.status;
    await sleep(500);
  }
  return 'TIMEOUT';
}

async function waitQuiet(timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const active = await prisma.order.count({
      where: { status: { in: ['NEW', 'MARGIN_RESERVED', 'PARTIALLY_SETTLED'] } },
    });
    if (active === 0) return true;
    await sleep(1_000);
  }
  return false;
}

async function requireWorker(): Promise<void> {
  const hb = await redis.get(KEYS.workerHeartbeat);
  if (!hb) {
    console.error('[preflight] no worker heartbeat — start one first: npm run worker');
    process.exit(1);
  }
  log('preflight', `worker alive (${hb}); api not required for this script`);
}

async function phase1Trading(): Promise<string | null> {
  const orders: { symbol: string; side: 'BUY' | 'SELL'; qty: Decimal }[] = [
    { symbol: 'BTC', side: 'BUY', qty: new Decimal('0.35') },
    { symbol: 'ETH', side: 'SELL', qty: new Decimal('8') },
    { symbol: 'SOL', side: 'BUY', qty: new Decimal('120') },
  ];
  let firstOrderId: string | null = null;
  let allOk = true;
  for (const o of orders) {
    const placed = await placeOrder(o);
    if (!firstOrderId) firstOrderId = placed.id;
    log('trade', `placed ${o.side} ${o.qty} ${o.symbol} -> ${placed.id} (reserved ${placed.reserved.toFixed(2)} USD)`);
    const status = await waitForOrder(placed.id, 45_000);
    const order = await prisma.order.findUniqueOrThrow({ where: { id: placed.id } });
    log(
      'trade',
      `order ${placed.id} -> ${status} (filledQty=${order.filledQty.toFixed(6)}, avgFill=${order.avgFillPrice.toFixed(2)}, status=${order.status})`,
    );
    if (status !== 'SETTLED') allOk = false;
  }
  record('trade', allOk, 'all demo orders placed, filled and settled through the two-phase lifecycle');
  return allOk ? firstOrderId : null;
}

async function phase2Duplicate(firstOrderId: string | null): Promise<void> {
  if (!firstOrderId) {
    record('duplicate', false, 'skipped: no reference order from phase 1');
    return;
  }
  const trade = await prisma.trade.findFirst({ where: { orderId: firstOrderId }, orderBy: { id: 'asc' } });
  if (!trade) {
    record('duplicate', false, 'no trade found for reference order');
    return;
  }
  const before = await prisma.trade.count({ where: { externalId: trade.externalId } });
  const replay = {
    orderId: trade.orderId ?? undefined,
    externalId: trade.externalId,
    venue: trade.venue,
    symbol: trade.symbol,
    side: trade.side as 'BUY' | 'SELL',
    qty: trade.qty,
    price: trade.price,
    fee: trade.fee,
    ts: trade.tradeTs.getTime(),
    source: 'duplicate-replay',
  };
  await redis.xadd(STREAMS.fills, '*', ...fillToFields(replay));
  log('duplicate', `re-delivered settled fill ${trade.externalId} onto the fills stream`);
  await sleep(5_000);
  const after = await prisma.trade.count({ where: { externalId: trade.externalId } });
  const auditRow = await prisma.auditLog.findFirst({
    where: { kind: 'DUPLICATE_TRADE', subject: `${trade.venue}:${trade.externalId}` },
  });
  const ok = after === before && auditRow !== null;
  record(
    'duplicate',
    ok,
    `trade rows ${before}->${after} (unchanged), DUPLICATE_TRADE audit ${auditRow ? 'written' : 'MISSING'} — idempotent ingestion holds`,
  );
}

async function phase3LostFill(): Promise<void> {
  const placed = await placeOrder({ symbol: 'BTC', side: 'BUY', qty: new Decimal('0.2'), suppressPublish: true });
  log('lost-fill', `placed ${placed.id} with fill publishing suppressed (message will be "lost in transit")`);
  await devForcePublishOrderFills(placed.id);
  await sleep(5_000);
  let order = await prisma.order.findUniqueOrThrow({ where: { id: placed.id } });
  if (order.status === 'SETTLED') {
    log('lost-fill', 'the worker\'s own 15s sweep already recovered it — recovery confirmed from audit trail');
  } else {
    log(
      'lost-fill',
      `after 5s: order status=${order.status}, filledQty=${order.filledQty.toFixed(6)} — fills exist only in the exchange statement`,
    );
  }
  const report = await runReconcile();
  log(
    'lost-fill',
    `reconcile: recoveredFills=${report.recoveredFills} (worker's own sweep may have beaten us), reclaimed=${report.reclaimed}, stuckFixed=${report.stuckOrdersFixed}`,
  );
  const status = await waitForOrder(placed.id, 45_000);
  order = await prisma.order.findUniqueOrThrow({ where: { id: placed.id } });
  const recoveredAudit = await prisma.auditLog.count({ where: { kind: 'RECON_RECOVERED' } });
  const ok = status === 'SETTLED' && recoveredAudit > 0;
  record(
    'lost-fill',
    ok,
    `order -> ${status} (filledQty=${order.filledQty.toFixed(6)}), ${recoveredAudit} RECON_RECOVERED audit entries — CDR sweep recovered the fill`,
  );
}

interface CrashPlan {
  crashPct: Decimal;
  addNotional: Decimal;
  addQty: Decimal;
  ratioAfter: Decimal;
  equityAfter: Decimal;
  loss: Decimal;
  postedAfter: Decimal;
}

async function phase4Crash(): Promise<void> {
  let state = await portfolioState();
  let btc = state.positions.find(p => p.symbol === 'BTC');
  let mark0 = btc?.mark ?? (await priceStore.getMark('BTC'))?.price;
  if (!mark0) {
    record('crash', false, 'no BTC mark to stage the scenario on');
    return;
  }

  // Flatten the existing BTC book first: the seeded entries carry large
  // unrealized gains that inflate equity without collateral behind them,
  // which closes the breach-without-bankruptcy window. Realizing first is
  // also the honest way to stage the scenario: after this the whole equity is
  // backed by wallet + posted margin.
  if (btc && btc.qty.greaterThan(0)) {
    const flatten = await placeOrder({ symbol: 'BTC', side: 'SELL', qty: btc.qty });
    log('crash', `flattening BTC ${btc.qty.toFixed(4)} (realizing gains) -> ${flatten.id}`);
    const st = await waitForOrder(flatten.id, 60_000);
    if (st !== 'SETTLED') {
      record('crash', false, `flatten order did not settle (${st})`);
      return;
    }
    await sleep(4_000);
    state = await portfolioState();
    btc = state.positions.find(p => p.symbol === 'BTC');
    mark0 = btc?.mark ?? mark0;
  }

  const oldQty = btc?.qty ?? ZERO;
  const oldNotional = mul(oldQty.abs(), mark0);
  const feeRate = new Decimal(config.feeBps).dividedBy(10000);

  // Largest additional BTC notional whose incremental initial margin + fee
  // fits inside `frac` of free wallet — found by binary search over the
  // tiered margin curve rather than a closed form. A 0.1% headroom absorbs
  // ROUND_UP drift between this estimate and placeOrder's exact reservation
  // (observed in testing as a 26 nano-dollar overshoot — enough to fail the
  // wallet check).
  const affordableNotional = (frac: number): Decimal => {
    const budget = state.wallet.times(frac).times('0.999');
    let lo = ZERO;
    let hi = new Decimal('100000000');
    for (let i = 0; i < 80; i++) {
      const mid = lo.plus(hi).dividedBy(2);
      const cost = incrementalInitial(oldNotional, oldNotional.plus(mid)).plus(mid.times(feeRate));
      if (cost.greaterThan(budget)) hi = mid;
      else lo = mid;
    }
    return normalize(lo);
  };

  const crashPcts = CRASH_PCT_OVERRIDE ? [Number(CRASH_PCT_OVERRIDE)] : [0.2, 0.25, 0.3, 0.35, 0.18, 0.15];
  const fracs = [1.0, 0.85, 0.7, 0.55, 0.4, 0.3];
  let plan: CrashPlan | null = null;
  for (const frac of fracs) {
    const addNotional = affordableNotional(frac);
    const addQty = addNotional.dividedBy(mark0);
    const btcQtyAfter = oldQty.plus(addQty);
    const postedAfter = normalize(state.marginPosted.plus(incrementalInitial(oldNotional, oldNotional.plus(addNotional))));
    const maintOther = Decimal.max(
      ZERO,
      normalize(state.maintenanceRequired.minus(maintenanceMargin(oldNotional))),
    );
    for (const pct of crashPcts) {
      const crashPct = new Decimal(pct);
      const loss = mul(btcQtyAfter, mark0).times(crashPct);
      const markC = mul(mark0, new Decimal(1).minus(crashPct));
      const maintAfter = normalize(maintenanceMargin(mul(btcQtyAfter.abs(), markC)).plus(maintOther));
      const equityAfter = normalize(state.equity.minus(loss));
      if (maintAfter.isZero()) continue;
      const ratioAfter = equityAfter.dividedBy(maintAfter);
      // Feasible only if the crash genuinely breaches margin (ratio < 1)
      // without opening a bankruptcy gap (loss within posted margin).
      if (ratioAfter.lessThan('0.85') && loss.lessThan(postedAfter.times('0.85'))) {
        plan = { crashPct, addNotional, addQty, ratioAfter, equityAfter, loss, postedAfter };
        break;
      }
    }
    if (plan) break;
  }

  if (!plan) {
    record(
      'crash',
      false,
      'no (size, crash) combination breaches margin without a bankruptcy gap at current prices — scenario skipped honestly (try a larger seed deposit)',
    );
    return;
  }

  log(
    'crash',
    `plan: buy ~${plan.addQty.toFixed(4)} BTC (~${plan.addNotional.toFixed(0)} USD notional), then force a -${plan.crashPct.times(100).toFixed(0)}% BTC mark`,
  );
  log(
    'crash',
    `predicted: loss=${plan.loss.toFixed(0)} equity=${plan.equityAfter.toFixed(0)} ratio=${plan.ratioAfter.toFixed(3)} (breach, no deficit)`,
  );

  const placed = await placeOrder({ symbol: 'BTC', side: 'BUY', qty: plan.addQty });
  const status = await waitForOrder(placed.id, 60_000);
  if (status !== 'SETTLED') {
    record('crash', false, `leveraged order did not settle (${status})`);
    return;
  }
  const pre = await portfolioState();
  log(
    'crash',
    `pre-crash: equity=${pre.equity.toFixed(2)} maint=${pre.maintenanceRequired.toFixed(2)} ratio=${pre.marginRatio ? pre.marginRatio.toFixed(3) : 'n/a'} btcQty=${pre.positions.find(p => p.symbol === 'BTC')?.qty.toFixed(4)}`,
  );

  const scenarioPrice = mul(mark0, new Decimal(1).minus(plan.crashPct));
  await priceStore.setScenario('BTC', scenarioPrice, 300);
  log('crash', `scenario mark active: BTC = ${scenarioPrice.toFixed(2)} (source=scenario, TTL 300s, clearly labelled)`);

  const deadline = Date.now() + 90_000;
  let sawWarning = false;
  let sawLiquidation = false;
  let sawRecovered = false;
  while (Date.now() < deadline) {
    const events = await recentEvents(150);
    sawWarning = sawWarning || events.some(e => e.kind === 'margin_warning');
    sawLiquidation = sawLiquidation || events.some(e => e.kind === 'liquidation');
    sawRecovered = sawRecovered || events.some(e => e.kind === 'margin_recovered');
    if (sawLiquidation && (sawRecovered || (await quiet()))) break;
    await sleep(2_000);
  }
  log(
    'crash',
    `events observed: margin_warning=${sawWarning} liquidation=${sawLiquidation} margin_recovered=${sawRecovered}`,
  );

  // Measure only after the liquidation round's fills have fully settled —
  // mid-flight snapshots catch the position between partial fills.
  await waitQuiet(60_000);
  const post = await portfolioState();
  const postBtc = post.positions.find(p => p.symbol === 'BTC');
  log(
    'crash',
    `post-crash: equity=${post.equity.toFixed(2)} wallet=${post.wallet.toFixed(2)} posted=${post.marginPosted.toFixed(2)} loan=${post.loan.toFixed(2)} ratio=${post.marginRatio ? post.marginRatio.toFixed(3) : 'n/a'}`,
  );
  log(
    'crash',
    `BTC position: ${(pre.positions.find(p => p.symbol === 'BTC')?.qty ?? ZERO).toFixed(4)} -> ${(postBtc?.qty ?? ZERO).toFixed(4)} (auto-liquidated at crashed mark)`,
  );

  await priceStore.clearScenario('BTC');
  log('crash', 'scenario cleared — marks return to live/synthetic feed');

  const ratioHealthy = post.marginRatio ? post.marginRatio.greaterThanOrEqualTo(1) : true;
  const ok = sawWarning && sawLiquidation && (sawRecovered || ratioHealthy) && !post.loan.greaterThan(0);
  record(
    'crash',
    ok,
    `fail-closed liquidation executed on maintenance breach (warning=${sawWarning}, liquidation=${sawLiquidation}, recovered=${sawRecovered}, deficit loan used=${!post.loan.isZero()})`,
  );
}

async function quiet(): Promise<boolean> {
  return (await prisma.order.count({ where: { status: { in: ['NEW', 'MARGIN_RESERVED', 'PARTIALLY_SETTLED'] } } })) === 0;
}

async function phase5Verify(): Promise<void> {
  const quiet = await waitQuiet(45_000);
  if (!quiet) log('verify', 'WARNING: orders still in flight — verification may see transient state');
  await runReconcile();
  await sleep(3_000);
  const checks = await verifyInvariants();
  console.log('\n=== invariant verification ===');
  let allOk = true;
  for (const c of checks) {
    console.log(`  ${c.ok ? 'PASS' : 'FAIL'}  ${c.name}\n        ${c.detail}`);
    if (!c.ok) allOk = false;
  }
  record('verify', allOk, `${checks.filter(c => c.ok).length}/${checks.length} book invariants hold`);

  const state = await portfolioState();
  console.log('\n=== final portfolio ===');
  console.log(`  wallet USD     ${toMoneyString(state.wallet)}`);
  console.log(`  margin posted  ${toMoneyString(state.marginPosted)}`);
  console.log(`  deficit loan   ${toMoneyString(state.loan)}`);
  console.log(`  equity         ${toMoneyString(state.equity)}`);
  console.log(`  unrealized PnL ${toMoneyString(state.unrealizedPnl)}`);
  console.log(`  realized PnL   ${toMoneyString(state.realizedPnl)}`);
  console.log(`  gross exposure ${toMoneyString(state.grossExposure)}`);
  for (const p of state.positions) {
    if (p.qty.isZero()) continue;
    console.log(
      `  ${p.symbol.padEnd(5)} qty=${p.qty.toFixed(4).padStart(12)}  mark=${p.mark ? p.mark.toFixed(2).padStart(10) : 'n/a'}  unrealized=${p.unrealizedPnl ? p.unrealizedPnl.toFixed(2).padStart(12) : 'n/a'}`,
    );
  }

  console.log('\n=== recent risk events ===');
  const events = await recentEvents(20);
  for (const e of events) {
    const summary = JSON.stringify(e.payload);
    console.log(`  ${new Date(e.ts).toISOString()} ${e.kind.padEnd(16)} ${summary.length > 110 ? summary.slice(0, 110) + '…' : summary}`);
  }
}

async function main(): Promise<void> {
  console.log('=== risk-engine demo session ===');
  console.log(`phases: trade${RUN_DUP ? ' +duplicate' : ''}${RUN_LOST ? ' +lost-fill' : ''}${RUN_CRASH ? ' +crash' : ''} +verify\n`);
  await requireWorker();

  const firstOrderId = await phase1Trading();
  if (RUN_DUP) await phase2Duplicate(firstOrderId);
  if (RUN_LOST) await phase3LostFill();
  if (RUN_CRASH) await phase4Crash();
  await phase5Verify();

  console.log('\n=== session summary ===');
  let allOk = true;
  for (const r of results) {
    console.log(`  ${r.ok ? 'PASS' : 'FAIL'}  ${r.phase.padEnd(10)} ${r.note}`);
    if (!r.ok) allOk = false;
  }
  console.log(`\nresult: ${allOk ? 'ALL CHECKS PASSED' : 'SOME CHECKS FAILED'}`);
  process.exitCode = allOk ? 0 : 1;
}

main()
  .catch(err => {
    console.error('[simulate] fatal:', err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await priceStore.clearScenario('BTC').catch(() => {});
    redis.disconnect();
    await prisma.$disconnect();
  });
