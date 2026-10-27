import { Prisma } from '@prisma/client';
import { prisma } from '../db.js';
import { redis, pingRedis, KEYS, STREAMS, GROUPS, ensureStreamGroup, tryLock, releaseLock, createBlockingConnection } from '../redis.js';
import { config, SYMBOLS } from '../config.js';
import { priceStore } from '../market-data/price-store.js';
import { startBinanceFeed, type FeedHandle } from '../market-data/binance-ws.js';
import { startCoinbaseFeed } from '../market-data/coinbase-ws.js';
import { parseFillFields, settleFill } from '../positions/position-tracker.js';
import { pumpFills } from '../positions/exchange-api.js';
import { runReconcile } from '../positions/reconcile.js';
import { runRetention } from './retention.js';
import { portfolioState } from '../risk/mark-to-market.js';
import { evaluateAndAct, type RiskAction } from '../risk/liquidation.js';
import { computeHistoricalVar } from '../risk/var.js';
import { computeExposure } from '../risk/exposure.js';
import { toMoneyString, type Decimal } from '../precision/decimal.js';
import { incMetric, setMetric } from '../observability/metrics.js';
import { component } from '../observability/logger.js';
import { uid, sleep } from '../util.js';

const workLog = component('worker');
const settleLog = component('settlement');
const riskLog = component('risk');
const pumpLog = component('pump');
const reconLog = component('reconcile');

let running = true;
let shuttingDown = false;
const feeds: FeedHandle[] = [];

async function preflight(): Promise<void> {
  try {
    await prisma.$queryRaw`SELECT 1 FROM "positions" LIMIT 1`;
  } catch {
    throw new Error('database not reachable/migrated — run: npm run db:up');
  }
  const daily = await prisma.price.count({ where: { granularity: '1d' } });
  if (daily === 0) {
    workLog.warn('no daily price history — seed first (npm run seed) or VaR stays unavailable');
  }
  await pingRedis();
}

async function handleFill(id: string, fields: string[]): Promise<void> {
  const fill = parseFillFields(fields);
  try {
    const outcome = await settleFill(fill);
    if (outcome.status === 'DEFERRED') {
      // Position row busy in another settler: leave the message pending — the
      // reclaim sweep re-processes it (idempotently) within a claim cycle.
      settleLog.info({ externalId: fill.externalId, reason: outcome.reason }, 'deferred — left for reclaim');
      incMetric('risk_engine_settles_total', { result: 'deferred' });
      return;
    }
    await redis.xack(STREAMS.fills, GROUPS.settlers, id);
    incMetric('risk_engine_settles_total', { result: outcome.status.toLowerCase() });
    if (outcome.status === 'SETTLED') {
      settleLog.info(
        { externalId: fill.externalId, side: fill.side, symbol: fill.symbol, qty: fill.qty.toFixed(6), price: fill.price.toFixed(2) },
        'settled',
      );
    } else if (outcome.status === 'DUPLICATE') {
      settleLog.info({ externalId: fill.externalId }, 'duplicate delivery ignored (audited)');
    } else {
      settleLog.warn({ externalId: fill.externalId, reason: outcome.reason }, 'rejected');
    }
  } catch (err) {
    // Transient (serialization retries exhausted, connectivity): do NOT ack.
    // The message stays pending and the reclaim sweep settles it later.
    incMetric('risk_engine_settlement_errors_total');
    settleLog.error({ externalId: fill.externalId, err: (err as Error).message }, 'settlement error — left pending');
  }
}

async function settlementLoop(): Promise<void> {
  const consumer = `settler-${process.pid}`;
  const conn = createBlockingConnection();
  while (running) {
    try {
      const res = (await conn.xreadgroup(
        'GROUP', GROUPS.settlers, consumer, 'COUNT', 10, 'BLOCK', 2000, 'STREAMS', STREAMS.fills, '>',
      )) as [string, [string, string[]][]][] | null;
      if (!res) continue;
      for (const [, messages] of res) {
        for (const [id, fields] of messages) await handleFill(id, fields);
      }
    } catch (err) {
      const msg = (err as Error).message;
      if (msg.includes('NOGROUP')) {
        // A --force reseed trims streams; recreate the consumer group.
        await ensureStreamGroup(STREAMS.fills, GROUPS.settlers);
        continue;
      }
      settleLog.error({ err: msg }, 'consumer loop error');
      await sleep(1000);
    }
  }
  conn.disconnect();
}

async function riskJobsLoop(): Promise<void> {
  const consumer = `evaluator-${process.pid}`;
  const conn = createBlockingConnection();
  while (running) {
    try {
      const res = (await conn.xreadgroup(
        'GROUP', GROUPS.evaluators, consumer, 'COUNT', 5, 'BLOCK', 2000, 'STREAMS', STREAMS.riskJobs, '>',
      )) as [string, [string, string[]][]][] | null;
      if (!res) continue;
      for (const [, messages] of res) {
        for (const [id] of messages) {
          await evaluateOnce();
          await conn.xack(STREAMS.riskJobs, GROUPS.evaluators, id);
        }
      }
    } catch (err) {
      const msg = (err as Error).message;
      if (msg.includes('NOGROUP')) {
        await ensureStreamGroup(STREAMS.riskJobs, GROUPS.evaluators);
        continue;
      }
      riskLog.error({ err: msg }, 'job loop error');
      await sleep(1000);
    }
  }
  conn.disconnect();
}

let lastVarSignature = '';
let cachedVar: { var95: Decimal; var99: Decimal; es95: Decimal } | null = null;

function mapStatus(action: RiskAction, hasPositions: boolean): string {
  if (action === 'PAUSED_STALE') return 'STALE_PRICES';
  if (action === 'LIQUIDATING') return 'LIQUIDATING';
  if (action === 'IN_DEFICIT') return 'IN_DEFICIT';
  if (action === 'WARNED') return 'MARGIN_WARNING';
  return hasPositions ? 'OK' : 'FLAT';
}

async function evaluateOnce(): Promise<void> {
  const started = Date.now();
  const state = await portfolioState();
  const { action } = await evaluateAndAct(state);

  const signature =
    state.positions.map(p => `${p.symbol}${p.venue}:${p.version}`).join(',') + '|' + Math.floor(Date.now() / 60_000);
  if (signature !== lastVarSignature) {
    lastVarSignature = signature;
    const varRes = await computeHistoricalVar(prisma, state.positions);
    cachedVar = varRes.ok && varRes.var95 && varRes.var99 && varRes.es95
      ? { var95: varRes.var95, var99: varRes.var99, es95: varRes.es95 }
      : null;
  }

  const exposure = computeExposure(state.positions);
  const hasPositions = state.positions.some(p => !p.qty.isZero());
  const status = mapStatus(action, hasPositions);

  const marksJson: Record<string, unknown> = {};
  for (const p of state.positions) {
    marksJson[p.symbol] = {
      price: p.mark ? toMoneyString(p.mark) : null,
      source: p.markSource,
      stale: p.markStale,
      ageMs: p.markAgeMs,
    };
  }
  const detailJson = {
    action,
    staleSymbols: state.staleSymbols,
    var: cachedVar
      ? { var95: toMoneyString(cachedVar.var95), var99: toMoneyString(cachedVar.var99), es95: toMoneyString(cachedVar.es95) }
      : null,
    exposure: {
      gross: toMoneyString(exposure.gross),
      net: toMoneyString(exposure.net),
      perAsset: exposure.perAsset.map(a => ({
        symbol: a.symbol,
        qty: toMoneyString(a.qty),
        grossNotional: toMoneyString(a.grossNotional),
        weightPct: a.weightPct,
      })),
      perVenue: exposure.perVenue.map(v => ({
        venue: v.venue,
        grossNotional: toMoneyString(v.grossNotional),
      })),
    },
  };

  await prisma.riskSnapshot.create({
    data: {
      status,
      equity: state.equity,
      walletBalance: state.wallet,
      marginPosted: state.marginPosted,
      loanBalance: state.loan,
      unrealizedPnl: state.unrealizedPnl,
      realizedPnl: state.realizedPnl,
      maintenanceRequired: state.maintenanceRequired,
      marginRatio: state.marginRatio,
      var95: cachedVar?.var95 ?? null,
      var99: cachedVar?.var99 ?? null,
      es95: cachedVar?.es95 ?? null,
      grossExposure: state.grossExposure,
      netExposure: state.netExposure,
      marks: marksJson as Prisma.InputJsonValue,
      detail: detailJson as Prisma.InputJsonValue,
      workerId: `worker-${process.pid}`,
    },
  });
  await redis.set(KEYS.workerLastEval, String(Date.now()));

  incMetric('risk_engine_risk_evals_total');
  incMetric('risk_engine_snapshot_rows_written_total');
  setMetric('risk_engine_risk_eval_duration_ms', Date.now() - started);
  setMetric('risk_engine_equity_usd', state.equity.toNumber());
  setMetric('risk_engine_maintenance_required_usd', state.maintenanceRequired.toNumber());
  setMetric('risk_engine_margin_ratio', state.marginRatio ? state.marginRatio.toNumber() : 0);
  setMetric('risk_engine_marks_stale', state.staleSymbols.length);
  riskLog.info(
    {
      status,
      action,
      equity: state.equity.toFixed(2),
      maintenance: state.maintenanceRequired.toFixed(2),
      ratio: state.marginRatio ? state.marginRatio.toFixed(3) : null,
      durationMs: Date.now() - started,
    },
    'risk evaluation',
  );
}

// Every evaluation is a claimed job on a stream: a timer enqueues ticks, price
// moves enqueue triggers, workers claim them via consumer groups, and a
// crashed worker's pending jobs are reclaimed by the sweep. Nothing evaluates
// twice invisibly and nothing is lost on a crash.
async function tickLoop(): Promise<void> {
  while (running) {
    try {
      await redis.xadd(STREAMS.riskJobs, 'MAXLEN', '~', '1000', '*', 'type', 'tick');
    } catch (err) {
      workLog.error({ err: (err as Error).message }, 'tick enqueue failed');
    }
    await sleep(config.riskEvalIntervalMs);
  }
}

async function fillPumpLoop(): Promise<void> {
  while (running) {
    try {
      const token = uid('pump');
      // The pump lock only reduces duplicate publishing between workers;
      // correctness never depends on it — external trade ids are deduplicated
      // at ingestion, so overlapping pumps are harmless.
      if (await tryLock(KEYS.fillPumpLock, token, Math.max(2000, config.fillPumpIntervalMs * 2))) {
        try {
          const n = await pumpFills();
          if (n > 0) {
            incMetric('risk_engine_fills_published_total', n);
            pumpLog.info({ fills: n }, 'published fills');
          }
        } finally {
          await releaseLock(KEYS.fillPumpLock, token);
        }
      }
    } catch (err) {
      pumpLog.error({ err: (err as Error).message }, 'pump error');
    }
    await sleep(config.fillPumpIntervalMs);
  }
}

async function reconcileLoop(): Promise<void> {
  while (running) {
    await sleep(config.reconcileIntervalMs);
    if (!running) break;
    try {
      const report = await runReconcile();
      incMetric('risk_engine_reclaimed_total', report.reclaimed);
      incMetric('risk_engine_recovered_fills_total', report.recoveredFills);
      incMetric('risk_engine_stuck_orders_fixed_total', report.stuckOrdersFixed);
      const failed = report.checks.filter(c => !c.ok).length;
      if (failed > 0) incMetric('risk_engine_invariant_failures_total', failed);
      reconLog.info(
        {
          reclaimed: report.reclaimed || undefined,
          recoveredFills: report.recoveredFills || undefined,
          stuckFixed: report.stuckOrdersFixed || undefined,
          reservationsReleased: report.releasedReservations.length || undefined,
          invariants: failed === 0 ? 'PASS' : `${failed} FAIL`,
        },
        'reconcile sweep',
      );
    } catch (err) {
      reconLog.error({ err: (err as Error).message }, 'reconcile error');
    }
  }
}

async function retentionLoop(): Promise<void> {
  // Hourly is plenty: retention is hygiene, not correctness.
  while (running) {
    await sleep(3_600_000);
    if (!running) break;
    try {
      await runRetention();
    } catch (err) {
      workLog.error({ err: (err as Error).message }, 'retention error');
    }
  }
}

async function heartbeatLoop(): Promise<void> {
  const startedAt = Date.now();
  while (running) {
    try {
      await redis.set(KEYS.workerHeartbeat, `worker-${process.pid}`, 'EX', 15);
      setMetric('risk_engine_process_uptime_seconds', Math.round((Date.now() - startedAt) / 1000));
    } catch (err) {
      workLog.error({ err: (err as Error).message }, 'heartbeat error');
    }
    await sleep(5_000);
  }
}

async function main(): Promise<void> {
  workLog.info({ pid: process.pid }, 'worker starting');
  await preflight();
  await priceStore.loadHistoryAnchors();
  priceStore.startHousekeeping();
  feeds.push(
    startBinanceFeed({ symbols: SYMBOLS, onTick: t => void priceStore.ingestTick(t).catch(() => {}) }),
    startCoinbaseFeed({ symbols: SYMBOLS, onTick: t => void priceStore.ingestTick(t).catch(() => {}) }),
  );
  await ensureStreamGroup(STREAMS.fills, GROUPS.settlers);
  await ensureStreamGroup(STREAMS.riskJobs, GROUPS.evaluators);
  void settlementLoop();
  void riskJobsLoop();
  void fillPumpLoop();
  void reconcileLoop();
  void heartbeatLoop();
  void tickLoop();
  void retentionLoop();
  workLog.info(
    { feeds: ['binance', 'coinbase'], loops: ['settlement', 'risk-eval', 'fill-pump', 'reconcile', 'retention', 'heartbeat'] },
    'worker running',
  );
}

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  workLog.info({ signal }, 'shutting down');
  running = false;
  for (const f of feeds) f.stop();
  priceStore.stopHousekeeping();
  await sleep(2_300);
  redis.disconnect();
  await prisma.$disconnect();
  process.exit(0);
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('unhandledRejection', err => workLog.error({ err: String(err) }, 'unhandled rejection'));

main().catch(err => {
  workLog.error({ err: (err as Error).stack ?? String(err) }, 'fatal');
  process.exit(1);
});
