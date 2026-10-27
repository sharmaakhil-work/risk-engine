import { prisma, withSerializable } from '../db.js';
import { audit } from '../audit.js';
import { redis, STREAMS, GROUPS } from '../redis.js';
import { config } from '../config.js';
import { parseFillFields, settleFill, fillToFields } from './position-tracker.js';
import { getExchangeOrder } from './exchange-api.js';
import { ACCOUNTS, appendWithinTx } from './ledger.js';
import { Decimal, fromDb, toMoneyString, ZERO } from '../precision/decimal.js';
import { component } from '../observability/logger.js';

const reconLog = component('reconcile');

export interface InvariantCheck {
  name: string;
  ok: boolean;
  detail: string;
}

export interface ReconcileReport {
  reclaimed: number;
  recoveredFills: number;
  stuckOrdersFixed: number;
  releasedReservations: string[];
}

const RECONCILER = `reconciler-${process.pid}`;

// XAUTOCLAIM-equivalent done with XPENDING (IDLE filter) + XCLAIM so the
// pending-entry shape is explicit: any message left un-acked longer than the
// claim threshold (a crashed or stuck consumer) is re-claimed and
// re-processed. Every step of re-processing is idempotent, so reclaiming is
// always safe.
async function reclaimPending(stream: string, group: string, settle: boolean): Promise<number> {
  let reclaimed = 0;
  for (;;) {
    let raw: unknown[][];
    try {
      raw = (await redis.xpending(stream, group, 'IDLE', config.claimMinIdleMs, '-', '+', 25)) as unknown[][];
    } catch (err) {
      // No consumer group yet (reconcile ran before any worker) — nothing to
      // reclaim.
      if (String((err as Error).message).includes('NOGROUP')) return 0;
      throw err;
    }
    const ids = raw
      .map(e => (Array.isArray(e) ? String(e[0] ?? '') : ''))
      .filter(id => id && id !== 'null' && id !== '0-0');
    if (ids.length === 0) break;
    const claimed = (await redis.xclaim(stream, group, RECONCILER, config.claimMinIdleMs, ...ids)) as [string, string[] | null][];
    for (const [id, fields] of claimed) {
      if (!fields || fields.length === 0) {
        await redis.xack(stream, group, id);
        continue;
      }
      if (settle) {
        const fill = parseFillFields(fields);
        const outcome = await settleFill(fill);
        reconLog.info({ externalId: fill.externalId, outcome: outcome.status }, 'reclaimed message');
      }
      await redis.xack(stream, group, id);
      reclaimed++;
    }
  }
  return reclaimed;
}

// CDR sweep (continuous data reconciliation): diff the simulated exchange's
// own trade statement against our ingested trades. Any fill the exchange
// reports that we never settled — a fill whose stream message was lost in
// transit — is republished to the ingestion stream and audited. This is the
// only path that recovers a lost fill.
async function cdrSweep(): Promise<number> {
  const since = Date.now() - 10 * 60_000;
  const entries = (await redis.xrange(STREAMS.exchangeTrades, String(since), '+')) as [string, string[]][];
  let recovered = 0;
  for (const [, fields] of entries) {
    const fill = parseFillFields(fields);
    if (!fill.externalId) continue;
    // status <> REJECTED implies the partial unique index, so this lookup can
    // use it; a previously REJECTED fill stays recoverable on purpose.
    const existing = await prisma.trade.findFirst({
      where: { venue: fill.venue, externalId: fill.externalId, status: { not: 'REJECTED' } },
      select: { id: true },
    });
    if (existing) continue;
    const guard = await redis.set(`recovered:${fill.venue}:${fill.externalId}`, '1', 'EX', 90, 'NX');
    if (!guard) continue;
    fill.source = 'recovered';
    await redis.xadd(STREAMS.fills, 'MAXLEN', '~', String(config.streamMaxLen), '*', ...fillToFields(fill));
    await audit(
      prisma,
      'RECON_RECOVERED',
      `${fill.venue}:${fill.externalId}`,
      'lost fill recovered from exchange statement',
      { symbol: fill.symbol, qty: toMoneyString(fill.qty), price: toMoneyString(fill.price) },
    );
    recovered++;
  }
  return recovered;
}

// Orders stuck mid-lifecycle beyond the TTL:
//   - reserved margin but never submitted (crash between the two phases) ->
//     release the reservation and cancel
//   - fully filled at the exchange but never completed locally -> complete it
//   - fills missing locally -> the CDR sweep republishes them, nothing to do
async function stuckOrders(): Promise<{ fixed: number; released: string[] }> {
  const cutoff = new Date(Date.now() - config.orderTtlMs);
  const stuck = await prisma.order.findMany({
    where: { status: { in: ['NEW', 'MARGIN_RESERVED', 'PARTIALLY_SETTLED'] }, updatedAt: { lt: cutoff } },
  });
  let fixed = 0;
  const released: string[] = [];
  for (const order of stuck) {
    const book = await getExchangeOrder(order.id);
    const marginReserved = fromDb(order.marginReserved);
    if (!book) {
      if (marginReserved.greaterThan(0)) {
        await withSerializable(tx =>
          appendWithinTx(tx, [
            {
              account: ACCOUNTS.MARGIN,
              kind: 'MARGIN_RELEASE',
              amount: marginReserved.negated(),
              idempotencyKey: `cancel:${order.id}:m`,
              orderId: order.id,
            },
            {
              account: ACCOUNTS.WALLET,
              kind: 'MARGIN_RELEASE',
              amount: marginReserved,
              idempotencyKey: `cancel:${order.id}`,
              orderId: order.id,
              note: 'stuck order cancelled, reservation released',
            },
          ]),
        );
        released.push(order.id);
      }
      await prisma.order.update({
        where: { id: order.id },
        data: {
          status: 'CANCELLED',
          marginReleased: marginReserved,
          failReason: 'never submitted to exchange',
          updatedAt: new Date(),
        },
      });
      await audit(prisma, 'ORDER_CANCELLED_STUCK', order.id, 'stuck order cancelled, reservation released', {
        reserved: toMoneyString(marginReserved),
      });
      fixed++;
    } else if (book.status === 'FILLED' && fromDb(order.filledQty).greaterThanOrEqualTo(fromDb(order.qty))) {
      await prisma.order.update({ where: { id: order.id }, data: { status: 'SETTLED', updatedAt: new Date() } });
      fixed++;
    }
  }
  return { fixed, released };
}

// Full-book invariants, checked from scratch rather than trusted:
//   1. ledger balance_after chain equals the sum of amounts per account
//   2. Σ positions.margin_posted equals the USD:MARGIN ledger balance
//   3. wallet >= 0, loan <= 0
//   4. no trade stuck in RECEIVED beyond two minutes
//   5. every SETTLED order is fully filled
export async function verifyInvariants(): Promise<InvariantCheck[]> {
  const checks: InvariantCheck[] = [];

  const balRows = await prisma.$queryRaw<{ account: string; balance: Decimal }[]>`
    SELECT DISTINCT ON ("account") "account", "balance_after" AS "balance"
    FROM "ledger_entries" ORDER BY "account", "id" DESC`;
  const bal = new Map(balRows.map(r => [r.account, fromDb(r.balance)]));

  const sumRows = await prisma.$queryRaw<{ account: string; total: Decimal }[]>`
    SELECT "account", SUM("amount") AS "total" FROM "ledger_entries" GROUP BY "account"`;
  const chainFailures: string[] = [];
  for (const s of sumRows) {
    const b = bal.get(s.account) ?? ZERO;
    const total = fromDb(s.total);
    if (!b.eq(total)) chainFailures.push(`${s.account}: chain ${b.toFixed(2)} vs sum ${total.toFixed(2)}`);
  }
  checks.push({
    name: 'ledger balance_after chain equals sum of amounts per account',
    ok: chainFailures.length === 0,
    detail: chainFailures.length === 0 ? 'consistent' : chainFailures.join('; '),
  });

  const marginBal = bal.get(ACCOUNTS.MARGIN) ?? ZERO;
  const postedRows = await prisma.$queryRaw<{ total: Decimal }[]>`
    SELECT COALESCE(SUM("margin_posted"), 0) AS "total" FROM "positions"`;
  const posted = fromDb(postedRows[0]?.total ?? '0');
  // Outstanding reservations: orders still mid-lifecycle that ring-fenced
  // margin but have not fully released it yet. The margin account is a pure
  // ring-fence: balance == posted margin + outstanding reservations, exactly,
  // at every point in the order lifecycle.
  const reservedRows = await prisma.$queryRaw<{ total: Decimal }[]>`
    SELECT COALESCE(SUM("margin_reserved" - "margin_released"), 0) AS "total" FROM "orders"
    WHERE "status" IN ('NEW', 'MARGIN_RESERVED', 'PARTIALLY_SETTLED')`;
  const outstandingReserved = fromDb(reservedRows[0]?.total ?? '0');
  const expectedMargin = posted.plus(outstandingReserved);
  checks.push({
    name: 'USD:MARGIN balance = Σ posted margin + outstanding reservations',
    ok: marginBal.eq(expectedMargin),
    detail: `ledger ${toMoneyString(marginBal)} vs posted ${toMoneyString(posted)} + reserved ${toMoneyString(outstandingReserved)}`,
  });

  const wallet = bal.get(ACCOUNTS.WALLET) ?? ZERO;
  const loan = bal.get(ACCOUNTS.LOAN) ?? ZERO;
  checks.push({
    name: 'wallet >= 0 and deficit loan <= 0',
    ok: !wallet.isNegative() && !loan.greaterThan(0),
    detail: `wallet ${toMoneyString(wallet)}, loan ${toMoneyString(loan)}`,
  });

  const stuckSince = new Date(Date.now() - 120_000);
  const stuckTrades = await prisma.trade.count({ where: { status: 'RECEIVED', ingestedAt: { lt: stuckSince } } });
  checks.push({
    name: 'no trade stuck in RECEIVED beyond 2 minutes',
    ok: stuckTrades === 0,
    detail: `${stuckTrades} stuck`,
  });

  const badOrders = await prisma.$queryRaw<{ count: bigint }[]>`
    SELECT COUNT(*) AS "count" FROM "orders" WHERE "status" = 'SETTLED' AND "filled_qty" <> "qty"`;
  const badCount = Number(badOrders[0]?.count ?? 0);
  checks.push({
    name: 'every SETTLED order is fully filled',
    ok: badCount === 0,
    detail: `${badCount} incomplete`,
  });

  return checks;
}

export async function runReconcile(): Promise<ReconcileReport & { checks: InvariantCheck[] }> {
  const reclaimed = (await reclaimPending(STREAMS.fills, GROUPS.settlers, true)) +
    (await reclaimPending(STREAMS.riskJobs, GROUPS.evaluators, false));
  const recoveredFills = await cdrSweep();
  const { fixed, released } = await stuckOrders();
  const checks = await verifyInvariants();
  const failed = checks.filter(c => !c.ok);
  if (failed.length > 0) {
    await audit(prisma, 'INVARIANT_VIOLATION', 'reconcile', `${failed.length} invariant check(s) failed`, {
      failed: failed.map(f => f.name),
    });
  }
  return { reclaimed, recoveredFills, stuckOrdersFixed: fixed, releasedReservations: released, checks };
}
