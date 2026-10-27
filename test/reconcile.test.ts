import './test-env.js';
import { after, before, describe, test } from 'node:test';
import assert from 'node:assert/strict';
import Decimal from 'decimal.js';
import { prisma } from '../src/db.js';
import { redis, KEYS, STREAMS, GROUPS, ensureStreamGroup } from '../src/redis.js';
import { priceStore } from '../src/market-data/price-store.js';
import { devForcePublishOrderFills, placeOrder, pumpFills } from '../src/positions/exchange-api.js';
import { runReconcile, verifyInvariants } from '../src/positions/reconcile.js';
import { parseFillFields, settleFill } from '../src/positions/position-tracker.js';
import { ACCOUNTS, balances, deposit } from '../src/positions/ledger.js';
import { closeSingletons } from './test-env.js';

const d = (s: string) => new Decimal(s);

async function resetDb(): Promise<void> {
  await prisma.$executeRawUnsafe(
    'TRUNCATE "ledger_entries","trades","positions","orders","risk_snapshots","audit_log","prices" RESTART IDENTITY',
  );
}

async function allInvariantsHold(): Promise<void> {
  const checks = await verifyInvariants();
  for (const c of checks) assert.ok(c.ok, `${c.name}: ${c.detail}`);
}

after(() => closeSingletons());

describe('reconciliation (CDR sweep, lost fills, stuck orders)', () => {
  before(async () => {
    await resetDb();
    await redis.flushdb();
    await ensureStreamGroup(STREAMS.fills, GROUPS.settlers);
    await ensureStreamGroup(STREAMS.riskJobs, GROUPS.evaluators);
    await deposit(d('1000000'), 'test-deposit');
    const now = Date.now();
    await priceStore.ingestTick({ venue: 'binance', symbol: 'BTC', price: d('50000'), size: d('1'), ts: now });
    await priceStore.ingestTick({ venue: 'binance', symbol: 'SOL', price: d('100'), size: d('1'), ts: now });
  });

  test('a fill lost in transit is recovered from the exchange statement and settles exactly', async () => {
    const placed = await placeOrder({ symbol: 'BTC', side: 'BUY', qty: d('0.5'), suppressPublish: true });
    assert.equal(placed.status, 'MARGIN_RESERVED');

    // while in flight, the margin ring-fence equals the reservation exactly
    const mid = await balances();
    assert.ok(mid[ACCOUNTS.MARGIN]!.eq(placed.reserved));
    await allInvariantsHold();

    // fills exist only in the exchange's own books
    await devForcePublishOrderFills(placed.id);
    await pumpFills();
    assert.equal(await redis.xlen(STREAMS.fills), 0);
    assert.ok((await redis.xlen(STREAMS.exchangeTrades)) >= 1);
    let order = await prisma.order.findUniqueOrThrow({ where: { id: placed.id } });
    assert.equal(order.status, 'MARGIN_RESERVED');

    // the CDR sweep republishes the lost fills
    const report = await runReconcile();
    assert.ok(report.recoveredFills >= 1, `recoveredFills=${report.recoveredFills}`);
    assert.ok((await redis.xlen(STREAMS.fills)) >= 1);
    assert.ok((await prisma.auditLog.count({ where: { kind: 'RECON_RECOVERED' } })) >= 1);

    // the settlement worker's step: claim via consumer group, settle, ack
    const res = (await redis.xreadgroup(
      'GROUP', GROUPS.settlers, 'test-settler', 'COUNT', 10, 'STREAMS', STREAMS.fills, '>',
    )) as [string, [string, string[]][]][] | null;
    assert.ok(res !== null);
    for (const [, messages] of res) {
      for (const [id, fields] of messages) {
        const out = await settleFill(parseFillFields(fields));
        assert.equal(out.status, 'SETTLED', JSON.stringify(out));
        await redis.xack(STREAMS.fills, GROUPS.settlers, id);
      }
    }

    order = await prisma.order.findUniqueOrThrow({ where: { id: placed.id } });
    assert.equal(order.status, 'SETTLED');
    assert.ok(order.filledQty.eq(d('0.5')));

    // the reservation was released to the last nano: margin == posted exactly
    const after = await balances();
    const pos = await prisma.position.findUniqueOrThrow({
      where: { symbol_venue: { symbol: 'BTC', venue: 'sim-binance' } },
    });
    assert.ok(after[ACCOUNTS.MARGIN]!.eq(pos.marginPosted), 'released exactly');
    await allInvariantsHold();
  });

  test('an order that never reached the exchange is cancelled and its reservation released', async () => {
    const placed = await placeOrder({ symbol: 'SOL', side: 'BUY', qty: d('10') });
    // simulate a crash between reservation and submit: the exchange never saw it
    await redis.del(KEYS.orderBook(placed.id));
    await redis.zrem(KEYS.openOrders, placed.id);
    await prisma.order.update({
      where: { id: placed.id },
      data: { updatedAt: new Date(Date.now() - 120_000) },
    });

    const report = await runReconcile();
    assert.ok(report.releasedReservations.includes(placed.id));
    const order = await prisma.order.findUniqueOrThrow({ where: { id: placed.id } });
    assert.equal(order.status, 'CANCELLED');
    assert.ok(order.marginReleased.eq(order.marginReserved));
    await allInvariantsHold();
  });
});
