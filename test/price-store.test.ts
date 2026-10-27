import './test-env.js';
import { after, before, describe, test } from 'node:test';
import assert from 'node:assert/strict';
import Decimal from 'decimal.js';
import { prisma } from '../src/db.js';
import { redis, STREAMS } from '../src/redis.js';
import { priceStore } from '../src/market-data/price-store.js';
import { closeSingletons } from './test-env.js';

const d = (s: string) => new Decimal(s);

async function resetDb(): Promise<void> {
  await prisma.$executeRawUnsafe(
    'TRUNCATE "ledger_entries","trades","positions","orders","risk_snapshots","audit_log","prices" RESTART IDENTITY',
  );
}

after(() => closeSingletons());

async function mustMark(symbol: string) {
  const mark = await priceStore.getMark(symbol);
  assert.ok(mark !== null, `no mark for ${symbol}`);
  return mark;
}

describe('price store (marks, staleness, scenario, divergence, bars)', () => {
  before(async () => {
    await resetDb();
    await redis.flushdb();
  });

  test('the freshest source wins; ties prefer a real venue over the fallback', async () => {
    const t0 = Date.now();
    await priceStore.ingestTick({ venue: 'binance', symbol: 'BTC', price: d('50000'), size: d('1'), ts: t0 });
    let mark = await mustMark('BTC');
    assert.ok(mark.price.eq(d('50000')));
    assert.equal(mark.source, 'binance');

    await priceStore.ingestTick({ venue: 'coinbase', symbol: 'BTC', price: d('50001'), size: null, ts: t0 + 1000 });
    mark = await mustMark('BTC');
    assert.equal(mark.source, 'coinbase');

    await priceStore.ingestTick({ venue: 'simulated', symbol: 'BTC', price: d('49999'), size: null, ts: t0 + 2000 });
    mark = await mustMark('BTC');
    assert.equal(mark.source, 'simulated');

    // same timestamp: the real print replaces the synthetic one
    await priceStore.ingestTick({ venue: 'binance', symbol: 'BTC', price: d('50002'), size: null, ts: t0 + 2000 });
    mark = await mustMark('BTC');
    assert.equal(mark.source, 'binance');
    assert.ok(mark.price.eq(d('50002')));
  });

  test('staleness is a pure function of mark age', async () => {
    const old = Date.now() - 60_000;
    await priceStore.ingestTick({ venue: 'binance', symbol: 'ETH', price: d('3000'), size: null, ts: old });
    const mark = await mustMark('ETH');
    assert.equal(mark.stale, true);
    assert.ok(mark.ageMs >= 60_000);
  });

  test('scenario marks are authoritative and never stale until cleared', async () => {
    await priceStore.ingestTick({ venue: 'binance', symbol: 'SOL', price: d('100'), size: null, ts: Date.now() });
    await priceStore.setScenario('SOL', d('75'), 60);
    let mark = await mustMark('SOL');
    assert.equal(mark.source, 'scenario');
    assert.ok(mark.price.eq(d('75')));
    assert.equal(mark.stale, false);
    await priceStore.clearScenario('SOL');
    mark = await mustMark('SOL');
    assert.equal(mark.source, 'binance');
    assert.ok(mark.price.eq(d('100')));
  });

  test('venue divergence beyond 1% is audited and published once per cooldown', async () => {
    const t = Date.now();
    await priceStore.ingestTick({ venue: 'binance', symbol: 'DOGE', price: d('0.1'), size: null, ts: t });
    await priceStore.ingestTick({ venue: 'coinbase', symbol: 'DOGE', price: d('0.12'), size: null, ts: t });
    assert.equal(await prisma.auditLog.count({ where: { kind: 'PRICE_DIVERGENCE' } }), 1);
    const events = (await redis.xrevrange(STREAMS.riskEvents, '+', '-', 'COUNT', 10)) as [string, string[]][];
    const kinds = events.map(([, f]) => f[f.indexOf('kind') + 1]);
    assert.ok(kinds.includes('price_divergence'));
    // second divergence inside the cooldown dedup window is not re-published
    await priceStore.ingestTick({ venue: 'binance', symbol: 'DOGE', price: d('0.1'), size: null, ts: t + 500 });
    await priceStore.ingestTick({ venue: 'coinbase', symbol: 'DOGE', price: d('0.12'), size: null, ts: t + 500 });
    assert.equal(await prisma.auditLog.count({ where: { kind: 'PRICE_DIVERGENCE' } }), 1);
  });

  test('minute bars merge monotonically and re-flushing is idempotent', async () => {
    // Use the NEXT minute bucket: earlier tests in this file ticked DOGE in
    // the current minute, and the in-memory bar for that bucket already
    // carries their prints — a fresh bucket keeps this test self-contained.
    const bucket = Math.floor(Date.now() / 60_000) * 60_000 + 60_000;
    await priceStore.ingestTick({ venue: 'binance', symbol: 'DOGE', price: d('0.100'), size: d('1000'), ts: bucket + 1000 });
    await priceStore.ingestTick({ venue: 'binance', symbol: 'DOGE', price: d('0.110'), size: d('2000'), ts: bucket + 2000 });
    await priceStore.ingestTick({ venue: 'binance', symbol: 'DOGE', price: d('0.090'), size: d('3000'), ts: bucket + 3000 });

    const key = { symbol: 'DOGE', granularity: '1m', ts: new Date(bucket) };
    // flush twice — the second pass must be a pure no-op for this bar
    await priceStore.flushBars();
    let bar = await prisma.price.findUniqueOrThrow({ where: { symbol_granularity_ts: key } });
    assert.ok(bar.open.eq(d('0.100')));
    assert.ok(bar.high.eq(d('0.110')));
    assert.ok(bar.low.eq(d('0.090')));
    assert.ok(bar.close.eq(d('0.090')));
    assert.ok(bar.volume.eq(d('6000')));
    await priceStore.flushBars();
    bar = await prisma.price.findUniqueOrThrow({ where: { symbol_granularity_ts: key } });
    assert.ok(bar.volume.eq(d('6000')), 're-flush must not double-count volume');
    // exactly one bar row for this bucket
    assert.equal(
      await prisma.price.count({ where: { symbol: 'DOGE', granularity: '1m', ts: new Date(bucket) } }),
      1,
    );
  });
});
