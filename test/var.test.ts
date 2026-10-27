import './test-env.js';
import { after, before, describe, test } from 'node:test';
import assert from 'node:assert/strict';
import Decimal from 'decimal.js';
import { prisma } from '../src/db.js';
import { priceStore } from '../src/market-data/price-store.js';
import { computeHistoricalVar, quantile, tailMean } from '../src/risk/var.js';
import type { PositionView } from '../src/risk/mark-to-market.js';
import { ZERO } from '../src/precision/decimal.js';
import { closeSingletons } from './test-env.js';

const d = (s: string) => new Decimal(s);

async function resetDb(): Promise<void> {
  await prisma.$executeRawUnsafe(
    'TRUNCATE "ledger_entries","trades","positions","orders","risk_snapshots","audit_log","prices" RESTART IDENTITY',
  );
}

function view(symbol: string, qty: string): PositionView {
  return {
    symbol,
    venue: 'sim-binance',
    version: 0,
    qty: d(qty),
    avgEntryPrice: d('100'),
    realizedPnl: ZERO,
    marginPosted: ZERO,
    mark: d('100'),
    markSource: 'fixture',
    markStale: false,
    markAgeMs: 0,
    notional: d('100'),
    unrealizedPnl: ZERO,
    maintenanceRequired: d('1'),
  };
}

// 120 alternating daily bars: closes 100,110,100,110,... — the resulting P&L
// series for qty=1 is +10 (60 times) and -10 (59 times), hand-verifiable.
async function insertAlternatingBars(symbol: string, count: number): Promise<void> {
  const now = new Date();
  const todayUtc = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  for (let offset = -(count + 5) * 86_400_000; offset <= 40 * 86_400_000; offset += 20 * 86_400_000) {
    await priceStore.ensurePartitionForDate(new Date(todayUtc + offset));
  }
  let close = d('100');
  const bars: {
    symbol: string;
    granularity: string;
    ts: Date;
    open: Decimal;
    high: Decimal;
    low: Decimal;
    close: Decimal;
    volume: Decimal;
    source: string;
  }[] = [];
  for (let i = 0; i < count; i++) {
    const ts = new Date(todayUtc - (count - i) * 86_400_000);
    const open = close;
    close = i % 2 === 0 ? d('110') : d('100');
    bars.push({
      symbol,
      granularity: '1d',
      ts,
      open,
      high: Decimal.max(open, close),
      low: Decimal.min(open, close),
      close,
      volume: d('100'),
      source: 'test-fixture',
    });
  }
  await prisma.price.createMany({ data: bars });
}

after(() => closeSingletons());

describe('historical VaR', () => {
  before(async () => {
    await resetDb();
    await insertAlternatingBars('BTC', 120);
    await insertAlternatingBars('SOL', 30);
  });

  test('hand-verifiable quantiles on a known distribution', async () => {
    const res = await computeHistoricalVar(prisma, [view('BTC', '1')]);
    assert.ok(res.ok, res.reason ?? '');
    assert.equal(res.days, 119);
    // 5th/1st percentile of the P&L series is -10 in both cases
    assert.ok(res.var95!.eq(d('10')));
    assert.ok(res.var99!.eq(d('10')));
    assert.ok(res.es95!.eq(d('10')));
    assert.equal(res.perSymbol.length, 1);
    assert.ok(res.perSymbol[0]!.var95.eq(d('10')));
  });

  test('position size scales the distribution linearly', async () => {
    const res = await computeHistoricalVar(prisma, [view('BTC', '2.5')]);
    assert.ok(res.ok);
    assert.ok(res.var95!.eq(d('25')));
    assert.ok(res.es95!.eq(d('25')));
  });

  test('insufficient history is reported, never guessed', async () => {
    const res = await computeHistoricalVar(prisma, [view('SOL', '1')]);
    assert.equal(res.ok, false);
    assert.match(res.reason ?? '', /insufficient/i);
    assert.equal(res.var95, null);
  });

  test('no open positions is a clean null result', async () => {
    const res = await computeHistoricalVar(prisma, [view('BTC', '0')]);
    assert.equal(res.ok, false);
    assert.match(res.reason ?? '', /no open positions/i);
  });

  test('pure quantile/tailMean helpers', () => {
    const asc = [d('-10'), d('-5'), d('0'), d('5'), d('10')];
    assert.ok(quantile(asc, 0.05).eq(d('-10')));
    assert.ok(quantile(asc, 0.5).eq(d('0')));
    // idx = floor(0.99 * 4) = 3 — the discrete order statistic, not the max
    assert.ok(quantile(asc, 0.99).eq(d('5')));
    assert.ok(tailMean(asc, 0.4).eq(d('7.5')));
  });
});
