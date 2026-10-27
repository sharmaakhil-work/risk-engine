import './test-env.js';
import { after, before, describe, test } from 'node:test';
import assert from 'node:assert/strict';
import Decimal from 'decimal.js';
import { prisma } from '../src/db.js';
import { ACCOUNTS, balances, deposit } from '../src/positions/ledger.js';
import { settleFill, type FillMessage } from '../src/positions/position-tracker.js';
import { verifyInvariants } from '../src/positions/reconcile.js';
import { ZERO } from '../src/precision/decimal.js';
import { closeSingletons } from './test-env.js';
import { hashSeed, mulberry32 } from '../src/util.js';

const VENUE = 'sim-binance';
const d = (s: string) => new Decimal(s);

async function resetDb(): Promise<void> {
  await prisma.$executeRawUnsafe(
    'TRUNCATE "ledger_entries","trades","positions","orders","risk_snapshots","audit_log","prices" RESTART IDENTITY',
  );
}

let fillSeq = 0;
function mkFill(symbol: string, side: 'BUY' | 'SELL', qty: string, price: string, fee = '0'): FillMessage {
  fillSeq++;
  return {
    externalId: `TEST-${symbol}-${fillSeq}`,
    venue: VENUE,
    symbol,
    side,
    qty: d(qty),
    price: d(price),
    fee: d(fee),
    ts: Date.now(),
    source: 'test',
  };
}

async function mustSettle(fill: FillMessage): Promise<{ status: 'SETTLED'; tradeId: number }> {
  const out = await settleFill(fill);
  assert.equal(out.status, 'SETTLED', `fill ${fill.externalId}: ${JSON.stringify(out)}`);
  return out;
}

async function getPosition(
  symbol: string,
): Promise<{ qty: Decimal; avgEntryPrice: Decimal; realizedPnl: Decimal; marginPosted: Decimal }> {
  const p = await prisma.position.findUniqueOrThrow({ where: { symbol_venue: { symbol, venue: VENUE } } });
  return { qty: p.qty, avgEntryPrice: p.avgEntryPrice, realizedPnl: p.realizedPnl, marginPosted: p.marginPosted };
}

async function allInvariantsHold(): Promise<void> {
  const checks = await verifyInvariants();
  for (const c of checks) assert.ok(c.ok, `${c.name}: ${c.detail}`);
}

before(async () => {
  await resetDb();
  await deposit(d('1000000'), 'test-deposit');
});

after(() => closeSingletons());

describe('settlement (the real SERIALIZABLE path)', () => {
  test('full position lifecycle: open, add, partial close, flatten, cross to short', async () => {
    // open: posted 20 (2% of 1000 notional), fee 7
    await mustSettle(mkFill('BTC', 'BUY', '2', '500', '7'));
    let btc = await getPosition('BTC');
    assert.ok(btc.qty.eq(d('2')));
    assert.ok(btc.avgEntryPrice.eq(d('500')));
    let bals = await balances();
    assert.ok(bals[ACCOUNTS.WALLET]!.eq(d('999973'))); // 1M - 7 fee - 20 posted
    assert.ok(bals[ACCOUNTS.MARGIN]!.eq(d('20')));

    // add: postedAdd 36 (fill-price tiering), fee 10
    await mustSettle(mkFill('BTC', 'BUY', '3', '600', '10'));
    btc = await getPosition('BTC');
    assert.ok(btc.qty.eq(d('5')));
    assert.ok(btc.avgEntryPrice.eq(d('560')));
    bals = await balances();
    assert.ok(bals[ACCOUNTS.WALLET]!.eq(d('999927'))); // - 10 fee - 36 posted
    assert.ok(bals[ACCOUNTS.MARGIN]!.eq(d('56')));

    // partial close: realized +280 lands in the wallet, pro-rata margin released
    await mustSettle(mkFill('BTC', 'SELL', '2', '700', '5'));
    btc = await getPosition('BTC');
    assert.ok(btc.qty.eq(d('3')));
    assert.ok(btc.avgEntryPrice.eq(d('560')));
    assert.ok(btc.realizedPnl.eq(d('280')));
    bals = await balances();
    // 999927 + 22.4 release + 280 realized - 5 fee
    assert.ok(bals[ACCOUNTS.WALLET]!.eq(d('1000224.4')));
    assert.ok(bals[ACCOUNTS.MARGIN]!.eq(d('33.6')));

    // flatten: realized -30, remaining margin released
    await mustSettle(mkFill('BTC', 'SELL', '3', '550', '5'));
    btc = await getPosition('BTC');
    assert.ok(btc.qty.isZero());
    assert.ok(btc.avgEntryPrice.isZero());
    assert.ok(btc.realizedPnl.eq(d('250')));
    bals = await balances();
    assert.ok(bals[ACCOUNTS.MARGIN]!.isZero());
    assert.ok(bals[ACCOUNTS.WALLET]!.eq(d('1000223'))); // + 33.6 release - 30 realized loss - 5 fee

    await allInvariantsHold();
  });

  test('crossing long through zero opens a short lot and posts fresh margin', async () => {
    // fresh symbol: realized_pnl is cumulative on the position row, and the
    // lifecycle test already realized +250 on BTC
    await mustSettle(mkFill('SOL', 'BUY', '2', '500', '1'));
    // sell 5: close 2 at -200 realized, open 3 short at 400 (posted 24)
    await mustSettle(mkFill('SOL', 'SELL', '5', '400', '2'));
    const btc = await getPosition('SOL');
    assert.ok(btc.qty.eq(d('-3')));
    assert.ok(btc.avgEntryPrice.eq(d('400')));
    assert.ok(btc.marginPosted.eq(d('24')));
    assert.ok(btc.realizedPnl.eq(d('-200')));
    await allInvariantsHold();
    // and back through zero to flat
    await mustSettle(mkFill('SOL', 'BUY', '3', '450', '1'));
    const flat = await getPosition('SOL');
    assert.ok(flat.qty.isZero());
    assert.ok(flat.marginPosted.isZero());
    assert.ok(flat.realizedPnl.eq(d('-350'))); // -200 + (400-450)*3
    await allInvariantsHold();
  });

  test('duplicate delivery of a settled fill is an audited no-op', async () => {
    const fill = mkFill('ETH', 'BUY', '1', '2000', '1');
    await mustSettle(fill);
    const balsBefore = await balances();
    const out = await settleFill(fill);
    assert.equal(out.status, 'DUPLICATE');
    assert.equal(await prisma.trade.count({ where: { externalId: fill.externalId } }), 1);
    const audit = await prisma.auditLog.findFirst({ where: { kind: 'DUPLICATE_TRADE', subject: `${VENUE}:${fill.externalId}` } });
    assert.ok(audit !== null);
    const balsAfter = await balances();
    assert.ok(balsAfter[ACCOUNTS.WALLET]!.eq(balsBefore[ACCOUNTS.WALLET]!), 'wallet must not move');
    assert.ok(balsAfter[ACCOUNTS.MARGIN]!.eq(balsBefore[ACCOUNTS.MARGIN]!), 'margin must not move');
  });

  test('invalid fills are rejected without touching the books', async () => {
    assert.equal((await settleFill(mkFill('ETH', 'BUY', '0', '100'))).status, 'REJECTED');
    assert.equal((await settleFill(mkFill('ETH', 'SELL', '-1', '100'))).status, 'REJECTED');
    assert.equal((await settleFill(mkFill('ETH', 'BUY', '1', '0'))).status, 'REJECTED');
    assert.equal((await settleFill({ ...mkFill('ETH', 'BUY', '1', '100'), externalId: '' })).status, 'REJECTED');
    assert.equal((await settleFill(mkFill('FAKE', 'BUY', '1', '100'))).status, 'REJECTED');
  });

  test('DEFERRED when the position row is locked by a concurrent settler (SKIP LOCKED)', async () => {
    const fill = mkFill('BTC', 'BUY', '0.1', '500', '0');
    // hold the BTC position row in an open transaction for 1.5s
    const hold = prisma.$transaction(async tx => {
      await tx.$queryRaw`SELECT "id" FROM "positions" WHERE "symbol" = 'BTC' AND "venue" = ${VENUE} FOR UPDATE`;
      await new Promise(r => setTimeout(r, 1500));
    });
    await new Promise(r => setTimeout(r, 300));
    const out = await settleFill(fill);
    assert.equal(out.status, 'DEFERRED');
    await hold;
    // after the lock releases, the same fill settles cleanly
    const retry = await settleFill(fill);
    assert.equal(retry.status, 'SETTLED');
  });

  test('property: 120 seeded-random fills keep every book invariant exact', async () => {
    const rand = mulberry32(hashSeed('settle-property'));
    const symbols = ['BTC', 'ETH', 'SOL'] as const;
    const shadow: Record<string, number> = { BTC: 0, ETH: 0, SOL: 0 };
    for (let i = 0; i < 120; i++) {
      const symbol = symbols[Math.floor(rand() * symbols.length)]!;
      // mean-revert the walk so exposure stays bounded and the wallet is never
      // close to exhaustion — the property under test is exactness, not solvency
      let side: 'BUY' | 'SELL';
      if (shadow[symbol]! > 4) side = 'SELL';
      else if (shadow[symbol]! < -4) side = 'BUY';
      else side = rand() < 0.5 ? 'BUY' : 'SELL';
      const qtyNum = 0.01 + rand() * 2;
      const qty = d(qtyNum.toFixed(2));
      const price = d((20000 + rand() * 70000).toFixed(2));
      const fee = qty.times(price).times('0.0007');
      shadow[symbol]! += side === 'BUY' ? qtyNum : -qtyNum;
      const out = await settleFill({
        externalId: `PROP-${i}`,
        venue: VENUE,
        symbol,
        side,
        qty,
        price,
        fee,
        ts: Date.now(),
        source: 'test',
      });
      assert.ok(out.status === 'SETTLED', `iteration ${i}: ${JSON.stringify(out)}`);
    }
    await allInvariantsHold();
    const bals = await balances();
    assert.ok(bals[ACCOUNTS.MARGIN]!.greaterThan(0), 'the walk must have built positions');
    assert.ok(bals[ACCOUNTS.WALLET]!.greaterThan(d('500000')));
    assert.ok((bals[ACCOUNTS.LOAN] ?? ZERO).isZero(), 'no bankruptcy gap in a bounded walk');
  });
});
