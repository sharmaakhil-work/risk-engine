import { after, describe, test } from 'node:test';
import assert from 'node:assert/strict';
import Decimal from 'decimal.js';
import { ZERO } from '../src/precision/decimal.js';
import { positionMath, type FillMessage, type PositionRow } from '../src/positions/position-tracker.js';
import { closeSingletons } from './test-env.js';

after(() => closeSingletons());

const d = (s: string) => new Decimal(s);

function pos(qty: string, avg: string, posted: string): PositionRow {
  return {
    id: 1,
    qty: d(qty),
    avg_entry_price: d(avg),
    realized_pnl: ZERO,
    margin_posted: d(posted),
    version: 0,
  };
}

function fill(side: 'BUY' | 'SELL', qty: string, price: string): FillMessage {
  return {
    externalId: 'T-1',
    venue: 'sim-binance',
    symbol: 'BTC',
    side,
    qty: d(qty),
    price: d(price),
    fee: ZERO,
    ts: 0,
  };
}

function expect(
  result: ReturnType<typeof positionMath>,
  qty: string,
  avg: string,
  realized: string,
  postedAdd: string,
  postedRelease: string,
): void {
  assert.ok(result.newQty.eq(d(qty)), `qty ${result.newQty} != ${qty}`);
  assert.ok(result.newAvg.eq(d(avg)), `avg ${result.newAvg} != ${avg}`);
  assert.ok(result.realized.eq(d(realized)), `realized ${result.realized} != ${realized}`);
  assert.ok(result.postedAdd.eq(d(postedAdd)), `postedAdd ${result.postedAdd} != ${postedAdd}`);
  assert.ok(result.postedRelease.eq(d(postedRelease)), `postedRelease ${result.postedRelease} != ${postedRelease}`);
}

describe('position math (the branch-dense core)', () => {
  test('fresh long open: weighted entry, initial margin posted', () => {
    // notional 2*500 = 1000 -> 2% tier = 20
    expect(positionMath(pos('0', '0', '0'), fill('BUY', '2', '500')), '2', '500', '0', '20', '0');
  });

  test('fresh short open', () => {
    // notional 3*400 = 1200 -> 2% of 1200 = 24
    expect(positionMath(pos('0', '0', '0'), fill('SELL', '3', '400')), '-3', '400', '0', '24', '0');
  });

  test('same-direction add reweights the average', () => {
    // tiering re-prices old exposure at the fill price: init(5*600) - init(2*600) = 60 - 24 = 36
    expect(positionMath(pos('2', '500', '20'), fill('BUY', '3', '600')), '5', '560', '0', '36', '0');
  });

  test('short extension reweights with absolute quantities', () => {
    // init(5*450) - init(3*450) = 45 - 27 = 18; avg (3*400 + 2*450)/5 = 420
    expect(positionMath(pos('-3', '400', '24'), fill('SELL', '2', '450')), '-5', '420', '0', '18', '0');
  });

  test('partial close realizes PnL and releases pro-rata posted margin', () => {
    // realized (700-560)*2 = 280; release 60 * 2/5 = 24
    expect(positionMath(pos('5', '560', '60'), fill('SELL', '2', '700')), '3', '560', '280', '0', '24');
  });

  test('full close flattens the position', () => {
    // realized (550-560)*3 = -30; whole posted margin released
    expect(positionMath(pos('3', '560', '36'), fill('SELL', '3', '550')), '0', '0', '-30', '0', '36');
  });

  test('short partial cover realizes loss when buying back higher', () => {
    // realized (400-480)*1 = -80; release 24 * 1/3 = 8
    expect(positionMath(pos('-3', '400', '24'), fill('BUY', '1', '480')), '-2', '400', '-80', '0', '8');
  });

  test('crossing long through zero opens a fresh short lot at fill price', () => {
    // close 2 @ (400-500) = -200 realized; remainder 3 opens short: init(1200) = 24
    expect(positionMath(pos('2', '500', '20'), fill('SELL', '5', '400')), '-3', '400', '-200', '24', '20');
  });

  test('crossing short through zero opens a fresh long lot at fill price', () => {
    // close 2 @ (400-500) = -200 realized (buying back a short higher is a
    // loss); remainder 1 opens long: init(500) = 10
    expect(positionMath(pos('-2', '400', '20'), fill('BUY', '3', '500')), '1', '500', '-200', '10', '20');
  });

  test('average entry keeps full precision through weighted adds', () => {
    const r = positionMath(pos('1', '41000', '1440'), fill('BUY', '1', '42000'));
    assert.ok(r.newAvg.eq(d('41500')));
    // init(84000) - init(42000) = 4520 - 1480 = 3040
    assert.ok(r.postedAdd.eq(d('3040')));
  });
});
