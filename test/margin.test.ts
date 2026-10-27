import { after, describe, test } from 'node:test';
import assert from 'node:assert/strict';
import Decimal from 'decimal.js';
import { ZERO } from '../src/precision/decimal.js';
import {
  TIERS,
  incrementalInitial,
  initialMargin,
  maintenanceMargin,
  marginRatio,
  tierBreakdown,
} from '../src/risk/margin.js';
import { closeSingletons } from './test-env.js';

after(() => closeSingletons());

const d = (s: string) => new Decimal(s);

describe('tiered margin', () => {
  test('exact requirements at every tier boundary (initial)', () => {
    assert.ok(initialMargin(d('0')).isZero());
    assert.ok(initialMargin(d('10000')).eq(d('200')));
    assert.ok(initialMargin(d('10001')).eq(d('200.04')));
    assert.ok(initialMargin(d('50000')).eq(d('1800')));
    assert.ok(initialMargin(d('200000')).eq(d('13800')));
    assert.ok(initialMargin(d('1000000')).eq(d('109800')));
    assert.ok(initialMargin(d('2000000')).eq(d('359800')));
  });

  test('exact requirements at every tier boundary (maintenance)', () => {
    assert.ok(maintenanceMargin(d('10000')).eq(d('100')));
    assert.ok(maintenanceMargin(d('50000')).eq(d('900')));
    assert.ok(maintenanceMargin(d('200000')).eq(d('6900')));
    assert.ok(maintenanceMargin(d('1000000')).eq(d('46900')));
    assert.ok(maintenanceMargin(d('2000000')).eq(d('146900')));
  });

  test('incremental initial margin is the marginal cost of growth', () => {
    assert.ok(incrementalInitial(ZERO, d('10000')).eq(d('200')));
    assert.ok(incrementalInitial(d('10000'), d('10000')).isZero());
    // init(60k) = 1800 + 10k*8% = 2600; minus init(10k) = 200
    assert.ok(incrementalInitial(d('10000'), d('60000')).eq(d('2400')));
    // shrinking exposure never posts margin
    assert.ok(incrementalInitial(d('60000'), d('10000')).isZero());
    // crossing several tiers at once
    assert.ok(incrementalInitial(d('0'), d('1000000')).eq(d('109800')));
  });

  test('margin ratio is null exactly when there is no requirement', () => {
    assert.equal(marginRatio(d('100'), ZERO), null);
    const r1 = marginRatio(d('100'), d('50'));
    assert.ok(r1 !== null);
    assert.ok(r1.eq(d('2')));
    const r2 = marginRatio(d('-100'), d('50'));
    assert.ok(r2 !== null);
    assert.ok(r2.eq(d('-2')));
  });

  test('tier breakdown slices partition the notional exactly', () => {
    const slices = tierBreakdown(d('1234567.89'));
    const total = slices.reduce((acc, s) => acc.plus(s.sliceUsd), ZERO);
    assert.ok(total.eq(d('1234567.89')));
    assert.equal(slices.length, 5);
    const tiny = tierBreakdown(d('5000'));
    assert.equal(tiny.length, 1);
    assert.ok(tiny[0]!.sliceUsd.eq(d('5000')));
  });

  test('tier table is well-formed', () => {
    assert.equal(TIERS.length, 5);
    assert.equal(TIERS[TIERS.length - 1]!.upToUsd, null);
    for (let i = 0; i < TIERS.length - 2; i++) {
      const a = TIERS[i]!;
      const b = TIERS[i + 1]!;
      assert.ok(new Decimal(a.upToUsd!).lessThan(new Decimal(b.upToUsd!)), 'ascending');
    }
    for (const t of TIERS) {
      assert.ok(new Decimal(t.initialRate).greaterThan(new Decimal(t.maintenanceRate)));
    }
  });
});
