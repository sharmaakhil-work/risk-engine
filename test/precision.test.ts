import { after, describe, test } from 'node:test';
import assert from 'node:assert/strict';
import Decimal from 'decimal.js';
import { SCALE, ZERO, fromDb, money, normalize, toMoneyString } from '../src/precision/decimal.js';
import { closeSingletons } from './test-env.js';

after(() => closeSingletons());

describe('money discipline', () => {
  test('single rounding mode: ROUND_UP (away from zero) at 18 dp', () => {
    assert.equal(toMoneyString(normalize(new Decimal('1.0000000000000000005'))), '1.000000000000000001');
    assert.equal(toMoneyString(normalize(new Decimal('-1.0000000000000000005'))), '-1.000000000000000001');
    assert.equal(toMoneyString(normalize(new Decimal('2.5'))), '2.500000000000000000');
    assert.equal(SCALE, 18);
  });

  test('string arithmetic is exact where floats famously are not', () => {
    assert.ok(money('0.1').plus(money('0.2')).eq(new Decimal('0.3')));
    assert.ok(money('0.3').minus(money('0.1')).eq(new Decimal('0.2')));
  });

  test('money() passes through configured Decimals untouched', () => {
    const d = new Decimal('12.34');
    assert.ok(money(d) === d);
  });

  test('ZERO is a real zero, and isPositive() is true for it (decimal.js quirk)', () => {
    // Documenting the trap that produced a real invariant-check bug: zero
    // carries a positive sign, so sign-based predicates must not be used
    // where zero matters. Explicit comparisons are the rule in this codebase.
    assert.ok(ZERO.isZero());
    assert.equal(ZERO.isPositive(), true);
    assert.equal(ZERO.greaterThan(0), false);
  });

  test('fromDb converts foreign low-precision Decimals exactly (the Prisma trap)', () => {
    // Prisma bundles its own decimal.js copy at the default precision of 20
    // significant digits; arithmetic performed on its instances silently
    // rounds there. This reproduces the drift and proves the boundary fix.
    const Foreign = Decimal.clone({ precision: 20 });
    const value = new Foreign('573138.0375515096758');
    const addend = new Foreign('0.000000000123456789');
    const exact = new Decimal('573138.0375515096758').plus(new Decimal('0.000000000123456789'));

    const foreignSum = value.plus(addend);
    assert.ok(!foreignSum.eq(exact), 'precondition: foreign arithmetic drifts');

    const viaBoundary = fromDb(value).plus(fromDb(addend));
    assert.ok(viaBoundary.eq(exact), 'fromDb-converted arithmetic is exact');
  });

  test('fromDb passes through strings and exact foreign values', () => {
    const Foreign = Decimal.clone({ precision: 20 });
    const foreign = new Foreign('573138.037551509675800');
    assert.ok(fromDb(foreign).eq(new Decimal('573138.037551509675800')));
    assert.ok(fromDb('42.5').eq(new Decimal('42.5')));
  });

  test('toMoneyString serializes foreign Decimals through the boundary', () => {
    const Foreign = Decimal.clone({ precision: 20 });
    assert.equal(toMoneyString(new Foreign('1.0000000000000000005')), '1.000000000000000001');
    assert.equal(toMoneyString(new Decimal('-0.5')), '-0.500000000000000000');
  });
});
