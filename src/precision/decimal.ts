import Decimal from 'decimal.js';

// Money discipline: every monetary value is a decimal.js Decimal quantized to
// 18 dp with ROUND_UP (away from zero), a single rounding mode for the whole
// system. ROUND_UP never understates fees, losses or margin requirements, and
// using one mode everywhere guarantees the same normalized value is written to
// ledger and positions. Binary floats are never used for money; the only place
// a JS number may appear is JSON input at the API boundary, converted once via
// its shortest decimal string representation.
Decimal.set({ precision: 40, toExpNeg: -30, toExpPos: 40 });

export const SCALE = 18;
export type Money = Decimal;
export { Decimal };

export const ZERO = new Decimal(0);
export const ONE = new Decimal(1);
export const HUNDRED = new Decimal(100);

// money() is also the read boundary for database values: Prisma bundles its
// own copy of decimal.js configured with the default 20-significant-digit
// precision, so any arithmetic performed on a Prisma-returned Decimal would
// silently round at 20 digits (observed as ~1e-8 drift on six-figure sums).
// Foreign Decimal instances are converted through their exact string form,
// which never loses a digit, and all subsequent arithmetic runs on this
// module's 40-digit configuration.
export function money(value: Decimal | string | number): Decimal {
  if (value instanceof Decimal) return value;
  return new Decimal(String(value));
}

export function fromDb(value: Decimal | string): Decimal {
  return value instanceof Decimal ? new Decimal(value.toString()) : new Decimal(value);
}

export function normalize(d: Decimal): Decimal {
  return d.toDecimalPlaces(SCALE, Decimal.ROUND_UP);
}

export function mul(a: Decimal, b: Decimal): Decimal {
  return normalize(a.times(b));
}

export function div(a: Decimal, b: Decimal): Decimal {
  return normalize(a.dividedBy(b));
}

export function add(a: Decimal, b: Decimal): Decimal {
  return normalize(a.plus(b));
}

export function sub(a: Decimal, b: Decimal): Decimal {
  return normalize(a.minus(b));
}

export function neg(a: Decimal): Decimal {
  return normalize(a.negated());
}

export function sum(values: Iterable<Decimal>): Decimal {
  let acc = ZERO;
  for (const v of values) acc = acc.plus(v);
  return normalize(acc);
}

export function maxDec(a: Decimal, b: Decimal): Decimal {
  return a.greaterThan(b) ? a : b;
}

export function minDec(a: Decimal, b: Decimal): Decimal {
  return a.lessThan(b) ? a : b;
}

export function isZero(a: Decimal): boolean {
  return a.isZero();
}

// Canonical storage/serialization form: fixed 18 dp string. The same string is
// what lands in Postgres DECIMAL(38,18) columns, Redis payloads and stream
// messages, so no representation drift is possible between stores.
export function toMoneyString(d: Decimal): string {
  return money(d).toFixed(SCALE);
}

export function abs(a: Decimal): Decimal {
  return normalize(a.abs());
}
