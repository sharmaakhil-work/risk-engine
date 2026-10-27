import { PrismaClient } from '@prisma/client';
import { config } from '../config.js';
import { Decimal, fromDb, normalize, ZERO } from '../precision/decimal.js';
import type { PositionView } from './mark-to-market.js';

export interface SymbolVar {
  symbol: string;
  days: number;
  var95: Decimal;
  var99: Decimal;
  es95: Decimal;
}

export interface VarResult {
  ok: boolean;
  reason: string | null;
  days: number;
  var95: Decimal | null;
  var99: Decimal | null;
  es95: Decimal | null;
  perSymbol: SymbolVar[];
  methodology: string;
  asOf: number;
}

const MIN_DAYS = 60;

// Exported for unit testing: the pure statistics of the historical method.
export function quantile(sortedAsc: Decimal[], p: number): Decimal {
  const n = sortedAsc.length;
  const idx = Math.max(0, Math.min(n - 1, Math.floor(p * (n - 1))));
  return sortedAsc[idx]!;
}

export function tailMean(sortedAsc: Decimal[], p: number): Decimal {
  const n = sortedAsc.length;
  const count = Math.max(1, Math.floor(p * n));
  let acc = ZERO;
  for (let i = 0; i < count && i < n; i++) acc = acc.plus(sortedAsc[i]!);
  return normalize(acc.dividedBy(count).negated());
}

// Historical-simulation VaR: today's position sizes are re-priced across the
// trailing window of daily closes, producing one portfolio P&L per historical
// day; VaR is the loss quantile of that distribution (positive = loss). All
// arithmetic is Decimal — no floats anywhere near a quantile.
export async function computeHistoricalVar(
  prisma: PrismaClient,
  positions: PositionView[],
  windowDays: number = config.varWindowDays,
): Promise<VarResult> {
  const methodology =
    'historical simulation: current position sizes re-priced across the trailing window of daily closes; VaR is the loss quantile of that P&L distribution (positive = loss)';
  const asOf = Date.now();
  const held = positions.filter(p => !p.qty.isZero());
  if (held.length === 0) {
    return { ok: false, reason: 'no open positions', days: 0, var95: null, var99: null, es95: null, perSymbol: [], methodology, asOf };
  }

  const portfolioPnl = new Map<string, Decimal>();
  const perSymbol: SymbolVar[] = [];
  let minDays = Infinity;

  for (const p of held) {
    const rows = await prisma.$queryRaw<{ ts: Date; close: Decimal }[]>`
      SELECT "ts","close" FROM "prices" WHERE "symbol" = ${p.symbol} AND "granularity" = '1d'
      ORDER BY "ts" DESC LIMIT ${windowDays + 1}`;
    const series = rows.slice().reverse();
    const symbolPnl = new Map<string, Decimal>();
    for (let i = 1; i < series.length; i++) {
      const prev = fromDb(series[i - 1]!.close);
      const cur = fromDb(series[i]!.close);
      const key = series[i]!.ts.toISOString().slice(0, 10);
      const dayPnl = normalize(p.qty.times(cur.minus(prev)));
      symbolPnl.set(key, dayPnl);
      portfolioPnl.set(key, (portfolioPnl.get(key) ?? ZERO).plus(dayPnl));
    }
    const values = [...symbolPnl.values()].sort((a, b) => a.comparedTo(b));
    minDays = Math.min(minDays, values.length);
    if (values.length >= MIN_DAYS) {
      perSymbol.push({
        symbol: p.symbol,
        days: values.length,
        var95: quantile(values, 0.05).negated(),
        var99: quantile(values, 0.01).negated(),
        es95: tailMean(values, 0.05),
      });
    }
  }

  if (minDays < MIN_DAYS) {
    return {
      ok: false,
      reason: `insufficient price history: ${minDays} daily returns available, need ${MIN_DAYS}`,
      days: minDays,
      var95: null,
      var99: null,
      es95: null,
      perSymbol,
      methodology,
      asOf,
    };
  }

  const values = [...portfolioPnl.values()].sort((a, b) => a.comparedTo(b));
  return {
    ok: true,
    reason: null,
    days: values.length,
    var95: quantile(values, 0.05).negated(),
    var99: quantile(values, 0.01).negated(),
    es95: tailMean(values, 0.05),
    perSymbol,
    methodology,
    asOf,
  };
}
