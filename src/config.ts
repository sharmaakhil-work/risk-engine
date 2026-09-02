import os from 'node:os';

export const SYMBOLS = ['BTC', 'ETH', 'SOL', 'DOGE'] as const;
export type Symbol = (typeof SYMBOLS)[number];

export const SYMBOL_SET = new Set<string>(SYMBOLS);

export const VENUES = {
  binance: 'sim-binance',
  coinbase: 'sim-coinbase',
} as const;

export function isSymbol(value: string): value is Symbol {
  return SYMBOL_SET.has(value);
}

const pgUser = process.env.PGUSER ?? os.userInfo().username;

export const config = {
  databaseUrl: process.env.DATABASE_URL ?? `postgresql://${pgUser}@localhost:5432/risk_engine?schema=public`,
  redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
  defaultVenue: VENUES.binance,

  apiPort: Number(process.env.PORT ?? 8080),

  staleAfterMs: 15_000,
  barFlushMs: 5_000,
  riskEvalIntervalMs: 3_000,
  reconcileIntervalMs: 15_000,
  fillPumpIntervalMs: 700,
  streamMaxLen: 5_000,
  claimMinIdleMs: 30_000,
  orderTtlMs: 45_000,

  feeBps: 7,
  slippageBps: 15,
  liquidationSlippageBps: 25,

  varWindowDays: 500,
  marginWarningRatio: 1.4,
  liquidationTargetRatio: 1.5,

  // Anchors are only used before any history exists (fresh install with
  // exchange feeds unreachable); seed + live feeds override them immediately.
  anchors: {
    BTC: '61000',
    ETH: '3100',
    SOL: '145',
    DOGE: '0.16',
  } as Record<string, string>,
};

if (!process.env.DATABASE_URL) {
  process.env.DATABASE_URL = config.databaseUrl;
}
