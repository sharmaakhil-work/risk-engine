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

  // Production knobs (all optional; defaults keep local development working).
  apiKey: process.env.API_KEY ?? '',
  logLevel: process.env.LOG_LEVEL ?? 'info',
  exchangeAdapter: process.env.EXCHANGE_ADAPTER ?? 'simulated',
  marginTiersJson: process.env.MARGIN_TIERS ?? '',
  retention: {
    snapshotDays: envInt('RETENTION_SNAPSHOT_DAYS', 7),
    auditDays: envInt('RETENTION_AUDIT_DAYS', 90),
    minuteBarDays: envInt('RETENTION_MINUTE_BAR_DAYS', 30),
  },

  // Anchors are only used before any history exists (fresh install with
  // exchange feeds unreachable); seed + live feeds override them immediately.
  anchors: {
    BTC: '61000',
    ETH: '3100',
    SOL: '145',
    DOGE: '0.16',
  } as Record<string, string>,
};

function envInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0) throw new Error(`${name} must be a non-negative integer`);
  return n;
}

if (!process.env.DATABASE_URL) {
  process.env.DATABASE_URL = config.databaseUrl;
}
