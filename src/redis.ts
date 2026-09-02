import Redis from 'ioredis';
import { config } from './config.js';

export const redis = new Redis(config.redisUrl, {
  maxRetriesPerRequest: 3,
  retryStrategy: t => Math.min(t * 200, 5_000),
  enableAutoPipelining: true,
});

// Redis processes commands per connection sequentially, so a blocking
// XREADGROUP would stall every other command sharing that connection for the
// full BLOCK window. Blocking consumers therefore get dedicated connections.
export function createBlockingConnection(): Redis {
  return new Redis(config.redisUrl, {
    maxRetriesPerRequest: null,
    retryStrategy: t => Math.min(t * 200, 5_000),
  });
}

export async function pingRedis(): Promise<boolean> {
  return (await redis.ping()) === 'PONG';
}

export const KEYS = {
  mark: (symbol: string) => `mark:${symbol}`,
  sourceTick: (symbol: string) => `src:${symbol}`,
  scenario: (symbol: string) => `scenario:${symbol}`,
  feedStatus: (venue: string) => `feed:${venue}`,
  workerHeartbeat: 'worker:heartbeat',
  workerLastEval: 'worker:last-eval',
  fillPumpLock: 'lock:fill-pump',
  liquidationLock: 'lock:liquidation',
  orderBook: (orderId: string) => `sim:order:${orderId}`,
  openOrders: 'sim:open',
  doneOrders: 'sim:done',
  orderSeq: 'sim:seq',
} as const;

export const STREAMS = {
  fills: 'fills',
  exchangeTrades: 'exchange:trades',
  riskJobs: 'risk:jobs',
  riskEvents: 'risk:events',
} as const;

export const GROUPS = {
  settlers: 'settlers',
  evaluators: 'evaluators',
} as const;

export async function ensureStreamGroup(stream: string, group: string): Promise<void> {
  try {
    await redis.xgroup('CREATE', stream, group, '$', 'MKSTREAM');
  } catch (err) {
    if (!String((err as Error).message).includes('BUSYGROUP')) throw err;
  }
}

// Compare-and-delete release: only the lock owner can clear it, so a slow
// worker can never delete a lock a faster successor already re-acquired.
const RELEASE_LOCK_LUA = `
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
`;

export async function tryLock(key: string, token: string, ttlMs: number): Promise<boolean> {
  return (await redis.set(key, token, 'PX', ttlMs, 'NX')) === 'OK';
}

export async function releaseLock(key: string, token: string): Promise<void> {
  await redis.eval(RELEASE_LOCK_LUA, 1, key, token);
}
