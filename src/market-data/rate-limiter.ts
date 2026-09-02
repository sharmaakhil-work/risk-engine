import type Redis from 'ioredis';

// Token bucket implemented as a single Redis Lua script: check-and-consume is
// atomic, and bucket state lives in Redis rather than process memory, so a
// crashed worker can never strand capacity (the failure mode of in-process
// semaphores). Redis server TIME is used so every caller shares one clock.
const TOKEN_BUCKET_LUA = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + tonumber(t[2]) / 1000
local cap = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local last = tonumber(state[2])
if tokens == nil then
  tokens = cap
  last = now
end
tokens = math.min(cap, tokens + math.max(0, now - last) / 1000 * refill)
local allowed = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], ttl)
return { allowed, math.floor(tokens) }
`;

export class RateLimitedError extends Error {
  constructor(
    readonly scope: string,
    readonly retryAfterMs: number,
  ) {
    super(`rate limited: ${scope}`);
    this.name = 'RateLimitedError';
  }
}

export interface BucketOptions {
  capacity: number;
  refillPerSec: number;
  ttlMs?: number;
}

export interface BucketDecision {
  allowed: boolean;
  remaining: number;
  retryAfterMs: number;
}

export class RateLimiter {
  constructor(
    private redis: Redis,
    private opts: BucketOptions,
  ) {}

  async check(scope: string, requested = 1): Promise<BucketDecision> {
    const res = (await this.redis.eval(
      TOKEN_BUCKET_LUA,
      1,
      `rl:${scope}`,
      this.opts.capacity,
      this.opts.refillPerSec,
      requested,
      this.opts.ttlMs ?? 120_000,
    )) as [number, number];
    const allowed = Number(res[0]) === 1;
    const remaining = Number(res[1]);
    const retryAfterMs = allowed ? 0 : Math.ceil(((requested - remaining) / this.opts.refillPerSec) * 1000);
    return { allowed, remaining, retryAfterMs };
  }

  async limit(scope: string, requested = 1): Promise<void> {
    const decision = await this.check(scope, requested);
    if (!decision.allowed) throw new RateLimitedError(scope, decision.retryAfterMs);
  }
}
