import './test-env.js';
import { after, before, describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { redis } from '../src/redis.js';
import { RateLimiter, RateLimitedError } from '../src/market-data/rate-limiter.js';
import { closeSingletons } from './test-env.js';

after(() => closeSingletons());

describe('token bucket rate limiter (atomic Redis Lua)', () => {
  before(async () => {
    await redis.flushdb();
  });

  test('burst up to capacity, then denied with a computed retry-after', async () => {
    const limiter = new RateLimiter(redis, { capacity: 3, refillPerSec: 20, ttlMs: 10_000 });
    assert.equal((await limiter.check('bucket-a')).allowed, true);
    assert.equal((await limiter.check('bucket-a')).allowed, true);
    assert.equal((await limiter.check('bucket-a')).allowed, true);
    const denied = await limiter.check('bucket-a');
    assert.equal(denied.allowed, false);
    assert.ok(denied.retryAfterMs > 0 && denied.retryAfterMs <= 200, `retryAfterMs=${denied.retryAfterMs}`);
    await new Promise(r => setTimeout(r, 120));
    assert.equal((await limiter.check('bucket-a')).allowed, true, 'refill restores capacity');
  });

  test('bucket state lives in Redis: a fresh instance continues the same bucket', async () => {
    // The crash-safety property: capacity can never leak, because state is
    // shared in Redis rather than held in process memory.
    const a = new RateLimiter(redis, { capacity: 1, refillPerSec: 0.001, ttlMs: 10_000 });
    assert.equal((await a.check('bucket-b')).allowed, true);
    const b = new RateLimiter(redis, { capacity: 1, refillPerSec: 0.001, ttlMs: 10_000 });
    assert.equal((await b.check('bucket-b')).allowed, false);
  });

  test('limit() throws the typed error exactly at the boundary', async () => {
    const limiter = new RateLimiter(redis, { capacity: 1, refillPerSec: 0.001, ttlMs: 10_000 });
    await limiter.limit('bucket-c');
    await assert.rejects(() => limiter.limit('bucket-c'), RateLimitedError);
  });

  test('scopes are independent buckets', async () => {
    const limiter = new RateLimiter(redis, { capacity: 1, refillPerSec: 0.001, ttlMs: 10_000 });
    await limiter.limit('bucket-d1');
    await limiter.limit('bucket-d2');
    await assert.rejects(() => limiter.limit('bucket-d1'), RateLimitedError);
  });
});
