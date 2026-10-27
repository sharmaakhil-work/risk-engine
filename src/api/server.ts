import Fastify from 'fastify';
import { config } from '../config.js';
import { redis } from '../redis.js';
import { RateLimiter, RateLimitedError } from '../market-data/rate-limiter.js';
import { ExchangeError, InsufficientFundsError, StaleMarketError, UnknownSymbolError } from '../positions/exchange-api.js';
import { toMoneyString } from '../precision/decimal.js';
import { log } from '../observability/logger.js';
import { registerAuth } from './auth.js';
import { registerHealthRoutes } from './health.js';
import { registerPortfolioRoutes } from './positions.js';
import { registerRiskRoutes } from './risk.js';

const app = Fastify({ logger: log });
const apiLimiter = new RateLimiter(redis, { capacity: 50, refillPerSec: 10 });

// Auth first (401 before 429), then the per-client rate limit.
registerAuth(app);

app.addHook('preHandler', async (request, reply) => {
  const decision = await apiLimiter.check(`api:${request.ip}`);
  if (!decision.allowed) {
    await reply.code(429).header('retry-after-ms', String(decision.retryAfterMs)).send({ error: 'rate limited' });
  }
});

app.setErrorHandler((err: unknown, _request, reply) => {
  if (err instanceof RateLimitedError) {
    return reply.code(429).header('retry-after-ms', String(err.retryAfterMs)).send({ error: 'rate limited' });
  }
  if (err instanceof StaleMarketError) {
    return reply.code(503).send({ error: 'stale market data — new orders refused', symbol: err.symbol });
  }
  if (err instanceof InsufficientFundsError) {
    return reply
      .code(422)
      .send({ error: 'insufficient free funds', required: toMoneyString(err.required), available: toMoneyString(err.available) });
  }
  if (err instanceof UnknownSymbolError) {
    return reply.code(400).send({ error: `unknown symbol: ${err.message}` });
  }
  if (err instanceof ExchangeError) {
    return reply.code(400).send({ error: err.message });
  }
  const e = err as Error & { statusCode?: number };
  const statusCode = e.statusCode;
  if (typeof statusCode === 'number' && statusCode >= 400 && statusCode < 500) {
    return reply.code(statusCode).send({ error: e.message });
  }
  log.error({ err: e.stack ?? String(err) }, 'unhandled request error');
  return reply.code(500).send({ error: 'internal error' });
});

async function start(): Promise<void> {
  await registerHealthRoutes(app);
  await registerPortfolioRoutes(app);
  await registerRiskRoutes(app);
  await app.listen({ port: config.apiPort, host: '127.0.0.1' });
  log.info({ url: `http://127.0.0.1:${config.apiPort}`, auth: Boolean(config.apiKey) }, 'api listening');
  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.on(signal, () => {
      void app.close().then(() => process.exit(0));
    });
  }
}

start().catch(err => {
  log.error({ err: (err as Error).stack ?? String(err) }, 'api fatal');
  process.exit(1);
});
