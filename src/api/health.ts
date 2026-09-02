import type { FastifyInstance } from 'fastify';
import { pingDb, prisma } from '../db.js';
import { redis, pingRedis, KEYS } from '../redis.js';
import { priceStore } from '../market-data/price-store.js';
import { toMoneyString } from '../precision/decimal.js';

export async function registerHealthRoutes(app: FastifyInstance): Promise<void> {
  app.get('/health', async () => {
    const started = Date.now();
    let postgres = 'ok';
    try {
      await pingDb();
    } catch {
      postgres = 'down';
    }
    let redisState = 'ok';
    try {
      await pingRedis();
    } catch {
      redisState = 'down';
    }

    const feeds = await priceStore.feedStatuses().catch(() => []);
    const marks = await priceStore.getMarks().catch(() => new Map());
    const heartbeat = await redis.get(KEYS.workerHeartbeat).catch(() => null);
    const lastEval = await redis.get(KEYS.workerLastEval).catch(() => null);
    const lastSnapshot = await prisma.riskSnapshot
      .findFirst({ orderBy: { id: 'desc' } })
      .catch(() => null);

    const workerAlive = Boolean(heartbeat);
    const degraded = postgres !== 'ok' || redisState !== 'ok' || !workerAlive;
    return {
      status: postgres === 'ok' && redisState === 'ok' ? (workerAlive ? 'ok' : 'degraded') : 'down',
      degraded,
      checks: { postgres, redis: redisState },
      worker: {
        alive: workerAlive,
        id: heartbeat,
        lastEvalMsAgo: lastEval ? Date.now() - Number(lastEval) : null,
        lastSnapshot: lastSnapshot
          ? {
              ts: lastSnapshot.ts.toISOString(),
              status: lastSnapshot.status,
              ageMs: Date.now() - lastSnapshot.ts.getTime(),
            }
          : null,
      },
      feeds,
      marks: [...marks.entries()].map(([symbol, m]) => ({
        symbol,
        source: m.source,
        price: toMoneyString(m.price),
        ageMs: m.ageMs,
        stale: m.stale,
      })),
      uptimeSec: Math.round(process.uptime()),
      checkedInMs: Date.now() - started,
    };
  });

  app.get('/ready', async (_request, reply) => {
    try {
      await pingDb();
      await pingRedis();
      return { ready: true };
    } catch {
      return await reply.code(503).send({ ready: false });
    }
  });
}
