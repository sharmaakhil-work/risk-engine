import os from 'node:os';

// MUST be the first import in every test file: it redirects every app module
// (via their config reads) to an isolated database and Redis logical DB so
// tests can never touch the dev books (risk_engine, db 0).
function resolveTestDbUrl(): string {
  if (process.env.DATABASE_URL_TEST) return process.env.DATABASE_URL_TEST;
  const existing = process.env.DATABASE_URL;
  // Only trust a pre-set URL if it provably points at the test database (the
  // CI service container case) — never at a dev database.
  if (existing && existing.includes('risk_engine_test')) return existing;
  const pgUser = process.env.PGUSER ?? os.userInfo().username;
  return `postgresql://${pgUser}@localhost:5432/risk_engine_test?schema=public`;
}

function resolveTestRedisUrl(): string {
  if (process.env.REDIS_URL_TEST) return process.env.REDIS_URL_TEST;
  const existing = process.env.REDIS_URL;
  if (existing && /:\d+\/1$/.test(existing)) return existing;
  return 'redis://localhost:6379/1';
}

export const TEST_DATABASE_URL = resolveTestDbUrl();
export const TEST_REDIS_URL = resolveTestRedisUrl();

process.env.DATABASE_URL = TEST_DATABASE_URL;
process.env.REDIS_URL = TEST_REDIS_URL;

// Tests import singleton-carrying modules (prisma, redis); the connections
// must be drained or the test process never exits.
export async function closeSingletons(): Promise<void> {
  const { prisma } = await import('../src/db.js');
  const { redis } = await import('../src/redis.js');
  redis.disconnect();
  await prisma.$disconnect();
}
