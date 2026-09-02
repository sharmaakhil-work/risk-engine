import './config.js';
import { Prisma, PrismaClient } from '@prisma/client';
import { jitterBackoff, sleep } from './util.js';

export const prisma = new PrismaClient({ log: ['warn', 'error'] });

// SERIALIZABLE transactions can abort with 40001 at commit time by design;
// retrying a bounded number of times with jitter is the textbook remedy. Every
// ledger/position mutation in this system goes through this wrapper.
const RETRYABLE = new Set(['P2034', 'P2024', 'P1001', 'P1002']);

function isRetryable(err: unknown): boolean {
  const e = err as { code?: string; message?: string };
  if (e?.code && RETRYABLE.has(e.code)) return true;
  const msg = String(e?.message ?? '');
  return msg.includes('40001') || msg.includes('could not serialize') || msg.includes('deadlock');
}

export async function withSerializable<T>(
  fn: (tx: Prisma.TransactionClient) => Promise<T>,
  attempts = 8,
): Promise<T> {
  let lastErr: unknown;
  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      return await prisma.$transaction(fn, {
        isolationLevel: Prisma.TransactionIsolationLevel.Serializable,
        timeout: 20_000,
      });
    } catch (err) {
      lastErr = err;
      if (isRetryable(err) && attempt < attempts - 1) {
        await sleep(jitterBackoff(attempt));
        continue;
      }
      throw err;
    }
  }
  throw lastErr;
}

export async function pingDb(): Promise<boolean> {
  await prisma.$queryRaw`SELECT 1`;
  return true;
}
