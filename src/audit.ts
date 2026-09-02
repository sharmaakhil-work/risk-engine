import { Prisma } from '@prisma/client';
import { prisma } from './db.js';

export type AuditClient = Prisma.TransactionClient | typeof prisma;

export async function audit(
  client: AuditClient,
  kind: string,
  subject: string,
  message: string,
  data?: Record<string, unknown>,
): Promise<void> {
  await client.auditLog.create({
    data: {
      kind,
      subject,
      message,
      data: data === undefined ? undefined : (data as Prisma.InputJsonValue),
    },
  });
}
