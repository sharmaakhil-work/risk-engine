import { prisma } from '../db.js';
import { config } from '../config.js';
import { component } from '../observability/logger.js';

const log = component('retention');

// Batched deletes: a sweep must never hold a big lock on tables the
// settlement path writes to. Table and column names are literals from this
// module — only the ISO cutoff is interpolated.
async function batchedDelete(table: string, column: string, extraWhere: string, cutoff: Date): Promise<number> {
  let total = 0;
  for (;;) {
    const n = await prisma.$executeRawUnsafe(
      `DELETE FROM "${table}" WHERE "${column}" < '${cutoff.toISOString()}' ${extraWhere} ` +
        `AND ctid IN (SELECT ctid FROM "${table}" WHERE "${column}" < '${cutoff.toISOString()}' ${extraWhere} LIMIT 5000)`,
    );
    total += n;
    if (n < 5000) break;
  }
  return total;
}

function daysAgo(days: number): Date {
  return new Date(Date.now() - days * 86_400_000);
}

export interface RetentionReport {
  snapshots: number;
  audit: number;
  bars: number;
}

// Unbounded growth is a production incident waiting to happen: snapshots are
// high-frequency, audit entries accumulate per anomaly, and 1-minute bars are
// only useful for a recent window (daily history lives on for VaR). A value
// of 0 disables that class of retention — the audit log is a reasonable
// candidate for keep-forever in a regulated setting.
export async function runRetention(): Promise<RetentionReport> {
  const { snapshotDays, auditDays, minuteBarDays } = config.retention;
  const snapshots = snapshotDays > 0 ? await batchedDelete('risk_snapshots', 'ts', '', daysAgo(snapshotDays)) : 0;
  const audit = auditDays > 0 ? await batchedDelete('audit_log', 'ts', '', daysAgo(auditDays)) : 0;
  const bars =
    minuteBarDays > 0 ? await batchedDelete('prices', 'ts', `AND "granularity" = '1m'`, daysAgo(minuteBarDays)) : 0;
  if (snapshots + audit + bars > 0) {
    log.info({ snapshots, audit, bars }, 'retention sweep');
  }
  return { snapshots, audit, bars };
}
