import './test-env.js';
import { after, before, describe, test } from 'node:test';
import assert from 'node:assert/strict';
import Decimal from 'decimal.js';
import { prisma, withSerializable } from '../src/db.js';
import { ACCOUNTS, LedgerInvariantError, appendWithinTx, balances, deposit } from '../src/positions/ledger.js';
import { closeSingletons } from './test-env.js';

const d = (s: string) => new Decimal(s);

async function resetDb(): Promise<void> {
  await prisma.$executeRawUnsafe(
    'TRUNCATE "ledger_entries","trades","positions","orders","risk_snapshots","audit_log","prices" RESTART IDENTITY',
  );
}

after(() => closeSingletons());

describe('ledger invariants (enforced by the database)', () => {
  before(async () => {
    await resetDb();
  });

  test('deposit credits the wallet with a chained balance_after', async () => {
    await deposit(d('1000'), 'ref-1');
    const bals = await balances();
    assert.ok(bals[ACCOUNTS.WALLET]!.eq(d('1000')));
    const entries = await prisma.ledgerEntry.findMany({ orderBy: { id: 'asc' } });
    assert.equal(entries.length, 1);
    assert.equal(entries[0]!.idempotencyKey, 'deposit:ref-1');
    assert.ok(entries[0]!.balanceAfter.eq(d('1000')));
  });

  test('idempotency keys are enforced by a partial unique index', async () => {
    await assert.rejects(() => deposit(d('500'), 'ref-1'), /23505|Unique|P2010/i);
  });

  test('append-only is enforced by a trigger, even for direct SQL', async () => {
    await assert.rejects(
      () => prisma.$executeRawUnsafe('UPDATE "ledger_entries" SET "amount" = 1 WHERE "id" = 1'),
      /append-only/i,
    );
    await assert.rejects(
      () => prisma.$executeRawUnsafe('DELETE FROM "ledger_entries" WHERE "id" = 1'),
      /append-only/i,
    );
  });

  test('the account-type CHECK constraint rejects a negative wallet at the database level', async () => {
    await assert.rejects(
      () =>
        prisma.$executeRawUnsafe(
          `INSERT INTO "ledger_entries" ("account","kind","amount","balance_after") VALUES ('USD','HACK','-1','-1')`,
        ),
      /ledger_balance_check/i,
    );
  });

  test('typed invariants: margin never negative, loan never positive, wallet never negative', async () => {
    await deposit(d('100'), 'wallet-100');
    // the deficit loan may go negative — that is its purpose
    await withSerializable(tx =>
      appendWithinTx(tx, [{ account: ACCOUNTS.LOAN, kind: 'DEFICIT_LOAN', amount: d('-50'), idempotencyKey: 't-loan-1' }]),
    );
    // ...but never positive: +60 from -50 would cross zero
    await assert.rejects(
      () =>
        withSerializable(tx =>
          appendWithinTx(tx, [{ account: ACCOUNTS.LOAN, kind: 'X', amount: d('60'), idempotencyKey: 't-loan-2' }]),
        ),
      (err: unknown) => err instanceof LedgerInvariantError,
    );
    // wallet and margin can never go negative
    await assert.rejects(
      () =>
        withSerializable(tx =>
          appendWithinTx(tx, [{ account: ACCOUNTS.WALLET, kind: 'X', amount: d('-999999'), idempotencyKey: 't-w-1' }]),
        ),
      (err: unknown) => err instanceof LedgerInvariantError,
    );
    await assert.rejects(
      () =>
        withSerializable(tx =>
          appendWithinTx(tx, [{ account: ACCOUNTS.MARGIN, kind: 'X', amount: d('-1'), idempotencyKey: 't-m-1' }]),
        ),
      (err: unknown) => err instanceof LedgerInvariantError,
    );
  });
});
