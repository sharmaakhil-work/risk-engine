import { Prisma } from '@prisma/client';
import { prisma, withSerializable } from '../db.js';
import { Decimal, fromDb, normalize, toMoneyString, ZERO } from '../precision/decimal.js';

export const ACCOUNTS = {
  WALLET: 'USD',
  MARGIN: 'USD:MARGIN',
  LOAN: 'USD:LOAN',
} as const;

export type AccountName = (typeof ACCOUNTS)[keyof typeof ACCOUNTS];
export const ACCOUNT_NAMES: string[] = Object.values(ACCOUNTS);

// Wallet and margin can never go negative, the deficit loan can never go
// positive. Checked here for a typed, auditable failure — and again by the
// database CHECK constraint as the last line of defence, so even a bug that
// bypasses this module cannot corrupt a balance.
export class LedgerInvariantError extends Error {
  constructor(
    readonly account: string,
    readonly wouldBeBalance: Decimal,
  ) {
    super(`ledger invariant violated: account ${account} would become ${toMoneyString(wouldBeBalance)}`);
    this.name = 'LedgerInvariantError';
  }
}

export interface LedgerEntryInput {
  account: AccountName;
  kind: string;
  amount: Decimal;
  idempotencyKey?: string;
  tradeId?: number;
  orderId?: string;
  note?: string;
}

export type LedgerClient = Prisma.TransactionClient | typeof prisma;

function assertBalanceOk(account: string, balance: Decimal): void {
  if (account === ACCOUNTS.LOAN) {
    if (balance.greaterThan(0)) throw new LedgerInvariantError(account, balance);
  } else if (balance.isNegative()) {
    throw new LedgerInvariantError(account, balance);
  }
}

export async function appendWithinTx(tx: Prisma.TransactionClient, entries: LedgerEntryInput[]): Promise<void> {
  for (const e of entries) {
    if (e.amount.isZero()) continue;
    // Locking the newest row of the account serializes every writer to that
    // account: the next transaction either blocks here or aborts as a
    // serialization victim, so the balance_after chain can never fork.
    const latest = await tx.$queryRaw<{ id: bigint; balance_after: Decimal }[]>`
      SELECT "id", "balance_after" FROM "ledger_entries" WHERE "account" = ${e.account}
      ORDER BY "id" DESC LIMIT 1 FOR UPDATE`;
    const prev = latest[0] ? fromDb(latest[0].balance_after) : ZERO;
    const next = normalize(prev.plus(e.amount));
    assertBalanceOk(e.account, next);
    await tx.$executeRaw`
      INSERT INTO "ledger_entries" ("account","kind","amount","balance_after","idempotency_key","trade_id","order_id","note")
      VALUES (${e.account}, ${e.kind}, ${toMoneyString(e.amount)}::numeric, ${toMoneyString(next)}::numeric,
              ${e.idempotencyKey ?? null}, ${e.tradeId ?? null}, ${e.orderId ?? null}, ${e.note ?? null})`;
  }
}

export type AccountBalances = Record<string, Decimal>;

export async function balances(client: LedgerClient = prisma): Promise<AccountBalances> {
  const rows = await client.$queryRaw<{ account: string; balance: Decimal }[]>`
    SELECT DISTINCT ON ("account") "account", "balance_after" AS "balance"
    FROM "ledger_entries" ORDER BY "account", "id" DESC`;
  const out: AccountBalances = {};
  for (const row of rows) out[row.account] = fromDb(row.balance);
  return out;
}

export async function deposit(amount: Decimal, reference: string): Promise<void> {
  await withSerializable(tx =>
    appendWithinTx(tx, [
      { account: ACCOUNTS.WALLET, kind: 'DEPOSIT', amount, idempotencyKey: `deposit:${reference}`, note: reference },
    ]),
  );
}

export async function recentEntries(limit = 100, account?: string) {
  return prisma.ledgerEntry.findMany({
    where: account ? { account } : undefined,
    orderBy: { id: 'desc' },
    take: limit,
  });
}
