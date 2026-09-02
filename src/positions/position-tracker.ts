import { prisma, withSerializable } from '../db.js';
import { audit } from '../audit.js';
import { config, isSymbol } from '../config.js';
import { ACCOUNTS, LedgerEntryInput, LedgerInvariantError, appendWithinTx } from './ledger.js';
import { incrementalInitial } from '../risk/margin.js';
import { Decimal, fromDb, mul, normalize, toMoneyString, ZERO } from '../precision/decimal.js';

export interface FillMessage {
  orderId?: string;
  externalId: string;
  venue: string;
  symbol: string;
  side: 'BUY' | 'SELL';
  qty: Decimal;
  price: Decimal;
  fee: Decimal;
  ts: number;
  liquidation?: boolean;
  source?: string;
}

export type SettleOutcome =
  | { status: 'SETTLED'; tradeId: number }
  | { status: 'DUPLICATE'; tradeId: number }
  | { status: 'REJECTED'; reason: string }
  | { status: 'DEFERRED'; reason: string };

interface PositionRow {
  id: number;
  qty: Decimal;
  avg_entry_price: Decimal;
  realized_pnl: Decimal;
  margin_posted: Decimal;
  version: number;
}

interface OrderRow {
  id: string;
  qty: Decimal;
  margin_reserved: Decimal;
  margin_released: Decimal;
  filled_qty: Decimal;
  avg_fill_price: Decimal;
  status: string;
}

export function fillToFields(fill: FillMessage): string[] {
  return [
    'orderId', fill.orderId ?? '',
    'externalId', fill.externalId,
    'venue', fill.venue,
    'symbol', fill.symbol,
    'side', fill.side,
    'qty', toMoneyString(fill.qty),
    'price', toMoneyString(fill.price),
    'fee', toMoneyString(fill.fee),
    'ts', String(fill.ts),
    'liquidation', fill.liquidation ? '1' : '0',
    'source', fill.source ?? 'stream',
  ];
}

export function parseFillFields(fields: string[]): FillMessage {
  const m = new Map<string, string>();
  for (let i = 0; i + 1 < fields.length; i += 2) m.set(fields[i]!, fields[i + 1]!);
  return {
    orderId: m.get('orderId') || undefined,
    externalId: m.get('externalId') ?? '',
    venue: m.get('venue') ?? config.defaultVenue,
    symbol: m.get('symbol') ?? '',
    side: (m.get('side') === 'SELL' ? 'SELL' : 'BUY') as 'BUY' | 'SELL',
    qty: new Decimal(m.get('qty') ?? '0'),
    price: new Decimal(m.get('price') ?? '0'),
    fee: new Decimal(m.get('fee') ?? '0'),
    ts: Number(m.get('ts') ?? Date.now()),
    liquidation: m.get('liquidation') === '1',
    source: m.get('source') ?? 'stream',
  };
}

interface SettleMath {
  newQty: Decimal;
  newAvg: Decimal;
  realized: Decimal;
  postedAdd: Decimal;
  postedRelease: Decimal;
}

function wrapPositionRow(row: PositionRow): PositionRow {
  return {
    id: row.id,
    qty: fromDb(row.qty),
    avg_entry_price: fromDb(row.avg_entry_price),
    realized_pnl: fromDb(row.realized_pnl),
    margin_posted: fromDb(row.margin_posted),
    version: row.version,
  };
}

function wrapOrderRow(row: OrderRow): OrderRow {
  return {
    id: row.id,
    qty: fromDb(row.qty),
    margin_reserved: fromDb(row.margin_reserved),
    margin_released: fromDb(row.margin_released),
    filled_qty: fromDb(row.filled_qty),
    avg_fill_price: fromDb(row.avg_fill_price),
    status: row.status,
  };
}

function positionMath(pos: PositionRow, fill: FillMessage): SettleMath {
  const oldQty = pos.qty;
  const oldAvg = pos.avg_entry_price;
  const oldPosted = pos.margin_posted;
  const signedFill = fill.side === 'BUY' ? fill.qty : fill.qty.negated();
  const newQty = normalize(oldQty.plus(signedFill));

  let realized = ZERO;
  let postedAdd = ZERO;
  let postedRelease = ZERO;
  let newAvg = oldAvg;

  if (oldQty.isZero()) {
    newAvg = fill.price;
    postedAdd = incrementalInitial(ZERO, mul(fill.qty, fill.price));
  } else if (oldQty.greaterThan(0) === signedFill.greaterThan(0)) {
    newAvg = normalize(oldAvg.times(oldQty.abs()).plus(fill.price.times(fill.qty)).dividedBy(newQty.abs()));
    postedAdd = incrementalInitial(mul(oldQty.abs(), fill.price), mul(newQty.abs(), fill.price));
  } else {
    const closeQty = Decimal.min(fill.qty, oldQty.abs());
    const perUnit = oldQty.greaterThan(0) ? fill.price.minus(oldAvg) : oldAvg.minus(fill.price);
    realized = normalize(perUnit.times(closeQty));
    postedRelease = normalize(oldPosted.times(closeQty).dividedBy(oldQty.abs()));
    if (newQty.isZero()) {
      newAvg = ZERO;
    } else if (newQty.greaterThan(0) === oldQty.greaterThan(0)) {
      newAvg = oldAvg;
    } else {
      newAvg = fill.price;
      postedAdd = incrementalInitial(ZERO, mul(newQty.abs(), fill.price));
    }
  }

  return { newQty, newAvg, realized, postedAdd, postedRelease };
}

// Settlement of a single exchange fill. Everything below runs in one
// SERIALIZABLE transaction with bounded retry:
//   1. claim the trade row via the partial unique index (duplicate = no-op)
//   2. lock the position row FOR UPDATE SKIP LOCKED (busy = defer to sweep)
//   3. compute position + ledger math once, in Decimal, with a single
//      ROUND_UP normalization — the same values are written to both stores
//   4. append ledger entries with per-entry balance_after under lock
//   5. update position, order, trade
// A crash at any point leaves either no trace (rollback) or a RECEIVED trade
// that the reclaim sweep re-settles idempotently.
export async function settleFill(fill: FillMessage): Promise<SettleOutcome> {
  if (!isSymbol(fill.symbol)) return { status: 'REJECTED', reason: `unknown symbol ${fill.symbol}` };
  if (!fill.externalId) return { status: 'REJECTED', reason: 'missing external trade id' };
  if (!fill.qty.greaterThan(0) || !fill.price.greaterThan(0)) {
    return { status: 'REJECTED', reason: 'qty and price must be positive' };
  }
  if (fill.fee.isNegative()) return { status: 'REJECTED', reason: 'fee must not be negative' };

  try {
    return await withSerializable(async tx => {
      const inserted = await tx.$queryRaw<{ id: number }[]>`
        INSERT INTO "trades" ("external_id","venue","symbol","side","qty","price","fee","order_id","is_liquidation","source","trade_ts","status")
        VALUES (${fill.externalId}, ${fill.venue}, ${fill.symbol}, ${fill.side}, ${toMoneyString(fill.qty)}::numeric, ${toMoneyString(fill.price)}::numeric,
                ${toMoneyString(fill.fee)}::numeric, ${fill.orderId ?? null}, ${fill.liquidation === true}, ${fill.source ?? 'stream'},
                ${new Date(fill.ts)}, 'RECEIVED')
        ON CONFLICT ("venue","external_id") WHERE "status" <> 'REJECTED' DO NOTHING
        RETURNING "id"`;

      let tradeId: number;
      if (inserted[0]) {
        tradeId = inserted[0].id;
      } else {
        const existing = await tx.$queryRaw<{ id: number; status: string }[]>`
          SELECT "id","status" FROM "trades" WHERE "venue" = ${fill.venue} AND "external_id" = ${fill.externalId} FOR UPDATE`;
        const row = existing[0];
        if (!row) return { status: 'REJECTED', reason: 'trade row vanished mid-transaction' } satisfies SettleOutcome;
        if (row.status === 'SETTLED') {
          // Duplicate delivery of an already-settled trade: audited, acked,
          // never settled twice. The database-level idempotency contract.
          await audit(tx, 'DUPLICATE_TRADE', `${fill.venue}:${fill.externalId}`, 'duplicate fill delivery ignored', {
            source: fill.source ?? 'stream',
            symbol: fill.symbol,
            qty: toMoneyString(fill.qty),
          });
          return { status: 'DUPLICATE', tradeId: row.id } satisfies SettleOutcome;
        }
        tradeId = row.id;
      }

      let pos = (await tx.$queryRaw<PositionRow[]>`
        SELECT "id","qty","avg_entry_price","realized_pnl","margin_posted","version" FROM "positions"
        WHERE "symbol" = ${fill.symbol} AND "venue" = ${fill.venue}
        FOR UPDATE SKIP LOCKED`)[0];
      if (!pos) {
        pos = (await tx.$queryRaw<PositionRow[]>`
          INSERT INTO "positions" ("symbol","venue","created_at","updated_at")
          VALUES (${fill.symbol}, ${fill.venue}, now(), now())
          ON CONFLICT ("symbol","venue") DO NOTHING
          RETURNING "id","qty","avg_entry_price","realized_pnl","margin_posted","version"`)[0];
        // No row and no insert means another settler holds the lock on this
        // (symbol, venue): defer rather than block, the reclaim sweep retries.
        if (!pos) return { status: 'DEFERRED', reason: 'position row locked by concurrent settler' } satisfies SettleOutcome;
      }
      // DB-returned Decimals are wrapped onto this module's 40-digit decimal.js
      // configuration before any arithmetic (see precision/decimal.ts).
      pos = wrapPositionRow(pos);

      let order: OrderRow | null = null;
      if (fill.orderId) {
        const rawOrder = (await tx.$queryRaw<OrderRow[]>`
          SELECT "id","qty","margin_reserved","margin_released","filled_qty","avg_fill_price","status" FROM "orders"
          WHERE "id" = ${fill.orderId} FOR UPDATE`)[0] ?? null;
        order = rawOrder ? wrapOrderRow(rawOrder) : null;
      }

      const math = positionMath(pos, fill);

      const balRows = await tx.$queryRaw<{ account: string; balance_after: Decimal }[]>`
        SELECT DISTINCT ON ("account") "account","balance_after" FROM "ledger_entries" ORDER BY "account","id" DESC`;
      const bal = new Map(balRows.map(r => [r.account, fromDb(r.balance_after)]));
      const walletBal = bal.get(ACCOUNTS.WALLET) ?? ZERO;

      const entries: LedgerEntryInput[] = [];

      let orderRelease = ZERO;
      if (order && order.margin_reserved.greaterThan(0) && !fill.liquidation) {
        const filledAfter = normalize(order.filled_qty.plus(fill.qty));
        if (filledAfter.greaterThanOrEqualTo(order.qty)) {
          // The final fill releases the exact remainder so releases sum to the
          // reservation to the last nano — ROUND_UP drift accumulated by
          // earlier pro-rata releases is absorbed here, possibly as a tiny
          // negative release back into the ring-fence.
          orderRelease = normalize(order.margin_reserved.minus(order.margin_released));
        } else {
          orderRelease = normalize(order.margin_reserved.times(fill.qty).dividedBy(order.qty));
        }
      }

      // Wallet end-state across this fill's entries. A negative outcome is the
      // bankruptcy gap (realized loss beyond wallet plus releases); the
      // deficit loan funds it so every intermediate balance stays invariant.
      // In production this is where an insurance fund / ADL step takes over.
      // The margin account itself stays a pure ring-fence: its balance always
      // equals Σ posted margin plus outstanding order reservations.
      const walletEnd = walletBal
        .plus(orderRelease)
        .plus(math.postedRelease)
        .plus(math.realized)
        .minus(math.postedAdd)
        .minus(fill.fee);
      if (walletEnd.isNegative()) {
        const cover = normalize(walletEnd.negated());
        entries.push(
          {
            account: ACCOUNTS.WALLET,
            kind: 'LOSS_COVER',
            amount: cover,
            idempotencyKey: `pnl:${fill.externalId}:cover`,
            tradeId,
            note: 'deficit loan covers loss beyond wallet',
          },
          {
            account: ACCOUNTS.LOAN,
            kind: 'DEFICIT_LOAN',
            amount: cover.negated(),
            idempotencyKey: `pnl:${fill.externalId}:loan`,
            tradeId,
          },
        );
      }

      // Credits before debits so no intermediate wallet balance can dip below
      // zero even when the end-state only just clears it. A negative
      // orderRelease (final-fill rounding residue) is a debit measured in
      // nanos and is applied the same way.
      if (!orderRelease.isZero()) {
        entries.push(
          {
            account: ACCOUNTS.MARGIN,
            kind: 'MARGIN_RELEASE',
            amount: orderRelease.negated(),
            idempotencyKey: `rl:${fill.externalId}`,
            tradeId,
            orderId: order?.id,
            note: 'order reservation release (pro-rata, exact on final fill)',
          },
          {
            account: ACCOUNTS.WALLET,
            kind: 'MARGIN_RELEASE',
            amount: orderRelease,
            idempotencyKey: `rl:${fill.externalId}:w`,
            tradeId,
            orderId: order?.id,
          },
        );
      }
      if (math.postedRelease.greaterThan(0)) {
        entries.push(
          {
            account: ACCOUNTS.MARGIN,
            kind: 'MARGIN_RELEASE',
            amount: math.postedRelease.negated(),
            idempotencyKey: `mr:${fill.externalId}`,
            tradeId,
            note: 'posted margin released on position reduction',
          },
          {
            account: ACCOUNTS.WALLET,
            kind: 'MARGIN_RELEASE',
            amount: math.postedRelease,
            idempotencyKey: `mr:${fill.externalId}:w`,
            tradeId,
          },
        );
      }
      if (math.realized.greaterThan(0)) {
        entries.push({
          account: ACCOUNTS.WALLET,
          kind: 'REALIZED_PNL',
          amount: math.realized,
          idempotencyKey: `pnl:${fill.externalId}`,
          tradeId,
        });
      }
      if (math.postedAdd.greaterThan(0)) {
        entries.push(
          {
            account: ACCOUNTS.WALLET,
            kind: 'MARGIN_POST',
            amount: math.postedAdd.negated(),
            idempotencyKey: `post:${fill.externalId}`,
            tradeId,
            note: 'incremental initial margin at fill price',
          },
          {
            account: ACCOUNTS.MARGIN,
            kind: 'MARGIN_POST',
            amount: math.postedAdd,
            idempotencyKey: `post:${fill.externalId}:m`,
            tradeId,
          },
        );
      }
      if (math.realized.lessThan(0)) {
        entries.push({
          account: ACCOUNTS.WALLET,
          kind: 'REALIZED_PNL',
          amount: math.realized,
          idempotencyKey: `pnl:${fill.externalId}`,
          tradeId,
        });
      }
      if (fill.fee.greaterThan(0)) {
        entries.push({
          account: ACCOUNTS.WALLET,
          kind: 'FEE',
          amount: fill.fee.negated(),
          idempotencyKey: `fee:${fill.externalId}`,
          tradeId,
        });
      }

      await appendWithinTx(tx, entries);

      await tx.$executeRaw`
        UPDATE "positions" SET
          "qty" = ${toMoneyString(math.newQty)}::numeric,
          "avg_entry_price" = ${toMoneyString(math.newAvg)}::numeric,
          "realized_pnl" = ${toMoneyString(normalize(pos.realized_pnl.plus(math.realized)))}::numeric,
          "margin_posted" = ${toMoneyString(normalize(pos.margin_posted.plus(math.postedAdd).minus(math.postedRelease)))}::numeric,
          "version" = ${pos.version + 1},
          "updated_at" = now()
        WHERE "id" = ${pos.id}`;

      if (order) {
        const newFilled = normalize(order.filled_qty.plus(fill.qty));
        const newAvgFill = normalize(
          order.avg_fill_price.times(order.filled_qty).plus(fill.price.times(fill.qty)).dividedBy(newFilled),
        );
        const orderStatus = newFilled.greaterThanOrEqualTo(order.qty) ? 'SETTLED' : 'PARTIALLY_SETTLED';
        await tx.$executeRaw`
          UPDATE "orders" SET
            "filled_qty" = ${toMoneyString(newFilled)}::numeric,
            "avg_fill_price" = ${toMoneyString(newAvgFill)}::numeric,
            "margin_released" = ${toMoneyString(normalize(order.margin_released.plus(orderRelease)))}::numeric,
            "status" = ${orderStatus},
            "updated_at" = now()
          WHERE "id" = ${order.id}`;
      }

      await tx.$executeRaw`
        UPDATE "trades" SET "status" = 'SETTLED', "settled_at" = now()
        WHERE "id" = ${tradeId} AND "status" <> 'SETTLED'`;

      if (fill.liquidation) {
        await audit(tx, 'LIQUIDATION_FILL', `${fill.venue}:${fill.externalId}`, 'liquidation fill settled', {
          symbol: fill.symbol,
          qty: toMoneyString(fill.qty),
          price: toMoneyString(fill.price),
          realized: toMoneyString(math.realized),
        });
      }

      return { status: 'SETTLED', tradeId } satisfies SettleOutcome;
    });
  } catch (err) {
    if (err instanceof LedgerInvariantError) {
      // Fail-closed: an invariant violation is audited loudly and the fill is
      // rejected — never silently absorbed into a broken balance.
      await audit(prisma, 'SETTLE_INVARIANT_REJECTED', `${fill.venue}:${fill.externalId}`, err.message, {
        symbol: fill.symbol,
        qty: toMoneyString(fill.qty),
      });
      return { status: 'REJECTED', reason: err.message };
    }
    // Anything else (serialization retries exhausted, connectivity) is
    // transient: the caller must NOT ack the message, so the reclaim sweep
    // re-settles it later.
    throw err;
  }
}

export async function listPositions() {
  return prisma.position.findMany({ orderBy: [{ symbol: 'asc' }, { venue: 'asc' }] });
}
