import { prisma, withSerializable } from '../db.js';
import { audit } from '../audit.js';
import { config, isSymbol } from '../config.js';
import { redis, KEYS, STREAMS } from '../redis.js';
import { RateLimiter } from '../market-data/rate-limiter.js';
import { priceStore } from '../market-data/price-store.js';
import { publishEvent } from '../pipeline/event-bus.js';
import { ACCOUNTS, appendWithinTx, balances as ledgerBalances } from './ledger.js';
import { fillToFields, type FillMessage } from './position-tracker.js';
import { incrementalInitial } from '../risk/margin.js';
import { Decimal, money, mul, normalize, toMoneyString, ZERO } from '../precision/decimal.js';
import { hashSeed, mulberry32 } from '../util.js';

export type Side = 'BUY' | 'SELL';

export class ExchangeError extends Error {}
export class UnknownSymbolError extends ExchangeError {}
export class StaleMarketError extends ExchangeError {
  constructor(readonly symbol: string) {
    super(`market data stale for ${symbol}`);
  }
}
export class InsufficientFundsError extends ExchangeError {
  constructor(
    readonly required: Decimal,
    readonly available: Decimal,
  ) {
    super(`insufficient free funds: need ${toMoneyString(required)}, have ${toMoneyString(available)}`);
  }
}

export interface PlaceOrderParams {
  symbol: string;
  side: Side;
  qty: Decimal;
  liquidation?: boolean;
  // Test hook modelling a fill message lost in transit: the fill is written to
  // the exchange's own books but never pushed to the ingestion stream. Only
  // the reconciliation sweep can recover it.
  suppressPublish?: boolean;
}

export interface PlacedOrder {
  id: string;
  venue: string;
  symbol: string;
  side: Side;
  qty: Decimal;
  reserved: Decimal;
  status: string;
}

interface FillPlan {
  n: number;
  qty: string;
  delayMs: number;
  slipBps: number;
}

interface ExchangeBook {
  id: string;
  venue: string;
  symbol: string;
  side: Side;
  qty: string;
  liquidation: boolean;
  suppressPublish: boolean;
  createdAt: number;
  fills: FillPlan[];
  emitted: number[];
}

export interface ExchangeOrderView {
  id: string;
  status: 'OPEN' | 'FILLED';
  symbol: string;
  side: Side;
  qty: Decimal;
  fillsPublished: number;
  fillsTotal: number;
}

const orderLimiter = new RateLimiter(redis, { capacity: 8, refillPerSec: 2 });

function planFills(orderId: string, qty: Decimal, maxSlipBps: number): FillPlan[] {
  const rand = mulberry32(hashSeed(orderId));
  const count = 1 + Math.floor(rand() * 3);
  if (count === 1) {
    return [{ n: 0, qty: toMoneyString(qty), delayMs: 400 + Math.floor(rand() * 900), slipBps: Math.floor(rand() * (maxSlipBps + 1)) }];
  }
  const weights = Array.from({ length: count }, () => new Decimal(rand() + 0.25));
  const totalWeight = weights.reduce((a, b) => a.plus(b), ZERO);
  const plans: FillPlan[] = [];
  let assigned = ZERO;
  let delay = 400 + Math.floor(rand() * 900);
  for (let i = 0; i < count - 1; i++) {
    const part = normalize(qty.times(weights[i]!).dividedBy(totalWeight));
    assigned = assigned.plus(part);
    plans.push({ n: i, qty: toMoneyString(part), delayMs: delay, slipBps: Math.floor(rand() * (maxSlipBps + 1)) });
    delay += 500 + Math.floor(rand() * 1200);
  }
  // The last fill absorbs rounding so the fills sum to exactly the order qty.
  const last = normalize(qty.minus(assigned));
  if (!last.greaterThan(0)) {
    return [{ n: 0, qty: toMoneyString(qty), delayMs: delay, slipBps: Math.floor(rand() * (maxSlipBps + 1)) }];
  }
  plans.push({ n: count - 1, qty: toMoneyString(last), delayMs: delay, slipBps: Math.floor(rand() * (maxSlipBps + 1)) });
  return plans;
}

function fillPrice(mark: Decimal, side: Side, slipBps: number): Decimal {
  const slip = new Decimal(slipBps).dividedBy(10000);
  const adj = side === 'BUY' ? mark.times(ONE_PLUS(slip)) : mark.times(ONE_PLUS(slip.negated()));
  return normalize(adj);
}

function ONE_PLUS(d: Decimal): Decimal {
  return new Decimal(1).plus(d);
}

async function newOrderId(): Promise<string> {
  const n = await redis.incr(KEYS.orderSeq);
  return `SIM-${Date.now().toString(36)}-${n.toString(36)}`;
}

async function submitToExchange(params: {
  id: string;
  symbol: string;
  side: Side;
  qty: Decimal;
  liquidation: boolean;
  suppressPublish: boolean;
}): Promise<void> {
  const venue = config.defaultVenue;
  const maxSlip = params.liquidation ? config.liquidationSlippageBps : config.slippageBps;
  const book: ExchangeBook = {
    id: params.id,
    venue,
    symbol: params.symbol,
    side: params.side,
    qty: toMoneyString(params.qty),
    liquidation: params.liquidation,
    suppressPublish: params.suppressPublish,
    createdAt: Date.now(),
    fills: planFills(params.id, params.qty, maxSlip),
    emitted: [],
  };
  // The simulated exchange keeps its own books in Redis rather than in memory:
  // any process can reconstruct its state, which is what makes the
  // reconciliation sweep against our Postgres books meaningful.
  await redis.hset(KEYS.orderBook(params.id), 'book', JSON.stringify(book));
  await redis.zadd(KEYS.openOrders, String(Date.now()), params.id);
}

export async function getExchangeOrder(orderId: string): Promise<ExchangeOrderView | null> {
  const raw = await redis.hget(KEYS.orderBook(orderId), 'book');
  if (!raw) return null;
  const book = JSON.parse(raw) as ExchangeBook;
  return {
    id: book.id,
    status: book.emitted.length >= book.fills.length ? 'FILLED' : 'OPEN',
    symbol: book.symbol,
    side: book.side,
    qty: new Decimal(book.qty),
    fillsPublished: book.emitted.length,
    fillsTotal: book.fills.length,
  };
}

// Publishes every fill of every open order whose scheduled delay has elapsed.
// Deterministic external trade ids (orderId:fillIndex) make double-publishing
// harmless: ingestion deduplicates on (venue, external_id).
export async function pumpFills(): Promise<number> {
  const ids = await redis.zrange(KEYS.openOrders, 0, -1);
  const now = Date.now();
  let published = 0;
  for (const id of ids) {
    const raw = await redis.hget(KEYS.orderBook(id), 'book');
    if (!raw) {
      await redis.zrem(KEYS.openOrders, id);
      continue;
    }
    const book = JSON.parse(raw) as ExchangeBook;
    const due = book.fills.filter(f => !book.emitted.includes(f.n) && now - book.createdAt >= f.delayMs);
    if (due.length === 0) continue;
    const mark = await priceStore.getMark(book.symbol);
    // The simulator prices fills off the live mark; a stale market delays
    // fills rather than inventing prices from old data.
    if (!mark || mark.stale) continue;
    for (const f of due) {
      const qty = new Decimal(f.qty);
      const price = fillPrice(mark.price, book.side, f.slipBps);
      const fee = mul(mul(qty, price), new Decimal(config.feeBps).dividedBy(10000));
      const fill: FillMessage = {
        orderId: book.id,
        externalId: `${book.id}:${f.n}`,
        venue: book.venue,
        symbol: book.symbol,
        side: book.side,
        qty,
        price,
        fee,
        ts: now,
        liquidation: book.liquidation,
        source: 'stream',
      };
      await redis.xadd(STREAMS.exchangeTrades, 'MAXLEN', '~', '10000', '*', ...fillToFields(fill));
      if (!book.suppressPublish) {
        await redis.xadd(STREAMS.fills, 'MAXLEN', '~', String(config.streamMaxLen), '*', ...fillToFields(fill));
      }
      book.emitted.push(f.n);
      published++;
    }
    await redis.hset(KEYS.orderBook(id), 'book', JSON.stringify(book));
    if (book.emitted.length >= book.fills.length) {
      await redis.zrem(KEYS.openOrders, id);
      await redis.zadd(KEYS.doneOrders, String(now), id);
    }
  }
  await redis.zremrangebyscore(KEYS.doneOrders, '-inf', String(now - 3_600_000));
  return published;
}

export async function listRecentExchangeTrades(sinceMs: number): Promise<{ id: string; fields: string[] }[]> {
  const entries = (await redis.xrange(STREAMS.exchangeTrades, String(sinceMs), '+')) as [string, string[]][];
  return entries.map(([id, fields]) => ({ id, fields }));
}

// Two-phase order lifecycle, phase one: estimate the marginal initial margin
// at the current mark, ring-fence it in the ledger (fail-closed: no free
// funds, no order), record the order, then hand it to the simulated exchange.
// The reservation is released pro-rata as fills settle (phase two).
export async function placeOrder(params: PlaceOrderParams): Promise<PlacedOrder> {
  const venue = config.defaultVenue;
  if (!isSymbol(params.symbol)) throw new UnknownSymbolError(params.symbol);
  if (!params.qty.greaterThan(0)) throw new ExchangeError('qty must be positive');
  await orderLimiter.limit('orders');

  const mark = await priceStore.getMark(params.symbol);
  if (!mark) throw new ExchangeError(`no mark available for ${params.symbol}`);
  // Fail-closed for opening new risk: refuse to price an order off a stale
  // mark instead of guessing where the market might be.
  if (mark.stale) throw new StaleMarketError(params.symbol);

  const position = await prisma.position.findUnique({ where: { symbol_venue: { symbol: params.symbol, venue } } });
  const oldQty = position ? money(position.qty.toString()) : ZERO;
  const signedNew = params.side === 'BUY' ? oldQty.plus(params.qty) : oldQty.minus(params.qty);
  const oldNotional = mul(oldQty.abs(), mark.price);
  const newNotional = mul(signedNew.abs(), mark.price);
  const estInit = params.liquidation ? ZERO : incrementalInitial(oldNotional, newNotional);
  const estFee = mul(mul(params.qty, mark.price), new Decimal(config.feeBps).dividedBy(10000));
  const reserve = normalize(estInit.plus(estFee));

  const bals = await ledgerBalances();
  const wallet = bals[ACCOUNTS.WALLET] ?? ZERO;
  if (!params.liquidation && wallet.lessThan(reserve)) throw new InsufficientFundsError(reserve, wallet);

  const id = await newOrderId();
  await withSerializable(async tx => {
    const entries =
      params.liquidation || !reserve.greaterThan(0)
        ? []
        : [
            {
              account: ACCOUNTS.MARGIN,
              kind: 'MARGIN_RESERVE',
              amount: reserve,
              idempotencyKey: `rs:${id}:m`,
              orderId: id,
            },
            {
              account: ACCOUNTS.WALLET,
              kind: 'MARGIN_RESERVE',
              amount: reserve.negated(),
              idempotencyKey: `rs:${id}`,
              orderId: id,
              note: 'initial margin + fee reservation',
            },
          ];
    await appendWithinTx(tx, entries);
    await tx.order.create({
      data: {
        id,
        venue,
        symbol: params.symbol,
        side: params.side,
        qty: params.qty,
        status: 'MARGIN_RESERVED',
        // Liquidation orders post no reservation (they release risk); the fee
        // is paid from the wallet at settlement, so the row records zero.
        marginReserved: params.liquidation ? new Decimal(0) : reserve,
        isLiquidation: params.liquidation === true,
        updatedAt: new Date(),
      },
    });
  });
  // A crash between the reservation transaction and this submit leaves the
  // order MARGIN_RESERVED with no exchange book entry; the reconciliation
  // sweep releases the reservation after the TTL. That is the recoverable
  // half of the two-phase lifecycle.
  await submitToExchange({
    id,
    symbol: params.symbol,
    side: params.side,
    qty: params.qty,
    liquidation: params.liquidation === true,
    suppressPublish: params.suppressPublish === true,
  });
  await publishEvent('order_placed', {
    orderId: id,
    symbol: params.symbol,
    side: params.side,
    qty: toMoneyString(params.qty),
    reserved: toMoneyString(reserve),
    liquidation: params.liquidation === true,
  });
  return { id, venue, symbol: params.symbol, side: params.side, qty: params.qty, reserved: reserve, status: 'MARGIN_RESERVED' };
}

export async function recentPlacedOrders(limit = 50) {
  return prisma.order.findMany({ orderBy: { createdAt: 'desc' }, take: limit });
}

export async function getOrderWithTrades(orderId: string) {
  const order = await prisma.order.findUnique({ where: { id: orderId } });
  if (!order) return null;
  const trades = await prisma.trade.findMany({ where: { orderId }, orderBy: { id: 'asc' } });
  return { order, trades };
}

export async function devForcePublishOrderFills(orderId: string): Promise<number> {
  // Test hook: force-publish all fills of a suppressed order straight to the
  // exchange statement without waiting for delays (used by the simulator to
  // stage the lost-fill scenario deterministically).
  const raw = await redis.hget(KEYS.orderBook(orderId), 'book');
  if (!raw) return 0;
  const book = JSON.parse(raw) as ExchangeBook;
  book.createdAt = Math.min(book.createdAt, Date.now() - 10_000);
  for (const f of book.fills) f.delayMs = Math.min(f.delayMs, 100);
  await redis.hset(KEYS.orderBook(orderId), 'book', JSON.stringify(book));
  return book.fills.length;
}
