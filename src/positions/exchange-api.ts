import { prisma, withSerializable } from '../db.js';
import { config, isSymbol } from '../config.js';
import { redis } from '../redis.js';
import { RateLimiter } from '../market-data/rate-limiter.js';
import { priceStore } from '../market-data/price-store.js';
import { publishEvent } from '../pipeline/event-bus.js';
import { ACCOUNTS, appendWithinTx, balances as ledgerBalances } from './ledger.js';
import { getExchangeAdapter, type ExchangeOrderView, type Side } from './exchange-adapter.js';
import { incrementalInitial } from '../risk/margin.js';
import { Decimal, money, mul, normalize, toMoneyString, ZERO } from '../precision/decimal.js';
import { incMetric } from '../observability/metrics.js';

export type { ExchangeOrderView, Side } from './exchange-adapter.js';

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

const orderLimiter = new RateLimiter(redis, { capacity: 8, refillPerSec: 2 });

// Two-phase order lifecycle, phase one: estimate the marginal initial margin
// at the current mark, ring-fence it in the ledger (fail-closed: no free
// funds, no order), record the order, then hand it to the exchange adapter.
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

  const adapter = getExchangeAdapter();
  const id = await adapter.nextOrderId();
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
  await adapter.submit({
    id,
    symbol: params.symbol,
    side: params.side,
    qty: params.qty,
    liquidation: params.liquidation === true,
    suppressPublish: params.suppressPublish === true,
  });
  incMetric('risk_engine_orders_placed_total', { liquidation: String(params.liquidation === true) });
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

export async function pumpFills(): Promise<number> {
  return getExchangeAdapter().pump();
}

export async function getExchangeOrder(orderId: string): Promise<ExchangeOrderView | null> {
  return getExchangeAdapter().getOrder(orderId);
}

export async function listRecentExchangeTrades(sinceMs: number): Promise<{ id: string; fields: string[] }[]> {
  return getExchangeAdapter().listRecentTrades(sinceMs);
}

export async function devForcePublishOrderFills(orderId: string): Promise<number> {
  return getExchangeAdapter().forcePublishFills(orderId);
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
