import type { FastifyInstance } from 'fastify';
import { portfolioState, type PositionView } from '../risk/mark-to-market.js';
import { computeExposure } from '../risk/exposure.js';
import { ExchangeError, getOrderWithTrades, placeOrder, recentPlacedOrders } from '../positions/exchange-api.js';
import { Decimal, toMoneyString } from '../precision/decimal.js';

function viewPosition(p: PositionView) {
  return {
    symbol: p.symbol,
    venue: p.venue,
    qty: toMoneyString(p.qty),
    avgEntryPrice: toMoneyString(p.avgEntryPrice),
    mark: p.mark ? toMoneyString(p.mark) : null,
    markSource: p.markSource,
    markStale: p.markStale,
    markAgeMs: p.markAgeMs,
    notional: p.notional ? toMoneyString(p.notional) : null,
    unrealizedPnl: p.unrealizedPnl ? toMoneyString(p.unrealizedPnl) : null,
    realizedPnl: toMoneyString(p.realizedPnl),
    marginPosted: toMoneyString(p.marginPosted),
    maintenanceRequired: p.maintenanceRequired ? toMoneyString(p.maintenanceRequired) : null,
    version: p.version,
  };
}

export async function registerPortfolioRoutes(app: FastifyInstance): Promise<void> {
  app.get('/portfolio', async () => {
    const state = await portfolioState();
    const exposure = computeExposure(state.positions);
    return {
      asOf: new Date(state.asOf).toISOString(),
      marksFresh: state.marksFresh,
      staleSymbols: state.staleSymbols,
      wallet: toMoneyString(state.wallet),
      marginPosted: toMoneyString(state.marginPosted),
      deficitLoan: toMoneyString(state.loan),
      equity: toMoneyString(state.equity),
      unrealizedPnl: toMoneyString(state.unrealizedPnl),
      realizedPnl: toMoneyString(state.realizedPnl),
      grossExposure: toMoneyString(exposure.gross),
      netExposure: toMoneyString(exposure.net),
      maintenanceRequired: toMoneyString(state.maintenanceRequired),
      marginRatio: state.marginRatio ? state.marginRatio.toFixed(4) : null,
      positions: state.positions.filter(p => !p.qty.isZero()).map(viewPosition),
    };
  });

  app.get('/positions', async () => {
    const state = await portfolioState();
    return {
      asOf: new Date(state.asOf).toISOString(),
      positions: state.positions.map(viewPosition),
    };
  });

  app.post('/orders', async (request, reply) => {
    const body = (request.body ?? {}) as { symbol?: unknown; side?: unknown; qty?: unknown };
    const symbol = String(body.symbol ?? '');
    const side = body.side === 'BUY' ? 'BUY' : body.side === 'SELL' ? 'SELL' : null;
    if (!side) throw new ExchangeError("side must be 'BUY' or 'SELL'");
    // qty as a JSON string is preferred (exact); a JSON number is converted
    // once via its shortest decimal string — the only float-adjacent spot in
    // the system, confined to the API boundary.
    if (typeof body.qty !== 'string' && typeof body.qty !== 'number') {
      throw new ExchangeError('qty must be a decimal string or number');
    }
    let qty: Decimal;
    try {
      qty = new Decimal(String(body.qty));
    } catch {
      throw new ExchangeError('qty is not a valid decimal');
    }
    if (!qty.isFinite() || !qty.greaterThan(0)) throw new ExchangeError('qty must be a positive finite decimal');

    const placed = await placeOrder({ symbol, side, qty });
    return await reply.code(202).send({
      orderId: placed.id,
      status: placed.status,
      venue: placed.venue,
      symbol: placed.symbol,
      side: placed.side,
      qty: toMoneyString(placed.qty),
      marginReserved: toMoneyString(placed.reserved),
    });
  });

  app.get('/orders', async () => {
    const orders = await recentPlacedOrders(50);
    return {
      orders: orders.map(o => ({
        id: o.id,
        symbol: o.symbol,
        side: o.side,
        qty: toMoneyString(o.qty),
        status: o.status,
        filledQty: toMoneyString(o.filledQty),
        avgFillPrice: toMoneyString(o.avgFillPrice),
        marginReserved: toMoneyString(o.marginReserved),
        isLiquidation: o.isLiquidation,
        createdAt: o.createdAt.toISOString(),
      })),
    };
  });

  app.get('/orders/:id', async (request, reply) => {
    const { id } = request.params as { id: string };
    const found = await getOrderWithTrades(id);
    if (!found) return await reply.code(404).send({ error: 'order not found' });
    return {
      order: {
        id: found.order.id,
        symbol: found.order.symbol,
        side: found.order.side,
        qty: toMoneyString(found.order.qty),
        status: found.order.status,
        filledQty: toMoneyString(found.order.filledQty),
        avgFillPrice: toMoneyString(found.order.avgFillPrice),
        marginReserved: toMoneyString(found.order.marginReserved),
        isLiquidation: found.order.isLiquidation,
        createdAt: found.order.createdAt.toISOString(),
      },
      trades: found.trades.map(t => ({
        externalId: t.externalId,
        symbol: t.symbol,
        side: t.side,
        qty: toMoneyString(t.qty),
        price: toMoneyString(t.price),
        fee: toMoneyString(t.fee),
        status: t.status,
        source: t.source,
        isLiquidation: t.isLiquidation,
        settledAt: t.settledAt?.toISOString() ?? null,
      })),
    };
  });
}
