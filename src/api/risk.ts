import type { FastifyInstance } from 'fastify';
import { config } from '../config.js';
import { prisma } from '../db.js';
import { portfolioState } from '../risk/mark-to-market.js';
import { computeExposure } from '../risk/exposure.js';
import { computeHistoricalVar } from '../risk/var.js';
import { TIERS, tierBreakdown } from '../risk/margin.js';
import { recentEvents } from '../pipeline/event-bus.js';
import { toMoneyString } from '../precision/decimal.js';

export async function registerRiskRoutes(app: FastifyInstance): Promise<void> {
  app.get('/risk/margin', async () => {
    const state = await portfolioState();
    const held = state.positions.filter(p => !p.qty.isZero());
    return {
      asOf: new Date(state.asOf).toISOString(),
      status: state.marksFresh ? 'OK' : 'STALE_PRICES',
      equity: toMoneyString(state.equity),
      wallet: toMoneyString(state.wallet),
      marginPosted: toMoneyString(state.marginPosted),
      deficitLoan: toMoneyString(state.loan),
      maintenanceRequired: toMoneyString(state.maintenanceRequired),
      marginRatio: state.marginRatio ? state.marginRatio.toFixed(4) : null,
      thresholds: {
        warningRatio: config.marginWarningRatio,
        liquidationTargetRatio: config.liquidationTargetRatio,
        staleAfterMs: config.staleAfterMs,
      },
      tiers: TIERS,
      perPosition: held.map(p => ({
        symbol: p.symbol,
        venue: p.venue,
        qty: toMoneyString(p.qty),
        notional: p.notional ? toMoneyString(p.notional) : null,
        maintenanceRequired: p.maintenanceRequired ? toMoneyString(p.maintenanceRequired) : null,
        marginPosted: toMoneyString(p.marginPosted),
        tierSlices:
          p.notional !== null
            ? tierBreakdown(p.notional).map(s => ({
                upToUsd: s.upToUsd,
                sliceUsd: toMoneyString(s.sliceUsd),
                initialRate: s.initialRate,
                maintenanceRate: s.maintenanceRate,
              }))
            : [],
      })),
    };
  });

  app.get('/risk/var', async () => {
    const state = await portfolioState();
    const res = await computeHistoricalVar(prisma, state.positions);
    return {
      ok: res.ok,
      reason: res.reason,
      days: res.days,
      windowDays: config.varWindowDays,
      var95: res.var95 ? toMoneyString(res.var95) : null,
      var99: res.var99 ? toMoneyString(res.var99) : null,
      es95: res.es95 ? toMoneyString(res.es95) : null,
      perSymbol: res.perSymbol.map(s => ({
        symbol: s.symbol,
        days: s.days,
        var95: toMoneyString(s.var95),
        var99: toMoneyString(s.var99),
        es95: toMoneyString(s.es95),
      })),
      methodology: res.methodology,
      asOf: new Date(res.asOf).toISOString(),
    };
  });

  app.get('/risk/exposure', async () => {
    const state = await portfolioState();
    const exposure = computeExposure(state.positions);
    return {
      asOf: new Date(state.asOf).toISOString(),
      gross: toMoneyString(exposure.gross),
      net: toMoneyString(exposure.net),
      maxConcentrationSymbol: exposure.maxConcentrationSymbol,
      unmarkedSymbols: exposure.unmarkedSymbols,
      perAsset: exposure.perAsset.map(a => ({
        symbol: a.symbol,
        qty: toMoneyString(a.qty),
        netNotional: toMoneyString(a.netNotional),
        grossNotional: toMoneyString(a.grossNotional),
        maintenanceRequired: toMoneyString(a.maintenanceRequired),
        weightPct: a.weightPct,
      })),
      perVenue: exposure.perVenue.map(v => ({
        venue: v.venue,
        netNotional: toMoneyString(v.netNotional),
        grossNotional: toMoneyString(v.grossNotional),
      })),
    };
  });

  app.get('/risk/events', async request => {
    const q = request.query as { limit?: string };
    const limit = Math.min(200, Math.max(1, Number(q.limit ?? 50) || 50));
    const events = await recentEvents(limit);
    return {
      events: events.map(e => ({
        id: e.id,
        at: new Date(e.ts).toISOString(),
        kind: e.kind,
        payload: e.payload,
      })),
    };
  });
}
