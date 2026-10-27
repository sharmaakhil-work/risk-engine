import { redis, KEYS, tryLock } from '../redis.js';
import { publishEvent } from '../pipeline/event-bus.js';
import { audit } from '../audit.js';
import { prisma } from '../db.js';
import { config } from '../config.js';
import type { PortfolioState } from './mark-to-market.js';
import { placeOrder } from '../positions/exchange-api.js';
import { Decimal, div, normalize, toMoneyString, ONE } from '../precision/decimal.js';
import { incMetric } from '../observability/metrics.js';
import { uid } from '../util.js';

export type RiskAction = 'NONE' | 'OK' | 'WARNED' | 'PAUSED_STALE' | 'LIQUIDATING' | 'IN_DEFICIT';

const MARGIN_STATE_KEY = 'risk:margin-state';
const FEED_STATE_KEY = 'risk:feed-state';

async function transitionMarginState(next: 'OK' | 'WARNING'): Promise<boolean> {
  const prev = await redis.get(MARGIN_STATE_KEY);
  if (prev === next) return false;
  await redis.set(MARGIN_STATE_KEY, next);
  return true;
}

interface ReductionStep {
  symbol: string;
  side: 'BUY' | 'SELL';
  qty: Decimal;
}

// Greedy reduction plan: cut the largest-maintenance positions first until the
// predicted requirement fits under equity / target ratio. Liquidation fills
// price with extra slippage, so the next evaluation re-checks with fresh
// marks and submits another round if needed — a fixed point by iteration
// rather than a fragile one-shot computation.
function planReduction(state: PortfolioState, target: Decimal): ReductionStep[] {
  const marked = state.positions.filter(
    p => !p.qty.isZero() && p.notional !== null && p.maintenanceRequired !== null && p.mark !== null,
  );
  const sorted = [...marked].sort((a, b) => (b.maintenanceRequired!).comparedTo(a.maintenanceRequired!));
  let remaining = state.maintenanceRequired;
  const steps: ReductionStep[] = [];
  for (const p of sorted) {
    if (remaining.lessThanOrEqualTo(target)) break;
    const m = p.maintenanceRequired!;
    const fraction = Decimal.min(ONE, remaining.minus(target).dividedBy(m));
    const qty = normalize(p.qty.abs().times(fraction));
    if (qty.greaterThan(0)) {
      steps.push({ symbol: p.symbol, side: p.qty.greaterThan(0) ? 'SELL' : 'BUY', qty });
      remaining = normalize(remaining.minus(m.times(fraction)));
    }
  }
  return steps;
}

export async function evaluateAndAct(
  state: PortfolioState,
): Promise<{ action: RiskAction; detail: Record<string, unknown> }> {
  if (state.positions.every(p => p.qty.isZero())) {
    return { action: 'NONE', detail: { note: 'no open positions' } };
  }

  // Fail-open on stale data: liquidating a portfolio priced off a stale mark
  // risks destroying a healthy book on a false breach — an irreversible
  // sell-low. Pause and shout instead. The breach path below is the
  // fail-closed counterpart: with trustworthy data, a maintenance breach
  // forces liquidation without waiting for a human.
  if (!state.marksFresh) {
    const prev = await redis.get(FEED_STATE_KEY);
    if (prev !== 'stale') {
      await redis.set(FEED_STATE_KEY, 'stale');
      await publishEvent('price_stale', { staleSymbols: state.staleSymbols });
      await audit(prisma, 'PRICE_FEED_STALE', 'market-data', `marks stale: ${state.staleSymbols.join(', ')}`, {
        staleSymbols: state.staleSymbols,
      });
    }
    return { action: 'PAUSED_STALE', detail: { staleSymbols: state.staleSymbols } };
  }
  if ((await redis.get(FEED_STATE_KEY)) === 'stale') {
    await redis.set(FEED_STATE_KEY, 'fresh');
    await publishEvent('price_fresh', { note: 'marks recovered' });
  }

  const ratio = state.marginRatio;
  if (ratio === null) return { action: 'OK', detail: { note: 'no maintenance requirement' } };

  if (ratio.greaterThanOrEqualTo(ONE)) {
    if (ratio.greaterThanOrEqualTo(config.liquidationTargetRatio) && (await transitionMarginState('OK'))) {
      await publishEvent('margin_recovered', { ratio: ratio.toFixed(4) });
    }
    return { action: 'OK', detail: { ratio: ratio.toFixed(4) } };
  }

  // ratio < 1: equity is below the maintenance requirement — breached.
  const entered = await transitionMarginState('WARNING');
  const warnDetail = {
    ratio: ratio.toFixed(4),
    equity: toMoneyString(state.equity),
    maintenanceRequired: toMoneyString(state.maintenanceRequired),
  };
  if (entered || ratio.lessThan(config.marginWarningRatio)) {
    const echo = await redis.set('risk:warn-echo', '1', 'EX', 30, 'NX');
    if (entered || echo) {
      await publishEvent('margin_warning', warnDetail);
      incMetric('risk_engine_margin_warnings_total');
    }
  }

  if (state.equity.isNegative()) {
    // Everything has been liquidated and equity is still negative: the
    // deficit loan absorbed the gap. Production systems hand this to an
    // insurance fund and auto-deleveraging; here it is surfaced loudly.
    await publishEvent('bankruptcy', { ...warnDetail, note: 'equity negative after liquidation' });
    await audit(prisma, 'BANKRUPTCY', 'portfolio', 'equity negative after liquidation — deficit loan funded', warnDetail);
    return { action: 'IN_DEFICIT', detail: warnDetail };
  }

  // Rounds are serialized two ways: no new round while a previous round's
  // orders are still in flight (their fills must settle before re-planning,
  // otherwise the same breach gets sold twice), and a TTL lock that is
  // deliberately NOT released early — a crashed liquidator's lock simply
  // expires and the next worker takes over.
  const inFlight = await prisma.order.count({
    where: { isLiquidation: true, status: { in: ['NEW', 'MARGIN_RESERVED', 'PARTIALLY_SETTLED'] } },
  });
  if (inFlight > 0) {
    return { action: 'LIQUIDATING', detail: { note: `${inFlight} liquidation order(s) still in flight` } };
  }

  const token = uid('liq');
  const acquired = await tryLock(KEYS.liquidationLock, token, 10_000);
  if (!acquired) {
    return { action: 'LIQUIDATING', detail: { note: 'liquidation lock held by another worker' } };
  }
  // The lock is intentionally NOT released after the round: its 10s TTL is
  // the round cooldown. Early release let a second round fire before the
  // first round's fills settled (observed in testing as a double-sell through
  // zero into a short). A crashed liquidator's lock simply expires.
  const target = div(state.equity, new Decimal(config.liquidationTargetRatio));
  const steps = planReduction(state, target);
  for (const step of steps) {
    await placeOrder({ symbol: step.symbol, side: step.side, qty: step.qty, liquidation: true });
  }
  const detail = {
    ...warnDetail,
    orders: steps.map(s => ({ symbol: s.symbol, side: s.side, qty: toMoneyString(s.qty) })),
  };
  await publishEvent('liquidation', detail);
  incMetric('risk_engine_liquidation_rounds_total');
  await audit(
    prisma,
    'LIQUIDATION_ROUND',
    'portfolio',
    `${steps.length} liquidation order(s) submitted to restore ratio >= ${config.liquidationTargetRatio}`,
    detail,
  );
  return { action: 'LIQUIDATING', detail };
}
