import { config } from '../config.js';
import { Decimal, normalize, ZERO } from '../precision/decimal.js';

export interface MarginTier {
  upToUsd: string | null;
  initialRate: string;
  maintenanceRate: string;
}

// Notional-laddered margin, mirroring how real venues de-leverage size: the
// first 10k of notional is cheap, size beyond 1M pays 25% initial / 10%
// maintenance. Rates are strings on purpose — they are exact decimals, never
// binary floats.
const DEFAULT_TIERS: readonly MarginTier[] = [
  { upToUsd: '10000', initialRate: '0.02', maintenanceRate: '0.01' },
  { upToUsd: '50000', initialRate: '0.04', maintenanceRate: '0.02' },
  { upToUsd: '200000', initialRate: '0.08', maintenanceRate: '0.04' },
  { upToUsd: '1000000', initialRate: '0.12', maintenanceRate: '0.05' },
  { upToUsd: null, initialRate: '0.25', maintenanceRate: '0.10' },
] as const;

// Risk parameters are operational config: they can change without a code
// deploy (MARGIN_TIERS as JSON), but an invalid override must stop the
// process at startup rather than let it trade on silently wrong margin.
function loadTiers(): readonly MarginTier[] {
  const json = config.marginTiersJson;
  if (!json) return DEFAULT_TIERS;

  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch {
    throw new Error('MARGIN_TIERS is not valid JSON');
  }
  if (!Array.isArray(parsed) || parsed.length === 0) {
    throw new Error('MARGIN_TIERS must be a non-empty array of tiers');
  }
  const tiers: MarginTier[] = [];
  for (const raw of parsed) {
    const t = raw as Partial<MarginTier>;
    if (typeof t.initialRate !== 'string' || typeof t.maintenanceRate !== 'string') {
      throw new Error('MARGIN_TIERS rates must be decimal strings (e.g. "0.02")');
    }
    if (t.upToUsd !== null && typeof t.upToUsd !== 'string') {
      throw new Error('MARGIN_TIERS upToUsd must be a decimal string or null (last tier only)');
    }
    const init = new Decimal(t.initialRate);
    const maint = new Decimal(t.maintenanceRate);
    for (const [name, rate] of [['initialRate', init], ['maintenanceRate', maint]] as const) {
      if (rate.lessThanOrEqualTo(0) || rate.greaterThanOrEqualTo(1)) {
        throw new Error(`MARGIN_TIERS ${name} must be in (0, 1)`);
      }
    }
    if (maint.greaterThanOrEqualTo(init)) {
      throw new Error('MARGIN_TIERS maintenanceRate must be strictly below initialRate');
    }
    if (t.upToUsd !== null && new Decimal(t.upToUsd).lessThanOrEqualTo(0)) {
      throw new Error('MARGIN_TIERS upToUsd must be positive');
    }
    tiers.push({ upToUsd: t.upToUsd ?? null, initialRate: t.initialRate, maintenanceRate: t.maintenanceRate });
  }
  for (let i = 0; i < tiers.length - 1; i++) {
    const a = tiers[i]!.upToUsd;
    if (a === null) throw new Error('only the last MARGIN_TIERS entry may have a null bound');
    const b = tiers[i + 1]!.upToUsd;
    if (b !== null && new Decimal(a).greaterThanOrEqualTo(new Decimal(b))) {
      throw new Error('MARGIN_TIERS bounds must be strictly ascending');
    }
  }
  if (tiers[tiers.length - 1]!.upToUsd !== null) {
    throw new Error('the last MARGIN_TIERS entry must have a null bound');
  }
  return tiers;
}

export const TIERS: readonly MarginTier[] = loadTiers();

function tieredRequirement(notional: Decimal, rateOf: (t: MarginTier) => string): Decimal {
  let remaining = notional;
  let floorUsd = ZERO;
  let requirement = ZERO;
  for (const tier of TIERS) {
    if (!remaining.greaterThan(0)) break;
    const cap = tier.upToUsd === null ? null : new Decimal(tier.upToUsd);
    const slice = cap === null ? remaining : Decimal.min(remaining, cap.minus(floorUsd));
    if (slice.greaterThan(0)) {
      requirement = requirement.plus(slice.times(new Decimal(rateOf(tier))));
      remaining = remaining.minus(slice);
    }
    if (cap) floorUsd = cap;
  }
  return normalize(requirement);
}

export function initialMargin(notional: Decimal): Decimal {
  return tieredRequirement(notional, t => t.initialRate);
}

export function maintenanceMargin(notional: Decimal): Decimal {
  return tieredRequirement(notional, t => t.maintenanceRate);
}

// Marginal cost of growing notional from old to new. Used at order time (the
// reservation estimate) and again at settlement (the exact posting), so the
// two phases agree by construction.
export function incrementalInitial(oldNotional: Decimal, newNotional: Decimal): Decimal {
  if (newNotional.lessThanOrEqualTo(oldNotional)) return ZERO;
  return normalize(initialMargin(newNotional).minus(initialMargin(oldNotional)));
}

export function marginRatio(equity: Decimal, maintenanceRequired: Decimal): Decimal | null {
  if (maintenanceRequired.isZero()) return null;
  return normalize(equity.dividedBy(maintenanceRequired));
}

export interface TierSlice {
  upToUsd: string;
  sliceUsd: Decimal;
  initialRate: string;
  maintenanceRate: string;
}

export function tierBreakdown(notional: Decimal): TierSlice[] {
  const slices: TierSlice[] = [];
  let remaining = notional;
  let floorUsd = ZERO;
  for (const tier of TIERS) {
    if (!remaining.greaterThan(0)) break;
    const cap = tier.upToUsd === null ? null : new Decimal(tier.upToUsd);
    const slice = cap === null ? remaining : Decimal.min(remaining, cap.minus(floorUsd));
    if (slice.greaterThan(0)) {
      slices.push({
        upToUsd: tier.upToUsd ?? '∞',
        sliceUsd: slice,
        initialRate: tier.initialRate,
        maintenanceRate: tier.maintenanceRate,
      });
      remaining = remaining.minus(slice);
    }
    if (cap) floorUsd = cap;
  }
  return slices;
}
