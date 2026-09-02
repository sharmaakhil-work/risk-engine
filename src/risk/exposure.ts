import type { PositionView } from './mark-to-market.js';
import { Decimal, mul, normalize, ZERO } from '../precision/decimal.js';

export interface AssetExposure {
  symbol: string;
  qty: Decimal;
  netNotional: Decimal;
  grossNotional: Decimal;
  maintenanceRequired: Decimal;
  weightPct: string;
}

export interface VenueExposure {
  venue: string;
  netNotional: Decimal;
  grossNotional: Decimal;
}

export interface ExposureReport {
  perAsset: AssetExposure[];
  perVenue: VenueExposure[];
  gross: Decimal;
  net: Decimal;
  maxConcentrationSymbol: string | null;
  unmarkedSymbols: string[];
}

export function computeExposure(positions: PositionView[]): ExposureReport {
  const byAsset = new Map<string, { qty: Decimal; net: Decimal; gross: Decimal; maint: Decimal }>();
  const byVenue = new Map<string, { net: Decimal; gross: Decimal }>();
  const unmarkedSymbols: string[] = [];
  let grossTotal = ZERO;
  let netTotal = ZERO;

  for (const p of positions) {
    if (!p.notional || !p.maintenanceRequired || !p.mark) {
      if (!p.qty.isZero()) unmarkedSymbols.push(p.symbol);
      continue;
    }
    const signedNotional = mul(p.qty, p.mark);
    const asset = byAsset.get(p.symbol) ?? { qty: ZERO, net: ZERO, gross: ZERO, maint: ZERO };
    asset.qty = asset.qty.plus(p.qty);
    asset.net = asset.net.plus(signedNotional);
    asset.gross = asset.gross.plus(p.notional);
    asset.maint = asset.maint.plus(p.maintenanceRequired);
    byAsset.set(p.symbol, asset);

    const venue = byVenue.get(p.venue) ?? { net: ZERO, gross: ZERO };
    venue.net = venue.net.plus(signedNotional);
    venue.gross = venue.gross.plus(p.notional);
    byVenue.set(p.venue, venue);

    grossTotal = grossTotal.plus(p.notional);
    netTotal = netTotal.plus(signedNotional);
  }

  const perAsset: AssetExposure[] = [...byAsset.entries()].map(([symbol, a]) => ({
    symbol,
    qty: a.qty,
    netNotional: a.net,
    grossNotional: a.gross,
    maintenanceRequired: a.maint,
    weightPct: grossTotal.isZero() ? '0' : a.gross.dividedBy(grossTotal).times(100).toFixed(2),
  }));
  const perVenue: VenueExposure[] = [...byVenue.entries()].map(([venue, v]) => ({
    venue,
    netNotional: v.net,
    grossNotional: v.gross,
  }));
  const maxConcentrationSymbol =
    perAsset.length === 0
      ? null
      : perAsset.reduce((best, cur) => (cur.grossNotional.greaterThan(best.grossNotional) ? cur : best)).symbol;

  return {
    perAsset,
    perVenue,
    gross: normalize(grossTotal),
    net: normalize(netTotal),
    maxConcentrationSymbol,
    unmarkedSymbols,
  };
}
