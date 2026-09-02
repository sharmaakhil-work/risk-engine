import { prisma } from '../db.js';
import { priceStore } from '../market-data/price-store.js';
import { maintenanceMargin, marginRatio } from './margin.js';
import { ACCOUNTS, balances } from '../positions/ledger.js';
import { Decimal, fromDb, mul, normalize, ZERO } from '../precision/decimal.js';

export interface PositionView {
  symbol: string;
  venue: string;
  version: number;
  qty: Decimal;
  avgEntryPrice: Decimal;
  realizedPnl: Decimal;
  marginPosted: Decimal;
  mark: Decimal | null;
  markSource: string | null;
  markStale: boolean | null;
  markAgeMs: number | null;
  notional: Decimal | null;
  unrealizedPnl: Decimal | null;
  maintenanceRequired: Decimal | null;
}

export interface PortfolioState {
  asOf: number;
  positions: PositionView[];
  staleSymbols: string[];
  marksFresh: boolean;
  wallet: Decimal;
  marginPosted: Decimal;
  loan: Decimal;
  unrealizedPnl: Decimal;
  realizedPnl: Decimal;
  equity: Decimal;
  maintenanceRequired: Decimal;
  marginRatio: Decimal | null;
  grossExposure: Decimal;
  netExposure: Decimal;
}

// equity = wallet + posted margin + deficit loan + Σ unrealized. Realizing
// PnL only moves value between unrealized and the margin account, so equity
// is invariant under settlement and only moves with marks, fees and loan use.
export async function portfolioState(): Promise<PortfolioState> {
  const [positionRows, markMap, bals] = await Promise.all([
    prisma.position.findMany({ orderBy: [{ symbol: 'asc' }, { venue: 'asc' }] }),
    priceStore.getMarks(),
    balances(),
  ]);

  const positions: PositionView[] = [];
  const staleSymbols: string[] = [];
  let unrealizedTotal = ZERO;
  let realizedTotal = ZERO;
  let maintTotal = ZERO;
  let gross = ZERO;
  let net = ZERO;
  let postedTotal = ZERO;

  for (const p of positionRows) {
    const qty = fromDb(p.qty);
    const avgEntryPrice = fromDb(p.avgEntryPrice);
    const realizedPnl = fromDb(p.realizedPnl);
    const marginPosted = fromDb(p.marginPosted);
    const mark = markMap.get(p.symbol) ?? null;
    if (!mark || mark.stale) staleSymbols.push(p.symbol);
    const notional = mark ? mul(qty.abs(), mark.price) : null;
    const unrealized = mark ? normalize(qty.times(mark.price.minus(avgEntryPrice))) : null;
    const maint = notional !== null ? maintenanceMargin(notional) : null;
    positions.push({
      symbol: p.symbol,
      venue: p.venue,
      version: p.version,
      qty,
      avgEntryPrice,
      realizedPnl,
      marginPosted,
      mark: mark?.price ?? null,
      markSource: mark?.source ?? null,
      markStale: mark?.stale ?? null,
      markAgeMs: mark?.ageMs ?? null,
      notional,
      unrealizedPnl: unrealized,
      maintenanceRequired: maint,
    });
    realizedTotal = realizedTotal.plus(realizedPnl);
    postedTotal = postedTotal.plus(marginPosted);
    if (notional !== null && unrealized !== null && mark) {
      unrealizedTotal = unrealizedTotal.plus(unrealized);
      gross = gross.plus(notional);
      net = net.plus(mul(qty, mark.price));
      if (maint !== null) maintTotal = maintTotal.plus(maint);
    }
  }

  const wallet = bals[ACCOUNTS.WALLET] ?? ZERO;
  const loan = bals[ACCOUNTS.LOAN] ?? ZERO;
  const equity = normalize(wallet.plus(postedTotal).plus(loan).plus(unrealizedTotal));
  const maintenanceRequired = normalize(maintTotal);

  return {
    asOf: Date.now(),
    positions,
    staleSymbols,
    marksFresh: staleSymbols.length === 0,
    wallet,
    marginPosted: postedTotal,
    loan,
    unrealizedPnl: normalize(unrealizedTotal),
    realizedPnl: normalize(realizedTotal),
    equity,
    maintenanceRequired,
    marginRatio: marginRatio(equity, maintenanceRequired),
    grossExposure: normalize(gross),
    netExposure: normalize(net),
  };
}
