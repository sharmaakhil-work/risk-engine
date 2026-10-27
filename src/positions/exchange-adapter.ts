import { config } from '../config.js';
import { redis, KEYS, STREAMS } from '../redis.js';
import { priceStore } from '../market-data/price-store.js';
import { fillToFields, type FillMessage } from './position-tracker.js';
import { Decimal, mul, normalize, toMoneyString, ZERO } from '../precision/decimal.js';
import { hashSeed, mulberry32 } from '../util.js';

export type Side = 'BUY' | 'SELL';

export interface SubmitParams {
  id: string;
  symbol: string;
  side: Side;
  qty: Decimal;
  liquidation: boolean;
  suppressPublish: boolean;
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

// The seam where a real venue would plug in: order submission, fill
// publication, authoritative order state, and the venue's own trade
// statement for reconciliation. The simulated adapter implements all of it
// with honest semantics (deterministic external trade ids, Redis-backed
// books, fills priced off the live mark) — but nothing in the engine knows
// it is talking to a simulator.
export interface ExchangeAdapter {
  readonly name: string;
  nextOrderId(): Promise<string>;
  submit(params: SubmitParams): Promise<void>;
  pump(): Promise<number>;
  getOrder(orderId: string): Promise<ExchangeOrderView | null>;
  listRecentTrades(sinceMs: number): Promise<{ id: string; fields: string[] }[]>;
  forcePublishFills(orderId: string): Promise<number>;
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

function ONE_PLUS(d: Decimal): Decimal {
  return new Decimal(1).plus(d);
}

export class SimulatedExchange implements ExchangeAdapter {
  readonly name = 'simulated';

  private planFills(orderId: string, qty: Decimal, maxSlipBps: number): FillPlan[] {
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

  private fillPrice(mark: Decimal, side: Side, slipBps: number): Decimal {
    const slip = new Decimal(slipBps).dividedBy(10000);
    const adj = side === 'BUY' ? mark.times(ONE_PLUS(slip)) : mark.times(ONE_PLUS(slip.negated()));
    return normalize(adj);
  }

  async nextOrderId(): Promise<string> {
    const n = await redis.incr(KEYS.orderSeq);
    return `SIM-${Date.now().toString(36)}-${n.toString(36)}`;
  }

  async submit(params: SubmitParams): Promise<void> {
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
      fills: this.planFills(params.id, params.qty, maxSlip),
      emitted: [],
    };
    // The simulated exchange keeps its own books in Redis rather than in memory:
    // any process can reconstruct its state, which is what makes the
    // reconciliation sweep against our Postgres books meaningful.
    await redis.hset(KEYS.orderBook(params.id), 'book', JSON.stringify(book));
    await redis.zadd(KEYS.openOrders, String(Date.now()), params.id);
  }

  async getOrder(orderId: string): Promise<ExchangeOrderView | null> {
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

  // Publishes every fill of every open order whose scheduled delay has
  // elapsed. Deterministic external trade ids (orderId:fillIndex) make
  // double-publishing harmless: ingestion deduplicates on (venue, external_id).
  async pump(): Promise<number> {
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
        const price = this.fillPrice(mark.price, book.side, f.slipBps);
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

  async listRecentTrades(sinceMs: number): Promise<{ id: string; fields: string[] }[]> {
    const entries = (await redis.xrange(STREAMS.exchangeTrades, String(sinceMs), '+')) as [string, string[]][];
    return entries.map(([id, fields]) => ({ id, fields }));
  }

  async forcePublishFills(orderId: string): Promise<number> {
    // Test/ops hook: force all fills of a suppressed order due immediately in
    // the exchange statement without waiting for delays.
    const raw = await redis.hget(KEYS.orderBook(orderId), 'book');
    if (!raw) return 0;
    const book = JSON.parse(raw) as ExchangeBook;
    book.createdAt = Math.min(book.createdAt, Date.now() - 10_000);
    for (const f of book.fills) f.delayMs = Math.min(f.delayMs, 100);
    await redis.hset(KEYS.orderBook(orderId), 'book', JSON.stringify(book));
    return book.fills.length;
  }
}

let instance: ExchangeAdapter | null = null;

export function getExchangeAdapter(): ExchangeAdapter {
  if (instance) return instance;
  if (config.exchangeAdapter === 'simulated') {
    instance = new SimulatedExchange();
    return instance;
  }
  // Fail-closed configuration: an unknown adapter name must stop the process,
  // not silently fall back to the simulator.
  throw new Error(`unknown EXCHANGE_ADAPTER "${config.exchangeAdapter}" — supported: simulated`);
}
