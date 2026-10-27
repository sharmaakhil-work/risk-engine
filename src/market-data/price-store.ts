import { PrismaClient } from '@prisma/client';
import type Redis from 'ioredis';
import { config, isSymbol, SYMBOLS } from '../config.js';
import { prisma } from '../db.js';
import { redis, KEYS, STREAMS } from '../redis.js';
import { audit } from '../audit.js';
import { publishEvent } from '../pipeline/event-bus.js';
import { component } from '../observability/logger.js';
import { Decimal, normalize, toMoneyString } from '../precision/decimal.js';

const log = component('price-store');

export interface Tick {
  venue: string;
  symbol: string;
  price: Decimal;
  size: Decimal | null;
  ts: number;
}

export interface Mark {
  symbol: string;
  price: Decimal;
  source: string;
  ts: number;
  ageMs: number;
  stale: boolean;
}

export interface FeedStatus {
  venue: string;
  lastTickAt: number;
  ageMs: number;
}

interface SourceState {
  p: string;
  ts: number;
}

interface LiveBar {
  symbol: string;
  bucketMs: number;
  open: Decimal;
  high: Decimal;
  low: Decimal;
  close: Decimal;
  volume: Decimal;
}

const DIVERGENCE_TOL = new Decimal('0.01');
const MOVE_TRIGGER = new Decimal('0.0025');

export class PriceStore {
  private partitionMonths = new Set<string>();
  private bars = new Map<string, LiveBar>();
  private timers: NodeJS.Timeout[] = [];
  private lastCanonical = new Map<string, Decimal>();

  constructor(
    private prisma: PrismaClient,
    private redis: Redis,
  ) {}

  async ingestTick(tick: Tick): Promise<void> {
    if (!isSymbol(tick.symbol) || !tick.price.greaterThan(0)) return;
    const payload = JSON.stringify({ p: toMoneyString(tick.price), ts: tick.ts } satisfies SourceState);
    await this.redis.hset(KEYS.sourceTick(tick.symbol), tick.venue, payload);
    await this.redis.set(KEYS.feedStatus(tick.venue), String(Date.now()), 'EX', 20);
    await this.recomputeMark(tick.symbol);
    this.updateBar(tick);
  }

  private async recomputeMark(symbol: string): Promise<void> {
    const [scenarioRaw, sourcesRaw] = await Promise.all([
      this.redis.get(KEYS.scenario(symbol)),
      this.redis.hgetall(KEYS.sourceTick(symbol)),
    ]);

    let chosen: { price: Decimal; source: string; ts: number } | null = null;
    if (scenarioRaw) {
      const s = JSON.parse(scenarioRaw) as { p: string };
      // Scenario marks are operator intent (test harness): authoritative price,
      // always considered fresh while the override exists.
      chosen = { price: new Decimal(s.p), source: 'scenario', ts: Date.now() };
    } else {
      for (const [venue, raw] of Object.entries(sourcesRaw)) {
        try {
          const st = JSON.parse(raw) as SourceState;
          const cand = { price: new Decimal(st.p), source: venue, ts: st.ts };
          if (!chosen) {
            chosen = cand;
          } else if (cand.ts > chosen.ts || (cand.ts === chosen.ts && chosen.source === 'simulated' && venue !== 'simulated')) {
            chosen = cand;
          }
        } catch {
          // corrupt source entry
        }
      }
    }
    if (!chosen) return;

    await this.redis.hset(KEYS.mark(symbol), {
      price: toMoneyString(chosen.price),
      source: chosen.source,
      ts: String(chosen.ts),
    });

    const prev = this.lastCanonical.get(symbol);
    if (prev?.greaterThan(0)) {
      const move = chosen.price.minus(prev).abs().dividedBy(prev);
      if (move.greaterThan(MOVE_TRIGGER)) {
        await this.redis.xadd(
          STREAMS.riskJobs,
          'MAXLEN',
          '~',
          '1000',
          '*',
          'type',
          'move',
          'symbol',
          symbol,
          'price',
          toMoneyString(chosen.price),
        );
      }
    }
    this.lastCanonical.set(symbol, chosen.price);

    await this.checkDivergence(symbol, sourcesRaw);
  }

  // If two real venues disagree by more than 1% something is wrong with one of
  // them; the fresher print wins for marking, but the disagreement is surfaced
  // loudly instead of silently averaged away.
  private async checkDivergence(symbol: string, sourcesRaw: Record<string, string>): Promise<void> {
    const states: { venue: string; price: Decimal; ts: number }[] = [];
    for (const venue of ['binance', 'coinbase']) {
      const raw = sourcesRaw[venue];
      if (!raw) continue;
      try {
        const st = JSON.parse(raw) as SourceState;
        states.push({ venue, price: new Decimal(st.p), ts: st.ts });
      } catch {
        // ignore
      }
    }
    if (states.length < 2) return;
    if (Date.now() - Math.max(...states.map(s => s.ts)) > 5_000) return;
    const [a, b] = states;
    const spread = a!.price.minus(b!.price).abs().dividedBy(Decimal.min(a!.price, b!.price));
    if (spread.lessThanOrEqualTo(DIVERGENCE_TOL)) return;
    const dedup = await this.redis.set(`div:${symbol}`, '1', 'EX', 300, 'NX');
    if (!dedup) return;
    await audit(
      this.prisma,
      'PRICE_DIVERGENCE',
      symbol,
      `venues diverged ${spread.toFixed(4)} (>1%)`,
      { binance: toMoneyString(a!.price), coinbase: toMoneyString(b!.price) },
    );
    await publishEvent('price_divergence', { symbol, spreadPct: spread.times(100).toFixed(2) });
  }

  async getMark(symbol: string): Promise<Mark | null> {
    const raw = await this.redis.hgetall(KEYS.mark(symbol));
    if (!raw.price) return null;
    const ts = Number(raw.ts ?? 0);
    const ageMs = Math.max(0, Date.now() - ts);
    return {
      symbol,
      price: new Decimal(raw.price),
      source: raw.source ?? 'unknown',
      ts,
      ageMs,
      stale: raw.source !== 'scenario' && ageMs > config.staleAfterMs,
    };
  }

  async getMarks(): Promise<Map<string, Mark>> {
    const marks = new Map<string, Mark>();
    for (const symbol of SYMBOLS) {
      const mark = await this.getMark(symbol);
      if (mark) marks.set(symbol, mark);
    }
    return marks;
  }

  async setScenario(symbol: string, price: Decimal, ttlSec: number): Promise<void> {
    await this.redis.set(KEYS.scenario(symbol), JSON.stringify({ p: toMoneyString(price) }), 'EX', ttlSec);
    await this.recomputeMark(symbol);
    await publishEvent('scenario_set', { symbol, price: toMoneyString(price), ttlSec });
  }

  async clearScenario(symbol: string): Promise<void> {
    await this.redis.del(KEYS.scenario(symbol));
    await this.recomputeMark(symbol);
  }

  async hasScenario(symbol: string): Promise<boolean> {
    return (await this.redis.exists(KEYS.scenario(symbol))) === 1;
  }

  private updateBar(tick: Tick): void {
    const bucketMs = Math.floor(tick.ts / 60_000) * 60_000;
    const key = `${tick.symbol}:${bucketMs}`;
    const size = tick.size?.greaterThan(0) ? tick.size : new Decimal(0);
    const bar = this.bars.get(key);
    if (bar) {
      bar.high = Decimal.max(bar.high, tick.price);
      bar.low = Decimal.min(bar.low, tick.price);
      bar.close = tick.price;
      bar.volume = normalize(bar.volume.plus(size));
    } else {
      this.bars.set(key, {
        symbol: tick.symbol,
        bucketMs,
        open: tick.price,
        high: tick.price,
        low: tick.price,
        close: tick.price,
        volume: size,
      });
    }
    if (this.bars.size > 60) {
      const cutoff = Date.now() - 180_000;
      for (const [k, b] of this.bars) if (b.bucketMs < cutoff) this.bars.delete(k);
    }
  }

  async flushBars(): Promise<number> {
    let written = 0;
    for (const bar of [...this.bars.values()]) {
      await this.ensurePartition(new Date(bar.bucketMs));
      // Re-flushing the still-open minute bar is idempotent: high/low/close
      // merge monotonically and volume takes the max, never a double count.
      await this.prisma.$executeRaw`
        INSERT INTO "prices" ("symbol","granularity","ts","open","high","low","close","volume","source")
        VALUES (${bar.symbol}, '1m', ${new Date(bar.bucketMs)}, ${toMoneyString(bar.open)}::numeric, ${toMoneyString(bar.high)}::numeric,
                ${toMoneyString(bar.low)}::numeric, ${toMoneyString(bar.close)}::numeric, ${toMoneyString(bar.volume)}::numeric, 'live')
        ON CONFLICT ("symbol","granularity","ts") DO UPDATE SET
          "high" = GREATEST("prices"."high", EXCLUDED."high"),
          "low" = LEAST("prices"."low", EXCLUDED."low"),
          "close" = EXCLUDED."close",
          "volume" = GREATEST("prices"."volume", EXCLUDED."volume")`;
      written++;
    }
    return written;
  }

  private async ensurePartition(ts: Date): Promise<void> {
    const iso = ts.toISOString();
    const ym = iso.slice(0, 7);
    const name = `prices_${ym.replace('-', '_')}`;
    if (this.partitionMonths.has(name)) return;
    if (!/^prices_\d{4}_\d{2}$/.test(name)) throw new Error(`bad partition name ${name}`);
    const year = Number(ym.slice(0, 4));
    const month = Number(ym.slice(5, 7));
    const nextYm = month === 12 ? `${year + 1}-01` : `${year}-${String(month + 1).padStart(2, '0')}`;
    await this.prisma.$executeRawUnsafe(
      `CREATE TABLE IF NOT EXISTS "${name}" PARTITION OF "prices" FOR VALUES FROM ('${ym}-01') TO ('${nextYm}-01')`,
    );
    this.partitionMonths.add(name);
  }

  async ensurePartitionForDate(ts: Date): Promise<void> {
    await this.ensurePartition(ts);
  }

  // Before any live tick arrives, mark from the newest daily bar so seeded
  // positions have a defensible mark; if no history exists either, fall back
  // to static anchors. Both are tagged with their true source.
  async loadHistoryAnchors(): Promise<void> {
    for (const symbol of SYMBOLS) {
      if (await this.redis.exists(KEYS.mark(symbol))) continue;
      const rows = await this.prisma.$queryRaw<{ close: Decimal }[]>`
        SELECT "close" FROM "prices" WHERE "symbol" = ${symbol} AND "granularity" = '1d'
        ORDER BY "ts" DESC LIMIT 1`;
      const row = rows[0];
      const price = row ? new Decimal(row.close) : new Decimal(config.anchors[symbol] ?? '1');
      await this.redis.hset(KEYS.mark(symbol), {
        price: toMoneyString(price),
        source: row ? 'history' : 'default-anchor',
        ts: String(Date.now()),
      });
    }
  }

  // When no real feed has produced a fresh mark, a synthetic random walk takes
  // over so the whole pipeline (bars, risk eval, liquidation) stays
  // demonstrable offline. Marks are tagged source=simulated and can never
  // override a live or scenario source.
  private async fallbackTick(): Promise<void> {
    for (const symbol of SYMBOLS) {
      if (await this.hasScenario(symbol)) continue;
      const mark = await this.getMark(symbol);
      if (!mark || mark.stale) {
        const base = mark?.price ?? new Decimal(config.anchors[symbol] ?? '1');
        const drift = 1 + (Math.random() - 0.5) * 0.0012;
        const price = normalize(base.times(drift));
        await this.ingestTick({ venue: 'simulated', symbol, price, size: null, ts: Date.now() });
      }
    }
  }

  startHousekeeping(): void {
    this.timers.push(
      setInterval(() => {
        void this.flushBars().catch(err => log.error({ err: err.message }, 'bar flush failed'));
      }, config.barFlushMs),
    );
    this.timers.push(
      setInterval(() => {
        void this.fallbackTick().catch(err => log.error({ err: err.message }, 'fallback tick failed'));
      }, 2_000),
    );
  }

  stopHousekeeping(): void {
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
  }

  async feedStatuses(): Promise<FeedStatus[]> {
    const out: FeedStatus[] = [];
    for (const venue of ['binance', 'coinbase', 'simulated']) {
      const raw = await this.redis.get(KEYS.feedStatus(venue));
      const lastTickAt = raw ? Number(raw) : 0;
      out.push({ venue, lastTickAt, ageMs: lastTickAt ? Date.now() - lastTickAt : Infinity });
    }
    return out;
  }
}

export const priceStore = new PriceStore(prisma, redis);
