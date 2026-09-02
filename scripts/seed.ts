import { prisma } from '../src/db.js';
import { redis, KEYS, STREAMS } from '../src/redis.js';
import { priceStore } from '../src/market-data/price-store.js';
import { ACCOUNTS, balances, deposit } from '../src/positions/ledger.js';
import { settleFill } from '../src/positions/position-tracker.js';
import { config, VENUES } from '../src/config.js';
import { Decimal, normalize, toMoneyString } from '../src/precision/decimal.js';
import { hashSeed, mulberry32 } from '../src/util.js';

const args = new Set(process.argv.slice(2));
const FORCE = args.has('--force');

async function wipeRedisState(): Promise<void> {
  // Streams are trimmed (not deleted) so consumer groups survive; the
  // simulated exchange's books and scenario/recovery guards are removed so no
  // pre-reset state can leak into the fresh books.
  for (const stream of [STREAMS.fills, STREAMS.exchangeTrades, STREAMS.riskJobs, STREAMS.riskEvents]) {
    await redis.xtrim(stream, 'MAXLEN', '0');
  }
  await redis.del(KEYS.openOrders, KEYS.doneOrders, 'risk:margin-state', 'risk:feed-state', 'risk:warn-echo');
  for (const pattern of ['sim:order:*', 'recovered:*', 'scenario:*', 'rl:*', 'div:*']) {
    let cursor = '0';
    do {
      const [next, keys] = (await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 200)) as [string, string[]];
      cursor = next;
      if (keys.length > 0) await redis.del(...keys);
    } while (cursor !== '0');
  }
}

// Synthetic but plausible per-symbol history: geometric random walk with
// realistic drift/volatility, fully deterministic per seed so runs reproduce.
const SYMBOL_PARAMS: Record<string, { anchor: string; vol: number; drift: number; volumeBase: number }> = {
  BTC: { anchor: '42000', vol: 0.035, drift: 0.0009, volumeBase: 800 },
  ETH: { anchor: '2300', vol: 0.045, drift: 0.0009, volumeBase: 6000 },
  SOL: { anchor: '90', vol: 0.06, drift: 0.0012, volumeBase: 40000 },
  DOGE: { anchor: '0.08', vol: 0.08, drift: 0.0012, volumeBase: 2_000_000 },
};

const DAYS = 500;

async function main(): Promise<void> {
  if (FORCE) {
    // TRUNCATE is statement-level, so it is the sanctioned way past the
    // row-level append-only trigger on the ledger — an explicit, deliberate
    // reset rather than a mutation of history.
    await prisma.$executeRawUnsafe(
      'TRUNCATE "ledger_entries","trades","positions","orders","risk_snapshots","audit_log","prices" RESTART IDENTITY',
    );
    await wipeRedisState();
    console.log('[seed] --force: wiped all tables and reset simulator state');
  } else {
    const existing = await prisma.ledgerEntry.count();
    if (existing > 0) {
      console.log(`[seed] ledger already has ${existing} entries — nothing to do (use --force to reset)`);
      return;
    }
  }

  const now = new Date();
  const todayUtc = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  const endDate = todayUtc - 86_400_000;

  for (let offsetMs = -(DAYS + 5) * 86_400_000; offsetMs <= 40 * 86_400_000; offsetMs += 20 * 86_400_000) {
    await priceStore.ensurePartitionForDate(new Date(endDate + offsetMs));
  }

  for (const [symbol, params] of Object.entries(SYMBOL_PARAMS)) {
    const rand = mulberry32(hashSeed(`seed-history-${symbol}`));
    let close = new Decimal(params.anchor);
    const bars: {
      symbol: string;
      granularity: string;
      ts: Date;
      open: Decimal;
      high: Decimal;
      low: Decimal;
      close: Decimal;
      volume: Decimal;
      source: string;
    }[] = [];
    for (let i = 0; i < DAYS; i++) {
      const ts = new Date(endDate - (DAYS - 1 - i) * 86_400_000);
      const raw = params.drift + params.vol * (rand() * 2 - 1);
      const r = Math.max(-3 * params.vol, Math.min(3 * params.vol, raw));
      const open = close;
      close = normalize(open.times(new Decimal(1 + r)));
      const high = normalize(Decimal.max(open, close).times(new Decimal(1 + rand() * 0.008)));
      const low = normalize(Decimal.min(open, close).times(new Decimal(1 - rand() * 0.008)));
      const volume = new Decimal(Math.round(params.volumeBase * (0.5 + rand())));
      bars.push({ symbol, granularity: '1d', ts, open, high, low, close, volume, source: 'seed-history' });
    }
    await prisma.price.createMany({ data: bars, skipDuplicates: true });
    console.log(`[seed] ${symbol}: ${bars.length} daily bars, first close ${bars[0]!.close.toFixed(2)}, last close ${close.toFixed(2)}`);
  }

  await priceStore.loadHistoryAnchors();
  console.log('[seed] initial marks loaded from history (source=history)');

  await deposit(new Decimal('1000000'), 'seed-deposit');
  console.log('[seed] deposited 1,000,000 USD (idempotency key deposit:seed-deposit)');

  const initialPositions: { symbol: string; qty: string }[] = [
    { symbol: 'BTC', qty: '8' },
    { symbol: 'ETH', qty: '60' },
    { symbol: 'SOL', qty: '400' },
  ];
  const feeRate = new Decimal(config.feeBps).dividedBy(10000);
  for (const p of initialPositions) {
    // Initial positions go through the REAL settlement path — same
    // SERIALIZABLE transaction, same ledger invariants, same tiered margin
    // posting. There is no backdoor into the books, not even for seeding.
    const firstBar = await prisma.$queryRaw<{ close: Decimal }[]>`
      SELECT "close" FROM "prices" WHERE "symbol" = ${p.symbol} AND "granularity" = '1d'
      ORDER BY "ts" ASC LIMIT 1`;
    const price = new Decimal(firstBar[0]!.close);
    const qty = new Decimal(p.qty);
    const fee = normalize(qty.times(price).times(feeRate));
    const outcome = await settleFill({
      externalId: `SEED-${p.symbol}-1`,
      venue: VENUES.binance,
      symbol: p.symbol,
      side: 'BUY',
      qty,
      price,
      fee,
      ts: Date.now() - DAYS * 86_400_000,
      source: 'seed',
    });
    const extra = outcome.status === 'REJECTED' ? ` (${outcome.reason})` : '';
    console.log(`[seed] ${p.symbol}: bought ${qty} @ ${price.toFixed(2)} (fee ${fee.toFixed(2)}) -> ${outcome.status}${extra}`);
    if (outcome.status !== 'SETTLED') throw new Error(`seed fill failed for ${p.symbol}`);
  }

  const bals = await balances();
  const positions = await prisma.position.findMany({ orderBy: { symbol: 'asc' } });
  console.log('\n[seed] done — current state:');
  console.log(`  wallet USD     ${toMoneyString(bals[ACCOUNTS.WALLET] ?? new Decimal(0))}`);
  console.log(`  margin posted  ${toMoneyString(bals[ACCOUNTS.MARGIN] ?? new Decimal(0))}`);
  for (const p of positions) {
    console.log(
      `  ${p.symbol.padEnd(5)} qty=${p.qty.toFixed(4).padStart(10)}  avgEntry=${p.avgEntryPrice.toFixed(2).padStart(10)}  posted=${p.marginPosted.toFixed(2).padStart(10)}`,
    );
  }
  console.log('\nnext steps (each in its own terminal):');
  console.log('  npm run worker    # feeds + settlement + risk engine + reconcile');
  console.log('  npm run api       # REST API on http://127.0.0.1:8080');
  console.log('  npm run simulate  # full demo session with invariant verification');
}

main()
  .catch(err => {
    console.error('[seed] fatal:', err);
    process.exitCode = 1;
  })
  .finally(() => {
    redis.disconnect();
    void prisma.$disconnect();
  });
