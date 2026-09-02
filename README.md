# risk-engine

A real-time crypto portfolio risk engine: live market data, idempotent trade
settlement against an append-only ledger, tiered margin, historical VaR,
auto-liquidation, and reconciliation sweeps — the patterns real trading
infrastructure is built on, implemented small enough to actually read.

> **This is an educational / portfolio project, not production infrastructure.
> It never routes orders to a real exchange, holds no funds, and makes no
> claims of fitness for any real trading purpose.** The exchange is an
> in-process simulator; the engineering around it (ledger, risk, recovery)
> is real and is the point.

## What is real vs simulated

| Surface | Status | Detail |
|---|---|---|
| Postgres / Redis | real | All persistence, streams, locks, rate limiting |
| Ledger & invariants | real | SERIALIZABLE transactions, DB CHECKs, append-only trigger, idempotency keys |
| Price feeds | real data, best-effort | Public Binance + Coinbase websocket tickers (no credentials). If unreachable, a clearly-tagged synthetic feed takes over (`source=simulated`) |
| Price history | synthetic | 500 days of seeded, deterministic-per-symbol random walks (for VaR) |
| The exchange | simulated | `sim-binance` venue: orders, partial fills, slippage, fees — books kept in Redis so reconciliation is meaningful |
| Risk math | real | Tiered margin, MTM equity, historical-simulation VaR/ES, exposure |
| Liquidation | real logic, simulated fills | Fail-closed engine; fills priced off the scenario/live mark with extra slippage |
| Crash scenario | explicit test control | `scenario:{SYMBOL}` mark override (tagged `source=scenario`, TTL'd) used by the simulator to force a breach deterministically |

## Architecture

```
                    real public market data (best-effort)
   binance-ws ─┐                                        ┌─ coinbase-ws
               ▼                                        ▼
          ┌─────────────────────────────────────────────────────┐
          │ price-store                                        │
          │  Redis latest marks (freshest wins, divergence     │
          │  alarm)  ·  1m OHLCV bars → Postgres (monthly      │
          │  range partitions)  ·  synthetic fallback feed     │
          └───────────┬───────────────────────────┬─────────────┘
                      │ mark moves > 0.25%        │ marks
                      ▼                           ▼
              XADD risk:jobs            ┌──────────────────────┐
                                        │ risk engine (worker) │
  POST /orders ──► exchange-api ──┐     │  MTM equity, tiered  │
   (reserve margin,               │     │  maintenance, VaR,   │
    two-phase)                    ▼     │  exposure            │
                     XADD fills stream   │  breach → liquidate │
                          │             │  stale → pause+warn │
                          ▼             └─────────┬────────────┘
                  ┌────────────────┐              │ snapshots + events
                  │ settlement     │              ▼
                  │ SERIALIZABLE   │      XADD risk:events (Redis Stream)
                  │ tx per fill:   │              │
                  │ · trade claim  │              │
                  │  (partial uniq │              ▼
                  │  idx = dedupe) │        REST API (:8080)
                  │ · position row │        /portfolio /positions
                  │  FOR UPDATE    │        /orders POST/GET
                  │  SKIP LOCKED   │        /risk/margin /risk/var
                  │ · ledger rows  │        /risk/exposure /risk/events
                  │  locked latest │        /health /ready
                  │  (balance_after│
                  │  chain)        │        reconcile sweep (every 15s)
                  │ · orders row   │        · XPENDING/XCLAIM stale messages
                  └────────┬───────┘        · CDR: exchange statement vs
                           ▼                 local trades (lost fills)
                      Postgres               · stuck orders → release
                   (append-only ledger,      · full invariant verification
                    positions, trades,
                    orders, snapshots,       one instance scales: consumer
                    audit, partitioned       groups + SKIP LOCKED + TTL
                    OHLCV prices)            locks — run `npm run worker` twice
```

## Design decisions

**Money discipline — one rounding mode, no floats, ever.** Every monetary
value is a `decimal.js` Decimal quantized to 18 dp with **ROUND_UP** (away
from zero): fees, losses and margin requirements are never understated, and
the same normalized value is written to ledger, positions and streams
(canonical 18-dp strings). The only place a JS number may appear is JSON
input at the API boundary, converted once via its shortest decimal string.

Two precision bugs found during the build are worth recording:

- *Prisma bundles its own decimal.js copy* configured with the default
  **20-significant-digit** precision. Arithmetic on DB-returned Decimals
  silently rounded at 20 digits — visible as ~1e-8 drift on six-figure sums
  and caught by the invariant checks. `src/precision/decimal.ts` now
  converts every DB-returned Decimal through its exact string form
  (`fromDb`) at the read boundary.
- *ROUND_UP on pro-rata reservation releases* made multi-fill releases sum
  to slightly more than the reserved amount. The final fill now releases the
  exact remainder (`margin_reserved − margin_released`), so releases sum to
  the reservation to the last nano — even if that means a tiny negative
  final release.

**Ledger invariants are the database's job, not a convention.**
`ledger_entries` is append-only — a `BEFORE UPDATE OR DELETE` trigger raises
on any mutation (TRUNCATE during an explicit `--force` reseed is the
sanctioned reset). A CHECK constraint enforces account-type invariants
(`USD`, `USD:MARGIN ≥ 0`; `USD:LOAN ≤ 0`). Every mutation runs in a
SERIALIZABLE transaction with bounded, jittered retry (40001 is expected,
not exceptional), and each append locks the account's newest row so the
`balance_after` chain can never fork. Idempotency keys are enforced by a
partial unique index — a replayed settlement cannot double-pay even if the
application logic were wrong.

**Idempotent trade ingestion.** A partial unique index on
`(venue, external_id) WHERE status <> 'REJECTED'` makes duplicate delivery a
database-level no-op: the duplicate is audited (`DUPLICATE_TRADE`) and acked,
never settled twice. A trade stuck in `RECEIVED` by a crash is re-settled by
the reclaim sweep; the same keys make that safe. `REJECTED` trades (bad
payloads) deliberately don't occupy their id, so a corrected retry stays
possible.

**Two-phase order lifecycle.** Phase one estimates the marginal initial
margin at the current mark and ring-fences it (wallet → margin) in the same
transaction that records the order — fail-closed: no free funds, no order.
Phase two (per fill, in one SERIALIZABLE transaction) releases the
reservation pro-rata (exact on the final fill), posts the exact incremental
margin, realizes PnL into the wallet, and pays the fee. A crash between the
phases leaves either a `MARGIN_RESERVED` order with no exchange book entry
(the sweep releases the reservation after the TTL) or a fill only in the
exchange's own statement (the CDR sweep recovers it).

The margin account is a **pure ring-fence**: its balance always equals
`Σ positions.margin_posted + Σ outstanding order reservations` — exactly,
at every point in the lifecycle, and the reconcile sweep re-proves it from
scratch every 15 seconds.

**Reconciliation (CDR).** Three sweeps, all idempotent: (1) stream messages
left pending longer than the claim threshold are re-claimed via
`XPENDING`/`XCLAIM` and re-processed; (2) the exchange's own trade statement
(Redis stream written by the simulated exchange) is diffed against local
trades — anything the exchange filled that we never settled is republished
to the ingestion stream and audited (`RECON_RECOVERED`); (3) orders stuck
mid-lifecycle beyond the TTL are completed or cancelled with their
reservations released. A lost fill has no path back except the sweep —
that's the point.

**Fail-open vs fail-closed, argued per surface.**

| Surface | Choice | Reasoning |
|---|---|---|
| Stale price feed → liquidation | **fail-open** (pause, never liquidate on stale data) | Liquidating on a stale mark can destroy a healthy portfolio on a false breach — an irreversible sell-low. Pausing is recoverable; a false liquidation is not. Marks are flagged, events fire, snapshots show `STALE_PRICES`. |
| Maintenance breach (fresh data) | **fail-closed** (liquidate) | A circuit breaker that might not trip is worse than one that trips. The engine submits reduction orders without waiting for a human; every round is audited and published. |
| New orders on a stale market | **fail-closed** (refuse, 503) | Refusing to price new risk off minutes-old data is cheap; accepting it is not. |
| Ledger invariant violation at settle | **fail-closed** (reject + audit, never swallow) | Loud, typed failure with an audit trail beats silent corruption. The DB CHECK is the backstop. |
| Feed goes offline entirely | **fail-open** (synthetic walk, clearly tagged) | Keeps the demo runnable offline; marks carry `source=simulated` so nothing synthetic is presented as a real print. Liquidation still only acts on fresh (synthetic or real) marks. |
| Bankruptcy gap (loss beyond wallet) | **fail-closed into an explicit account** | Realized losses that exceed the wallet are funded by the `USD:LOAN` deficit account (≤ 0) — the simulator's stand-in for an insurance fund / ADL. It is surfaced loudly (`bankruptcy` event, `IN_DEFICIT` status), never hidden. |

**Rate limiting.** A token bucket implemented as one Redis Lua script:
check-and-consume is atomic, bucket state lives in Redis rather than process
memory, so a crashed worker can never strand capacity (the failure mode of
in-process semaphores), and the clock is Redis server time so all callers
agree. Used for order submission (8 burst / 2 per second) and the API
(50 burst / 10 per second per client) — verified to return exact 429s.

**Margin model.** Notional-laddered tiers (2%→25% initial, 1%→10%
maintenance as size grows past 10k / 50k / 200k / 1M / ∞ USD). Equity =
wallet + posted margin + deficit loan + unrealized PnL; realization only
moves value between accounts, so equity moves with marks, fees and loan use
— never with settlement. `marginRatio = equity / maintenanceRequired`:
warning below 1.4, breach below 1.0, liquidation targets 1.5.

**Liquidation engine.** Greedy reduction: cut the largest-maintenance
positions first until the predicted requirement fits under equity / 1.5.
Rounds are serialized two ways — an in-flight guard (no new round while the
previous round's orders are unsettled) and a TTL lock that is deliberately
not released early (its 10 s expiry is the round cooldown; a crashed
liquidator's lock simply expires). Early release was observed in testing to
let a second round double-sell through zero into a short; the cooldown
removed that class of failure.

**Risk evaluation as claimed jobs.** Every evaluation is a job on a Redis
Stream: a timer enqueues ticks, price moves enqueue triggers, workers claim
them through consumer groups, and a crashed worker's pending jobs are
reclaimed by the sweep. Nothing evaluates twice invisibly; nothing is lost
on a crash.

**Blocking consumers get dedicated Redis connections.** Redis processes
commands per connection sequentially, so a blocking `XREADGROUP` on the
shared connection stalled every other command (heartbeats, pump, evaluation)
for the full BLOCK window — observed as 16-second evaluation cycles.
Blocking loops now run on their own connections
(`createBlockingConnection`).

**Historical VaR.** Current position sizes are re-priced across the trailing
window of daily closes (500 days seeded), producing one portfolio P&L per
historical day; VaR95/99 are loss quantiles of that distribution, ES95 the
tail mean. All Decimal arithmetic. Positions reprice the whole distribution
on change (or per minute), so snapshots carry live VaR.

## Running it

Prerequisites (macOS/Homebrew; see `docker-compose.yml` for an optional
containerized alternative):

```bash
brew install postgresql@16 redis
brew services start postgresql@16 redis
node -v   # needs >= 20
```

Then, in the project root:

```bash
npm install
npm run db:up      # creates the risk_engine database + applies migrations
npm run seed       # 500d price history, 1M USD deposit, 3 seeded positions
npm run worker     # terminal 1: feeds + settlement + risk engine + reconcile
npm run api        # terminal 2: REST API on http://127.0.0.1:8080
npm run simulate   # terminal 3: full demo session with verification
```

Useful extras:

```bash
npm run seed -- --force       # wipe books + simulator state and reseed
npm run simulate -- --no-crash --no-dup --no-lost   # pick phases
npm run simulate -- --crash-pct 0.25                # tune the scenario
npm run worker                # a second instance: consumer groups + locks share the load
npm run typecheck
```

## Verifying it works

**1. Seed** prints the settled seed positions and balances — wallet +
posted + fees must equal the 1,000,000 deposit to the last digit:

```
[seed] BTC: bought 8 @ 40952.15 (fee 229.33) -> SETTLED
...
  wallet USD     960262.918844157125171200
  margin posted  39383.685947285636832000
```

**2. Worker** evaluates every ~3 s and reconciles every 15 s:

```
[risk] ... status=OK equity=1316055.23 maint=34033.90 ratio=38.669 action=OK
[reconcile] invariants=PASS
```

**3. API** (marks show live exchange prices when feeds are reachable):

```bash
curl -s localhost:8080/health     # status ok, feeds fresh, worker alive
curl -s localhost:8080/portfolio | python3 -m json.tool
curl -s localhost:8080/risk/var   | python3 -m json.tool
curl -s localhost:8080/risk/margin | python3 -m json.tool
curl -s localhost:8080/risk/events?limit=20 | python3 -m json.tool
curl -s -X POST localhost:8080/orders -H 'content-type: application/json' \
     -d '{"symbol":"BTC","side":"BUY","qty":"0.1"}'
```

**4. Simulate** runs the full session and exits non-zero on any failure.
Each phase is independently checked:

```
[trade]     PASS — all demo orders placed, filled and settled through the two-phase lifecycle
[duplicate] PASS — trade rows 1->1 (unchanged), DUPLICATE_TRADE audit written — idempotent ingestion holds
[lost-fill] PASS — order -> SETTLED, RECON_RECOVERED audit entries — CDR sweep recovered the fill
[crash]     PASS — fail-closed liquidation executed on maintenance breach (...)
=== invariant verification ===
  PASS  ledger balance_after chain equals sum of amounts per account
  PASS  USD:MARGIN balance = Σ posted margin + outstanding reservations
  PASS  wallet >= 0 and deficit loan <= 0
  PASS  no trade stuck in RECEIVED beyond 2 minutes
  PASS  every SETTLED order is fully filled
result: ALL CHECKS PASSED
```

**5. Database guarantees** (try to break them):

```bash
psql -h localhost -d risk_engine -c 'UPDATE ledger_entries SET amount = 1 WHERE id = 1;'
# ERROR: ledger_entries is append-only

psql -h localhost -d risk_engine -c \
  "INSERT INTO ledger_entries (account,kind,amount,balance_after) VALUES ('USD','HACK','-1','-1');"
# ERROR: new row ... violates check constraint "ledger_balance_check"
```

The event timeline of a full session (`curl localhost:8080/risk/events`)
tells the story end-to-end: `order_placed` → `scenario_set` →
`margin_warning` → `liquidation` → `margin_recovered` → `price_fresh`.

## API

| Method & path | Purpose |
|---|---|
| `GET /health` | deep health: postgres, redis, feed freshness per venue, worker heartbeat, last snapshot |
| `GET /ready` | liveness (200/503) |
| `GET /portfolio` | wallet/margin/loan/equity, per-position marks and PnL |
| `GET /positions` | all position rows (incl. flat) |
| `POST /orders` | `{symbol, side, qty}` — qty as decimal *string* preferred; full two-phase lifecycle; 202 |
| `GET /orders`, `GET /orders/:id` | order list / order with its settled trades |
| `GET /risk/margin` | equity, tiered maintenance breakdown per position, ratio, thresholds, tier table |
| `GET /risk/var` | VaR95/99, ES95, per-symbol, methodology, window |
| `GET /risk/exposure` | per-asset / per-venue net & gross notional, concentration |
| `GET /risk/events` | recent risk events from the stream |

Errors are mapped: 429 rate limited (with `retry-after-ms`), 503 stale
market, 422 insufficient free funds, 400 bad request.

## Project layout

```
prisma/schema.prisma            models (columns @map to snake_case)
prisma/migrations/…/migration.sql   hand-written DDL: partitions, partial
                                 unique indexes, CHECKs, append-only trigger
src/precision/decimal.ts        money discipline (ROUND_UP, fromDb boundary)
src/market-data/                binance-ws, coinbase-ws, price-store,
                                rate-limiter (Lua token bucket)
src/positions/                  exchange-api (simulated venue, two-phase
                                orders), position-tracker (idempotent
                                settle), ledger, reconcile (CDR sweep)
src/risk/                       mark-to-market, margin, var, exposure,
                                liquidation
src/pipeline/                   event-bus (Redis Streams), worker
src/api/                        server, positions, risk, health
scripts/                        db-up, seed, simulate
```

## Limitations (honesty notes)

- Single portfolio, single wallet — no multi-account routing.
- The simulated exchange fills from the live mark with seeded slippage; it
  cannot model depth, queue position, or real liquidity.
- Liquidation is iterative (round → settle → re-evaluate), not a solver.
- The deficit loan is a placeholder for a real insurance fund / ADL design.
- VaR uses seeded synthetic history by default; plug a real price history
  into `prices` (`granularity='1d'`) and the same code computes from it.
- Snapshot rows accumulate ~20/min while the worker runs; `TRUNCATE
  risk_snapshots` (or a retention job) in long-running demos.
