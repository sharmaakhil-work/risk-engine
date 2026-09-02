-- Hand-written init migration.
-- Prisma's schema language cannot express: range partitioning, partial unique
-- indexes, account-conditional CHECK constraints, or triggers. The schema.prisma
-- file mirrors these tables for the client; this SQL is the source of truth.

CREATE TABLE "positions" (
    "id" SERIAL NOT NULL,
    "symbol" TEXT NOT NULL,
    "venue" TEXT NOT NULL,
    "qty" DECIMAL(38,18) NOT NULL DEFAULT 0,
    "avg_entry_price" DECIMAL(38,18) NOT NULL DEFAULT 0,
    "realized_pnl" DECIMAL(38,18) NOT NULL DEFAULT 0,
    "margin_posted" DECIMAL(38,18) NOT NULL DEFAULT 0,
    "status" TEXT NOT NULL DEFAULT 'OPEN',
    "version" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "positions_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "positions_margin_posted_check" CHECK ("margin_posted" >= 0),
    CONSTRAINT "positions_avg_check" CHECK ("avg_entry_price" >= 0)
);

CREATE UNIQUE INDEX "positions_symbol_venue_key" ON "positions"("symbol","venue");
CREATE INDEX "positions_symbol_idx" ON "positions"("symbol");

CREATE TABLE "trades" (
    "id" SERIAL NOT NULL,
    "external_id" TEXT NOT NULL,
    "venue" TEXT NOT NULL,
    "symbol" TEXT NOT NULL,
    "side" TEXT NOT NULL,
    "qty" DECIMAL(38,18) NOT NULL,
    "price" DECIMAL(38,18) NOT NULL,
    "fee" DECIMAL(38,18) NOT NULL DEFAULT 0,
    "order_id" TEXT,
    "is_liquidation" BOOLEAN NOT NULL DEFAULT false,
    "source" TEXT NOT NULL DEFAULT 'stream',
    "trade_ts" TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    "ingested_at" TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "settled_at" TIMESTAMP(6) WITH TIME ZONE,
    "status" TEXT NOT NULL DEFAULT 'RECEIVED',
    CONSTRAINT "trades_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "trades_qty_check" CHECK ("qty" > 0),
    CONSTRAINT "trades_price_check" CHECK ("price" > 0),
    CONSTRAINT "trades_fee_check" CHECK ("fee" >= 0)
);

-- Partial unique index: a REJECTED trade (bad payload) never blocks a corrected
-- retry that reuses the same exchange trade id; everything else is deduplicated
-- at the database level, so duplicate delivery can never double-settle.
CREATE UNIQUE INDEX "trades_venue_external_id_key" ON "trades"("venue","external_id") WHERE "status" <> 'REJECTED';
CREATE INDEX "trades_order_idx" ON "trades"("order_id");
CREATE INDEX "trades_status_idx" ON "trades"("status");

CREATE TABLE "ledger_entries" (
    "id" BIGSERIAL NOT NULL,
    "ts" TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "account" TEXT NOT NULL,
    "kind" TEXT NOT NULL,
    "amount" DECIMAL(38,18) NOT NULL,
    "balance_after" DECIMAL(38,18) NOT NULL,
    "idempotency_key" TEXT,
    "trade_id" INTEGER,
    "order_id" TEXT,
    "note" TEXT,
    CONSTRAINT "ledger_entries_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "ledger_amount_check" CHECK ("amount" <> 0),
    -- Account-type invariants enforced by the database, not just application code:
    -- wallet and ring-fenced margin can never go negative; the deficit loan can
    -- never go positive. This is the last line of defence against settlement bugs.
    CONSTRAINT "ledger_balance_check" CHECK (
        ("account" IN ('USD', 'USD:MARGIN') AND "balance_after" >= 0)
        OR ("account" = 'USD:LOAN' AND "balance_after" <= 0)
    )
);

CREATE UNIQUE INDEX "ledger_idempotency_key" ON "ledger_entries"("idempotency_key") WHERE "idempotency_key" IS NOT NULL;
CREATE INDEX "ledger_account_id_idx" ON "ledger_entries"("account", "id" DESC);
CREATE INDEX "ledger_ts_idx" ON "ledger_entries"("ts");

-- Append-only is a database guarantee here, not a convention. Any UPDATE or
-- DELETE on the ledger raises, so no code path (including manual psql sessions)
-- can silently rewrite financial history.
CREATE FUNCTION "forbid_ledger_mutation"() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'ledger_entries is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "ledger_append_only" BEFORE UPDATE OR DELETE ON "ledger_entries"
    FOR EACH ROW EXECUTE FUNCTION "forbid_ledger_mutation"();

CREATE TABLE "orders" (
    "id" TEXT NOT NULL,
    "venue" TEXT NOT NULL,
    "symbol" TEXT NOT NULL,
    "side" TEXT NOT NULL,
    "type" TEXT NOT NULL DEFAULT 'MARKET',
    "qty" DECIMAL(38,18) NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'NEW',
    "margin_reserved" DECIMAL(38,18) NOT NULL DEFAULT 0,
    "margin_released" DECIMAL(38,18) NOT NULL DEFAULT 0,
    "filled_qty" DECIMAL(38,18) NOT NULL DEFAULT 0,
    "avg_fill_price" DECIMAL(38,18) NOT NULL DEFAULT 0,
    "is_liquidation" BOOLEAN NOT NULL DEFAULT false,
    "fail_reason" TEXT,
    "created_at" TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "orders_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "orders_qty_check" CHECK ("qty" > 0),
    CONSTRAINT "orders_margin_reserved_check" CHECK ("margin_reserved" >= 0)
);

CREATE INDEX "orders_status_idx" ON "orders"("status");
CREATE INDEX "orders_created_idx" ON "orders"("created_at");

CREATE TABLE "risk_snapshots" (
    "id" BIGSERIAL NOT NULL,
    "ts" TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "status" TEXT NOT NULL,
    "equity" DECIMAL(38,18) NOT NULL,
    "wallet_balance" DECIMAL(38,18) NOT NULL,
    "margin_posted" DECIMAL(38,18) NOT NULL,
    "loan_balance" DECIMAL(38,18) NOT NULL,
    "unrealized_pnl" DECIMAL(38,18) NOT NULL,
    "realized_pnl" DECIMAL(38,18) NOT NULL,
    "maintenance_required" DECIMAL(38,18) NOT NULL,
    "margin_ratio" DECIMAL(38,18),
    "var95" DECIMAL(38,18),
    "var99" DECIMAL(38,18),
    "es95" DECIMAL(38,18),
    "gross_exposure" DECIMAL(38,18) NOT NULL,
    "net_exposure" DECIMAL(38,18) NOT NULL,
    "marks" JSONB,
    "detail" JSONB,
    "worker_id" TEXT,
    CONSTRAINT "risk_snapshots_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "risk_snapshots_ts_idx" ON "risk_snapshots"("ts");

CREATE TABLE "audit_log" (
    "id" BIGSERIAL NOT NULL,
    "ts" TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "kind" TEXT NOT NULL,
    "subject" TEXT,
    "message" TEXT NOT NULL,
    "data" JSONB,
    CONSTRAINT "audit_log_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "audit_ts_idx" ON "audit_log"("ts");
CREATE INDEX "audit_kind_idx" ON "audit_log"("kind");

-- OHLCV bars are range-partitioned by month: 500 days of daily history plus
-- live 1-minute bars stay queryable without one ever-growing heap. Partitions
-- for the current and following month are created here; the price store and
-- seed script create further partitions on demand.
CREATE TABLE "prices" (
    "symbol" TEXT NOT NULL,
    "granularity" TEXT NOT NULL,
    "ts" TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    "open" DECIMAL(38,18) NOT NULL,
    "high" DECIMAL(38,18) NOT NULL,
    "low" DECIMAL(38,18) NOT NULL,
    "close" DECIMAL(38,18) NOT NULL,
    "volume" DECIMAL(38,18) NOT NULL DEFAULT 0,
    "source" TEXT NOT NULL,
    CONSTRAINT "prices_pkey" PRIMARY KEY ("symbol","granularity","ts"),
    CONSTRAINT "prices_ohlc_check" CHECK (
        "high" >= "low" AND "high" >= "open" AND "high" >= "close"
        AND "low" <= "open" AND "low" <= "close" AND "volume" >= 0
    )
) PARTITION BY RANGE ("ts");

CREATE INDEX "prices_lookup_idx" ON "prices" ("symbol","granularity","ts" DESC);

DO $$
DECLARE
    m timestamptz := date_trunc('month', CURRENT_DATE);
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF "prices" FOR VALUES FROM (%L) TO (%L)',
        'prices_' || to_char(m, 'YYYY_MM'), m, m + interval '1 month');
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF "prices" FOR VALUES FROM (%L) TO (%L)',
        'prices_' || to_char(m + interval '1 month', 'YYYY_MM'), m + interval '1 month', m + interval '2 months');
END $$;
