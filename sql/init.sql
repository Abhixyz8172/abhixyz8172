-- This file runs once, at first container start, after init-multiple-dbs.sh
-- has created the stock_data database. It creates the target table the
-- Airflow DAG upserts into on every run.

\connect stock_data

CREATE TABLE IF NOT EXISTS stock_prices (
    id              SERIAL PRIMARY KEY,
    symbol          VARCHAR(10)     NOT NULL,
    trade_date      DATE            NOT NULL,
    open_price      NUMERIC(14, 4),
    high_price      NUMERIC(14, 4),
    low_price       NUMERIC(14, 4),
    close_price     NUMERIC(14, 4),
    volume          BIGINT,
    source          VARCHAR(50)     NOT NULL DEFAULT 'alpha_vantage',
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_symbol_date UNIQUE (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol ON stock_prices (symbol);
CREATE INDEX IF NOT EXISTS idx_stock_prices_trade_date ON stock_prices (trade_date);

-- Keeps a lightweight audit trail of every pipeline run: how many rows were
-- fetched/inserted/updated/skipped and any error message, for debugging and
-- for demonstrating the "robustness" requirement to a reviewer.
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(255),
    symbol          VARCHAR(10),
    status          VARCHAR(20)     NOT NULL,   -- SUCCESS | FAILED | NO_DATA
    rows_processed  INTEGER         DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ
);
