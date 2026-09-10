# Dockerized Stock Market Data Pipeline (Airflow + PostgreSQL)

A fully containerized data pipeline that fetches daily stock price data from
the [Alpha Vantage](https://www.alphavantage.co/) API on a schedule, parses
it, and upserts it into a PostgreSQL table — orchestrated with **Apache
Airflow**, built and run with a single `docker compose up`.

## Architecture

```
                 ┌────────────────────┐
   Alpha Vantage │                    │
   API  ───────► │  Airflow Scheduler │──┐
                 │  (stock_market_    │  │  fetch → parse → upsert
                 │   pipeline DAG)    │  │
                 └────────────────────┘  │
                                          ▼
┌──────────────────┐             ┌───────────────────┐
│ Airflow Webserver │◄───────────│     PostgreSQL     │
│   (UI, :8080)     │  metadata  │  - airflow db      │
└──────────────────┘             │  - stock_data db   │
                                  │    stock_prices    │
                                  │    pipeline_run_log│
                                  └────────────────────┘
```

One Postgres container hosts two databases: Airflow's own metadata store,
and `stock_data`, which holds the pipeline's output tables. This keeps the
whole stack to a single `docker compose up` while still separating the two
concerns.

## Project structure

```
.
├── docker-compose.yml          # Orchestrates Postgres + Airflow (init, webserver, scheduler)
├── Dockerfile                  # Airflow image + Python deps (requests, SQLAlchemy, etc.)
├── requirements.txt
├── .env.example                # Template for all required environment variables
├── dags/
│   └── stock_pipeline_dag.py   # Airflow DAG: fetch_process_store -> validate_run
├── scripts/
│   └── fetch_stock_data.py     # Fetch / parse / upsert logic (importable + standalone-runnable)
├── sql/
│   ├── init-multiple-dbs.sh    # Creates the airflow + stock_data roles/databases on first boot
│   └── init.sql                # Creates stock_prices + pipeline_run_log tables
└── logs/, plugins/             # Airflow working directories (auto-created, mounted as volumes)
```

## What the pipeline does

1. **Fetch** — calls Alpha Vantage's `TIME_SERIES_DAILY` endpoint for each
   symbol in `STOCK_SYMBOLS`, with retries (3 attempts, exponential
   backoff) on network errors.
2. **Parse** — extracts OHLCV (open/high/low/close/volume) values per
   trading day. Missing or malformed individual fields become `NULL`
   instead of crashing the run; a day with no usable prices at all is
   skipped and logged rather than stored as junk.
3. **Store** — `INSERT ... ON CONFLICT (symbol, trade_date) DO UPDATE` into
   `stock_prices`, so re-running the DAG for an already-loaded date range
   refreshes values instead of erroring on duplicates.
4. **Validate** — a second task checks the run actually produced data and
   surfaces a clear failure if it didn't (e.g. every symbol came back
   empty), rather than silently succeeding with zero rows written.
5. **Log** — every symbol/run combination gets a row in
   `pipeline_run_log` (`SUCCESS` / `NO_DATA` / `FAILED` + row counts + error
   message), so you can audit pipeline health straight from SQL without
   digging through Airflow logs.

### Error handling, specifically

- A bad ticker, a single rate-limited symbol, or a network blip on one
  symbol never stops the other symbols in the same run — failures are
  isolated per symbol (see `process_symbol` in `fetch_stock_data.py`).
- The DAG task only raises (triggering Airflow's retry policy) if **every**
  symbol failed outright, which is the signal of a real outage (bad API
  key, API down, network unreachable) worth retrying/alerting on.
- Airflow-level `retries=3` with exponential backoff (`retry_delay=2min`,
  capped at 15min) handles transient failures automatically.
- `on_failure_callback` is a stubbed alert hook — swap the log line for a
  Slack/email/PagerDuty call to wire up real alerting.

## Prerequisites

- Docker & Docker Compose v2
- A free Alpha Vantage API key: https://www.alphavantage.co/support/#api-key
  (the free tier is rate-limited to 25 requests/day and 5/minute — plenty
  for a daily/hourly pipeline over a handful of symbols)

## Setup

1. **Clone and configure environment variables**

   ```bash
   cp .env.example .env
   ```

   Edit `.env` and fill in:
   - `STOCK_API_KEY` — your Alpha Vantage key
   - `STOCK_SYMBOLS` — comma-separated tickers, e.g. `AAPL,MSFT,GOOGL`
   - Strong passwords for `POSTGRES_SUPERUSER_PASSWORD`, `STOCK_DB_PASSWORD`,
     `AIRFLOW_DB_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`
   - `AIRFLOW_FERNET_KEY` — generate one with:
     ```bash
     python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
     ```

2. **Build and start everything with one command**

   ```bash
   docker compose up --build -d
   ```

   This builds the custom Airflow image, starts Postgres, runs
   `airflow-init` (schema migration + admin user creation), then starts the
   webserver and scheduler.

3. **Open the Airflow UI**

   http://localhost:8080 — log in with `AIRFLOW_ADMIN_USER` /
   `AIRFLOW_ADMIN_PASSWORD` from your `.env`. The `stock_market_pipeline`
   DAG is unpaused by default and scheduled `@daily`; trigger it manually
   with the ▶ button to run it immediately.

4. **Check the data landed in Postgres**

   ```bash
   docker compose exec postgres psql -U "$STOCK_DB_USER" -d "$STOCK_DB_NAME" \
     -c "SELECT symbol, trade_date, close_price, volume FROM stock_prices ORDER BY trade_date DESC LIMIT 10;"

   docker compose exec postgres psql -U "$STOCK_DB_USER" -d "$STOCK_DB_NAME" \
     -c "SELECT run_id, symbol, status, rows_processed FROM pipeline_run_log ORDER BY started_at DESC LIMIT 10;"
   ```

## Running the fetch script standalone (outside Airflow)

Useful for local testing without spinning up the whole Airflow stack:

```bash
docker compose run --rm airflow-scheduler python /opt/airflow/scripts/fetch_stock_data.py
```

or, with a local Python env pointed at the containerized Postgres
(`docker compose up postgres -d` first):

```bash
pip install -r requirements.txt
export STOCK_API_KEY=... STOCK_SYMBOLS=AAPL STOCK_DB_HOST=localhost \
       STOCK_DB_NAME=stock_data STOCK_DB_USER=stock_user STOCK_DB_PASSWORD=...
python scripts/fetch_stock_data.py
```

## Changing the schedule

Edit `schedule_interval` in `dags/stock_pipeline_dag.py`:

- `"@daily"` (default) → once a day
- `"0 * * * *"` → hourly
- Any standard cron expression works

## Scaling & extending

- **More symbols / more frequent runs**: just extend `STOCK_SYMBOLS` or
  tighten the schedule — Alpha Vantage's free-tier rate limit
  (5 req/min, 25/day) is the practical ceiling; a paid key removes it.
- **More throughput**: swap `AIRFLOW__CORE__EXECUTOR` from `LocalExecutor`
  to `CeleryExecutor` (with a Redis/RabbitMQ broker and worker containers)
  to parallelize symbol fetches across multiple workers without changing
  the DAG logic.
- **Different/additional data source**: the fetch/parse logic is isolated
  in `scripts/fetch_stock_data.py` — add a new `fetch_raw_data_<provider>`
  function and branch on `STOCK_API_PROVIDER` without touching the DAG.
- **Alerting**: replace the `logger.error(...)` call in
  `_on_failure_alert` (`dags/stock_pipeline_dag.py`) with a Slack webhook
  or `EmailOperator` call.

## Stopping / resetting

```bash
docker compose down          # stop containers, keep data
docker compose down -v       # stop containers AND wipe the Postgres volume
```

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `airflow-webserver` unhealthy / restarting | Check `docker compose logs airflow-init` — usually a DB connection or Fernet key issue. |
| DAG task fails with "STOCK_API_KEY is not set" | `.env` wasn't filled in, or you started containers before creating `.env`. Re-run `docker compose up -d --build` after fixing `.env`. |
| Every symbol shows `FAILED` with a "Note"/rate-limit message | You've hit Alpha Vantage's free-tier rate limit (5 req/min or 25/day) — reduce `STOCK_SYMBOLS` or wait. |
| `stock_prices` table doesn't exist | The Postgres volume already existed from a previous run, so `sql/init.sql` didn't re-run (init scripts only run on a fresh volume). Run `docker compose down -v` and start again, or apply `sql/init.sql` manually. |
