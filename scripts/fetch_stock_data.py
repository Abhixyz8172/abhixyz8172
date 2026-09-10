"""
fetch_stock_data.py
--------------------
Fetches daily stock price data from the Alpha Vantage API, parses the JSON
response, and upserts it into the `stock_prices` table in PostgreSQL.

Designed to be:
  * Callable standalone (`python fetch_stock_data.py`) for local testing.
  * Imported by the Airflow DAG, one function per pipeline step, so each
    step shows up as its own task in the Airflow UI and can be retried
    independently.

All configuration (API key, symbols, DB credentials) comes from environment
variables -- see the "Environment variables" section below and the .env.example
file. Nothing sensitive is hardcoded.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger("stock_pipeline")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------------
# Environment variables
# --------------------------------------------------------------------------
API_KEY = os.environ.get("STOCK_API_KEY", "")
API_PROVIDER = os.environ.get("STOCK_API_PROVIDER", "alpha_vantage")
SYMBOLS = [s.strip().upper() for s in os.environ.get("STOCK_SYMBOLS", "AAPL").split(",") if s.strip()]

DB_HOST = os.environ.get("STOCK_DB_HOST", "postgres")
DB_PORT = os.environ.get("STOCK_DB_PORT", "5432")
DB_NAME = os.environ.get("STOCK_DB_NAME", "stock_data")
DB_USER = os.environ.get("STOCK_DB_USER", "stock_user")
DB_PASSWORD = os.environ.get("STOCK_DB_PASSWORD", "")

ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"
REQUEST_TIMEOUT_SECONDS = 30


class StockAPIError(Exception):
    """Raised when the upstream API returns an error, a rate-limit notice,
    or a payload that doesn't contain usable data."""


class NoDataError(Exception):
    """Raised when the API call succeeds but there is no data to persist
    (e.g. market holiday, empty symbol response). Not treated as a hard
    failure -- the pipeline logs it and moves on to the next symbol."""


@dataclass
class DailyBar:
    symbol: str
    trade_date: str  # ISO date, e.g. "2026-09-09"
    open_price: float | None
    high_price: float | None
    low_price: float | None
    close_price: float | None
    volume: int | None


def get_engine() -> Engine:
    """Builds a SQLAlchemy engine for the stock_data Postgres database.
    Uses a small connection pool with pre-ping so stale connections
    (e.g. after Postgres restarts) don't silently break the DAG."""
    if not DB_PASSWORD:
        raise StockAPIError("STOCK_DB_PASSWORD is not set - refusing to connect with an empty credential")

    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=2)


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((requests.exceptions.RequestException, StockAPIError)),
)
def fetch_raw_data(symbol: str) -> dict[str, Any]:
    """
    Calls the Alpha Vantage TIME_SERIES_DAILY endpoint for a single symbol.
    Retries transient network errors with exponential backoff (3 attempts).
    Raises StockAPIError immediately for non-retryable problems (bad API
    key, invalid symbol, rate limit) so callers can handle them explicitly.
    """
    if not API_KEY:
        raise StockAPIError("STOCK_API_KEY is not set")

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
        "outputsize": "compact",  # last ~100 trading days
    }

    logger.info("Fetching data for symbol=%s from %s", symbol, API_PROVIDER)
    response = requests.get(ALPHA_VANTAGE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise StockAPIError(f"Response for {symbol} was not valid JSON: {exc}") from exc

    # Alpha Vantage returns HTTP 200 even for errors/rate limits - the error
    # is embedded in the JSON body, so we have to check these keys explicitly.
    if "Error Message" in payload:
        raise StockAPIError(f"API error for {symbol}: {payload['Error Message']}")
    if "Note" in payload:
        # Typically a rate-limit notice ("Thank you for using Alpha Vantage...")
        raise StockAPIError(f"Rate limited while fetching {symbol}: {payload['Note']}")
    if "Information" in payload:
        raise StockAPIError(f"API info/limit message for {symbol}: {payload['Information']}")

    return payload


def parse_daily_series(symbol: str, payload: dict[str, Any]) -> list[DailyBar]:
    """
    Extracts the "Time Series (Daily)" block from the raw API payload and
    converts it into a list of DailyBar records. Missing or malformed
    individual fields are handled gracefully -- a bad field becomes None
    rather than crashing the whole parse, and a row with no usable prices
    at all is skipped and logged.
    """
    series = payload.get("Time Series (Daily)")
    if not series:
        raise NoDataError(f"No 'Time Series (Daily)' block in response for {symbol}")

    def _safe_float(value: str | None) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _safe_int(value: str | None) -> int | None:
        if value is None:
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    bars: list[DailyBar] = []
    skipped = 0
    for trade_date, fields in series.items():
        open_p = _safe_float(fields.get("1. open"))
        high_p = _safe_float(fields.get("2. high"))
        low_p = _safe_float(fields.get("3. low"))
        close_p = _safe_float(fields.get("4. close"))
        volume = _safe_int(fields.get("5. volume"))

        if open_p is None and high_p is None and low_p is None and close_p is None:
            # Every price field was missing/unparseable - nothing worth storing.
            skipped += 1
            continue

        bars.append(
            DailyBar(
                symbol=symbol,
                trade_date=trade_date,
                open_price=open_p,
                high_price=high_p,
                low_price=low_p,
                close_price=close_p,
                volume=volume,
            )
        )

    if skipped:
        logger.warning("symbol=%s: skipped %d day(s) with no usable price fields", symbol, skipped)
    if not bars:
        raise NoDataError(f"symbol={symbol}: parsed 0 usable rows out of {len(series)} entries")

    logger.info("symbol=%s: parsed %d daily bars", symbol, len(bars))
    return bars


def upsert_bars(engine: Engine, bars: list[DailyBar]) -> int:
    """
    Upserts a list of DailyBar rows into stock_prices using
    INSERT ... ON CONFLICT (symbol, trade_date) DO UPDATE, so re-running the
    pipeline for a date range already stored just refreshes the values
    instead of failing on a duplicate-key error or creating duplicate rows.
    Runs inside a single transaction per symbol so a partial failure does
    not leave half a symbol's data written.
    """
    if not bars:
        return 0

    upsert_sql = text(
        """
        INSERT INTO stock_prices
            (symbol, trade_date, open_price, high_price, low_price, close_price, volume, source, fetched_at)
        VALUES
            (:symbol, :trade_date, :open_price, :high_price, :low_price, :close_price, :volume, :source, :fetched_at)
        ON CONFLICT (symbol, trade_date)
        DO UPDATE SET
            open_price  = EXCLUDED.open_price,
            high_price  = EXCLUDED.high_price,
            low_price   = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume      = EXCLUDED.volume,
            source      = EXCLUDED.source,
            fetched_at  = EXCLUDED.fetched_at,
            updated_at  = NOW();
        """
    )

    fetched_at = datetime.now(timezone.utc)
    rows = [
        {
            "symbol": bar.symbol,
            "trade_date": bar.trade_date,
            "open_price": bar.open_price,
            "high_price": bar.high_price,
            "low_price": bar.low_price,
            "close_price": bar.close_price,
            "volume": bar.volume,
            "source": API_PROVIDER,
            "fetched_at": fetched_at,
        }
        for bar in bars
    ]

    with engine.begin() as conn:
        conn.execute(upsert_sql, rows)

    return len(rows)


def log_run(engine: Engine, run_id: str, symbol: str, status: str, rows_processed: int, error_message: str | None) -> None:
    """Writes one row to pipeline_run_log per symbol per run. Failures to
    write the log itself are swallowed (logged, not raised) so a logging
    problem never masks or replaces the real pipeline result."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO pipeline_run_log
                        (run_id, symbol, status, rows_processed, error_message, finished_at)
                    VALUES
                        (:run_id, :symbol, :status, :rows_processed, :error_message, NOW());
                    """
                ),
                {
                    "run_id": run_id,
                    "symbol": symbol,
                    "status": status,
                    "rows_processed": rows_processed,
                    "error_message": error_message,
                },
            )
    except Exception:
        logger.exception("Failed to write pipeline_run_log entry for symbol=%s (non-fatal)", symbol)


def process_symbol(engine: Engine, symbol: str, run_id: str) -> dict[str, Any]:
    """
    Runs the full fetch -> parse -> store flow for a single symbol and
    returns a small result dict. Any error is caught here so that one bad
    symbol (invalid ticker, rate limit, network blip) never stops the rest
    of the batch from being processed - this is the core "handle missing
    data / errors gracefully" requirement.
    """
    try:
        payload = fetch_raw_data(symbol)
        bars = parse_daily_series(symbol, payload)
        rows_written = upsert_bars(engine, bars)
        log_run(engine, run_id, symbol, "SUCCESS", rows_written, None)
        logger.info("symbol=%s: SUCCESS (%d rows upserted)", symbol, rows_written)
        return {"symbol": symbol, "status": "SUCCESS", "rows_written": rows_written}

    except NoDataError as exc:
        logger.warning("symbol=%s: NO_DATA (%s)", symbol, exc)
        log_run(engine, run_id, symbol, "NO_DATA", 0, str(exc))
        return {"symbol": symbol, "status": "NO_DATA", "rows_written": 0, "error": str(exc)}

    except (StockAPIError, requests.exceptions.RequestException) as exc:
        logger.error("symbol=%s: FAILED (%s)", symbol, exc)
        log_run(engine, run_id, symbol, "FAILED", 0, str(exc))
        return {"symbol": symbol, "status": "FAILED", "rows_written": 0, "error": str(exc)}

    except Exception as exc:  # noqa: BLE001 - last-resort catch-all, logged with full trace
        logger.exception("symbol=%s: FAILED with unexpected error", symbol)
        log_run(engine, run_id, symbol, "FAILED", 0, f"Unexpected error: {exc}")
        return {"symbol": symbol, "status": "FAILED", "rows_written": 0, "error": str(exc)}


def run_pipeline(symbols: list[str] | None = None, run_id: str | None = None) -> dict[str, Any]:
    """
    Entry point used by both the Airflow task and standalone execution.
    Iterates over every configured symbol, isolating failures per symbol,
    and returns a summary. Raises only if EVERY symbol failed, so a
    single-symbol outage doesn't fail the whole Airflow task -- but a
    total API outage still surfaces as a task failure for alerting/retries.
    """
    symbols = symbols or SYMBOLS
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    engine = get_engine()

    results = [process_symbol(engine, symbol, run_id) for symbol in symbols]

    succeeded = [r for r in results if r["status"] == "SUCCESS"]
    failed = [r for r in results if r["status"] == "FAILED"]
    no_data = [r for r in results if r["status"] == "NO_DATA"]

    summary = {
        "run_id": run_id,
        "total_symbols": len(symbols),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "no_data": len(no_data),
        "results": results,
    }
    logger.info("Pipeline run %s summary: %s", run_id, summary)

    if symbols and not succeeded and not no_data:
        # Every single symbol hit a hard error - almost certainly an
        # API-key/network/outage problem, not a per-symbol data issue.
        # Fail loudly so Airflow marks the task failed and retries/alerts.
        raise StockAPIError(f"All {len(symbols)} symbol(s) failed in run {run_id}: {failed}")

    return summary


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    result = run_pipeline()
    print(json.dumps(result, indent=2, default=str))
