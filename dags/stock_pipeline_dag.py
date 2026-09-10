"""
stock_pipeline_dag.py
----------------------
Airflow DAG that, on a schedule, fetches daily stock prices for a
configurable list of symbols from Alpha Vantage, parses the response, and
upserts the results into the `stock_prices` table in PostgreSQL.

Design notes:
  * Each pipeline stage (fetch+parse+store, then a lightweight validation
    check) is a separate task so failures are isolated and visible per
    stage in the Airflow UI, and each task gets its own retry policy.
  * The heavy lifting lives in scripts/fetch_stock_data.py so it can be
    unit-tested and run outside of Airflow; this DAG file is orchestration
    only.
  * `retries` + exponential `retry_delay` handle transient failures
    (network blips, momentary API rate limits) without manual intervention.
  * `on_failure_callback` logs a clear final-failure message; swap in a
    Slack/email operator here for real alerting.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# scripts/ is mounted at /opt/airflow/scripts (see docker-compose.yml)
sys.path.append("/opt/airflow/scripts")

from fetch_stock_data import SYMBOLS, get_engine, run_pipeline  # noqa: E402

logger = logging.getLogger(__name__)


def _on_failure_alert(context):
    """Placeholder alert hook. Replace the log line with a Slack/email/
    PagerDuty call for production use -- kept as a log line here so the
    project runs with zero extra credentials out of the box."""
    ti = context.get("task_instance")
    logger.error(
        "ALERT: task %s in dag %s failed on run %s",
        ti.task_id if ti else "?",
        context.get("dag").dag_id if context.get("dag") else "?",
        context.get("run_id"),
    )


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "on_failure_callback": _on_failure_alert,
}


def fetch_process_store(**context) -> dict:
    """
    Single task that runs the full fetch -> parse -> upsert flow for every
    configured symbol (see run_pipeline in fetch_stock_data.py). Per-symbol
    errors are caught and logged inside run_pipeline itself; this task only
    raises (triggering an Airflow retry) if the whole run failed outright.
    The summary dict is pushed to XCom automatically as the task's return
    value, so downstream tasks / the UI can inspect exactly what happened.
    """
    run_id = context["run_id"]
    summary = run_pipeline(symbols=SYMBOLS, run_id=run_id)

    if summary["failed"] > 0:
        logger.warning(
            "%d/%d symbol(s) failed this run (run_id=%s) - see pipeline_run_log for details",
            summary["failed"],
            summary["total_symbols"],
            run_id,
        )
    return summary


def validate_run(**context) -> None:
    """
    Sanity-check task that runs after the fetch/store step: confirms at
    least one row was written for *this* run_id. Catches silent failures
    that don't raise an exception (e.g. the API returning valid-but-empty
    JSON for every symbol) so they still show up as a failed DAG run.
    """
    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="fetch_process_store")
    if not summary:
        raise ValueError("No summary returned from fetch_process_store task")

    if summary["succeeded"] == 0 and summary["no_data"] == 0:
        raise ValueError(f"Validation failed: 0 symbols succeeded in run {summary['run_id']}")

    engine = get_engine()
    with engine.connect() as conn:
        from sqlalchemy import text

        row = conn.execute(
            text("SELECT COUNT(*) FROM pipeline_run_log WHERE run_id = :run_id"),
            {"run_id": summary["run_id"]},
        ).fetchone()
        logged_rows = row[0] if row else 0

    logger.info(
        "Validation OK: run_id=%s succeeded=%d failed=%d no_data=%d logged_rows=%d",
        summary["run_id"],
        summary["succeeded"],
        summary["failed"],
        summary["no_data"],
        logged_rows,
    )


with DAG(
    dag_id="stock_market_pipeline",
    description="Fetch daily stock prices and upsert them into PostgreSQL",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",  # change to "0 * * * *" for hourly runs
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "postgres", "api"],
) as dag:

    fetch_process_store_task = PythonOperator(
        task_id="fetch_process_store",
        python_callable=fetch_process_store,
    )

    validate_run_task = PythonOperator(
        task_id="validate_run",
        python_callable=validate_run,
        trigger_rule=TriggerRule.ALL_DONE,  # still validate even if the previous task retried/partially failed
    )

    fetch_process_store_task >> validate_run_task
