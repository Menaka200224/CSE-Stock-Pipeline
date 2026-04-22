"""
CSE Stock Market Pipeline DAG
==============================
Runs daily at 06:00 UTC (11:30 AM Sri Lanka time, just after CSE market close).
The pipeline has four tasks:
  1. health_check     — verify the CSE API is reachable
  2. extract_and_load — run the full extraction script
  3. validate_data    — check row counts are sensible
  4. log_summary      — print a summary for the Airflow logs
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "cse-pipeline",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
}

# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="cse_stock_market_pipeline",
    default_args=default_args,
    description="Daily extraction of Colombo Stock Exchange data",
    schedule_interval="0 6 * * 1-5",   # Weekdays at 06:00 UTC (Mon–Fri)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cse", "stock-market", "sri-lanka", "devops"],
) as dag:

    # ── Task 1: Health check ───────────────────────────────────────────────────
    def health_check(**context):
        """Ping the CSE API to confirm it is up before running the pipeline."""
        log = logging.getLogger(__name__)
        url = "https://www.cse.lk/api/dailyMarketSummery"
        try:
            resp = requests.post(url, timeout=10)
            resp.raise_for_status()
            log.info("CSE API health check PASSED (HTTP %s)", resp.status_code)
        except Exception as exc:
            raise RuntimeError(f"CSE API unreachable: {exc}") from exc


    t1_health = PythonOperator(
        task_id="health_check",
        python_callable=health_check,
    )

    # ── Task 2: Extract and load ───────────────────────────────────────────────
    def extract_and_load(**context):
        """Import and run the CSE extractor module."""
        import sys
        sys.path.insert(0, "/opt/airflow/scripts")
        from cse_extractor import run_extraction
        run_extraction()


    t2_extract = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
    )

    # ── Task 3: Validate data ──────────────────────────────────────────────────
    def validate_data(**context):
        """Check that today's rows were actually inserted."""
        log = logging.getLogger(__name__)
        conn = psycopg2.connect(
            host="postgres", port=5432,
            dbname="airflow", user="airflow", password="airflow",
        )
        cur = conn.cursor()

        checks = {
            "cse_daily_summary":      "SELECT COUNT(*) FROM cse_daily_summary     WHERE trade_date = CURRENT_DATE",
            "cse_stock_prices":       "SELECT COUNT(*) FROM cse_stock_prices       WHERE trade_date = CURRENT_DATE",
            "cse_sector_data":        "SELECT COUNT(*) FROM cse_sector_data        WHERE trade_date = CURRENT_DATE",
            "cse_top_gainers_losers": "SELECT COUNT(*) FROM cse_top_gainers_losers WHERE trade_date = CURRENT_DATE",
        }

        failures = []
        for table, query in checks.items():
            cur.execute(query)
            count = cur.fetchone()[0]
            log.info("  %s: %d rows for today", table, count)
            if count == 0:
                failures.append(table)

        cur.close()
        conn.close()

        if failures:
            raise ValueError(f"Validation FAILED — no rows for today in: {failures}")
        log.info("Data validation PASSED ✓")


    t3_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    # ── Task 4: Log summary ────────────────────────────────────────────────────
    def log_summary(**context):
        """Pull today's summary row and log it for easy monitoring."""
        log = logging.getLogger(__name__)
        conn = psycopg2.connect(
            host="postgres", port=5432,
            dbname="airflow", user="airflow", password="airflow",
        )
        cur = conn.cursor()
        cur.execute(
            """
            SELECT aspi, aspi_change_pct, sp_sl20, total_turnover, advances, declines
            FROM   cse_daily_summary
            WHERE  trade_date = CURRENT_DATE
            ORDER  BY extracted_at DESC
            LIMIT  1
            """
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row:
            aspi, aspi_pct, sp20, turnover, adv, dec = row
            log.info(
                "CSE Summary — ASPI: %.2f (%+.2f%%) | S&P SL20: %.2f | "
                "Turnover: LKR %s | Advances: %s | Declines: %s",
                aspi or 0, aspi_pct or 0, sp20 or 0,
                f"{turnover:,}" if turnover else "N/A",
                adv or 0, dec or 0,
            )
        else:
            log.warning("No summary row found for today")


    t4_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
    )

    # ── Task dependencies ──────────────────────────────────────────────────────
    t1_health >> t2_extract >> t3_validate >> t4_summary