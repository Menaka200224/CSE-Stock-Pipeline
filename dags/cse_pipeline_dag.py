"""
CSE Stock Market Pipeline DAG
==============================
Runs daily at 06:00 UTC (11:30 AM Sri Lanka time, just after CSE market close).
Data source: Yahoo Finance (.CM suffix for CSE-listed stocks)

Tasks:
  1. health_check     — verify Yahoo Finance API is reachable
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

default_args = {
    "owner":            "cse-pipeline",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
}

with DAG(
    dag_id="cse_stock_market_pipeline",
    default_args=default_args,
    description="Daily extraction of Colombo Stock Exchange data via Yahoo Finance",
    schedule_interval="0 6 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cse", "stock-market", "sri-lanka", "devops"],
) as dag:

    #Health check
    def health_check(**context):
        """Ping Yahoo Finance to confirm connectivity before running pipeline."""
        log = logging.getLogger(__name__)
        url = "https://query1.finance.yahoo.com/v8/finance/chart/JKH.CM?interval=1d&range=1d"
        try:
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            resp.raise_for_status()
            data   = resp.json()
            result = data.get("chart", {}).get("result", [])
            if result:
                log.info("Yahoo Finance health check PASSED — JKH.CM data received")
            else:
                raise RuntimeError("Yahoo Finance returned empty result")
        except Exception as exc:
            raise RuntimeError(f"Yahoo Finance unreachable: {exc}") from exc

    t1_health = PythonOperator(
        task_id="health_check",
        python_callable=health_check,
    )

    # Extract and load
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

    #Validate data
    def validate_data(**context):
        """Check that today's rows were actually inserted."""
        log  = logging.getLogger(__name__)
        conn = psycopg2.connect(
            host="postgres", port=5432,
            dbname="airflow", user="airflow", password="airflow",
        )
        cur = conn.cursor()

        # Only check tables that Yahoo Finance populates
        checks = {
            "cse_daily_summary":      "SELECT COUNT(*) FROM cse_daily_summary      WHERE trade_date = CURRENT_DATE",
            "cse_stock_prices":       "SELECT COUNT(*) FROM cse_stock_prices        WHERE trade_date = CURRENT_DATE",
            "cse_top_gainers_losers": "SELECT COUNT(*) FROM cse_top_gainers_losers  WHERE trade_date = CURRENT_DATE",
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

    # Log summary
    def log_summary(**context):
        """Pull today's summary and log it."""
        log  = logging.getLogger(__name__)
        conn = psycopg2.connect(
            host="postgres", port=5432,
            dbname="airflow", user="airflow", password="airflow",
        )
        cur = conn.cursor()

        # Market summary
        cur.execute("""
            SELECT total_trades, aspi_change_pct, total_turnover, advances, declines
            FROM   cse_daily_summary
            WHERE  trade_date = CURRENT_DATE
            ORDER  BY extracted_at DESC
            LIMIT  1
        """)
        row = cur.fetchone()

        # Top gainer and loser
        cur.execute("""
            SELECT symbol, company_name, change_pct
            FROM   cse_top_gainers_losers
            WHERE  trade_date = CURRENT_DATE AND category = 'gainer' AND rank_position = 1
        """)
        gainer = cur.fetchone()

        cur.execute("""
            SELECT symbol, company_name, change_pct
            FROM   cse_top_gainers_losers
            WHERE  trade_date = CURRENT_DATE AND category = 'loser' AND rank_position = 1
        """)
        loser = cur.fetchone()

        cur.close()
        conn.close()

        if row:
            trades, avg_chg, turnover, adv, dec = row
            log.info("=== CSE Daily Summary ===")
            log.info("Stocks tracked  : %s", trades)
            log.info("Avg change      : %+.2f%%", avg_chg or 0)
            log.info("Total turnover  : LKR %s", f"{turnover:,}" if turnover else "N/A")
            log.info("Advances        : %s | Declines: %s", adv, dec)
            if gainer:
                log.info("Top gainer      : %s (%s) %+.2f%%", gainer[1], gainer[0], gainer[2])
            if loser:
                log.info("Top loser       : %s (%s) %+.2f%%", loser[1], loser[0], loser[2])
        else:
            log.warning("No summary row found for today")

    t4_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
    )

    #Task dependencies
    t1_health >> t2_extract >> t3_validate >> t4_summary
