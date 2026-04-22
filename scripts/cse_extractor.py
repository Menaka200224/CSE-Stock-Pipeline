"""
CSE Stock Market Data Extractor
================================
Fetches live data from the Colombo Stock Exchange (CSE) unofficial API
at https://www.cse.lk/api/ and stores it in PostgreSQL.

Endpoints used:
  - dailyMarketSummery  : ASPI, S&P SL20, turnover
  - todaySharePrice     : all listed stock prices
  - allSectors          : sector-level indices
  - tradeSummary        : top movers (gainers/losers)
"""

import requests
import psycopg2
import logging
from datetime import date, datetime

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
CSE_BASE_URL = "https://www.cse.lk/api/"

DB_CONFIG = {
    "host":     "postgres",   # Docker service name; use "localhost" outside Docker
    "port":     5432,
    "dbname":   "airflow",
    "user":     "airflow",
    "password": "airflow",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CSE-DataPipeline/1.0)",
    "Accept":     "application/json",
}

REQUEST_TIMEOUT = 15  # seconds


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get(endpoint: str, payload: dict = None) -> dict | list | None:
    """POST to a CSE API endpoint and return parsed JSON."""
    url = CSE_BASE_URL + endpoint
    try:
        resp = requests.post(url, data=payload or {}, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        log.error("Timeout hitting %s", url)
    except requests.exceptions.HTTPError as exc:
        log.error("HTTP %s from %s: %s", exc.response.status_code, url, exc)
    except requests.exceptions.ConnectionError as exc:
        log.error("Connection error for %s: %s", url, exc)
    except ValueError:
        log.error("Non-JSON response from %s", url)
    return None


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    try:
        return int(float(val)) if val is not None else None
    except (ValueError, TypeError):
        return None


# ── Extractors ────────────────────────────────────────────────────────────────

def extract_daily_summary() -> dict | None:
    """Fetch ASPI, S&P SL20, turnover, advance/decline counts."""
    log.info("Fetching daily market summary …")
    data = _get("dailyMarketSummery")
    if not data:
        return None

    # The API nests data differently depending on the trading session;
    # handle both wrapped and bare responses.
    summary = data.get("reqDailyMarketSummery") or (data if isinstance(data, dict) else None)
    if not summary:
        log.warning("Unexpected dailyMarketSummery shape: %s", list(data.keys()))
        return None

    return {
        "trade_date":      date.today().isoformat(),
        "aspi":            _safe_float(summary.get("aspi")),
        "aspi_change":     _safe_float(summary.get("aspiChange")),
        "aspi_change_pct": _safe_float(summary.get("aspiChangePercentage")),
        "sp_sl20":         _safe_float(summary.get("sp20")),
        "sp_sl20_change":  _safe_float(summary.get("sp20Change")),
        "total_turnover":  _safe_int(summary.get("turnover")),
        "total_volume":    _safe_int(summary.get("volume")),
        "total_trades":    _safe_int(summary.get("trades")),
        "advances":        _safe_int(summary.get("advances")),
        "declines":        _safe_int(summary.get("declines")),
        "unchanged":       _safe_int(summary.get("unchanged")),
    }


def extract_stock_prices() -> list[dict]:
    """Fetch today's price for every listed security."""
    log.info("Fetching today's share prices …")
    data = _get("todaySharePrice")
    if not data:
        return []

    records = []
    items = data if isinstance(data, list) else data.get("reqTodaySharePrice", [])
    today = date.today().isoformat()

    for item in items:
        records.append({
            "trade_date":         today,
            "symbol":             item.get("symbol", ""),
            "company_name":       item.get("name", ""),
            "last_traded_price":  _safe_float(item.get("lastTradedPrice")),
            "previous_close":     _safe_float(item.get("previousClose")),
            "price_change":       _safe_float(item.get("change")),
            "change_pct":         _safe_float(item.get("changePercentage")),
            "open_price":         _safe_float(item.get("openPrice")),
            "high_price":         _safe_float(item.get("highPrice")),
            "low_price":          _safe_float(item.get("lowPrice")),
            "volume":             _safe_int(item.get("volume")),
            "turnover":           _safe_int(item.get("turnover")),
            "market_cap":         _safe_int(item.get("marketCap")),
            "sector":             item.get("sector", ""),
        })

    log.info("  → %d stocks fetched", len(records))
    return records


def extract_sector_data() -> list[dict]:
    """Fetch sector-level index values."""
    log.info("Fetching sector data …")
    data = _get("allSectors")
    if not data:
        return []

    records = []
    items = data if isinstance(data, list) else data.get("reqAllSectors", [])
    today = date.today().isoformat()

    for item in items:
        records.append({
            "trade_date":         today,
            "sector_name":        item.get("sectorName", ""),
            "sector_index":       _safe_float(item.get("sectorIndex")),
            "sector_change":      _safe_float(item.get("sectorChange")),
            "sector_change_pct":  _safe_float(item.get("sectorChangePercentage")),
            "turnover":           _safe_int(item.get("turnover")),
        })

    log.info("  → %d sectors fetched", len(records))
    return records


def extract_top_movers(stock_records: list[dict], top_n: int = 10) -> list[dict]:
    """Derive top gainers and losers from the stock prices already fetched."""
    if not stock_records:
        return []

    valid = [s for s in stock_records if s.get("change_pct") is not None]
    sorted_stocks = sorted(valid, key=lambda x: x["change_pct"], reverse=True)

    today = date.today().isoformat()
    movers = []

    for rank, stock in enumerate(sorted_stocks[:top_n], start=1):
        movers.append({
            "trade_date":    today,
            "category":      "gainer",
            "rank_position": rank,
            "symbol":        stock["symbol"],
            "company_name":  stock["company_name"],
            "price":         stock["last_traded_price"],
            "change_pct":    stock["change_pct"],
            "volume":        stock["volume"],
        })

    for rank, stock in enumerate(reversed(sorted_stocks[-top_n:]), start=1):
        movers.append({
            "trade_date":    today,
            "category":      "loser",
            "rank_position": rank,
            "symbol":        stock["symbol"],
            "company_name":  stock["company_name"],
            "price":         stock["last_traded_price"],
            "change_pct":    stock["change_pct"],
            "volume":        stock["volume"],
        })

    log.info("  → %d top movers derived", len(movers))
    return movers


# ── Database loader ────────────────────────────────────────────────────────────

def load_to_db(summary: dict, stocks: list, sectors: list, movers: list) -> None:
    """Insert all extracted records into PostgreSQL."""
    log.info("Connecting to PostgreSQL …")
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    now  = datetime.utcnow()

    try:
        # Daily summary
        if summary:
            cur.execute(
                """
                INSERT INTO cse_daily_summary
                    (extracted_at, trade_date, aspi, aspi_change, aspi_change_pct,
                     sp_sl20, sp_sl20_change, total_turnover, total_volume,
                     total_trades, advances, declines, unchanged)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (now, summary["trade_date"], summary["aspi"], summary["aspi_change"],
                 summary["aspi_change_pct"], summary["sp_sl20"], summary["sp_sl20_change"],
                 summary["total_turnover"], summary["total_volume"], summary["total_trades"],
                 summary["advances"], summary["declines"], summary["unchanged"]),
            )
            log.info("Inserted daily summary row")

        # Stock prices (batch insert)
        if stocks:
            cur.executemany(
                """
                INSERT INTO cse_stock_prices
                    (extracted_at, trade_date, symbol, company_name, last_traded_price,
                     previous_close, price_change, change_pct, open_price, high_price,
                     low_price, volume, turnover, market_cap, sector)
                VALUES
                    (%(extracted_at)s, %(trade_date)s, %(symbol)s, %(company_name)s,
                     %(last_traded_price)s, %(previous_close)s, %(price_change)s,
                     %(change_pct)s, %(open_price)s, %(high_price)s, %(low_price)s,
                     %(volume)s, %(turnover)s, %(market_cap)s, %(sector)s)
                """,
                [{**s, "extracted_at": now} for s in stocks],
            )
            log.info("Inserted %d stock price rows", len(stocks))

        # Sectors
        if sectors:
            cur.executemany(
                """
                INSERT INTO cse_sector_data
                    (extracted_at, trade_date, sector_name, sector_index,
                     sector_change, sector_change_pct, turnover)
                VALUES
                    (%(extracted_at)s, %(trade_date)s, %(sector_name)s, %(sector_index)s,
                     %(sector_change)s, %(sector_change_pct)s, %(turnover)s)
                """,
                [{**s, "extracted_at": now} for s in sectors],
            )
            log.info("Inserted %d sector rows", len(sectors))

        # Top movers
        if movers:
            cur.executemany(
                """
                INSERT INTO cse_top_gainers_losers
                    (extracted_at, trade_date, category, rank_position, symbol,
                     company_name, price, change_pct, volume)
                VALUES
                    (%(extracted_at)s, %(trade_date)s, %(category)s, %(rank_position)s,
                     %(symbol)s, %(company_name)s, %(price)s, %(change_pct)s, %(volume)s)
                """,
                [{**m, "extracted_at": now} for m in movers],
            )
            log.info("Inserted %d top movers rows", len(movers))

        conn.commit()
        log.info("All data committed successfully ✓")

    except Exception as exc:
        conn.rollback()
        log.error("DB error — rolled back: %s", exc)
        raise
    finally:
        cur.close()
        conn.close()


# ── Entry point ────────────────────────────────────────────────────────────────

def run_extraction():
    """Full pipeline: extract → transform → load."""
    log.info("=== CSE Data Extraction Pipeline START ===")

    summary = extract_daily_summary()
    stocks  = extract_stock_prices()
    sectors = extract_sector_data()
    movers  = extract_top_movers(stocks)

    if not any([summary, stocks, sectors]):
        log.warning("No data extracted — CSE market may be closed today")
        return

    load_to_db(summary, stocks, sectors, movers)
    log.info("=== CSE Data Extraction Pipeline END ===")


if __name__ == "__main__":
    run_extraction()