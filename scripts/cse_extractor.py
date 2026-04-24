"""
CSE Stock Market Data Extractor
================================
Fetches Sri Lanka related financial data via Alpha Vantage API.

Data collected:
  1. Daily stock prices for Sri Lanka related / CSE-linked companies
  2. USD/LKR exchange rate (critical Sri Lanka economic indicator)
  3. Derived market summary (advances, declines, avg change)
  4. Top gainers and losers

Alpha Vantage free tier: 25 requests/day, max 5/minute
We add 12 second delays between calls to stay within limits.
"""

import requests
import psycopg2
import logging
import time
from datetime import date, datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

API_KEY         = "QTLFVAIKTZOH1WRW"
AV_BASE         = "https://www.alphavantage.co/query"
REQUEST_TIMEOUT = 15
RATE_LIMIT_WAIT = 12

DB_CONFIG = {
    "host": "postgres", "port": 5432,
    "dbname": "airflow", "user": "airflow", "password": "airflow",
}

CSE_STOCKS = [
    {"symbol": "EXPO",  "company": "Expolanka Holdings (Global)",      "sector": "Logistics"},
    {"symbol": "HDB",   "company": "HDFC Bank (Regional Banking)",     "sector": "Banking"},
    {"symbol": "TEA",   "company": "iPath Bloomberg Softs (Tea/Agri)", "sector": "Agriculture"},
    {"symbol": "STER",  "company": "Sterling Infrastructure",          "sector": "Infrastructure"},
    {"symbol": "CEYL",  "company": "Ceylon Graphite Corp",             "sector": "Mining"},
]

def _safe_float(val):
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None

def _safe_int(val):
    try:
        return int(float(val)) if val is not None else None
    except (ValueError, TypeError):
        return None

def _av_get(params):
    params["apikey"] = API_KEY
    try:
        resp = requests.get(AV_BASE, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if "Information" in data:
            log.warning("Rate limit hit — waiting 60 seconds ...")
            time.sleep(60)
            resp = requests.get(AV_BASE, params=params, timeout=REQUEST_TIMEOUT)
            data = resp.json()
        return data
    except requests.exceptions.Timeout:
        log.error("Timeout calling Alpha Vantage")
    except requests.exceptions.HTTPError as e:
        log.error("HTTP error: %s", e)
    except requests.exceptions.ConnectionError as e:
        log.error("Connection error: %s", e)
    except ValueError as e:
        log.error("JSON parse error: %s", e)
    return None

def fetch_global_quote(symbol):
    data = _av_get({"function": "GLOBAL_QUOTE", "symbol": symbol})
    if not data:
        return None
    quote = data.get("Global Quote", {})
    if not quote or not quote.get("05. price"):
        log.warning("No quote data for %s", symbol)
        return None
    return {
        "last_traded_price": _safe_float(quote.get("05. price")),
        "previous_close":    _safe_float(quote.get("08. previous close")),
        "price_change":      _safe_float(quote.get("09. change")),
        "change_pct":        _safe_float(quote.get("10. change percent", "0%").replace("%", "")),
        "open_price":        _safe_float(quote.get("02. open")),
        "high_price":        _safe_float(quote.get("03. high")),
        "low_price":         _safe_float(quote.get("04. low")),
        "volume":            _safe_int(quote.get("06. volume")),
        "market_cap":        None,
    }

def fetch_usd_lkr_rate():
    log.info("Fetching USD/LKR exchange rate ...")
    data = _av_get({"function": "CURRENCY_EXCHANGE_RATE", "from_currency": "USD", "to_currency": "LKR"})
    if not data:
        return None
    rate_data = data.get("Realtime Currency Exchange Rate", {})
    if not rate_data:
        return None
    rate = _safe_float(rate_data.get("5. Exchange Rate"))
    log.info("  USD/LKR rate: %.2f", rate or 0)
    return {
        "trade_date": date.today().isoformat(),
        "symbol": "USD/LKR", "company_name": "US Dollar to Sri Lankan Rupee",
        "sector": "Forex", "last_traded_price": rate,
        "previous_close": None, "price_change": None, "change_pct": None,
        "open_price": None, "high_price": _safe_float(rate_data.get("6. Ask Price")),
        "low_price": _safe_float(rate_data.get("5. Exchange Rate")),
        "volume": None, "market_cap": None, "turnover": None,
    }

def extract_stock_prices():
    log.info("Fetching stock prices via Alpha Vantage ...")
    records = []
    today   = date.today().isoformat()
    for i, stock in enumerate(CSE_STOCKS):
        log.info("  [%d/%d] Fetching %s ...", i+1, len(CSE_STOCKS), stock["symbol"])
        quote = fetch_global_quote(stock["symbol"])
        if quote:
            records.append({
                "trade_date": today, "symbol": stock["symbol"],
                "company_name": stock["company"], "sector": stock["sector"],
                "last_traded_price": quote["last_traded_price"],
                "previous_close": quote["previous_close"],
                "price_change": quote["price_change"],
                "change_pct": quote["change_pct"],
                "open_price": quote["open_price"],
                "high_price": quote["high_price"],
                "low_price": quote["low_price"],
                "volume": quote["volume"], "market_cap": quote["market_cap"],
                "turnover": _safe_int((quote["last_traded_price"] or 0) * (quote["volume"] or 0)),
            })
            log.info("    Price: %.2f | Change: %+.2f%%", quote["last_traded_price"] or 0, quote["change_pct"] or 0)
        else:
            log.warning("    No data for %s", stock["symbol"])
        if i < len(CSE_STOCKS) - 1:
            log.info("    Waiting %ds ...", RATE_LIMIT_WAIT)
            time.sleep(RATE_LIMIT_WAIT)
    log.info("Fetched %d/%d stocks", len(records), len(CSE_STOCKS))
    return records

def extract_market_summary(stocks, usd_lkr):
    if not stocks:
        return None
    valid     = [s for s in stocks if s.get("change_pct") is not None]
    advances  = sum(1 for s in valid if s["change_pct"] > 0)
    declines  = sum(1 for s in valid if s["change_pct"] < 0)
    unchanged = sum(1 for s in valid if s["change_pct"] == 0)
    turnover  = sum((s.get("last_traded_price") or 0) * (s.get("volume") or 0) for s in stocks)
    avg_chg   = sum(s["change_pct"] for s in valid) / len(valid) if valid else 0
    lkr_rate  = usd_lkr["last_traded_price"] if usd_lkr else None
    return {
        "trade_date": date.today().isoformat(),
        "aspi": lkr_rate, "aspi_change": None,
        "aspi_change_pct": round(avg_chg, 3),
        "sp_sl20": None, "sp_sl20_change": None,
        "total_turnover": _safe_int(turnover),
        "total_volume": _safe_int(sum(s.get("volume") or 0 for s in stocks)),
        "total_trades": len(stocks),
        "advances": advances, "declines": declines, "unchanged": unchanged,
    }

def extract_top_movers(stocks, top_n=3):
    if not stocks:
        return []
    valid   = [s for s in stocks if s.get("change_pct") is not None]
    sorted_ = sorted(valid, key=lambda x: x["change_pct"], reverse=True)
    today   = date.today().isoformat()
    movers  = []
    for rank, s in enumerate(sorted_[:top_n], 1):
        movers.append({"trade_date": today, "category": "gainer", "rank_position": rank,
                       "symbol": s["symbol"], "company_name": s["company_name"],
                       "price": s["last_traded_price"], "change_pct": s["change_pct"], "volume": s["volume"]})
    for rank, s in enumerate(reversed(sorted_[-top_n:]), 1):
        movers.append({"trade_date": today, "category": "loser", "rank_position": rank,
                       "symbol": s["symbol"], "company_name": s["company_name"],
                       "price": s["last_traded_price"], "change_pct": s["change_pct"], "volume": s["volume"]})
    return movers

def load_to_db(summary, stocks, usd_lkr, movers):
    log.info("Connecting to PostgreSQL ...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    now  = datetime.utcnow()
    try:
        if summary:
            cur.execute("""
                INSERT INTO cse_daily_summary
                    (extracted_at,trade_date,aspi,aspi_change,aspi_change_pct,
                     sp_sl20,sp_sl20_change,total_turnover,total_volume,
                     total_trades,advances,declines,unchanged)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (now, summary["trade_date"], summary["aspi"], summary["aspi_change"],
                  summary["aspi_change_pct"], summary["sp_sl20"], summary["sp_sl20_change"],
                  summary["total_turnover"], summary["total_volume"], summary["total_trades"],
                  summary["advances"], summary["declines"], summary["unchanged"]))

        all_records = stocks + ([usd_lkr] if usd_lkr else [])
        if all_records:
            cur.executemany("""
                INSERT INTO cse_stock_prices
                    (extracted_at,trade_date,symbol,company_name,last_traded_price,
                     previous_close,price_change,change_pct,open_price,high_price,
                     low_price,volume,turnover,market_cap,sector)
                VALUES
                    (%(extracted_at)s,%(trade_date)s,%(symbol)s,%(company_name)s,
                     %(last_traded_price)s,%(previous_close)s,%(price_change)s,%(change_pct)s,
                     %(open_price)s,%(high_price)s,%(low_price)s,%(volume)s,
                     %(turnover)s,%(market_cap)s,%(sector)s)
            """, [{**s, "extracted_at": now} for s in all_records])
            log.info("Inserted %d rows", len(all_records))

        if movers:
            cur.executemany("""
                INSERT INTO cse_top_gainers_losers
                    (extracted_at,trade_date,category,rank_position,
                     symbol,company_name,price,change_pct,volume)
                VALUES
                    (%(extracted_at)s,%(trade_date)s,%(category)s,%(rank_position)s,
                     %(symbol)s,%(company_name)s,%(price)s,%(change_pct)s,%(volume)s)
            """, [{**m, "extracted_at": now} for m in movers])

        conn.commit()
        log.info("All data committed successfully ✓")
    except Exception as exc:
        conn.rollback()
        log.error("DB error: %s", exc)
        raise
    finally:
        cur.close()
        conn.close()

def run_extraction():
    log.info("=== CSE Market Data Pipeline START ===")
    usd_lkr = fetch_usd_lkr_rate()
    time.sleep(RATE_LIMIT_WAIT)
    stocks  = extract_stock_prices()
    summary = extract_market_summary(stocks, usd_lkr)
    movers  = extract_top_movers(stocks)
    if not stocks and not usd_lkr:
        log.warning("No data extracted")
        return
    load_to_db(summary, stocks, usd_lkr, movers)
    log.info("Stocks: %d | Advances: %d | Declines: %d | USD/LKR: %.2f",
             len(stocks),
             summary["advances"] if summary else 0,
             summary["declines"] if summary else 0,
             usd_lkr["last_traded_price"] if usd_lkr else 0)
    log.info("=== Pipeline END ===")

if __name__ == "__main__":
    run_extraction()
