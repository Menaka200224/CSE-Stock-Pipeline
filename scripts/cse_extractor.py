"""
CSE Stock Market Data Extractor
================================
Fetches Colombo Stock Exchange (CSE) listed company data via Yahoo Finance API.
Yahoo Finance uses the .CM suffix for CSE-listed stocks.
"""

import requests
import psycopg2
import logging
from datetime import date, datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

YAHOO_BASE      = "https://query1.finance.yahoo.com"
YAHOO_HEADERS   = {"User-Agent": "Mozilla/5.0 (compatible; CSE-DataPipeline/1.0)"}
REQUEST_TIMEOUT = 15

CSE_STOCKS = [
    {"symbol": "JKH.CM",  "company": "John Keells Holdings PLC",      "sector": "Diversified"},
    {"symbol": "COMB.CM", "company": "Commercial Bank of Ceylon PLC",  "sector": "Banking"},
    {"symbol": "DIAL.CM", "company": "Dialog Axiata PLC",              "sector": "Telecom"},
    {"symbol": "SAMP.CM", "company": "Sampath Bank PLC",               "sector": "Banking"},
    {"symbol": "LOLC.CM", "company": "LOLC Holdings PLC",              "sector": "Diversified Finance"},
    {"symbol": "HNB.CM",  "company": "Hatton National Bank PLC",       "sector": "Banking"},
    {"symbol": "EXPO.CM", "company": "Expolanka Holdings PLC",         "sector": "Logistics"},
    {"symbol": "CTC.CM",  "company": "Ceylon Tobacco Company PLC",     "sector": "Consumer Goods"},
    {"symbol": "DIPD.CM", "company": "Dipped Products PLC",            "sector": "Manufacturing"},
    {"symbol": "GRAN.CM", "company": "Guardian Capital Partners PLC",  "sector": "Finance"},
]

DB_CONFIG = {
    "host": "postgres", "port": 5432,
    "dbname": "airflow", "user": "airflow", "password": "airflow",
}

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

def fetch_stock_quote(symbol):
    url = f"{YAHOO_BASE}/v8/finance/chart/{symbol}?interval=1d&range=5d"
    try:
        resp = requests.get(url, headers=YAHOO_HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data   = resp.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return None
        meta       = result[0].get("meta", {})
        indicators = result[0].get("indicators", {}).get("quote", [{}])[0]
        closes     = [c for c in indicators.get("close",  []) if c is not None]
        opens      = [c for c in indicators.get("open",   []) if c is not None]
        highs      = [c for c in indicators.get("high",   []) if c is not None]
        lows       = [c for c in indicators.get("low",    []) if c is not None]
        volumes    = [c for c in indicators.get("volume", []) if c is not None]
        if not closes:
            return None
        current = closes[-1]
        prev    = closes[-2] if len(closes) >= 2 else None
        change  = (current - prev)       if prev else None
        pct     = (change / prev * 100)  if prev else None
        return {
            "last_traded_price": _safe_float(current),
            "previous_close":    _safe_float(prev),
            "price_change":      _safe_float(change),
            "change_pct":        _safe_float(pct),
            "open_price":        _safe_float(opens[-1]   if opens   else None),
            "high_price":        _safe_float(highs[-1]   if highs   else None),
            "low_price":         _safe_float(lows[-1]    if lows    else None),
            "volume":            _safe_int(volumes[-1]   if volumes  else None),
            "market_cap":        _safe_int(meta.get("marketCap")),
        }
    except requests.exceptions.Timeout:
        log.error("Timeout fetching %s", symbol)
    except requests.exceptions.HTTPError as e:
        log.error("HTTP %s for %s", e.response.status_code, symbol)
    except requests.exceptions.ConnectionError as e:
        log.error("Connection error for %s: %s", symbol, e)
    except (ValueError, KeyError) as e:
        log.error("Parse error for %s: %s", symbol, e)
    return None

def extract_stock_prices():
    log.info("Fetching CSE stock prices via Yahoo Finance ...")
    records = []
    today   = date.today().isoformat()
    for stock in CSE_STOCKS:
        log.info("  Fetching %s ...", stock["symbol"])
        quote = fetch_stock_quote(stock["symbol"])
        if quote:
            records.append({
                "trade_date":        today,
                "symbol":            stock["symbol"],
                "company_name":      stock["company"],
                "sector":            stock["sector"],
                "last_traded_price": quote["last_traded_price"],
                "previous_close":    quote["previous_close"],
                "price_change":      quote["price_change"],
                "change_pct":        quote["change_pct"],
                "open_price":        quote["open_price"],
                "high_price":        quote["high_price"],
                "low_price":         quote["low_price"],
                "volume":            quote["volume"],
                "market_cap":        quote["market_cap"],
            })
            log.info("    Price: %.2f | Change: %+.2f%%", quote["last_traded_price"] or 0, quote["change_pct"] or 0)
        else:
            log.warning("    No data for %s", stock["symbol"])
    log.info("Fetched %d/%d stocks", len(records), len(CSE_STOCKS))
    return records

def extract_market_summary(stocks):
    if not stocks:
        return None
    valid     = [s for s in stocks if s.get("change_pct") is not None]
    advances  = sum(1 for s in valid if s["change_pct"] > 0)
    declines  = sum(1 for s in valid if s["change_pct"] < 0)
    unchanged = sum(1 for s in valid if s["change_pct"] == 0)
    turnover  = sum((s["last_traded_price"] or 0) * (s["volume"] or 0) for s in stocks)
    avg_chg   = sum(s["change_pct"] for s in valid) / len(valid) if valid else 0
    return {
        "trade_date": date.today().isoformat(),
        "aspi": None, "aspi_change": None,
        "aspi_change_pct": round(avg_chg, 3),
        "sp_sl20": None, "sp_sl20_change": None,
        "total_turnover": _safe_int(turnover),
        "total_volume":   _safe_int(sum(s.get("volume") or 0 for s in stocks)),
        "total_trades":   len(stocks),
        "advances":  advances,
        "declines":  declines,
        "unchanged": unchanged,
    }

def extract_top_movers(stocks, top_n=5):
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

def load_to_db(summary, stocks, movers):
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
            """, (now,summary["trade_date"],summary["aspi"],summary["aspi_change"],
                  summary["aspi_change_pct"],summary["sp_sl20"],summary["sp_sl20_change"],
                  summary["total_turnover"],summary["total_volume"],summary["total_trades"],
                  summary["advances"],summary["declines"],summary["unchanged"]))
        if stocks:
            cur.executemany("""
                INSERT INTO cse_stock_prices
                    (extracted_at,trade_date,symbol,company_name,last_traded_price,
                     previous_close,price_change,change_pct,open_price,high_price,
                     low_price,volume,turnover,market_cap,sector)
                VALUES
                    (%(extracted_at)s,%(trade_date)s,%(symbol)s,%(company_name)s,%(last_traded_price)s,
                     %(previous_close)s,%(price_change)s,%(change_pct)s,%(open_price)s,%(high_price)s,
                     %(low_price)s,%(volume)s,%(turnover)s,%(market_cap)s,%(sector)s)
            """, [{**s, "extracted_at": now,
                   "turnover": _safe_int((s.get("last_traded_price") or 0)*(s.get("volume") or 0))
                  } for s in stocks])
            log.info("Inserted %d stock rows", len(stocks))
        if movers:
            cur.executemany("""
                INSERT INTO cse_top_gainers_losers
                    (extracted_at,trade_date,category,rank_position,symbol,company_name,price,change_pct,volume)
                VALUES
                    (%(extracted_at)s,%(trade_date)s,%(category)s,%(rank_position)s,
                     %(symbol)s,%(company_name)s,%(price)s,%(change_pct)s,%(volume)s)
            """, [{**m, "extracted_at": now} for m in movers])
        conn.commit()
        log.info("All data committed successfully")
    except Exception as exc:
        conn.rollback()
        log.error("DB error: %s", exc)
        raise
    finally:
        cur.close()
        conn.close()

def run_extraction():
    log.info("=== CSE Data Extraction Pipeline START ===")
    stocks  = extract_stock_prices()
    summary = extract_market_summary(stocks)
    movers  = extract_top_movers(stocks)
    if not stocks:
        log.warning("No stock data extracted")
        return
    load_to_db(summary, stocks, movers)
    log.info("Stocks: %d | Advances: %d | Declines: %d",
             len(stocks), summary["advances"], summary["declines"])
    log.info("=== CSE Data Extraction Pipeline END ===")

if __name__ == "__main__":
    run_extraction()