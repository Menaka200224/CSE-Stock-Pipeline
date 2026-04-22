-- CSE Stock Market Data Pipeline - Database Schema
-- Creates all required tables for storing Colombo Stock Exchange data

-- Drop tables if they exist (for clean restarts)
DROP TABLE IF EXISTS cse_daily_summary;
DROP TABLE IF EXISTS cse_stock_prices;
DROP TABLE IF EXISTS cse_sector_data;
DROP TABLE IF EXISTS cse_top_gainers_losers;

-- Daily market summary (ASPI, S&P SL20, turnover)
CREATE TABLE cse_daily_summary (
    id              SERIAL PRIMARY KEY,
    extracted_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    trade_date      DATE NOT NULL,
    aspi            NUMERIC(12, 2),
    aspi_change     NUMERIC(8, 2),
    aspi_change_pct NUMERIC(6, 3),
    sp_sl20         NUMERIC(12, 2),
    sp_sl20_change  NUMERIC(8, 2),
    total_turnover  BIGINT,
    total_volume    BIGINT,
    total_trades    INTEGER,
    advances        INTEGER,
    declines        INTEGER,
    unchanged       INTEGER
);

-- Individual stock prices
CREATE TABLE cse_stock_prices (
    id                  SERIAL PRIMARY KEY,
    extracted_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    trade_date          DATE NOT NULL,
    symbol              VARCHAR(20) NOT NULL,
    company_name        VARCHAR(200),
    last_traded_price   NUMERIC(12, 2),
    previous_close      NUMERIC(12, 2),
    price_change        NUMERIC(8, 2),
    change_pct          NUMERIC(6, 3),
    open_price          NUMERIC(12, 2),
    high_price          NUMERIC(12, 2),
    low_price           NUMERIC(12, 2),
    volume              BIGINT,
    turnover            BIGINT,
    market_cap          BIGINT,
    sector              VARCHAR(100)
);

-- Sector-level performance
CREATE TABLE cse_sector_data (
    id              SERIAL PRIMARY KEY,
    extracted_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    trade_date      DATE NOT NULL,
    sector_name     VARCHAR(100) NOT NULL,
    sector_index    NUMERIC(12, 2),
    sector_change   NUMERIC(8, 2),
    sector_change_pct NUMERIC(6, 3),
    turnover        BIGINT
);

-- Top gainers and losers of the day
CREATE TABLE cse_top_gainers_losers (
    id              SERIAL PRIMARY KEY,
    extracted_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    trade_date      DATE NOT NULL,
    category        VARCHAR(10) NOT NULL,  -- 'gainer' or 'loser'
    rank_position   INTEGER,
    symbol          VARCHAR(20),
    company_name    VARCHAR(200),
    price           NUMERIC(12, 2),
    change_pct      NUMERIC(6, 3),
    volume          BIGINT
);

-- Indexes for common query patterns
CREATE INDEX idx_stock_prices_date   ON cse_stock_prices(trade_date);
CREATE INDEX idx_stock_prices_symbol ON cse_stock_prices(symbol);
CREATE INDEX idx_daily_summary_date  ON cse_daily_summary(trade_date);
CREATE INDEX idx_sector_date         ON cse_sector_data(trade_date);