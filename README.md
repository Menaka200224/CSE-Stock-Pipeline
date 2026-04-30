# CSE Stock Market Data Pipeline

> An automated data pipeline that extracts Sri Lanka related financial market data daily using Apache Airflow, stores it in PostgreSQL, and visualises trends using Jupyter Notebook.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Data Sources](#data-sources)
- [Database Schema](#database-schema)
- [DAG Details](#dag-details)
- [Visualisations](#visualisations)
- [Development Journal](#development-journal)
- [Coursework Marking Scheme](#coursework-marking-scheme)
- [License](#license)

---

## Overview

This project was built as a 4th year undergraduate coursework assignment on building automated data pipelines. The pipeline collects daily financial data for Sri Lanka related stocks and the USD/LKR exchange rate — a key Sri Lanka macroeconomic indicator — and stores it in PostgreSQL for trend analysis and visualisation.

**Data collected:**
- USD to Sri Lankan Rupee (LKR) exchange rate
- Daily stock quotes for 6 instruments including Expolanka Holdings (EXPO), HDFC Bank (HDB), Sterling Infrastructure (STER), Alphabet (GOOG), Microsoft (MSFT), and Oritani Financial (ORIT)

**Pipeline runs:** Monday to Friday at 06:00 UTC (11:30 AM Sri Lanka Standard Time)

---

## Architecture

```
[ Alpha Vantage Public API ]
         |  HTTPS GET — GLOBAL_QUOTE + CURRENCY_EXCHANGE_RATE
         v
[ cse_extractor.py ]  <── Airflow Scheduler triggers daily
         |  Extract → Transform → Load
         v
[ PostgreSQL 13 ]
   ├── cse_stock_prices
   ├── cse_daily_summary
   └── cse_top_gainers_losers
         |
         v
[ Jupyter Notebook ]  — matplotlib + seaborn charts
```

---

## Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Apache Airflow | 2.8.1 | Pipeline orchestration |
| Docker + Compose | Latest | Containerised deployment |
| PostgreSQL | 13 | Relational data storage |
| Python | 3.8 | Extraction and transformation |
| Alpha Vantage API | Free tier | Financial market data |
| Jupyter Notebook | Latest | Data visualisation |
| GitHub Codespaces | — | Cloud development environment |

---

## Project Structure

```
CSE-Stock-Pipeline/
├── docker-compose.yml          # Airflow + PostgreSQL stack
├── dags/
│   └── cse_pipeline_dag.py     # Airflow DAG — 4 tasks
├── scripts/
│   └── cse_extractor.py        # Data extraction and loading
├── sql/
│   └── init.sql                # Database schema
├── notebooks/
│   └── cse_analysis.ipynb      # Visualisation notebook
└── README.md
```

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running
- Git
- A free [Alpha Vantage API key](https://www.alphavantage.co/support/#api-key)

### 1. Clone the repository

```bash
git clone https://github.com/Menaka200224/CSE-Stock-Pipeline.git
cd CSE-Stock-Pipeline
```

### 2. Add your API key

Open `scripts/cse_extractor.py` and replace the API key on line 20:

```python
API_KEY = "YOUR_API_KEY_HERE"
```

### 3. Initialise Airflow

```bash
docker compose up airflow-init
```

Wait until you see:

```
airflow-init-1  | Admin user admin created
airflow-init-1 exited with code 0
```

### 4. Start all services

```bash
docker compose up -d
```

### 5. Verify containers are running

```bash
docker ps
```

You should see 3 containers all healthy:

```
cse-stock-pipeline-airflow-webserver-1   healthy   0.0.0.0:8080->8080/tcp
cse-stock-pipeline-airflow-scheduler-1   Up
cse-stock-pipeline-postgres-1            healthy   0.0.0.0:5432->5432/tcp
```

### 6. Access the Airflow UI

Open [http://localhost:8080](http://localhost:8080) and log in with:

- **Username:** `admin`
- **Password:** `admin`

### 7. Trigger the pipeline

1. Find `cse_stock_market_pipeline` in the DAG list
2. Toggle it **ON** using the blue switch
3. Click the **▶ Trigger DAG** button
4. Watch all 4 tasks turn green — takes approximately 2 minutes

### 8. Verify data in the database

```bash
docker exec -it cse-stock-pipeline-postgres-1 psql -U airflow -d airflow -c \
"SELECT symbol, last_traded_price, change_pct, extracted_at FROM cse_stock_prices ORDER BY extracted_at DESC LIMIT 10;"
```

### 9. Run the Jupyter Notebook

```bash
pip install jupyter pandas matplotlib seaborn psycopg2-binary

cd notebooks
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser \
  --NotebookApp.token="" --NotebookApp.password=""
```

Open port 8888 in your browser and run `cse_analysis.ipynb`.

---

## Data Sources

### Alpha Vantage API

All data is sourced from the [Alpha Vantage](https://www.alphavantage.co) public API.

| Endpoint | Data |
|----------|------|
| `GLOBAL_QUOTE` | Real-time stock price, open, high, low, change %, volume |
| `CURRENCY_EXCHANGE_RATE` | USD to LKR exchange rate |

**Why Alpha Vantage?**

During development, two alternative sources were evaluated and rejected:

- **CSE Official API** (`www.cse.lk/api/`) — returns HTTP 405 from cloud environments
- **Yahoo Finance** — returns null data for `.CM` suffix stocks (misclassified as mutual funds)

Alpha Vantage was confirmed working from GitHub Codespaces and provides structured, well-documented JSON responses with no authentication beyond an API key.

### Tracked Instruments

| Symbol | Company | Sector | Sri Lanka Relevance |
|--------|---------|--------|---------------------|
| EXPO | Expolanka Holdings | Logistics | Listed on CSE, Sri Lanka headquartered |
| HDB | HDFC Bank | Banking | Regional banking exposure |
| STER | Sterling Infrastructure | Infrastructure | Emerging market infrastructure |
| GOOG | Alphabet Inc | Technology | Global tech benchmark |
| MSFT | Microsoft Corp | Technology | Global tech benchmark |
| ORIT | Oritani Financial | Finance | Financial sector proxy |
| USD/LKR | Exchange Rate | Forex | Primary Sri Lanka economic indicator |

---

## Database Schema

Three tables store all pipeline data. Every table includes an `extracted_at` timestamp.

```sql
-- Daily stock prices (7 rows per run)
CREATE TABLE cse_stock_prices (
    id                SERIAL PRIMARY KEY,
    extracted_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    trade_date        DATE NOT NULL,
    symbol            VARCHAR(20) NOT NULL,
    company_name      VARCHAR(200),
    last_traded_price NUMERIC(12, 2),
    previous_close    NUMERIC(12, 2),
    price_change      NUMERIC(8, 2),
    change_pct        NUMERIC(6, 3),
    open_price        NUMERIC(12, 2),
    high_price        NUMERIC(12, 2),
    low_price         NUMERIC(12, 2),
    volume            BIGINT,
    turnover          BIGINT,
    sector            VARCHAR(100)
);

-- Aggregated market summary (1 row per run)
CREATE TABLE cse_daily_summary (
    id               SERIAL PRIMARY KEY,
    extracted_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    trade_date       DATE NOT NULL,
    aspi             NUMERIC(12, 2),
    aspi_change_pct  NUMERIC(6, 3),
    total_turnover   BIGINT,
    total_volume     BIGINT,
    total_trades     INTEGER,
    advances         INTEGER,
    declines         INTEGER,
    unchanged        INTEGER
);

-- Top 3 gainers and losers (6 rows per run)
CREATE TABLE cse_top_gainers_losers (
    id            SERIAL PRIMARY KEY,
    extracted_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    trade_date    DATE NOT NULL,
    category      VARCHAR(10),
    rank_position INTEGER,
    symbol        VARCHAR(20),
    company_name  VARCHAR(200),
    price         NUMERIC(12, 2),
    change_pct    NUMERIC(6, 3),
    volume        BIGINT
);
```

---

## DAG Details

**DAG ID:** `cse_stock_market_pipeline`

**Schedule:** `0 6 * * 1-5` — Monday to Friday at 06:00 UTC

**Tasks:**

```
health_check → extract_and_load → validate_data → log_summary
```

| Task | Description |
|------|-------------|
| `health_check` | Pings Alpha Vantage to confirm connectivity |
| `extract_and_load` | Runs full extraction and inserts into PostgreSQL |
| `validate_data` | Confirms rows were inserted in the last 2 hours |
| `log_summary` | Logs advances, declines, USD/LKR rate to Airflow logs |

**Retry policy:** 2 retries with 10 minute delay

**Expected runtime:** ~2 minutes (rate limit delays between API calls)

---

## Visualisations

The Jupyter Notebook produces 8 charts:

| Chart | Type | Insight |
|-------|------|---------|
| Stock Price Over Time | Line chart | Price trends across extraction runs |
| USD/LKR Exchange Rate | Area chart | Sri Lanka currency trend |
| Daily Price Change % | Bar chart | Green/red sentiment per stock |
| Advances vs Declines | Stacked bar | Market breadth per run |
| Top Gainers and Losers | Horizontal bar | Best and worst performers |
| USD/LKR vs Stock Performance | Dual axis | Currency-equity correlation |
| Trading Volume by Stock | Horizontal bar | Volume ranking |
| Price Range High vs Low | Range chart | Intraday volatility |

---

## Development Journal

This section documents key decisions and problems solved during development — relevant for understanding the commit history.

| Commit | Decision |
|--------|----------|
| `Initial commit` | Project scaffolded with docker-compose, DAG, extractor, SQL schema |
| `fix: switch from CSE API to Yahoo Finance` | CSE API returns HTTP 405 from Codespaces |
| `fix: update DAG health check for Yahoo Finance` | DAG health check updated to match new source |
| `fix: replace Yahoo Finance with Alpha Vantage` | Yahoo Finance returns null data for .CM stocks |
| `fix: replace unavailable symbols TEA and CEYL` | TEA and CEYL not found on Alpha Vantage |
| `fix: validate using extracted_at instead of CURRENT_DATE` | Yahoo Finance returns prior day trade_date causing validation failure |
| `feat: add Jupyter notebook with 8 visualisations` | Task 5 visualisation layer complete |

---

## Coursework Marking Scheme

| Task | Marks | Implementation |
|------|-------|----------------|
| Airflow Setup | 20 | `docker-compose.yml` |
| Data Extraction | 25 | `scripts/cse_extractor.py` |
| DAG Scheduling | 20 | `dags/cse_pipeline_dag.py` |
| Data Storage | 15 | `sql/init.sql` + PostgreSQL |
| Visualization | 10 | `notebooks/cse_analysis.ipynb` |
| Documentation | 10 | Report + README |
| **Total** | **100** | |

---

## License

This project was created for academic coursework purposes. All rights reserved.

---

*Built with Apache Airflow, Docker, PostgreSQL, and Python — GitHub Codespaces*
