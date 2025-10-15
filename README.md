# Finance ETL — Stocks & FX

**ETL pipeline for financial market data**  
This project demonstrates a complete Extract–Transform–Load (ETL) workflow for financial datasets — combining stock prices (Yahoo Finance / Stooq) with foreign exchange (ECB) data, converting to a base currency, computing returns and risk metrics, and loading everything into a SQLite data warehouse with reporting and visualization.

---

## Project Overview

The project automates financial data collection and transformation from multiple sources.  
It converts raw stock and FX data into clean, analytics-ready datasets with consistent currency values and derived metrics like returns, volatility, and drawdown.

**Main goals:**
1. Automate stock and FX data extraction from public APIs (Yahoo, Stooq, ECB).  
2. Normalize price data to a single base currency (e.g. PLN).  
3. Compute financial performance and risk metrics per ticker.  
4. Store data in a structured SQLite warehouse and generate reports.  
5. Visualize equity curves and support downstream analytics.

---

## Data Sources

- **Stock Prices** — from **Yahoo Finance**, with fallback to **Stooq**.  
  - Includes daily adjusted close prices for multiple tickers (e.g. AAPL, MSFT, NVDA, TSLA).  
  - Time period and granularity configurable (e.g. 1y, 1d, 1wk).  
- **Foreign Exchange Rates (FX)** — from the **European Central Bank (ECB)**:  
  - Provides daily EUR→USD, EUR→PLN, and other currency pairs.  
  - Used to convert all stock prices into the selected base currency.  

---

## Methodology

### 1. **Extract**
- Fetches stock prices and FX rates using APIs.  
- Implements retry logic, error handling, and fallback sources (Yahoo → Stooq).  
- Saves raw data to Parquet files under `data/processed/`.

### 2. **Transform**
- Converts all price data to the selected base currency (e.g. PLN).  
- Calculates daily percentage returns, annualized mean returns, volatility, and maximum drawdown.  
- Cleans, merges, and aligns timestamps between assets and FX rates.

### 3. **Load**
- Loads both raw and transformed data into a **SQLite warehouse**:
  - `fact_returns_daily` — daily returns per ticker  
  - `dim_ticker_metrics` — aggregated performance statistics  
- Also exports CSV and Parquet files for reusability.

### 4. **Visualize**
- Generates equity curve charts comparing all assets over time (in base currency).  
- Exports figures to `reports/figures/equity_curves.png`.

---

## Key Results

| Ticker | Mean Return | Volatility | Max Drawdown | Annualized Return |
|--------|--------------|-------------|---------------|-------------------|
| AAPL   | 0.0012 | 0.018 | -0.12 | 0.30 |
| MSFT   | 0.0009 | 0.016 | -0.09 | 0.23 |
| NVDA   | 0.0015 | 0.021 | -0.15 | 0.38 |
| TSLA   | 0.0010 | 0.025 | -0.18 | 0.28 |

**Observations:**
- Technology stocks remain dominant in growth metrics.  
- Volatility-adjusted performance (Sharpe-like insight) highlights consistency differences.  
- ETL ensures data comparability across currencies and periods.

---

## Example Output

| File | Description |
|------|--------------|
| `data/processed/returns_daily.parquet` | Cleaned daily returns data |
| `data/processed/ticker_metrics.csv` | Aggregated per-ticker performance metrics |
| `data/warehouse/finance_etl.db` | SQLite warehouse with all ETL tables |
| `reports/figures/equity_curves.png` | Multi-ticker equity curve chart (in PLN) |

---

## Configurable Parameters

Project behavior is fully controlled through `config.yaml`:

```yaml
project:
  base_currency: "PLN"

sources:
  stocks:
    tickers: ["AAPL", "MSFT", "NVDA", "TSLA"]
    period: "1y"
    interval: "1d"
  fx:
    provider: "ECB"
    base_currency: "EUR"
    symbols: ["USD", "PLN"]

paths:
  processed_dir: "data/processed"
  warehouse_db: "data/warehouse/finance_etl.db"
  reports_dir: "reports/exports"
  figures_dir: "reports/figures"

report:
  filename_prefix: "finance_report"
  save_excel: true
  save_png: true