# Crypto vs Traditional Assets Pipeline

An end-to-end **data engineering pipeline** that ingests, processes, and
analyzes price data for:

-   Cryptocurrency
-   Fiat currencies
-   Stocks
-   Market index

The pipeline analyzes **daily asset prices for the past 364 days
(configurable)** and produces analytical metrics comparing traditional
assets against Bitcoin.

This project demonstrates core **data engineering concepts** including:

-   API ingestion
-   data validation
-   star schema modeling
-   time-series analytics
-   automated reporting

All data is stored locally using **DuckDB**, while intermediate datasets
are persisted as **Parquet files**.

------------------------------------------------------------------------

# Tech Stack

-   Python
-   DuckDB
-   Pandas
-   Parquet
-   REST APIs
-   YAML configuration

------------------------------------------------------------------------

# Data Sources

The pipeline retrieves daily price data from two APIs.

## Cryptocurrency

Source: **CoinGecko API**

Assets: - BTC

## FX Rates

Source: **Massive API**

Assets: - USD - EUR - GBP

## Stocks

Source: **Massive API**

Assets: - AAPL - GOOGL - MSFT

## Market Index

Source: **Massive API**

Asset: - SPY (S&P 500 ETF proxy)

------------------------------------------------------------------------

# Pipeline Architecture

    Massive API (FX / Stocks / SPY)
                |
                v
            Ingestion
         fetch_massive.py
                |
                |
       CoinGecko API (BTC)
                |
                v
            Ingestion
         fetch_coingecko.py
                |
                v
            RAW LAYER
       data/raw/*.parquet
                |
                v
       TRANSFORM + MODEL
       DuckDB Star Schema
                |
                v
       ANALYTICS + REPORTS
       summary_metrics.csv
       written_analysis.md

------------------------------------------------------------------------

# Project Structure

    .
    ├── config
    │   └── config.yaml
    │
    ├── data
    │   ├── raw
    │   │   └── raw_prices_*.parquet
    │   ├── processed
    │   │   └── metrics_*.parquet
    │   └── warehouse
    │       └── warehouse.duckdb
    │
    ├── output
    │   ├── data_quality_report.csv
    │   ├── missing_days_by_asset.csv
    │   ├── summary_metrics.csv
    │   └── written_analysis.md
    │
    ├── src
    │   ├── ingestion
    │   │   ├── massive_client.py
    │   │   ├── coingecko_client.py
    │   │   ├── fetch_massive.py
    │   │   └── fetch_coingecko.py
    │   │
    │   ├── processing
    │   │   ├── metrics.py
    │   │   └── data_quality.py
    │   │
    │   ├── analytics
    │   │   ├── reports.py
    │   │   └── written_analysis.py
    │   │
    │   ├── models
    │   │   └── warehouse.py
    │   │
    │   └── utils
    │       ├── logger.py
    │       ├── dates.py
    │       └── io.py
    │
    ├── run_pipeline.py
    ├── requirements.txt
    └── README.md

------------------------------------------------------------------------

# Data Model (Star Schema)

## Dimensions

### dim_date

| Column | Description |
|-------|-------------|
| date_key | integer key (YYYYMMDD) |
| date | calendar date |
| year | year |
| month | month |
| day | day |
| iso_week | ISO week |
| quarter | quarter |

### dim_asset

| Column | Description |
|-------|-------------|
| asset_key | surrogate key |
| asset_symbol | ticker |
| asset_name | asset name |
| asset_type | crypto / fx / stock / index |
| currency | quote currency |

## Fact Tables

### fact_daily_price

| Column | Description |
|-------|-------------|
| date_key | date dimension key |
| asset_key | asset dimension key |
| price | closing price |
| source | data source |
| ingested_at | ingestion timestamp |

### fact_daily_metrics

| Column | Description |
|-------|-------------|
| date_key | date dimension key |
| asset_key | asset dimension key |
| daily_return | daily return |
| vol_30d | 30 day volatility |
| vol_90d | 90 day volatility |
| vol_365d | 365 day volatility |
| corr_btc_30d | BTC correlation (30d) |
| corr_btc_90d | BTC correlation (90d) |
| corr_btc_365d | BTC correlation (365d) |
| r7d | 7 day return |
| r30d | 30 day return |
| r90d | 90 day return |
| r180d | 180 day return |
| r365d | 365 day return |
| rel_r7d | return relative to BTC |
| rel_r30d | return relative to BTC |
| rel_r90d | return relative to BTC |
| rel_r180d | return relative to BTC |
| rel_r365d | return relative to BTC |

------------------------------------------------------------------------

# Quick Start

## 1 Create Virtual Environment

    python -m venv .venv
    source .venv/bin/activate

Windows:

    .venv\Scripts\activate

## 2 Install Dependencies

    pip install -r requirements.txt

## 3 Configure API Keys

Edit:

    config/config.yaml

Example:

``` yaml
massive:
  base_url: https://api.massive.com
  api_key: YOUR_KEY

coingecko:
  base_url: https://api.coingecko.com/api/v3
  api_key: YOUR_KEY
```

## 4 Run the Pipeline

Default:

    python run_pipeline.py --config config/config.yaml

Optional custom range:

    python run_pipeline.py --config config/config.yaml --start 2025-01-01 --end 2025-12-31

------------------------------------------------------------------------

# Output Artifacts

Raw data

    data/raw/*.parquet

Processed metrics

    data/processed/*.parquet

Warehouse

    data/warehouse/warehouse.duckdb

Reports

    output/summary_metrics.csv
    output/written_analysis.md

------------------------------------------------------------------------

# Data Quality Checks

The pipeline validates:

-   null values
-   duplicates
-   non-positive prices
-   missing trading days

Outputs:

    output/data_quality_report.csv
    output/missing_days_by_asset.csv

------------------------------------------------------------------------

# Design Decisions

## DuckDB

Chosen for:

-   fast local analytics
-   Parquet integration
-   zero infrastructure setup

## Idempotent Loads

Upserts are done using:

    (date_key, asset_key)

This allows safe re-runs and backfills.

## Modular Architecture

Pipeline layers:

-   ingestion
-   processing
-   models
-   analytics
-   utils

## Production Extension

DuckDB can be replaced with:

-   BigQuery
-   Snowflake
-   Redshift
-   Postgres

without changing ingestion logic.

------------------------------------------------------------------------

# FX Data Notes

FX values represent USD per 1 unit of currency.

Example:

    EURUSD = 1.10

Meaning:

    1 EUR = 1.10 USD

USD is stored as **1.0** as the baseline.

------------------------------------------------------------------------

# Summary

This project demonstrates an end-to-end data engineering workflow
including:

-   API ingestion
-   data validation
-   warehouse modeling
-   time-series analytics
-   automated report generation

using **Python, DuckDB, and Parquet**.
