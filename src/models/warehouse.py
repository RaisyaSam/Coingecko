from __future__ import annotations

import duckdb
import pandas as pd
from src.utils.logger import get_logger

log = get_logger("warehouse")

DDL = [
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key INTEGER PRIMARY KEY,
        date DATE UNIQUE,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        iso_week INTEGER,
        quarter INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_asset (
        asset_key INTEGER PRIMARY KEY,
        asset_symbol VARCHAR UNIQUE,
        asset_name VARCHAR,
        asset_type VARCHAR,
        currency VARCHAR
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_daily_price (
        date_key INTEGER,
        asset_key INTEGER,
        price DOUBLE,
        source VARCHAR,
        ingested_at TIMESTAMP,
        PRIMARY KEY (date_key, asset_key)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_daily_metrics (
        date_key INTEGER,
        asset_key INTEGER,
        daily_return DOUBLE,
        vol_30d DOUBLE,
        vol_90d DOUBLE,
        vol_365d DOUBLE,
        corr_btc_30d DOUBLE,
        corr_btc_90d DOUBLE,
        corr_btc_365d DOUBLE,
        r7d DOUBLE,
        r30d DOUBLE,
        r90d DOUBLE,
        r180d DOUBLE,
        r365d DOUBLE,
        rel_r7d DOUBLE,
        rel_r30d DOUBLE,
        rel_r90d DOUBLE,
        rel_r180d DOUBLE,
        rel_r365d DOUBLE,
        PRIMARY KEY (date_key, asset_key)
    );
    """,
]

ASSET_SEED = [
    ("BTC", "Bitcoin", "crypto", "USD"),
    ("USD", "US Dollar", "fx", "USD"),
    ("EUR", "Euro", "fx", "USD"),
    ("GBP", "British Pound", "fx", "USD"),
    ("AAPL", "Apple Inc.", "stock", "USD"),
    ("GOOGL", "Alphabet Inc. (Class A)", "stock", "USD"),
    ("MSFT", "Microsoft Corp.", "stock", "USD"),
    ("SPY", "SPDR S&P 500 ETF Trust", "index", "USD"),
]


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    for stmt in DDL:
        con.execute(stmt)

    con.execute(
        """
        CREATE TEMP TABLE _seed(
            symbol VARCHAR,
            name VARCHAR,
            typ VARCHAR,
            currency VARCHAR
        );
        """
    )
    con.executemany("INSERT INTO _seed VALUES (?,?,?,?)", ASSET_SEED)

    con.execute(
        """
        INSERT INTO dim_asset(asset_key, asset_symbol, asset_name, asset_type, currency)
        SELECT
            COALESCE((SELECT MAX(asset_key) FROM dim_asset), 0)
                + ROW_NUMBER() OVER (ORDER BY symbol) AS asset_key,
            s.symbol,
            s.name,
            s.typ,
            s.currency
        FROM _seed s
        WHERE NOT EXISTS (
            SELECT 1
            FROM dim_asset a
            WHERE a.asset_symbol = s.symbol
        );
        """
    )

    con.execute("DROP TABLE _seed;")


def upsert_dates(con: duckdb.DuckDBPyConnection, dates: pd.Series) -> None:
    df = pd.DataFrame({"date": pd.to_datetime(dates).dt.date.unique()})
    df = df.dropna().sort_values("date")

    if df.empty:
        return

    dt = pd.to_datetime(df["date"])
    df["year"] = dt.dt.year
    df["month"] = dt.dt.month
    df["day"] = dt.dt.day
    df["iso_week"] = dt.dt.isocalendar().week.astype(int)
    df["quarter"] = ((df["month"] - 1) // 3 + 1).astype(int)

    con.register("_dates", df)

    con.execute(
        """
        INSERT INTO dim_date(date_key, date, year, month, day, iso_week, quarter)
        SELECT
            CAST(STRFTIME(date, '%Y%m%d') AS INTEGER) AS date_key,
            date,
            year,
            month,
            day,
            iso_week,
            quarter
        FROM _dates d
        WHERE NOT EXISTS (
            SELECT 1
            FROM dim_date dd
            WHERE dd.date = d.date
        );
        """
    )


def upsert_prices(con: duckdb.DuckDBPyConnection, prices: pd.DataFrame) -> None:
    if prices.empty:
        return

    upsert_dates(con, prices["date"])
    con.register("_prices", prices.copy())

    con.execute(
        """
        CREATE TEMP TABLE _mapped AS
        SELECT
            dd.date_key,
            da.asset_key,
            p.price,
            p.source,
            CURRENT_TIMESTAMP AS ingested_at
        FROM _prices p
        JOIN dim_date dd
            ON dd.date = p.date
        JOIN dim_asset da
            ON da.asset_symbol = p.asset_symbol;
        """
    )

    con.execute(
        """
        INSERT OR REPLACE INTO fact_daily_price
        SELECT
            date_key,
            asset_key,
            price,
            source,
            ingested_at
        FROM _mapped;
        """
    )

    con.execute("DROP TABLE _mapped;")