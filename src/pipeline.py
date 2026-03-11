from __future__ import annotations

import os
import argparse
import yaml
import pandas as pd
from datetime import date, datetime

from src.utils.logger import get_logger
from src.utils.dates import last_n_days_range
from src.utils.io import ensure_dir, write_parquet

from src.ingestion.massive_client import MassiveClient
from src.ingestion.coingecko_client import CoinGeckoClient
from src.ingestion.fetch_massive import fetch_massive_daily_prices
from src.ingestion.fetch_coingecko import fetch_btc_daily_prices

from src.models.warehouse import connect, init_schema, upsert_prices
from src.processing.data_quality import (
    check_duplicates,
    check_nulls,
    check_non_positive_prices,
    missing_dates_report,
)
from src.processing.metrics import build_metrics
from src.analytics.reports import best_worst_vs_btc
from src.analytics.written_analysis import write_written_analysis

log = get_logger("pipeline")


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def run(config: dict, start: date, end: date) -> None:
    storage = config["storage"]
    ensure_dir(storage["raw_dir"])
    ensure_dir(storage["processed_dir"])
    ensure_dir(os.path.dirname(storage["warehouse_path"]))
    ensure_dir(storage["output_dir"])

    massive_cfg = config["massive"]
    cg_cfg = config["coingecko"]

    massive = MassiveClient(
        base_url=massive_cfg["base_url"],
        api_key=massive_cfg["api_key"],
        rpm=5,
    )
    coingecko = CoinGeckoClient(
        base_url=cg_cfg["base_url"],
        api_key=cg_cfg.get("api_key", ""),
        rpm=30,
    )

    frames = []

    # BTC
    btc_asset = config["assets"]["crypto"][0]
    frames.append(
        fetch_btc_daily_prices(
            client=coingecko,
            coingecko_id=btc_asset.get("coingecko_id", "bitcoin"),
            vs_currency=btc_asset.get("currency", "USD"),
            start_date=start,
            end_date=end,
        )
    )

    # FX
    for fx in config["assets"]["fx"]:
        sym = fx["symbol"]

        if sym == "USD":
            dates = pd.date_range(start, end, freq="D").date
            frames.append(
                pd.DataFrame(
                    {
                        "date": dates,
                        "asset_symbol": "USD",
                        "price": 1.0,
                        "source": "synthetic",
                    }
                )
            )
        else:
            pair = fx["pair"]
            frames.append(
                fetch_massive_daily_prices(
                    massive,
                    symbol=pair,
                    kind="fx",
                    start_date=start,
                    end_date=end,
                ).assign(asset_symbol=sym)
            )

    # Stocks
    for st in config["assets"]["stocks"]:
        frames.append(
            fetch_massive_daily_prices(
                massive,
                symbol=st["symbol"],
                kind="stock",
                start_date=start,
                end_date=end,
            )
        )

    # Index
    for idx in config["assets"]["index"]:
        frames.append(
            fetch_massive_daily_prices(
                massive,
                symbol=idx["symbol"],
                kind="index",
                start_date=start,
                end_date=end,
            )
        )

    raw = pd.concat(frames, ignore_index=True)
    raw = raw[["date", "asset_symbol", "price", "source"]].copy()
    raw["date"] = pd.to_datetime(raw["date"]).dt.date

    raw_path = os.path.join(storage["raw_dir"], f"raw_prices_{start}_{end}.parquet")
    write_parquet(raw, raw_path)
    log.info(f"Wrote raw parquet: {raw_path}")

    dup = check_duplicates(raw, ["date", "asset_symbol"])
    nulls = check_nulls(raw, ["date", "asset_symbol", "price"])
    nonpos = check_non_positive_prices(raw)
    missing = missing_dates_report(raw, "date", "asset_symbol")

    dq_path = os.path.join(storage["output_dir"], "data_quality_report.csv")
    dq = pd.DataFrame(
        {
            "metric": [
                "null_date",
                "null_asset_symbol",
                "null_price",
                "duplicate_rows",
                "non_positive_prices",
            ],
            "value": [
                nulls["date"],
                nulls["asset_symbol"],
                nulls["price"],
                int(len(dup)),
                int(len(nonpos)),
            ],
        }
    )
    dq.to_csv(dq_path, index=False)
    missing.to_csv(
        os.path.join(storage["output_dir"], "missing_days_by_asset.csv"),
        index=False,
    )

    if len(dup) > 0:
        log.warning(f"Found duplicates: {len(dup)} (keeping latest per day+asset)")
        raw = (
            raw.sort_values(["date", "asset_symbol"])
            .groupby(["date", "asset_symbol"], as_index=False)
            .tail(1)
        )

    if len(nonpos) > 0:
        log.warning(f"Found non-positive prices: {len(nonpos)} (dropping)")
        raw = raw[raw["price"] > 0]

    con = connect(storage["warehouse_path"])
    init_schema(con)
    upsert_prices(con, raw)
    log.info("Upserted prices into DuckDB warehouse")

    prices = con.execute(
        """
        SELECT
            d.date,
            a.asset_symbol,
            p.price
        FROM fact_daily_price p
        JOIN dim_date d
            ON d.date_key = p.date_key
        JOIN dim_asset a
            ON a.asset_key = p.asset_key
        WHERE d.date BETWEEN ? AND ?
        ORDER BY a.asset_symbol, d.date
        """,
        [start, end],
    ).df()

    metrics = build_metrics(prices)

    proc_path = os.path.join(
        storage["processed_dir"],
        f"metrics_{start}_{end}.parquet",
    )
    write_parquet(metrics, proc_path)
    log.info(f"Wrote processed parquet: {proc_path}")

    # Upsert metrics into warehouse
    con.register("_m", metrics)

    con.execute(
        """
        CREATE TEMP TABLE _mapped AS
        SELECT
            d.date_key,
            a.asset_key,
            m.daily_return,
            m.vol_30d,
            m.vol_90d,
            m.vol_365d,
            m.corr_btc_30d,
            m.corr_btc_90d,
            m.corr_btc_365d,
            m.r7d,
            m.r30d,
            m.r90d,
            m.r180d,
            m.r365d,
            m.rel_r7d,
            m.rel_r30d,
            m.rel_r90d,
            m.rel_r180d,
            m.rel_r365d
        FROM _m m
        JOIN dim_date d
            ON d.date = m.date
        JOIN dim_asset a
            ON a.asset_symbol = m.asset_symbol
        """
    )

    con.execute(
        """
        INSERT OR REPLACE INTO fact_daily_metrics
        SELECT
            date_key,
            asset_key,
            daily_return,
            vol_30d,
            vol_90d,
            vol_365d,
            corr_btc_30d,
            corr_btc_90d,
            corr_btc_365d,
            r7d,
            r30d,
            r90d,
            r180d,
            r365d,
            rel_r7d,
            rel_r30d,
            rel_r90d,
            rel_r180d,
            rel_r365d
        FROM _mapped
        """
    )

    con.execute("DROP TABLE _mapped")

    summary = best_worst_vs_btc(metrics)
    summary_path = os.path.join(storage["output_dir"], "summary_metrics.csv")
    summary.to_csv(summary_path, index=False)
    log.info(f"Wrote summary: {summary_path}")

    written_path = os.path.join(storage["output_dir"], "written_analysis.md")
    write_written_analysis(prices=prices, metrics=metrics, out_path=written_path)
    log.info(f"Wrote written analysis: {written_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.yaml")
    ap.add_argument("--start", default="", help="YYYY-MM-DD (optional)")
    ap.add_argument("--end", default="", help="YYYY-MM-DD (optional)")
    args = ap.parse_args()

    cfg = load_config(args.config)

    if args.start and args.end:
        start = _parse_date(args.start)
        end = _parse_date(args.end)
    else:
        n = int(cfg["pipeline"].get("days", 365))
        start, end = last_n_days_range(n_days=n)

    run(cfg, start, end)


if __name__ == "__main__":
    main()