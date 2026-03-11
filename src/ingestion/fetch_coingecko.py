from __future__ import annotations
import pandas as pd
from datetime import date

from src.ingestion.coingecko_client import CoinGeckoClient
from src.utils.logger import get_logger

log = get_logger("fetch_coingecko")

def fetch_btc_daily_prices(
    client: CoinGeckoClient,
    coingecko_id: str,
    vs_currency: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    log.info(f"Fetching CoinGecko daily prices: {coingecko_id} vs {vs_currency} {start_date} -> {end_date}")
    payload = client.market_chart_range(coingecko_id=coingecko_id, vs_currency=vs_currency, start_date=start_date, end_date=end_date)
    prices = payload.get("prices", [])
    if not prices:
        raise ValueError(f"No prices returned from CoinGecko. Keys: {list(payload.keys())}")

    df = pd.DataFrame(prices, columns=["timestamp_ms", "price"])
    df["date"] = pd.to_datetime(df["timestamp_ms"], unit="ms").dt.date
    out = pd.DataFrame({
        "date": df["date"],
        "asset_symbol": "BTC",
        "price": pd.to_numeric(df["price"], errors="coerce"),
        "source": "coingecko",
    }).dropna(subset=["price"])

    out = out.sort_values(["date"]).groupby(["date","asset_symbol"], as_index=False).tail(1)
    return out[["date","asset_symbol","price","source"]]
