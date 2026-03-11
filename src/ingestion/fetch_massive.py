from __future__ import annotations
import pandas as pd
from datetime import date
from typing import Literal

from src.ingestion.massive_client import MassiveClient
from src.utils.logger import get_logger

log = get_logger("fetch_massive")


def _normalize_massive_response(symbol: str, kind: str, payload: dict) -> pd.DataFrame:
    rows = payload.get("results", [])
    if not rows:
        raise ValueError(
            f"No rows returned for {symbol} ({kind}). "
            f"Keys={list(payload.keys())}, payload={payload}"
        )

    df = pd.DataFrame(rows)


    if "t" not in df.columns or "c" not in df.columns:
        raise ValueError(
            f"Unexpected Massive response for {symbol}. "
            f"Columns={df.columns.tolist()}, payload={payload}"
        )

    out = pd.DataFrame({
        "date": pd.to_datetime(df["t"], unit="ms").dt.date,
        "asset_symbol": symbol,
        "price": pd.to_numeric(df["c"], errors="coerce"),
        "source": "massive",
    }).dropna(subset=["price"])

    return out


def fetch_massive_daily_prices(
    client: MassiveClient,
    symbol: str,
    kind: Literal["stock", "index", "fx"],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    log.info(f"Fetching Massive daily prices: {symbol} ({kind}) {start_date} -> {end_date}")
    payload = client.get_time_series(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        kind=kind,
    )
    return _normalize_massive_response(symbol, kind, payload)