from __future__ import annotations
import pandas as pd
import numpy as np

ROLLING_WINDOWS = [7, 30, 90, 180, 365]

def compute_daily_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """prices columns: date, asset_symbol, price"""
    df = prices.sort_values(["asset_symbol","date"]).copy()
    df["daily_return"] = df.groupby("asset_symbol")["price"].pct_change()
    return df

def compute_rolling_returns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for w in ROLLING_WINDOWS:
        out[f"r{w}d"] = out.groupby("asset_symbol")["price"].pct_change(periods=w)
    return out

def compute_volatility(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for w in [30, 90, 365]:
        out[f"vol_{w}d"] = (
            out.groupby("asset_symbol")["daily_return"]
               .rolling(window=w, min_periods=max(5, w//6))
               .std()
               .reset_index(level=0, drop=True)
        )
    return out

def compute_correlation_vs_btc(df: pd.DataFrame) -> pd.DataFrame:
    """Compute rolling correlation of each asset daily_return vs BTC daily_return."""
    out = df.copy()
    btc = out[out["asset_symbol"]=="BTC"][["date","daily_return"]].rename(columns={"daily_return":"btc_return"})
    out = out.merge(btc, on="date", how="left")

    for w in [30, 90, 365]:
        def _rolling_corr(g: pd.DataFrame) -> pd.Series:
            return g["daily_return"].rolling(window=w, min_periods=max(5, w//6)).corr(g["btc_return"])
        out[f"corr_btc_{w}d"] = out.groupby("asset_symbol", group_keys=False).apply(_rolling_corr)
    return out

def compute_relative_performance(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    btc_rr = out[out["asset_symbol"]=="BTC"][["date"] + [f"r{w}d" for w in ROLLING_WINDOWS]]
    btc_rr = btc_rr.rename(columns={f"r{w}d": f"btc_r{w}d" for w in ROLLING_WINDOWS})
    out = out.merge(btc_rr, on="date", how="left")
    for w in ROLLING_WINDOWS:
        out[f"rel_r{w}d"] = out[f"r{w}d"] - out[f"btc_r{w}d"]
    out = out.drop(columns=[f"btc_r{w}d" for w in ROLLING_WINDOWS])
    return out

def build_metrics(prices: pd.DataFrame) -> pd.DataFrame:
    df = compute_daily_returns(prices)
    df = compute_rolling_returns(df)
    df = compute_volatility(df)
    df = compute_correlation_vs_btc(df)
    df = compute_relative_performance(df)
    return df
