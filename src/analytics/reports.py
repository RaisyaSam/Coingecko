from __future__ import annotations
import pandas as pd
from datetime import date

WINDOW_LABELS = [
    ("r365d", "1Y"),
    ("r180d", "6M"),
    ("r90d", "3M"),
    ("r30d", "1M"),
    ("r7d", "7D"),
]


def best_worst_vs_btc(metrics: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for col, label in WINDOW_LABELS:
        rel_col = f"rel_{col}"

        sub = metrics[
            ~metrics["asset_symbol"].isin(["BTC", "USD"])
        ][["date", "asset_symbol", col, rel_col]].copy()

        sub = sub.dropna(subset=[col, rel_col])

        if sub.empty:
            continue

        # keep the latest valid row for each asset
        sub = (
            sub.sort_values(["asset_symbol", "date"])
               .groupby("asset_symbol", as_index=False)
               .tail(1)
        )

        best = sub.sort_values(rel_col, ascending=False).head(1).iloc[0]
        worst = sub.sort_values(rel_col, ascending=True).head(1).iloc[0]

        rows.append(
            {
                "window": label,
                "best_asset": best["asset_symbol"],
                "best_rel_vs_btc": float(best[rel_col]),
                "worst_asset": worst["asset_symbol"],
                "worst_rel_vs_btc": float(worst[rel_col]),
            }
        )

    return pd.DataFrame(rows)


def investment_value(
    prices: pd.DataFrame,
    asset_symbol: str,
    amount: float,
    start_date: date,
    end_date: date,
) -> float:
    p0 = prices[
        (prices["asset_symbol"] == asset_symbol) & (prices["date"] >= start_date)
    ].sort_values("date").head(1)

    p1 = prices[
        (prices["asset_symbol"] == asset_symbol) & (prices["date"] <= end_date)
    ].sort_values("date").tail(1)

    if p0.empty or p1.empty:
        return float("nan")

    return float(amount * (p1["price"].iloc[0] / p0["price"].iloc[0]))


def dca_btc(prices: pd.DataFrame, monthly_amount: float) -> dict:
    """Invest monthly into BTC by buying on the first available day of each month."""
    btc = prices[prices["asset_symbol"] == "BTC"].sort_values("date").copy()

    if btc.empty:
        return {
            "btc_units": float("nan"),
            "final_value": float("nan"),
            "months": 0,
        }

    btc["month"] = pd.to_datetime(btc["date"]).dt.to_period("M")
    buys = btc.groupby("month").head(1)

    units = (monthly_amount / buys["price"]).sum()
    final_price = btc.tail(1)["price"].iloc[0]

    return {
        "btc_units": float(units),
        "final_value": float(units * final_price),
        "months": int(buys.shape[0]),
    }


def volatility_comparison(latest_metrics: pd.DataFrame) -> pd.DataFrame:
    cols = ["asset_symbol", "vol_30d", "vol_90d", "vol_365d"]
    return latest_metrics[cols].sort_values("vol_30d", ascending=False)


def correlation_table(latest_metrics: pd.DataFrame) -> pd.DataFrame:
    cols = ["asset_symbol", "corr_btc_30d", "corr_btc_90d", "corr_btc_365d"]
    return latest_metrics[
        ~latest_metrics["asset_symbol"].isin(["USD"])
    ][cols].sort_values("corr_btc_30d", ascending=False)