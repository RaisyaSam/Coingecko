from __future__ import annotations
import pandas as pd

def check_duplicates(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    dup = df[df.duplicated(keys, keep=False)].sort_values(keys)
    return dup

def check_nulls(df: pd.DataFrame, cols: list[str]) -> dict:
    return {c: int(df[c].isna().sum()) for c in cols}

def check_non_positive_prices(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["price"] <= 0]

def missing_dates_report(df: pd.DataFrame, date_col: str, group_col: str) -> pd.DataFrame:
    out = []
    for sym, g in df.groupby(group_col):
        g = g.sort_values(date_col)
        all_days = pd.date_range(g[date_col].min(), g[date_col].max(), freq="D").date
        present = set(g[date_col].tolist())
        missing = [d for d in all_days if d not in present]
        out.append({"asset_symbol": sym, "missing_days": len(missing)})
    return pd.DataFrame(out).sort_values("missing_days", ascending=False)
