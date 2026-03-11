from __future__ import annotations
import pandas as pd
from datetime import date, timedelta

from src.analytics.reports import best_worst_vs_btc, investment_value, dca_btc, volatility_comparison, correlation_table

def write_written_analysis(prices: pd.DataFrame, metrics: pd.DataFrame, out_path: str) -> None:
    latest_date = metrics["date"].max()
    latest = metrics[metrics["date"] == latest_date].copy()
    
    bw = best_worst_vs_btc(metrics)
    vol = volatility_comparison(latest)
    corr = correlation_table(latest)

    one_year_ago = latest_date - timedelta(days=365)
    btc_1k = investment_value(prices, "BTC", 1000.0, one_year_ago, latest_date)

    dca = dca_btc(prices, 100.0)
    # lump sum: $1200 on first day
    lump = investment_value(prices, "BTC", 1200.0, prices[prices["asset_symbol"]=="BTC"]["date"].min(), latest_date)

    # volatility statement
    fiat = vol[vol["asset_symbol"].isin(["USD","EUR","GBP"])]
    btc_vol_30 = float(vol[vol["asset_symbol"]=="BTC"]["vol_30d"].iloc[0]) if not vol[vol["asset_symbol"]=="BTC"].empty else float("nan")
    fiat_vol_30 = float(fiat["vol_30d"].mean()) if not fiat.empty else float("nan")
    vol_winner = "Bitcoin" if btc_vol_30 > fiat_vol_30 else "Fiat currencies (avg)"

    md = []
    md.append(f"# Written Analysis\n")
    md.append(f"## 1) Which asset outperformed Bitcoin across each time window?\n")
    if bw.empty:
        md.append("Not enough data to compute windows.\n")
    else:
        for _, r in bw.iterrows():
            md.append(f"- **{r['window']}**: Best vs BTC = **{r['best_asset']}** (relative: {r['best_rel_vs_btc']:.4f}); Worst vs BTC = **{r['worst_asset']}** (relative: {r['worst_rel_vs_btc']:.4f})\n")

    md.append("\n## 2) Current worth of $1K invested in BTC one year ago\n")
    md.append(f"- $1,000 invested in BTC about one year ago is worth **${btc_1k:,.2f}** (based on first available price >= {one_year_ago} and last price at {latest_date}).\n")

    md.append("\n## 3) $100/month DCA into BTC (12 months) vs initial lump sum\n")
    md.append(f"- DCA ($100/month): bought over **{dca.get('months','?')}** months, accumulated **{dca.get('btc_units', float('nan')):.6f} BTC**, final value **${dca.get('final_value', float('nan')):,.2f}**\n")
    md.append(f"- Lump sum ($1200 at start): final value **${lump:,.2f}**\n")
    md.append("\nInterpretation: lump sum usually wins in rising markets; DCA reduces timing risk.\n")

    md.append("\n## 4) Volatility: fiat currencies or Bitcoin?\n")
    md.append(f"- 30D volatility BTC: **{btc_vol_30:.6f}**, average fiat (USD/EUR/GBP): **{fiat_vol_30:.6f}**\n")
    md.append(f"- More volatile (30D): **{vol_winner}**\n")

    md.append("\n## Appendix: Correlation vs BTC (latest)\n")
    for _, r in corr.iterrows():
        md.append(f"- {r['asset_symbol']}: corr_30d={r['corr_btc_30d']}, corr_90d={r['corr_btc_90d']}, corr_365d={r['corr_btc_365d']}\n")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("".join(md))
