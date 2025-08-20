
# charts.py
# Generate three figures for email:
# 1) D1 Weight Change (Top N) — horizontal bar
# 2) Daily Weight Trend (Top Movers x5) — smoothed line (rolling average)
# 3) Weekly Cumulative Weight Change (vs first week) — trend per code
#
# Env:
#   REPORT_DATE=YYYY-MM-DD (optional; default = latest summary_*.json)

import os, glob, json
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

REPORT_DIR = Path("reports")
CHART_DIR = Path("charts")
DATA_DIR = Path("data")
CHART_DIR.mkdir(parents=True, exist_ok=True)

def _latest_date():
    js = sorted(glob.glob(str(REPORT_DIR / "summary_*.json")))
    if not js:
        raise FileNotFoundError("no summary_*.json, run build_change_table.py first")
    return Path(js[-1]).stem.split("_")[1]

def _load_summary(date_str):
    with open(REPORT_DIR / f"summary_{date_str}.json", "r", encoding="utf-8") as f:
        return json.load(f)

def fig_d1_bars(date_str, summary, topn=10):
    # Combine up/down into one bar chart (positive/negative)
    df_up = pd.DataFrame(summary["d1_up"])
    df_dn = pd.DataFrame(summary["d1_dn"])
    df_up["label"] = df_up["股票代號"] + " " + df_up["股票名稱"]
    df_dn["label"] = df_dn["股票代號"] + " " + df_dn["股票名稱"]
    df_up = df_up.head(topn)
    df_dn = df_dn.head(topn)

    fig, ax = plt.subplots(figsize=(7,4.2), dpi=150)
    # plot down (negative) then up
    if len(df_dn):
        ax.barh(df_dn["label"], df_dn["Δ%"], label="Down")
    if len(df_up):
        ax.barh(df_up["label"], df_up["Δ%"], label="Up")
    ax.set_title("D1 Weight Change (Top Movers)")
    ax.set_xlabel("Δ% (percentage points)")
    ax.set_ylabel("Ticker Name")
    ax.legend()
    plt.tight_layout()
    out = CHART_DIR / f"chart_d1_{date_str}.png"
    fig.savefig(out)
    plt.close(fig)
    return str(out)

def fig_daily_trend(date_str, summary, k=5):
    # pick top |Δ%| movers up to k
    df_up = pd.DataFrame(summary["d1_up"])
    df_dn = pd.DataFrame(summary["d1_dn"])
    pool = pd.concat([df_up, df_dn], ignore_index=True)
    pool["abs"] = pool["Δ%"].abs()
    pool = pool.sort_values("abs", ascending=False).head(k)

    # Build a time-series frame for last N dates for those codes
    dates = summary["last5_dates"]
    series = {}
    for d in dates:
        csv = pd.read_csv(DATA_DIR / f"{d}.csv", encoding="utf-8-sig")
        s = csv.set_index("股票代號")["持股權重"]
        series[d] = s
    fig, ax = plt.subplots(figsize=(7,4.2), dpi=150)
    for _, row in pool.iterrows():
        code = str(row["股票代號"])
        y = [float(series[d].get(code, 0.0)) for d in dates]
        # smoothing by simple rolling window (size=2)
        if len(y) >= 3:
            y_s = pd.Series(y).rolling(2, min_periods=1).mean().tolist()
        else:
            y_s = y
        ax.plot(dates, y_s, marker="o", label=code)
    ax.set_title("Daily Weight Trend (Top Movers x5)")
    ax.set_xlabel("Date")
    ax.set_ylabel("Weight %")
    ax.legend()
    plt.tight_layout()
    out = CHART_DIR / f"chart_daily_{date_str}.png"
    fig.savefig(out)
    plt.close(fig)
    return str(out)

def fig_weekly_cum(date_str, summary):
    # Use last5_dates; compute change vs the first date
    dates = summary["last5_dates"]
    first = dates[0]
    series = {}
    for d in dates:
        csv = pd.read_csv(DATA_DIR / f"{d}.csv", encoding="utf-8-sig")
        s = csv.set_index("股票代號")["持股權重"]
        series[d] = s

    # choose top 5 by today's weight
    last_csv = pd.read_csv(DATA_DIR / f"{dates[-1]}.csv", encoding="utf-8-sig")
    top_codes = last_csv.sort_values("持股權重", ascending=False).head(5)["股票代號"].astype(str).tolist()

    fig, ax = plt.subplots(figsize=(7,4.2), dpi=150)
    for code in top_codes:
        y = [float(series[d].get(code, 0.0) - series[first].get(code, 0.0)) for d in dates]
        ax.plot(dates, y, marker="o", label=code)
    ax.set_title("Weekly Cumulative Weight Change (vs first week)")
    ax.set_xlabel("Date")
    ax.set_ylabel("Δ% vs First")
    ax.legend()
    plt.tight_layout()
    out = CHART_DIR / f"chart_weekly_{date_str}.png"
    fig.savefig(out)
    plt.close(fig)
    return str(out)

def main():
    date_str = os.getenv("REPORT_DATE") or _latest_date()
    summary = _load_summary(date_str)
    p1 = fig_d1_bars(date_str, summary)
    p2 = fig_daily_trend(date_str, summary)
    p3 = fig_weekly_cum(date_str, summary)
    print("[charts] saved:", p1, p2, p3)

if __name__ == "__main__":
    main()
