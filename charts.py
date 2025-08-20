# charts.py
# Generate three figures for email:
# 1) D1 Weight Change (Top Movers) — horizontal bar
# 2) Daily Weight Trend (Top Movers x5) — smoothed line (rolling average)
# 3) Weekly Cumulative Weight Change (vs first week)
#
# Env:
#   REPORT_DATE=YYYY-MM-DD or string containing yyyymmdd (e.g., ETF_Investment_Portfolio_20250808)
# If REPORT_DATE not set, it will use the latest reports/summary_*.json

import os, glob, json, re
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

def _normalize_report_date(s: str) -> str:
    """
    Accepts:
      - '2025-08-08'
      - 'ETF_Investment_Portfolio_20250808'
      - 'downloads/ETF_Investment_Portfolio_20250808.xlsx'
    Returns: '2025-08-08'
    """
    if not s:
        return s
    s = s.strip()
    # direct ISO date
    m = re.fullmatch(r"\d{4}-\d{2}-\d{2}", s)
    if m:
        return s
    # find yyyymmdd anywhere
    m = re.search(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    raise ValueError(f"Cannot parse REPORT_DATE: {s}")

def _load_summary(date_str):
    with open(REPORT_DIR / f"summary_{date_str}.json", "r", encoding="utf-8") as f:
        return json.load(f)

def fig_d1_bars(date_str, summary, topn=10):
    # Combine up/down into one bar chart (positive/negative)
    df_up = pd.DataFrame(summary.get("d1_up", []))
    df_dn = pd.DataFrame(summary.get("d1_dn", []))
    if not df_up.empty:
        df_up["label"] = df_up["股票代號"].astype(str) + " " + df_up["股票名稱"].astype(str)
    if not df_dn.empty:
        df_dn["label"] = df_dn["股票代號"].astype(str) + " " + df_dn["股票名稱"].astype(str)
    df_up = df_up.head(topn)
    df_dn = df_dn.head(topn)

    fig, ax = plt.subplots(figsize=(7,4.2), dpi=150)
    if not df_dn.empty:
        ax.barh(df_dn["label"], df_dn["Δ%"], label="Down")
    if not df_up.empty:
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
    df_up = pd.DataFrame(summary.get("d1_up", []))
    df_dn = pd.DataFrame(summary.get("d1_dn", []))
    pool = pd.concat([df_up, df_dn], ignore_index=True) if not df_up.empty or not df_dn.empty else pd.DataFrame()
    if pool.empty:
        # no movers -> skip
        return None

    pool["abs"] = pool["Δ%"].abs()
    pool = pool.sort_values("abs", ascending=False).head(k)

    # Build a time-series frame for last N dates for those codes
    dates = summary.get("last5_dates", [])
    if not dates:
        return None

    series = {}
    for d in dates:
        csv_path = DATA_DIR / f"{d}.csv"
        if not csv_path.exists():
            continue
        csv = pd.read_csv(csv_path, encoding="utf-8-sig")
        if "股票代號" not in csv.columns or "持股權重" not in csv.columns:
            continue
        s = csv.set_index("股票代號")["持股權重"]
        series[d] = s

    if len(series) < 2:
        return None

    fig, ax = plt.subplots(figsize=(7,4.2), dpi=150)
    for _, row in pool.iterrows():
        code = str(row["股票代號"])
        y = [float(series.get(d, pd.Series()).get(code, 0.0)) for d in dates]
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
    dates = summary.get("last5_dates", [])
    if not dates:
        return None
    first = dates[0]

    series = {}
    for d in dates:
        csv_path = DATA_DIR / f"{d}.csv"
        if not csv_path.exists():
            continue
        csv = pd.read_csv(csv_path, encoding="utf-8-sig")
        if "股票代號" not in csv.columns or "持股權重" not in csv.columns:
            continue
        s = csv.set_index("股票代號")["持股權重"]
        series[d] = s

    if len(series) < 2:
        return None

    # choose top 5 by today's weight
    last_csv = DATA_DIR / f"{dates[-1]}.csv"
    if not last_csv.exists():
        return None
    last_df = pd.read_csv(last_csv, encoding="utf-8-sig")
    if "股票代號" not in last_df.columns or "持股權重" not in last_df.columns:
        return None
    top_codes = last_df.sort_values("持股權重", ascending=False).head(5)["股票代號"].astype(str).tolist()

    fig, ax = plt.subplots(figsize=(7,4.2), dpi=150)
    for code in top_codes:
        y = [float(series.get(d, pd.Series()).get(code, 0.0) - series.get(first, pd.Series()).get(code, 0.0)) for d in dates]
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
    raw = os.getenv("REPORT_DATE")
    date_str = _normalize_report_date(raw) if raw else _latest_date()
    summary = _load_summary(date_str)
    p1 = fig_d1_bars(date_str, summary)
    p2 = fig_daily_trend(date_str, summary)
    p3 = fig_weekly_cum(date_str, summary)
    print("[charts] saved:", p1, p2, p3)

if __name__ == "__main__":
    main()
