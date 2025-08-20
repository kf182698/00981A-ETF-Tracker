# charts.py (auto-convert CSV & auto-build summary if missing)
# 1) D1 Weight Change (Top Movers) — barh
# 2) Daily Weight Trend (Top Movers x5) — smoothed line
# 3) Weekly Cumulative Weight Change (vs first week)
#
# Env:
#   REPORT_DATE=YYYY-MM-DD or string containing yyyymmdd
#   e.g., "2025-08-11" or "ETF_Investment_Portfolio_20250811"
#
import os, glob, json, re, subprocess
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

REPORT_DIR = Path("reports")
CHART_DIR = Path("charts")
DATA_DIR = Path("data")
DL_DIR = Path("downloads")
CHART_DIR.mkdir(parents=True, exist_ok=True)

def _latest_date():
    js = sorted(glob.glob(str(REPORT_DIR / "summary_*.json")))
    if not js:
        raise FileNotFoundError("no summary_*.json, run build_change_table.py first")
    return Path(js[-1]).stem.split("_")[1]

def _normalize_report_date(s: str) -> str:
    """Accepts 'YYYY-MM-DD' or any string containing yyyymmdd -> returns 'YYYY-MM-DD'."""
    if not s:
        return s
    s = s.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    m = re.search(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    raise ValueError(f"Cannot parse REPORT_DATE: {s}")

def _ensure_csv(date_str: str):
    """If data/{date}.csv missing, try convert from downloads xlsx automatically."""
    csv_path = DATA_DIR / f"{date_str}.csv"
    if csv_path.exists():
        return
    # 找 downloads/{YYYY-MM-DD}.xlsx
    xlsx_candidates = []
    p1 = DL_DIR / f"{date_str}.xlsx"
    if p1.exists():
        xlsx_candidates.append(p1)
    # 找 downloads/*{yyyymmdd}*.xlsx
    yyyymmdd = date_str.replace("-", "")
    xlsx_candidates += [Path(p) for p in glob.glob(str(DL_DIR / f"*{yyyymmdd}*.xlsx"))]
    xlsx_candidates = [p for p in xlsx_candidates if p.exists()]

    if not xlsx_candidates:
        raise FileNotFoundError(
            f"missing {csv_path} 且找不到對應 xlsx。\n"
            f"請先轉檔：python xlsx_to_csv.py --date {date_str}"
        )
    # 有 xlsx → 自動轉
    env = os.environ.copy()
    print(f"[charts] CSV missing, auto convert from XLSX → data/{date_str}.csv")
    subprocess.check_call(["python", "xlsx_to_csv.py", "--date", date_str], env=env)
    if not csv_path.exists():
        raise FileNotFoundError(f"轉檔後仍無 {csv_path}")

def _load_summary(date_str):
    with open(REPORT_DIR / f"summary_{date_str}.json", "r", encoding="utf-8") as f:
        return json.load(f)

def _ensure_summary(date_str: str):
    """If summary missing, build it by calling build_change_table.py (will use data/{date}.csv)."""
    sum_path = REPORT_DIR / f"summary_{date_str}.json"
    if sum_path.exists():
        return
    _ensure_csv(date_str)
    env = os.environ.copy()
    env["REPORT_DATE"] = date_str  # 傳標準日期進去
    print(f"[charts] summary missing, auto build via build_change_table.py for {date_str} …")
    subprocess.check_call(["python", "build_change_table.py"], env=env)
    if not sum_path.exists():
        raise FileNotFoundError(f"build_change_table.py 執行後仍找不到 {sum_path}")

def fig_d1_bars(date_str, summary, topn=10):
    df_up = pd.DataFrame(summary.get("d1_up", []))
    df_dn = pd.DataFrame(summary.get("d1_dn", []))
    if not df_up.empty:
        df_up["label"] = df_up["股票代號"].astype(str) + " " + df_up["股票名稱"].astype(str)
    if not df_dn.empty:
        df_dn["label"] = df_dn["股票代號"].astype(str) + " " + df_dn["股票名稱"].astype(str)
    df_up = df_up.head(topn); df_dn = df_dn.head(topn)

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
    fig.savefig(out); plt.close(fig)
    return str(out)

def fig_daily_trend(date_str, summary, k=5):
    df_up = pd.DataFrame(summary.get("d1_up", []))
    df_dn = pd.DataFrame(summary.get("d1_dn", []))
    pool = pd.concat([df_up, df_dn], ignore_index=True) if (not df_up.empty or not df_dn.empty) else pd.DataFrame()
    if pool.empty: return None

    pool["abs"] = pool["Δ%"].abs()
    pool = pool.sort_values("abs", ascending=False).head(k)

    dates = summary.get("last5_dates", [])
    if not dates: return None

    series = {}
    for d in dates:
        csv_path = DATA_DIR / f"{d}.csv"
        if not csv_path.exists(): continue
        csv = pd.read_csv(csv_path, encoding="utf-8-sig")
        if "股票代號" not in csv.columns or "持股權重" not in csv.columns: continue
        series[d] = csv.set_index("股票代號")["持股權重"]

    if len(series) < 2: return None

    fig, ax = plt.subplots(figsize=(7,4.2), dpi=150)
    for _, row in pool.iterrows():
        code = str(row["股票代號"])
        y = [float(series.get(d, pd.Series()).get(code, 0.0)) for d in dates]
        # simple smoothing
        y_s = pd.Series(y).rolling(2, min_periods=1).mean().tolist() if len(y) >= 3 else y
        ax.plot(dates, y_s, marker="o", label=code)
    ax.set_title("Daily Weight Trend (Top Movers x5)")
    ax.set_xlabel("Date"); ax.set_ylabel("Weight %"); ax.legend()
    plt.tight_layout()
    out = CHART_DIR / f"chart_daily_{date_str}.png"
    fig.savefig(out); plt.close(fig)
    return str(out)

def fig_weekly_cum(date_str, summary):
    dates = summary.get("last5_dates", [])
    if not dates: return None
    first = dates[0]

    series = {}
    for d in dates:
        csv_path = DATA_DIR / f"{d}.csv"
        if not csv_path.exists(): continue
        csv = pd.read_csv(csv_path, encoding="utf-8-sig")
        if "股票代號" not in csv.columns or "持股權重" not in csv.columns: continue
        series[d] = csv.set_index("股票代號")["持股權重"]
    if len(series) < 2: return None

    last_csv = DATA_DIR / f"{dates[-1]}.csv"
    if not last_csv.exists(): return None
    last_df = pd.read_csv(last_csv, encoding="utf-8-sig")
    if "股票代號" not in last_df.columns or "持股權重" not in last_df.columns: return None
    top_codes = last_df.sort_values("持股權重", ascending=False).head(5)["股票代號"].astype(str).tolist()

    fig, ax = plt.subplots(figsize=(7,4.2), dpi=150)
    for code in top_codes:
        y = [float(series.get(d, pd.Series()).get(code, 0.0) - series.get(first, pd.Series()).get(code, 0.0)) for d in dates]
        ax.plot(dates, y, marker="o", label=code)
    ax.set_title("Weekly Cumulative Weight Change (vs first week)")
    ax.set_xlabel("Date"); ax.set_ylabel("Δ% vs First"); ax.legend()
    plt.tight_layout()
    out = CHART_DIR / f"chart_weekly_{date_str}.png"
    fig.savefig(out); plt.close(fig)
    return str(out)

def main():
    raw = os.getenv("REPORT_DATE")
    date_str = _normalize_report_date(raw) if raw else _latest_date()
    _ensure_summary(date_str)   # 自動補 CSV / 補 summary
    summary = _load_summary(date_str)
    p1 = fig_d1_bars(date_str, summary)
    p2 = fig_daily_trend(date_str, summary)
    p3 = fig_weekly_cum(date_str, summary)
    print("[charts] saved:", p1, p2, p3)

if __name__ == "__main__":
    main()