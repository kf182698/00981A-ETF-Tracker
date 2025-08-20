# build_change_table.py
# Create day-over-day (D1) and five-snapshots (D5) comparison table
# Inputs: data/YYYY-MM-DD.csv (today and previous snapshots)
# Output:
#   reports/holdings_change_table_{DATE}.csv
#   reports/summary_{DATE}.json
#
# Env (optional):
#   REPORT_DATE = YYYY-MM-DD or string containing yyyymmdd
#                 e.g., "2025-08-14" or "ETF_Investment_Portfolio_20250814"
#   NEW_HOLDING_MIN_WEIGHT = "0.5"   # % threshold for "首次新增"
#   SELL_ALERT_MAX_WEIGHT  = "0.1"   # % for sell alert (today<=this & yesterday>noise)
#   NOISE_THRESHOLD        = "0.01"  # % abs change floor for D1 movers
#   TOP_N                  = "10"

import os
import re
import sys
import glob
import json
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

DATA_DIR = Path("data")
REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(exist_ok=True, parents=True)

def _parse_date_from_name(p: Path):
    try:
        return datetime.strptime(p.stem, "%Y-%m-%d").date()
    except Exception:
        return None

def _prev_biz_day(d):
    one = timedelta(days=1)
    cand = d - one
    while cand.weekday() >= 5:
        cand -= one
    return cand

def _find_prev_existing(d):
    tries = 0
    cand = _prev_biz_day(d)
    while tries < 60:
        fp = DATA_DIR / f"{cand}.csv"
        if fp.exists():
            return cand, fp
        cand = _prev_biz_day(cand)
        tries += 1
    return None, None

def _read_csv(path: Path):
    df = pd.read_csv(path, encoding="utf-8-sig")
    req = ["股票代號","股票名稱","股數","持股權重"]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"{path} 缺少欄位：{c}，實際欄位={list(df.columns)}")
    df["股票代號"] = df["股票代號"].astype(str).str.strip()
    df["股票名稱"] = df["股票名稱"].astype(str).str.strip()
    df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)
    df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)
    return df

def _normalize_report_date(s: str) -> str:
    """Accepts 'YYYY-MM-DD' or any string containing yyyymmdd, returns 'YYYY-MM-DD'."""
    s = s.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    m = re.search(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    raise ValueError(f"Cannot parse REPORT_DATE: {s}")

def _pick_today_csv():
    raw = os.getenv("REPORT_DATE")
    if raw:
        date_str = _normalize_report_date(raw)
        p = DATA_DIR / f"{date_str}.csv"
        if not p.exists():
            raise FileNotFoundError(f"指定日期不存在 CSV：{p}\n請先執行：python xlsx_to_csv.py --date {date_str}")
        return p
    # default: latest data/*.csv
    csvs = sorted([Path(p) for p in glob.glob(str(DATA_DIR / "*.csv"))], key=lambda p: p.name)
    if not csvs:
        raise FileNotFoundError("data/*.csv not found")
    return csvs[-1]

def main():
    today_path = _pick_today_csv()
    today = _parse_date_from_name(today_path)
    if not today:
        raise RuntimeError(f"無法解析日期：{today_path}")

    prev_date, prev_path = _find_prev_existing(today)
    if not prev_path:
        raise RuntimeError(f"找不到 {today} 之前的可用 CSV 作為基期")

    print(f"[build] today={today}, prev={prev_date}")
    df_t = _read_csv(today_path)
    df_y = _read_csv(prev_path)

    key = ["股票代號","股票名稱"]
    dfm = pd.merge(df_t, df_y, on=key, how="outer", suffixes=("_今日","_昨日"))
    dfm["股數_今日"] = pd.to_numeric(dfm["股數_今日"], errors="coerce").fillna(0).astype(int)
    dfm["股數_昨日"] = pd.to_numeric(dfm["股數_昨日"], errors="coerce").fillna(0).astype(int)
    dfm["持股權重_今日"] = pd.to_numeric(dfm["持股權重_今日"], errors="coerce").fillna(0.0)
    dfm["持股權重_昨日"] = pd.to_numeric(dfm["持股權重_昨日"], errors="coerce").fillna(0.0)
    dfm["Δ%"] = (dfm["持股權重_今日"] - dfm["持股權重_昨日"]).round(4)

    NEW_MIN = float(os.getenv("NEW_HOLDING_MIN_WEIGHT", "0.5"))
    SELL_MAX = float(os.getenv("SELL_ALERT_MAX_WEIGHT", "0.1"))
    NOISE   = float(os.getenv("NOISE_THRESHOLD", "0.01"))
    TOPN    = int(os.getenv("TOP_N", "10"))

    movers = dfm.copy()
    movers["abs"] = movers["Δ%"].abs()
    movers = movers[movers["abs"] >= NOISE]
    d1_up = movers.sort_values("Δ%", ascending=False).head(TOPN)
    d1_dn = movers.sort_values("Δ%", ascending=True).head(TOPN)

    new_mask = (dfm["持股權重_昨日"] <= 0.0000001) & (dfm["持股權重_今日"] >= NEW_MIN)
    new_holdings = dfm[new_mask].sort_values("持股權重_今日", ascending=False)

    sell_mask = (dfm["持股權重_今日"] <= SELL_MAX) & (dfm["持股權重_昨日"] > NOISE)
    sell_alerts = dfm[sell_mask].sort_values("Δ%", ascending=True)

    # D5 window: last 5 available snapshots including today
    csvs = sorted([Path(p) for p in glob.glob(str(DATA_DIR / "*.csv"))], key=lambda p: p.name)
    # ensure today's file is included and at the end of window
    up_to_today = [p for p in csvs if p.name <= today_path.name]
    last5 = up_to_today[-5:] if len(up_to_today) >= 1 else [today_path]

    weights = {}
    date_labels = []
    for p in last5:
        d = _parse_date_from_name(p)
        if not d:
            continue
        w = _read_csv(p).set_index("股票代號")["持股權重"]
        weights[str(d)] = w
        date_labels.append(str(d))

    today_codes = df_t["股票代號"].astype(str).tolist()
    w_today = weights[str(today)].reindex(today_codes).fillna(0.0)
    oldest_key = date_labels[0]
    w_oldest = weights[oldest_key].reindex(today_codes).fillna(0.0)
    d5_delta = (w_today - w_oldest).round(4)
    d5_df = pd.DataFrame({"股票代號": today_codes, "D5Δ%": d5_delta.values}).set_index("股票代號")
    d5_up = d5_df.sort_values("D5Δ%", ascending=False).head(TOPN)
    d5_dn = d5_df.sort_values("D5Δ%", ascending=True).head(TOPN)

    out_csv = REPORT_DIR / f"holdings_change_table_{today}.csv"
    dfm_out = dfm[["股票代號","股票名稱","股數_今日","持股權重_今日","股數_昨日","持股權重_昨日","Δ%"]].copy()
    dfm_out.rename(columns={
        "股數_今日":"股數_今日", "持股權重_今日":"今日權重%",
        "股數_昨日":"股數_昨日", "持股權重_昨日":"昨日權重%"
    }, inplace=True)
    dfm_out.to_csv(out_csv, index=False, encoding="utf-8-sig")

    total_count = df_t["股票代號"].nunique()
    top10_sum = df_t.sort_values("持股權重", ascending=False).head(10)["持股權重"].sum()
    top_weight_row = df_t.sort_values("持股權重", ascending=False).head(1)[["股票代號","股票名稱","持股權重"]].iloc[0]
    top_weight = {"code": str(top_weight_row["股票代號"]), "name": str(top_weight_row["股票名稱"]), "weight": round(float(top_weight_row["持股權重"]), 4)}

    summary = {
        "date": str(today),
        "base_prev": str(prev_date),
        "top10_sum": round(float(top10_sum), 4),
        "total_count": int(total_count),
        "top_weight": top_weight,
        "d1_up": d1_up[["股票代號","股票名稱","持股權重_昨日","持股權重_今日","Δ%"]].to_dict(orient="records"),
        "d1_dn": d1_dn[["股票代號","股票名稱","持股權重_昨日","持股權重_今日","Δ%"]].to_dict(orient="records"),
        "new_holdings_min": NEW_MIN,
        "new_holdings": new_holdings[["股票代號","股票名稱","持股權重_今日"]].to_dict(orient="records"),
        "sell_alert_max": SELL_MAX,
        "sell_alerts": sell_alerts[["股票代號","股票名稱","持股權重_昨日","持股權重_今日","Δ%"]].to_dict(orient="records"),
        "d5_up": d5_up.reset_index().to_dict(orient="records"),
        "d5_dn": d5_dn.reset_index().to_dict(orient="records"),
        "last5_dates": date_labels,
    }
    with open(REPORT_DIR / f"summary_{today}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"[build] saved: {out_csv}")
    print(f"[build] saved: reports/summary_{today}.json")

if __name__ == "__main__":
    main()
