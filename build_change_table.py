# build_change_table.py
from __future__ import annotations
import os, re, glob, json
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

DATA_DIR        = Path("data")
SNAP_DATA_DIR   = Path("data_snapshots")
REPORT_DIR      = Path("reports")
PRICE_DIR       = Path("prices")
for d in (REPORT_DIR,):
    d.mkdir(parents=True, exist_ok=True)

def _normalize_report_date(raw: str) -> str:
    s = raw.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    m = re.search(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    raise ValueError(f"無法解析 REPORT_DATE：{raw}")

def _prev_calendar_day(d):
    return d - timedelta(days=1)

def _find_prev_existing(today_date, base_dir: Path):
    tries = 0
    cand = _prev_calendar_day(today_date)
    while tries < 90:
        fp = base_dir / f"{cand}.csv"
        if fp.exists():
            return cand, fp
        cand = _prev_calendar_day(cand)
        tries += 1
    return None, None

def _read_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    required = ["股票代號","股票名稱","股數","持股權重"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"{path} 缺少欄位：{c}，實際={list(df.columns)}")
    df["股票代號"] = df["股票代號"].astype(str).str.strip()
    df["股票名稱"] = df["股票名稱"].astype(str).str.strip()
    df["股數"]       = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)
    df["持股權重"]   = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)
    return df

def _normalize_price_df(df: pd.DataFrame):
    if "股票代號" not in df.columns:
        rename_map = {}
        for c in df.columns:
            if str(c).strip() in ("代號","證券代號","StockCode"):
                rename_map[c]="股票代號"
            if str(c).strip() in ("收盤", "收盤價","Close","close"):
                rename_map[c]="收盤價"
        df.rename(columns=rename_map, inplace=True)
    df["股票代號"] = df["股票代號"].astype(str).str.strip()
    if "收盤價" in df.columns:
        df["收盤價"] = pd.to_numeric(df["收盤價"], errors="coerce")

def _load_prices_for(date_str: str) -> pd.DataFrame:
    target = PRICE_DIR / f"{date_str}.csv"
    if target.exists():
        df = pd.read_csv(target, encoding="utf-8-sig")
        df["source_date"] = date_str
        df["price_source"] = "today"
        _normalize_price_df(df)
        return df
    from datetime import datetime, timedelta
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    tries = 0
    while tries < 90:
        d = d - timedelta(days=1)
        alt = PRICE_DIR / f"{d}.csv"
        if alt.exists():
            df = pd.read_csv(alt, encoding="utf-8-sig")
            df["source_date"] = str(d)
            df["price_source"] = "prev_close"
            _normalize_price_df(df)
            return df
        tries += 1
    return pd.DataFrame(columns=["股票代號","收盤價","source_date","price_source"])

def _merge_price(df_today: pd.DataFrame, date_str: str) -> pd.DataFrame:
    px = _load_prices_for(date_str)
    if px.empty:
        df_today["收盤價"] = pd.NA
        return df_today
    px = px[["股票代號","收盤價"]].copy()
    return df_today.merge(px, on="股票代號", how="left")

def main():
    raw = os.getenv("REPORT_DATE")
    if not raw:
        snap_csvs = sorted(glob.glob(str(SNAP_DATA_DIR / "*.csv")))
        if snap_csvs:
            raw = Path(snap_csvs[-1]).stem
        else:
            csvs = sorted(glob.glob(str(DATA_DIR / "*.csv")))
            if not csvs:
                raise FileNotFoundError("找不到 data/*.csv 或 data_snapshots/*.csv")
            raw = Path(csvs[-1]).stem

    today_str = raw if re.fullmatch(r"\\d{4}-\\d{2}-\\d{2}", raw) else f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    base_dir = SNAP_DATA_DIR if (SNAP_DATA_DIR / f"{today_str}.csv").exists() or SNAP_DATA_DIR.exists() else DATA_DIR

    today_path = base_dir / f"{today_str}.csv"
    if not today_path.exists():
        raise FileNotFoundError(f"找不到今日 CSV：{today_path}")

    from datetime import datetime as _dt, timedelta as _td
    today_date = _dt.strptime(today_str,"%Y-%m-%d").date()
    prev_date, prev_path = None, None
    # 找到前一個可用快照
    d = today_date
    for _ in range(90):
        d = d - _td(days=1)
        p = base_dir / f"{d}.csv"
        if p.exists():
            prev_date, prev_path = d, p
            break
    if not prev_path:
        raise RuntimeError(f"找不到 {today_str} 之前的可用 CSV 作為比較基期（於 {base_dir}）")

    print(f"[build] today={today_str}, prev={prev_date}, base_dir={base_dir}")

    df_t = _read_csv(today_path)
    df_y = _read_csv(prev_path)

    df_t = _merge_price(df_t, today_str)

    key = ["股票代號","股票名稱"]
    dfm = pd.merge(df_t, df_y, on=key, how="outer", suffixes=("_今","_昨"))
    for c in ["股數_今","股數_昨"]:
        dfm[c] = pd.to_numeric(dfm.get(c), errors="coerce").fillna(0).astype(int)
    for c in ["持股權重_今","持股權重_昨"]:
        dfm[c] = pd.to_numeric(dfm.get(c), errors="coerce").fillna(0.0)

    dfm["買賣超股數"] = dfm["股數_今"] - dfm["股數_昨"]
    dfm["權重Δ%"]   = (dfm["持股權重_今"] - dfm["持股權重_昨"]).round(4)

    NEW_MIN = float(os.getenv("NEW_HOLDING_MIN_WEIGHT","0.4"))
    SELL_MAX = float(os.getenv("SELL_ALERT_MAX_WEIGHT","0.1"))
    NOISE    = float(os.getenv("NOISE_THRESHOLD","0.01"))
    TOPN     = int(os.getenv("TOP_N","10"))

    new_mask  = (dfm["持股權重_昨"] <= 0.0 + 1e-12) & (dfm["持股權重_今"] >= NEW_MIN)
    sell_mask = (dfm["持股權重_今"] <= SELL_MAX) & (dfm["持股權重_昨"] > NOISE)

    today_col = f"股數_{today_str}"
    prev_col  = f"股數_{prev_date}"
    out = dfm[[
        "股票代號","股票名稱","收盤價",
        "股數_今","持股權重_今","股數_昨","持股權重_昨","買賣超股數","權重Δ%"
    ]].copy()
    out.rename(columns={
        "股數_今": today_col, "股數_昨": prev_col,
        "持股權重_今":"今日權重%", "持股權重_昨":"昨日權重%"
    }, inplace=True)
    out = out.sort_values("權重Δ%", ascending=False)

    out_path = REPORT_DIR / f"holdings_change_table_{today_str}.csv"
    out.to_csv(out_path, index=False, encoding="utf-8-sig")

    movers = dfm.copy()
    movers["abs"] = movers["權重Δ%"].abs()
    movers = movers[movers["abs"] >= NOISE]
    d1_up = movers.sort_values("權重Δ%", ascending=False).head(TOPN)
    d1_dn = movers.sort_values("權重Δ%", ascending=True).head(TOPN)

    csvs = sorted([Path(p) for p in glob.glob(str(base_dir / "*.csv"))], key=lambda p: p.name)
    upto_today = [p for p in csvs if p.name <= f"{today_str}.csv"]
    last5 = upto_today[-5:] if upto_today else [today_path]
    last5_dates = [p.stem for p in last5]

    top10_sum = df_t.sort_values("持股權重", ascending=False).head(10)["持股權重"].sum()
    top1 = df_t.sort_values("持股權重", ascending=False).head(1)[["股票代號","股票名稱","持股權重"]].iloc[0]
    summary = {
        "date": today_str,
        "baseline_date": str(prev_date),
        "total_count": int(df_t["股票代號"].nunique()),
        "top10_sum": round(float(top10_sum),4),
        "top_weight": {"code": str(top1["股票代號"]), "name": str(top1["股票名稱"]), "weight": round(float(top1["持股權重"]),4)},
        "new_holdings_min": NEW_MIN,
        "new_holdings": d1_up[new_mask][["股票代號","股票名稱","持股權重_今"]].rename(columns={"持股權重_今":"今日權重%"}).to_dict(orient="records"),
        "sell_alert_max": SELL_MAX,
        "sell_alerts": dfm[sell_mask][["股票代號","股票名稱","持股權重_昨","持股權重_今","權重Δ%"]].to_dict(orient="records"),
        "d1_up": d1_up[["股票代號","股票名稱","持股權重_昨","持股權重_今","權重Δ%"]].to_dict(orient="records"),
        "d1_dn": d1_dn[["股票代號","股票名稱","持股權重_昨","持股權重_今","權重Δ%"]].to_dict(orient="records"),
        "last5_dates": last5_dates,
        "table_columns": ["股票代號","股票名稱","收盤價", today_col,"今日權重%", prev_col,"昨日權重%","買賣超股數","權重Δ%"],
    }
    with open(REPORT_DIR / f"summary_{today_str}.json","w",encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"[build] saved: {out_path}")
    print(f"[build] saved: reports/summary_{today_str}.json")

if __name__ == "__main__":
    main()
