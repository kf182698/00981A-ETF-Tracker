# build_change_table.py — baseline fallback + price from prices/ or XLSX + robust new/sell
from __future__ import annotations
import os, re, glob, json
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

DATA_DIR        = Path("data")
SNAP_DATA_DIR   = Path("data_snapshots")
REPORT_DIR      = Path("reports")
PRICE_DIR       = Path("prices")
ARCHIVE_DIR     = Path("archive")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# -------- 日期處理 --------
def _normalize_report_date(raw: str) -> str:
    if raw is None:
        raise ValueError("REPORT_DATE is None")
    s = str(raw).strip()
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y, mo, d = m.groups()
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    m = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    raise ValueError(f"無法解析 REPORT_DATE：{raw}")

def _pick_default_date_and_base():
    snaps = sorted(glob.glob(str(SNAP_DATA_DIR / "*.csv")))
    if snaps:
        date_str = Path(snaps[-1]).stem
        return _normalize_report_date(date_str), SNAP_DATA_DIR
    dailies = sorted(glob.glob(str(DATA_DIR / "*.csv")))
    if dailies:
        date_str = Path(dailies[-1]).stem
        return _normalize_report_date(date_str), DATA_DIR
    raise FileNotFoundError("找不到任何 CSV（data_snapshots/ 或 data/）")

def _choose_base_dir(date_str: str) -> Path:
    if (SNAP_DATA_DIR / f"{date_str}.csv").exists():
        return SNAP_DATA_DIR
    if (DATA_DIR / f"{date_str}.csv").exists():
        return DATA_DIR
    return SNAP_DATA_DIR if SNAP_DATA_DIR.exists() else DATA_DIR

# -------- 讀檔 --------
def _read_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    required = ["股票代號","股票名稱","股數","持股權重"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"{path} 缺少欄位：{c}，實際={list(df.columns)}")
    df["股票代號"] = df["股票代號"].astype(str).str.strip()
    df["股票名稱"] = df["股票名稱"].astype(str).str.strip()
    df["股數"]     = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)
    df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)
    return df

# -------- 價格處理 --------
def _normalize_price_df(df: pd.DataFrame) -> None:
    if "股票代號" not in df.columns:
        rename_map = {}
        for c in df.columns:
            sc = str(c).strip()
            if sc in ("代號","證券代號","StockCode"): rename_map[c] = "股票代號"
            if sc in ("收盤","收盤價","Close","close","收盤價(元)"): rename_map[c] = "收盤價"
        if rename_map:
            df.rename(columns=rename_map, inplace=True)
    df["股票代號"] = df["股票代號"].astype(str).str.strip()
    if "收盤價" in df.columns:
        df["收盤價"] = pd.to_numeric(df["收盤價"], errors="coerce")

def _load_prices_for(date_str: str) -> pd.DataFrame:
    """優先讀 prices/<date>.csv；若無 → 讀 archive/<YYYY-MM>/*YYYYMMDD*.xlsx 的 with_prices；
       再無 → 向前回補最近 90 天（兩來源都試）。"""
    # 1) prices/YYYY-MM-DD.csv
    p = PRICE_DIR / f"{date_str}.csv"
    if p.exists():
        df = pd.read_csv(p, encoding="utf-8-sig")
        _normalize_price_df(df)
        print(f"[price] use prices CSV: {p}")
        return df[["股票代號","收盤價"]].copy()

    # 2) archive 的 with_prices
    yyyymm   = date_str[:7]
    yyyymmdd = date_str.replace("-", "")
    month_dir = ARCHIVE_DIR / yyyymm
    cands = sorted(glob.glob(str(month_dir / f"*{yyyymmdd}*.xlsx")))
    if cands:
        try:
            df = pd.read_excel(cands[-1], sheet_name="with_prices", dtype={"股票代號": str})
            _normalize_price_df(df)
            out = df[["股票代號","收盤價"]].copy()
            print(f"[price] use archive xlsx: {Path(cands[-1]).name}")
            return out
        except Exception as e:
            print(f"[price] read xlsx with_prices failed: {e}")

    # 3) 往前回補最多 90 天
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    for _ in range(90):
        d = d - timedelta(days=1)
        # prices/
        p = PRICE_DIR / f"{d}.csv"
        if p.exists():
            df = pd.read_csv(p, encoding="utf-8-sig")
            _normalize_price_df(df)
            print(f"[price] fallback prices CSV: {p}")
            return df[["股票代號","收盤價"]].copy()
        # archive/with_prices
        yyyymm = str(d)[:7]; yyyymmdd = str(d).replace("-", "")
        cands = sorted(glob.glob(str(ARCHIVE_DIR / yyyymm / f"*{yyyymmdd}*.xlsx")))
        if cands:
            try:
                df = pd.read_excel(cands[-1], sheet_name="with_prices", dtype={"股票代號": str})
                _normalize_price_df(df)
                out = df[["股票代號","收盤價"]].copy()
                print(f"[price] fallback archive xlsx: {Path(cands[-1]).name}")
                return out
            except Exception as e:
                print(f"[price] fallback xlsx failed: {e}")

    print("[price] no price found")
    return pd.DataFrame(columns=["股票代號","收盤價"])

def _merge_price(df_today: pd.DataFrame, date_str: str) -> pd.DataFrame:
    if "收盤價" in df_today.columns:
        df_today = df_today.drop(columns=["收盤價"])
    px = _load_prices_for(date_str)
    if px.empty:
        df_today["收盤價"] = pd.NA
        return df_today
    return df_today.merge(px, on="股票代號", how="left")

# -------- 基期 --------
def _find_prev_by_listing(today_str: str, base_dir: Path):
    files = sorted(Path(base_dir).glob("*.csv"), key=lambda p: p.name)
    cands = [p for p in files if p.stem < today_str]
    if not cands:
        return None, None
    prev = cands[-1]
    return prev.stem, prev

# -------- 主流程 --------
def main():
    raw = os.getenv("REPORT_DATE")
    if raw:
        today_str = _normalize_report_date(raw)
        base_dir = _choose_base_dir(today_str)
    else:
        today_str, base_dir = _pick_default_date_and_base()

    today_path = base_dir / f"{today_str}.csv"
    if not today_path.exists():
        alt = DATA_DIR / f"{today_str}.csv"
        if alt.exists():
            base_dir, today_path = DATA_DIR, alt
        else:
            raise FileNotFoundError(f"找不到今日 CSV：{today_path}")

    prev_str, prev_path = _find_prev_by_listing(today_str, base_dir)
    if not prev_path and base_dir == SNAP_DATA_DIR:
        prev_str, prev_path = _find_prev_by_listing(today_str, DATA_DIR)

    first_run = False
    if not prev_path:
        first_run = True
        prev_str = today_str

    print(f"[build] today={today_str}, prev={prev_str}, base_dir={base_dir}")

    # 讀今、昨
    df_t = _read_csv(today_path)
    df_t = _merge_price(df_t, today_str)
    if first_run:
        df_y = df_t.copy()
    else:
        df_y = _read_csv(prev_path)

    if "收盤價" in df_y.columns:
        df_y = df_y.drop(columns=["收盤價"])

    # 以股票代號合併，名稱以今日優先
    key = ["股票代號"]
    dfm = pd.merge(df_t, df_y, on=key, how="outer", suffixes=("_今","_昨"))
    if "股票名稱_今" in dfm.columns or "股票名稱_昨" in dfm.columns:
        dfm["股票名稱"] = dfm.get("股票名稱_今").fillna(dfm.get("股票名稱_昨"))
        for c in ("股票名稱_今","股票名稱_昨"):
            if c in dfm.columns: dfm.drop(columns=[c], inplace=True)

    for c in ["股數_今","股數_昨"]:
        dfm[c] = pd.to_numeric(dfm.get(c), errors="coerce").fillna(0).astype(int)
    for c in ["持股權重_今","持股權重_昨"]:
        dfm[c] = pd.to_numeric(dfm.get(c), errors="coerce").fillna(0.0)

    # 價格欄位保險正規化
    for cand in ["收盤價_今","收盤價","收盤價_x","收盤價_y","收盤價_昨"]:
        if cand in dfm.columns:
            dfm.rename(columns={cand:"收盤價"}, inplace=True)
            break
    if "收盤價" not in dfm.columns:
        dfm["收盤價"] = pd.NA

    dfm["買賣超股數"] = dfm["股數_今"] - dfm["股數_昨"]
    dfm["權重Δ%"]   = (dfm["持股權重_今"] - dfm["持股權重_昨"]).round(4)

    # 門檻（可環境覆寫）
    NEW_MIN = float(os.getenv("NEW_HOLDING_MIN_WEIGHT","0.4"))
    SELL_MAX = float(os.getenv("SELL_ALERT_MAX_WEIGHT","0.1"))
    NOISE    = float(os.getenv("NOISE_THRESHOLD","0.01"))
    TOPN     = int(os.getenv("TOP_N","10"))

    # 首次買進 / 關鍵賣出（不受 TopN/NOISE 限制）
    new_mask  = (dfm["持股權重_昨"] <= 0.0 + 1e-12) & (dfm["持股權重_今"] >= NEW_MIN)
    sell_mask = (dfm["持股權重_今"] <= SELL_MAX)   & (dfm["持股權重_昨"] >  NOISE)

    # 圖用 movers（保留 TopN 與 NOISE）
    movers = dfm.copy()
    movers["abs"] = movers["權重Δ%"].abs()
    movers = movers[movers["abs"] >= NOISE]
    d1_up = movers.sort_values("權重Δ%", ascending=False).head(TOPN)
    d1_dn = movers.sort_values("權重Δ%", ascending=True).head(TOPN)

    # 輸出表
    today_col = f"股數_{today_str}"
    prev_col  = f"股數_{prev_str}"
    out = dfm[[
        "股票代號","股票名稱","收盤價",
        "股數_今","持股權重_今","股數_昨","持股權重_昨","買賣超股數","權重Δ%"
    ]].rename(columns={
        "股數_今": today_col, "股數_昨": prev_col,
        "持股權重_今": "今日權重%", "持股權重_昨": "昨日權重%"
    }).sort_values("權重Δ%", ascending=False)

    out_path = REPORT_DIR / f"holdings_change_table_{today_str}.csv"
    out.to_csv(out_path, index=False, encoding="utf-8-sig")

    # 近 5 個日期
    files = sorted(Path(base_dir).glob("*.csv"), key=lambda p: p.name)
    upto_today = [p for p in files if p.stem <= today_str]
    last5 = upto_today[-5:] if upto_today else [today_path]
    last5_dates = [p.stem for p in last5]

    top10_sum = df_t.sort_values("持股權重", ascending=False).head(10)["持股權重"].sum()
    top1 = df_t.sort_values("持股權重", ascending=False).head(1)[["股票代號","股票名稱","持股權重"]].iloc[0]

    summary = {
        "date": today_str,
        "baseline_date": prev_str,
        "first_run_mode": bool(first_run),
        "total_count": int(df_t["股票代號"].nunique()),
        "top10_sum": round(float(top10_sum), 4),
        "top_weight": {
            "code": str(top1["股票代號"]),
            "name": str(top1["股票名稱"]),
            "weight": round(float(top1["持股權重"]), 4),
        },
        "new_holdings": (
            dfm[new_mask]
            .sort_values("持股權重_今", ascending=False)
            [["股票代號","股票名稱","持股權重_今"]]
            .rename(columns={"持股權重_今":"今日權重%"}).to_dict(orient="records")
        ),
        "sell_alerts": (
            dfm[sell_mask]
            .sort_values("權重Δ%", ascending=True)
            [["股票代號","股票名稱","持股權重_昨","持股權重_今","權重Δ%"]]
            .to_dict(orient="records")
        ),
        "d1_up": d1_up[["股票代號","股票名稱","持股權重_昨","持股權重_今","權重Δ%"]].to_dict(orient="records"),
        "d1_dn": d1_dn[["股票代號","股票名稱","持股權重_昨","持股權重_今","權重Δ%"]].to_dict(orient="records"),
        "last5_dates": last5_dates,
        "table_columns": ["股票代號","股票名稱","收盤價",
                          today_col,"今日權重%", prev_col,"昨日權重%","買賣超股數","權重Δ%"],
    }
    with open(REPORT_DIR / f"summary_{today_str}.json","w",encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"[build] saved: {out_path}")
    print(f"[build] saved: reports/summary_{today_str}.json")

if __name__ == "__main__":
    main()
