# build_change_table.py — Clean fixed version (no triple-quoted strings)
# 功能：
# - 產出指定日期的「今 vs 昨」持股變化表與摘要 JSON
# - 支援 REPORT_DATE 格式：YYYY-MM-DD / YYYY-M-D / YYYYMMDD
# - 預設優先使用 data_snapshots/（去重後的“真快照序列”），否則回退 data/
# - 基期挑選：在相同資料夾中挑選 < 今日 的最近一份（對跨假日友善）
# - 價格：只讀取已保存的 prices/<date>.csv，若當天沒有則向前回補最近一筆

from __future__ import annotations
import os
import re
import glob
import json
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

DATA_DIR        = Path("data")
SNAP_DATA_DIR   = Path("data_snapshots")
REPORT_DIR      = Path("reports")
PRICE_DIR       = Path("prices")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------- 日期工具 ------------------------- #
def _normalize_report_date(raw: str) -> str:
    # 接受 'YYYY-MM-DD' / 'YYYY-M-D' / 'YYYYMMDD'，回傳 'YYYY-MM-DD'
    if raw is None:
        raise ValueError("REPORT_DATE is None")
    s = str(raw).strip()

    # yyyy-m-d / yyyy-mm-dd
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y, mo, d = m.groups()
        return f"{y}-{int(mo):02d}-{int(d):02d}"

    # yyyymmdd
    m = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    raise ValueError(f"無法解析 REPORT_DATE：{raw}")

def _pick_default_date_and_base():
    # 未提供 REPORT_DATE 時：
    #   1) 優先使用 data_snapshots/ 最新一份
    #   2) 否則使用 data/ 最新一份
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
    # 對於顯式指定的 REPORT_DATE：
    #   - 若 data_snapshots/<date>.csv 存在 → 使用 data_snapshots/
    #   - 否則檢查 data/<date>.csv → 使用 data/
    if (SNAP_DATA_DIR / f"{date_str}.csv").exists():
        return SNAP_DATA_DIR
    if (DATA_DIR / f"{date_str}.csv").exists():
        return DATA_DIR
    # 兩邊都沒有，仍偏好 snapshots 作搜尋/提示
    if SNAP_DATA_DIR.exists():
        return SNAP_DATA_DIR
    return DATA_DIR

# ------------------------- I/O 與清洗 ------------------------- #
def _read_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    required = ["股票代號", "股票名稱", "股數", "持股權重"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"{path} 缺少欄位：{c}，實際={list(df.columns)}")
    df["股票代號"] = df["股票代號"].astype(str).str.strip()
    df["股票名稱"] = df["股票名稱"].astype(str).str.strip()
    df["股數"]     = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)
    df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)
    return df

# ------------------------- 價格處理（只讀已保存快取） ------------------------- #
def _normalize_price_df(df: pd.DataFrame) -> None:
    if "股票代號" not in df.columns:
        rename_map = {}
        for c in df.columns:
            sc = str(c).strip()
            if sc in ("代號", "證券代號", "StockCode"):
                rename_map[c] = "股票代號"
            if sc in ("收盤", "收盤價", "Close", "close"):
                rename_map[c] = "收盤價"
        if rename_map:
            df.rename(columns=rename_map, inplace=True)
    df["股票代號"] = df["股票代號"].astype(str).str.strip()
    if "收盤價" in df.columns:
        df["收盤價"] = pd.to_numeric(df["收盤價"], errors="coerce")

def _load_prices_for(date_str: str) -> pd.DataFrame:
    # 優先讀 prices/<date>.csv；若不存在，往前尋找最近的一天（最多回溯 90 天）
    p = PRICE_DIR / f"{date_str}.csv"
    if p.exists():
        df = pd.read_csv(p, encoding="utf-8-sig")
        _normalize_price_df(df)
        return df[["股票代號", "收盤價"]].copy()

    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    for _ in range(90):
        d = d - timedelta(days=1)
        q = PRICE_DIR / f"{d}.csv"
        if q.exists():
            df = pd.read_csv(q, encoding="utf-8-sig")
            _normalize_price_df(df)
            return df[["股票代號", "收盤價"]].copy()

    return pd.DataFrame(columns=["股票代號", "收盤價"])

def _merge_price(df_today: pd.DataFrame, date_str: str) -> pd.DataFrame:
    px = _load_prices_for(date_str)
    if px.empty:
        df_today["收盤價"] = pd.NA
        return df_today
    return df_today.merge(px, on="股票代號", how="left")

# ------------------------- 基期挑選 ------------------------- #
def _find_prev_by_listing(today_str: str, base_dir: Path):
    # 在 base_dir/*.csv 內，挑選「日期 < today_str」的最近一份。
    files = sorted(Path(base_dir).glob("*.csv"), key=lambda p: p.name)
    cands = [p for p in files if p.stem < today_str]
    if not cands:
        return None, None
    prev = cands[-1]
    return prev.stem, prev

# ------------------------- 主流程 ------------------------- #
def main():
    raw = os.getenv("REPORT_DATE")
    if raw:
        # 顯式指定日期 → 正規化 → 選 base_dir
        today_str = _normalize_report_date(raw)
        base_dir = _choose_base_dir(today_str)
    else:
        # 預設使用最新快照（無則回退 data/）
        today_str, base_dir = _pick_default_date_and_base()

    today_path = base_dir / f"{today_str}.csv"
    if not today_path.exists():
        raise FileNotFoundError(f"找不到今日 CSV：{today_path}")

    prev_str, prev_path = _find_prev_by_listing(today_str, base_dir)
    if not prev_path:
        raise RuntimeError(f"找不到 {today_str} 之前的可用 CSV 作為比較基期（於 {base_dir}）")

    print(f"[build] today={today_str}, prev={prev_str}, base_dir={base_dir}")

    df_t = _read_csv(today_path)
    df_y = _read_csv(prev_path)

    # 合併價格（若無今日價，會自動回補最近一筆）
    df_t = _merge_price(df_t, today_str)

    # 對齊今昨並計算變動
    key = ["股票代號", "股票名稱"]
    dfm = pd.merge(df_t, df_y, on=key, how="outer", suffixes=("_今", "_昨"))

    for c in ["股數_今", "股數_昨"]:
        dfm[c] = pd.to_numeric(dfm.get(c), errors="coerce").fillna(0).astype(int)
    for c in ["持股權重_今", "持股權重_昨"]:
        dfm[c] = pd.to_numeric(dfm.get(c), errors="coerce").fillna(0.0)

    dfm["買賣超股數"] = dfm["股數_今"] - dfm["股數_昨"]
    dfm["權重Δ%"]   = (dfm["持股權重_今"] - dfm["持股權重_昨"]).round(4)

    # 門檻（可由環境覆寫）
    NEW_MIN = float(os.getenv("NEW_HOLDING_MIN_WEIGHT", "0.4"))  # 首次新增持股最小權重
    SELL_MAX = float(os.getenv("SELL_ALERT_MAX_WEIGHT", "0.1"))  # 賣出警示門檻
    NOISE    = float(os.getenv("NOISE_THRESHOLD", "0.01"))       # 噪音門檻（百分點）
    TOPN     = int(os.getenv("TOP_N", "10"))

    # 新增/賣出標記
    new_mask  = (dfm["持股權重_昨"] <= 0.0 + 1e-12) & (dfm["持股權重_今"] >= NEW_MIN)
    sell_mask = (dfm["持股權重_今"] <= SELL_MAX) & (dfm["持股權重_昨"] > NOISE)

    # 輸出表格（依規格命名與欄位）
    today_col = f"股數_{today_str}"
    prev_col  = f"股數_{prev_str}"
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

    # 摘要資料
    movers = dfm.copy()
    movers["abs"] = movers["權重Δ%"].abs()
    movers = movers[movers["abs"] >= NOISE]
    d1_up = movers.sort_values("權重Δ%", ascending=False).head(TOPN)
    d1_dn = movers.sort_values("權重Δ%", ascending=True).head(TOPN)

    # 近 5 個可用日期（含今日）
    files = sorted(Path(base_dir).glob("*.csv"), key=lambda p: p.name)
    upto_today = [p for p in files if p.stem <= today_str]
    last5 = upto_today[-5:] if upto_today else [today_path]
    last5_dates = [p.stem for p in last5]

    top10_sum = df_t.sort_values("持股權重", ascending=False).head(10)["持股權重"].sum()
    top1 = df_t.sort_values("持股權重", ascending=False).head(1)[["股票代號","股票名稱","持股權重"]].iloc[0]

    summary = {
        "date": today_str,
        "baseline_date": prev_str,
        "total_count": int(df_t["股票代號"].nunique()),
        "top10_sum": round(float(top10_sum), 4),
        "top_weight": {
            "code": str(top1["股票代號"]),
            "name": str(top1["股票名稱"]),
            "weight": round(float(top1["持股權重"]), 4),
        },
        "new_holdings_min": NEW_MIN,
        "new_holdings": d1_up[new_mask][["股票代號","股票名稱","持股權重_今"]]
            .rename(columns={"持股權重_今":"今日權重%"}).to_dict(orient="records"),
        "sell_alert_max": SELL_MAX,
        "sell_alerts": movers[sell_mask][["股票代號","股票名稱","持股權重_昨","持股權重_今","權重Δ%"]]
            .to_dict(orient="records"),
        "d1_up": d1_up[["股票代號","股票名稱","持股權重_昨","持股權重_今","權重Δ%"]]
            .to_dict(orient="records"),
        "d1_dn": d1_dn[["股票代號","股票名稱","持股權重_昨","持股權重_今","權重Δ%"]]
            .to_dict(orient="records"),
        "last5_dates": last5_dates,
        "table_columns": ["股票代號","股票名稱","收盤價",
                          today_col,"今日權重%", prev_col,"昨日權重%","買賣超股數","權重Δ%"],
    }

    with open(REPORT_DIR / f"summary_{today_str}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"[build] saved: {out_path}")
    print(f"[build] saved: reports/summary_{today_str}.json")

if __name__ == "__main__":
    main()
