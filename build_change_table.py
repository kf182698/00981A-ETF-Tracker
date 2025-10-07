# build_change_table.py — 以 data/REPORT_DATE.csv 與 data_snapshots 中「報告日前最後一筆」比較
# 產出：reports/ 內的表格與摘要（此檔只負責資料計算與輸出 CSV/MD，由你的寄信程式再組信）
import os, glob
from pathlib import Path
import pandas as pd

OUT_DIR = Path("reports")
OUT_DIR.mkdir(exist_ok=True, parents=True)

def _report_date() -> str:
    d = (os.getenv("REPORT_DATE") or "").strip()
    if len(d) == 8 and d.isdigit(): return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d  # 已是 YYYY-MM-DD

def _load_df(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    # 欄位保險處理
    rename = {}
    for c in df.columns:
        s = str(c)
        if any(k in s for k in ["股票代號","證券代號","股票代碼","代號"]): rename[c] = "股票代號"
        elif any(k in s for k in ["股票名稱","個股名稱","名稱"]):          rename[c] = "股票名稱"
        elif any(k in s for k in ["持股權重","投資比例","比重","權重"]):     rename[c] = "持股權重"
        elif any(k in s for k in ["股數","持有股數"]):                      rename[c] = "股數"
    if rename: df.rename(columns=rename, inplace=True)
    if "股票代號" not in df.columns:
        # 從名稱嘗試抓 4 碼
        import re
        if "股票名稱" in df.columns:
            df["股票代號"] = df["股票名稱"].astype(str).str.extract(r"([1-9]\d{3})", expand=False)
        else:
            any_text = df.astype(str).agg(" ".join, axis=1)
            df["股票代號"] = any_text.str.extract(r"([1-9]\d{3})", expand=False)
    df["股票代號"] = df["股票代號"].astype(str).str.extract(r"([1-9]\d{3})", expand=False)
    df = df.dropna(subset=["股票代號"])
    if "股票名稱" not in df.columns: df["股票名稱"] = ""
    if "股數" not in df.columns: df["股數"] = 0
    if "持股權重" not in df.columns: df["持股權重"] = 0.0
    df["股票名稱"] = df["股票名稱"].astype(str).str.strip()
    df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)
    df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)
    # 權重缺 -> 以股數占比回推
    if df["持股權重"].sum() == 0 and df["股數"].sum() > 0:
        total = df["股數"].sum()
        df["持股權重"] = (df["股數"] / total * 100).round(6)
    df = df[["股票代號","股票名稱","股數","持股權重"]].drop_duplicates("股票代號")
    return df.sort_values("股票代號").reset_index(drop=True)

def _find_prev_snapshot(report_date: str) -> Path:
    snaps = sorted(glob.glob("data_snapshots/*.csv"))
    prev_path = None
    for p in reversed(snaps):
        name = Path(p).stem  # YYYY-MM-DD
        if name < report_date:
            prev_path = Path(p); break
    if prev_path is None:
        raise RuntimeError(f"找不到 {report_date} 之前的可用 CSV 作為比較基期（於 data_snapshots）")
    return prev_path

def _load_prices(report_date: str) -> pd.DataFrame:
    """從 prices/ 目錄讀取今日收盤價（及可能的昨日收盤價）"""
    prices_today = Path("prices") / f"{report_date}.csv"
    if not prices_today.exists():
        print(f"[警告] 找不到今日價格檔 {prices_today}，收盤價欄位將為空")
        return pd.DataFrame(columns=["股票代號", "今日收盤價"])
    
    df_price = pd.read_csv(prices_today, encoding="utf-8-sig")
    # 標準化欄位名稱
    rename_map = {}
    for col in df_price.columns:
        col_str = str(col)
        if any(k in col_str for k in ["股票代號", "證券代號", "代號", "代碼"]):
            rename_map[col] = "股票代號"
        elif any(k in col_str for k in ["收盤價", "收盤", "close", "Close"]):
            rename_map[col] = "今日收盤價"
    
    if rename_map:
        df_price.rename(columns=rename_map, inplace=True)
    
    # 確保必要欄位存在
    if "股票代號" not in df_price.columns or "今日收盤價" not in df_price.columns:
        print(f"[警告] 價格檔欄位不符，需包含股票代號與收盤價")
        return pd.DataFrame(columns=["股票代號", "今日收盤價"])
    
    # 清理資料
    df_price["股票代號"] = df_price["股票代號"].astype(str).str.extract(r"([1-9]\d{3})", expand=False)
    df_price = df_price.dropna(subset=["股票代號"])
    df_price["今日收盤價"] = pd.to_numeric(df_price["今日收盤價"], errors="coerce")
    
    return df_price[["股票代號", "今日收盤價"]].drop_duplicates("股票代號")

def _load_prices_yesterday(prev_date: str) -> pd.DataFrame:
    """從 prices/ 目錄讀取昨日收盤價（選配）"""
    prices_yesterday = Path("prices") / f"{prev_date}.csv"
    if not prices_yesterday.exists():
        return pd.DataFrame(columns=["股票代號", "昨日收盤價"])
    
    df_price = pd.read_csv(prices_yesterday, encoding="utf-8-sig")
    # 標準化欄位名稱
    rename_map = {}
    for col in df_price.columns:
        col_str = str(col)
        if any(k in col_str for k in ["股票代號", "證券代號", "代號", "代碼"]):
            rename_map[col] = "股票代號"
        elif any(k in col_str for k in ["收盤價", "收盤", "close", "Close"]):
            rename_map[col] = "昨日收盤價"
    
    if rename_map:
        df_price.rename(columns=rename_map, inplace=True)
    
    # 確保必要欄位存在
    if "股票代號" not in df_price.columns or "昨日收盤價" not in df_price.columns:
        return pd.DataFrame(columns=["股票代號", "昨日收盤價"])
    
    # 清理資料
    df_price["股票代號"] = df_price["股票代號"].astype(str).str.extract(r"([1-9]\d{3})", expand=False)
    df_price = df_price.dropna(subset=["股票代號"])
    df_price["昨日收盤價"] = pd.to_numeric(df_price["昨日收盤價"], errors="coerce")
    
    return df_price[["股票代號", "昨日收盤價"]].drop_duplicates("股票代號")

def main():
    report_date = _report_date()
    if not report_date:
        raise SystemExit("REPORT_DATE 未設定")
    
    today_csv = Path("data")/f"{report_date}.csv"
    if not today_csv.exists():
        raise FileNotFoundError(f"找不到今日 CSV：{today_csv}")
    
    prev_csv = _find_prev_snapshot(report_date)
    prev_date = prev_csv.stem  # 取得昨日日期 YYYY-MM-DD
    
    df_t = _load_df(today_csv).rename(columns={"股數":"今日股數","持股權重":"今日權重%"})
    df_y = _load_df(prev_csv).rename(columns={"股數":"昨日股數","持股權重":"昨日權重%"})
    
    df = pd.merge(df_t, df_y, on=["股票代號"], how="outer")
    df["股票名稱"] = df["股票名稱_x"].fillna(df["股票名稱_y"]).fillna("")
    df.drop(columns=["股票名稱_x","股票名稱_y"], inplace=True)
    
    for col in ["今日股數","昨日股數","今日權重%","昨日權重%"]:
        if col not in df.columns: df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    df["買賣超股數"] = (df["今日股數"] - df["昨日股數"]).astype(int)
    df["權重Δ%"]   = (df["今日權重%"] - df["昨日權重%"]).round(2)
    df["首次買進"] = (df["昨日股數"]==0) & (df["今日股數"]>0)
    df["關鍵賣出"] = (df["昨日股數"]>0) & (df["今日股數"]==0)
    
    # === 新增：合併今日收盤價 ===
    df_price_today = _load_prices(report_date)
    if not df_price_today.empty:
        df = pd.merge(df, df_price_today, on="股票代號", how="left")
    else:
        df["今日收盤價"] = None
    
    # === 新增：合併昨日收盤價（選配）===
    df_price_yesterday = _load_prices_yesterday(prev_date)
    if not df_price_yesterday.empty:
        df = pd.merge(df, df_price_yesterday, on="股票代號", how="left")
    else:
        df["昨日收盤價"] = None
    
    # 輸出結果
    out_csv = OUT_DIR / f"change_table_{report_date}.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[build] saved {out_csv}  rows={len(df)}")

if __name__ == "__main__":
    main()
