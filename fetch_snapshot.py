# fetch_snapshot.py — 官方 API 直接下載，兼容 CSV / XLSX
import os, re, requests
from pathlib import Path
from datetime import datetime
import pandas as pd
import io

ARCHIVE = Path("archive")
ARCHIVE.mkdir(exist_ok=True)
OFFICIAL_API = "https://www.ezmoney.com.tw/ETF/Fund/DownloadHoldingFile?fundCode=49YTW"

def today_str() -> str:
    raw = (os.getenv("REPORT_DATE") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw): return raw
    if re.fullmatch(r"\d{8}", raw): return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return datetime.now().strftime("%Y-%m-%d")

def fetch_snapshot():
    date_str = today_str()
    yyyymm   = date_str[:7]
    yyyymmdd = date_str.replace("-", "")

    outdir = ARCHIVE / yyyymm
    outdir.mkdir(parents=True, exist_ok=True)
    out_xlsx = outdir / f"ETF_Investment_Portfolio_{yyyymmdd}.xlsx"

    # 下載檔案
    resp = requests.get(OFFICIAL_API, timeout=60)
    resp.raise_for_status()
    content_type = resp.headers.get("Content-Type","").lower()

    # 嘗試解析
    df = None
    if "csv" in content_type or resp.content.startswith(b"\xef\xbb\xbf") or resp.content[:1] in [b'c', b'C']:
        # CSV 檔
        df = pd.read_csv(io.BytesIO(resp.content), dtype={"股票代號": str})
    else:
        # 預設嘗試 Excel
        try:
            df = pd.read_excel(io.BytesIO(resp.content), engine="openpyxl", dtype={"股票代號": str})
        except Exception:
            # 如果失敗，再 fallback CSV
            df = pd.read_csv(io.BytesIO(resp.content), dtype={"股票代號": str})

    # 欄位標準化
    rename = {}
    for c in df.columns:
        s = str(c)
        if "股票代號" in s or "證券代號" in s: rename[c] = "股票代號"
        elif "股票名稱" in s or "名稱" in s:  rename[c] = "股票名稱"
        elif "股數" in s:                   rename[c] = "股數"
        elif "持股權重" in s or "投資比例" in s or "比重" in s: rename[c] = "持股權重"
    if rename: df.rename(columns=rename, inplace=True)

    cols = [c for c in ["股票代號","股票名稱","股數","持股權重"] if c in df.columns]
    df = df[cols].copy()
    if "股數" in df.columns:
        df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)
    if "持股權重" in df.columns:
        df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)

    # 存成 XLSX，包含 holdings / with_prices 兩張 sheet
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="holdings", index=False)
        df2 = df.copy(); df2["收盤價"] = pd.NA
        df2.to_excel(w, sheet_name="with_prices", index=False)

    print(f"[fetch] saved {out_xlsx} rows={len(df)}")
    return out_xlsx

if __name__ == "__main__":
    fetch_snapshot()