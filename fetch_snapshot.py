# fetch_snapshot.py — 官方 API 直接下載 XLSX
# 來源：統一投信 00981A 官方 API (ezmoney)
import os, re, requests
from pathlib import Path
from datetime import datetime
import pandas as pd

ARCHIVE = Path("archive")
ARCHIVE.mkdir(exist_ok=True)
# 官方 XLSX 下載 API
OFFICIAL_XLSX_API = "https://www.ezmoney.com.tw/ETF/Fund/DownloadHoldingFile?fundCode=49YTW"

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

    # 直接下載 XLSX
    resp = requests.get(OFFICIAL_XLSX_API, timeout=60)
    resp.raise_for_status()
    with open(out_xlsx, "wb") as f:
        f.write(resp.content)

    # 加上 holdings / with_prices 工作表
    df = pd.read_excel(out_xlsx, sheet_name=0, dtype={"股票代號": str})
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

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="holdings", index=False)
        df2 = df.copy(); df2["收盤價"] = pd.NA
        df2.to_excel(w, sheet_name="with_prices", index=False)

    print(f"[fetch] saved {out_xlsx} rows={len(df)}")
    return out_xlsx

if __name__ == "__main__":
    fetch_snapshot()