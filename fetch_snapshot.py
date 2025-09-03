# fetch_snapshot.py — 官方頁下載XLSX，轉存至 archive/
import os, re
from pathlib import Path
from datetime import datetime
import pandas as pd

from playwright.sync_api import sync_playwright

OFFICIAL_URL = "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW"
ARCHIVE_DIR = Path("archive")

def _today_str() -> str:
    raw = (os.getenv("REPORT_DATE") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return raw
    if re.fullmatch(r"\d{8}", raw):
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return datetime.now().strftime("%Y-%m-%d")

def fetch_and_download(date_str: str) -> Path:
    """用 Playwright 模擬瀏覽器 → 點擊下載 → 存檔"""
    yyyymm   = date_str[:7]
    yyyymmdd = date_str.replace("-", "")

    outdir = ARCHIVE_DIR / yyyymm
    outdir.mkdir(parents=True, exist_ok=True)
    out_xlsx = outdir / f"ETF_Investment_Portfolio_{yyyymmdd}.xlsx"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(accept_downloads=True)
        page = ctx.new_page()
        page.goto(OFFICIAL_URL, wait_until="domcontentloaded", timeout=60000)

        # 等待下載按鈕 (文字可能是「下載」或有 .btn-download 類別)
        try:
            page.wait_for_selector("text=下載", timeout=20000)
            with page.expect_download() as dl_info:
                page.click("text=下載")
            download = dl_info.value
            download.save_as(out_xlsx)
        except Exception as e:
            browser.close()
            raise SystemExit(f"下載失敗: {e}")

        browser.close()
    return out_xlsx

def convert_to_with_prices(xlsx_path: Path):
    """將下載的 XLSX 檔案，複製出 holdings / with_prices 兩張表"""
    # 讀入第一個 sheet（官方檔案通常就是 holdings）
    df = pd.read_excel(xlsx_path, sheet_name=0, dtype={"股票代號": str})

    # 欄位清理
    rename = {}
    for c in df.columns:
        s = str(c)
        if "股票代號" in s or "證券代號" in s: rename[c] = "股票代號"
        elif "股票名稱" in s or "名稱" in s:  rename[c] = "股票名稱"
        elif "股數" in s:                   rename[c] = "股數"
        elif "持股權重" in s or "投資比例" in s or "比重" in s: rename[c] = "持股權重"
    if rename:
        df.rename(columns=rename, inplace=True)

    # 只保留主要欄位
    cols = [c for c in ["股票代號","股票名稱","股數","持股權重"] if c in df.columns]
    df = df[cols].copy()

    # 數字清理
    if "股數" in df.columns:
        df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)
    if "持股權重" in df.columns:
        df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)

    # 重寫檔案：sheet1=holdings, sheet2=with_prices
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="holdings", index=False)
        df2 = df.copy(); df2["收盤價"] = pd.NA
        df2.to_excel(w, sheet_name="with_prices", index=False)

def main():
    date_str = _today_str()
    xlsx_path = fetch_and_download(date_str)
    convert_to_with_prices(xlsx_path)
    print(f"[fetch] saved {xlsx_path}")

if __name__ == "__main__":
    main()