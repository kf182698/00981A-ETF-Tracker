# fetch_snapshot.py — 用 Playwright 模擬瀏覽器下載官方 XLSX（多重保險，確保拿到檔案）
import os, re, io
from pathlib import Path
from datetime import datetime
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

INFO_URL = "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW"
ARCHIVE = Path("archive")

def _date_str() -> str:
    raw = (os.getenv("REPORT_DATE") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw): return raw
    if re.fullmatch(r"\d{8}", raw): return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return datetime.now().strftime("%Y-%m-%d")

def _save_to_xlsx_bytes(content: bytes, out_xlsx: Path):
    """
    將官方下載內容（xlsx 或 csv）讀入 DataFrame，標準化欄位，最後輸出成
    archive/YYYY-MM/ETF_Investment_Portfolio_YYYYMMDD.xlsx
    （含 holdings 與 with_prices 兩張工作表）
    """
    # 嘗試 xlsx（OpenXML ZIP 的魔術字頭 PK\x03\x04）
    df = None
    if content[:4] == b"PK\x03\x04":
        df = pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype={"股票代號": str})
    else:
        # 可能是 CSV（UTF-8/Big5）
        for enc in ("utf-8-sig", "utf-8", "cp950", "big5-hkscs"):
            try:
                df = pd.read_csv(io.BytesIO(content), encoding=enc, dtype={"股票代號": str})
                break
            except Exception:
                continue
        if df is None:
            # 有些站會回 HTML；此處不再嘗試解析 HTML，直接丟錯，避免誤判
            raise SystemExit("官方下載回應非有效檔案（非 XLSX/CSV）。")

    # 欄位標準化
    rename = {}
    for c in df.columns:
        s = str(c)
        if any(k in s for k in ["股票代號","證券代號","股票代碼","代號"]): rename[c] = "股票代號"
        elif any(k in s for k in ["股票名稱","個股名稱","名稱"]):          rename[c] = "股票名稱"
        elif any(k in s for k in ["持股權重","投資比例","比重","權重"]):     rename[c] = "持股權重"
        elif any(k in s for k in ["股數","持有股數"]):                      rename[c] = "股數"
    if rename: df.rename(columns=rename, inplace=True)

    cols = [c for c in ["股票代號","股票名稱","股數","持股權重"] if c in df.columns]
    if not cols:
        raise SystemExit("下載檔案中沒有可辨識欄位。")
    df = df[cols].copy()

    if "股票代號" not in df.columns and "股票名稱" in df.columns:
        df["股票代號"] = df["股票名稱"].astype(str).str.extract(r"(\d{4})", expand=False)

    if "股票名稱" in df.columns:
        df["股票名稱"] = df["股票名稱"].astype(str).str.replace(r"\(\d{4}\)","",regex=True).str.strip()
    if "股數" in df.columns:
        df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)
    if "持股權重" in df.columns:
        df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)

    df["股票代號"] = df["股票代號"].astype(str).str.extract(r"(\d{4})", expand=False)
    df = df.dropna(subset=["股票代號"]).drop_duplicates("股票代號").sort_values("股票代號").reset_index(drop=True)

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="holdings", index=False)
        df2 = df.copy(); df2["收盤價"] = pd.NA
        df2.to_excel(w, sheet_name="with_prices", index=False)
    return len(df)

def fetch_snapshot():
    date = _date_str()
    yyyymm = date[:7]
    yyyymmdd = date.replace("-","")
    outdir = ARCHIVE / yyyymm
    outdir.mkdir(parents=True, exist_ok=True)
    out_xlsx = outdir / f"ETF_Investment_Portfolio_{yyyymmdd}.xlsx"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(accept_downloads=True, locale="zh-TW")
        page = ctx.new_page()
        page.goto(INFO_URL, wait_until="domcontentloaded", timeout=60000)

        # 若有分頁/頁籤，先切到「持股明細」之類的 tab（容錯）
        for tab_text in ["持股", "持股明細", "成分股", "投資組合"]:
            try:
                if page.locator(f'text="{tab_text}"').count() > 0:
                    page.locator(f'text="{tab_text}"').first.click(timeout=2000)
                    break
            except Exception:
                pass

        # 1) 嘗試直接讀取連結 href
        href = None
        for sel in [
            'a:has-text("下載")',
            'a[download]',
            'a[href*="Download"]',
            'a[role="button"]:has-text("下載")',
            'button:has-text("下載")'
        ]:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0:
                    # 有些是 <a href="...xlsx">下載</a>
                    h = loc.get_attribute("href")
                    if h and h != "javascript:void(0)":
                        href = h
                        break
            except Exception:
                continue
        if href and href.startswith("/"):
            href = "https://www.ezmoney.com.tw" + href

        # 2) 先走 download event 與 response 監聽（雙保險）
        content_bytes = None
        if not href:
            try:
                with page.expect_download(timeout=45000) as dl_info:
                    # 可能的真按鈕
                    clicked = False
                    for sel in [
                        'a:has-text("下載")',
                        'button:has-text("下載")',
                        'text=下載'
                    ]:
                        if page.locator(sel).count() > 0:
                            page.locator(sel).first.click()
                            clicked = True
                            break
                    if not clicked:
                        # 若前面沒找到，嘗試任何包含 Download 的連結
                        if page.locator('a[href*="Download"]').count() > 0:
                            page.locator('a[href*="Download"]').first.click()
                            clicked = True
                    if not clicked:
                        raise PWTimeout("找不到可點擊的下載按鈕")
                download = dl_info.value
                content_bytes = download.content()
            except PWTimeout:
                # 改為等回應（API 下載）
                try:
                    resp = page.wait_for_response(lambda r: "Download" in r.url and ("fundCode=49YTW" in r.url or r.url.endswith((".xlsx",".csv"))), timeout=45000)
                    href = resp.url
                except PWTimeout:
                    pass

        # 3) 若拿到 href，用同一個 context 的 request 抓檔（保留 cookie/headers）
        if content_bytes is None and href:
            r = ctx.request.get(href, headers={"Referer": INFO_URL})
            if not r.ok:
                raise SystemExit(f"下載失敗：{r.status} {r.status_text()}")
            content_bytes = r.body()

        browser.close()

    if content_bytes is None or len(content_bytes) < 200:  # 太小多半是錯誤頁
        raise SystemExit("官方下載回應非有效資料表（可能為 HTML 錯誤頁或格式變動）。")

    rows = _save_to_xlsx_bytes(content_bytes, out_xlsx)
    print(f"[fetch] saved {out_xlsx} rows={rows}")

if __name__ == "__main__":
    fetch_snapshot()