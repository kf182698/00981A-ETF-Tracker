# fetch_snapshot.py — 用 Playwright 模擬瀏覽器下載官方 XLSX（修正版：用 context.wait_for_event 捕捉 response）
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
    # 嘗試 xlsx（OpenXML ZIP 魔術字頭 PK\x03\x04），否則以多編碼讀 CSV
    if content[:4] == b"PK\x03\x04":
        df = pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype={"股票代號": str})
    else:
        df = None
        for enc in ("utf-8-sig", "utf-8", "cp950", "big5-hkscs"):
            try:
                df = pd.read_csv(io.BytesIO(content), encoding=enc, dtype={"股票代號": str})
                break
            except Exception:
                continue
        if df is None:
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

        # 嘗試先找到直接的下載 href
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
                    h = loc.get_attribute("href")
                    if h and h != "javascript:void(0)":
                        href = h
                        break
            except Exception:
                continue

        if href and href.startswith("/"):
            href = "https://www.ezmoney.com.tw" + href

        content_bytes = None

        # A) 有 href：直接用同一個 context 的 request 取檔（保留 cookie/headers）
        if href:
            r = ctx.request.get(href, headers={"Referer": INFO_URL})
            if not r.ok:
                raise SystemExit(f"下載失敗：{r.status} {r.status_text()}")
            content_bytes = r.body()
        else:
            # B) 沒有 href：點擊「下載」，用 context.wait_for_event('response') 捕捉 Download API
            # 先嘗試觸發按鈕
            clicked = False
            for sel in ['a:has-text("下載")','button:has-text("下載")','text=下載']:
                try:
                    if page.locator(sel).count() > 0:
                        page.locator(sel).first.click()
                        clicked = True
                        break
                except Exception:
                    continue
            if not clicked and page.locator('a[href*="Download"]').count() > 0:
                page.locator('a[href*="Download"]').first.click()
                clicked = True

            if not clicked:
                browser.close()
                raise SystemExit("找不到可點擊的下載按鈕")

            # 等待符合條件的回應
            try:
                resp = ctx.wait_for_event(
                    "response",
                    predicate=lambda r: (
                        "Download" in r.url
                        and ("fundCode=49YTW" in r.url or r.url.endswith((".xlsx", ".csv")))
                        and r.status == 200
                    ),
                    timeout=45000
                )
                content_bytes = resp.body()
            except PWTimeout:
                # 某些站觸發原生下載（非 XHR），改用 download 事件當後備
                try:
                    with page.expect_download(timeout=20000) as dl_info:
                        # 再點一次
                        if page.locator('text=下載').count() > 0:
                            page.locator('text=下載').first.click()
                    download = dl_info.value
                    content_bytes = download.content()
                except PWTimeout:
                    browser.close()
                    raise SystemExit("等待下載/回應逾時，可能頁面流程已變更。")

        browser.close()

    if content_bytes is None or len(content_bytes) < 200:
        raise SystemExit("官方下載回應非有效資料表（可能為 HTML 錯誤頁或格式變動）。")

    rows = _save_to_xlsx_bytes(content_bytes, out_xlsx)
    print(f"[fetch] saved {out_xlsx} rows={rows}")

if __name__ == "__main__":
    fetch_snapshot()