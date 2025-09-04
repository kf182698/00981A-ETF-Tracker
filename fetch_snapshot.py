# fetch_snapshot.py — 官方頁下載：iframe/彈窗/延遲 全面處理；抓不到時用 cookies 直打 API
import os, re, io, json
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

INFO_URL = "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW"
DOWNLOAD_API = "https://www.ezmoney.com.tw/ETF/Fund/DownloadHoldingFile?fundCode=49YTW"
ARCHIVE = Path("archive")
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"

# ---------- 日期與存檔 ----------
def _date_str() -> str:
    raw = (os.getenv("REPORT_DATE") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw): return raw
    if re.fullmatch(r"\d{8}", raw): return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return datetime.now().strftime("%Y-%m-%d")

def _out_path(date_str: str) -> Path:
    yyyymm = date_str[:7]
    yyyymmdd = date_str.replace("-", "")
    outdir = ARCHIVE / yyyymm
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir / f"ETF_Investment_Portfolio_{yyyymmdd}.xlsx"

# ---------- 內容儲存（XLSX/CSV 皆可） ----------
def _save_to_xlsx_bytes(content: bytes, out_xlsx: Path) -> int:
    # 嘗試 Excel（OpenXML zip 魔術字頭）
    if content[:4] == b"PK\x03\x04":
        df = pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype={"股票代號": str})
    else:
        # 依序試 CSV 編碼
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

# ---------- Playwright 下載（含 iframe 與彈窗處理） ----------
def _try_click_download_and_capture(page, ctx, timeout_ms=90000) -> bytes | None:
    # 先等更完整的載入狀態
    try:
        page.wait_for_load_state("networkidle", timeout=30000)
    except Exception:
        pass

    # 處理可能的 cookie/同意彈窗
    for txt in ["同意", "我知道了", "接受", "關閉"]:
        try:
            loc = page.locator(f'text="{txt}"')
            if loc.count() > 0:
                loc.first.click(timeout=1000)
                break
        except Exception:
            pass

    # 構造「主頁 + 所有 iframe」查找器
    def all_frames():
        yield page
        for f in page.frames:
            yield f

    # 1) 嘗試直接拿 a[href*="Download"] 的 href
    href = None
    for f in all_frames():
        try:
            loc = f.locator('a[href*="Download"]').first
            if loc.count() > 0:
                h = loc.get_attribute("href")
                if h and h != "javascript:void(0)":
                    href = h
                    break
        except Exception:
            continue
    if href and href.startswith("/"):
        href = "https://www.ezmoney.com.tw" + href
    if href:
        r = ctx.request.get(href, headers={"Referer": INFO_URL})
        if r.ok:
            return r.body()

    # 2) 沒有直接 href：嘗試點擊「下載」並在 Context 層等待 response
    # 找可點擊的目標（主頁與各 iframe）
    click_targets = []
    sels = ['a:has-text("下載")','button:has-text("下載")','text=下載','a[role="button"]:has-text("下載")']
    for f in all_frames():
        for sel in sels:
            try:
                if f.locator(sel).count() > 0:
                    click_targets.append((f, sel))
            except Exception:
                continue

    # 去重（frame, selector）並逐一嘗試
    seen = set()
    for f, sel in click_targets:
        key = (id(f), sel)
        if key in seen: continue
        seen.add(key)

        try:
            # 先準備 Context 層級 response 監聽
            resp = ctx.wait_for_event(
                "response",
                predicate=lambda r: ("Download" in r.url and ("fundCode=49YTW" in r.url or r.url.endswith((".xlsx",".csv")))) and r.status == 200,
                timeout=timeout_ms
            )
        except PWTimeout:
            resp = None

        # 觸發點擊
        try:
            f.locator(sel).first.click(timeout=2000)
        except Exception:
            continue

        # 若已經先掛 wait_for_event，需再次等待實際回應
        if resp is None:
            try:
                resp = ctx.wait_for_event(
                    "response",
                    predicate=lambda r: ("Download" in r.url and ("fundCode=49YTW" in r.url or r.url.endswith((".xlsx",".csv")))) and r.status == 200,
                    timeout=timeout_ms
                )
            except PWTimeout:
                # 嘗試原生 Download 事件後備
                try:
                    with f.page.expect_download(timeout=20000) as dl_info:
                        # 再點一次
                        try:
                            f.locator(sel).first.click(timeout=1000)
                        except Exception:
                            pass
                    download = dl_info.value
                    return download.content()
                except PWTimeout:
                    continue

        if resp:
            try:
                return resp.body()
            except Exception:
                continue

    return None

# ---------- 直打 API：用瀏覽器 cookies 組合 Cookie header ----------
def _fallback_download_with_cookies(ctx) -> bytes | None:
    # 從瀏覽器情境取 cookies
    state = ctx.storage_state()
    if isinstance(state, str):
        state = json.loads(state)
    cookies = state.get("cookies", [])
    jar = []
    for c in cookies:
        # 只要 ezmoney 網域的 cookie
        dom = c.get("domain") or ""
        if "ezmoney.com.tw" in dom:
            jar.append(f"{c['name']}={c['value']}")
    cookie_header = "; ".join(jar)

    headers = {"User-Agent": UA, "Referer": INFO_URL}
    if cookie_header:
        headers["Cookie"] = cookie_header

    try:
        r = requests.get(DOWNLOAD_API, headers=headers, timeout=60)
        if r.ok and len(r.content) > 200:
            return r.content
    except Exception:
        pass
    return None

# ---------- 主程式 ----------
def fetch_snapshot():
    date = _date_str()
    out_xlsx = _out_path(date)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(accept_downloads=True, locale="zh-TW", user_agent=UA)
        page = ctx.new_page()
        page.goto(INFO_URL, wait_until="domcontentloaded", timeout=60000)

        # 嘗試點擊/捕捉
        content = None
        try:
            content = _try_click_download_and_capture(page, ctx, timeout_ms=90000)
        except Exception:
            content = None

        # 失敗則以 cookies 直打 API
        if content is None:
            content = _fallback_download_with_cookies(ctx)

        browser.close()

    if content is None or len(content) < 200:
        raise SystemExit("官方下載回應非有效資料表（流程可能變更）。")

    rows = _save_to_xlsx_bytes(content, out_xlsx)
    print(f"[fetch] saved {out_xlsx} rows={rows}")

if __name__ == "__main__":
    fetch_snapshot()