# fetch_snapshot.py — 官方頁抓取（下載失敗時，改抓已渲染 DOM 表格）
import os, re, io, json
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
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

# ---------- 內容整理（XLSX/CSV/HTML 轉 DataFrame） ----------
def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    # 攤平成單層欄
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["".join([str(x) for x in tup if str(x) != "nan"]).strip() for tup in df.columns.values]
    df.columns = [str(c).strip() for c in df.columns]
    # 欄位映射
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
        return pd.DataFrame(columns=["股票代號","股票名稱","股數","持股權重"])
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
    return df

def _save_xlsx(df: pd.DataFrame, out_xlsx: Path) -> int:
    if df.empty:
        raise SystemExit("官方頁仍未取得任何有效持股列。")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="holdings", index=False)
        df2 = df.copy(); df2["收盤價"] = pd.NA
        df2.to_excel(w, sheet_name="with_prices", index=False)
    return len(df)

def _bytes_to_df(content: bytes) -> pd.DataFrame | None:
    # XLSX（OpenXML zip 魔術字頭）
    if content[:4] == b"PK\x03\x04":
        try:
            return _normalize(pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype={"股票代號": str}))
        except Exception:
            pass
    # CSV 編碼嘗試
    for enc in ("utf-8-sig","utf-8","cp950","big5-hkscs"):
        try:
            return _normalize(pd.read_csv(io.BytesIO(content), encoding=enc, dtype={"股票代號": str}))
        except Exception:
            continue
    return None

def _html_to_df(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    candidates = []
    # 把頁面所有 table 丟給 pandas
    for t in soup.find_all("table"):
        try:
            for df in pd.read_html(io.StringIO(str(t))):
                candidates.append(df)
        except Exception:
            continue
    # 全頁再試一次
    try:
        for df in pd.read_html(io.StringIO(html)):
            candidates.append(df)
    except Exception:
        pass
    # 遴選最像持股表的一張
    for df in candidates:
        df2 = _normalize(df)
        if not df2.empty and {"股票代號","股票名稱"}.issubset(df2.columns):
            # 至少要有名稱，且有股數或權重其中之一
            if ("股數" in df2.columns) or ("持股權重" in df2.columns):
                return df2
    return pd.DataFrame(columns=["股票代號","股票名稱","股數","持股權重"])

# ---------- Playwright 下載（含 iframe 與彈窗處理） ----------
def _try_click_download_and_capture(page, ctx, timeout_ms=120000) -> bytes | None:
    # 加強等待，盡量讓表格渲染完成
    try:
        page.wait_for_load_state("networkidle", timeout=30000)
    except Exception:
        pass

    # 可能的 cookie/公告彈窗
    for txt in ["同意","我知道了","接受","關閉","確定"]:
        try:
            loc = page.locator(f'text="{txt}"')
            if loc.count() > 0:
                loc.first.click(timeout=1000)
                break
        except Exception:
            pass

    def frames():
        yield page
        for f in page.frames:
            yield f

    # 直接 href
    href = None
    for f in frames():
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

    # 沒有直接 href：點擊並等 response 或原生下載
    click_targets = []
    for f in frames():
        for sel in ['a:has-text("下載")','button:has-text("下載")','text=下載','a[role="button"]:has-text("下載")']:
            try:
                if f.locator(sel).count() > 0:
                    click_targets.append((f, sel))
            except Exception:
                continue

    seen = set()
    for f, sel in click_targets:
        key = (id(f), sel)
        if key in seen: continue
        seen.add(key)
        try:
            # 先掛 response 監聽
            resp = None
            try:
                resp = ctx.wait_for_event(
                    "response",
                    predicate=lambda r: ("Download" in r.url and ("fundCode=49YTW" in r.url or r.url.endswith((".xlsx",".csv")))) and r.status == 200,
                    timeout=timeout_ms
                )
            except PWTimeout:
                resp = None

            f.locator(sel).first.click(timeout=2000)

            if resp is None:
                try:
                    resp = ctx.wait_for_event(
                        "response",
                        predicate=lambda r: ("Download" in r.url and ("fundCode=49YTW" in r.url or r.url.endswith((".xlsx",".csv")))) and r.status == 200,
                        timeout=timeout_ms
                    )
                except PWTimeout:
                    # 原生下載後備
                    try:
                        with f.page.expect_download(timeout=20000) as dl_info:
                            try: f.locator(sel).first.click(timeout=1000)
                            except Exception: pass
                        download = dl_info.value
                        return download.content()
                    except PWTimeout:
                        continue

            if resp:
                try:
                    return resp.body()
                except Exception:
                    continue
        except Exception:
            continue

    return None

# ---------- Cookies 直打 API ----------
def _fallback_download_with_cookies(ctx) -> bytes | None:
    state = ctx.storage_state()
    if isinstance(state, str):
        state = json.loads(state)
    cookies = state.get("cookies", [])
    jar = []
    for c in cookies:
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

    html_snapshot = None  # 下載全失敗時，保留 DOM 以解析表格

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(accept_downloads=True, locale="zh-TW", user_agent=UA)
        page = ctx.new_page()
        page.goto(INFO_URL, wait_until="domcontentloaded", timeout=60000)

        # 儲存渲染後頁面（最後保險用）
        try:
            page.wait_for_load_state("networkidle", timeout=30000)
        except Exception:
            pass
        html_snapshot = page.content()

        # 下載路徑（多層保險）
        content = None
        try:
            content = _try_click_download_and_capture(page, ctx, timeout_ms=120000)
        except Exception:
            content = None

        if content is None:
            content = _fallback_download_with_cookies(ctx)

        # 關閉瀏覽器（DOM 解析用的是 html_snapshot，不再依賴 page 狀態）
        browser.close()

    # 嘗試把 bytes 直接變成 DF
    df = None
    if content is not None and len(content) > 200:
        df = _bytes_to_df(content)

    # 若仍失敗，改用 DOM 解析（這一步等同你肉眼看到的表格）
    if (df is None or df.empty) and html_snapshot:
        df = _html_to_df(html_snapshot)

    if df is None or df.empty:
        raise SystemExit("官方頁仍無法取得有效資料（下載/API/DOM 皆失敗，流程可能大改）。")

    rows = _save_xlsx(df, out_xlsx)
    print(f"[fetch] saved {out_xlsx} rows={rows}")

if __name__ == "__main__":
    fetch_snapshot()