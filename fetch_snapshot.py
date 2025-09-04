# fetch_snapshot.py — 00981A 官方頁抓檔
# 流程：下載連結 → 事件/回應 → cookies 直打 API → DOM 表備援
# 產出：archive/YYYY-MM/ETF_Investment_Portfolio_YYYYMMDD.xlsx
# 工作表：holdings（整理後）、with_prices（同資料多一欄「收盤價」留空）

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
CODE_RE = re.compile(r"([1-9]\d{3})")  # 僅接受 1000-9999，避免 00981A 被誤抓成 0098
EXCLUDE_TITLES = {"基金資產","項目","現金","期貨保證金","申贖應付款","應收付證券款"}

# ------------------ 日期處理 ------------------
def _date_str_default() -> str:
    raw = (os.getenv("REPORT_DATE") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw): return raw
    if re.fullmatch(r"\d{8}", raw): return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return datetime.now().strftime("%Y-%m-%d")

def _extract_info_date_from_html(html: str) -> str | None:
    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    m = re.search(r"資料日期[:：]\s*(\d{4})[/-](\d{2})[/-](\d{2})", text)
    if m:
        y,mn,d = m.groups()
        return f"{y}-{mn}-{d}"
    return None

def _out_path(date_str: str) -> Path:
    yyyymm = date_str[:7]
    yyyymmdd = date_str.replace("-", "")
    outdir = ARCHIVE / yyyymm
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir / f"ETF_Investment_Portfolio_{yyyymmdd}.xlsx"

# ------------------ 欄位標準化 ------------------
def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["".join([str(x) for x in tup if str(x) != "nan"]).strip() for tup in df.columns.values]
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = _flatten_columns(df.copy())

    # 欄位映射
    rename = {}
    for c in df.columns:
        s = str(c)
        if any(k in s for k in ["股票代號","證券代號","股票代碼","代號"]): rename[c] = "股票代號"
        elif any(k in s for k in ["股票名稱","個股名稱","名稱"]):          rename[c] = "股票名稱"
        elif any(k in s for k in ["持股權重","投資比例","比重","權重"]):     rename[c] = "持股權重"
        elif any(k in s for k in ["股數","持有股數"]):                      rename[c] = "股數"
    if rename:
        df.rename(columns=rename, inplace=True)

    # 至少要有名稱或代號其一
    if "股票代號" not in df.columns and "股票名稱" not in df.columns:
        return pd.DataFrame(columns=["股票代號","股票名稱","股數","持股權重"])

    # 補代號（僅接受 [1-9]\d{3}）
    if "股票代號" not in df.columns:
        if "股票名稱" in df.columns:
            df["股票代號"] = df["股票名稱"].astype(str).str.extract(CODE_RE, expand=False)
        if "股票代號" not in df.columns or df["股票代號"].isna().all():
            any_text = df.astype(str).agg(" ".join, axis=1)
            df["股票代號"] = any_text.str.extract(CODE_RE, expand=False)

    # 清洗
    if "股票名稱" in df.columns:
        df["股票名稱"] = df["股票名稱"].astype(str).str.replace(r"\(\d{4}\)","",regex=True).str.strip()
    else:
        df["股票名稱"] = ""

    has_shares = "股數" in df.columns
    has_weight = "持股權重" in df.columns

    if has_shares:
        df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)
    else:
        df["股數"] = 0

    if has_weight:
        df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)
    else:
        df["持股權重"] = 0.0

    # 只有股數、沒有權重 → 以股數占比回推權重（避免整排 0）
    if has_shares and not has_weight and df["股數"].sum() > 0:
        total = df["股數"].sum()
        df["持股權重"] = (df["股數"] / total * 100).round(6)

    # 最終代號清洗與輸出
    df["股票代號"] = df["股票代號"].astype(str).str.extract(CODE_RE, expand=False)
    df = df.dropna(subset=["股票代號"]).drop_duplicates("股票代號").sort_values("股票代號").reset_index(drop=True)
    return df[["股票代號","股票名稱","股數","持股權重"]]

def _looks_like_holdings(df: pd.DataFrame) -> bool:
    cols = [str(c) for c in df.columns]
    if any(any(k in str(c) for k in EXCLUDE_TITLES) for c in cols):
        return False
    if any(any(k in c for k in ["股票代號","證券代號","股票代碼"]) for c in cols):
        return True
    sample = df.astype(str).agg(" ".join, axis=1).str.extractall(CODE_RE)
    return sample.size >= 5

# ------------------ 各型資料轉 DF ------------------
def _bytes_to_df(content: bytes) -> pd.DataFrame | None:
    if content[:4] == b"PK\x03\x04":  # xlsx
        try:
            return _normalize(pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype=str))
        except Exception:
            pass
    for enc in ("utf-8-sig","utf-8","cp950","big5-hkscs"):
        try:
            return _normalize(pd.read_csv(io.BytesIO(content), encoding=enc, dtype=str))
        except Exception:
            continue
    return None

def _html_to_df(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    candidates = []
    for t in soup.find_all("table"):
        try:
            for df in pd.read_html(io.StringIO(str(t))):
                candidates.append(df)
        except Exception:
            continue
    try:
        for df in pd.read_html(io.StringIO(html)):
            candidates.append(df)
    except Exception:
        pass

    best, best_score = None, -1
    for raw in candidates:
        if not _looks_like_holdings(raw):
            continue
        df = _normalize(raw)
        if df.empty: 
            continue
        if (df["持股權重"].sum() == 0) and (df["股數"].sum() == 0):
            continue
        score = len(df)
        if score > best_score:
            best_score, best = score, df
    return best if best is not None else pd.DataFrame(columns=["股票代號","股票名稱","股數","持股權重"])

# ------------------ 存檔 ------------------
def _save_xlsx(df: pd.DataFrame, out_xlsx: Path) -> int:
    if df.empty:
        raise SystemExit("仍未取得任何有效持股列。")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="holdings", index=False)
        df2 = df.copy(); df2["收盤價"] = pd.NA
        df2.to_excel(w, sheet_name="with_prices", index=False)
    return len(df)

# ------------------ 抓檔（多層保險） ------------------
def _try_click_download_and_capture(page, ctx, timeout_ms=120000) -> bytes | None:
    try: page.wait_for_load_state("networkidle", timeout=30000)
    except Exception: pass

    # 直接 href
    href = None
    try:
        loc = page.locator('a[href*="Download"]').first
        if loc.count() > 0:
            h = loc.get_attribute("href")
            if h and h != "javascript:void(0)":
                href = h
    except Exception:
        pass
    if href and href.startswith("/"): href = "https://www.ezmoney.com.tw" + href
    if href:
        r = ctx.request.get(href, headers={"Referer": INFO_URL})
        if r.ok and len(r.body()) > 200:
            return r.body()

    # 點擊 + 等 context response / 原生下載
    for sel in ['a:has-text("下載")','button:has-text("下載")','text=下載','a[role="button"]:has-text("下載")']:
        try:
            if page.locator(sel).count() == 0: continue
            page.locator(sel).first.click(timeout=2000)
            try:
                resp = ctx.wait_for_event(
                    "response",
                    predicate=lambda r: ("Download" in r.url and ("fundCode=49YTW" in r.url or r.url.endswith((".xlsx",".csv")))) and r.status == 200,
                    timeout=timeout_ms
                )
                if resp and len(resp.body()) > 200:
                    return resp.body()
            except PWTimeout:
                try:
                    with page.expect_download(timeout=20000) as dl_info:
                        try: page.locator(sel).first.click(timeout=1000)
                        except Exception: pass
                    return dl_info.value.content()
                except PWTimeout:
                    continue
        except Exception:
            continue
    return None

def _fallback_download_with_cookies(ctx) -> bytes | None:
    state = ctx.storage_state()
    if isinstance(state, str):
        state = json.loads(state)
    cookies = state.get("cookies", [])
    jar = "; ".join(f"{c['name']}={c['value']}" for c in cookies if "ezmoney.com.tw" in (c.get("domain") or ""))
    headers = {"User-Agent": UA, "Referer": INFO_URL}
    if jar: headers["Cookie"] = jar
    try:
        r = requests.get(DOWNLOAD_API, headers=headers, timeout=60)
        if r.ok and len(r.content) > 200:
            return r.content
    except Exception:
        pass
    return None

# ------------------ 主程式 ------------------
def fetch_snapshot():
    # 預設用 workflow 給的 REPORT_DATE；稍後若頁面有「資料日期」再覆蓋
    effective_date = _date_str_default()
    out_xlsx = _out_path(effective_date)

    html_snapshot = None
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(accept_downloads=True, locale="zh-TW", user_agent=UA)
        page = ctx.new_page()
        page.goto(INFO_URL, wait_until="domcontentloaded", timeout=60000)
        try: page.wait_for_load_state("networkidle", timeout=30000)
        except Exception: pass
        html_snapshot = page.content()

        content = _try_click_download_and_capture(page, ctx, timeout_ms=120000)
        if content is None:
            content = _fallback_download_with_cookies(ctx)
        browser.close()

    # 若頁面寫有「資料日期」，以它為準重設輸出檔名
    info_date = _extract_info_date_from_html(html_snapshot or "")
    if info_date:
        effective_date = info_date
        out_xlsx = _out_path(effective_date)

    df = None
    if content is not None and len(content) > 200:
        df = _bytes_to_df(content)

    if (df is None or df.empty) and html_snapshot:
        df = _html_to_df(html_snapshot)

    if df is None or df.empty:
        raise SystemExit("官方頁仍無法取得有效資料（下載/API/DOM 皆失敗）。")

    rows = _save_xlsx(df, out_xlsx)
    print(f"[fetch] saved {out_xlsx} rows={rows} (report_date={effective_date})")

if __name__ == "__main__":
    fetch_snapshot()