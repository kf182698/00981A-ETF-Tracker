# fetch_snapshot.py — 修正：嚴格挑選成分股表 + 僅接受 [1-9]\d{3} 代號 + 以頁面「資料日期」命名
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

# -------- 日期：以頁面資料日期優先，否則用 REPORT_DATE（workflow 已設為前一工作日）
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

# -------- 欄位標準化（只在欄存在時清洗；股票代號只接受 [1-9]\d{3}）
CODE_RE = re.compile(r"([1-9]\d{3})")

def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["".join([str(x) for x in tup if str(x) != "nan"]).strip() for tup in df.columns.values]
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = _flatten_columns(df.copy())

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

    if "股票名稱" in df.columns:
        df["股票名稱"] = df["股票名稱"].astype(str).str.replace(r"\(\d{4}\)","",regex=True).str.strip()
    else:
        df["股票名稱"] = ""

    if "股數" in df.columns:
        df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)
    else:
        df["股數"] = 0

    if "持股權重" in df.columns:
        df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)
    else:
        df["持股權重"] = 0.0

    df["股票代號"] = df["股票代號"].astype(str).str.extract(CODE_RE, expand=False)
    df = df.dropna(subset=["股票代號"]).drop_duplicates("股票代號").sort_values("股票代號").reset_index(drop=True)
    return df[["股票代號","股票名稱","股數","持股權重"]]

# -------- DOM 表挑選：必須「像成分股表」
EXCLUDE_TITLES = {"基金資產","項目","現金","期貨保證金","申贖應付款","應收付證券款"}

def _looks_like_holdings(df: pd.DataFrame) -> bool:
    cols = [str(c) for c in df.columns]
    # 頭欄出現 EXCLUDE 關鍵字的，直接排除（總覽表）
    if any(any(k in str(c) for k in EXCLUDE_TITLES) for c in cols):
        return False
    # 欄名有「股票代號/證券代號」直接通過
    if any(any(k in c for k in ["股票代號","證券代號","股票代碼"]) for c in cols):
        return True
    # 否則檢查內容：若一張表能找到 >= 5 個 [1-9]\d{3} 的代號，視為成分股表
    sample = df.astype(str).agg(" ".join, axis=1).str.extractall(CODE_RE)
    return sample.size >= 5

def _html_to_df(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    candidates = []
    # 個別 table
    for t in soup.find_all("table"):
        try:
            for df in pd.read_html(io.StringIO(str(t))):
                candidates.append(df)
        except Exception:
            continue
    # 全頁備援
    try:
        for df in pd.read_html(io.StringIO(html)):
            candidates.append(df)
    except Exception:
        pass

    # 嚴格挑選最像成分股表的一張
    best = None
    best_score = -1
    for raw in candidates:
        if not _looks_like_holdings(raw):
            continue
        df = _normalize(raw)
        if df.empty: 
            continue
        # 用股票列數當分數
        score = len(df)
        if score > best_score:
            best_score, best = score, df
    return best if best is not None else pd.DataFrame(columns=["股票代號","股票名稱","股數","持股權重"])

# -------- XLSX/CSV bytes 轉 DataFrame
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

def _save_xlsx(df: pd.DataFrame, out_xlsx: Path) -> int:
    if df.empty:
        raise SystemExit("仍未取得任何有效持股列。")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="holdings", index=False)
        df2 = df.copy(); df2["收盤價"] = pd.NA
        df2.to_excel(w, sheet_name="with_prices", index=False)
    return len(df)

# -------- 下載流程（同前一版，略）
def _try_click_download_and_capture(page, ctx, timeout_ms=120000) -> bytes | None:
    try: page.wait_for_load_state("networkidle", timeout=30000)
    except Exception: pass

    # 先找直接 href
    href = None
    for sel in ['a[href*="Download"]']:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0:
                h = loc.get_attribute("href")
                if h and h != "javascript:void(0)": href = h; break
        except Exception:
            pass
    if href and href.startswith("/"):
        href = "https://www.ezmoney.com.tw" + href
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

# -------- 主程式
def fetch_snapshot():
    # 先以 workflow 設的 REPORT_DATE 當預設，稍後若頁面有「資料日期」再覆蓋
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