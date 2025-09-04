# fetch_snapshot.py — 官方 Info 頁取 cookie → 擷取真下載連結 → 智能判斷 XLSX/CSV/HTML 解析
import os, re, io, csv
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup

INFO_URL = "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW"
ARCHIVE = Path("archive")

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"

def _today_str() -> str:
    raw = (os.getenv("REPORT_DATE") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw): return raw
    if re.fullmatch(r"\d{8}", raw): return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return datetime.now().strftime("%Y-%m-%d")

def _ensure_dirs(date_str: str) -> Path:
    yyyymm = date_str[:7]
    outdir = ARCHIVE / yyyymm
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir

def _sniff_encoding(b: bytes) -> str:
    # 優先嘗試 UTF-8 BOM；再退 CP950；最後用 charset-normalizer
    if b.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    try:
        b.decode("utf-8")
        return "utf-8"
    except Exception:
        pass
    for enc in ("cp950", "big5-hkscs"):
        try:
            b.decode(enc)
            return enc
        except Exception:
            continue
    try:
        from charset_normalizer import from_bytes
        res = from_bytes(b).best()
        if res:
            return str(res.encoding or "utf-8")
    except Exception:
        pass
    return "utf-8"

def _sniff_delimiter(sample_text: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample_text[:2048], delimiters=",\t;|")
        return dialect.delimiter
    except Exception:
        # 預設逗號
        return ","

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # 欄位正規化
    rename = {}
    for c in df.columns:
        s = str(c)
        if any(k in s for k in ["股票代號", "證券代號", "股票代碼", "代號"]): rename[c] = "股票代號"
        elif any(k in s for k in ["股票名稱", "個股名稱", "名稱"]):          rename[c] = "股票名稱"
        elif any(k in s for k in ["股數", "持有股數"]):                      rename[c] = "股數"
        elif any(k in s for k in ["持股權重", "投資比例", "比重", "權重"]):    rename[c] = "持股權重"
    if rename:
        df.rename(columns=rename, inplace=True)
    cols = [c for c in ["股票代號", "股票名稱", "股數", "持股權重"] if c in df.columns]
    df = df[cols].copy()
    # 數值清洗
    if "股數" in df.columns:
        df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)
    if "持股權重" in df.columns:
        df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)
    # 若沒有代號但名稱內有 (2330) 就擷取
    if "股票代號" not in df.columns and "股票名稱" in df.columns:
        df["股票代號"] = df["股票名稱"].astype(str).str.extract(r"(\d{4})", expand=False)
    # 最後整理
    if "股票名稱" in df.columns:
        df["股票名稱"] = df["股票名稱"].astype(str).str.replace(r"\(\d{4}\)", "", regex=True).str.strip()
    df["股票代號"] = df["股票代號"].astype(str).str.extract(r"(\d{4})", expand=False)
    df = df.dropna(subset=["股票代號"]).drop_duplicates("股票代號").sort_values("股票代號").reset_index(drop=True)
    return df

def _parse_html_tables(html: str) -> pd.DataFrame:
    # 從 HTML 解析所有 table，挑可能的持股表
    soup = BeautifulSoup(html, "lxml")
    candidates = []
    for t in soup.find_all("table"):
        try:
            for df in pd.read_html(io.StringIO(str(t))):
                # 攤平多層欄
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = ["".join([str(x) for x in tup if str(x) != "nan"]).strip() for tup in df.columns.values]
                df.columns = [str(c).strip() for c in df.columns]
                candidates.append(df)
        except Exception:
            continue
    # 全頁再試一次
    try:
        for df in pd.read_html(io.StringIO(html)):
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ["".join([str(x) for x in tup if str(x) != "nan"]).strip() for tup in df.columns.values]
            df.columns = [str(c).strip() for c in df.columns]
            candidates.append(df)
    except Exception:
        pass

    for df in candidates:
        cols = [str(c) for c in df.columns]
        has_code = any(any(k in c for k in ["股票代號","證券代號","股票代碼","代號"]) for c in cols)
        has_name = any(any(k in c for k in ["股票名稱","個股名稱","名稱"]) for c in cols)
        has_sh   = any(("股數" in c) or ("持有股數" in c) for c in cols)
        has_wt   = any(any(k in c for k in ["持股權重","投資比例","比重","權重"]) for c in cols)
        if (has_code or has_name) and (has_sh or has_wt) and df.shape[1] >= 3:
            return _normalize_columns(df)
    # 沒找到就給空表
    return pd.DataFrame(columns=["股票代號","股票名稱","股數","持股權重"])

def fetch_snapshot():
    date_str = _today_str()
    outdir = _ensure_dirs(date_str)
    yyyymmdd = date_str.replace("-", "")
    out_xlsx = outdir / f"ETF_Investment_Portfolio_{yyyymmdd}.xlsx"

    with requests.Session() as s:
        s.headers.update({"User-Agent": UA})
        # 1) 先打 Info 頁取得 cookie & 解析下載連結
        r = s.get(INFO_URL, timeout=30)
        r.raise_for_status()
        html = r.text
        soup = BeautifulSoup(html, "lxml")
        dl_url = None
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "Download" in href and "fundCode=49YTW" in href:
                dl_url = href
                break
        # 若為相對路徑 → 補成絕對
        if dl_url and dl_url.startswith("/"):
            dl_url = f"https://www.ezmoney.com.tw{dl_url}"

        if not dl_url:
            # 有些站點按鈕不是 <a>，而是 JS 呼叫固定 API；退而求其次：猜測常見 API
            dl_url = "https://www.ezmoney.com.tw/ETF/Fund/DownloadHoldingFile?fundCode=49YTW"

        # 2) 下載檔案（帶 Referer）
        r2 = s.get(dl_url, timeout=60, headers={"Referer": INFO_URL})
        r2.raise_for_status()
        content = r2.content
        ctype = (r2.headers.get("Content-Type") or "").lower()

    # 3) 判斷型別並讀成 DataFrame
    df = None
    # a) XLSX：ZIP 檔頭 PK\x03\x04
    if content[:4] == b"PK\x03\x04" or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in ctype:
        df = pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype={"股票代號": str})
    else:
        # b) CSV / TSV：用編碼 + 分隔符偵測
        enc = _sniff_encoding(content)
        sample = content[:4096].decode(enc, errors="ignore")
        delim = _sniff_delimiter(sample)
        try:
            df = pd.read_csv(io.BytesIO(content), encoding=enc, sep=delim, dtype={"股票代號": str})
        except Exception:
            # c) 若其實是 HTML（例如錯誤頁或需驗證）→ 直接解析表格
            df = _parse_html_tables(sample)

    if df is None or df.empty:
        raise SystemExit("官方下載回應非有效資料表（可能為 HTML 錯誤頁或格式變動）。")

    # 4) 欄位標準化與輸出
    df = _normalize_columns(df)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="holdings", index=False)
        df2 = df.copy(); df2["收盤價"] = pd.NA
        df2.to_excel(w, sheet_name="with_prices", index=False)

    print(f"[fetch] saved {out_xlsx} rows={len(df)}")
    return out_xlsx

if __name__ == "__main__":
    fetch_snapshot()