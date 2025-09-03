# fetch_snapshot.py — Official-only, hardcoded URL (no env variables)
# 來源：統一投信 00981A 官方頁（ezmoney）
import re, io, requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

# 直接寫死官方頁網址（依你提供）
OFFICIAL_URL = "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW"

ARCHIVE_DIR = Path("archive")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
}

def today_str() -> str:
    # runner 已在 workflow 設 TZ=Asia/Taipei；這裡用 UTC 今天也可
    return datetime.utcnow().strftime("%Y-%m-%d")

def try_decode(content: bytes) -> str:
    # ezmoney 通常是 UTF-8；保留幾個常見備用編碼以防萬一
    for enc in ("utf-8", "cp950", "big5-hkscs"):
        try:
            return content.decode(enc)
        except Exception:
            pass
    try:
        import charset_normalizer as ch
        r = ch.from_bytes(content).best()
        if r:
            return str(r)
    except Exception:
        pass
    return content.decode("utf-8", errors="ignore")

def clean_int(x):
    if pd.isna(x): return 0
    s = str(x).replace(",", "").strip()
    m = re.search(r"-?\d+(\.\d+)?", s)
    return int(float(m.group(0))) if m else 0

def clean_float(x):
    if pd.isna(x): return 0.0
    s = str(x).replace(",", "").replace("%","").strip()
    try: return float(s)
    except: return 0.0

def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["".join([str(x) for x in tup if str(x) != "nan"]).strip() for tup in df.columns.values]
    df.columns = [str(c).strip() for c in df.columns]
    return df

def fetch_from_official(url: str) -> pd.DataFrame:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    html = try_decode(r.content)
    soup = BeautifulSoup(html, "lxml")

    # 逐個 table 讀入並嘗試辨識欄位
    cands = []
    for t in soup.find_all("table"):
        try:
            for df in pd.read_html(io.StringIO(str(t))):
                cands.append(flatten_columns(df))
        except Exception:
            continue
    # 全頁再讀一次（有些頁面不是標準 table）
    try:
        for df in pd.read_html(io.StringIO(html)):
            cands.append(flatten_columns(df))
    except Exception:
        pass

    picked = None
    for df in cands:
        cols = [str(c) for c in df.columns]
        has_code = any(any(k in c for k in ["股票代號","證券代號","代號","股票代碼"]) for c in cols)
        has_name = any(any(k in c for k in ["股票名稱","個股名稱","名稱"]) for c in cols)
        has_sh   = any(("股數" in c) or ("持有股數" in c) for c in cols)
        has_wt   = any(any(k in c for k in ["持股權重","投資比例","比重","權重"]) for c in cols)
        if (has_code or has_name) and (has_sh or has_wt) and df.shape[1] >= 3:
            picked = df; break
    if picked is None:
        raise SystemExit("官方頁無法辨識持股表（請稍後重試或確認頁面結構）。")

    df = picked.copy()

    # 欄位標準化
    rename = {}
    for c in list(df.columns):
        s = str(c)
        if any(k in s for k in ["股票代號","證券代號","代號","股票代碼"]): rename[c] = "股票代號"
        elif any(k in s for k in ["股票名稱","個股名稱","名稱"]):         rename[c] = "股票名稱"
        elif any(k in s for k in ["持股權重","投資比例","比重","權重"]):     rename[c] = "持股權重"
        elif any(k in s for k in ["股數","持有股數"]):                     rename[c] = "股數"
    if rename:
        df.rename(columns=rename, inplace=True)

    need = [c for c in ["股票代號","股票名稱","股數","持股權重"] if c in df.columns]
    if len(need) < 3:
        raise SystemExit("官方表欄位不足（需至少含 名稱 + 股數/權重 其中兩項）。")

    df = df[need].copy()

    # 若沒有代號欄，從名稱擷取 4 碼
    if "股票代號" not in df.columns:
        df["股票代號"] = df["股票名稱"].astype(str).str.extract(r"(\d{4})", expand=False)

    # 清洗
    df["股票代號"] = df["股票代號"].astype(str).str.extract(r"(\d{4})", expand=False)
    df = df.dropna(subset=["股票代號"])
    if "股票名稱" in df.columns:
        df["股票名稱"] = df["股票名稱"].astype(str).str.replace(r"\(\d{4}\)","",regex=True).str.strip()
    if "股數" in df.columns:
        df["股數"] = df["股數"].map(clean_int).astype(int)
    else:
        df["股數"] = 0
    if "持股權重" in df.columns:
        df["持股權重"] = df["持股權重"].map(clean_float).astype(float)
    else:
        df["持股權重"] = 0.0

    out = (
        df[["股票代號","股票名稱","股數","持股權重"]]
        .drop_duplicates("股票代號")
        .sort_values("股票代號")
        .reset_index(drop=True)
    )
    return out

def main():
    date_str = today_str()
    yyyymm   = date_str[:7]
    yyyymmdd = date_str.replace("-", "")

    df = fetch_from_official(OFFICIAL_URL)

    outdir = ARCHIVE_DIR / yyyymm
    outdir.mkdir(parents=True, exist_ok=True)
    xlsx = outdir / f"ETF_Investment_Portfolio_{yyyymmdd}.xlsx"

    with pd.ExcelWriter(xlsx, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="holdings", index=False)
        df_w = df.copy()
        df_w["收盤價"] = pd.NA
        df_w.to_excel(w, sheet_name="with_prices", index=False)

    print(f"[fetch] (official) saved {xlsx} rows={len(df)}")

if __name__ == "__main__":
    main()