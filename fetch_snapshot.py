# fetch_snapshot.py — 強韌版：多樣式表格解析、代號多來源對應、數字/百分比清洗
import os, re, io, requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
}
URL_HOLDING = "https://www.moneydj.com/ETF/X/Basic/Basic0007.xdjhtm?etfid=00981A.TW"

ARCHIVE = Path("archive")

# ---------- 小工具 ----------
def norm_date(raw: str) -> str:
    raw = (raw or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw): return raw
    if re.fullmatch(r"\d{8}", raw): return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return datetime.utcnow().strftime("%Y-%m-%d")

def clean_int(x):
    if pd.isna(x): return 0
    s = str(x).replace(",", "").strip()
    m = re.search(r"-?\d+(\.\d+)?", s)
    if not m: return 0
    return int(float(m.group(0)))

def clean_float(x):
    if pd.isna(x): return 0.0
    s = str(x).replace(",", "").replace("%","").strip()
    try: return float(s)
    except: return 0.0

def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """把 MultiIndex 欄名攤平成單層字串"""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["".join([str(x) for x in tup if str(x) != "nan"]).strip() for tup in df.columns.values]
    df.columns = [str(c).strip() for c in df.columns]
    return df

def guess_columns(df: pd.DataFrame):
    """找出名稱/股數/比重/代號欄位名稱（各取第一個匹配）"""
    cols = list(df.columns)
    def pick(names):
        for c in cols:
            s = str(c)
            if any(k in s for k in names):
                return c
        return None

    col_name  = pick(["股票名稱", "個股名稱", "名稱", "成分股"])
    col_shares= pick(["持有股數", "股數"])
    col_wt    = pick(["投資比例", "投資比例(%)", "比重", "比例", "權重"])
    col_code  = pick(["證券代號", "股票代號", "代號", "股票代碼"])

    return col_name, col_shares, col_wt, col_code

def extract_code_from_text(name: str) -> str | None:
    m = re.search(r"(\d{4})", name or "")
    return m.group(1) if m else None

# ---------- 主要解析 ----------
def fetch_from_moneydj() -> pd.DataFrame:
    resp = requests.get(URL_HOLDING, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    html = resp.text
    soup = BeautifulSoup(html, "lxml")

    # A) 先從超連結建立 名稱->代號 對照
    name2code = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # 1) 典型：...etfid=2330.TW
        m = re.search(r"etfid=(\d{4})\.?TW", href, flags=re.I)
        if m:
            nm = a.get_text(strip=True)
            if nm:
                name2code[nm] = m.group(1)
        # 2) 少數：...stkno=2330
        m2 = re.search(r"stkno=(\d{4})", href, flags=re.I)
        if m2:
            nm = a.get_text(strip=True)
            if nm and nm not in name2code:
                name2code[nm] = m2.group(1)

    # B) 找出所有 table，逐一用 read_html 讀，並嘗試辨識欄位
    candidate_frames = []
    for tbl in soup.find_all("table"):
        try:
            dflist = pd.read_html(io.StringIO(str(tbl)))
            for df in dflist:
                df = flatten_columns(df)
                candidate_frames.append(df)
        except Exception:
            continue

    # 若整頁直接丟 read_html（搭配 StringIO）也嘗試一次（兼容某些沒有 table tag 的寫法）
    try:
        for df in pd.read_html(io.StringIO(html)):
            candidate_frames.append(flatten_columns(df))
    except Exception:
        pass

    picked = None
    debug_cols = []
    for df in candidate_frames:
        col_name, col_shares, col_wt, col_code = guess_columns(df)
        debug_cols.append(list(df.columns))
        if not col_name and not col_code:
            continue  # 連名稱/代號都沒有，不可能是持股表
        # 至少要有 名稱 + (股數或比重 其一)
        if not col_name and col_code:
            col_name = col_code  # 有些表直接用代號欄當名稱列
        if col_name and (col_shares or col_wt or col_code):
            picked = (df, col_name, col_shares, col_wt, col_code)
            break

    if not picked:
        # 輸出診斷資訊（不終止，回傳空表交給上游處理）
        print("[fetch] 無法辨識表格；以下為候選表頭樣式（最多3張）：")
        for i, cols in enumerate(debug_cols[:3], 1):
            print(f"  [table {i}] {cols}")
        return pd.DataFrame(columns=["股票代號","股票名稱","股數","持股權重"])

    df_raw, col_name, col_shares, col_wt, col_code = picked
    df = df_raw.copy()

    # 只取我們需要的欄，缺的就之後補
    keep = [c for c in [col_name, col_shares, col_wt, col_code] if c]
    df = df[keep].copy()

    # 標準欄名
    rename = {}
    if col_name:   rename[col_name]   = "股票名稱"
    if col_shares: rename[col_shares] = "股數"
    if col_wt:     rename[col_wt]     = "持股權重"
    if col_code:   rename[col_code]   = "股票代號"
    df.rename(columns=rename, inplace=True)

    # 清理
    if "股票名稱" in df.columns:
        df["股票名稱"] = df["股票名稱"].astype(str).str.strip()
    if "股數" in df.columns:
        df["股數"] = df["股數"].map(clean_int).astype(int)
    if "持股權重" in df.columns:
        df["持股權重"] = df["持股權重"].map(clean_float).astype(float)

    # 代號補齊：優先 1) 連結映射 2) 自帶代號欄 3) 名稱括號/數字
    if "股票代號" not in df.columns:
        df["股票代號"] = pd.NA
    if "股票名稱" in df.columns:
        df.loc[df["股票代號"].isna(), "股票代號"] = df.loc[df["股票代號"].isna(), "股票名稱"].map(name2code)
        df.loc[df["股票代號"].isna(), "股票代號"] = df.loc[df["股票代號"].isna(), "股票名稱"].map(extract_code_from_text)

    df["股票代號"] = df["股票代號"].astype(str).str.extract(r"(\d{4})", expand=False)
    df = df.dropna(subset=["股票代號"]).copy()

    # 整理輸出欄位
    if "股票名稱" not in df.columns: df["股票名稱"] = ""
    if "股數" not in df.columns:     df["股數"] = 0
    if "持股權重" not in df.columns: df["持股權重"] = 0.0

    df = df[["股票代號","股票名稱","股數","持股權重"]].drop_duplicates(subset=["股票代號"]).sort_values("股票代號").reset_index(drop=True)
    return df

# ---------- 主程式 ----------
def main():
    date_str = norm_date(os.getenv("REPORT_DATE", ""))
    yyyymm   = date_str[:7]
    yyyymmdd = date_str.replace("-", "")

    df = fetch_from_moneydj()
    if df.empty:
        raise SystemExit("抓取失敗：無法從來源辨識任何持股表格（可稍後重試，或改用備援來源）。")

    outdir = ARCHIVE / yyyymm
    outdir.mkdir(parents=True, exist_ok=True)
    xlsx = outdir / f"ETF_Investment_Portfolio_{yyyymmdd}.xlsx"

    with pd.ExcelWriter(xlsx, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="holdings", index=False)
        df_w = df.copy()
        df_w["收盤價"] = pd.NA
        df_w.to_excel(w, sheet_name="with_prices", index=False)

    print(f"[fetch] saved {xlsx} rows={len(df)}")

if __name__ == "__main__":
    main()
