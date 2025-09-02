# fetch_snapshot.py — Big5/CP950 encoding robust + table heuristics
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

# ---------- utils ----------
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
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["".join([str(x) for x in tup if str(x) != "nan"]).strip() for tup in df.columns.values]
    df.columns = [str(c).strip() for c in df.columns]
    return df

def guess_columns(df: pd.DataFrame):
    cols = list(df.columns)
    def pick(names):
        for c in cols:
            s = str(c)
            if any(k in s for k in names):
                return c
        return None
    # 正常（中文）表頭
    col_name  = pick(["股票名稱", "個股名稱", "名稱", "成分股"])
    col_shares= pick(["持有股數", "股數"])
    col_wt    = pick(["投資比例", "投資比例(%)", "比重", "比例", "權重"])
    col_code  = pick(["證券代號", "股票代號", "代號", "股票代碼"])
    return col_name, col_shares, col_wt, col_code

def extract_code_from_text(name: str) -> str | None:
    m = re.search(r"(\d{4})", name or "")
    return m.group(1) if m else None

def try_decodes(content: bytes) -> str:
    # 依序嘗試幾種常見編碼
    for enc in ("utf-8", "cp950", "big5-hkscs"):
        try:
            return content.decode(enc)
        except Exception:
            continue
    # 最後用 requests 自帶的偵測
    try:
        import charset_normalizer as ch
        r = ch.from_bytes(content).best()
        if r:
            return str(r)
    except Exception:
        pass
    # 退而求其次
    return content.decode("utf-8", errors="ignore")

# ---------- parser ----------
def fetch_from_moneydj() -> pd.DataFrame:
    resp = requests.get(URL_HOLDING, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    html = try_decodes(resp.content)
    soup = BeautifulSoup(html, "lxml")

    # A) 名稱->代號
    name2code = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        nm = a.get_text(strip=True)
        if not nm: continue
        m = re.search(r"etfid=(\d{4})\.?TW", href, flags=re.I)
        if m:
            name2code[nm] = m.group(1); continue
        m2 = re.search(r"stkno=(\d{4})", href, flags=re.I)
        if m2 and nm not in name2code:
            name2code[nm] = m2.group(1)

    # B) 逐 table 讀（用 StringIO 避免 FutureWarning）
    candidate_frames = []
    for tbl in soup.find_all("table"):
        try:
            dflist = pd.read_html(io.StringIO(str(tbl)))
            for df in dflist:
                candidate_frames.append(flatten_columns(df))
        except Exception:
            continue
    # 全頁再嘗試一次
    try:
        for df in pd.read_html(io.StringIO(html)):
            candidate_frames.append(flatten_columns(df))
    except Exception:
        pass

    debug_cols = []
    picked = None

    # 先用表頭關鍵字挑
    for df in candidate_frames:
        col_name, col_shares, col_wt, col_code = guess_columns(df)
        debug_cols.append(list(df.columns))
        if col_name and (col_shares or col_wt or col_code):
            picked = (df, col_name, col_shares, col_wt, col_code)
            break

    # 若沒挑到，改用「位置/內容」啟發式（處理亂碼表頭）
    if not picked:
        for df in candidate_frames:
            if df.shape[1] < 3:  # 至少 3 欄：名稱/比重/股數
                continue
            # 嘗試把第一列當表頭（很多網站會把表頭落在第一列數據）
            df2 = df.copy()
            first_row = df2.iloc[0].astype(str).tolist()
            # 觀察像「個股名稱 / 投資比例(%) / 持有股數」的關鍵詞（即使亂碼也會帶 % 或 數字）
            hits = sum(1 for x in first_row if ("%" in x) or ("股" in x) or ("名" in x))
            if hits >= 2:
                df2.columns = [str(x).strip() for x in first_row]
                df2 = df2.iloc[1:].reset_index(drop=True)
                df2 = flatten_columns(df2)
                col_name, col_shares, col_wt, col_code = guess_columns(df2)
                if col_name or col_code:
                    picked = (df2, col_name or col_code, col_shares, col_wt, col_code)
                    break
            # 再退一步：直接取前三欄，假設 [名稱, 比重, 股數] 或 [名稱, 股數, 比重]
            tmp = df.copy()
            tmp.columns = [f"C{i}" for i in range(tmp.shape[1])]
            t3 = tmp.iloc[:, :3].copy()
            # 判斷哪欄像百分比
            pct_idx = None
            for i in range(3):
                sample = "".join(t3.iloc[:10, i].astype(str).tolist())
                if "%" in sample:
                    pct_idx = i; break
            # 判斷哪欄像整數
            int_idx = None
            for i in range(3):
                if i == pct_idx: continue
                s = t3.iloc[:10, i].astype(str).tolist()
                hits_int = sum(bool(re.search(r"\d", x)) for x in s)
                if hits_int >= 5:
                    int_idx = i; break
            if pct_idx is not None:
                name_idx = ({0,1,2} - {pct_idx} - ({int_idx} if int_idx is not None else set())).pop()
                df3 = pd.DataFrame({
                    "股票名稱": t3.iloc[:, name_idx].astype(str).str.strip(),
                    "持股權重": t3.iloc[:, pct_idx].astype(str),
                    "股數": t3.iloc[:, int_idx].astype(str) if int_idx is not None else "0",
                })
                picked = (df3, "股票名稱", "股數", "持股權重", None)
                break

    if not picked:
        print("[fetch] 無法辨識表格；以下為候選表頭樣式（最多3張）：")
        for i, cols in enumerate(debug_cols[:3], 1):
            print(f"  [table {i}] {cols}")
        return pd.DataFrame(columns=["股票代號","股票名稱","股數","持股權重"])

    df_raw, col_name, col_shares, col_wt, col_code = picked
    df = df_raw.copy()

    # 標準欄名
    rename = {}
    if col_name not in df.columns:
        # 如果 col_name 是我們自己填的 "股票名稱"
        pass
    else:
        rename[col_name] = "股票名稱"
    if col_shares and col_shares in df.columns: rename[col_shares] = "股數"
    if col_wt and col_wt in df.columns:         rename[col_wt]     = "持股權重"
    if col_code and col_code in df.columns:     rename[col_code]   = "股票代號"
    if rename:
        df.rename(columns=rename, inplace=True)

    # 清理/型別
    if "股票名稱" in df.columns:
        df["股票名稱"] = df["股票名稱"].astype(str).str.strip()
    if "股數" in df.columns:
        df["股數"] = df["股數"].map(clean_int).astype(int)
    else:
        df["股數"] = 0
    if "持股權重" in df.columns:
        df["持股權重"] = df["持股權重"].map(clean_float).astype(float)
    else:
        df["持股權重"] = 0.0

    # 代號補齊
    if "股票代號" not in df.columns:
        df["股票代號"] = pd.NA
    if "股票名稱" in df.columns:
        # 1) 連結映射
        df.loc[df["股票代號"].isna(), "股票代號"] = df.loc[df["股票代號"].isna(), "股票名稱"].map(name2code)
        # 2) 名稱內括號或純數字
        df.loc[df["股票代號"].isna(), "股票代號"] = df.loc[df["股票代號"].isna(), "股票名稱"].map(extract_code_from_text)

    df["股票代號"] = df["股票代號"].astype(str).str.extract(r"(\d{4})", expand=False)
    df = df.dropna(subset=["股票代號"]).copy()

    # 最終欄位
    if "股票名稱" not in df.columns: df["股票名稱"] = ""
    df = df[["股票代號","股票名稱","股數","持股權重"]].drop_duplicates(subset=["股票代號"]).sort_values("股票代號").reset_index(drop=True)
    return df

# ---------- main ----------
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
