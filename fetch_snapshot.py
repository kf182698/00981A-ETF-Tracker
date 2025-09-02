# fetch_snapshot.py — 下載 00981A 每日持股，存成 Xlsx（holdings / with_prices）
import os, re, requests, pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
}
URL_HOLDING = "https://www.moneydj.com/ETF/X/Basic/Basic0007.xdjhtm?etfid=00981A.TW"

ARCHIVE = Path("archive")

def norm_date(raw: str) -> str:
    raw = (raw or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return raw
    if re.fullmatch(r"\d{8}", raw):
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    # default: today Asia/Taipei on runner (UTC treated as today)
    return datetime.utcnow().strftime("%Y-%m-%d")

def clean_num(x):
    if pd.isna(x): return 0
    return pd.to_numeric(str(x).replace(",", "").strip(), errors="coerce") or 0

def fetch_from_moneydj() -> pd.DataFrame:
    resp = requests.get(URL_HOLDING, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    html = resp.text

    # 1) 名稱→代號 對照（從超連結的 etfid=2330.TW 擷取）
    soup = BeautifulSoup(html, "lxml")
    name2code = {}
    for a in soup.find_all("a", href=True):
        m = re.search(r"etfid=(\d{4})\.?TW", a["href"], flags=re.I)
        if m:
            nm = a.get_text(strip=True)
            if nm:
                name2code[nm] = m.group(1)

    # 2) 用 pandas 直接把持股表抓下來
    tables = pd.read_html(html)   # 可能有多張表
    hold = None
    for t in tables:
        cols = "".join([str(c) for c in t.columns])
        if ("個股名稱" in cols or "個股" in cols) and ("持有股數" in cols or "投資比例" in cols):
            hold = t
            break
    if hold is None:
        raise RuntimeError("找不到持股表格（MoneyDJ 結構可能變動）")

    # 3) 欄位統一
    rename = {}
    for c in hold.columns:
        s = str(c).strip()
        if "個股" in s or "名稱" in s:   rename[c] = "股票名稱"
        if "投資比例" in s:             rename[c] = "持股權重"
        if "持有股數" in s or "股數" in s: rename[c] = "股數"
    hold.rename(columns=rename, inplace=True)

    need = [c for c in ["股票名稱", "股數", "持股權重"] if c in hold.columns]
    hold = hold[need].copy()

    hold["股票名稱"] = hold["股票名稱"].astype(str).str.strip()
    hold["股票代號"] = hold["股票名稱"].map(name2code)

    # 數值
    if "股數" in hold.columns:
        hold["股數"] = hold["股數"].map(clean_num).astype(int)
    if "持股權重" in hold.columns:
        hold["持股權重"] = pd.to_numeric(hold["持股權重"], errors="coerce").fillna(0.0)

    # 移除沒有代號的列
    hold = hold.dropna(subset=["股票代號"]).copy()
    hold["股票代號"] = hold["股票代號"].astype(str)

    # 排序與欄位順序
    out = hold[["股票代號", "股票名稱", "股數", "持股權重"]].sort_values(["股票代號"]).reset_index(drop=True)
    return out

def main():
    date_str = norm_date(os.getenv("REPORT_DATE", ""))
    yyyymm   = date_str[:7]
    yyyymmdd = date_str.replace("-", "")

    df = fetch_from_moneydj()

    outdir = ARCHIVE / yyyymm
    outdir.mkdir(parents=True, exist_ok=True)
    xlsx = outdir / f"ETF_Investment_Portfolio_{yyyymmdd}.xlsx"

    with pd.ExcelWriter(xlsx, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="holdings", index=False)
        df_w = df.copy()
        df_w["收盤價"] = pd.NA
        df_w.to_excel(w, sheet_name="with_prices", index=False)

    print(f"[fetch] saved {xlsx} with {len(df)} rows")

if __name__ == "__main__":
    main()
