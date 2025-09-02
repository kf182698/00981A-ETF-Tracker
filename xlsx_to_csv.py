# xlsx_to_csv.py — 將每日 Xlsx 轉成 data/YYYY-MM-DD.csv
import os, re, glob, pandas as pd
from pathlib import Path

ARCHIVE = Path("archive")
DATA    = Path("data"); DATA.mkdir(exist_ok=True)

def norm_date(raw: str) -> str:
    raw = (raw or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return raw
    if re.fullmatch(r"\d{8}", raw):
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    # 預設用今天（runner 時區由 workflow 設為 Asia/Taipei）
    return pd.Timestamp("today").strftime("%Y-%m-%d")

def load_holdings_from_xlsx(date_str: str) -> pd.DataFrame:
    yyyymm = date_str[:7]
    yyyymmdd = date_str.replace("-", "")
    month_dir = ARCHIVE / yyyymm
    cands = sorted(glob.glob(str(month_dir / f"*{yyyymmdd}*.xlsx")))
    if not cands:
        raise SystemExit(f"找不到當日 Xlsx：{month_dir}/*{yyyymmdd}*.xlsx")
    fp = cands[-1]

    # 優先讀 holdings，沒有就讀第一張
    try:
        df = pd.read_excel(fp, sheet_name="holdings", dtype={"股票代號": str})
    except Exception:
        xl = pd.ExcelFile(fp)
        df = pd.read_excel(xl, sheet_name=xl.sheet_names[0], dtype={"股票代號": str})

    # 欄位正規化
    rename = {
        "代號":"股票代號","證券代號":"股票代號","StockCode":"股票代號",
        "名稱":"股票名稱","個股名稱":"股票名稱",
        "投資比例(%)":"持股權重","投資比例":"持股權重","比重":"持股權重",
        "持有股數":"股數"
    }
    for k, v in rename.items():
        if k in df.columns and v not in df.columns:
            df.rename(columns={k: v}, inplace=True)

    need = ["股票代號","股票名稱","股數","持股權重"]
    df = df[[c for c in need if c in df.columns]].copy()

    # 型別清理
    df["股票代號"] = df["股票代號"].astype(str).str.strip()
    if "股票名稱" in df.columns:
        df["股票名稱"] = df["股票名稱"].astype(str).str.strip()
    if "股數" in df.columns:
        df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)
    if "持股權重" in df.columns:
        df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)

    # 最終輸出
    df = df.sort_values(["股票代號"]).reset_index(drop=True)
    out_csv = DATA / f"{date_str}.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[xlsx2csv] saved {out_csv} rows={len(df)} from {Path(fp).name}")
    return out_csv

def main():
    date_str = norm_date(os.getenv("REPORT_DATE", ""))
    load_holdings_from_xlsx(date_str)

if __name__ == "__main__":
    main()
