# export_prices_from_xlsx.py — 把 archive/<YYYY-MM>/*.xlsx 的 with_prices 轉成 prices/YYYY-MM-DD.csv
import os, re, glob
from pathlib import Path
import pandas as pd

ARCHIVE = Path("archive")
PRICES  = Path("prices"); PRICES.mkdir(parents=True, exist_ok=True)

def norm_date(s: str) -> str:
    s = s.strip()
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s) or re.fullmatch(r"(\d{4})(\d{2})(\d{2})", s)
    if not m: raise SystemExit(f"REPORT_DATE 不合法: {s}")
    if len(m.groups()) == 3:
        y, mm, dd = m.groups()
    else:
        y, mm, dd = m.group(1), m.group(2), m.group(3)
    return f"{y}-{mm}-{dd}"

date_str = norm_date(os.getenv("REPORT_DATE","").strip())
yyyymm = date_str[:7]
yyyymmdd = date_str.replace("-","")

month_dir = ARCHIVE / yyyymm
cands = sorted(glob.glob(str(month_dir / f"*{yyyymmdd}*.xlsx")))
if not cands:
    raise SystemExit(f"找不到當日 Xlsx：{month_dir}/*{yyyymmdd}*.xlsx")

fp = cands[-1]
df = pd.read_excel(fp, sheet_name="with_prices", dtype={"股票代號": str})
rename = {
    "證券代號":"股票代號","代號":"股票代號","StockCode":"股票代號",
    "收盤":"收盤價","Close":"收盤價","close":"收盤價","收盤價(元)":"收盤價"
}
df.rename(columns={k:v for k,v in rename.items() if k in df.columns}, inplace=True)
out = df[["股票代號","收盤價"]].copy()
out["股票代號"] = out["股票代號"].astype(str).str.strip()
out["收盤價"] = pd.to_numeric(out["收盤價"], errors="coerce")
out.to_csv(PRICES / f"{date_str}.csv", index=False, encoding="utf-8-sig")
print(f"[export_prices] saved prices/{date_str}.csv from {Path(fp).name}")
