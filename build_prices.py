# build_prices.py — 產出 prices/YYYY-MM-DD.csv（TWSE/TPEx 優先，缺的用 Yahoo 補）
import os, re, glob, time, json
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import requests

DATA_DIR   = Path("data")
PRICE_DIR  = Path("prices"); PRICE_DIR.mkdir(parents=True, exist_ok=True)

TWSE_API_1 = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={date}&type=ALLBUT0999&response=json"
TWSE_API_2 = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date}&type=ALLBUT0999"
TPEx_API   = "https://www.tpex.org.tw/www/stock/exchange_report/MI_INDEX?response=json&date={date}"

HEADERS = {
    "User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
}

def _norm_date(raw: str) -> str:
    s = (raw or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s): return s
    if re.fullmatch(r"\d{8}", s): return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    # 預設今天（由 workflow 設 TZ=Asia/Taipei）
    return datetime.now().strftime("%Y-%m-%d")

def _clean_price(x):
    if pd.isna(x): return None
    s = str(x).replace(",", "").replace("--", "").strip()
    try:
        return float(s)
    except:
        try:
            return float(s.replace('X',''))  # 少數來源會有特殊標記
        except:
            return None

def _extract_code_price_from_table(headers, rows):
    """從一張表提取 (code, close)；欄名容錯：代號/證券代號/股票代號；收盤/收盤價/收盤價(元)/Close"""
    code_idx = price_idx = None
    for i, h in enumerate(headers):
        hh = str(h).strip()
        if code_idx is None and any(k in hh for k in ["證券代號", "股票代號", "代號", "Code"]):
            code_idx = i
        if price_idx is None and any(k in hh for k in ["收盤價", "收盤", "Close"]):
            price_idx = i
    if code_idx is None or price_idx is None:
        return pd.DataFrame(columns=["股票代號","收盤價"])

    out = []
    for r in rows:
        try:
            code = str(r[code_idx]).strip()
            price = _clean_price(r[price_idx])
            if code and re.fullmatch(r"\d{4}", code) and price is not None:
                out.append((code, price))
        except:
            continue
    return pd.DataFrame(out, columns=["股票代號","收盤價"])

def _fetch_twse(date_yyyymmdd: str) -> pd.DataFrame:
    # 嘗試新版 rwd
    try:
        j = requests.get(TWSE_API_1.format(date=date_yyyymmdd), headers=HEADERS, timeout=30).json()
        if "tables" in j:
            for t in j["tables"]:
                headers = t.get("fields") or t.get("columns") or []
                rows    = t.get("data")   or t.get("rows")    or []
                df = _extract_code_price_from_table(headers, rows)
                if not df.empty:
                    return df
    except Exception as e:
        print("[twse] rwd fail:", e)

    # 回退舊版 exchangeReport
    try:
        j = requests.get(TWSE_API_2.format(date=date_yyyymmdd), headers=HEADERS, timeout=30).json()
        # 這個版本會有 data9/fields9 或 dataX/fieldsX
        for k in list(j.keys()):
            if k.startswith("fields"):
                idx = k.replace("fields","")
                headers = j[k]
                rows = j.get("data"+idx) or []
                df = _extract_code_price_from_table(headers, rows)
                if not df.empty:
                    return df
    except Exception as e:
        print("[twse] legacy fail:", e)
    return pd.DataFrame(columns=["股票代號","收盤價"])

def _fetch_tpex(date_yyyymmdd: str) -> pd.DataFrame:
    try:
        j = requests.get(TPEx_API.format(date=date_yyyymmdd), headers=HEADERS, timeout=30).json()
        # 可能的形狀：tables[] / aaData / data
        if "tables" in j:
            for t in j["tables"]:
                headers = t.get("fields") or t.get("columns") or []
                rows    = t.get("data")   or t.get("rows")    or []
                df = _extract_code_price_from_table(headers, rows)
                if not df.empty:
                    return df
        if "aaData" in j:
            # 有些站點欄位固定位置：0=代號, 8=收盤
            rows = j["aaData"]
            headers = ["代號","...","...","...","...","...","...","...","收盤"]
            df = _extract_code_price_from_table(headers, rows)
            if not df.empty:
                return df
        if "data" in j and "fields" in j:
            df = _extract_code_price_from_table(j["fields"], j["data"])
            if not df.empty:
                return df
    except Exception as e:
        print("[tpex] fail:", e)
    return pd.DataFrame(columns=["股票代號","收盤價"])

def _fetch_yahoo(codes, date_str):
    """逐檔嘗試 Yahoo（.TW -> .TWO），回傳 DataFrame(code, close)"""
    try:
        import yfinance as yf
    except Exception:
        print("[yahoo] yfinance not installed; skip")
        return pd.DataFrame(columns=["股票代號","收盤價"])

    start = pd.to_datetime(date_str)
    end   = (start + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    out = []
    for code in codes:
        close = None
        for suf in [".TW", ".TWO"]:
            try:
                df = yf.download(code + suf, start=date_str, end=end, interval="1d", progress=False, auto_adjust=False, threads=False)
                if isinstance(df, pd.DataFrame) and not df.empty and "Close" in df.columns:
                    v = float(df["Close"].iloc[-1])
                    if v and v == v:  # not NaN
                        close = v; break
            except Exception as e:
                print(f"[yahoo] {code}{suf} fail:", e)
                time.sleep(0.5)
        if close is not None:
            out.append((code, close))
    return pd.DataFrame(out, columns=["股票代號","收盤價"])

def main():
    date_str = _norm_date(os.getenv("REPORT_DATE"))
    yyyymmdd = date_str.replace("-", "")
    src = DATA_DIR / f"{date_str}.csv"
    if not src.exists():
        raise SystemExit(f"找不到當日持股 CSV：{src}")

    # 代號清單（字串）
    codes = pd.read_csv(src, encoding="utf-8-sig")["股票代號"].astype(str).str.strip().unique().tolist()

    twse = _fetch_twse(yyyymmdd)
    tpex = _fetch_tpex(yyyymmdd)
    px = pd.concat([twse, tpex], ignore_index=True).drop_duplicates("股票代號")

    # Yahoo 補齊缺的
    miss = sorted(set(codes) - set(px["股票代號"].tolist()))
    if miss:
        print("[price] missing from TWSE/TPEx:", miss[:10], "..." if len(miss)>10 else "")
        ydf = _fetch_yahoo(miss, date_str)
        px = pd.concat([px, ydf], ignore_index=True).drop_duplicates("股票代號")

    # 最終只保留需要的代號
    px = px[px["股票代號"].isin(codes)].copy()
    px["收盤價"] = pd.to_numeric(px["收盤價"], errors="coerce")

    out = PRICE_DIR / f"{date_str}.csv"
    px.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[prices] {len(px)}/{len(codes)} saved -> {out}")

if __name__ == "__main__":
    main()
