#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
依據 data/ 下「本次變動的 CSV 清單」，新增/覆蓋「收盤價」欄位。
資料來源僅官方：
- TWSE 月表 STOCK_DAY（JSON）
- 若該代號該月查無：備援 TPEx 「每日收盤表」CSV
規則：
- 同日值一律覆蓋 (--overwrite-same-day)
- 當日無收盤價則往前回補，最多 --max-backdays 天（預設 15）
- 仍無則 NA（空值）
不改動 daily_fetch.yml；此腳本由獨立 workflow 在 data 有新 CSV push 後執行。
"""

import argparse
import io
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from dateutil import tz, parser as dtparser

TPE_TZ = tz.gettz("Asia/Taipei")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ClosePriceBot/1.0; +https://github.com/)",
    "Accept": "application/json, text/plain, */*",
}

# 官方端點（集中管理，若未來調整只需改此處）
TWSE_STOCK_DAY = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
TPEX_DAILY_CSV = "https://www.tpex.org.tw/en/stock/aftertrading/DAILY_CLOSE_quotes/stk_quote_download.php"

DATE_RE_YYYYMMDD = re.compile(r"^\d{8}$")
DATE_RE_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-list-file", required=True, help="本次需處理 CSV 清單檔（每行一個路徑）")
    ap.add_argument("--max-backdays", type=int, default=15, help="往前回補天數上限（預設 15）")
    ap.add_argument("--overwrite-same-day", action="store_true", help="同日值一律覆蓋")
    return ap.parse_args()

def _read_changed_list(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def _guess_report_date_from_filename(path: str) -> Optional[datetime]:
    base = os.path.basename(path)
    name, _ = os.path.splitext(base)
    if DATE_RE_ISO.match(name) or DATE_RE_YYYYMMDD.match(name):
        return dtparser.parse(name).replace(tzinfo=TPE_TZ)
    return None

def _ensure_code(s: str) -> str:
    s = s.strip().replace(".TW", "").replace(".TWO", "")
    return s.zfill(4) if s.isdigit() else s

# ---------------------- TWSE ----------------------

def fetch_twse_month_json(stock_no: str, any_day: datetime) -> Optional[dict]:
    # 取該日所在月的月表（一次拿整月）
    date_param = f"{any_day.year}{any_day.month:02d}01"
    params = {"response": "json", "date": date_param, "stockNo": stock_no}
    try:
        resp = requests.get(TWSE_STOCK_DAY, params=params, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return None
        js = resp.json()
        if js.get("data"):
            return js
        return None
    except Exception:
        return None

def parse_twse_close_map(js: dict) -> Dict[str, float]:
    out: Dict[str, float] = {}
    rows = js.get("data", [])
    for row in rows:
        if len(row) < 7:
            continue
        raw_date = str(row[0]).strip()
        close_str = str(row[6]).replace(",", "").strip()
        # 可能為 2025/09/08 或 114/09/08（民國）
        parts = raw_date.split("/")
        try:
            if len(parts) == 3:
                y = int(parts[0]); m = int(parts[1]); d = int(parts[2])
                if y < 1911:
                    y += 1911
                dt = datetime(y, m, d, tzinfo=TPE_TZ)
            else:
                dt = dtparser.parse(raw_date).astimezone(TPE_TZ)
        except Exception:
            continue
        try:
            close = float(close_str)
        except ValueError:
            continue
        out[dt.date().isoformat()] = close
    return out

# ---------------------- TPEx ----------------------

def fetch_tpex_daily_csv(date_dt: datetime) -> Optional[pd.DataFrame]:
    """下載 TPEx 該日的每日收盤 CSV（英文站）。"""
    roc_y = date_dt.year - 1911
    roc_date = f"{roc_y:03d}/{date_dt.month:02d}/{date_dt.day:02d}"
    params = {"d": roc_date}
    try:
        resp = requests.get(TPEX_DAILY_CSV, params=params, headers=HEADERS, timeout=30)
        if resp.status_code != 200 or not resp.text.strip():
            return None
        content = resp.content.decode("utf-8", errors="ignore")
        df = pd.read_csv(io.StringIO(content))
        # 欄名統一去空白
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception:
        return None

def build_tpex_code_close_map(df: pd.DataFrame) -> Dict[str, float]:
    code_col = None
    close_col = None
    for c in ["證券代號", "代號", "Code", "Symbol"]:
        if c in df.columns:
            code_col = c; break
    for c in ["收盤", "收盤價", "Closing Price", "Close"]:
        if c in df.columns:
            close_col = c; break
    if code_col is None or close_col is None:
        return {}
    out: Dict[str, float] = {}
    for _, row in df.iterrows():
        code = _ensure_code(str(row[code_col]))
        try:
            close = float(str(row[close_col]).replace(",", ""))
        except ValueError:
            continue
        out[code] = close
    return out

# ---------------- 先 TWSE → 再 TPEx、最多回補 N 天 ----------------

def get_close_price_for_code(code: str, target_date: datetime, max_backdays: int,
                             tpex_cache: Dict[str, Dict[str, float]]) -> Tuple[Optional[float], Optional[str]]:
    code = _ensure_code(code)
    for i in range(max_backdays + 1):
        day = (target_date - timedelta(days=i)).astimezone(TPE_TZ)
        dkey = day.date().isoformat()

        # 1) TWSE：該月月表
        js = fetch_twse_month_json(code, day)
        if js:
            m = parse_twse_close_map(js)
            if dkey in m:
                return m[dkey], dkey

        # 2) TPEx：該日整批 CSV（快取避免重抓）
        if dkey not in tpex_cache:
            df = fetch_tpex_daily_csv(day)
            tpex_cache[dkey] = build_tpex_code_close_map(df) if df is not None else {}
        mp = tpex_cache.get(dkey, {})
        if code in mp:
            return mp[code], dkey

    return None, None

# ---------------- 主流程：處理單一 CSV ----------------

def process_csv(path: str, max_backdays: int, overwrite_same_day: bool) -> bool:
    # 讀檔
    df = pd.read_csv(path, dtype=str)

    # 👉 C 的欄名清洗：去 BOM 與空白，避免找不到「股票代號」
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]

    # 找代號欄位（多種常見寫法）
    code_col = None
    for c in ["股票代號", "代號", "證券代號", "code", "Code", "股票代碼", "證券代碼"]:
        if c in df.columns:
            code_col = c; break
    if code_col is None:
        print(f"[WARN] {path} 找不到股票代號欄位，略過。")
        return False

    # 目標日：優先檔名解析，否則今天（台北）
    rpt_dt = _guess_report_date_from_filename(path)
    if rpt_dt is None:
        rpt_dt = datetime.now(TPE_TZ)

    # 準備收盤價欄
    if "收盤價" not in df.columns:
        df["收盤價"] = pd.NA

    codes = df[code_col].astype(str).map(_ensure_code).tolist()
    tpex_cache: Dict[str, Dict[str, float]] = {}

    print(f"[INFO] Processing {path} (target={rpt_dt.date().isoformat()})")

    changed = False
    for idx, code in enumerate(codes):
        old_val = df.at[idx, "收盤價"]
        price, got_date = get_close_price_for_code(code, rpt_dt, max_backdays, tpex_cache)

        if price is not None:
            # 同日覆蓋；其他情況：空值才填
            if pd.isna(old_val) or overwrite_same_day:
                df.at[idx, "收盤價"] = price
                changed = True
            print(f"[OK] {code} -> {price} (date={got_date})")
        else:
            print(f"[MISS] {code} -> NA (no price within {max_backdays} days)")

    if changed:
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"[INFO] Updated: {path}")
    else:
        print(f"[INFO] No change for: {path}")
    return changed

def main():
    args = parse_args()
    csv_paths = _read_changed_list(args.csv_list_file)
    any_changed = False
    for p in csv_paths:
        if not os.path.exists(p):
            print(f"[WARN] Not found: {p}")
            continue
        chg = process_csv(p, max_backdays=args.max_backdays, overwrite_same_day=args.overwrite_same_day)
        any_changed = any_changed or chg
    if not any_changed:
        print("[INFO] No CSV updated.")

if __name__ == "__main__":
    main()
