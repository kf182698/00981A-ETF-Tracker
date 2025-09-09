#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
依據「data/ 下新進/變動的 CSV」，新增/覆蓋「收盤價」欄位。
資料來源只用官方：
- TWSE 月表 STOCK_DAY（JSON / CSV 皆可；本程式走 JSON）
- 若該代號在 TWSE 當月查無 → 回補 TPEx 每日收盤（整日 CSV，單日取一次後映射所有代號）
規則：
- 同日值一律覆蓋 (--overwrite-same-day)
- 若當日無收盤價 → 依序往前最多 15 天 (--max-backdays 可調)
- 若仍無 → 填空值（NA）

注意：不判斷上市/上櫃，直接先打 TWSE；拿不到再嘗試 TPEx（契合你的第 2 點）。
"""

import argparse
import csv
import io
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from dateutil import tz, parser as dtparser

TPE_TZ = tz.gettz("Asia/Taipei")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ClosePriceBot/1.0; +https://github.com/)",
    "Accept": "application/json, text/plain, */*",
}

TWSE_STOCK_DAY = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
# TPEx 走「每日收盤表」頁面背後的 CSV 下載端點。實務上該頁提供 CSV 下載，
# 這裡採可用且穩定的 download 端點（若未來路徑調整，僅需在此集中修改）。
TPEX_DAILY_CSV = "https://www.tpex.org.tw/en/stock/aftertrading/DAILY_CLOSE_quotes/stk_quote_download.php"

DATE_RE_YYYYMMDD = re.compile(r"^\d{8}$")
DATE_RE_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-list-file", required=True, help="包含本次需處理 CSV 清單的檔案（每行一個路徑）")
    ap.add_argument("--max-backdays", type=int, default=15, help="往前回補天數上限（預設 15）")
    ap.add_argument("--overwrite-same-day", action="store_true", help="同日值一律覆蓋")
    return ap.parse_args()

def _read_changed_list(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def _guess_report_date_from_filename(path: str) -> Optional[datetime]:
    base = os.path.basename(path)
    name, _ = os.path.splitext(base)
    # 支援 YYYY-MM-DD 或 YYYYMMDD
    if DATE_RE_ISO.match(name):
        return dtparser.parse(name).replace(tzinfo=TPE_TZ)
    if DATE_RE_YYYYMMDD.match(name):
        return dtparser.parse(name).replace(tzinfo=TPE_TZ)
    return None

def _ensure_code(s: str) -> str:
    s = s.strip()
    # 去除可能的 .TW/.TWO 後補零到4位
    s = s.replace(".TW", "").replace(".TWO", "")
    return s.zfill(4) if s.isdigit() else s

# ---------- TWSE 來源 ----------

def fetch_twse_month_json(stock_no: str, any_day: datetime) -> Optional[dict]:
    """
    取「該日所在月份」之 TWSE STOCK_DAY JSON
    參考格式：/exchangeReport/STOCK_DAY?response=json&date=20250101&stockNo=2330
    回傳 dict 或 None
    """
    y = any_day.year
    m = any_day.month
    date_param = f"{y}{m:02d}01"
    params = {"response": "json", "date": date_param, "stockNo": stock_no}
    resp = requests.get(TWSE_STOCK_DAY, params=params, headers=HEADERS, timeout=20)
    if resp.status_code != 200:
        return None
    js = resp.json()
    if js.get("stat") and "OK" in js.get("stat"):
        return js
    # 有些情況 stat 可能是中文 OK，這裡保守再檢查 data 存在
    if "data" in js and js.get("data"):
        return js
    return None

def parse_twse_close_map(js: dict) -> Dict[str, float]:
    """
    從 TWSE STOCK_DAY JSON 解析出 { 'YYYY-MM-DD': 收盤價 } 映射
    data 每列格式通常為：[日期, 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌價差, 成交筆數]
    日期可能為西元或民國格式，這裡全部轉成 YYYY-MM-DD。
    """
    out = {}
    rows = js.get("data", [])
    for row in rows:
        if len(row) < 7:
            continue
        raw_date = str(row[0]).strip()
        close_str = str(row[6]).replace(",", "").strip()
        # 日期可能像 "2025/09/08" 或 "114/09/08"
        parts = raw_date.split("/")
        if len(parts) == 3:
            y = int(parts[0])
            if y < 1911:  # 民國轉西元
                y += 1911
            try:
                dt = datetime(y, int(parts[1]), int(parts[2]), tzinfo=TPE_TZ)
            except ValueError:
                continue
            dkey = dt.date().isoformat()
        else:
            # fallback 直接 parse
            try:
                dt = dtparser.parse(raw_date).astimezone(TPE_TZ)
                dkey = dt.date().isoformat()
            except Exception:
                continue

        try:
            close = float(close_str)
        except ValueError:
            continue
        out[dkey] = close
    return out

# ---------- TPEx 來源（每日 CSV 整批取一次） ----------

def fetch_tpex_daily_csv(date_dt: datetime) -> Optional[pd.DataFrame]:
    """
    取得 TPEx 指定日期的「每日收盤表」CSV。
    英文頁提供 CSV 下載，d=民國年月日，例如 114/09/08。
    """
    roc_y = date_dt.year - 1911
    roc_date = f"{roc_y:03d}/{date_dt.month:02d}/{date_dt.day:02d}"
    params = {"d": roc_date}  # 若未來官方改參數，此處集中調整
    try:
        resp = requests.get(TPEX_DAILY_CSV, params=params, headers=HEADERS, timeout=30)
        if resp.status_code != 200 or not resp.text.strip():
            return None
        # CSV 內含英文逗號與千分位，先丟給 pandas 讀
        content = resp.content.decode("utf-8", errors="ignore")
        df = pd.read_csv(io.StringIO(content))
        # 嘗試標準化欄名：常見如「證券代號」「收盤」
        cols = {c: str(c).strip() for c in df.columns}
        df.rename(columns=cols, inplace=True)
        # 找出代號與收盤相關欄位（不同語系可能略有差異）
        # 典型英文 CSV 欄名可能是 "Code","Closing Price"；中文是「證券代號」「收盤」
        return df
    except Exception:
        return None

def build_tpex_code_close_map(df: pd.DataFrame) -> Dict[str, float]:
    code_col = None
    close_col = None
    # 嘗試多種欄名
    for c in ["證券代號", "代號", "Code", "Symbol"]:
        if c in df.columns:
            code_col = c
            break
    for c in ["收盤", "收盤價", "Closing Price", "Close"]:
        if c in df.columns:
            close_col = c
            break
    if code_col is None or close_col is None:
        return {}
    out = {}
    for _, row in df.iterrows():
        code = _ensure_code(str(row[code_col]))
        try:
            close = float(str(row[close_col]).replace(",", ""))
        except ValueError:
            continue
        out[code] = close
    return out

# ---------- 抽象查價：先 TWSE → 再 TPEx，最長回補 N 天 ----------

def get_close_price_for_code(code: str, target_date: datetime, max_backdays: int,
                             tpex_cache: Dict[str, Dict[str, float]]) -> Tuple[Optional[float], Optional[str]]:
    """
    回傳 (收盤價, 實際取價日期字串YYYY-MM-DD)
    """
    code = _ensure_code(code)
    d = target_date

    for i in range(max_backdays + 1):
        day = (target_date - timedelta(days=i)).astimezone(TPE_TZ)
        dkey = day.date().isoformat()

        # 1) TWSE 當月資料
        twse_json = fetch_twse_month_json(code, day)
        if twse_json:
            m = parse_twse_close_map(twse_json)
            if dkey in m:
                return m[dkey], dkey
            # 當月無該日，但也許往前日存在（例如當月前幾日仍可用）
            # 若 i==0 且當日無值，會繼續 i=1,2... 的回補流程
        # 2) TPEx 當日整批 CSV（用快取避免重抓）
        if dkey not in tpex_cache:
            df = fetch_tpex_daily_csv(day)
            tpex_cache[dkey] = build_tpex_code_close_map(df) if df is not None else {}
        close_map = tpex_cache.get(dkey, {})
        if code in close_map:
            return close_map[code], dkey

    # 超過回補上限
    return None, None

# ---------- 主流程：處理變動的 CSV ----------

def process_csv(path: str, max_backdays: int, overwrite_same_day: bool):
    df = pd.read_csv(path, dtype=str)

    # 找股票代號欄位
    code_col = None
    for c in ["股票代號", "代號", "證券代號", "code", "Code"]:
        if c in df.columns:
            code_col = c
            break
    if code_col is None:
        print(f"[WARN] {path} 找不到股票代號欄位，略過。")
        return False

    # 目標日期：優先從檔名推斷，否則用台北當日
    rpt_dt = _guess_report_date_from_filename(path)
    if rpt_dt is None:
        rpt_dt = datetime.now(TPE_TZ)

    # 既有收盤價欄位處理
    if "收盤價" not in df.columns:
        df["收盤價"] = pd.NA

    # 準備代號清單
    codes = df[code_col].astype(str).map(_ensure_code).tolist()

    # TPEx 當日整批 CSV 的快取（key: YYYY-MM-DD → {code: close}）
    tpex_cache: Dict[str, Dict[str, float]] = {}

    changed = False
    for idx, code in enumerate(codes):
        old_val = df.at[idx, "收盤價"]
        close_val, got_date = get_close_price_for_code(
            code=code,
            target_date=rpt_dt,
            max_backdays=max_backdays,
            tpex_cache=tpex_cache,
        )

        # 填值邏輯：同日覆蓋；其餘若原本為空也寫入
        if close_val is not None:
            if pd.isna(old_val) or overwrite_same_day:
                df.at[idx, "收盤價"] = close_val
                changed = True
        else:
            # 仍無法取得 → 保持 NA（空值）
            if pd.notna(old_val):
                # 若原本有值但我們規則是覆蓋同日才動手，這裡不改舊值
                pass

    if changed:
        # 輸出為 UTF-8 with BOM（多數國產 Excel 友善）
        df.to_csv(path, index=False, encoding="utf-8-sig")
    return changed

def main():
    args = parse_args()
    csv_paths = _read_changed_list(args.csv_list_file)
    any_changed = False
    for p in csv_paths:
        if not os.path.exists(p):
            continue
        print(f"[INFO] Processing {p}")
        chg = process_csv(p, max_backdays=args.max_backdays, overwrite_same_day=args.overwrite_same_day)
        print(f"[INFO] {p} changed={chg}")
        any_changed = any_changed or chg
    if not any_changed:
        print("[INFO] No CSV updated.")

if __name__ == "__main__":
    main()
