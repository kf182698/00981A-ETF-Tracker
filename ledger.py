# ledger.py — 逐檔滾算平均成本：ledger/{代碼}.csv
import os
from pathlib import Path
from datetime import datetime
import pandas as pd

LEDGER_DIR = "ledger"
Path(LEDGER_DIR).mkdir(parents=True, exist_ok=True)

SMALL_DELTA_SHARES = 1  # 忽略極小股數誤差

def _ledger_path(code): return os.path.join(LEDGER_DIR, f"{code}.csv")

def _load_latest(code):
    p = _ledger_path(code)
    if not os.path.exists(p):
        return 0, 0.0  # shares, avg_cost
    df = pd.read_csv(p)
    if df.empty: return 0, 0.0
    last = df.iloc[-1]
    return int(last["shares"]), float(last["avg_cost"])

def _append_ledger(code, row):
    p = _ledger_path(code)
    df = pd.DataFrame([row])
    header = not os.path.exists(p)
    df.to_csv(p, index=False, mode="a", header=header, encoding="utf-8-sig")

def update_ledgers(today_csv, date_str):
    """
    讀取 data/YYYY-MM-DD.csv → 逐檔更新 ledger。
    today_csv 欄位需含：股票代號, 股票名稱, 股數, 持股權重, 收盤價
    """
    df = pd.read_csv(today_csv)
    if "收盤價" not in df.columns:
        df["收盤價"] = None

    for _, r in df.iterrows():
        code = str(r["股票代號"]).strip()
        close = float(r["收盤價"]) if pd.notna(r["收盤價"]) else None
        shares_t = int(r["股數"])

        shares_prev, avg_prev = _load_latest(code)

        delta = shares_t - shares_prev
        # 忽略極小誤差
        if abs(delta) < SMALL_DELTA_SHARES:
            delta = 0

        if shares_prev == 0 and shares_t == 0:
            # 沒持倉跳過
            continue

        if delta > 0:
            # 買進：用收盤價估成交
            trade_price = close if close is not None else avg_prev
            cost_prev = shares_prev * avg_prev
            cost_new  = cost_prev + delta * (trade_price if trade_price is not None else 0.0)
            avg_new   = (cost_new / shares_t) if shares_t > 0 else 0.0
            cash_flow = - delta * (trade_price if trade_price is not None else 0.0)
            note = "buy_est_close"
        elif delta < 0:
            # 賣出：按平均成本出庫（均價不變）
            trade_price = avg_prev
            cost_prev = shares_prev * avg_prev
            cost_new  = cost_prev + delta * avg_prev  # delta < 0
            avg_new   = (cost_new / shares_t) if shares_t > 0 else 0.0
            cash_flow = - delta * avg_prev  # 賣出流入（正值）
            note = "sell_avg_cost"
        else:
            trade_price = None
            avg_new = avg_prev
            cash_flow = 0.0
            note = "hold"

        _append_ledger(code, {
            "date": date_str,
            "shares": shares_t,
            "avg_cost": round(avg_new, 6),
            "delta_shares": int(delta),
            "trade_price": round(trade_price, 6) if trade_price is not None else "",
            "cash_flow": round(cash_flow, 2),
            "note": note
        })

def load_avg_cost_map():
    """回傳 {code: avg_cost} (ledger 最末一筆)"""
    out={}
    if not os.path.exists(LEDGER_DIR): return out
    for fn in os.listdir(LEDGER_DIR):
        if not fn.endswith(".csv"): continue
        code = fn[:-4]
        df = pd.read_csv(os.path.join(LEDGER_DIR, fn))
        if df.empty: continue
        out[code] = float(df.iloc[-1]["avg_cost"])
    return out