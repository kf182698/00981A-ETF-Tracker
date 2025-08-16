# ========= 當日收盤價抓取（修正版：TWSE rwd 端點 + 正確欄位 + TPEX 正確路徑 + 就近回填） =========
import math
import random

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.twse.com.tw/zh/trading/historical/stock-day.html",
    "Connection": "keep-alive",
})

def _to_roc_date(yyyy_mm_dd: str) -> str:
    y, m, d = yyyy_mm_dd.split("-")
    roc = int(y) - 1911
    return f"{roc:03d}/{int(m):02d}/{int(d):02d}"

def _retry(fn, times=3, sleep=0.6):
    last = None
    for i in range(times):
        try:
            return fn()
        except Exception as e:
            last = e
            time.sleep(sleep + random.random()*0.3)
    if last:
        raise last
    return None

def _twse_stock_day(code: str, date_yyyymmdd: str):
    """
    TWSE RWD 端點（官方新版）：
    https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=YYYYMMDD&stockNo=XXXX
    回傳 data 欄位每列：
    [日期(ROC), 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌價差, 成交筆數]
    我們要的收盤價在 index=6。
    """
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
    params = {"response": "json", "date": date_yyyymmdd, "stockNo": code}
    def _do():
        r = SESSION.get(url, params=params, timeout=12)
        r.raise_for_status()
        j = r.json()
        if j.get("stat") != "OK":
            return None
        rows = j.get("data", [])
        if not rows:
            return None
        # 目標日期（ROC），若當天沒資料，用「就近不晚於目標日」的最近一筆
        target_roc = _to_roc_date(f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:]}")
        candidate = None
        for row in rows:
            d = str(row[0]).strip()
            if d == target_roc:
                candidate = row
                break
            # 記住最後一筆（通常是最近交易日）
            candidate = row
        if candidate is None:
            return None
        close_str = str(candidate[6]).replace(",", "")
        return float(close_str)
    return _retry(_do)

def _tpex_daily(code: str, roc_yyy_mm: str, roc_yyy_mm_dd: str):
    """
    TPEX（櫃買）月表端點：
    https://www.tpex.org.tw/www/stock/trading/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d=113/08&stkno=XXXX
    aaData 每列：
    [日期, 成交股數, 成交金額, 開盤, 最高, 最低, 收盤, 漲跌, 均價, 成交筆數, ...]
    收盤在 index=6。若找不到當日，取月內最後一筆（就近回填）。
    """
    url = "https://www.tpex.org.tw/www/stock/trading/aftertrading/daily_trading_info/st43_result.php"
    params = {"l": "zh-tw", "d": roc_yyy_mm, "stkno": code}
    def _do():
        r = SESSION.get(url, params=params, timeout=12)
        r.raise_for_status()
        j = r.json()
        rows = j.get("aaData") or j.get("data") or []
        if not rows:
            return None
        # 先找精確日期；不然就用月內最後一筆
        cand = None
        for row in rows:
            if str(row[0]).strip() == roc_yyy_mm_dd:
                cand = row
                break
            cand = row
        if cand is None:
            return None
        close_str = str(cand[6]).replace(",", "")
        return float(close_str)
    return _retry(_do)

def fetch_close_price(code: str, ymd: str) -> float | None:
    """
    先嘗試 TWSE；失敗再試 TPEX。
    若當天/當月無資料，會以月表「就近最後一筆」回填。
    """
    ymd_compact = ymd.replace("-", "")
    # TWSE（上市）
    try:
        c = _twse_stock_day(code, ymd_compact)
        if c is not None and not math.isnan(c):
            return round(float(c), 2)
    except Exception:
        pass
    # TPEX（上櫃）
    try:
        roc_date  = _to_roc_date(ymd)     # 113/08/09
        roc_month = roc_date[:7]          # 113/08
        c = _tpex_daily(code, roc_month, roc_date)
        if c is not None and not math.isnan(c):
            return round(float(c), 2)
    except Exception:
        pass
    return None
