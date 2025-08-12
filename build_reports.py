# build_reports.py
import os
import re
import glob
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from config import TOP_N, THRESH_UPDOWN_EPS, NEW_WEIGHT_MIN, SELL_ALERT_THRESHOLD, REPORT_DIR, PCT_DECIMALS

DATA_DIR = "data"
Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)

def _normalize_cols(cols):
    return [str(c).strip().replace("　","").replace("\u3000","") for c in cols]

def _list_dates_sorted():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    dates = []
    for f in files:
        m = re.search(r"(\d{4}-\d{2}-\d{2})\.csv$", f)
        if m: dates.append(m.group(1))
    return sorted(set(dates))

def _read_by_date(d):
    p = os.path.join(DATA_DIR, f"{d}.csv")
    return pd.read_csv(p) if (d and os.path.exists(p)) else None

def _read_today_csv():
    today = datetime.today().strftime("%Y-%m-%d")
    p = os.path.join(DATA_DIR, f"{today}.csv")
    if os.path.exists(p): return today, pd.read_csv(p)
    # fallback: 最新一份
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    if not files: return None, None
    from pathlib import Path
    return Path(files[-1]).stem, pd.read_csv(files[-1])

def _prev_available_date(today_str):
    dates = _list_dates_sorted()
    if today_str not in dates:
        dates = [*dates, today_str]
        dates = sorted(set(dates))
    try:
        idx = dates.index(today_str)
    except ValueError:
        return None
    prevs = dates[:idx]  # 全部在今天之前
    return prevs[-1] if prevs else None

def _to_weight_num(df):
    df = df.copy()
    df.columns = _normalize_cols(df.columns)
    # 權重欄位
    col = None
    for c in ["持股權重","持股比例","權重","占比","比重(%)","占比(%)"]:
        if c in df.columns: col = c; break
    if col is None:
        df["持股權重_num"] = 0.0
    else:
        s = (df[col].astype(str).str.replace(",","",regex=False).str.replace("%","",regex=False))
        df["持股權重_num"] = pd.to_numeric(s, errors="coerce").fillna(0.0)
    # 代號/名稱容錯
    if "股票代號" not in df.columns:
        for alt in ["證券代號","代號","證券代號/代碼"]:
            if alt in df.columns:
                df["股票代號"] = df[alt]; break
    if "股票名稱" not in df.columns:
        for alt in ["證券名稱","名稱"]:
            if alt in df.columns:
                df["股票名稱"] = df[alt]; break
    return df

def _merge_yesterday(today_df, yest_df):
    t = _to_weight_num(today_df)
    y = _to_weight_num(yest_df) if yest_df is not None else pd.DataFrame(columns=t.columns)
    merged = pd.merge(
        y[["股票代號","股票名稱","持股權重_num"]].rename(columns={"股票名稱":"股票名稱_昨","持股權重_num":"昨_權重"}),
        t[["股票代號","股票名稱","持股權重_num"]].rename(columns={"股票名稱":"股票名稱_今","持股權重_num":"今_權重"}),
        on="股票代號", how="outer", indicator=True
    )
    merged["昨_權重"] = merged["昨_權重"].fillna(0.0)
    merged["今_權重"] = merged["今_權重"].fillna(0.0)
    merged["D1_Δ"] = merged["今_權重"] - merged["昨_權重"]
    merged["股票名稱"] = merged["股票名稱_今"].fillna(merged["股票名稱_昨"])
    return merged

def _save_up_down_today(today, merged):
    out = merged.copy()
    out["昨日權重%"] = out["昨_權重"].round(PCT_DECIMALS)
    out["今日權重%"] = out["今_權重"].round(PCT_DECIMALS)
    out["Δ%"] = out["D1_Δ"].round(PCT_DECIMALS)
    cols = ["股票代號","股票名稱","昨日權重%","今日權重%","Δ%","_merge"]
    out = out[cols]
    path = os.path.join(REPORT_DIR, f"up_down_today_{today}.csv")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return path

def _save_new_gt_threshold(today, merged, threshold_pct):
    new_df = merged[(merged["_merge"]=="right_only") & (merged["今_權重"] > threshold_pct)].copy()
    if new_df.empty:
        path = os.path.join(REPORT_DIR, f"new_gt_{str(threshold_pct).replace('.','p')}_{today}.csv")
        pd.DataFrame(columns=["股票代號","股票名稱","今日權重%","今日股數"]).to_csv(path, index=False, encoding="utf-8-sig")
        return path
    new_df["今日權重%"] = new_df["今_權重"].round(PCT_DECIMALS)
    new_df["今日股數"] = ""  # 目前沒有帶股數進來，保留欄位以便後續擴充
    out = new_df[["股票代號","股票名稱","今日權重%","今日股數"]].rename(columns={"股票名稱":"股票名稱"})
    out = out.sort_values("今日權重%", ascending=False)
    path = os.path.join(REPORT_DIR, f"new_gt_{str(threshold_pct).replace('.','p')}_{today}.csv")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return path

def _save_sell_alerts(today, merged, threshold_pct):
    # 定義：昨日 > 門檻 且 今日 <= 門檻 且 D1 為負（避免 0→0 誤報）
    alert = merged[(merged["昨_權重"] > threshold_pct) & (merged["今_權重"] <= threshold_pct) & (merged["D1_Δ"] < 0)].copy()
    alert["昨日權重%"] = alert["昨_權重"].round(PCT_DECIMALS)
    alert["今日權重%"] = alert["今_權重"].round(PCT_DECIMALS)
    alert["Δ%"] = alert["D1_Δ"].round(PCT_DECIMALS)
    out = alert[["股票代號","股票名稱","昨日權重%","今日權重%","Δ%"]]
    path = os.path.join(REPORT_DIR, f"sell_alerts_{today}.csv")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return path

def _save_weights_chg_5d(today, today_df):
    dates = _list_dates_sorted()
    if today not in dates:
        dates.append(today); dates = sorted(set(dates))
    idx = dates.index(today)
    if idx < 5:
        path = os.path.join(REPORT_DIR, f"weights_chg_5d_{today}.csv")
        pd.DataFrame(columns=["股票代號","股票名稱","今日%","昨日%","D1Δ%","T-5日%","D5Δ%"]).to_csv(path, index=False, encoding="utf-8-sig")
        return path

    d5 = dates[idx-5]
    df_t = _to_weight_num(today_df)
    df_d5 = _to_weight_num(_read_by_date(d5))

    # 只針對今日仍在持股
    m = pd.merge(
        df_t[["股票代號","股票名稱","持股權重_num"]].rename(columns={"持股權重_num":"今_權重"}),
        df_d5[["股票代號","持股權重_num"]].rename(columns={"持股權重_num":"T5_權重"}),
        on="股票代號", how="left"
    )
    m["T5_權重"] = m["T5_權重"].fillna(0.0)

    # 昨日用「最近一份小於今天的可用快照」回補
    prev_date = _prev_available_date(today)
    y_df = _read_by_date(prev_date) if prev_date else None
    if y_df is not None:
        y_df = _to_weight_num(y_df)[["股票代號","持股權重_num"]].rename(columns={"持股權重_num":"昨_權重"})
        m = pd.merge(m, y_df, on="股票代號", how="left")
        m["昨_權重"] = m["昨_權重"].fillna(0.0)
    else:
        m["昨_權重"] = 0.0

    m["D1_Δ"] = m["今_權重"] - m["昨_權重"]
    m["D5_Δ"] = m["今_權重"] - m["T5_權重"]

    out = m.copy()
    out["今日%"]  = out["今_權重"].round(PCT_DECIMALS)
    out["昨日%"]  = out["昨_權重"].round(PCT_DECIMALS)
    out["D1Δ%"]  = out["D1_Δ"].round(PCT_DECIMALS)
    out["T-5日%"] = out["T5_權重"].round(PCT_DECIMALS)
    out["D5Δ%"]  = out["D5_Δ"].round(PCT_DECIMALS)
    out = out[["股票代號","股票名稱","今日%","昨日%","D1Δ%","T-5日%","D5Δ%"]]

    path = os.path.join(REPORT_DIR, f"weights_chg_5d_{today}.csv")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return path

def main():
    today, df_today = _read_today_csv()
    if df_today is None:
        print("No data found; skip reports.")
        return

    # 昨日→以「最近一份在今天之前的檔」回補
    prev_date = _prev_available_date(today)
    df_yest = _read_by_date(prev_date) if prev_date else None

    merged = _merge_yesterday(df_today, df_yest)

    p1 = _save_up_down_today(today, merged)
    p2 = _save_new_gt_threshold(today, merged, NEW_WEIGHT_MIN)
    p3 = _save_weights_chg_5d(today, df_today)
    p4 = _save_sell_alerts(today, merged, SELL_ALERT_THRESHOLD)

    print("Reports generated:", p1, p2, p3, p4)

if __name__ == "__main__":
    main()
