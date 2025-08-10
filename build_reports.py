# build_reports.py
import os
import re
import glob
import math
import pandas as pd
from datetime import datetime
from pathlib import Path
from config import TOP_N, THRESH_UPDOWN_EPS, NEW_WEIGHT_MIN, REPORT_DIR, PCT_DECIMALS

DATA_DIR = "data"
Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)

def _normalize_cols(cols):
    return [str(c).strip().replace("　","").replace("\u3000","") for c in cols]

def _read_today_csv():
    today = datetime.today().strftime("%Y-%m-%d")
    p = os.path.join(DATA_DIR, f"{today}.csv")
    if os.path.exists(p):
        return today, pd.read_csv(p)
    # fallback: 取最新一份
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    if not files:
        return None, None
    return Path(files[-1]).stem, pd.read_csv(files[-1])

def _read_by_date(date_str):
    p = os.path.join(DATA_DIR, f"{date_str}.csv")
    return pd.read_csv(p) if os.path.exists(p) else None

def _list_dates_sorted():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    dates = []
    for f in files:
        m = re.search(r"(\d{4}-\d{2}-\d{2})\.csv$", f)
        if m:
            dates.append(m.group(1))
    dates = sorted(set(dates))
    return dates

def _to_weight_num(df):
    # 欄名容錯
    df = df.copy()
    df.columns = _normalize_cols(df.columns)
    cand = ["持股權重","持股比例","權重","占比","比重(%)","占比(%)"]
    col = None
    for c in cand:
        if c in df.columns:
            col = c; break
    if col is None:
        df["持股權重_num"] = 0.0
    else:
        s = (df[col].astype(str)
                     .str.replace(",","",regex=False)
                     .str.replace("%","",regex=False))
        df["持股權重_num"] = pd.to_numeric(s, errors="coerce").fillna(0.0)
    # 代號/名稱容錯
    if "股票代號" not in df.columns:
        # 有些來源可能叫 證券代號/代碼
        for alt in ["證券代號","代號","證券代號/代碼"]:
            if alt in df.columns:
                df["股票代號"] = df[alt]
                break
    if "股票名稱" not in df.columns:
        for alt in ["證券名稱","名稱"]:
            if alt in df.columns:
                df["股票名稱"] = df[alt]
                break
    return df

def _fmt_pct(x):
    if pd.isna(x):
        return ""
    return f"{float(x):.{PCT_DECIMALS}f}%"

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
    # 名稱回填
    merged["股票名稱"] = merged["股票名稱_今"].fillna(merged["股票名稱_昨"])
    return merged

def _save_up_down_today(today, merged):
    out = merged.copy()
    # 全量輸出（含 D1_Δ=0），同時加上格式欄位
    out["昨日權重%"] = out["昨_權重"].round(PCT_DECIMALS)
    out["今日權重%"] = out["今_權重"].round(PCT_DECIMALS)
    out["Δ%"] = out["D1_Δ"].round(PCT_DECIMALS)
    cols = ["股票代號","股票名稱","昨日權重%","今日權重%","Δ%","_merge"]
    out = out[cols]
    path = os.path.join(REPORT_DIR, f"up_down_today_{today}.csv")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return path

def _save_new_gt_threshold(today, merged, threshold_pct):
    # 昨天沒有、今天有，且今_權重 > 門檻
    new_df = merged[(merged["_merge"]=="right_only") & (merged["今_權重"] > threshold_pct)]
    if new_df.empty:
        path = os.path.join(REPORT_DIR, f"new_gt_{str(threshold_pct).replace('.','p')}_{today}.csv")
        pd.DataFrame(columns=["股票代號","股票名稱","今日權重%","今日股數"]).to_csv(path, index=False, encoding="utf-8-sig")
        return path
    new_df = new_df.copy()
    new_df["今日權重%"] = new_df["今_權重"].round(PCT_DECIMALS)
    # 今日股數若存在就帶出（從今日原始 df 取，但我們此程式不直接拿到，留空或日後擴充）
    new_df["今日股數"] = ""
    out = new_df[["股票代號","股票名稱","今日權重%","今日股數"]].rename(columns={"股票名稱":"股票名稱"})
    # 依權重高到低
    out = out.sort_values("今日權重%", ascending=False)
    path = os.path.join(REPORT_DIR, f"new_gt_0p5_{today}.csv")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return path

def _save_weights_chg_5d(today, today_df):
    # 找到今天往回的第5份可用快照
    dates = _list_dates_sorted()
    if today not in dates:
        # 可能是 fallback 讀的最新檔
        dates.append(today)
        dates = sorted(set(dates))
    try:
        idx = dates.index(today)
    except ValueError:
        return None
    if idx < 5:
        # 歷史不足 5 份
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
    # 同時計算昨日（方便信件摘要一起用）
    # 找到 idx-1 的前一日
    d1_date = dates[idx-1] if idx >= 1 else None
    y_df = _read_by_date(d1_date) if d1_date else None
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
        print("No data found in data/; skip reports.")
        return

    # 昨日資料
    try:
        yesterday = (datetime.strptime(today, "%Y-%m-%d"))
        from datetime import timedelta
        y_date = (yesterday - timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        y_date = None
    df_yest = _read_by_date(y_date) if y_date else None

    merged = _merge_yesterday(df_today, df_yest)

    # 報表1：D1 全量
    p1 = _save_up_down_today(today, merged)

    # 報表2：首次新增且 > 閾值（A: 對昨天）
    p2 = _save_new_gt_threshold(today, merged, NEW_WEIGHT_MIN)

    # 報表3：D5 變化（只針對今日仍在持股）
    p3 = _save_weights_chg_5d(today, df_today)

    print("Reports generated:", p1, p2, p3)

if __name__ == "__main__":
    main()
