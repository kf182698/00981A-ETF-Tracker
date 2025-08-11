# charts.py
import os
import re
import glob
import math
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
from config import TOP_N, REPORT_DIR, PCT_DECIMALS

DATA_DIR = "data"
CHART_DIR = "charts"
Path(CHART_DIR).mkdir(parents=True, exist_ok=True)

def _normalize_cols(cols):
    return [str(c).strip().replace("　","").replace("\u3000","") for c in cols]

def _read_latest(pattern):
    files = glob.glob(pattern)
    return max(files, key=os.path.getmtime) if files else None

def _list_dates_sorted():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    dates = []
    for f in files:
        m = re.search(r"(\d{4}-\d{2}-\d{2})\.csv$", f)
        if m: dates.append(m.group(1))
    return sorted(set(dates))

def _read_day(date_str):
    p = os.path.join(DATA_DIR, f"{date_str}.csv")
    if not os.path.exists(p): return None
    df = pd.read_csv(p)
    df.columns = _normalize_cols(df.columns)
    # 權重轉數字
    cand = ["持股權重","持股比例","權重","占比","比重(%)","占比(%)"]
    col = None
    for c in cand:
        if c in df.columns: col = c; break
    if col is None:
        df["w"] = 0.0
    else:
        df["w"] = pd.to_numeric(
            df[col].astype(str).str.replace(",","",regex=False).str.replace("%","",regex=False),
            errors="coerce"
        ).fillna(0.0)
    if "股票代號" not in df.columns:
        for alt in ["證券代號","代號","證券代號/代碼"]:
            if alt in df.columns:
                df["股票代號"] = df[alt]; break
    if "股票名稱" not in df.columns:
        for alt in ["證券名稱","名稱"]:
            if alt in df.columns:
                df["股票名稱"] = df[alt]; break
    return df[["股票代號","股票名稱","w"]]

def _pick_top5_for_trend(today):
    # 優先用 D5 報表選 |D5Δ%| 最大的 5 檔；否則用今日權重前 5
    p5 = _read_latest(os.path.join(REPORT_DIR, f"weights_chg_5d_{today}.csv"))
    if p5:
        df = pd.read_csv(p5)
        for c in ["D5Δ%","今日%"]:
            if c not in df.columns: return None
        df["absD5"] = df["D5Δ%"].abs()
        sel = df.sort_values("absD5", ascending=False).head(5)["股票代號"].tolist()
        return sel
    # fallback: 用今日權重前 5
    df_t = _read_day(today)
    if df_t is None: return None
    return df_t.sort_values("w", ascending=False).head(5)["股票代號"].tolist()

def chart_d1_bar(today):
    updown = _read_latest(os.path.join(REPORT_DIR, f"up_down_today_{today}.csv"))
    if not updown: return None
    df = pd.read_csv(updown)
    for c in ["股票代號","股票名稱","Δ%"]:
        if c not in df.columns: return None
    df["Δ%"] = pd.to_numeric(df["Δ%"], errors="coerce").fillna(0.0)
    # TopN 上/下
    up = df.sort_values("Δ%", ascending=False).head(TOP_N)
    dn = df.sort_values("Δ%", ascending=True).head(TOP_N)
    comb = pd.concat([up, dn[::-1]])  # 上前N+下前N（反向讓條形圖從負到正好看）
    labels = [f"{r['股票代號']}" for _, r in comb.iterrows()]
    vals = comb["Δ%"].tolist()

    plt.figure(figsize=(10, 6))
    y = range(len(labels))
    plt.barh(y, vals)  # 不指定顏色，避免不同客戶端差異
    plt.yticks(y, labels)
    plt.axvline(0, linewidth=1)
    plt.title(f"D1 權重變動 Top{TOP_N}（上/下） - {today}")
    plt.xlabel("Δ (百分點)")
    out = os.path.join(CHART_DIR, f"d1_top_changes_{today}.png")
    plt.tight_layout()
    plt.savefig(out, dpi=150)
    plt.close()
    return out

def chart_daily_trend(today):
    dates = _list_dates_sorted()
    if not dates: return None
    if today not in dates: today = dates[-1]
    # 取最近 12 份快照
    idx = dates.index(today)
    start = max(0, idx-11)
    span = dates[start:idx+1]

    top_codes = _pick_top5_for_trend(today)
    if not top_codes: return None

    # 組成 panel：index=date, columns=code, values=weight
    panel = {}
    for d in span:
        df = _read_day(d)
        if df is None: continue
        s = df[df["股票代號"].isin(top_codes)].set_index("股票代號")["w"]
        panel[d] = s
    if not panel: return None
    mat = pd.DataFrame(panel).T.sort_index()  # rows: date, cols: code
    # 畫圖
    plt.figure(figsize=(10, 6))
    for code in mat.columns:
        plt.plot(mat.index, mat[code], marker="o", linewidth=1.5)
    plt.title(f"每日權重趨勢（Top Movers x5） - {span[0]} → {span[-1]}")
    plt.xlabel("日期")
    plt.ylabel("權重（%）")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    out = os.path.join(CHART_DIR, f"daily_trend_{span[-1]}.png")
    plt.savefig(out, dpi=150)
    plt.close()
    return out

def chart_weekly_cum_trend(today):
    dates = _list_dates_sorted()
    if not dates: return None
    if today not in dates: today = dates[-1]
    # 取最近 12 份快照（足夠構成近幾週）
    idx = dates.index(today)
    start = max(0, idx-11)
    span = dates[start:idx+1]

    top_codes = _pick_top5_for_trend(today)
    if not top_codes: return None

    # 取每日 panel
    rows = []
    for d in span:
        df = _read_day(d)
        if df is None: continue
        df2 = df[df["股票代號"].isin(top_codes)].copy()
        df2["date"] = pd.to_datetime(d)
        rows.append(df2)
    if not rows: return None
    mat = pd.concat(rows, ignore_index=True)
    # 週聚合：每檔每週平均權重，再相對第一週取「累積變化」
    mat["week"] = mat["date"].dt.to_period("W").apply(lambda p: p.start_time.date())
    pivot = mat.pivot_table(index=["week"], columns="股票代號", values="w", aggfunc="mean").sort_index()
    base = pivot.iloc[0]
    cum = (pivot - base)  # 對第一週的變化
    plt.figure(figsize=(10, 6))
    for code in cum.columns:
        plt.plot(cum.index, cum[code], marker="o", linewidth=1.5)
    plt.title(f"週累積權重變化（對第一週） - {cum.index.min()} → {cum.index.max()}")
    plt.xlabel("週起始日")
    plt.ylabel("累積變化（百分點）")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    out = os.path.join(CHART_DIR, f"weekly_cum_trend_{today}.png")
    plt.savefig(out, dpi=150)
    plt.close()
    return out

def main():
    today = datetime.today().strftime("%Y-%m-%d")
    # 產圖
    p1 = chart_d1_bar(today)
    p2 = chart_daily_trend(today)
    p3 = chart_weekly_cum_trend(today)
    print("Charts:", p1, p2, p3)

if __name__ == "__main__":
    main()
