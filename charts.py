# charts.py — Daily/Weekly trend charts with English titles & legends
import os
import re
import glob
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
from config import TOP_N, REPORT_DIR, CHART_DIR, SMOOTH_ROLLING_WINDOW

DATA_DIR = "data"
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
    col = None
    for c in ["持股權重","持股比例","權重","占比","比重(%)","占比(%)"]:
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
            if alt in df.columns: df["股票代號"] = df[alt]; break
    if "股票名稱" not in df.columns:
        for alt in ["證券名稱","名稱"]:
            if alt in df.columns: df["股票名稱"] = df[alt]; break
    return df[["股票代號","股票名稱","w"]]

def _pick_top5_for_trend(today):
    p5 = _read_latest(os.path.join(REPORT_DIR, f"weights_chg_5d_{today}.csv"))
    if p5:
        df = pd.read_csv(p5)
        if "D5Δ%" in df.columns:
            df["absD5"] = pd.to_numeric(df["D5Δ%"], errors="coerce").abs()
            sel = df.sort_values("absD5", ascending=False).head(5)["股票代號"].tolist()
            return sel
    # fallback：今日權重前 5
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
    up = df.sort_values("Δ%", ascending=False).head(TOP_N)
    dn = df.sort_values("Δ%", ascending=True).head(TOP_N)
    comb = pd.concat([up, dn[::-1]])
    labels = [f"{r['股票代號']}" for _, r in comb.iterrows()]
    vals = comb["Δ%"].tolist()

    plt.figure(figsize=(10, 6))
    y = range(len(labels))
    plt.barh(y, vals, color=["green" if v>0 else "red" for v in vals])
    plt.yticks(y, labels)
    plt.axvline(0, linewidth=1, color="black")
    plt.title(f"D1 Weight Change (Top {TOP_N} Up/Down) - {today}")  # English title
    plt.xlabel("Δ (percentage points)")  # English axis
    plt.ylabel("Stock ID")
    out = os.path.join(CHART_DIR, f"d1_top_changes_{today}.png")
    plt.tight_layout(); plt.savefig(out, dpi=150); plt.close()
    return out

def chart_daily_trend(today):
    dates = _list_dates_sorted()
    if not dates: return None
    if today not in dates: today = dates[-1]
    idx = dates.index(today)
    span = dates[max(0, idx-11): idx+1]  # last ~12 snapshots

    top_codes = _pick_top5_for_trend(today)
    if not top_codes: return None

    panel = {}
    for d in span:
        df = _read_day(d)
        if df is None: continue
        s = df[df["股票代號"].isin(top_codes)].set_index("股票代號")["w"]
        panel[d] = s
    if not panel: return None

    mat = pd.DataFrame(panel).T.sort_index()
    mat = mat.fillna(method="ffill")
    smooth = mat.rolling(window=max(1, SMOOTH_ROLLING_WINDOW), center=True, min_periods=1).mean()

    plt.figure(figsize=(10, 6))
    for code in smooth.columns:
        plt.plot(smooth.index, smooth[code], linewidth=2, label=str(code))
    plt.title(f"Daily Weight Trend (Top Movers x5) - {span[0]} → {span[-1]}")
    plt.xlabel("Date"); plt.ylabel("Weight (%)")
    plt.xticks(rotation=45, ha="right")
    plt.grid(True, axis="y", alpha=0.3)
    plt.legend(loc="best", frameon=True)
    out = os.path.join(CHART_DIR, f"daily_trend_{span[-1]}.png")
    plt.tight_layout(); plt.savefig(out, dpi=150); plt.close()
    return out

def chart_weekly_cum_trend(today):
    dates = _list_dates_sorted()
    if not dates: return None
    if today not in dates: today = dates[-1]
    idx = dates.index(today)
    span = dates[max(0, idx-11): idx+1]

    top_codes = _pick_top5_for_trend(today)
    if not top_codes: return None

    rows = []
    for d in span:
        df = _read_day(d)
        if df is None: continue
        df2 = df[df["股票代號"].isin(top_codes)].copy()
        df2["date"] = pd.to_datetime(d)
        rows.append(df2)
    if not rows: return None
    mat = pd.concat(rows, ignore_index=True)
    mat["week"] = mat["date"].dt.to_period("W").apply(lambda p: p.start_time.date())
    pivot = mat.pivot_table(index=["week"], columns="股票代號", values="w", aggfunc="mean").sort_index()
    pivot = pivot.fillna(method="ffill")
    base = pivot.iloc[0]
    cum = (pivot - base)
    smooth = cum.rolling(window=max(1, SMOOTH_ROLLING_WINDOW), center=True, min_periods=1).mean()

    plt.figure(figsize=(10, 6))
    for code in smooth.columns:
        plt.plot(smooth.index, smooth[code], linewidth=2, label=str(code))
    plt.title("Weekly Cumulative Weight Change (vs first week)")
    plt.xlabel("Week start"); plt.ylabel("Δ (percentage points)")
    plt.xticks(rotation=45, ha="right")
    plt.grid(True, axis="y", alpha=0.3)
    plt.legend(loc="best", frameon=True)
    out = os.path.join(CHART_DIR, f"weekly_cum_trend_{today}.png")
    plt.tight_layout(); plt.savefig(out, dpi=150); plt.close()
    return out

def main():
    today = datetime.today().strftime("%Y-%m-%d")
    chart_d1_bar(today)
    chart_daily_trend(today)
    chart_weekly_cum_trend(today)
    print("Charts generated for", today)

if __name__ == "__main__":
    main()
