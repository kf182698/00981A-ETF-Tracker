# charts.py — 產出 ETF 追蹤圖表：
# 1) D1 Weight Change（英文、含 Up/Down 圖例）
# 2) Daily Weight Trend (Top Movers x5)（平滑線）
# 3) Weekly Cumulative Weight Change (vs first week)（平滑線）
# 4) Top Unrealized P/L%（水平條）
#
# 依賴：
# - reports/up_down_today_YYYY-MM-DD.csv
# - reports/holdings_change_table_YYYY-MM-DD.csv（取 Top Movers 與 PL%）
# - data/YYYY-MM-DD.csv（逐日權重時間序列）
#
# 可用環境變數：
# - REPORT_DATE=YYYY-MM-DD 指定日期（預設抓 reports/ 最新）
# - TOP_N：D1/PL% 的 Top N（預設 10）
# - TREND_DAYS：Daily Trend 近幾天（預設 12）
# - WEEKS：Weekly 累積變化取近幾週（預設 8）
# - SMOOTH_WIN：平滑視窗（預設 3）

import os
import re
import glob
from pathlib import Path
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

# ---- 參數 ----
REPORT_DIR = "reports"
DATA_DIR = "data"
CHART_DIR = "charts"

TOP_N = int(os.environ.get("TOP_N", 10))          # D1 / PL% Top N
TREND_DAYS = int(os.environ.get("TREND_DAYS", 12)) # Daily Trend 回看天數
WEEKS = int(os.environ.get("WEEKS", 8))            # Weekly 回看週數
SMOOTH_WIN = int(os.environ.get("SMOOTH_WIN", 3))  # 曲線平滑的移動視窗

Path(CHART_DIR).mkdir(parents=True, exist_ok=True)

# 在大多數 Runner 環境可套用的英文字型（避免中文）
plt.rcParams["font.family"] = ["DejaVu Sans", "Arial", "Liberation Sans"]
plt.rcParams["axes.unicode_minus"] = False  # 負號正確顯示

# ---- 共用：找最新日期 / 讀檔 ----
def _latest_file(pattern: str):
    files = glob.glob(pattern)
    return max(files, key=os.path.getmtime) if files else None

def _latest_report_date():
    # 從 holdings_change_table_YYYY-MM-DD.csv 抓最新日期
    files = glob.glob(os.path.join(REPORT_DIR, "holdings_change_table_*.csv"))
    if not files:
        return None
    latest = max(files, key=os.path.getmtime)
    m = re.search(r"holdings_change_table_(\d{4}-\d{2}-\d{2})\.csv$", latest)
    return m.group(1) if m else None

def _get_report_date():
    d = os.environ.get("REPORT_DATE")
    if d:
        return d
    return _latest_report_date()

def _read_updown(date_str):
    """讀 reports/up_down_today_YYYY-MM-DD.csv"""
    path = os.path.join(REPORT_DIR, f"up_down_today_{date_str}.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    # 欄：股票代號/股票名稱/昨日權重%/今日權重%/Δ%
    for col in ["昨日權重%","今日權重%","Δ%"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    # 去掉 meta（理論上這張不會有）
    return df

def _read_change_table(date_str):
    """讀 reports/holdings_change_table_YYYY-MM-DD.csv，移除 meta 列"""
    path = os.path.join(REPORT_DIR, f"holdings_change_table_{date_str}.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    if "股票代號" in df.columns:
        df = df[~df["股票代號"].astype(str).str.startswith("BASE_")].copy()
    # 嘗試把數值欄轉成數字
    for col in ["昨日權重%","今日權重%","Δ%","PL%","Close","AvgCost"]:
        if col in df.columns:
            # 可能帶 % 符號
            df[col] = pd.to_numeric(df[col].astype(str).str.replace("%", "", regex=False), errors="coerce")
    return df

def _list_data_dates():
    dates=[]
    for f in sorted(glob.glob(os.path.join(DATA_DIR, "*.csv"))):
        m = re.search(r"(\d{4}-\d{2}-\d{2})\.csv$", f)
        if m: dates.append(m.group(1))
    return sorted(dates)

def _read_day_weights(date_str):
    """回傳 {code: weight}；讀 data/YYYY-MM-DD.csv 的持股權重"""
    path = os.path.join(DATA_DIR, f"{date_str}.csv")
    if not os.path.exists(path):
        return {}
    df = pd.read_csv(path)
    # 容錯欄名
    def pick(df, cands):
        for c in cands:
            if c in df.columns: return c
        return None
    c_code = pick(df, ["股票代號","代號","證券代號","Code"])
    c_weight = pick(df, ["持股權重","權重","占比","占比(%)","比重(%)","Weight"])
    if not c_code or not c_weight:
        return {}
    s = pd.to_numeric(df[c_weight], errors="coerce").fillna(0.0).values
    code = df[c_code].astype(str).str.strip().values
    return {code[i]: float(s[i]) for i in range(len(code))}

# ---- 平滑工具：簡單移動平均 ----
def _smooth(series, win=SMOOTH_WIN):
    if win <= 1 or len(series) <= 2:
        return series
    s = pd.Series(series, dtype="float64")
    return s.rolling(window=win, min_periods=1, center=True).mean().tolist()

# ---- 圖 1：D1 Weight Change ----
def chart_d1_weight_change(date_str):
    df = _read_updown(date_str)
    if df is None or df.empty:
        return None

    # 取 Top N by |Δ%|
    df["abs"] = df["Δ%"].abs()
    top = df.sort_values("abs", ascending=False).head(TOP_N).copy()
    top = top.sort_values("Δ%")  # 從負到正，畫面更直觀

    labels = top["股票代號"].astype(str).tolist()
    values = top["Δ%"].tolist()
    colors = ["red" if v < 0 else "green" for v in values]

    plt.figure(figsize=(10,6))
    y = range(len(labels))
    plt.barh(y, values, color=colors)
    plt.yticks(y, labels)
    plt.axvline(0, linewidth=1)
    plt.title("D1 Weight Change")
    plt.xlabel("Change (pp)")  # 百分點
    plt.ylabel("Ticker")

    # 圖例（Up/Down）
    from matplotlib.patches import Patch
    legend_elems = [Patch(facecolor="green", label="Up"), Patch(facecolor="red", label="Down")]
    plt.legend(handles=legend_elems, loc="lower right")

    out = os.path.join(CHART_DIR, f"d1_weight_change_{date_str}.png")
    plt.tight_layout()
    plt.savefig(out, dpi=150)
    plt.close()
    print("[charts] saved:", out)
    return out

# ---- 圖 2：Daily Weight Trend (Top Movers x5) ----
def chart_daily_trend_top5(date_str):
    # 從 change table 抓當日 Top Movers（依 |Δ%|），取前 5 檔
    ct = _read_change_table(date_str)
    if ct is None or ct.empty or "Δ%" not in ct.columns:
        return None
    ct["abs"] = ct["Δ%"].abs()
    movers = ct.sort_values("abs", ascending=False).head(5)["股票代號"].astype(str).tolist()
    if not movers:
        return None

    # 取近 TREND_DAYS 個日期（含今天）
    dates = _list_data_dates()
    if date_str not in dates:
        return None
    idx = dates.index(date_str)
    start = max(0, idx - (TREND_DAYS - 1))
    window = dates[start: idx+1]

    # 組時間序列矩陣：index=日期, columns=代碼
    panel = {}
    for d in window:
        panel[d] = _read_day_weights(d)
    if not panel:
        return None
    mat = pd.DataFrame(panel).T  # rows=day, cols=code
    mat = mat[movers]  # 只留 top5
    mat = mat.fillna(method="ffill").fillna(0.0)

    # 繪圖：平滑
    plt.figure(figsize=(11,6))
    for code in movers:
        if code not in mat.columns:
            continue
        y = mat[code].tolist()
        y_smooth = _smooth(y, SMOOTH_WIN)
        plt.plot(range(len(window)), y_smooth, label=code, linewidth=2)

    plt.title("Daily Weight Trend (Top Movers x5)")
    plt.xlabel("Day")
    plt.ylabel("Weight (%)")
    plt.xticks(range(len(window)), [w[5:] for w in window], rotation=0)  # show MM-DD
    plt.legend(loc="upper left", ncol=2)
    plt.grid(alpha=0.2)

    out = os.path.join(CHART_DIR, f"daily_trend_top5_{date_str}.png")
    plt.tight_layout()
    plt.savefig(out, dpi=150)
    plt.close()
    print("[charts] saved:", out)
    return out

# ---- 圖 3：Weekly Cumulative Weight Change (vs first week) ----
def chart_weekly_cum_change(date_str):
    # 基底仍用上面 daily window，但聚合到週層級（ISO 週）
    dates = _list_data_dates()
    if not dates or date_str not in dates:
        return None
    idx = dates.index(date_str)
    # 取最近 WEEKS 週的「日期窗」：抓約 7*WEEKS 天的原始日資料
    start = max(0, idx - (7 * WEEKS - 1))
    window = dates[start: idx+1]
    if not window:
        return None

    # 先用 change table 的 Top Movers（若沒有就 fallback 用今日權重最高前 5）
    ct = _read_change_table(date_str)
    if ct is not None and not ct.empty and "Δ%" in ct.columns:
        ct["abs"] = ct["Δ%"].abs()
        movers = ct.sort_values("abs", ascending=False).head(5)["股票代號"].astype(str).tolist()
    else:
        # fallback：今日權重最高前 5
        ct2 = _read_change_table(date_str)
        if ct2 is None or ct2.empty or "今日權重%" not in ct2.columns:
            return None
        movers = ct2.sort_values("今日權重%", ascending=False).head(5)["股票代號"].astype(str).tolist()

    # 將 window 的每日資料讀進來
    records = []
    for d in window:
        wmap = _read_day_weights(d)
        for code in movers:
            records.append({"date": d, "code": code, "weight": float(wmap.get(code, 0.0))})
    if not records:
        return None

    df = pd.DataFrame(records)
    # 轉成週：用 ISO 週（year, week）
    dt = pd.to_datetime(df["date"])
    df["iso_year"] = dt.dt.isocalendar().year.astype(int)
    df["iso_week"] = dt.dt.isocalendar().week.astype(int)
    # 每週取「該週最後一日」的權重（或平均也可以；這裡採最後一日更貼近週收）
    df = df.sort_values(["code", "iso_year", "iso_week", "date"])
    last_of_week = df.groupby(["code","iso_year","iso_week"], as_index=False).last()

    # 對每個 code，把第一週的權重當基準，畫「本週 - 第一週」
    plt.figure(figsize=(11,6))
    for code, sub in last_of_week.groupby("code"):
        sub = sub.sort_values(["iso_year","iso_week"])
        base = sub["weight"].iloc[0]
        delta = (sub["weight"] - base).tolist()
        delta_smooth = _smooth(delta, SMOOTH_WIN)
        x_labels = [f"{int(y)}-W{int(w):02d}" for y, w in zip(sub["iso_year"], sub["iso_week"])]
        plt.plot(range(len(delta_smooth)), delta_smooth, label=code, linewidth=2)

    plt.title("Weekly Cumulative Weight Change (vs first week)")
    plt.xlabel("Week")
    plt.ylabel("Δ Weight vs Week1 (pp)")
    # x 軸標籤：每隔一個標示，避免擁擠
    if 'sub' in locals():
        ticks = range(len(sub))
        plt.xticks(ticks, x_labels, rotation=0)
    plt.legend(loc="upper left", ncol=2)
    plt.grid(alpha=0.2)

    out = os.path.join(CHART_DIR, f"weekly_cum_change_{date_str}.png")
    plt.tight_layout()
    plt.savefig(out, dpi=150)
    plt.close()
    print("[charts] saved:", out)
    return out

# ---- 圖 4：Top Unrealized P/L% ----
def chart_top_unrealized_pl(date_str):
    path = os.path.join(REPORT_DIR, f"holdings_change_table_{date_str}.csv")
    alt  = _latest_file(os.path.join(REPORT_DIR, "holdings_change_table_*.csv"))
    if not os.path.exists(path):
        path = alt
    if not path:
        return None

    df = pd.read_csv(path)
    if "股票代號" in df.columns:
        df = df[~df["股票代號"].astype(str).str.startswith("BASE_")].copy()
    if "PL%" not in df.columns:
        return None
    df["PL%"] = pd.to_numeric(df["PL%"].astype(str).str.replace("%","",regex=False), errors="coerce")

    df = df.dropna(subset=["PL%"])
    if df.empty:
        return None

    top_g = df.sort_values("PL%", ascending=False).head(TOP_N)
    top_l = df.sort_values("PL%", ascending=True).head(TOP_N)
    top = pd.concat([top_l[::-1], top_g])  # 先放最差，再放最好

    labels = top["股票代號"].astype(str).tolist()
    vals = top["PL%"].tolist()
    colors = ["red" if v < 0 else "green" for v in vals]

    plt.figure(figsize=(10,6))
    y = range(len(labels))
    plt.barh(y, vals, color=colors)
    plt.yticks(y, labels)
    plt.axvline(0, linewidth=1)
    plt.title(f"Top Unrealized P/L% (±{TOP_N})")
    plt.xlabel("Unrealized P/L (%)")
    plt.ylabel("Ticker")
    out = os.path.join(CHART_DIR, f"top_unrealized_pl_{date_str}.png")
    plt.tight_layout()
    plt.savefig(out, dpi=150)
    plt.close()
    print("[charts] saved:", out)
    return out

# ---- 主程式：一次產四張 ----
def main():
    date_str = _get_report_date()
    if not date_str:
        print("[charts] No report date found in reports/."); return

    print(f"[charts] REPORT_DATE = {date_str}")
    p1 = chart_d1_weight_change(date_str)
    p2 = chart_daily_trend_top5(date_str)
    p3 = chart_weekly_cum_change(date_str)
    p4 = chart_top_unrealized_pl(date_str)
    print("[charts] done:", p1, p2, p3, p4)

if __name__ == "__main__":
    main()