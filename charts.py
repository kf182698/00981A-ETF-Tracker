# charts.py — JhengHei font; fix code dtype; ffill; D1 sorted by |Δ|
import os, re, glob, json, subprocess
from pathlib import Path
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

# 字型：微軟正黑體（Runner 無時自動回退）
matplotlib.rcParams["font.sans-serif"] = ["Microsoft JhengHei","Noto Sans CJK TC","PingFang TC","Heiti TC","Arial","DejaVu Sans","sans-serif"]
matplotlib.rcParams["axes.unicode_minus"] = False

REPORT_DIR = Path("reports")
DATA_DIR   = Path("data")
SNAP_DIR   = Path("data_snapshots")
CHART_DIR  = Path("charts")
CHART_DIR.mkdir(parents=True, exist_ok=True)

def _normalize_date(raw: str) -> str:
    raw = (raw or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw): return raw
    m = re.search(r"(\d{4})(\d{2})(\d{2})", raw)
    if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    js = sorted(glob.glob(str(REPORT_DIR / "summary_*.json")))
    if not js: raise FileNotFoundError("找不到可用日期")
    return Path(js[-1]).stem.split("_")[1]

def _ensure_summary(date_str: str):
    sp = REPORT_DIR / f"summary_{date_str}.json"
    if sp.exists(): return
    env = os.environ.copy(); env["REPORT_DATE"] = date_str
    subprocess.check_call(["python","build_change_table.py"], env=env)

def _load_summary(date_str: str):
    with open(REPORT_DIR / f"summary_{date_str}.json","r",encoding="utf-8") as f:
        return json.load(f)

def _load_weight_series(date_str: str):
    for base in (SNAP_DIR, DATA_DIR):
        fp = base / f"{date_str}.csv"
        if fp.exists():
            df = pd.read_csv(fp, encoding="utf-8-sig")
            if "股票代號" not in df.columns or "持股權重" not in df.columns:
                return pd.Series(dtype=float)
            df["股票代號"] = df["股票代號"].astype(str).str.strip()
            df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)
            return df.set_index("股票代號")["持股權重"]
    return pd.Series(dtype=float)

def fig_d1(date_str, summary, topn=10):
    up = pd.DataFrame(summary.get("d1_up", []))
    dn = pd.DataFrame(summary.get("d1_dn", []))
    pool = pd.concat([up, dn], ignore_index=True)
    if pool.empty: return None
    pool["label"] = pool["股票代號"].astype(str)
    pool["delta"] = pd.to_numeric(pool["權重Δ%"], errors="coerce").fillna(0.0)
    pool["abs"]   = pool["delta"].abs()
    sel = (pool.sort_values("abs", ascending=False)
                .head(topn)
                .sort_values("delta"))                 # 先負後正
    colors = ["#1f77b4" if x < 0 else "#ff7f0e" for x in sel["delta"]]
    fig, ax = plt.subplots(figsize=(8, 4.6), dpi=150)
    ax.barh(sel["label"], sel["delta"], color=colors)
    ax.axvline(0, linewidth=0.8, color="gray")
    ax.set_title("D1 權重變化（Top Movers）")
    ax.set_xlabel("Δ%（百分點）"); ax.set_ylabel("股票代號")
    ax.invert_yaxis()
    plt.tight_layout()
    out = CHART_DIR / f"chart_d1_{date_str}.png"
    fig.savefig(out); plt.close(fig)
    return str(out)

def fig_daily_trend(date_str, summary, k=5):
    dates = summary.get("last5_dates", [])
    if not dates: return None
    pool = pd.DataFrame(summary.get("d1_up", []) + summary.get("d1_dn", []))
    if pool.empty: return None
    pool["abs"] = pd.to_numeric(pool["權重Δ%"], errors="coerce").abs()
    codes = pool.sort_values("abs", ascending=False).head(k)["股票代號"].astype(str).tolist()

    series_by_day = {d: _load_weight_series(d) for d in dates}

    fig, ax = plt.subplots(figsize=(8, 4.6), dpi=150)
    for code in codes:
        y, last = [], None
        for d in dates:
            val = series_by_day.get(d, pd.Series(dtype=float)).get(code)
            if pd.isna(val): val = last
            if pd.isna(val): val = 0.0
            y.append(float(val)); last = val
        ax.plot(dates, y, marker="o", label=str(code))
    ax.set_title("近5日權重走勢（Top Movers）")
    ax.set_xlabel("日期"); ax.set_ylabel("權重%"); ax.legend()
    plt.tight_layout()
    out = CHART_DIR / f"chart_daily_{date_str}.png"
    fig.savefig(out); plt.close(fig)
    return str(out)

def fig_weekly_cum(date_str, summary):
    dates = summary.get("last5_dates", [])
    if not dates: return None
    last_series = _load_weight_series(dates[-1])
    top_codes = last_series.sort_values(ascending=False).head(5).index.astype(str).tolist()
    if not top_codes: return None
    series_by_day = {d: _load_weight_series(d) for d in dates}
    base = series_by_day.get(dates[0], pd.Series(dtype=float))
    fig, ax = plt.subplots(figsize=(8, 4.6), dpi=150)
    for code in top_codes:
        y, prev = [], None
        base_v = base.get(code, 0.0)
        for d in dates:
            val = series_by_day.get(d, pd.Series(dtype=float)).get(code)
            if pd.isna(val): val = prev
            if pd.isna(val): val = 0.0
            y.append(float(val - base_v)); prev = val
        ax.plot(dates, y, marker="o", label=str(code))
    ax.set_title("週累計權重變化（相對首日）")
    ax.set_xlabel("日期"); ax.set_ylabel("Δ%"); ax.legend()
    plt.tight_layout()
    out = CHART_DIR / f"chart_weekly_{date_str}.png"
    fig.savefig(out); plt.close(fig)
    return str(out)

def main():
    raw = os.getenv("REPORT_DATE")
    date_str = _normalize_date(raw)
    _ensure_summary(date_str)
    summary = _load_summary(date_str)
    p1 = fig_d1(date_str, summary)
    p2 = fig_daily_trend(date_str, summary)
    p3 = fig_weekly_cum(date_str, summary)
    print("[charts] saved:", p1, p2, p3)

if __name__ == "__main__":
    main()
