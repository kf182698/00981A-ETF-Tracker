# charts.py — Font: Microsoft JhengHei; D1 labels use code only
import os, re, glob, json, subprocess
from pathlib import Path
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

# 字型（Runner 未安裝時會自動回退）
matplotlib.rcParams["font.sans-serif"] = ["Microsoft JhengHei", "Noto Sans CJK TC", "PingFang TC", "Heiti TC", "Arial", "DejaVu Sans", "sans-serif"]
matplotlib.rcParams["axes.unicode_minus"] = False  # 避免負號成方塊

REPORT_DIR = Path("reports")
DATA_DIR   = Path("data")
SNAP_DATA_DIR = Path("data_snapshots")
CHART_DIR  = Path("charts")
CHART_DIR.mkdir(parents=True, exist_ok=True)

def _normalize_date(raw: str) -> str:
    raw = raw.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw): return raw
    m = re.search(r"(\d{4})(\d{2})(\d{2})", raw)
    if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    raise ValueError(f"無法解析日期：{raw}")

def _latest_summary_date():
    js = sorted(glob.glob(str(REPORT_DIR / "summary_*.json")))
    if not js:
        snaps = sorted(glob.glob(str(SNAP_DATA_DIR / "*.csv")))
        if not snaps:
            raise FileNotFoundError("找不到任何 summary_*.json 或 data_snapshots/*.csv")
        date_str = Path(snaps[-1]).stem
        env = os.environ.copy(); env["REPORT_DATE"] = date_str
        subprocess.check_call(["python","build_change_table.py"], env=env)
        return date_str
    return Path(js[-1]).stem.split("_")[1]

def _ensure_summary(date_str: str):
    sp = REPORT_DIR / f"summary_{date_str}.json"
    if sp.exists(): return
    env = os.environ.copy(); env["REPORT_DATE"] = date_str
    subprocess.check_call(["python","build_change_table.py"], env=env)

def _load_summary(date_str):
    with open(REPORT_DIR / f"summary_{date_str}.json","r",encoding="utf-8") as f:
        return json.load(f)

def fig_d1(date_str, summary, topn=10):
    df_up = pd.DataFrame(summary.get("d1_up",[]))
    df_dn = pd.DataFrame(summary.get("d1_dn",[]))
    # ✅ 只用股票代號作為標籤
    for df in (df_up, df_dn):
        if not df.empty:
            df["label"] = df["股票代號"].astype(str)
    fig, ax = plt.subplots(figsize=(8,4.6), dpi=150)
    if not df_dn.empty:
        ax.barh(df_dn.head(topn)["label"], df_dn.head(topn)["權重Δ%"], label="Down")
    if not df_up.empty:
        ax.barh(df_up.head(topn)["label"], df_up.head(topn)["權重Δ%"], label="Up")
    ax.set_title("D1 權重變化（Top Movers）")
    ax.set_xlabel("Δ%（百分點）"); ax.set_ylabel("股票代號")
    ax.legend()
    plt.tight_layout()
    out = CHART_DIR / f"chart_d1_{date_str}.png"
    fig.savefig(out); plt.close(fig)
    return str(out)

def fig_daily_trend(date_str, summary, k=5):
    dates = summary.get("last5_dates",[])
    if not dates: return None
    pool = pd.DataFrame(summary.get("d1_up",[]) + summary.get("d1_dn",[]))
    if pool.empty: return None
    pool["abs"] = pool["權重Δ%"].abs()
    pool = pool.sort_values("abs", ascending=False).head(k)

    series = {}
    for d in dates:
        for base in (SNAP_DATA_DIR, DATA_DIR):
            fp = base / f"{d}.csv"
            if fp.exists():
                df = pd.read_csv(fp, encoding="utf-8-sig")
                if "股票代號" in df.columns and "持股權重" in df.columns:
                    series[d] = df.set_index("股票代號")["持股權重"]
                    break
    if len(series) < 2: return None

    fig, ax = plt.subplots(figsize=(8,4.6), dpi=150)
    for _, row in pool.iterrows():
        code = str(row["股票代號"])
        y = [float(series.get(d, pd.Series()).get(code, 0.0)) for d in dates]
        y = pd.Series(y).rolling(2, min_periods=1).mean().tolist()
        ax.plot(dates, y, marker="o", label=code)
    ax.set_title("近5日權重走勢（Top Movers）")
    ax.set_xlabel("日期"); ax.set_ylabel("權重%"); ax.legend()
    plt.tight_layout()
    out = CHART_DIR / f"chart_daily_{date_str}.png"
    fig.savefig(out); plt.close(fig)
    return str(out)

def fig_weekly_cum(date_str, summary):
    dates = summary.get("last5_dates",[])
    if not dates: return None
    base = SNAP_DATA_DIR if (SNAP_DATA_DIR / f"{dates[-1]}.csv").exists() else DATA_DIR
    last_fp = base / f"{dates[-1]}.csv"
    if not last_fp.exists(): return None
    last_df = pd.read_csv(last_fp, encoding="utf-8-sig")
    top_codes = last_df.sort_values("持股權重", ascending=False).head(5)["股票代號"].astype(str).tolist()

    series = {}
    first = dates[0]
    for d in dates:
        fp = base / f"{d}.csv"
        if not fp.exists(): continue
        df = pd.read_csv(fp, encoding="utf-8-sig")
        series[d] = df.set_index("股票代號")["持股權重"] if "股票代號" in df.columns else pd.Series(dtype=float)

    fig, ax = plt.subplots(figsize=(8,4.6), dpi=150)
    for code in top_codes:
        y = [float(series.get(d, pd.Series()).get(code, 0.0) - series.get(first, pd.Series()).get(code, 0.0)) for d in dates]
        ax.plot(dates, y, marker="o", label=code)
    ax.set_title("週累計權重變化（相對首日）")
    ax.set_xlabel("日期"); ax.set_ylabel("Δ%"); ax.legend()
    plt.tight_layout()
    out = CHART_DIR / f"chart_weekly_{date_str}.png"
    fig.savefig(out); plt.close(fig)
    return str(out)

def main():
    raw = os.getenv("REPORT_DATE")
    date_str = _normalize_date(raw) if raw else _latest_summary_date()
    _ensure_summary(date_str)
    summary = _load_summary(date_str)
    p1 = fig_d1(date_str, summary)
    p2 = fig_daily_trend(date_str, summary)
    p3 = fig_weekly_cum(date_str, summary)
    print("[charts] saved:", p1, p2, p3)

if __name__ == "__main__":
    main()
