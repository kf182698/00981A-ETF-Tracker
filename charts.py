# charts.py — 以 reports/change_table_{REPORT_DATE}.csv 繪圖
import os
from pathlib import Path
import glob
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["Microsoft JhengHei", "PingFang TC", "Noto Sans CJK TC", "Arial"]
plt.rcParams["axes.unicode_minus"] = False

def get_report_date() -> str:
    p = Path("manifest/effective_date.txt")
    if p.exists():
        d = p.read_text(encoding="utf-8").strip()
        if d:
            return d
    d = (os.getenv("REPORT_DATE") or "").strip()
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d

def find_prev_snapshot(report_date: str) -> str:
    snaps = sorted(glob.glob("data_snapshots/*.csv"))
    prev = ""
    for p in reversed(snaps):
        name = Path(p).stem
        if name < report_date:
            prev = name
            break
    return prev

def save(fig, out):
    Path("charts").mkdir(exist_ok=True)
    fig.savefig(out, bbox_inches="tight", dpi=150)
    plt.close(fig)

def main():
    date = get_report_date()
    if not date:
        raise SystemExit("REPORT_DATE 未設定")

    change_csv = Path("reports")/f"change_table_{date}.csv"
    if not change_csv.exists():
        raise SystemExit(f"缺少 {change_csv}")

    df = pd.read_csv(change_csv, encoding="utf-8-sig")
    prev_date = find_prev_snapshot(date) or "N/A"

    # D1 Top Movers（依 Δ% 絕對值排序，僅代號作 y 標籤）
    d1 = df.copy()
    d1["absΔ"] = d1["權重Δ%"].abs()
    d1 = d1.sort_values("absΔ", ascending=False).head(20)
    codes = d1["股票代號"].astype(str).tolist()
    vals  = d1["權重Δ%"].tolist()

    fig, ax = plt.subplots(figsize=(10,6))
    y = range(len(codes))
    ax.barh(y, vals)
    ax.set_yticks(y, labels=codes)
    ax.set_xlabel("Δ% 權重變化")
    ax.set_title(f"D1 權重變化 Top Movers（{date} vs {prev_date}）")
    save(fig, f"charts/chart_d1_{date}.png")

    # Daily cum trend（僅示意：以「權重Δ%」累加）
    fig, ax = plt.subplots(figsize=(10,6))
    s = df["權重Δ%"].sort_values()
    ax.plot(s.values.cumsum(), marker="o", linewidth=2)
    ax.set_title(f"每日累積權重變化（{date}）")
    ax.set_xlabel("排序後持股")
    ax.set_ylabel("累積 Δ%")
    save(fig, f"charts/chart_daily_{date}.png")

    # Weekly（簡化：近 5 日 Δ% 加總；若你有 weekly 資料也可替換）
    fig, ax = plt.subplots(figsize=(10,6))
    ax.plot([0,1,2,3,4], [0,0,0,0,0], marker="o", linewidth=2)  # 佔位
    ax.set_title(f"近5日權重變化（示意，{date}）")
    ax.set_xlabel("日")
    ax.set_ylabel("Δ%")
    save(fig, f"charts/chart_weekly_{date}.png")

if __name__ == "__main__":
    main()