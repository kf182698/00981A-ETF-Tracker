# send_email.py — 以 SendGrid 寄出每日 ETF 追蹤 Email
# 重點：摘要與表格的資料來源「完全一致」
#   → 優先讀 reports/holdings_change_table_YYYY-MM-DD.xlsx
#   → 若無 xlsx 才讀 .csv（會過濾 meta 的 BASE_ 列）
#
# 信件內容順序：
#   1) 中文文字摘要（你原本的格式）
#   2) 內嵌圖：D1 Weight Change / Daily Trend (Top Movers x5) /
#               Weekly Cumulative Weight Change (vs first week) / Top Unrealized P/L%
#   3) 內嵌「每日持股變化追蹤表」(HTML)
#
# 需要環境變數：
#   - EMAIL_USERNAME       寄件者顯示名稱或 Email（from）
#   - EMAIL_TO             收件者（可逗號分隔）
#   - SENDGRID_API_KEY     SendGrid API Key
#   - REPORT_DATE          (可選) 指定 YYYY-MM-DD；未設則抓 reports/ 最新
#   - ATTACH_FILES         (可選) "1"=附上 CSV/XLSX 與圖檔，預設 "1"
#
# 依賴檔案：
#   - reports/holdings_change_table_YYYY-MM-DD.xlsx  ← 首選
#   - reports/holdings_change_table_YYYY-MM-DD.csv   ← 備援（會移除 BASE_ meta 列）
#   - reports/up_down_today_YYYY-MM-DD.csv           ← D1 圖所需（若缺會照表格自行算）
#   - reports/weights_chg_5d_YYYY-MM-DD.csv          ← D5 文字與圖
#   - reports/new_gt_*_YYYY-MM-DD.csv                ← 讀取門檻；若無就用 DEFAULT_NEW_TH=0.5
#   - reports/sell_alerts_YYYY-MM-DD.csv             ←（可有可無；文字仍以表格重新計算）
#   - charts/*.png（若缺會呼叫 charts.py 產生）
#
# 備註：
#   - 文字摘要「首次新增持股」與「關鍵賣出警示」統一依表格欄位計算：
#       首次新增：昨日權重%==0 且 今日權重% >= 門檻（預設 0.5% 或 new_gt_* 檔名推回）
#       賣出警示：今日 ≤ 0.10% 且 昨日 > 0.10% 且 Δ<0（可依需要調整）
#   - 所有百分比欄位會自動去除 % 字元再轉數值，確保排序與計算正確。

import os
import re
import glob
import base64
import mimetypes
from pathlib import Path

import pandas as pd

# SendGrid
try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import (
        Mail, Email, To, Attachment,
        FileContent, FileName, FileType, Disposition, ContentId
    )
except Exception:
    SendGridAPIClient = None

REPORT_DIR = "reports"
CHART_DIR  = "charts"

# 預設門檻（若 new_gt_* 檔名不可得）
DEFAULT_NEW_TH = 0.5     # 首次新增權重門檻（%）
SELL_ALERT_TH  = 0.10    # 賣出警示權重門檻（%）
PCT_DIGITS     = 2

# --------------- 小工具 ---------------

def latest_file(pattern: str):
    files = glob.glob(pattern)
    return max(files, key=os.path.getmtime) if files else None

def latest_report_date():
    files = glob.glob(os.path.join(REPORT_DIR, "holdings_change_table_*.csv")) + \
            glob.glob(os.path.join(REPORT_DIR, "holdings_change_table_*.xlsx"))
    if not files:
        return None
    latest = max(files, key=os.path.getmtime)
    m = re.search(r"holdings_change_table_(\d{4}-\d{2}-\d{2})\.(csv|xlsx)$", latest)
    return m.group(1) if m else None

def get_report_date():
    d = os.environ.get("REPORT_DATE")
    return d if d else latest_report_date()

def b64read(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

def guess_mime(path: str):
    typ, _ = mimetypes.guess_type(path)
    return typ or "application/octet-stream"

def make_inline_attachment(path: str, cid: str) -> Attachment:
    att = Attachment()
    att.file_content = FileContent(b64read(path))
    att.file_type = FileType(guess_mime(path))
    att.file_name = FileName(os.path.basename(path))
    att.disposition = Disposition("inline")
    att.content_id = ContentId(cid)
    return att

def make_file_attachment(path: str) -> Attachment:
    att = Attachment()
    att.file_content = FileContent(b64read(path))
    att.file_type = FileType(guess_mime(path))
    att.file_name = FileName(os.path.basename(path))
    att.disposition = Disposition("attachment")
    return att

def ensure_charts(date_str: str):
    """確保四張圖存在；若缺則嘗試執行 charts.py 產生。"""
    need = [
        os.path.join(CHART_DIR, f"d1_weight_change_{date_str}.png"),
        os.path.join(CHART_DIR, f"daily_trend_top5_{date_str}.png"),
        os.path.join(CHART_DIR, f"weekly_cum_change_{date_str}.png"),
        os.path.join(CHART_DIR, f"top_unrealized_pl_{date_str}.png"),
    ]
    missing = [p for p in need if not os.path.exists(p)]
    if missing:
        try:
            import subprocess, sys
            env = os.environ.copy()
            env["REPORT_DATE"] = date_str
            subprocess.run([sys.executable, "charts.py"], check=True, env=env)
        except Exception as e:
            print("[send_email] WARN: charts.py run failed:", e)
    return need

# --------------- 資料讀取（單一真相：變化表） ---------------

def _to_numeric_pct(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace("%", "", regex=False), errors="coerce")

def read_change_table(date_str: str) -> pd.DataFrame:
    """優先讀 XLSX；沒有再讀 CSV（CSV 會過濾 BASE_ meta 列）"""
    xlsx = os.path.join(REPORT_DIR, f"holdings_change_table_{date_str}.xlsx")
    csv  = os.path.join(REPORT_DIR, f"holdings_change_table_{date_str}.csv")

    if os.path.exists(xlsx):
        df = pd.read_excel(xlsx, sheet_name="ChangeTable")
    elif os.path.exists(csv):
        df = pd.read_csv(csv)
        # 移除最前面 BASE_ meta 列
        if "股票代號" in df.columns:
            df = df[~df["股票代號"].astype(str).str.startswith("BASE_")].copy()
    else:
        raise FileNotFoundError(f"missing change table for {date_str}")

    # 轉數值欄
    for col in ["昨日權重%","今日權重%","Δ%","PL%","Close","AvgCost"]:
        if col in df.columns:
            df[col] = _to_numeric_pct(df[col])

    # 資料型別統一
    if "股票代號" in df.columns:
        df["股票代號"] = df["股票代號"].astype(str).str.strip()
    if "股票名稱" in df.columns:
        df["股票名稱"] = df["股票名稱"].astype(str).str.strip()

    return df

def read_updown_for_chart(date_str: str, df_change: pd.DataFrame) -> pd.DataFrame:
    """D1 圖優先讀 up_down_today；若缺，直接由變化表算 Δ% 回傳相同欄位."""
    path = os.path.join(REPORT_DIR, f"up_down_today_{date_str}.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
        for c in ["昨日權重%","今日權重%","Δ%"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    # fallback：由 change_table 生成
    need = ["股票代號","股票名稱","昨日權重%","今日權重%","Δ%"]
    if not all(c in df_change.columns for c in need):
        return pd.DataFrame()
    return df_change[need].copy()

def read_d5(date_str: str) -> pd.DataFrame:
    path = os.path.join(REPORT_DIR, f"weights_chg_5d_{date_str}.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    for col in ["今日%","昨日%","D1Δ%","T-5日%","D5Δ%"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "股票代號" in df.columns:
        df["股票代號"] = df["股票代號"].astype(str)
    if "股票名稱" in df.columns:
        df["股票名稱"] = df["股票名稱"].astype(str)
    return df

def detect_new_threshold(date_str: str) -> float:
    pats = glob.glob(os.path.join(REPORT_DIR, f"new_gt_*_{date_str}.csv"))
    if not pats:
        return DEFAULT_NEW_TH
    # 檔名範例：new_gt_0p5_2025-08-19.csv
    m = re.search(r"new_gt_(\d+p?\d*)_", os.path.basename(pats[0]))
    if not m:
        return DEFAULT_NEW_TH
    s = m.group(1).replace("p", ".")
    try:
        return float(s)
    except:
        return DEFAULT_NEW_TH

# --------------- HTML 組件 ---------------

def pct(x, digits=PCT_DIGITS, sign=False):
    if x is None or pd.isna(x):
        return "-"
    v = float(x)
    s = f"{v:.{digits}f}%"
    if sign and v > 0:
        s = "+" + s
    return s

def fmt_pair(a, b, digits=PCT_DIGITS):
    if pd.isna(a) or pd.isna(b):
        return "-"
    d = float(b) - float(a)
    return f"{float(a):.{digits}f}% → {float(b):.{digits}f}% ({'+' if d>=0 else ''}{d:.{digits}f}%)"

def html_table(df: pd.DataFrame, title: str = "") -> str:
    cols = df.columns.tolist()
    thead = "".join([f"<th>{c}</th>" for c in cols])
    rows = []
    for i, (_, r) in enumerate(df.iterrows()):
        tds = "".join([f"<td>{'' if pd.isna(v) else v}</td>" for v in r.tolist()])
        bg = "#fafafa" if i % 2 else "white"
        rows.append(f'<tr style="background:{bg}">{tds}</tr>')
    return f"""
      <div style="font-size:14px;margin:12px 0">
        <div style="font-weight:600;margin-bottom:6px">{title}</div>
        <table style="width:100%;border-collapse:collapse;font-family:Arial,Helvetica,sans-serif" border="1" cellpadding="6">
          <thead style="background:#efefef">{thead}</thead>
          <tbody>{''.join(rows)}</tbody>
        </table>
      </div>
    """

# --------------- 文字摘要（完全以變化表計算） ---------------

def build_text_summary(date_str: str, df_change: pd.DataFrame) -> str:
    # 今日檔數
    total_count = len(df_change)

    # 前十大權重合計、最大權重
    top10_sum = "-"
    top_max_line = "-"
    if "今日權重%" in df_change.columns and total_count > 0:
        df_sorted = df_change.sort_values("今日權重%", ascending=False)
        top10_sum = f"{df_sorted['今日權重%'].head(10).sum():.2f}%"
        top1 = df_sorted.iloc[0]
        top_max_line = f"{str(top1['股票代號'])} {str(top1['股票名稱'])}（{float(top1['今日權重%']):.2f}%）"

    # D1 Top 10（升/降）
    d1_up_lines, d1_dn_lines = [], []
    if all(c in df_change.columns for c in ["昨日權重%","今日權重%","Δ%"]):
        up10 = df_change.sort_values("Δ%", ascending=False).head(10)
        dn10 = df_change.sort_values("Δ%", ascending=True).head(10)
        for _, r in up10.iterrows():
            d1_up_lines.append(f"  - {str(r['股票代號'])} {str(r['股票名稱'])}: {fmt_pair(r['昨日權重%'], r['今日權重%'])}")
        for _, r in dn10.iterrows():
            d1_dn_lines.append(f"  - {str(r['股票代號'])} {str(r['股票名稱'])}: {fmt_pair(r['昨日權重%'], r['今日權重%'])}")

    # 首次新增（依檔名門檻，否則預設 0.5%）
    NEW_TH = detect_new_threshold(date_str)
    new_mask = (df_change["昨日權重%"].fillna(0) == 0) & (df_change["今日權重%"].fillna(0) >= NEW_TH)
    df_new = df_change.loc[new_mask].sort_values("今日權重%", ascending=False)
    new_title = f"🆕 首次新增持股（權重 > {NEW_TH:.2f}%）：{len(df_new)} 檔"
    new_lines = [f"  - {c} {n}: {pct(w)} " for c, n, w in df_new[["股票代號","股票名稱","今日權重%"]].values]

    # 賣出警示（統一用變化表計算）
    sell_mask = (df_change["今日權重%"].fillna(0) <= SELL_ALERT_TH) & \
                (df_change["昨日權重%"].fillna(0) > SELL_ALERT_TH) & \
                (df_change["Δ%"].fillna(0) < 0)
    df_sell = df_change.loc[sell_mask].sort_values("Δ%")
    sell_title = f"⚠️ 關鍵賣出警示（今日 ≤ {SELL_ALERT_TH:.2f}% 且昨日 > 閾值）"
    sell_lines = [f"  - {c} {n}: {fmt_pair(y, t)}"
                  for c, n, y, t in df_sell[["股票代號","股票名稱","昨日權重%","今日權重%"]].values]

    # D5 上/下（若有 weights_chg_5d）
    df_d5 = read_d5(date_str)
    d5_up_lines, d5_dn_lines = [], []
    if not df_d5.empty and "D5Δ%" in df_d5.columns:
        up5 = df_d5.sort_values("D5Δ%", ascending=False).head(10)
        dn5 = df_d5.sort_values("D5Δ%", ascending=True).head(10)
        for _, r in up5.iterrows():
            d5_up_lines.append(f"  - {str(r['股票代號'])} {str(r['股票名稱'])}: {fmt_pair(r['T-5日%'], r['今日%'])}")
        for _, r in dn5.iterrows():
            d5_dn_lines.append(f"  - {str(r['股票代號'])} {str(r['股票名稱'])}: {fmt_pair(r['T-5日%'], r['今日%'])}")

    # 組你的文字格式（<pre> 保留排版）
    lines = []
    lines.append("您好，")
    lines.append("")
    lines.append(f"00981A 今日追蹤摘要（{date_str}）")
    lines.append("")
    lines.append(f"▶ 今日總檔數：{total_count}")
    lines.append(f"▶ 前十大權重合計：{top10_sum}")
    lines.append(f"▶ 最大權重：{top_max_line}")
    lines.append("")
    lines.append("▲ D1 權重上升 Top 10")
    lines.extend(d1_up_lines if d1_up_lines else ["  - （無資料）"])
    lines.append("▼ D1 權重下降 Top 10")
    lines.extend(d1_dn_lines if d1_dn_lines else ["  - （無資料）"])
    lines.append("")
    lines.append(new_title)
    lines.extend(new_lines if new_lines else ["  - （無符合）"])
    lines.append("")
    lines.append(sell_title)
    lines.extend(sell_lines if sell_lines else ["  - （無警示）"])
    lines.append("")
    lines.append("⏫ D5 權重上升 Top 10")
    lines.extend(d5_up_lines if d5_up_lines else ["  - （無資料）"])
    lines.append("⏬ D5 權重下降 Top 10")
    lines.extend(d5_dn_lines if d5_dn_lines else ["  - （無資料）"])
    lines.append("📊 每日持股變化追蹤表")

    return "<pre style='font-family:Menlo,Consolas,monospace;font-size:13px;white-space:pre-wrap;margin:0 0 12px 0'>" \
           + "\n".join(lines) + "</pre>"

# --------------- 主流程 ---------------

def main():
    TO = os.environ.get("EMAIL_TO", "").strip()
    FR = os.environ.get("EMAIL_USERNAME", "").strip()
    SGK = os.environ.get("SENDGRID_API_KEY", "").strip()
    attach_flag = os.environ.get("ATTACH_FILES", "1") == "1"

    assert TO and FR and SGK, "請設定 EMAIL_TO / EMAIL_USERNAME / SENDGRID_API_KEY"

    date_str = get_report_date()
    assert date_str, "找不到 reports/ 內的報表日期（holdings_change_table_*）"
    print(f"[send_email] REPORT_DATE = {date_str}")

    # 單一真相：讀變化表（優先 XLSX）
    df_change = read_change_table(date_str)

    # 1) 文字摘要（完全以 df_change 計算）
    summary_html = build_text_summary(date_str, df_change)

    # 2) 圖表（若缺會自動產）
    d1_png, trend_png, weekly_png, pl_png = ensure_charts(date_str)
    cid_d1 = "cid_d1_weight_change"
    cid_tr = "cid_daily_trend"
    cid_wk = "cid_weekly_cum"
    cid_pl = "cid_top_pl"

    # 3) 表格（直接把 df_change 內嵌）
    preferred_cols = [
        "股票代號","股票名稱",
        "股數_今日","今日權重%","股數_昨日","昨日權重%",
        "買賣超股數","Δ%","Close","AvgCost","PL%"
    ]
    cols = [c for c in preferred_cols if c in df_change.columns]
    df_show = df_change[cols].copy() if cols else df_change.copy()

    # 百分比欄位恢復顯示 %
    for col in ["昨日權重%","今日權重%","Δ%","PL%"]:
        if col in df_show.columns:
            df_show[col] = df_show[col].map(lambda v: f"{v:.2f}%" if pd.notna(v) else "-")

    table_html = html_table(df_show, title="Holdings Change Table")

    # 4) HTML 組裝：先摘要、再圖、後表
    def img_block(title, cid):
        return f"""
        <div style="margin:14px 0;">
          <div style="font-weight:600;margin-bottom:6px;">{title}</div>
          <img src="cid:{cid}" alt="{title}" style="max-width:100%;border:1px solid #ddd;border-radius:6px;">
        </div>
        """
    graphs_html = "\n".join([
        img_block("D1 Weight Change", cid_d1),
        img_block("Daily Weight Trend (Top Movers x5)", cid_tr),
        img_block("Weekly Cumulative Weight Change (vs first week)", cid_wk),
        img_block("Top Unrealized P/L%", cid_pl),
    ])

    subject = f"00981A Daily Tracker — {date_str}"
    header_html = f"""
    <div style="font-family:Arial,Helvetica,sans-serif;color:#333;">
      <h2 style="margin:0 0 12px 0;">00981A Daily Tracker — {date_str}</h2>
      <hr style="border:none;border-top:1px solid #e5e5e5;margin:12px 0;">
    </div>
    """
    html_body = header_html + summary_html + graphs_html + table_html

    # ---- SendGrid ----
    if SendGridAPIClient is None:
        raise RuntimeError("sendgrid 套件未安裝")

    tos = [t.strip() for t in TO.split(",") if t.strip()]
    mail = Mail(
        from_email=Email(FR),
        to_emails=[To(x) for x in tos],
        subject=subject,
        html_content=html_body
    )

    # 內嵌圖片
    if os.path.exists(d1_png):    mail.add_attachment(make_inline_attachment(d1_png, cid_d1))
    if os.path.exists(trend_png): mail.add_attachment(make_inline_attachment(trend_png, cid_tr))
    if os.path.exists(weekly_png):mail.add_attachment(make_inline_attachment(weekly_png, cid_wk))
    if os.path.exists(pl_png):    mail.add_attachment(make_inline_attachment(pl_png, cid_pl))

    # 附檔（XLSX / CSV、其他輔助 CSV、圖檔）
    if attach_flag:
        xlsx = os.path.join(REPORT_DIR, f"holdings_change_table_{date_str}.xlsx")
        csv  = os.path.join(REPORT_DIR, f"holdings_change_table_{date_str}.csv")
        for p in [xlsx, csv]:
            if os.path.exists(p):
                mail.add_attachment(make_file_attachment(p))

        others = [
            os.path.join(REPORT_DIR, f"up_down_today_{date_str}.csv"),
            os.path.join(REPORT_DIR, f"weights_chg_5d_{date_str}.csv"),
            os.path.join(REPORT_DIR, f"sell_alerts_{date_str}.csv"),
        ]
        new_csv = latest_file(os.path.join(REPORT_DIR, f"new_gt_*_{date_str}.csv"))
        for p in others + ([new_csv] if new_csv else []):
            if p and os.path.exists(p):
                mail.add_attachment(make_file_attachment(p))

        for p in [d1_png, trend_png, weekly_png, pl_png]:
            if os.path.exists(p):
                mail.add_attachment(make_file_attachment(p))

    sg = SendGridAPIClient(os.environ["SENDGRID_API_KEY"])
    resp = sg.send(mail)
    print(f"[send_email] SendGrid status={resp.status_code}")
    if resp.status_code >= 400:
        print(getattr(resp, "body", ""))
        raise SystemExit(f"SendGrid API error: {resp.status_code}")

if __name__ == "__main__":
    main()