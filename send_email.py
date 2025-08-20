# send_email.py — 以 SendGrid 寄出每日 ETF 追蹤 Email
# 流程：先輸出「中文文字摘要」，再內嵌圖表，最後內嵌「每日持股變化追蹤表」
#
# 需要的環境變數：
#   - EMAIL_USERNAME: 寄件者顯示名稱或 Email（from）
#   - EMAIL_TO: 收件者（可逗號分隔）
#   - SENDGRID_API_KEY: SendGrid API Key
#   - REPORT_DATE: (可選) 指定 YYYY-MM-DD；未設則抓 reports/ 最新
#   - ATTACH_FILES: (可選) "1"=同時附上 CSV 與圖檔（預設 "1"）
#
# 依賴的檔案（於專案根目錄）：
#   - reports/holdings_change_table_YYYY-MM-DD.csv
#   - reports/up_down_today_YYYY-MM-DD.csv
#   - reports/weights_chg_5d_YYYY-MM-DD.csv
#   - reports/new_gt_*.YYYY-MM-DD.csv（首次新增持股）
#   - reports/sell_alerts_YYYY-MM-DD.csv
#   - charts/*.png（若不存在會嘗試呼叫 charts.py 產生）
#
# 內嵌圖表（cid）：
#   - D1 Weight Change
#   - Daily Weight Trend (Top Movers x5)
#   - Weekly Cumulative Weight Change (vs first week)
#   - Top Unrealized P/L%

import os
import re
import glob
import base64
import mimetypes
from pathlib import Path
from datetime import datetime

import pandas as pd

# SendGrid
try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Email, To, Attachment, FileContent, FileName, FileType, Disposition, ContentId
except Exception:
    SendGridAPIClient = None

REPORT_DIR = "reports"
CHART_DIR  = "charts"

# ----------------- 小工具 -----------------
def latest_file(pattern: str):
    files = glob.glob(pattern)
    return max(files, key=os.path.getmtime) if files else None

def latest_report_date():
    files = glob.glob(os.path.join(REPORT_DIR, "holdings_change_table_*.csv"))
    if not files:
        return None
    latest = max(files, key=os.path.getmtime)
    m = re.search(r"holdings_change_table_(\d{4}-\d{2}-\d{2})\.csv$", latest)
    return m.group(1) if m else None

def get_report_date():
    d = os.environ.get("REPORT_DATE")
    return d if d else latest_report_date()

def b64read(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

def guess_mime(path: str):
    typ, enc = mimetypes.guess_type(path)
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
    """確保四張圖存在；若缺則嘗試執行 charts.py 產生"""
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

# ----------------- 讀檔與格式化 -----------------
def read_change_table(date_str: str) -> pd.DataFrame:
    path = os.path.join(REPORT_DIR, f"holdings_change_table_{date_str}.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"missing {path}")
    df = pd.read_csv(path)
    if "股票代號" in df.columns:
        df = df[~df["股票代號"].astype(str).str.startswith("BASE_")].copy()
    # 嘗試把可能帶%的數字欄位轉成數字
    for col in ["昨日權重%","今日權重%","Δ%","PL%","Close","AvgCost"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace("%","",regex=False), errors="coerce")
    return df

def read_updown(date_str: str) -> pd.DataFrame:
    path = os.path.join(REPORT_DIR, f"up_down_today_{date_str}.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    for col in ["昨日權重%","今日權重%","Δ%"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def read_d5(date_str: str) -> pd.DataFrame:
    path = os.path.join(REPORT_DIR, f"weights_chg_5d_{date_str}.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    for col in ["今日%","昨日%","D1Δ%","T-5日%","D5Δ%"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def read_new_list(date_str: str) -> pd.DataFrame:
    # new_gt_*_{date}.csv（門檻字樣不固定，找當天的第一個）
    pats = glob.glob(os.path.join(REPORT_DIR, f"new_gt_*_{date_str}.csv"))
    if not pats:
        return pd.DataFrame()
    df = pd.read_csv(pats[0])
    for col in ["今日權重%","Close"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def read_sell_alerts(date_str: str) -> pd.DataFrame:
    path = os.path.join(REPORT_DIR, f"sell_alerts_{date_str}.csv")
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    for col in ["昨日權重%","今日權重%","Δ%","Close"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def pct(x, digits=2, with_sign=False):
    if x is None or pd.isna(x):
        return "-"
    s = f"{float(x):.{digits}f}%"
    if with_sign and float(x) > 0:
        s = "+" + s
    return s

def fmt_change_pair(a, b, digits=2):
    # 形如：3.28% → 4.79% (+1.51%)
    if a is None or b is None or pd.isna(a) or pd.isna(b):
        return "-"
    delta = float(b) - float(a)
    return f"{float(a):.{digits}f}% → {float(b):.{digits}f}% ({'+' if delta>=0 else ''}{delta:.{digits}f}%)"

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

# ----------------- 文字摘要區塊 -----------------
def build_text_summary(date_str: str) -> str:
    df_main = read_change_table(date_str)
    df_updn = read_updown(date_str)
    df_d5   = read_d5(date_str)
    df_new  = read_new_list(date_str)
    df_sell = read_sell_alerts(date_str)

    # 今日檔數
    total_count = len(df_main) if not df_main.empty else 0

    # 前十大權重合計 & 最大權重
    top10_sum = "-"
    top_max_line = "-"
    if not df_main.empty and "今日權重%" in df_main.columns:
        df_sorted = df_main.sort_values("今日權重%", ascending=False)
        top10_sum = f"{df_sorted['今日權重%'].head(10).sum():.2f}%"
        top1 = df_sorted.iloc[0]
        top_max_line = f"{str(top1['股票代號'])} {str(top1['股票名稱'])}（{float(top1['今日權重%']):.2f}%）"

    # D1 上/下 Top 10
    d1_up_lines, d1_dn_lines = [], []
    if not df_updn.empty:
        df_updn = df_updn.copy()
        df_updn["abs"] = df_updn["Δ%"].abs()
        df_up = df_updn.sort_values("Δ%", ascending=False).head(10)
        df_dn = df_updn.sort_values("Δ%", ascending=True).head(10)
        for _, r in df_up.iterrows():
            d1_up_lines.append(f"  - {str(r['股票代號'])} {str(r['股票名稱'])}: {fmt_change_pair(r['昨日權重%'], r['今日權重%'])}")
        for _, r in df_dn.iterrows():
            d1_dn_lines.append(f"  - {str(r['股票代號'])} {str(r['股票名稱'])}: {fmt_change_pair(r['昨日權重%'], r['今日權重%'])}")

    # 首次新增（門檻從檔名解析）
    new_title = "🆕 首次新增持股"
    new_count = 0
    new_threshold = None
    if not df_new.empty:
        new_count = len(df_new)
        # 嘗試從檔名取門檻
        pattern = os.path.join(REPORT_DIR, f"new_gt_*_{date_str}.csv")
        fpath = latest_file(pattern)
        if fpath:
            m = re.search(r"new_gt_(\d+p?\d*)_", os.path.basename(fpath))
            if m:
                s = m.group(1).replace("p", ".")
                try:
                    new_threshold = float(s)
                except:
                    pass
    if new_threshold is not None:
        new_title = f"🆕 首次新增持股（權重 > {new_threshold:.2f}%）：{new_count} 檔"
    else:
        new_title = f"🆕 首次新增持股：{new_count} 檔"

    new_lines = []
    if not df_new.empty:
        df_new = df_new.sort_values("今日權重%", ascending=False)
        for _, r in df_new.iterrows():
            new_lines.append(f"  - {str(r['股票代號'])} {str(r['股票名稱'])}: {pct(r['今日權重%'])} ")

    # 賣出警示
    sell_title = "⚠️ 關鍵賣出警示（今日 ≤ 0.10% 且昨日 > 閾值）"
    sell_lines = []
    if not df_sell.empty:
        for _, r in df_sell.iterrows():
            sell_lines.append(f"  - {str(r['股票代號'])} {str(r['股票名稱'])}: {fmt_change_pair(r['昨日權重%'], r['今日權重%'])}")

    # D5 上/下 Top10
    d5_up_lines, d5_dn_lines = [], []
    if not df_d5.empty:
        df_d5 = df_d5.copy()
        df_up5 = df_d5.sort_values("D5Δ%", ascending=False).head(10)
        df_dn5 = df_d5.sort_values("D5Δ%", ascending=True).head(10)
        for _, r in df_up5.iterrows():
            d5_up_lines.append(f"  - {str(r['股票代號'])} {str(r['股票名稱'])}: {fmt_change_pair(r['T-5日%'], r['今日%'])}")
        for _, r in df_dn5.iterrows():
            d5_dn_lines.append(f"  - {str(r['股票代號'])} {str(r['股票名稱'])}: {fmt_change_pair(r['T-5日%'], r['今日%'])}")

    # 組 HTML（以 <pre> 保留你的原本文字風格對齊感）
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
    lines.append("⚠️ 關鍵賣出警示（今日 ≤ 0.10% 且昨日 > 閾值）")
    lines.extend(sell_lines if sell_lines else ["  - （無警示）"])
    lines.append("")
    lines.append("⏫ D5 權重上升 Top 10")
    lines.extend(d5_up_lines if d5_up_lines else ["  - （無資料）"])
    lines.append("⏬ D5 權重下降 Top 10")
    lines.extend(d5_dn_lines if d5_dn_lines else ["  - （無資料）"])
    lines.append("📊 每日持股變化追蹤表")

    # 包成 <pre>
    html_block = "<pre style='font-family:Menlo,Consolas,monospace;font-size:13px;white-space:pre-wrap;margin:0 0 12px 0'>" \
                 + "\n".join(lines) + "</pre>"
    return html_block

# ----------------- 主流程：組 HTML + 寄送 -----------------
def main():
    TO = os.environ.get("EMAIL_TO", "").strip()
    FR = os.environ.get("EMAIL_USERNAME", "").strip()
    SGK = os.environ.get("SENDGRID_API_KEY", "").strip()
    attach_flag = os.environ.get("ATTACH_FILES", "1") == "1"

    assert TO and FR and SGK, "請設定 EMAIL_TO / EMAIL_USERNAME / SENDGRID_API_KEY"

    date_str = get_report_date()
    assert date_str, "找不到 reports/ 內的報表日期（holdings_change_table_*）"
    print(f"[send_email] REPORT_DATE = {date_str}")

    # 1) 文字摘要區塊（中文）
    summary_html = build_text_summary(date_str)

    # 2) 準備圖表（若缺則 charts.py 產生）
    d1_png, trend_png, weekly_png, pl_png = ensure_charts(date_str)

    cid_d1 = "cid_d1_weight_change"
    cid_tr = "cid_daily_trend"
    cid_wk = "cid_weekly_cum"
    cid_pl = "cid_top_pl"

    # 3) 讀變化表 → 內嵌完整表格
    df_change = read_change_table(date_str)
    preferred_cols = [
        "股票代號","股票名稱",
        "股數_今日","今日權重%","股數_昨日","昨日權重%",
        "買賣超股數","Δ%","Close","AvgCost","PL%"
    ]
    cols = [c for c in preferred_cols if c in df_change.columns]
    df_show = df_change[cols].copy() if cols else df_change.copy()
    table_html = html_table(df_show, title="Holdings Change Table")

    # 4) HTML 組裝：先「文字摘要」，再「圖表」，最後「表格」
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

    if SendGridAPIClient is None:
        raise RuntimeError("sendgrid 套件未安裝")

    tos = [t.strip() for t in TO.split(",") if t.strip()]
    mail = Mail(
        from_email=Email(FR),
        to_emails=[To(x) for x in tos],
        subject=subject,
        html_content=html_body
    )

    # 內嵌圖片（inline）
    if os.path.exists(d1_png):    mail.add_attachment(make_inline_attachment(d1_png, cid_d1))
    if os.path.exists(trend_png): mail.add_attachment(make_inline_attachment(trend_png, cid_tr))
    if os.path.exists(weekly_png):mail.add_attachment(make_inline_attachment(weekly_png, cid_wk))
    if os.path.exists(pl_png):    mail.add_attachment(make_inline_attachment(pl_png, cid_pl))

    # 附檔
    if attach_flag:
        main_csv = os.path.join(REPORT_DIR, f"holdings_change_table_{date_str}.csv")
        aux = [
            os.path.join(REPORT_DIR, f"up_down_today_{date_str}.csv"),
            os.path.join(REPORT_DIR, f"weights_chg_5d_{date_str}.csv"),
            os.path.join(REPORT_DIR, f"sell_alerts_{date_str}.csv"),
        ]
        # new_gt_* 檔案（若有）
        new_csv = latest_file(os.path.join(REPORT_DIR, f"new_gt_*_{date_str}.csv"))
        for p in [main_csv] + aux + ([new_csv] if new_csv else []):
            if p and os.path.exists(p):
                mail.add_attachment(make_file_attachment(p))
        # 也可一併附上圖檔
        for p in [d1_png, trend_png, weekly_png, pl_png]:
            if os.path.exists(p):
                mail.add_attachment(make_file_attachment(p))

    # 送出
    sg = SendGridAPIClient(os.environ["SENDGRID_API_KEY"])
    resp = sg.send(mail)
    print(f"[send_email] SendGrid status={resp.status_code}")
    if resp.status_code >= 400:
        print(getattr(resp, "body", ""))
        raise SystemExit(f"SendGrid API error: {resp.status_code}")

if __name__ == "__main__":
    main()