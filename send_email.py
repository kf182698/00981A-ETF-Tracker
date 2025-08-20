# send_email.py — 以 SendGrid 寄出每日 ETF 追蹤 Email（內嵌圖表 + 變化表）
# 需求：
#   環境變數：
#     - EMAIL_USERNAME: 寄件者顯示名稱或 Email
#     - EMAIL_TO: 收件者（可用逗號分隔多位）
#     - SENDGRID_API_KEY: SendGrid API Key
#     - REPORT_DATE: (可選) 指定 YYYY-MM-DD；未設則抓 reports/ 最新
#     - ATTACH_FILES: (可選) "1" 代表同時附上 CSV 與圖檔；預設 "1"
#
# 依賴檔案：
#   - reports/holdings_change_table_YYYY-MM-DD.csv
#   - reports/up_down_today_YYYY-MM-DD.csv
#   - charts/ 由 charts.py 產生的四張圖
#
# 送信管道：
#   - SendGrid REST API：支援 inline attachments（Content-ID）嵌入 <img src="cid:...">

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
    SendGridAPIClient = None  # 之後檢查再報錯

REPORT_DIR = "reports"
CHART_DIR  = "charts"

# ============== 工具 ==============

def latest_file(pattern: str):
    files = glob.glob(pattern)
    return max(files, key=os.path.getmtime) if files else None

def latest_report_date():
    # 從 holdings_change_table_YYYY-MM-DD.csv 找最新日期
    files = glob.glob(os.path.join(REPORT_DIR, "holdings_change_table_*.csv"))
    if not files:
        return None
    latest = max(files, key=os.path.getmtime)
    m = re.search(r"holdings_change_table_(\d{4}-\d{2}-\d{2})\.csv$", latest)
    return m.group(1) if m else None

def get_report_date():
    d = os.environ.get("REPORT_DATE")
    if d:
        return d
    return latest_report_date()

def read_change_table(date_str: str) -> pd.DataFrame:
    path = os.path.join(REPORT_DIR, f"holdings_change_table_{date_str}.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"missing {path}")
    df = pd.read_csv(path)
    # 去掉 meta 開頭的 BASE_ 列（若有）
    if "股票代號" in df.columns:
        df = df[~df["股票代號"].astype(str).str.startswith("BASE_")].copy()
    return df

def df_to_html_table(df: pd.DataFrame, title: str = "") -> str:
    # 轉成簡潔 HTML 表格，避免內嵌 CSS 過重
    # 單純加上表頭加粗、偶數行底色
    cols = df.columns.tolist()
    thead = "".join([f"<th>{c}</th>" for c in cols])
    rows = []
    for i, (_, r) in enumerate(df.iterrows()):
        tds = "".join([f"<td>{'' if pd.isna(v) else v}</td>" for v in r.tolist()])
        bg = "#fafafa" if i % 2 else "white"
        rows.append(f'<tr style="background:{bg}">{tds}</tr>')
    table_html = f"""
      <div style="font-size:14px;margin:12px 0">
        <div style="font-weight:600;margin-bottom:6px">{title}</div>
        <table style="width:100%;border-collapse:collapse;font-family:Arial,Helvetica,sans-serif" border="1" cellpadding="6">
          <thead style="background:#efefef">{thead}</thead>
          <tbody>
            {''.join(rows)}
          </tbody>
        </table>
      </div>
    """
    return table_html

def b64read(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

def guess_mime(path: str):
    typ, enc = mimetypes.guess_type(path)
    if not typ:
        typ = "application/octet-stream"
    return typ

def ensure_charts(date_str: str):
    """
    若 charts/ 的四張圖不存在，嘗試呼叫 charts.py 產生。
    """
    needed = [
        os.path.join(CHART_DIR, f"d1_weight_change_{date_str}.png"),
        os.path.join(CHART_DIR, f"daily_trend_top5_{date_str}.png"),
        os.path.join(CHART_DIR, f"weekly_cum_change_{date_str}.png"),
        os.path.join(CHART_DIR, f"top_unrealized_pl_{date_str}.png"),
    ]
    missing = [p for p in needed if not os.path.exists(p)]
    if not missing:
        return needed
    # 嘗試呼叫 charts.py
    try:
        import subprocess, sys
        env = os.environ.copy()
        env["REPORT_DATE"] = date_str
        subprocess.run([sys.executable, "charts.py"], check=True, env=env)
    except Exception as e:
        print("[send_email] WARN: charts.py run failed:", e)
    # 再檢查一次
    return needed

def make_inline_attachment(path: str, cid: str) -> Attachment:
    typ = guess_mime(path)
    content = b64read(path)
    att = Attachment()
    att.file_content = FileContent(content)
    att.file_type = FileType(typ)
    att.file_name = FileName(os.path.basename(path))
    att.disposition = Disposition("inline")
    att.content_id = ContentId(cid)
    return att

def make_file_attachment(path: str) -> Attachment:
    typ = guess_mime(path)
    content = b64read(path)
    att = Attachment()
    att.file_content = FileContent(content)
    att.file_type = FileType(typ)
    att.file_name = FileName(os.path.basename(path))
    att.disposition = Disposition("attachment")
    return att

# ============== 主流程：產 HTML + 寄信 ==============

def main():
    TO = os.environ.get("EMAIL_TO", "").strip()
    FR = os.environ.get("EMAIL_USERNAME", "").strip()
    SGK = os.environ.get("SENDGRID_API_KEY", "").strip()
    attach_flag = os.environ.get("ATTACH_FILES", "1") == "1"

    assert TO and FR and SGK, "請設定 EMAIL_TO / EMAIL_USERNAME / SENDGRID_API_KEY"

    date_str = get_report_date()
    assert date_str, "找不到 reports/ 內的報表日期（holdings_change_table_*）"
    print(f"[send_email] REPORT_DATE = {date_str}")

    # 讀變化表，產 HTML 表格
    df_change = read_change_table(date_str)

    # 建議欄位順序（如果都有的話）
    preferred_cols = [
        "股票代號","股票名稱",
        "股數_今日","今日權重%","股數_昨日","昨日權重%",
        "買賣超股數","Δ%","Close","AvgCost","PL%"
    ]
    cols = [c for c in preferred_cols if c in df_change.columns]
    if cols:
        df_show = df_change[cols].copy()
    else:
        df_show = df_change.copy()

    # 生成圖（若不存在）
    chart_paths = ensure_charts(date_str)
    d1_png, trend_png, weekly_png, pl_png = chart_paths

    # 準備 cid 與 inline attachments
    inline_map = []
    cid_d1 = "cid_d1_weight_change"
    cid_tr = "cid_daily_trend"
    cid_wk = "cid_weekly_cum"
    cid_pl = "cid_top_pl"

    for p in [d1_png, trend_png, weekly_png, pl_png]:
        if not os.path.exists(p):
            print(f"[send_email] WARN: chart missing: {p}")

    # HTML 內文
    subject = f"00981A Daily Tracker — {date_str}"
    html_parts = []

    html_parts.append(f"""
    <div style="font-family:Arial,Helvetica,sans-serif; color:#333;">
      <h2 style="margin:0 0 12px 0;">00981A Daily Tracker — {date_str}</h2>
      <div style="font-size:13px;opacity:0.8;margin-bottom:8px;">
        This email includes D1 weight change, trends, and the daily holdings change table.
      </div>
      <hr style="border:none;border-top:1px solid #e5e5e5;margin:12px 0;">
    </div>
    """)

    # 四張圖（inline）
    def img_block(title, cid):
        return f"""
        <div style="margin:14px 0;">
          <div style="font-weight:600;margin-bottom:6px;">{title}</div>
          <img src="cid:{cid}" alt="{title}" style="max-width:100%;border:1px solid #ddd;border-radius:6px;">
        </div>
        """

    html_parts.append(img_block("D1 Weight Change", cid_d1))
    html_parts.append(img_block("Daily Weight Trend (Top Movers x5)", cid_tr))
    html_parts.append(img_block("Weekly Cumulative Weight Change (vs first week)", cid_wk))
    html_parts.append(img_block("Top Unrealized P/L%", cid_pl))

    # 變化表（整表嵌入）
    html_parts.append(df_to_html_table(df_show, title="Holdings Change Table"))

    html_body = "\n".join(html_parts)

    # === 準備 SendGrid Mail ===
    if SendGridAPIClient is None:
        raise RuntimeError("sendgrid 套件未安裝")

    tos = [t.strip() for t in TO.split(",") if t.strip()]
    mail = Mail(
        from_email=Email(FR),
        to_emails=[To(x) for x in tos],
        subject=subject,
        html_content=html_body
    )

    # inline 圖片
    if os.path.exists(d1_png):
        mail.add_attachment(make_inline_attachment(d1_png, cid_d1))
    if os.path.exists(trend_png):
        mail.add_attachment(make_inline_attachment(trend_png, cid_tr))
    if os.path.exists(weekly_png):
        mail.add_attachment(make_inline_attachment(weekly_png, cid_wk))
    if os.path.exists(pl_png):
        mail.add_attachment(make_inline_attachment(pl_png, cid_pl))

    # 主要 CSV / 圖檔作為附件（可關閉）
    if attach_flag:
        main_csv = os.path.join(REPORT_DIR, f"holdings_change_table_{date_str}.csv")
        aux_csvs = [
            os.path.join(REPORT_DIR, f"up_down_today_{date_str}.csv"),
            os.path.join(REPORT_DIR, f"weights_chg_5d_{date_str}.csv"),
            os.path.join(REPORT_DIR, f"new_gt_0p5_{date_str}.csv"),  # 可能不存在，忽略錯誤
            os.path.join(REPORT_DIR, f"sell_alerts_{date_str}.csv"),
        ]
        for p in [main_csv] + aux_csvs:
            if os.path.exists(p):
                mail.add_attachment(make_file_attachment(p))

        # 可再附上圖檔（非必須）
        for p in [d1_png, trend_png, weekly_png, pl_png]:
            if os.path.exists(p):
                mail.add_attachment(make_file_attachment(p))

    # 標記 inline cids（SendGrid 需要在 HTML 使用 cid，附件需 disposition=inline + content_id）
    # 上面 Attachment 已設好 Content-Id 與 disposition，此處不需要額外處理。

    # 送出
    sg = SendGridAPIClient(SGK)
    try:
        resp = sg.send(mail)
        print(f"[send_email] SendGrid status={resp.status_code}")
        if resp.status_code >= 400:
            print(getattr(resp, "body", ""))
            raise SystemExit(f"SendGrid API error: {resp.status_code}")
    except Exception as e:
        raise SystemExit(f"SendGrid send failed: {e}")

if __name__ == "__main__":
    main()