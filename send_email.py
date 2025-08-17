# send_email.py — Send via SendGrid HTTP API (CID inline images + attachments)
import os
import glob
import base64
import mimetypes
from datetime import datetime

import pandas as pd
from email.utils import make_msgid

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Email, To, Content, Attachment,
    FileContent, FileName, FileType, Disposition, ContentId
)

from config import (
    TOP_N,
    THRESH_UPDOWN_EPS,
    NEW_WEIGHT_MIN,
    SELL_ALERT_THRESHOLD,
    REPORT_DIR,
    PCT_DECIMALS,
)

# ===== Secrets / Settings =====
TO   = os.environ.get("EMAIL_TO")
FR   = os.environ.get("EMAIL_USERNAME") or "no-reply@example.com"  # 顯示寄件人
SGK  = os.environ.get("SENDGRID_API_KEY")
assert TO and FR and SGK, "請設定 EMAIL_TO / EMAIL_USERNAME / SENDGRID_API_KEY"

# ===== Utilities =====
def latest_file(pattern: str):
    files = glob.glob(pattern)
    return max(files, key=os.path.getmtime) if files else None

def read_csv_safe(path: str):
    return pd.read_csv(path) if path and os.path.exists(path) else None

def fmt_pct(v):
    try:
        return f"{float(v):.{PCT_DECIMALS}f}%"
    except Exception:
        return "-"

def fmt_pair(y, t):
    return f"{fmt_pct(y)} → {fmt_pct(t)}"

def df_to_html_table(df: pd.DataFrame, max_rows: int = 30) -> str:
    if df is None or df.empty:
        return "<i>(No data for today)</i>"
    view = df.head(max_rows).copy()
    for col in view.columns:
        if view[col].dtype.kind in "if":
            if "權重" in col or "Weight" in col:
                view[col] = view[col].map(lambda x: fmt_pct(x))
            else:
                view[col] = view[col].map(lambda x: f"{int(x):,}" if pd.notna(x) else "")
    view = view.fillna("").astype(str)
    style = """
      style="
        border-collapse:collapse;
        width:100%;
        font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,'Noto Sans','PingFang TC','Microsoft JhengHei',sans-serif;
        font-size:13px;"
    """
    th_style = 'style="background:#f5f6f7;border:1px solid #ddd;padding:6px;text-align:center;"'
    td_style = 'style="border:1px solid #ddd;padding:6px;text-align:right;white-space:nowrap;"'
    td_left  = 'style="border:1px solid #ddd;padding:6px;text-align:left;white-space:nowrap;"'
    cols = list(view.columns)
    html = [f"<table {style}>", "<thead><tr>"]
    for c in cols: html.append(f"<th {th_style}>{c}</th>")
    html.append("</tr></thead><tbody>")
    for _, r in view.iterrows():
        html.append("<tr>")
        for c in cols:
            cell_style = td_left if ("股票代號" in c or "股票名稱" in c or "Code" in c or "Name" in c) else td_style
            html.append(f"<td {cell_style}>{r[c]}</td>")
        html.append("</tr>")
    html.append("</tbody></table>")
    if len(df) > max_rows:
        html.append(f'<div style="color:#666;font-size:12px;margin-top:4px;">(Showing first {max_rows} rows; full table in attachment)</div>')
    return "".join(html)

def b64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

# ===== Locate files =====
today = datetime.today().strftime("%Y-%m-%d")

data_path   = latest_file(f"data/{today}.csv") or latest_file("data/*.csv")
diff_path   = latest_file(f"diff/diff_{today}.csv") or latest_file("diff/*.csv")
updown_path = latest_file(f"{REPORT_DIR}/up_down_today_{today}.csv") or latest_file(f"{REPORT_DIR}/up_down_today_*.csv")
new_path    = latest_file(f"{REPORT_DIR}/new_gt_0p5_{today}.csv") or latest_file(f"{REPORT_DIR}/new_gt_*_{today}.csv") or latest_file(f"{REPORT_DIR}/new_gt_*_*.csv")
w5d_path    = latest_file(f"{REPORT_DIR}/weights_chg_5d_{today}.csv") or latest_file(f"{REPORT_DIR}/weights_chg_5d_*.csv")
sell_path   = latest_file(f"{REPORT_DIR}/sell_alerts_{today}.csv") or latest_file(f"{REPORT_DIR}/sell_alerts_*.csv")

change_csv  = latest_file(f"{REPORT_DIR}/holdings_change_table_{today}.csv") or latest_file(f"{REPORT_DIR}/holdings_change_table_*.csv")
change_xlsx = latest_file(f"{REPORT_DIR}/holdings_change_table_{today}.xlsx") or latest_file(f"{REPORT_DIR}/holdings_change_table_*.xlsx")

chart_d1    = latest_file(f"charts/d1_top_changes_{today}.png") or latest_file("charts/d1_top_changes_*.png")
chart_daily = latest_file(f"charts/daily_trend_{today}.png")   or latest_file("charts/daily_trend_*.png")
chart_week  = latest_file(f"charts/weekly_cum_trend_{today}.png") or latest_file("charts/weekly_cum_trend_*.png")

# ===== Read data =====
df_today  = read_csv_safe(data_path)
df_updn   = read_csv_safe(updown_path)
df_new    = read_csv_safe(new_path)
df_5d     = read_csv_safe(w5d_path)
df_sell   = read_csv_safe(sell_path)
df_change = read_csv_safe(change_csv)

# ===== Build summary =====
lines = []

def top_weights_summary(df_today: pd.DataFrame):
    df = df_today.copy()
    df.columns = [str(c).strip().replace("　","").replace("\u3000","") for c in df.columns]
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
    total_rows = len(df)
    top10_sum = df.sort_values("w", ascending=False).head(10)["w"].sum()
    if total_rows:
        r0 = df.sort_values("w", ascending=False).iloc[0]
        max_one = {"code": r0.get("股票代號","-"), "name": r0.get("股票名稱","-"), "w": float(r0["w"])}
    else:
        max_one = {"code":"-","name":"-","w":0.0}
    return total_rows, top10_sum, max_one

if df_today is not None:
    total_rows, top10_sum, max_one = top_weights_summary(df_today)
    lines += [
        f"▶ 今日總檔數：{total_rows}",
        f"▶ 前十大權重合計：{top10_sum:.2f}%",
        f"▶ 最大權重：{max_one['code']} {max_one['name']}（{max_one['w']:.2f}%）",
        ""
    ]
else:
    lines.append("（今日資料缺失）")

# D1 TopN
if df_updn is not None and not df_updn.empty:
    t = df_updn.copy()
    for c in ["Δ%","昨日權重%","今日權重%"]:
        t[c] = pd.to_numeric(t[c], errors="coerce").fillna(0.0)
    sig = t[abs(t["Δ%"]) >= THRESH_UPDOWN_EPS]
    up = sig.sort_values("Δ%", ascending=False).head(TOP_N)
    dn = sig.sort_values("Δ%", ascending=True).head(TOP_N)
    def to_lines(df_sel, title):
        out = [title]
        for _, r in df_sel.iterrows():
            out.append(f"  - {r['股票代號']} {r['股票名稱']}: {fmt_pair(r['昨日權重%'], r['今日權重%'])} ({r['Δ%']:+.{PCT_DECIMALS}f}%)")
        return out
    lines += to_lines(up, f"▲ D1 權重上升 Top {TOP_N}")
    lines += to_lines(dn, f"▼ D1 權重下降 Top {TOP_N}")
    lines.append("")
else:
    lines.append(f"（無 D1 報表或變動低於噪音門檻 {THRESH_UPDOWN_EPS:.2f}%）")
    lines.append("")

# New holdings > threshold — append close price in text
if df_new is not None and not df_new.empty:
    n = df_new.copy()
    if "今日權重%" in n.columns:
        n["今日權重%"] = pd.to_numeric(n["今日權重%"], errors="coerce").fillna(0.0)
        n = n.sort_values("今日權重%", ascending=False)

    px_map = {}
    if df_today is not None and "股票代號" in df_today.columns and "收盤價" in df_today.columns:
        for _, r in df_today.iterrows():
            px_map[str(r["股票代號"]).strip()] = r["收盤價"]

    lines.append(f"🆕 首次新增持股（權重 > {NEW_WEIGHT_MIN:.2f}%）：{len(n)} 檔")
    for _, r in n.iterrows():
        code = str(r.get('股票代號','')).strip()
        name = r.get('股票名稱','-')
        w = r.get('今日權重%')
        price = px_map.get(code)
        price_str = f"（收盤價：${price:.2f}）" if price is not None and pd.notna(price) else ""
        lines.append(f"  - {code} {name}: {fmt_pct(w)} {price_str}")
    lines.append("")
else:
    lines.append(f"🆕 首次新增持股（權重 > {NEW_WEIGHT_MIN:.2f}%）：0 檔")
    lines.append("")

# Sell alerts
if df_sell is not None and not df_sell.empty:
    s = df_sell.copy()
    for c in ["昨日權重%","今日權重%","Δ%"]:
        s[c] = pd.to_numeric(s[c], errors="coerce").fillna(0.0)
    lines.append(f"⚠️ 關鍵賣出警示（今日 ≤ {SELL_ALERT_THRESHOLD:.2f}% 且昨日 > 閾值）：{len(s)} 檔")
    s = s.sort_values("Δ%", ascending=True)
    for _, r in s.iterrows():
        lines.append(f"  - {r['股票代號']} {r['股票名稱']}: {r['昨日權重%']:.2f}% → {r['今日權重%']:.2f}%（{r['Δ%']:+.{PCT_DECIMALS}f}%）")
    lines.append("")
else:
    lines.append(f"⚠️ 關鍵賣出警示：0 檔（門檻 {SELL_ALERT_THRESHOLD:.2f}%）")
    lines.append("")

# D5 TopN
if df_5d is not None and not df_5d.empty:
    t5 = df_5d.copy()
    for c in ["今日%","昨日%","D1Δ%","T-5日%","D5Δ%"]:
        t5[c] = pd.to_numeric(t5[c], errors="coerce").fillna(0.0)
    up5 = t5.sort_values("D5Δ%", ascending=False).head(TOP_N)
    dn5 = t5.sort_values("D5Δ%", ascending=True).head(TOP_N)
    def to_lines5(df_sel, title):
        out = [title]
        for _, r in df_sel.iterrows():
            out.append(f"  - {r['股票代號']} {r['股票名稱']}: {fmt_pair(r['T-5日%'], r['今日%'])} ({r['D5Δ%']:+.{PCT_DECIMALS}f}%)")
        return out
    lines += to_lines5(up5, f"⏫ D5 權重上升 Top {TOP_N}")
    lines += to_lines5(dn5, f"⏬ D5 權重下降 Top {TOP_N}")
    lines.append("")
else:
    lines.append("（歷史不足 5 份快照，暫無 D5 報表）")
    lines.append("")

# ===== Build HTML (with inline images via CID) =====
subject = f"[ETF追蹤通知] 00981A 投資組合變動報告（{today}）"

text_body = (
    "您好，\n\n"
    f"00981A 今日追蹤摘要（{today}）\n" +
    "\n".join(lines) +
    "\n\n（若看不到圖片/表格，請查看附件）\n"
)

def cid_if_exists(path):
    return make_msgid(domain="charts.local")[1:-1] if path and os.path.exists(path) else None

cid_d1    = cid_if_exists(chart_d1)
cid_daily = cid_if_exists(chart_daily)
cid_week  = cid_if_exists(chart_week)

change_table_html = df_to_html_table(df_change, max_rows=30)
html_lines = "<br>".join(lines).replace("  - ", "&nbsp;&nbsp;- ")
html_body = f"""
<p>您好，</p>
<p>00981A 今日追蹤摘要（{today}）</p>
<pre style="font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 13px; white-space: pre-wrap;">{html_lines}</pre>

<h3 style="margin:12px 0 8px 0;">📊 每日持股變化追蹤表</h3>
{change_table_html}

<h3 style="margin:16px 0 8px 0;">D1 Weight Change</h3>
{f'<img src="cid:{cid_d1}" />' if cid_d1 else '<i>(No image)</i>'}

<h3 style="margin:16px 0 8px 0;">Daily Weight Trend (Top Movers x5)</h3>
{f'<img src="cid:{cid_daily}" />' if cid_daily else '<i>(No image)</i>'}

<h3 style="margin:16px 0 8px 0;">Weekly Cumulative Weight Change (vs first week)</h3>
{f'<img src="cid:{cid_week}" />' if cid_week else '<i>(No image)</i>'}

<p style="color:#666;">（若看不到圖片/表格，請查看附件 CSV / Excel / PNG 檔）</p>
"""

# ===== Build SendGrid Mail =====
mail = Mail(
    from_email=Email(FR),
    to_emails=[To(TO)],
    subject=subject,
    html_content=Content("text/html", html_body),
)
# 同步加入純文字備份（避免只 HTML）
mail.add_content(Content("text/plain", text_body))

# Inline images with CID
def attach_inline_image(path, cid):
    if not (path and os.path.exists(path) and cid): return
    ctype, _ = mimetypes.guess_type(path)
    if not ctype: ctype = "image/png"
    with open(path, "rb") as f:
        data = base64.b64encode(f.read()).decode()
    att = Attachment()
    att.file_content = FileContent(data)
    att.file_type    = FileType(ctype)
    att.file_name    = FileName(os.path.basename(path))
    att.disposition  = Disposition("inline")
    att.content_id   = ContentId(cid)  # <- cid for <img src="cid:...">
    mail.add_attachment(att)

attach_inline_image(chart_d1,    cid_d1)
attach_inline_image(chart_daily, cid_daily)
attach_inline_image(chart_week,  cid_week)

# Regular attachments (reports + images backup)
def attach_file(path):
    if not path or not os.path.exists(path): return
    ctype, _ = mimetypes.guess_type(path)
    if not ctype: ctype = "application/octet-stream"
    data = b64(path)
    att = Attachment()
    att.file_content = FileContent(data)
    att.file_type    = FileType(ctype)
    att.file_name    = FileName(os.path.basename(path))
    att.disposition  = Disposition("attachment")
    mail.add_attachment(att)

for p in [
    data_path, diff_path, updown_path, new_path, w5d_path, sell_path,
    change_csv, change_xlsx,
    chart_d1, chart_daily, chart_week
]:
    attach_file(p)

# ===== Send via SendGrid HTTP API (port 443) =====
from_email = os.environ.get("SENDGRID_FROM") or FR  # 優先用已驗證 Sender
# 重建 mail 物件以套用新的 from（或你也可以在上面建 Mail 時就用 from_email）
mail.from_email = Email(from_email)

# 可選：把回覆信設為你想收件的地址（例如原本的 EMAIL_USERNAME）
# from sendgrid.helpers.mail import ReplyTo
# mail.reply_to = ReplyTo(FR)

sg = SendGridAPIClient(SGK)
try:
    resp = sg.send(mail)
    print("SendGrid status:", resp.status_code)
    # 若非 202，印出 body 幫助除錯
    if resp.status_code != 202:
        try:
            print("SendGrid response body:", resp.body.decode() if hasattr(resp.body, "decode") else resp.body)
        except Exception:
            print("SendGrid response body (raw):", resp.body)
except Exception as e:
    # 印出更完整的錯誤，SendGrid 會回 JSON 說明
    body = getattr(e, "body", None)
    if body:
        try:
            print("SendGrid error body:", body.decode() if hasattr(body, "decode") else body)
        except Exception:
            print("SendGrid error body (raw):", body)
    raise
