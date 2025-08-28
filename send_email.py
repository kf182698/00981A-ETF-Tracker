# send_email.py — Gmail SMTP 主送 + SendGrid 備援（自動 fallback）
import os, re, json, glob, base64, subprocess, smtplib, ssl, mimetypes
from pathlib import Path
from email.message import EmailMessage
import pandas as pd

try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Attachment, Disposition, FileContent, FileName, FileType
    HAS_SENDGRID = True
except Exception:
    HAS_SENDGRID = False

REPORT_DIR = Path("reports")
CHART_DIR  = Path("charts")
DATA_DIR   = Path("data")
SNAP_DIR   = Path("data_snapshots")

def _normalize_date(raw: str) -> str:
    if raw and re.fullmatch(r"\\d{4}-\\d{2}-\\d{2}", raw.strip()):
        return raw.strip()
    if raw:
        m = re.search(r"(\\d{4})(\\d{2})(\\d{2})", raw)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    snaps = sorted(glob.glob(str(SNAP_DIR / "*.csv")))
    if snaps:
        return Path(snaps[-1]).stem
    js = sorted(glob.glob(str(DATA_DIR / "*.csv")))
    if not js:
        raise FileNotFoundError("無法解析 REPORT_DATE，且找不到任何 CSV")
    return Path(js[-1]).stem

def _ensure_built(date_str: str):
    sum_p = REPORT_DIR / f"summary_{date_str}.json"
    tbl_p = REPORT_DIR / f"holdings_change_table_{date_str}.csv"
    if sum_p.exists() and tbl_p.exists():
        return
    env = os.environ.copy()
    env["REPORT_DATE"] = date_str
    subprocess.check_call(["python","build_change_table.py"], env=env)
    if not (sum_p.exists() and tbl_p.exists()):
        raise FileNotFoundError(f"缺少報表：{sum_p} 或 {tbl_p}")

def _read_summary(date_str):
    with open(REPORT_DIR / f"summary_{date_str}.json","r",encoding="utf-8") as f:
        return json.load(f)

def _read_table(date_str):
    p = REPORT_DIR / f"holdings_change_table_{date_str}.csv"
    df = pd.read_csv(p, encoding="utf-8-sig")
    return df, p

def _fmt_pct(v):
    try: return f"{float(v):.2f}%"
    except: return "-"

def _fmt_int(v):
    try: return f"{int(v):,}"
    except: return "-"

def _fmt_price(v):
    try: return f"{float(v):.2f}"
    except: return "-"

def _render_html(summary, df, date_str):
    NEW_MIN = float(os.getenv("NEW_HOLDING_MIN_WEIGHT","0.4"))
    baseline = summary.get("baseline_date","(unknown)")
    top10 = summary.get("top10_sum",0.0)
    topw  = summary.get("top_weight",{})
    total = summary.get("total_count", len(df))

    new_list = []
    for r in summary.get("new_holdings", []):
        try:
            wt = float(r.get("今日權重%",0))
            if wt >= NEW_MIN:
                new_list.append(f"{r['股票代號']} {r['股票名稱']}: {_fmt_pct(wt)}")
        except Exception:
            continue

    imgs = []
    for name in (f"chart_d1_{date_str}.png", f"chart_daily_{date_str}.png", f"chart_weekly_{date_str}.png"):
        p = CHART_DIR / name
        if p.exists(): imgs.append(p)

    cols = list(df.columns)
    def _cell(val, align="right", style=""):
        return f'<td style="text-align:{align};padding:4px 6px;{style}">{val}</td>'
    def _th(val):
        return f'<th style="text-align:right;padding:6px;border-bottom:1px solid #ddd;">{val}</th>'
    def _df_to_html_manual(df: pd.DataFrame) -> str:
        html = ['<table style="border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:12px;width:100%;">']
        html.append("<thead><tr>")
        for c in cols: html.append(_th(c))
        html.append("</tr></thead><tbody>")
        for _, row in df.iterrows():
            tds = []
            for c in cols:
                v = row[c]
                if c.endswith("權重%") or c == "權重Δ%":
                    val = _fmt_pct(v)
                elif c.startswith("股數_") or c == "買賣超股數":
                    val = _fmt_int(v)
                elif c == "收盤價":
                    val = _fmt_price(v)
                else:
                    val = str(v)
                style = ""
                if c == "權重Δ%":
                    try:
                        f = float(v)
                        if f > 0: style = "color:#008800;font-weight:600;"
                        elif f < 0: style = "color:#cc0000;font-weight:600;"
                    except: pass
                align = "left" if c in ("股票代號","股票名稱") else "right"
                tds.append(_cell(val, align=align, style=style))
            html.append("<tr>" + "".join(tds) + "</tr>")
        html.append("</tbody></table>")
        return "".join(html)

    html = []
    html.append(f"<p>您好，</p>")
    html.append(f"<p><b>00981A 今日追蹤摘要（{date_str}）</b></p>")
    html.append(
        "<p>"
        f"▶ 今日總檔數：{total}　"
        f"▶ 前十大權重合計：{top10:.2f}%　"
        f"▶ 最大權重：{topw.get('code','')} {topw.get('name','')}（{float(topw.get('weight',0)):.2f}%）<br>"
        f"▶ 比較基期（昨）：{baseline}"
        "</p>"
    )
    if new_list:
        html.append(f"<p><b>🆕 首次新增持股（權重 &gt; {NEW_MIN:.2f}%）</b><br> &nbsp; - " + "<br> &nbsp; - ".join(new_list) + "</p>")
    for p in imgs:
        html.append(f'<p><i>附圖：</i> {p.name}</p>')
    html.append("<p><b>📊 每日持股變化追蹤表</b></p>")
    html.append(_df_to_html_manual(df))
    html.append('<p style="color:#666;font-size:12px">* Price may be carried from the last available trading day.</p>')
    return "".join(html), imgs

def _send_via_smtp(subject, html_body, attachments, from_addr, to_list, username, password):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_list)
    msg.set_content("HTML only")
    msg.add_alternative(html_body, subtype="html")

    for p in attachments:
        ctype, _ = mimetypes.guess_type(p)
        maintype, subtype = (ctype.split("/",1) if ctype else ("application","octet-stream"))
        with open(p, "rb") as f:
            msg.add_attachment(f.read(), maintype=maintype, subtype=subtype, filename=os.path.basename(p))

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context, timeout=30) as server:
        server.login(username, password)
        server.send_message(msg)
    return True

def _send_via_sendgrid(subject, html_body, attachments, from_addr, to_list, api_key):
    if not HAS_SENDGRID:
        raise RuntimeError("sendgrid 套件未安裝")
    mail = Mail(from_email=from_addr, to_emails=to_list, subject=subject, html_content=html_body)
    for p in attachments:
        with open(p, "rb") as f:
            data = base64.b64encode(f.read()).decode()
        ctype, _ = mimetypes.guess_type(p)
        mail.add_attachment(Attachment(
            file_content=FileContent(data),
            file_type=FileType(ctype or "application/octet-stream"),
            file_name=FileName(os.path.basename(p)),
            disposition=Disposition("attachment"),
        ))
    sg = SendGridAPIClient(api_key)
    resp = sg.send(mail)
    return 200 <= resp.status_code < 300

def main():
    raw = os.getenv("REPORT_DATE")
    date_str = _normalize_date(raw)
    _ensure_built(date_str)

    summary = _read_summary(date_str)
    df, table_path = _read_table(date_str)
    html, imgs = _render_html(summary, df, date_str)

    FR = os.getenv("EMAIL_USERNAME","no-reply@example.com")
    TO = [t.strip() for t in os.getenv("EMAIL_TO","").split(",") if t.strip()]
    if not TO:
        raise RuntimeError("缺少 EMAIL_TO")
    subject = f"00981A Daily Tracker — {date_str}"

    attachments = [str(table_path)] + [str(p) for p in imgs if p]

    SMTP_USER = os.getenv("EMAIL_USERNAME")
    SMTP_PASS = os.getenv("EMAIL_PASSWORD")
    sent = False
    if SMTP_USER and SMTP_PASS:
        try:
            print("[send_email] trying Gmail SMTP...")
            _send_via_smtp(subject, html, attachments, FR, TO, SMTP_USER, SMTP_PASS)
            sent = True
            print("[send_email] sent via Gmail SMTP")
        except Exception as e:
            print("[send_email] SMTP failed:", e)

    if not sent:
        SGK = os.getenv("SENDGRID_API_KEY")
        if not SGK:
            raise RuntimeError("SMTP 失敗且缺少 SENDGRID_API_KEY 作為備援")
        print("[send_email] trying SendGrid fallback...")
        ok = _send_via_sendgrid(subject, html, attachments, FR, TO, SGK)
        if not ok:
            raise RuntimeError("SendGrid 傳送失敗")
        print("[send_email] sent via SendGrid")

if __name__ == "__main__":
    main()
