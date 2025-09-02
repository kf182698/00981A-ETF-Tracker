# send_email.py — Gmail SMTP 主送 + SendGrid 備援；字型：微軟正黑體；反垃圾郵件友善落款
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
    if raw and re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw.strip()):
        return raw.strip()
    if raw:
        m = re.search(r"(\d{4})(\d{2})(\d{2})", raw)
        if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    snaps = sorted(glob.glob(str(SNAP_DIR / "*.csv")))
    if snaps: return Path(snaps[-1]).stem
    js = sorted(glob.glob(str(DATA_DIR / "*.csv")))
    if not js: raise FileNotFoundError("無法解析 REPORT_DATE，且找不到任何 CSV")
    return Path(js[-1]).stem

def _ensure_built(date_str: str):
    sum_p = REPORT_DIR / f"summary_{date_str}.json"
    tbl_p = REPORT_DIR / f"holdings_change_table_{date_str}.csv"
    if sum_p.exists() and tbl_p.exists(): return
    env = os.environ.copy(); env["REPORT_DATE"] = date_str
    subprocess.check_call(["python","build_change_table.py"], env=env)

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
    NEW_MIN  = float(os.getenv("NEW_HOLDING_MIN_WEIGHT","0.4"))
    baseline = summary.get("baseline_date","(unknown)")
    top10    = summary.get("top10_sum",0.0)
    topw     = summary.get("top_weight",{})
    total    = summary.get("total_count", len(df))

    # 首次買進 / 關鍵賣出（已由 summary 完成，不受 TopN/NOISE 影響）
    new_list  = summary.get("new_holdings", [])
    sell_list = summary.get("sell_alerts", [])

    # 內嵌 CSS：字型微軟正黑體
    CSS = """
    body { font-family: 'Microsoft JhengHei','Noto Sans CJK TC','PingFang TC','Heiti TC','Arial','DejaVu Sans',sans-serif;
           font-size:14px; color:#111; line-height:1.6; }
    h1,h2,h3 { margin: 0.2em 0 0.1em; }
    .muted { color:#666; font-size:12px; }
    .kpi   { font-weight:600; }
    .tag   { display:inline-block; padding:2px 6px; border-radius:4px; background:#f2f3f5; margin-right:6px; }
    table  { border-collapse:collapse; width:100%; font-size:12px; }
    th,td  { padding:6px 8px; border-bottom:1px solid #eee; text-align:right; }
    th:first-child, td:first-child, th:nth-child(2), td:nth-child(2) { text-align:left; }
    .pos { color:#0a7a0a; font-weight:600; } .neg { color:#c11; font-weight:600; }
    """

    # 清單渲染
    def _render_new():
        if not new_list: return ""
        items = "".join([f"<li>{r['股票代號']} {r.get('股票名稱','')}: {_fmt_pct(r.get('今日權重%',0))}</li>" for r in new_list])
        return f"<p><b>🆕 首次買進（權重 ≥ {NEW_MIN:.2f}%）</b></p><ul>{items}</ul>"

    def _render_sell():
        if not sell_list: return ""
        items = "".join([f"<li>{r['股票代號']} {r.get('股票名稱','')}: {_fmt_pct(r.get('持股權重_昨',0))} → {_fmt_pct(r.get('持股權重_今',0))}（Δ {_fmt_pct(r.get('權重Δ%',0))}）</li>" for r in sell_list])
        return f"<p><b>🔻 關鍵賣出</b></p><ul>{items}</ul>"

    # 表格渲染（手寫，避免 styler 外掛字型）
    cols = list(df.columns)
    def _th(v): return f'<th>{v}</th>'
    def _cell(val, col):
        if col.endswith("權重%") or col == "權重Δ%":
            s = float(val) if pd.notna(val) else None
            css = "pos" if (s is not None and s > 0) else ("neg" if (s is not None and s < 0) else "")
            return f'<td class="{css}">{_fmt_pct(val) if s is not None else "-"}</td>'
        if col.startswith("股數_") or col == "買賣超股數":
            return f'<td>{_fmt_int(val)}</td>'
        if col == "收盤價":
            return f'<td>{_fmt_price(val)}</td>'
        return f'<td>{val}</td>'

    rows_html = []
    for _, r in df.iterrows():
        tds = "".join([_cell(r[c], c) for c in cols])
        rows_html.append(f"<tr>{tds}</tr>")

    # 圖片（附件名稱列在信內，實際圖片作附件）
    imgs = []
    for name in (f"chart_d1_{date_str}.png", f"chart_daily_{date_str}.png", f"chart_weekly_{date_str}.png"):
        p = CHART_DIR / name
        if p.exists(): imgs.append(p)

    html = f"""
    <html><head><meta charset="utf-8"><style>{CSS}</style></head><body>
      <h2>00981A 今日追蹤摘要（{date_str}）</h2>
      <p>
        <span class="kpi">▶ 今日總檔數：</span>{total}　
        <span class="kpi">▶ 前十大權重合計：</span>{top10:.2f}%　
        <span class="kpi">▶ 最大權重：</span>{topw.get('code','')} {topw.get('name','')}（{float(topw.get('weight',0)):.2f}%）<br>
        <span class="kpi">▶ 比較基期（昨）：</span>{baseline}
      </p>
      { _render_new() }
      { _render_sell() }
      {"".join([f'<p class="muted"><i>附圖：</i> {p.name}</p>' for p in imgs])}
      <h3>📊 每日持股變化追蹤表</h3>
      <table>
        <thead><tr>{"".join([_th(c) for c in cols])}</tr></thead>
        <tbody>{"".join(rows_html)}</tbody>
      </table>
      <p class="muted">* 收盤價若遇非交易日或暫缺，將以最近可得交易日價格回補。</p>
      <hr>
      <p class="muted">
        您會收到這封信，是因為您（或專案維運帳號）在本專案中訂閱了每日報告。
        若您不想再收到，直接回覆此郵件寫下「取消訂閱」即可。This message is a transactional report update; if you no longer wish to receive it, reply with "unsubscribe".
      </p>
    </body></html>
    """
    return html, imgs

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

    # 確保圖存在（避免不小心漏跑 charts）
    env = os.environ.copy(); env["REPORT_DATE"] = date_str
    try:
        subprocess.check_call(["python","charts.py"], env=env)
    except Exception:
        pass

    html, imgs = _render_html(summary, df, date_str)

    FR = os.getenv("EMAIL_USERNAME","no-reply@example.com")
    TO = [t.strip() for t in os.getenv("EMAIL_TO","").split(",") if t.strip()]
    if not TO:
        raise RuntimeError("缺少 EMAIL_TO")
    subject = f"00981A Daily Tracker — {date_str}"

    attachments = [str(table_path)] + [str(p) for p in imgs if p and Path(p).exists()]

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
