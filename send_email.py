# send_email.py
# Build rich email with text summary + inline charts + HTML table
# Uses SendGrid API.
#
# Env:
#   EMAIL_USERNAME, EMAIL_TO, SENDGRID_API_KEY
#   REPORT_DATE=YYYY-MM-DD or string containing yyyymmdd (e.g., ETF_Investment_Portfolio_20250808)
# If REPORT_DATE not set, it will use the latest summary_*.json

import os, json, glob, base64, re
from pathlib import Path
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Attachment, FileContent, FileName, FileType, Disposition, ContentId

REPORT_DIR = Path("reports")
CHART_DIR = Path("charts")

def _latest_date():
    js = sorted(glob.glob(str(REPORT_DIR / "summary_*.json")))
    if not js:
        raise FileNotFoundError("no summary_*.json")
    return Path(js[-1]).stem.split("_")[1]

def _normalize_report_date(s: str) -> str:
    """
    Accepts:
      - '2025-08-08'
      - 'ETF_Investment_Portfolio_20250808'
      - 'downloads/ETF_Investment_Portfolio_20250808.xlsx'
    Returns: '2025-08-08'
    """
    if not s:
        return s
    s = s.strip()
    m = re.fullmatch(r"\d{4}-\d{2}-\d{2}", s)
    if m:
        return s
    m = re.search(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    raise ValueError(f"Cannot parse REPORT_DATE: {s}")

def _load_summary(date_str):
    with open(REPORT_DIR / f"summary_{date_str}.json", "r", encoding="utf-8") as f:
        return json.load(f)

def _fmt_pct(x):
    return f"{x:.2f}%"

def build_html(date_str, summary):
    # headline
    top = summary["top_weight"]
    lines = []
    lines.append(f"您好，<br><br>")
    lines.append(f"<b>00981A 今日追蹤摘要（{date_str}）</b><br><br>")
    lines.append(f"▶ 今日總檔數：{summary['total_count']}<br>")
    lines.append(f"▶ 前十大權重合計：{_fmt_pct(summary['top10_sum'])}<br>")
    lines.append(f"▶ 最大權重：{top['code']} {top['name']}（{_fmt_pct(top['weight'])}）<br><br>")

    def _ul(items, title):
        if not items:
            return ""
        out = [f"<b>{title}</b><br><ul style='margin-top:4px'>"]
        for it in items:
            before = _fmt_pct(it.get("持股權重_昨日", 0.0))
            after  = _fmt_pct(it.get("持股權重_今日", 0.0))
            delta  = _fmt_pct(it.get("Δ%", 0.0))
            out.append(f"<li>{it['股票代號']} {it['股票名稱']}: {before} → {after} (<b>{delta}</b>)</li>")
        out.append("</ul><br>")
        return "\n".join(out)

    lines.append(_ul(summary.get("d1_up", []), "▲ D1 權重上升 Top 10"))
    lines.append(_ul(summary.get("d1_dn", []), "▼ D1 權重下降 Top 10"))

    nh = summary.get("new_holdings", [])
    if nh:
        nh_items = [f"{it['股票代號']} {it['股票名稱']}: {_fmt_pct(it['持股權重_今日'])}" for it in nh]
        lines.append(f"🆕 首次新增持股（權重 > {summary.get('new_holdings_min', 0.5):.2f}%）：{len(nh_items)} 檔<br> - " + "<br> - ".join(nh_items) + "<br><br>")

    sa = summary.get("sell_alerts", [])
    if sa:
        lines.append(_ul(sa, "⚠️ 關鍵賣出警示（今日 ≤ 閾值 且昨日 > 噪音門檻）"))

    # charts inline
    img_names = [
        ("D1 Weight Change (Top Movers)", CHART_DIR / f"chart_d1_{date_str}.png", "d1"),
        ("Daily Weight Trend (Top Movers x5)", CHART_DIR / f"chart_daily_{date_str}.png", "daily"),
        ("Weekly Cumulative Weight Change (vs first week)", CHART_DIR / f"chart_weekly_{date_str}.png", "weekly"),
    ]
    for title, p, cid in img_names:
        if p.exists():
            lines.append(f"<div><b>{title}</b><br><img src='cid:{cid}' style='max-width:100%'></div><br>")

    # HTML change table
    csv_path = REPORT_DIR / f"holdings_change_table_{date_str}.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        df_html = df.to_html(index=False, border=0, classes='tbl', justify='center')
        lines.append("<b>📊 每日持股變化追蹤表</b><br>" + df_html)

    return "\n".join(lines), img_names

def main():
    TO = os.getenv("EMAIL_TO")
    FR = os.getenv("EMAIL_USERNAME")
    SGK = os.getenv("SENDGRID_API_KEY")
    assert TO and FR and SGK, "請設定 EMAIL_TO / EMAIL_USERNAME / SENDGRID_API_KEY"

    raw = os.getenv("REPORT_DATE")
    date_str = _normalize_report_date(raw) if raw else _latest_date()
    summary = _load_summary(date_str)

    html, img_list = build_html(date_str, summary)

    mail = Mail(
        from_email=Email(FR),
        to_emails=[To(TO)],
        subject=f"00981A Daily Tracker — {date_str}",
        html_content=html
    )

    # attach inline images (cid)
    for title, p, cid in img_list:
        if not p.exists():
            continue
        with open(p, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        att = Attachment()
        att.file_content = FileContent(b64)
        att.file_type = FileType("image/png")
        att.file_name = FileName(p.name)
        att.disposition = Disposition("inline")
        att.content_id = ContentId(cid)
        mail.attachment = mail.attachment + [att] if mail.attachment else [att]

    sg = SendGridAPIClient(SGK)
    resp = sg.send(mail)
    print("[send_email] status:", resp.status_code)

if __name__ == "__main__":
    main()
