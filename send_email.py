# send_email.py
# 直接讀 reports/holdings_change_table_<DATE>.csv 與 summary_<DATE>.json，組信寄出
# 若缺 summary/表格，會先自動呼叫 build_change_table.py 建好再寄。
#
# Env：
#   REPORT_DATE=YYYY-MM-DD 或含 yyyymmdd 的字串
#   EMAIL_USERNAME, EMAIL_TO, SENDGRID_API_KEY
#   NEW_HOLDING_MIN_WEIGHT（同步顯示於內文標題，預設 0.4）

import os, re, json, glob, subprocess, base64
from pathlib import Path
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, Disposition, FileContent, FileName, FileType

REPORT_DIR = Path("reports")
CHART_DIR  = Path("charts")
DATA_DIR   = Path("data")

def _normalize_date(raw: str) -> str:
    if raw and re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw.strip()):
        return raw.strip()
    if raw:
        m = re.search(r"(\d{4})(\d{2})(\d{2})", raw)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # fallback：取最新一份 summary
    js = sorted(glob.glob(str(REPORT_DIR / "summary_*.json")))
    if not js:
        raise FileNotFoundError("無法解析 REPORT_DATE，且找不到任何 summary_*.json")
    return Path(js[-1]).stem.split("_")[1]

def _ensure_built(date_str: str):
    sum_p = REPORT_DIR / f"summary_{date_str}.json"
    tbl_p = REPORT_DIR / f"holdings_change_table_{date_str}.csv"
    if sum_p.exists() and tbl_p.exists():
        return
    env = os.environ.copy()
    env["REPORT_DATE"] = date_str
    print(f"[send_email] build change table for {date_str}")
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
    try:
        return f"{float(v):.2f}%"
    except Exception:
        return "-"

def _fmt_int(v):
    try:
        return f"{int(v):,}"
    except Exception:
        return "-"

def _fmt_price(v):
    try:
        return f"{float(v):.2f}"
    except Exception:
        return "-"

def _cell(val, align="right", style=""):
    return f'<td style="text-align:{align};padding:4px 6px;{style}">{val}</td>'

def _th(val):
    return f'<th style="text-align:right;padding:6px;border-bottom:1px solid #ddd;">{val}</th>'

def _df_to_html_manual(df: pd.DataFrame) -> str:
    """不用 pandas Styler，手工渲染 HTML，Δ% 正綠負紅。"""
    cols = list(df.columns)
    # 表頭
    html = ['<table style="border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:12px;width:100%;">']
    html.append("<thead><tr>")
    for c in cols:
        html.append(_th(c))
    html.append("</tr></thead><tbody>")
    # 資料列
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
                except Exception:
                    pass

            align = "right"
            if c in ("股票代號","股票名稱"):
                align = "left"
            tds.append(_cell(val, align=align, style=style))
        html.append("<tr>" + "".join(tds) + "</tr>")
    html.append("</tbody></table>")
    return "".join(html)

def main():
    raw = os.getenv("REPORT_DATE")
    date_str = _normalize_date(raw)
    _ensure_built(date_str)

    summary = _read_summary(date_str)
    df, table_path = _read_table(date_str)

    # 文字摘要（含首次新增持股與比較基期）
    NEW_MIN = float(os.getenv("NEW_HOLDING_MIN_WEIGHT","0.4"))
    baseline = summary.get("baseline_date","(unknown)")
    top10 = summary.get("top10_sum",0.0)
    topw  = summary.get("top_weight",{})
    total = summary.get("total_count", len(df))

    # 首次新增持股（權重 > NEW_MIN）
    new_list = []
    for r in summary.get("new_holdings", []):
        try:
            wt = float(r.get("今日權重%",0))
            if wt >= NEW_MIN:
                new_list.append(f"{r['股票代號']} {r['股票名稱']}: {_fmt_pct(wt)}")
        except Exception:
            continue

    # 圖表（有就附上）
    imgs = []
    for name in (f"chart_d1_{date_str}.png", f"chart_daily_{date_str}.png", f"chart_weekly_{date_str}.png"):
        p = CHART_DIR / name
        if p.exists(): imgs.append(p)

    # HTML 內容
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
        html.append(f"<p><b>🆕 首次新增持股（權重 &gt; {NEW_MIN:.2f}%）</b><br>")
        html.append(" &nbsp; - " + "<br> &nbsp; - ".join(new_list) + "</p>")

    # 附圖（這版以附件方式附上；若改用內嵌 CID，需改成 SMTP/MIME 組件）
    for p in imgs:
        html.append(f'<p><i>附圖：</i> {p.name}</p>')

    # 表格
    html.append("<p><b>📊 每日持股變化追蹤表</b></p>")
    html.append(_df_to_html_manual(df))
    html.append('<p style="color:#666;font-size:12px">* Price may be carried from the last available trading day.</p>')

    # 寄送
    FR = os.getenv("EMAIL_USERNAME","no-reply@example.com")
    TO = os.getenv("EMAIL_TO")
    SGK= os.getenv("SENDGRID_API_KEY")
    if not (TO and SGK):
        raise RuntimeError("缺少 EMAIL_TO 或 SENDGRID_API_KEY")

    mail = Mail(
        from_email=FR,
        to_emails=[t.strip() for t in TO.split(",") if t.strip()],
        subject=f"00981A Daily Tracker — {date_str}",
        html_content="".join(html),
    )

    # 以附件方式附上圖檔與 CSV
    for p in imgs:
        mail.add_attachment(Attachment(
            file_content=FileContent(base64.b64encode(p.read_bytes()).decode()),
            file_type=FileType("image/png"),
            file_name=FileName(p.name),
            disposition=Disposition("attachment"),
        ))

    # 附上當日 CSV 報表（方便點開檢視）
    csv_path = REPORT_DIR / f"holdings_change_table_{date_str}.csv"
    if csv_path.exists():
        mail.add_attachment(Attachment(
            file_content=FileContent(base64.b64encode(csv_path.read_bytes()).decode()),
            file_type=FileType("text/csv"),
            file_name=FileName(csv_path.name),
            disposition=Disposition("attachment"),
        ))

    sg = SendGridAPIClient(SGK)
    resp = sg.send(mail)
    print("[send_email] status:", resp.status_code)

if __name__ == "__main__":
    main()