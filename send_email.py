# send_email.py
# 直接讀 reports/holdings_change_table_<DATE>.csv 與 summary_<DATE>.json，組信寄出
# 若缺 summary/表格，會先自動呼叫 build_change_table.py 建好再寄。
#
# Env：
#   REPORT_DATE=YYYY-MM-DD 或含 yyyymmdd 的字串
#   EMAIL_USERNAME, EMAIL_TO, SENDGRID_API_KEY
#   NEW_HOLDING_MIN_WEIGHT（同步顯示於內文標題，預設 0.4）

import os, re, json, glob, subprocess
from pathlib import Path
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, Disposition, FileContent, FileName, FileType

REPORT_DIR = Path("reports")
CHART_DIR  = Path("charts")
DATA_DIR   = Path("data")

def _normalize_date(raw: str) -> str:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw.strip()):
        return raw.strip()
    m = re.search(r"(\d{4})(\d{2})(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    raise ValueError(f"無法解析日期：{raw}")

def _latest_date():
    js = sorted(glob.glob(str(REPORT_DIR / "summary_*.json")))
    if not js:
        raise FileNotFoundError("沒有任何 summary_*.json")
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
    if pd.isna(v): return "-"
    return f"{float(v):.2f}%"

def _fmt_int(v):
    if pd.isna(v): return "-"
    return f"{int(v):,}"

def _fmt_price(v):
    if pd.isna(v): return "-"
    return f"{float(v):.2f}"

def _df_to_html(df: pd.DataFrame) -> str:
    # 欄位名不改（已按規格命名），做簡單格式化
    fmt = {}
    # 自動偵測欄位
    for c in df.columns:
        if c.endswith("權重%") or c == "權重Δ%":
            fmt[c] = lambda x: _fmt_pct(x)
        elif c.startswith("股數_") or c == "買賣超股數":
            fmt[c] = lambda x: _fmt_int(x)
        elif c == "收盤價":
            fmt[c] = lambda x: _fmt_price(x)

    df_fmt = df.copy()
    for c, fn in fmt.items():
        if c in df_fmt.columns:
            df_fmt[c] = df_fmt[c].apply(fn)

    # 簡易樣式：Δ% 正綠負紅
    styles = [
        dict(selector="th", props=[("text-align","right"),("padding","6px")]),
        dict(selector="td", props=[("text-align","right"),("padding","4px 6px")]),
        dict(selector="table", props=[("border-collapse","collapse"),("font-family","Arial"),("font-size","12px")]),
    ]
    def color_delta(val):
        try:
            v = float(str(val).replace("%",""))
        except:
            return ""
        if v > 0:  return "color:#008800;"
        if v < 0:  return "color:#cc0000;"
        return ""

    if "權重Δ%" in df_fmt.columns:
        styler = df_fmt.style.applymap(color_delta, subset=["權重Δ%"]).set_table_styles(styles)
    else:
        styler = df_fmt.style.set_table_styles(styles)
    return styler.hide(axis="index").to_html()

def main():
    raw = os.getenv("REPORT_DATE")
    date_str = _normalize_date(raw) if raw else _latest_date()
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
        if float(r.get("今日權重%",0)) >= NEW_MIN:
            new_list.append(f"{r['股票代號']} {r['股票名稱']}: {_fmt_pct(r['今日權重%'])}")

    # 圖表（有就附上）
    imgs = []
    for name in (f"chart_d1_{date_str}.png", f"chart_daily_{date_str}.png", f"chart_weekly_{date_str}.png"):
        p = CHART_DIR / name
        if p.exists(): imgs.append(p)

    # HTML
    html = []
    html.append(f"<p>您好，</p>")
    html.append(f"<p><b>00981A 今日追蹤摘要（{date_str}）</b></p>")
    html.append(f"<p>▶ 今日總檔數：{total}　▶ 前十大權重合計：{top10:.2f}%　▶ 最大權重：{topw.get('code','')} {topw.get('name','')}（{topw.get('weight',0):.2f}%）<br>")
    html.append(f"▶ 比較基期（昨）：{baseline}</p>")

    if new_list:
        html.append(f"<p><b>🆕 首次新增持股（權重 &gt; {NEW_MIN:.2f}%）</b><br>")
        html.append(" &nbsp; - " + "<br> &nbsp; - ".join(new_list) + "</p>")

    # 附圖
    for p in imgs:
        html.append(f'<p><img src="cid:{p.name}" style="max-width:800px;width:100%;"></p>')

    # 表格（依你指定欄位，已在 build 階段排序）
    html.append("<p><b>📊 每日持股變化追蹤表</b></p>")
    html.append(_df_to_html(df))

    # 價格說明
    html.append('<p style="color:#666;font-size:12px">* Price may be carried from the last available trading day.</p>')

    # 寄送
    FR = os.getenv("EMAIL_USERNAME","no-reply@example.com")
    TO = os.getenv("EMAIL_TO")
    SGK= os.getenv("SENDGRID_API_KEY")
    if not (TO and SGK):
        raise RuntimeError("缺少 EMAIL_TO 或 SENDGRID_API_KEY")

    mail = Mail(
        from_email=FR,
        to_emails=TO.split(","),
        subject=f"00981A Daily Tracker — {date_str}",
        html_content="".join(html),
    )

    # 內嵌圖片
    for p in imgs:
        b64 = p.read_bytes().hex()  # SendGrid 需 base64；此處走 attachment cid 簡化可用 MIME，但 sendgrid helpers 不直接支援 related。
        # 簡化：改為附件（非內嵌），避免 content-id 複雜處理。若你一定要內嵌，可改用 SMTP 或自行構建 MIME。
        mail.add_attachment(Attachment(
            file_content=FileContent(p.read_bytes()),
            file_type=FileType("image/png"),
            file_name=FileName(p.name),
            disposition=Disposition("attachment"),
        ))

    sg = SendGridAPIClient(SGK)
    resp = sg.send(mail)
    print("[send_email] status:", resp.status_code)

if __name__ == "__main__":
    main()