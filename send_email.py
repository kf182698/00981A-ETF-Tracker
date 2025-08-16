# send_email.py —— 完整版（含內嵌圖片、賣出警示、三份報表、每日變化表「嵌入Email」）
import os
import glob
import smtplib
import mimetypes
from datetime import datetime

import pandas as pd
from email.message import EmailMessage
from email.utils import make_msgid

from config import (
    TOP_N,
    THRESH_UPDOWN_EPS,
    NEW_WEIGHT_MIN,
    SELL_ALERT_THRESHOLD,
    REPORT_DIR,
    PCT_DECIMALS,
)

# ===== Secrets（環境變數） =====
TO   = os.environ.get("EMAIL_TO")
USER = os.environ.get("EMAIL_USERNAME")
PWD  = os.environ.get("EMAIL_PASSWORD")
assert TO and USER and PWD, "請在 Secrets 設定 EMAIL_TO / EMAIL_USERNAME / EMAIL_PASSWORD"

# ===== 工具 =====
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
    """將 DataFrame 渲染為簡潔 HTML 表格（前 max_rows 列），含簡單樣式"""
    if df is None or df.empty:
        return "<i>(今日無變化表資料)</i>"
    view = df.head(max_rows).copy()
    # 數值欄位加上格式（不改原檔，僅渲染）
    for col in view.columns:
        if view[col].dtype.kind in "if":
            if "權重" in col:
                view[col] = view[col].map(lambda x: fmt_pct(x))
            else:
                # 股數/買賣超加千分位
                view[col] = view[col].map(lambda x: f"{int(x):,}" if pd.notna(x) else "")
    # 安全轉字串
    view = view.fillna("").astype(str)

    # inline CSS：各家郵件客戶端友善
    style = """
      style="
        border-collapse:collapse;
        width:100%;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, 'Noto Sans', 'PingFang TC', 'Microsoft JhengHei', sans-serif;
        font-size:13px;"
    """
    th_style = 'style="background:#f5f6f7;border:1px solid #ddd;padding:6px;text-align:center;"'
    td_style = 'style="border:1px solid #ddd;padding:6px;text-align:right;white-space:nowrap;"'
    td_left  = 'style="border:1px solid #ddd;padding:6px;text-align:left;white-space:nowrap;"'

    # 建表頭
    cols = list(view.columns)
    html = [f"<table {style}>", "<thead><tr>"]
    for c in cols:
        html.append(f"<th {th_style}>{c}</th>")
    html.append("</tr></thead><tbody>")

    # 建資料列
    for _, r in view.iterrows():
        html.append("<tr>")
        for i, c in enumerate(cols):
            cell_style = td_left if ("股票代號" in c or "股票名稱" in c) else td_style
            html.append(f"<td {cell_style}>{r[c]}</td>")
        html.append("</tr>")
    html.append("</tbody></table>")
    if len(df) > max_rows:
        html.append(f'<div style="color:#666;font-size:12px;margin-top:4px;">(僅顯示前 {max_rows} 列，完整內容見附件 Excel)</div>')
    return "".join(html)

# ===== 找檔 =====
today = datetime.today().strftime("%Y-%m-%d")

data_path   = latest_file(f"data/{today}.csv") or latest_file("data/*.csv")
diff_path   = latest_file(f"diff/diff_{today}.csv") or latest_file("diff/*.csv")
updown_path = latest_file(f"{REPORT_DIR}/up_down_today_{today}.csv") or latest_file(f"{REPORT_DIR}/up_down_today_*.csv")
new_path    = latest_file(f"{REPORT_DIR}/new_gt_0p5_{today}.csv") or latest_file(f"{REPORT_DIR}/new_gt_*_{today}.csv") or latest_file(f"{REPORT_DIR}/new_gt_*_*.csv")
w5d_path    = latest_file(f"{REPORT_DIR}/weights_chg_5d_{today}.csv") or latest_file(f"{REPORT_DIR}/weights_chg_5d_*.csv")
sell_path   = latest_file(f"{REPORT_DIR}/sell_alerts_{today}.csv") or latest_file(f"{REPORT_DIR}/sell_alerts_*.csv")

# 新增：每日變化表（CSV/Excel）
change_csv  = latest_file(f"{REPORT_DIR}/holdings_change_table_{today}.csv") or latest_file(f"{REPORT_DIR}/holdings_change_table_*.csv")
change_xlsx = latest_file(f"{REPORT_DIR}/holdings_change_table_{today}.xlsx") or latest_file(f"{REPORT_DIR}/holdings_change_table_*.xlsx")

# 圖片（可能沒有就回退到最近一張）
chart_d1    = latest_file(f"charts/d1_top_changes_{today}.png") or latest_file("charts/d1_top_changes_*.png")
chart_daily = latest_file(f"charts/daily_trend_{today}.png")   or latest_file("charts/daily_trend_*.png")
chart_week  = latest_file(f"charts/weekly_cum_trend_{today}.png") or latest_file("charts/weekly_cum_trend_*.png")

# ===== 讀資料 =====
df_data   = read_csv_safe(data_path)
df_updn   = read_csv_safe(updown_path)
df_new    = read_csv_safe(new_path)
df_5d     = read_csv_safe(w5d_path)
df_sell   = read_csv_safe(sell_path)
df_change = read_csv_safe(change_csv)

# ===== 摘要組裝 =====
lines = []

def top_weights_summary(df_today: pd.DataFrame):
    df = df_today.copy()
    df.columns = [str(c).strip().replace("　","").replace("\u3000","") for c in df.columns]
    col = None
    for c in ["持股權重","持股比例","權重","占比","比重(%)","占比(%)"]:
        if c in df.columns:
            col = c
            break
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

# 基本概況
if df_data is not None:
    total_rows, top10_sum, max_one = top_weights_summary(df_data)
    lines += [
        f"▶ 今日總檔數：{total_rows}",
        f"▶ 前十大權重合計：{top10_sum:.2f}%",
        f"▶ 最大權重：{max_one['code']} {max_one['name']}（{max_one['w']:.2f}%）",
        ""
    ]
else:
    lines.append("（今日資料缺失）")

# D1 上/下 TopN
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

# 首次新增 > 閾值（門檻可調）
if df_new is not None and not df_new.empty:
    n = df_new.copy()
    if "今日權重%" in n.columns:
        n["今日權重%"] = pd.to_numeric(n["今日權重%"], errors="coerce").fillna(0.0)
        n = n.sort_values("今日權重%", ascending=False)
    lines.append(f"🆕 首次新增持股（權重 > {NEW_WEIGHT_MIN:.2f}%）：{len(n)} 檔")
    for _, r in n.iterrows():
        lines.append(f"  - {r.get('股票代號','-')} {r.get('股票名稱','-')}: {fmt_pct(r.get('今日權重%'))}")
    lines.append("")
else:
    lines.append(f"🆕 首次新增持股（權重 > {NEW_WEIGHT_MIN:.2f}%）：0 檔")
    lines.append("")

# ⚠️ 關鍵賣出警示（今日 ≤ 閾值且昨日 > 閾值，且 D1 為負）
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

# D5 上/下 TopN
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

# ===== 組信：text + html + inline images（正確流程） =====
subject = f"[ETF追蹤通知] 00981A 投資組合變動報告（{today}）"

msg = EmailMessage()
msg["From"] = USER
msg["To"]   = TO
msg["Subject"] = subject

# 純文字版本（不含表格）
text_body = (
    "您好，\n\n"
    f"00981A 今日追蹤摘要（{today}）\n" +
    "\n".join(lines) +
    "\n\n（若看不到圖片/表格，請查看附件）\n"
)
msg.set_content(text_body)

# 先產 CID（如果圖片檔存在才產）
def cid_if_exists(path):
    return make_msgid(domain="charts.local")[1:-1] if path and os.path.exists(path) else None

cid_d1    = cid_if_exists(chart_d1)
cid_daily = cid_if_exists(chart_daily)
cid_week  = cid_if_exists(chart_week)

# 變化表：轉成 HTML（最多 30 列）
change_table_html = df_to_html_table(df_change, max_rows=30)

# HTML 內容（把 CID 放進 <img src="cid:...">），並嵌入變化表
html_lines = "<br>".join(lines).replace("  - ", "&nbsp;&nbsp;- ")
html_final = f"""
<html>
  <body>
    <p>您好，</p>
    <p>00981A 今日追蹤摘要（{today}）</p>
    <pre style="font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 13px; white-space: pre-wrap;">{html_lines}</pre>

    <h3 style="margin:12px 0 8px 0;">📊 每日持股變化追蹤表</h3>
    {change_table_html}

    <h3 style="margin:16px 0 8px 0;">D1 增減幅度排序圖</h3>
    {f'<img src="cid:{cid_d1}" />' if cid_d1 else '<i>(無圖)</i>'}

    <h3 style="margin:16px 0 8px 0;">每日權重趨勢（Top Movers x5）</h3>
    {f'<img src="cid:{cid_daily}" />' if cid_daily else '<i>(無圖)</i>'}

    <h3 style="margin:16px 0 8px 0;">週累積權重變化（對第一週）</h3>
    {f'<img src="cid:{cid_week}" />' if cid_week else '<i>(無圖)</i>'}

    <p style="color:#666;">（若看不到圖片/表格，請查看附件 CSV / Excel / PNG 檔）</p>
  </body>
</html>
"""
# 加入 HTML part
msg.add_alternative(html_final, subtype="html")

# 取得 HTML part（注意：add_alternative 不回傳 part，要用 get_body）
html_part = msg.get_body(preferencelist=('html',))

# 把圖片資料以同一個 CID 內嵌到 HTML part
def embed(html_part, path, cid):
    if not (html_part and path and os.path.exists(path) and cid):
        return
    with open(path, "rb") as f:
        html_part.add_related(f.read(), maintype="image", subtype="png", cid=f"<{cid}>")

embed(html_part, chart_d1,    cid_d1)
embed(html_part, chart_daily, cid_daily)
embed(html_part, chart_week,  cid_week)

# ===== 附件（報表 + 表格 + 圖片備援）=====
def attach_file(path):
    if not path or not os.path.exists(path):
        return
    ctype, encoding = mimetypes.guess_type(path)
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"
    maintype, subtype = ctype.split("/", 1)
    with open(path, "rb") as f:
        msg.add_attachment(f.read(), maintype=maintype, subtype=subtype, filename=os.path.basename(path))

for p in [
    data_path, diff_path, updown_path, new_path, w5d_path, sell_path,
    change_csv, change_xlsx,
    chart_d1, chart_daily, chart_week
]:
    attach_file(p)

# ===== 寄信 =====
with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
    smtp.login(USER, PWD)
    smtp.send_message(msg)

print("Email sent to:", TO)
