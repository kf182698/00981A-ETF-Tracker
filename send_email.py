# send_email.py
import os
import glob
import smtplib
import mimetypes
from email.message import EmailMessage
from email.utils import make_msgid
from datetime import datetime
import pandas as pd
from config import TOP_N, THRESH_UPDOWN_EPS, NEW_WEIGHT_MIN, REPORT_DIR, PCT_DECIMALS

TO = os.environ.get("EMAIL_TO")
USER = os.environ.get("EMAIL_USERNAME")
PWD = os.environ.get("EMAIL_PASSWORD")
assert TO and USER and PWD, "請在 Secrets 設定 EMAIL_TO / EMAIL_USERNAME / EMAIL_PASSWORD"

def latest_file(pattern):
    files = glob.glob(pattern)
    return max(files, key=os.path.getmtime) if files else None

today = datetime.today().strftime("%Y-%m-%d")

data_path = latest_file(f"data/{today}.csv") or latest_file("data/*.csv")
diff_path = latest_file(f"diff/diff_{today}.csv") or latest_file("diff/*.csv")
updown_path = latest_file(f"{REPORT_DIR}/up_down_today_{today}.csv")
new_path    = latest_file(f"{REPORT_DIR}/new_gt_0p5_{today}.csv")
w5d_path    = latest_file(f"{REPORT_DIR}/weights_chg_5d_{today}.csv")

# charts
chart_d1   = latest_file(f"charts/d1_top_changes_{today}.png")
chart_daily= latest_file(f"charts/daily_trend_{today}.png") or latest_file("charts/daily_trend_*.png")
chart_week = latest_file(f"charts/weekly_cum_trend_{today}.png") or latest_file("charts/weekly_cum_trend_*.png")

def _fmt_pct(v):
    if pd.isna(v): return "-"
    return f"{float(v):.{PCT_DECIMALS}f}%"
def _fmt_pair(y, t): return f"{_fmt_pct(y)} → {_fmt_pct(t)}"

def _read_csv_safe(p): return pd.read_csv(p) if p and os.path.exists(p) else None

df_data = _read_csv_safe(data_path)
df_diff = _read_csv_safe(diff_path)
df_updn = _read_csv_safe(updown_path)
df_new  = _read_csv_safe(new_path)
df_5d   = _read_csv_safe(w5d_path)

# ===== 摘要文字（與你既有邏輯一致） =====
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

if df_data is not None:
    total_rows, top10_sum, max_one = top_weights_summary(df_data)
    lines += [
        f"▶ 今日總檔數：{total_rows}",
        f"▶ 前十大權重合計：{top10_sum:.2f}%",
        f"▶ 最大權重：{max_one['code']} {max_one['name']}（{max_one['w']:.2f}%）",
        ""
    ]

if df_updn is not None and not df_updn.empty:
    tmp = df_updn.copy()
    tmp["Δ%"] = pd.to_numeric(tmp["Δ%"], errors="coerce").fillna(0.0)
    tmp["昨日權重%"] = pd.to_numeric(tmp["昨日權重%"], errors="coerce").fillna(0.0)
    tmp["今日權重%"] = pd.to_numeric(tmp["今日權重%"], errors="coerce").fillna(0.0)
    sig = tmp[abs(tmp["Δ%"]) >= THRESH_UPDOWN_EPS]
    up = sig.sort_values("Δ%", ascending=False).head(TOP_N)
    dn = sig.sort_values("Δ%", ascending=True).head(TOP_N)
    def _to_lines(df_sel, title):
        out = [title]
        for _, r in df_sel.iterrows():
            out.append(f"  - {r['股票代號']} {r['股票名稱']}: {_fmt_pair(r['昨日權重%'], r['今日權重%'])} ({r['Δ%']:+.{PCT_DECIMALS}f}%)")
        return out
    lines += _to_lines(up, f"▲ D1 權重上升 Top {TOP_N}")
    lines += _to_lines(dn, f"▼ D1 權重下降 Top {TOP_N}")
    lines.append("")

if df_new is not None and not df_new.empty:
    nn = df_new.copy()
    if "今日權重%" in nn.columns:
        nn = nn.sort_values("今日權重%", ascending=False)
    lines.append(f"🆕 首次新增持股（權重 > {NEW_WEIGHT_MIN:.2f}%）：{len(nn)} 檔")
    for _, r in nn.iterrows():
        lines.append(f"  - {r.get('股票代號','-')} {r.get('股票名稱','-')}: {_fmt_pct(r.get('今日權重%'))}")
    lines.append("")
else:
    lines.append(f"🆕 首次新增持股（權重 > {NEW_WEIGHT_MIN:.2f}%）：0 檔")
    lines.append("")

if df_5d is not None and not df_5d.empty:
    t5 = df_5d.copy()
    for c in ["今日%","昨日%","D1Δ%","T-5日%","D5Δ%"]:
        t5[c] = pd.to_numeric(t5[c], errors="coerce").fillna(0.0)
    up5 = t5.sort_values("D5Δ%", ascending=False).head(TOP_N)
    dn5 = t5.sort_values("D5Δ%", ascending=True).head(TOP_N)
    def _to_lines5(df_sel, title):
        out = [title]
        for _, r in df_sel.iterrows():
            out.append(f"  - {r['股票代號']} {r['股票名稱']}: { _fmt_pair(r['T-5日%'], r['今日%']) } ({r['D5Δ%']:+.{PCT_DECIMALS}f}%)")
        return out
    lines += _to_lines5(up5, f"⏫ D5 權重上升 Top {TOP_N}")
    lines += _to_lines5(dn5, f"⏬ D5 權重下降 Top {TOP_N}")
    lines.append("")

subject_tag = "變動"
subject = f"[ETF追蹤通知] 00981A 投資組合{subject_tag}報告（{today}）"

# ===== 組信：text + html + inline images =====
msg = EmailMessage()
msg["From"] = USER
msg["To"] = TO
msg["Subject"] = subject

text_body = ("您好，\n\n"
             f"00981A 今日追蹤摘要（{today}）\n" +
             "\n".join(lines) +
             "\n\n（若看不到圖片，請查看附件）\n")
msg.set_content(text_body)

# 內嵌圖片（有就嵌）
def _attach_inline_img(msg, path):
    if not path or not os.path.exists(path): return None
    cid = make_msgid(domain="charts.local")  # <...>
    with open(path, "rb") as f:
        maintype, subtype = "image", "png"
        msg.get_payload()[0].add_related(f.read(), maintype=maintype, subtype=subtype, cid=cid)
    return cid[1:-1]  # 去掉尖括號

cid_d1   = _attach_inline_img(msg, chart_d1)
cid_daily= _attach_inline_img(msg, chart_daily)
cid_week = _attach_inline_img(msg, chart_week)

# 加上 HTML 版本
html_lines = "<br>".join(lines).replace("  - ", "&nbsp;&nbsp;- ")
html_body = f"""
<html>
  <body>
    <p>您好，</p>
    <p>00981A 今日追蹤摘要（{today}）</p>
    <pre style="font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 13px;">{html_lines}</pre>
    <p><b>D1 增減幅度排序圖</b></p>
    {'<img src="cid:'+cid_d1+'" />' if cid_d1 else '<i>(無圖)</i>'}
    <p><b>每日權重趨勢（Top Movers x5）</b></p>
    {'<img src="cid:'+cid_daily+'" />' if cid_daily else '<i>(無圖)</i>'}
    <p><b>週累積權重變化（對第一週）</b></p>
    {'<img src="cid:'+cid_week+'" />' if cid_week else '<i>(無圖)</i>'}
    <p>（若看不到圖片，請查看附件 PNG 檔）</p>
  </body>
</html>
"""
msg.add_alternative(html_body, subtype="html")

# 同時把圖片與報表「作為附件」附上（郵件客戶端若不支援 inline 也能看）
def _attach_file(path):
    if not path or not os.path.exists(path): return
    ctype, encoding = mimetypes.guess_type(path)
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"
    maintype, subtype = ctype.split("/", 1)
    with open(path, "rb") as f:
        msg.add_attachment(f.read(), maintype=maintype, subtype=subtype, filename=os.path.basename(path))

for p in [data_path, diff_path, updown_path, new_path, w5d_path, chart_d1, chart_daily, chart_week]:
    _attach_file(p)

# 寄信
with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
    smtp.login(USER, PWD)
    smtp.send_message(msg)

print("Email sent to:", TO)
