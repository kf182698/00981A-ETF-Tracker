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

# 先建立 HTML 版本，拿到 html_part 之後再把圖片「加到這個 part」裡
html_lines = "<br>".join(lines).replace("  - ", "&nbsp;&nbsp;- ")
html_body = f"""
<html>
  <body>
    <p>您好，</p>
    <p>00981A 今日追蹤摘要（{today}）</p>
    <pre style="font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 13px;">{html_lines}</pre>
    <p><b>D1 增減幅度排序圖</b></p>
    {{IMG_D1}}
    <p><b>每日權重趨勢（Top Movers x5）</b></p>
    {{IMG_DAILY}}
    <p><b>週累積權重變化（對第一週）</b></p>
    {{IMG_WEEK}}
    <p>（若看不到圖片，請查看附件 PNG 檔）</p>
  </body>
</html>
"""

from email.utils import make_msgid
def _embed_img(html_part, path):
    if not path or not os.path.exists(path):
        return None
    cid = make_msgid(domain="charts.local")  # 會回傳 <...>
    with open(path, "rb") as f:
        html_part.add_related(f.read(), maintype="image", subtype="png", cid=cid)
    return cid[1:-1]  # 去掉 <> 供 HTML 使用

# 先加 HTML part，拿引用
html_part = msg.add_alternative(html_body, subtype="html")

# 針對 html_part 內嵌圖片（注意是對 html_part 調用 add_related）
cid_d1   = _embed_img(html_part, chart_d1)
cid_daily= _embed_img(html_part, chart_daily)
cid_week = _embed_img(html_part, chart_week)

# 把對應的 <img src="cid:..."> 填入 HTML 內容
html_final = html_body.replace(
    "{IMG_D1}",    f'<img src="cid:{cid_d1}" />' if cid_d1 else "<i>(無圖)</i>"
).replace(
    "{IMG_DAILY}", f'<img src="cid:{cid_daily}" />' if cid_daily else "<i>(無圖)</i>"
).replace(
    "{IMG_WEEK}",  f'<img src="cid:{cid_week}" />' if cid_week else "<i>(無圖)</i>"
)

# 更新 html_part 的 payload（覆蓋原本文字樣板）
html_part.set_content(html_final, subtype="html")
