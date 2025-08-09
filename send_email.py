import os
import glob
import smtplib
import mimetypes
from email.message import EmailMessage
from datetime import datetime
import pandas as pd

TO = os.environ.get("EMAIL_TO")
USER = os.environ.get("EMAIL_USERNAME")
PWD = os.environ.get("EMAIL_PASSWORD")

assert TO and USER and PWD, "請設定 EMAIL_TO / EMAIL_USERNAME / EMAIL_PASSWORD 為 GitHub Secrets"

def latest_file(pattern):
    files = glob.glob(pattern)
    return max(files, key=os.path.getmtime) if files else None

# 找今天 data 檔、今日 diff 檔（若有）
today = datetime.today().strftime("%Y-%m-%d")
data_path = latest_file(f"data/{today}.csv") or latest_file("data/*.csv")
diff_path = latest_file(f"diff/diff_{today}.csv") or latest_file("diff/*.csv")

# 建摘要（若有 diff）
summary_lines = []
subject_tag = "穩定"  # 預設無變化
if diff_path and os.path.exists(diff_path):
    df = pd.read_csv(diff_path)
    # 新增/刪除/變動統計
    added = df[df["_merge"] == "right_only"].shape[0] if "_merge" in df.columns else 0
    removed = df[df["_merge"] == "left_only"].shape[0] if "_merge" in df.columns else 0
    changed_up = df[(df.get("股數變動", 0) > 0) | (df.get("持股權重變動", 0) > 0)].shape[0]
    changed_dn = df[(df.get("股數變動", 0) < 0) | (df.get("持股權重變動", 0) < 0)].shape[0]
    summary_lines.append(f"新增持股：{added} 檔")
    summary_lines.append(f"移除持股：{removed} 檔")
    summary_lines.append(f"持股上升（股數或權重）：{changed_up} 檔")
    summary_lines.append(f"持股下降（股數或權重）：{changed_dn} 檔")
    if any([added, removed, changed_up, changed_dn]):
        subject_tag = "變動"
else:
    summary_lines.append("第一天建檔或尚無昨日資料，暫無差異報告。")

summary = "\n".join(summary_lines)
subject = f"[ETF追蹤通知] 00981A 投資組合{subject_tag}報告（{today}）"

# 組信
msg = EmailMessage()
msg["From"] = USER
msg["To"] = TO
msg["Subject"] = subject
msg.set_content(
    f"""您好，

00981A 今日追蹤摘要（{today}）：
{summary}

附件包含：
- 當日投資組合（data）
- 差異報告（diff，若有）

此信由 GitHub Actions 自動發送。
"""
)

def attach_file(path):
    if not path or not os.path.exists(path):
        return
    ctype, encoding = mimetypes.guess_type(path)
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"
    maintype, subtype = ctype.split("/", 1)
    with open(path, "rb") as f:
        msg.add_attachment(f.read(), maintype=maintype, subtype=subtype,
                           filename=os.path.basename(path))

attach_file(data_path)
attach_file(diff_path)

# 寄信（Gmail SMTP）
with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
    smtp.login(USER, PWD)
    smtp.send_message(msg)

print("Email sent to:", TO)
