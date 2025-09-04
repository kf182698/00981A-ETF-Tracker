# send_email.py — 讀取已產出的報表與圖檔寄送；主送 SMTP、備援 SendGrid
# 使用日期邏輯：
#   1) 若存在 manifest/effective_date.txt → 以該日期為 REPORT_DATE
#   2) 否則讀取環境變數 REPORT_DATE（workflow 會已覆寫）
#   3) 信件中所有標題/欄位日期 = REPORT_DATE 與「data_snapshots 中 REPORT_DATE 之前最後一筆」日期

import os
from pathlib import Path
import glob
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import pandas as pd

# 讀日期（以 manifest 覆寫）
def get_report_date() -> str:
    p = Path("manifest/effective_date.txt")
    if p.exists():
        d = p.read_text(encoding="utf-8").strip()
        if d:
            return d
    d = (os.getenv("REPORT_DATE") or "").strip()
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d

def find_prev_snapshot(report_date: str) -> str:
    snaps = sorted(glob.glob("data_snapshots/*.csv"))
    prev = ""
    for p in reversed(snaps):
        name = Path(p).stem
        if name < report_date:
            prev = name
            break
    return prev

def human(x, digits=2):
    if pd.isna(x): return ""
    if isinstance(x, (int,)) or float(x).is_integer():
        return f"{int(x):,}"
    try:
        return f"{float(x):.{digits}f}"
    except Exception:
        return str(x)

def build_html(report_date: str) -> tuple[str, list[tuple[str, bytes]]]:
    # 載入變動表（由 build_change_table.py 產出）
    change_csv = Path("reports")/f"change_table_{report_date}.csv"
    if not change_csv.exists():
        raise SystemExit(f"缺少 {change_csv}，請先執行 build_change_table.py")

    df = pd.read_csv(change_csv, encoding="utf-8-sig")
    df["今日股數"] = pd.to_numeric(df.get("今日股數", 0), errors="coerce").fillna(0).astype(int)
    df["昨日股數"] = pd.to_numeric(df.get("昨日股數", 0), errors="coerce").fillna(0).astype(int)
    df["今日權重%"] = pd.to_numeric(df.get("今日權重%", 0.0), errors="coerce").fillna(0.0)
    df["昨日權重%"] = pd.to_numeric(df.get("昨日權重%", 0.0), errors="coerce").fillna(0.0)
    df["權重Δ%"]   = pd.to_numeric(df.get("權重Δ%", 0.0), errors="coerce").fillna(0.0)

    # 找「昨日日期」（基期）
    prev_date = find_prev_snapshot(report_date) or "N/A"

    # 摘要資料
    total_files = 1
    top10_sum = df.sort_values("今日權重%", ascending=False)["今日權重%"].head(10).sum()
    max_row = df.sort_values("今日權重%", ascending=False).head(1)
    if not max_row.empty:
        max_name = str(max_row.iloc[0].get("股票名稱", ""))
        max_code = str(max_row.iloc[0].get("股票代號", ""))
        max_weight = float(max_row.iloc[0].get("今日權重%", 0.0))
        max_text = f"{max_code} {max_name}（{max_weight:.2f}%）"
    else:
        max_text = "—"

    # 嵌圖（若不存在就忽略）
    chart_files = [
        f"charts/chart_d1_{report_date}.png",
        f"charts/chart_daily_{report_date}.png",
        f"charts/chart_weekly_{report_date}.png",
    ]
    images = []
    cid_map = {}
    for i, f in enumerate(chart_files, start=1):
        p = Path(f)
        if p.exists():
            data = p.read_bytes()
            cid = f"img{i}"
            images.append((cid, data))
            cid_map[p.name] = cid

    # 表格（挑前 30 筆權重變動絕對值最大的）
    df_show = df.copy()
    df_show["absΔ"] = df_show["權重Δ%"].abs()
    df_show = df_show.sort_values(["absΔ","今日權重%"], ascending=[False, False]).head(30)

    # 以 REPORT_DATE / prev_date 命名欄
    col_today_w = f"今日權重%（{report_date}）"
    col_yestd_w = f"昨日權重%（{prev_date}）"
    col_today_sh = f"股數（{report_date}）"
    col_yestd_sh = f"股數（{prev_date}）"

    # HTML（MS JhengHei 字型）
    style = """
      <style>
        body { font-family: 'Microsoft JhengHei', 'PingFang TC', 'Noto Sans CJK TC', Arial, sans-serif; }
        .title { font-size: 22px; font-weight: 800; margin-bottom: 12px; }
        .note { color: #6b7280; font-size: 12px; }
        table { border-collapse: collapse; width: 100%; font-size: 13px; }
        th, td { border-bottom: 1px solid #e5e7eb; text-align: right; padding: 6px 8px; }
        th:nth-child(1), td:nth-child(1),
        th:nth-child(2), td:nth-child(2) { text-align: left; }
        th { background: #f9fafb; }
        .pos { color: #16a34a; font-weight: 600; }
        .neg { color: #dc2626; font-weight: 600; }
      </style>
    """

    rows_html = []
    for _, r in df_show.iterrows():
        code = r.get("股票代號","")
        name = r.get("股票名稱","")
        cp = ""  # 收盤價目前由 with_prices 填寫，這裡僅顯示空欄
        s_t = human(r["今日股數"])
        s_y = human(r["昨日股數"])
        w_t = f"{r['今日權重%']:.2f}%"
        w_y = f"{r['昨日權重%']:.2f}%"
        dlt = float(r["權重Δ%"])
        dlt_s = f"{dlt:+.2f}%"
        cls = "pos" if dlt > 0 else "neg" if dlt < 0 else ""
        rows_html.append(
            f"<tr><td>{code}</td><td>{name}</td><td>{cp}</td>"
            f"<td>{s_t}</td><td>{w_t}</td>"
            f"<td>{s_y}</td><td>{w_y}</td>"
            f"<td class='{cls}'>{dlt_s}</td></tr>"
        )

    # 圖片連結（若有）
    attach_html = ""
    if images:
        li = []
        for p in chart_files:
            nm = Path(p).name
            if nm in cid_map:
                li.append(f"附圖： <b>{nm}</b><br><img src='cid:{cid_map[nm]}' style='max-width:100%;border:1px solid #e5e7eb;margin:8px 0;'/>")
        attach_html = "<br>".join(li)

    html = f"""
    <html><head>{style}</head><body>
      <div class="title">00981A 今日追蹤摘要（{report_date}）</div>
      <div>▶ 今日總檔數：{total_files}　▶ 前十大權重合計：{top10_sum:.2f}%　▶ 最大權重：{max_text}<br>
          ▶ 比較基期（昨）：{prev_date}</div>
      <br>
      {attach_html}
      <br>
      <h3 style="font-family: 'Microsoft JhengHei', 'PingFang TC', 'Noto Sans CJK TC', Arial, sans-serif;">📊 每日持股變化追蹤表</h3>
      <table>
        <thead>
          <tr>
            <th>股票代號</th><th>股票名稱</th><th>收盤價</th>
            <th>{col_today_sh}</th><th>{col_today_w}</th>
            <th>{col_yestd_sh}</th><th>{col_yestd_w}</th>
            <th>權重 Δ%</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows_html)}
        </tbody>
      </table>
      <br>
      <div class="note">
        本信件為自動產生，字型統一使用微軟正黑體。若您誤收此信或不需再接收，煩請直接回覆告知；此郵件僅供研究追蹤用途，並非投資建議。
      </div>
    </body></html>
    """
    return html, images

def send_with_smtp(html: str, images: list[tuple[str, bytes]]):
    user = os.getenv("EMAIL_USERNAME")
    pwd  = os.getenv("EMAIL_PASSWORD")
    to   = os.getenv("EMAIL_TO")
    if not (user and pwd and to):
        raise RuntimeError("缺少 EMAIL_USERNAME / EMAIL_PASSWORD / EMAIL_TO")

    msg = MIMEMultipart("related")
    msg["From"] = user
    msg["To"]   = to
    msg["Subject"] = "00981A Daily Tracker"

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText("本郵件為 HTML 版，請使用支援 HTML 的郵件客戶端檢視。", "plain", "utf-8"))
    alt.attach(MIMEText(html, "html", "utf-8"))
    msg.attach(alt)

    for cid, data in images:
        img = MIMEImage(data)
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline", filename=f"{cid}.png")
        msg.attach(img)

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as server:
        server.login(user, pwd)
        server.sendmail(user, [to], msg.as_string())

def send_with_sendgrid(html: str, images: list[tuple[str, bytes]]):
    key = os.getenv("SENDGRID_API_KEY")
    to  = os.getenv("EMAIL_TO")
    user = os.getenv("EMAIL_USERNAME") or "report@bot.local"
    if not (key and to):
        raise RuntimeError("缺少 SENDGRID_API_KEY / EMAIL_TO")

    # 輕量純 API 呼叫，避免額外依賴
    import base64, json, requests  # type: ignore
    payload = {
        "personalizations": [{"to": [{"email": to}]}],
        "from": {"email": user, "name": "00981A Daily"},
        "subject": "00981A Daily Tracker",
        "content": [{"type": "text/html", "value": html}],
    }
    # 內嵌圖片作為 attachment（cid）
    atts = []
    for cid, data in images:
        atts.append({
            "content": base64.b64encode(data).decode("ascii"),
            "type": "image/png",
            "filename": f"{cid}.png",
            "disposition": "inline",
            "content_id": cid,
        })
    if atts:
        payload["attachments"] = atts

    r = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        data=json.dumps(payload).encode("utf-8"),
        timeout=30,
    )
    if r.status_code >= 300:
        raise RuntimeError(f"SendGrid error: {r.status_code} {r.text[:200]}")

def main():
    report_date = get_report_date()
    if not report_date:
        raise SystemExit("REPORT_DATE 未設定")

    html, images = build_html(report_date)

    # 主送 SMTP，失敗即切換 SendGrid
    try:
        send_with_smtp(html, images)
        print("[mail] SMTP sent")
    except Exception as e:
        print(f"[mail] SMTP failed → fallback: {e}")
        send_with_sendgrid(html, images)
        print("[mail] SendGrid sent")

if __name__ == "__main__":
    main()