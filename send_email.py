# send_email.py — 純報表郵件（無圖片）
# - 嚴格以 REPORT_DATE（或 manifest/effective_date.txt）為準
# - 讀取 reports/change_table_{REPORT_DATE}.csv
# - 表格依「權重Δ%」由大到小排序
# - 固定列出「首次新增持股」與「關鍵賣出」，若無則顯示「無」
# - 新增欄位：買賣超股數 = 今日股數 - 昨日股數（若檔案內已帶此欄仍會覆蓋為此計算）
# - 主送 SMTP（Gmail），失敗則自動改用 SendGrid API

import os
import glob
import smtplib
import ssl
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd

# -------------------- 共用：日期/檔案 --------------------
def get_report_date() -> str:
    """優先讀 manifest/effective_date.txt，其次讀環境變數 REPORT_DATE。"""
    m = Path("manifest/effective_date.txt")
    if m.exists():
        d = m.read_text(encoding="utf-8").strip()
        if d:
            return d
    d = (os.getenv("REPORT_DATE") or "").strip()
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d

def find_prev_snapshot(report_date: str) -> str:
    """回傳 data_snapshots 中 < report_date 的最後一筆日期（YYYY-MM-DD）。找不到回傳空字串。"""
    snaps = sorted(glob.glob("data_snapshots/*.csv"))
    prev = ""
    for p in reversed(snaps):
        name = Path(p).stem
        if name < report_date:
            prev = name
            break
    return prev

def human_int(x) -> str:
    """千位分隔符。"""
    if pd.isna(x):
        return ""
    try:
        return f"{int(float(x)):,}"
    except:
        return str(x)

def human_float(x, digits=2) -> str:
    """浮點數格式化。"""
    if pd.isna(x):
        return ""
    try:
        return f"{float(x):.{digits}f}"
    except:
        return str(x)

# -------------------- 建構 HTML --------------------
def build_html(report_date: str) -> str:
    """建構郵件 HTML 內容"""
    csv_path = f"reports/change_table_{report_date}.csv"
    if not Path(csv_path).exists():
        return f"<p>找不到檔案：{csv_path}</p>"
    
    df = pd.read_csv(csv_path, encoding="utf-8")
    if df.empty:
        return "<p>表格為空</p>"
    
    # 讀取補價資料，建立 price_map
    price_map = {}
    price_path = f"data/{report_date}_with_price.csv"
    if Path(price_path).exists():
        try:
            price_df = pd.read_csv(price_path, encoding="utf-8")
            # 嘗試多種可能欄位名稱
            if {'code','close'}.issubset(price_df.columns):
                price_map = dict(zip(price_df['code'].astype(str), price_df['close']))
            elif {'股票代號','收盤價'}.issubset(price_df.columns):
                price_map = dict(zip(price_df['股票代號'].astype(str), price_df['收盤價']))
            elif {'symbol','close'}.issubset(price_df.columns):
                price_map = dict(zip(price_df['symbol'].astype(str), price_df['close']))
        except Exception as e:
            print(f"[警告] 無法讀取價格資料 {price_path}: {e}")
    
    # 確保必要欄位存在
    required_cols = ['code', 'name', 'today_shares', 'yesterday_shares', 'weight_delta_pct']
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0
    
    # 計算買賣超股數
    df['net_shares'] = df['today_shares'] - df['yesterday_shares']
    
    # 補上收盤價（用 code 查 price_map）
    df['close'] = df['code'].astype(str).map(price_map).fillna('')
    
    # 依權重Δ%排序（由大到小）
    if 'weight_delta_pct' in df.columns:
        df = df.sort_values('weight_delta_pct', ascending=False).reset_index(drop=True)
    
    # 分類：首次新增、關鍵賣出、一般異動
    new_adds = df[(df['yesterday_shares'] == 0) & (df['today_shares'] > 0)]
    key_sells = df[(df['yesterday_shares'] > 0) & (df['today_shares'] == 0)]
    others = df[~((df['yesterday_shares'] == 0) & (df['today_shares'] > 0)) & 
               ~((df['yesterday_shares'] > 0) & (df['today_shares'] == 0))]
    
    def make_table(data, title):
        if data.empty:
            return f"<h3>{title}</h3><p>無</p>"
        rows = []
        for _, row in data.iterrows():
            close_val = row['close']
            close_str = human_float(close_val, 2) if (close_val != '' and pd.notna(close_val)) else ''
            rows.append(f"""
            <tr>
                <td>{row['code']}</td>
                <td>{row.get('name','')}</td>
                <td>{human_int(row['today_shares'])}</td>
                <td>{human_int(row['yesterday_shares'])}</td>
                <td>{human_int(row['net_shares'])}</td>
                <td>{close_str}</td>
                <td>{human_float(row.get('weight_delta_pct', ''), 2)}%</td>
            </tr>
            """)
        table_html = f"""
        <h3>{title}</h3>
        <table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">
            <tr>
                <th>代號</th>
                <th>股票名稱</th>
                <th>今日股數</th>
                <th>昨日股數</th>
                <th>買賣超股數</th>
                <th>收盤價</th>
                <th>權重Δ%</th>
            </tr>
            {''.join(rows)}
        </table>
        """
        return table_html
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset=\"UTF-8\">
        <title>00981A Daily Report - {report_date}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: right; }}
            th {{ background-color: #f2f2f2; text-align: center; }}
            td:nth-child(1), td:nth-child(2) {{ text-align: left; }}
            h1 {{ color: #333; }}
            h3 {{ color: #666; margin-top: 30px; }}
        </style>
    </head>
    <body>
        <h1>00981A Daily Tracker - {report_date}</h1>
        {make_table(new_adds, '首次新增持股')}
        {make_table(key_sells, '關鍵賣出')}
        {make_table(others, '一般異動')}
        <p><small>※ 買賣超股數 = 今日股數 - 昨日股數</small></p>
        <p><small>※ 收盤價資料來源：{price_path if Path(price_path).exists() else '無價格資料檔案'}</small></p>
    </body>
    </html>
    """
    return html_content

# -------------------- SMTP 發送 --------------------
def send_with_smtp(html: str):
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    username = os.getenv("EMAIL_USERNAME")
    password = os.getenv("EMAIL_PASSWORD")
    to_email = os.getenv("EMAIL_TO")
    if not all([username, password, to_email]):
        raise RuntimeError("缺少 EMAIL_USERNAME / EMAIL_PASSWORD / EMAIL_TO")
    msg = MIMEMultipart()
    msg['From'] = username
    msg['To'] = to_email
    msg['Subject'] = "00981A Daily Tracker"
    msg.attach(MIMEText(html, 'html', 'utf-8'))
    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls(context=context)
        server.login(username, password)
        server.sendmail(username, to_email, msg.as_string())

# -------------------- SendGrid 發送 --------------------
def send_with_sendgrid(html: str):
    key = os.getenv("SENDGRID_API_KEY")
    to  = os.getenv("EMAIL_TO")
    user = os.getenv("EMAIL_USERNAME") or "report@bot.local"
    if not (key and to):
        raise RuntimeError("缺少 SENDGRID_API_KEY / EMAIL_TO")
    import json, requests
    payload = {
        "personalizations": [{"to": [{"email": to}]}],
        "from": {"email": user, "name": "00981A Daily"},
        "subject": "00981A Daily Tracker",
        "content": [{"type": "text/html", "value": html}],
    }
    r = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        data=json.dumps(payload).encode("utf-8"),
        timeout=30,
    )
    if r.status_code >= 300:
        raise RuntimeError(f"SendGrid error: {r.status_code} {r.text[:200]}")

# -------------------- 入口 --------------------
def main():
    report_date = get_report_date()
    if not report_date:
        raise SystemExit("REPORT_DATE 未設定")
    html = build_html(report_date)
    try:
        send_with_smtp(html)
        print("[mail] SMTP sent")
    except Exception as e:
        print(f"[mail] SMTP failed → fallback: {e}")
        send_with_sendgrid(html)
        print("[mail] SendGrid sent")

if __name__ == "__main__":
    main()
