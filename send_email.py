#!/usr/bin/env python3
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
    try:
        return f"{int(float(x)):,}"
    except Exception:
        return "0"


def human_float(x, digits=2) -> str:
    try:
        return f"{float(x):.{digits}f}"
    except Exception:
        return "0.00"


# -------------------- 郵件內容 --------------------
def _build_cost_basis_section(report_date: str, price_map: dict) -> str:
    """讀取 data/cost_basis.csv，產出成本追蹤 HTML 段落。找不到檔案則回傳空字串。"""
    cost_path = Path("data/cost_basis.csv")
    if not cost_path.exists():
        return ""
    try:
        df = pd.read_csv(cost_path, encoding="utf-8-sig")
        df.columns = [str(c).replace("﻿", "").strip() for c in df.columns]
        if "股票代號" not in df.columns or "成本市值" not in df.columns:
            return ""
        df["股數"] = pd.to_numeric(df.get("股數", 0), errors="coerce").fillna(0).astype(int)
        df["成本市值"] = pd.to_numeric(df["成本市值"], errors="coerce").fillna(0.0)
        # 只顯示仍持有的股票（股數 > 0）
        df = df[df["股數"] > 0].copy()
        if df.empty:
            return ""
    except Exception:
        return ""

    rows_html = []
    total_cost = 0.0
    total_market = 0.0

    for _, r in df.sort_values("成本市值", ascending=False).iterrows():
        code = str(r.get("股票代號", ""))
        name = str(r.get("股票名稱", ""))
        shares = int(r["股數"])
        cost_val = float(r["成本市值"])
        avg_cost = cost_val / shares if shares > 0 else 0.0

        price = price_map.get(code)
        if price is not None and price > 0:
            market_val = price * shares
            pnl = market_val - cost_val
            roi = pnl / cost_val if cost_val > 0 else 0.0
            market_str = f"{market_val:,.0f}"
            pnl_str = f"{pnl:+,.0f}"
            roi_str = f"{roi:+.2%}"
            pnl_cls = "pos" if pnl >= 0 else "neg"
            total_market += market_val
        else:
            market_str = "—"
            pnl_str = "—"
            roi_str = "—"
            pnl_cls = ""

        total_cost += cost_val
        rows_html.append(
            f"<tr><td>{code}</td><td>{name}</td>"
            f"<td>{shares:,}</td><td>{avg_cost:.2f}</td>"
            f"<td>{cost_val:,.0f}</td><td>{market_str}</td>"
            f"<td class='{pnl_cls}'>{pnl_str}</td>"
            f"<td class='{pnl_cls}'>{roi_str}</td></tr>"
        )

    total_pnl = total_market - total_cost if total_market > 0 else None
    total_pnl_str = f"{total_pnl:+,.0f}" if total_pnl is not None else "—"
    total_roi_str = f"{total_pnl/total_cost:+.2%}" if (total_pnl is not None and total_cost > 0) else "—"
    total_market_str = f"{total_market:,.0f}" if total_market > 0 else "—"
    pnl_cls_total = ("pos" if total_pnl >= 0 else "neg") if total_pnl is not None else ""

    return f"""
      <div class="sec">💰 成本追蹤（{report_date}）</div>
      <div style="margin-bottom:6px;font-size:13px;">
        總投資成本：<b>{total_cost:,.0f}</b> 元
        今日持倉市值：<b>{total_market_str}</b> 元
        浮動損益：<b class="{pnl_cls_total}">{total_pnl_str}</b> 元
        報酬率：<b class="{pnl_cls_total}">{total_roi_str}</b>
      </div>
      <table>
        <thead>
          <tr>
            <th>股票代號</th><th>股票名稱</th>
            <th>持有股數</th><th>平均成本(元)</th>
            <th>成本市值(元)</th><th>今日市值(元)</th>
            <th>浮動損益(元)</th><th>報酬率</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows_html)}
        </tbody>
      </table>
    """


def build_html(report_date: str) -> str:
    change_csv = Path("reports") / f"change_table_{report_date}.csv"
    if not change_csv.exists():
        raise SystemExit(f"缺少 {change_csv}，請先執行 build_change_table.py")

    df = pd.read_csv(change_csv, encoding="utf-8-sig")

    # 嘗試讀取當日收盤價檔，方便郵件內容顯示最新收盤價。若檔案不存在或格式不符則略過。
    price_map = {}
    price_csv = Path("prices") / f"{report_date}.csv"
    if price_csv.exists():
        try:
            pf = pd.read_csv(price_csv, encoding="utf-8-sig", dtype=str)
            # 去除欄名 BOM 與空白
            pf.columns = [str(c).replace("\ufeff", "").strip() for c in pf.columns]
            # 尋找股票代號與收盤價欄位名稱
            code_col = None
            price_col = None
            for c in ["股票代號", "代號", "證券代號", "code", "Code"]:
                if c in pf.columns:
                    code_col = c
                    break
            if code_col is None:
                code_col = pf.columns[0]
            for c in ["收盤價", "收盤", "Close", "Closing Price"]:
                if c in pf.columns:
                    price_col = c
                    break
            if price_col is None:
                price_col = pf.columns[1] if len(pf.columns) > 1 else pf.columns[0]
            for _, row in pf.iterrows():
                code = str(row[code_col]).strip()
                val = str(row[price_col]).strip()
                if val:
                    try:
                        price_map[code] = float(val)
                    except Exception:
                        pass
        except Exception:
            price_map = {}

    # 數字欄位保險轉型
    for c in ["今日股數", "昨日股數"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        else:
            df[c] = 0

    for c in ["今日權重%", "昨日權重%", "權重Δ%"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        else:
            df[c] = 0.0

    # ✅ 買賣超股數：今日股數 - 昨日股數（即使原檔有，也以這個公式重算一次）
    df["買賣超股數"] = (df["今日股數"] - df["昨日股數"]).astype(int)

    # 依「權重Δ%」由大到小排序
    df_sorted = df.sort_values("權重Δ%", ascending=False).reset_index(drop=True)

    # 找基期日期
    prev_date = find_prev_snapshot(report_date) or "N/A"

    # 摘要資料（前十大權重、最大權重）
    top10_sum = df_sorted["今日權重%"].nlargest(10).sum()
    max_row = df_sorted.nlargest(1, "今日權重%")
    if not max_row.empty:
        max_code = str(max_row.iloc[0]["股票代號"])
        max_name = str(max_row.iloc[0].get("股票名稱", ""))
        max_weight = float(max_row.iloc[0]["今日權重%"])
        max_text = f"{max_code} {max_name}（{max_weight:.2f}%）"
    else:
        max_text = "—"

    # 首次新增持股 / 大量減持近出清 / 剃除持股清單
    first_buys = df_sorted.loc[(df_sorted["昨日股數"] == 0) & (df_sorted["今日股數"] > 0)]
    heavy_trim = df_sorted.loc[(df_sorted["昨日股數"] >= 2001) & (df_sorted["今日股數"] <= 2000)]
    trimmed_positions = df_sorted.loc[(df_sorted["昨日股數"] > 0) & (df_sorted["今日股數"] == 0)]

    def list_codes_names(sub: pd.DataFrame) -> str:
        if sub.empty:
            return "無"
        items = [f"{str(r['股票代號'])} {str(r.get('股票名稱',''))}".strip()
                 for _, r in sub.sort_values("今日權重%", ascending=False).iterrows()]
        return "、".join(items)

    first_buys_str = list_codes_names(first_buys)
    heavy_trim_str = list_codes_names(heavy_trim)
    trimmed_positions_str = list_codes_names(trimmed_positions)

    # 欄名顯示（帶日期）
    col_today_w  = f"今日權重%（{report_date}）"
    col_yestd_w  = f"昨日權重%（{prev_date}）"
    col_today_sh = f"股數（{report_date}）"
    col_yestd_sh = f"股數（{prev_date}）"

    # HTML 樣式（微軟正黑體）
    style = """
    <style>
      body { font-family: 'Microsoft JhengHei','PingFang TC','Noto Sans CJK TC',Arial,sans-serif; }
      .title { font-size: 22px; font-weight: 800; margin-bottom: 12px; }
      .meta  { margin: 8px 0 16px 0; }
      .sec   { margin: 14px 0 8px 0; font-weight:700; }
      table { border-collapse: collapse; width: 100%; font-size: 13px; }
      th, td { border-bottom: 1px solid #e5e7eb; text-align: right; padding: 6px 8px; }
      th:nth-child(1), td:nth-child(1),
      th:nth-child(2), td:nth-child(2) { text-align: left; }
      th { background: #f9fafb; }
      .pos { color: #16a34a; font-weight: 600; }
      .neg { color: #dc2626; font-weight: 600; }
      .note { color:#6b7280; font-size:12px; margin-top:12px;}
    </style>
    """

    # 表格列（新增「買賣超股數」欄位，並以正負色彩標示）
    rows = []
    for _, r in df_sorted.iterrows():
        code = str(r.get("股票代號", ""))
        name = str(r.get("股票名稱", ""))
        # 讀取此股票當天收盤價（若有）
        # 優先從 price_map 取得收盤價；若無則回退至 change_table 的「今日收盤價」欄位
        price_val = price_map.get(code)
        if price_val is None:
            # fallback: 試讀 change_table 中的「今日收盤價」欄位
            try:
                val = r.get("今日收盤價", None)
                # 如果值存在且非缺失，嘗試轉為 float
                if val not in (None, "") and not pd.isna(val):
                    price_val = float(val)
            except Exception:
                price_val = None
        if price_val is not None:
            close = f"{price_val:.2f}"
        else:
            close = ""
        s_t = human_int(r["今日股數"])
        s_y = human_int(r["昨日股數"])
        w_t = f"{human_float(r['今日權重%']):s}%"
        w_y = f"{human_float(r['昨日權重%']):s}%"
        delta_shares = int(r["買賣超股數"])
        delta_shares_s = f"{delta_shares:+,}"
        dlt = float(r["權重Δ%"])
        dlt_s = f"{dlt:+.2f}%"
        cls_sh = "pos" if delta_shares > 0 else "neg" if delta_shares < 0 else ""
        cls_w  = "pos" if dlt > 0 else "neg" if dlt < 0 else ""
        rows.append(
            f"<tr><td>{code}</td><td>{name}</td><td>{close}</td>"
            f"<td>{s_t}</td><td>{w_t}</td>"
            f"<td>{s_y}</td><td>{w_y}</td>"
            f"<td class='{cls_sh}'>{delta_shares_s}</td>"
            f"<td class='{cls_w}'>{dlt_s}</td></tr>"
        )

    cost_basis_html = _build_cost_basis_section(report_date, price_map)

    html = f"""
    <html><head>{style}</head><body>
      <div class="title">00981A 今日追蹤摘要（{report_date}）</div>
      <div class="meta">
        ▶ 前十大權重合計：{top10_sum:.2f}%　▶ 最大權重：{max_text}　▶ 比較基期（昨）：{prev_date}
      </div>

      <div class="sec">📌 首次新增持股</div>
      <div>{first_buys_str}</div>

      <div class="sec">📌 大量減持近出清</div>
      <div>{heavy_trim_str}</div>

      <div class="sec">📌 剃除持股</div>
      <div>{trimmed_positions_str}</div>

      {cost_basis_html}

      <div class="sec">📊 每日持股變化追蹤表（依「權重Δ%」由大到小）</div>
      <table>
        <thead>
          <tr>
            <th>股票代號</th><th>股票名稱</th><th>收盤價</th>
            <th>{col_today_sh}</th><th>{col_today_w}</th>
            <th>{col_yestd_sh}</th><th>{col_yestd_w}</th>
            <th>買賣超股數</th><th>權重 Δ%</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows)}
        </tbody>
      </table>

      <div class="note">
        本信件為自動產生，字型統一使用微軟正黑體。若您誤收此信或不需再接收，煩請直接回覆告知；
        本郵件僅供研究追蹤用途，非投資建議，謝謝。
      </div>
    </body></html>
    """
    return html


# -------------------- 寄信（SMTP/SendGrid） --------------------
def send_with_smtp(html: str):
    user = os.getenv("EMAIL_USERNAME")
    pwd  = os.getenv("EMAIL_PASSWORD")
    to   = os.getenv("EMAIL_TO")
    if not (user and pwd and to):
        raise RuntimeError("缺少 EMAIL_USERNAME / EMAIL_PASSWORD / EMAIL_TO")

    msg = MIMEMultipart("alternative")
    msg["From"] = user
    msg["To"] = to
    msg["Subject"] = "00981A Daily Tracker"

    msg.attach(MIMEText("本郵件為 HTML 版，請使用支援 HTML 的郵件客戶端檢視。", "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as server:
        server.login(user, pwd)
        server.sendmail(user, [to], msg.as_string())


def send_with_sendgrid(html: str):
    key = os.getenv("SENDGRID_API_KEY")
    to  = os.getenv("EMAIL_TO")
    user = os.getenv("EMAIL_USERNAME") or "report@bot.local"
    if not (key and to):
        raise RuntimeError("缺少 SENDGRID_API_KEY / EMAIL_TO")

    import json, requests  # 輕量直接呼叫 API
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


def main():
    report_date = get_report_date()
    if not report_date:
        raise SystemExit("REPORT_DATE 未設定")

    html = build_html(report_date)

    # 主送 SMTP，失敗即切換 SendGrid
    try:
        send_with_smtp(html)
        print("[mail] SMTP sent")
    except Exception as e:
        print(f"[mail] SMTP failed → fallback: {e}")
        send_with_sendgrid(html)
        print("[mail] SendGrid sent")


if __name__ == "__main__":
    main()