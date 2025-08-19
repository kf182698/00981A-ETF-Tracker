# build_change_table.py — 產生每日變化表 + 報表 (D1/D5/首次新增/賣出警示)
import os
import re
import glob
import hashlib
from pathlib import Path
import pandas as pd  # ← 只在最上方匯入一次

# ===== 共用設定（若無 config.py 則使用預設） =====
try:
    from config import (
        REPORT_DIR,
        NEW_WEIGHT_MIN,        # 首次新增持股權重門檻（%）
        THRESH_UPDOWN_EPS,     # D1 噪音門檻（百分點，用於摘要顯示）
        SELL_ALERT_THRESHOLD,  # 賣出警示門檻（%）
        PCT_DECIMALS,          # 百分比小數位
    )
except Exception:
    REPORT_DIR = "reports"
    NEW_WEIGHT_MIN = 0.5
    THRESH_UPDOWN_EPS = 0.01
    SELL_ALERT_THRESHOLD = 0.10
    PCT_DECIMALS = 2

DATA_DIR = "data"
Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)

# ===== 小工具 =====
def _normalize_cols(cols):
    return [str(c).strip().replace("　", "").replace("\u3000", "") for c in cols]

ALIAS = {
    "股票代號": ["股票代號", "證券代號", "代號", "證券代號/代碼", "Code"],
    "股票名稱": ["股票名稱", "證券名稱", "名稱", "Name"],
    "股數":     ["股數", "持股股數", "持有股數", "Shares"],
    "持股權重": ["持股權重", "持股比例", "權重", "占比", "占比(%)", "比重(%)", "Weight"],
    "收盤價":   ["收盤價", "Close", "Price"],
}

def _pick(df: pd.DataFrame, key: str):
    for cand in ALIAS[key]:
        if cand in df.columns:
            return cand
    return None

def _fmt_pct(x, dec=PCT_DECIMALS):
    try:
        return f"{float(x):.{dec}f}%"
    except Exception:
        return "-"

def _fmt_int(x):
    try:
        return f"{int(x):,}"
    except Exception:
        return "-"

def _list_dates_sorted():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    dates = []
    for f in files:
        m = re.search(r"(\d{4}-\d{2}-\d{2})\.csv$", f)
        if m:
            dates.append(m.group(1))
    return sorted(set(dates))

def _read_day(date_str):
    """讀取某日 CSV，回傳標準欄位：股票代號/股票名稱/股數/持股權重/(可選)收盤價"""
    p = os.path.join(DATA_DIR, f"{date_str}.csv")
    if not os.path.exists(p):
        return None
    df = pd.read_csv(p)
    df.columns = _normalize_cols(df.columns)

    c_code   = _pick(df, "股票代號")
    c_name   = _pick(df, "股票名稱")
    c_shares = _pick(df, "股數")
    c_weight = _pick(df, "持股權重")
    c_close  = _pick(df, "收盤價")  # optional

    if not all([c_code, c_name, c_shares, c_weight]):
        return None

    out = df[[c_code, c_name, c_shares, c_weight]].copy()
    out.columns = ["股票代號", "股票名稱", "股數", "持股權重"]

    # 代碼/名稱一律字串（避免 int/str 對不上）
    out["股票代號"] = df[c_code].astype(str).str.strip()
    out["股票名稱"] = df[c_name].astype(str).str.strip()

    # 數值清洗
    out["股數"] = out["股數"].astype(str).str.replace(",", "", regex=False)
    out["持股權重"] = out["持股權重"].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False)
    out["股數"] = pd.to_numeric(out["股數"], errors="coerce").fillna(0).astype(int)
    out["持股權重"] = pd.to_numeric(out["持股權重"], errors="coerce").fillna(0.0)

    if c_close:
        out["收盤價"] = pd.to_numeric(df[c_close], errors="coerce")

    return out

def _md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _csv_path(d):
    return os.path.join(DATA_DIR, f"{d}.csv")

# ===== 主流程 =====
def main():
    dates = _list_dates_sorted()
    if not dates:
        print("No data/*.csv found.")
        return

    # 嚴格使用「前一個不同日期」做比較
    today = max(dates)  # 最新日期（YYYY-MM-DD）
    prev_candidates = [d for d in dates if d < today]
    prev = max(prev_candidates) if prev_candidates else None

    print(f"[build] pick dates -> today={today}, prev={prev}")

    df_t = _read_day(today)
    if df_t is None:
        print(f"[build] {today}.csv 格式不符或缺欄位")
        return

    if prev:
        df_y = _read_day(prev)
    else:
        df_y = pd.DataFrame(columns=["股票代號","股票名稱","股數","持股權重","收盤價"])

    # Log 檢查：today/prev 檔案與 MD5
    p_today = _csv_path(today)
    p_prev  = _csv_path(prev) if prev else None
    if os.path.exists(p_today):
        print(f"[build] today_path={p_today}, md5={_md5(p_today)}")
    if p_prev and os.path.exists(p_prev):
        print(f"[build] prev_path={p_prev}, md5={_md5(p_prev)}")

    if not prev or (p_prev and os.path.exists(p_prev) and _md5(p_prev) == _md5(p_today)):
        print("[build][WARN] prev is missing or equals today snapshot. "
              "Comparisons may show 0. Ensure data/ has previous trading day.")

    # 保險：兩邊代碼都轉字串
    df_t["股票代號"] = df_t["股票代號"].astype(str).str.strip()
    df_y["股票代號"] = df_y["股票代號"].astype(str).str.strip()

    # ===== 合併今日/昨日 =====
    key = ["股票代號"]
    df_merge = pd.merge(df_t, df_y, on=key, how="outer", suffixes=("_今日", "_昨日"))
    # 名稱以今日優先
    df_merge["股票名稱"] = df_merge["股票名稱_今日"].fillna(df_merge["股票名稱_昨日"])

    # 數值補齊
    for col in ["股數_今日","股數_昨日","持股權重_今日","持股權重_昨日"]:
        if col in df_merge.columns:
            df_merge[col] = pd.to_numeric(df_merge[col], errors="coerce").fillna(0)

    # 指標計算
    df_merge["買賣超股數"] = (df_merge["股數_今日"] - df_merge["股數_昨日"]).astype(int)
    df_merge["昨日權重%"]  = df_merge["持股權重_昨日"]
    df_merge["今日權重%"]  = df_merge["持股權重_今日"]
    df_merge["Δ%"]        = (df_merge["今日權重%"] - df_merge["昨日權重%"]).round(PCT_DECIMALS)

    # 排序：今日權重 desc、代碼 asc
    df_merge = df_merge.sort_values(["今日權重%","股票代號"], ascending=[False, True])

    # ===== 每日持股變化追蹤表（人眼友善版 + meta 列） =====
    out_cols = [
        "股票代號", "股票名稱",
        "股數_今日", "今日權重%",
        "股數_昨日", "昨日權重%",
        "買賣超股數", "Δ%"
    ]
    df_table = df_merge[out_cols].copy()

    df_human = df_table.copy()
    df_human["股數_今日"] = df_human["股數_今日"].map(_fmt_int)
    df_human["股數_昨日"] = df_human["股數_昨日"].map(_fmt_int)
    df_human["昨日權重%"] = df_human["昨日權重%"].map(_fmt_pct)
    df_human["今日權重%"] = df_human["今日權重%"].map(_fmt_pct)
    df_human["Δ%"]       = df_human["Δ%"].map(lambda x: _fmt_pct(x))

    # 在 CSV 最前面加 meta 兩列（方便在 Email 快速確認比對基準）
    meta = pd.DataFrame({
        "股票代號": [f"BASE_TODAY={today}"],
        "股票名稱": [f"BASE_PREV={prev or 'N/A'}"],
    })
    df_out_with_meta = pd.concat([meta, df_human], ignore_index=True)

    csv_out  = os.path.join(REPORT_DIR, f"holdings_change_table_{today}.csv")
    xlsx_out = os.path.join(REPORT_DIR, f"holdings_change_table_{today}.xlsx")
    df_out_with_meta.to_csv(csv_out, index=False, encoding="utf-8-sig")
    try:
        with pd.ExcelWriter(xlsx_out, engine="openpyxl") as w:
            df_human.to_excel(w, index=False, sheet_name="ChangeTable")
    except Exception as e:
        print("[build] Excel export failed:", e)
    print("Saved:", csv_out)
    print("Saved:", xlsx_out)

    # ===== D1 up/down（數值版，供圖表/摘要） =====
    d1_out = df_table[["股票代號","股票名稱","昨日權重%","今日權重%","Δ%"]].copy()
    d1_path = os.path.join(REPORT_DIR, f"up_down_today_{today}.csv")
    d1_out.to_csv(d1_path, index=False, encoding="utf-8-sig")
    print("Saved:", d1_path)

    # ===== 首次新增持股（昨日 ~ 0；今日 >= 門檻） =====
    EPS = 1e-6
    new_mask = (df_merge["昨日權重%"].fillna(0) <= EPS) & (df_merge["今日權重%"] >= float(NEW_WEIGHT_MIN))
    df_new = df_merge.loc[new_mask, ["股票代號","股票名稱","今日權重%"]].sort_values("今日權重%", ascending=False)
    new_path = os.path.join(REPORT_DIR, f"new_gt_{str(NEW_WEIGHT_MIN).replace('.','p')}_{today}.csv")
    df_new.to_csv(new_path, index=False, encoding="utf-8-sig")
    print("Saved:", new_path)

    # ===== D5（回溯 5 份快照；不足就近回填；只在今日持股集合上報）=====
    back_dates = [d for d in dates if d <= today]
    back_dates = back_dates[-5:]  # 取最後 5 份（含 today）
    while len(back_dates) < 5 and back_dates:
        back_dates.insert(0, back_dates[0])

    panel = {}
    for d in back_dates:
        df = _read_day(d)
        if df is None:
            prev_key = sorted(panel.keys())[-1] if panel else None
            panel[d] = panel[prev_key].copy() if prev_key else {}
            continue
        panel[d] = dict(zip(df["股票代號"], df["持股權重"]))

    if panel:
        mat = pd.DataFrame(panel).T  # index=date, columns=code
        mat = mat.fillna(method="ffill").fillna(0.0)

        try:
            w_today = mat.loc[today]
        except KeyError:
            w_today = mat.iloc[-1]
        w_yest = mat.iloc[-2] if len(mat) >= 2 else mat.iloc[-1]
        w_t5   = mat.iloc[0]

        today_codes = set(df_t["股票代號"].astype(str))
        df_d5 = pd.DataFrame({
            "今日%":  w_today.reindex(today_codes).fillna(0.0),
            "昨日%":  w_yest.reindex(today_codes).fillna(0.0
