# build_change_table.py — 產生每日變化表 + 報表 (D1/D5/新增/賣出警示)
import os
import re
import glob
from datetime import datetime
from pathlib import Path
import pandas as pd

# --- 讀取共用設定（若無 config.py 則使用預設） ---
try:
    from config import (
        REPORT_DIR,
        NEW_WEIGHT_MIN,
        THRESH_UPDOWN_EPS,
        SELL_ALERT_THRESHOLD,
        PCT_DECIMALS,
    )
except Exception:
    REPORT_DIR = "reports"
    NEW_WEIGHT_MIN = 0.5          # 首次新增持股門檻（%）
    THRESH_UPDOWN_EPS = 0.01      # D1 噪音門檻（百分點）
    SELL_ALERT_THRESHOLD = 0.10   # 關鍵賣出警示閾值（%）
    PCT_DECIMALS = 2              # 百分比小數位

DATA_DIR = "data"
Path(REPORT_DIR).mkdir(parents=True, exist_ok=True)

# ---------- 小工具 ----------
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

    # ---- 重要：代碼/名稱轉字串，避免 int/str 對不上 ----
    out["股票代號"] = df[c_code].astype(str).str.strip()
    out["股票名稱"] = df[c_name].astype(str).str.strip()

    # 數值轉換
    out["股數"] = (
        out["股數"].astype(str).str.replace(",", "", regex=False)
    )
    out["持股權重"] = (
        out["持股權重"].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False)
    )
    out["股數"] = pd.to_numeric(out["股數"], errors="coerce").fillna(0).astype(int)
    out["持股權重"] = pd.to_numeric(out["持股權重"], errors="coerce").fillna(0.0)

    if c_close:
        out["收盤價"] = pd.to_numeric(df[c_close], errors="coerce")

    return out

# ---------- 主流程 ----------
def main():
    dates = _list_dates_sorted()
    if not dates:
        print("No data/*.csv found.")
        return
    today = dates[-1]
    prev  = dates[-2] if len(dates) >= 2 else None

    df_t = _read_day(today)
    df_y = _read_day(prev) if prev else pd.DataFrame(columns=["股票代號","股票名稱","股數","持股權重","收盤價"])

    if df_t is None:
        print(f"{today}.csv 格式不符或缺欄位")
        return
    if df_y is None:
        df_y = pd.DataFrame(columns=["股票代號","股票名稱","股數","持股權重","收盤價"])

    # ---- 保險：兩邊代碼都轉字串 ----
    df_t["股票代號"] = df_t["股票代號"].astype(str).str.strip()
    df_y["股票代號"] = df_y["股票代號"].astype(str).str.strip()

    # ====== 合併今日/昨日 ======
    key = ["股票代號"]
    df_merge = pd.merge(df_t, df_y, on=key, how="outer", suffixes=("_今日", "_昨日"))
    # 名稱以今日優先，無則用昨日
    df_merge["股票名稱"] = df_merge["股票名稱_今日"].fillna(df_merge["股票名稱_昨日"])

    # 填空值
    for col in ["股數_今日","股數_昨日","持股權重_今日","持股權重_昨日"]:
        if col in df_merge.columns:
            df_merge[col] = pd.to_numeric(df_merge[col], errors="coerce").fillna(0)

    # 計算欄位
    df_merge["買賣超股數"] = (df_merge["股數_今日"] - df_merge["股數_昨日"]).astype(int)
    df_merge["昨日權重%"]  = df_merge["持股權重_昨日"]
    df_merge["今日權重%"]  = df_merge["持股權重_今日"]
    df_merge["Δ%"]        = (df_merge["今日權重%"] - df_merge["昨日權重%"]).round(PCT_DECIMALS)

    # 排序：預設依照「今日權重%」大到小
    df_merge = df_merge.sort_values(["今日權重%","股票代號"], ascending=[False, True])

    # ====== 輸出「每日持股變化追蹤表」 ======
    out_cols = [
        "股票代號", "股票名稱",
        "股數_今日", "今日權重%",
        "股數_昨日", "昨日權重%",
        "買賣超股數", "Δ%"
    ]
    df_table = df_merge[out_cols].copy()

    # for email/table：人眼友善格式
    df_human = df_table.copy()
    df_human["股數_今日"]  = df_human["股數_今日"].map(_fmt_int)
    df_human["股數_昨日"]  = df_human["股數_昨日"].map(_fmt_int)
    df_human["昨日權重%"]  = df_human["昨日權重%"].map(_fmt_pct)
    df_human["今日權重%"]  = df_human["今日權重%"].map(_fmt_pct)
    df_human["Δ%"]        = df_human["Δ%"].map(lambda x: _fmt_pct(x))

    csv_out  = os.path.join(REPORT_DIR, f"holdings_change_table_{today}.csv")
    xlsx_out = os.path.join(REPORT_DIR, f"holdings_change_table_{today}.xlsx")
    df_human.to_csv(csv_out, index=False, encoding="utf-8-sig")
    with pd.ExcelWriter(xlsx_out, engine="openpyxl") as w:
        df_human.to_excel(w, index=False, sheet_name="ChangeTable")
    print("Saved:", csv_out)
    print("Saved:", xlsx_out)

    # ====== D1 up/down (數值版，供圖表/摘要) ======
    d1 = df_table.copy()
    d1_out = d1[["股票代號","股票名稱","昨日權重%","今日權重%","Δ%"]].copy()
    d1_out.to_csv(os.path.join(REPORT_DIR, f"up_down_today_{today}.csv"), index=False, encoding="utf-8-sig")

    # ====== 首次新增持股（昨日 ~ 0；今日 >= 門檻）=====
    EPS = 1e-6  # 避免極小殘值影響判斷
    new_mask = (df_merge["昨日權重%"].fillna(0) <= EPS) & (df_merge["今日權重%"] >= float(NEW_WEIGHT_MIN))
    df_new = df_merge.loc[new_mask, ["股票代號","股票名稱","今日權重%"]].sort_values("今日權重%", ascending=False)
    df_new.to_csv(
        os.path.join(REPORT_DIR, f"new_gt_{str(NEW_WEIGHT_MIN).replace('.','p')}_{today}.csv"),
        index=False, encoding="utf-8-sig"
    )

    # ====== D5（近五份快照；不足就近回填）=====
    # 取包含 today 在內，往前最多 5 份
    dates_all = dates[:]  # 已排序
    if today not in dates_all:
        dates_all.append(today)
        dates_all = sorted(set(dates_all))
    idx = dates_all.index(today)
    back = dates_all[max(0, idx-4): idx+1]  # 最多 5 份（含今日）

    # 若不足 5 份，用最早那份重複回填至 5 份
    while len(back) < 5 and back:
        back.insert(0, back[0])

    # 建 panel：各日 code -> weight
    panel = {}
    for d in back:
        df = _read_day(d)
        if df is None:
            # 回填：用上一個可用
            prev_key = sorted(panel.keys())[-1] if panel else None
            panel[d] = panel[prev_key].copy() if prev_key else {}
            continue
        panel[d] = dict(zip(df["股票代號"], df["持股權重"]))

    if panel:
        mat = pd.DataFrame(panel).T  # index: date, columns: code
        mat = mat.fillna(method="ffill").fillna(0.0)
        # 取得今日/昨日/T-5
        try:
            w_today = mat.loc[today]
        except KeyError:
            w_today = mat.iloc[-1]
        w_yest = mat.iloc[-2] if len(mat) >= 2 else mat.iloc[-1]
        w_t5   = mat.iloc[0]

        # 僅在「今日持股」集合上計算，避免移除檔的 0→X 噪音
        today_codes = set(df_t["股票代號"].astype(str))
        df_d5 = pd.DataFrame({
            "今日%":  w_today.reindex(today_codes).fillna(0.0),
            "昨日%":  w_yest.reindex(today_codes).fillna(0.0),
            "T-5日%": w_t5.reindex(today_codes).fillna(0.0),
        })
        name_map = dict(zip(df_t["股票代號"].astype(str), df_t["股票名稱"]))
        df_d5["股票代號"] = df_d5.index.astype(str)
        df_d5["股票名稱"] = df_d5["股票代號"].map(name_map).fillna("")
        df_d5["D1Δ%"] = (df_d5["今日%"] - df_d5["昨日%"]).round(PCT_DECIMALS)
        df_d5["D5Δ%"] = (df_d5["今日%"] - df_d5["T-5日%"]).round(PCT_DECIMALS)
        df_d5 = df_d5[["股票代號","股票名稱","今日%","昨日%","D1Δ%","T-5日%","D5Δ%"]]
        df_d5.to_csv(os.path.join(REPORT_DIR, f"weights_chg_5d_{today}.csv"), index=False, encoding="utf-8-sig")
        print("Saved:", os.path.join(REPORT_DIR, f"weights_chg_5d_{today}.csv"))
    else:
        print("Skip D5: panel empty")

    # ====== 關鍵賣出警示（今日 ≤ 閾值；昨日 > 閾值；Δ<0）=====
    sell_mask = (df_merge["今日權重%"] <= float(SELL_ALERT_THRESHOLD)) & \
                (df_merge["昨日權重%"] >  float(SELL_ALERT_THRESHOLD)) & \
                (df_merge["Δ%"] < 0)
    df_sell = df_merge.loc[sell_mask, ["股票代號","股票名稱","昨日權重%","今日權重%","Δ%"]].copy()
    df_sell = df_sell.sort_values("Δ%")
    df_sell.to_csv(os.path.join(REPORT_DIR, f"sell_alerts_{today}.csv"), index=False, encoding="utf-8-sig")
    print("Saved:", os.path.join(REPORT_DIR, f"sell_alerts_{today}.csv"))

    print("Reports saved to:", REPORT_DIR)

if __name__ == "__main__":
    main()
