import streamlit as st
import pandas as pd
import glob
from pathlib import Path
import google.generativeai as genai
import os

# 1. 初始化頁面與 API 金鑰
st.set_page_config(page_title="00981A 分析 APP", layout="wide")
st.title("📊 00981A ETF 持股變化 AI 篩選 APP")

# 嘗試從 環境變數 或 Streamlit Secrets 讀取 API Key
api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    try:
        api_key = st.secrets["GEMINI_API_KEY"]
    except Exception:
        pass

if not api_key:
    api_key_input = st.sidebar.text_input("輸入 Google AI Studio 取得的 Gemini API Key", type="password")
    if api_key_input:
        api_key = api_key_input

if api_key:
    genai.configure(api_key=api_key)
    has_api = True
else:
    has_api = False
    st.sidebar.warning("請先輸入 API Key 才能啟用 AI 分析。")

# 2. 爬取本地端 CSV 資料 (包含 data_snapshots 與 data)
@st.cache_data
def load_available_dates():
    files = glob.glob("data_snapshots/*.csv") + glob.glob("data/*.csv")
    dates = sorted(list(set([Path(f).stem for f in files if Path(f).stem.replace("-", "").isdigit()])))
    return dates

@st.cache_data
def load_data(date_str):
    files = glob.glob(f"data_snapshots/{date_str}.csv") + glob.glob(f"data/{date_str}.csv")
    if not files: return pd.DataFrame()
    df = pd.read_csv(files[0], encoding="utf-8-sig")

    rename_map = {}
    for c in df.columns:
        s = str(c)
        if any(k in s for k in ["股票代號","證券代號","代碼","代號"]): rename_map[c] = "股票代號"
        elif any(k in s for k in ["股票名稱","名稱"]): rename_map[c] = "股票名稱"
        elif any(k in s for k in ["持股權重","投資比例","權重"]): rename_map[c] = "持股權重"
        elif any(k in s for k in ["股數","持有股數"]): rename_map[c] = "股數"
    df.rename(columns=rename_map, inplace=True)

    if "股票代號" in df.columns:
        df["股票代號"] = df["股票代號"].astype(str).str.extract(r"([1-9]\d{3})", expand=False)
    for col in ["持股權重", "股數"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "股票代號" in df.columns:
        df = df.dropna(subset=["股票代號"]).drop_duplicates("股票代號")
    return df

@st.cache_data
def load_cost_basis():
    cost_path = Path("data/cost_basis.csv")
    if not cost_path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(cost_path, encoding="utf-8-sig")
        df.columns = [str(c).replace("﻿", "").strip() for c in df.columns]
        df["股數"] = pd.to_numeric(df.get("股數", 0), errors="coerce").fillna(0).astype(int)
        df["成本市值"] = pd.to_numeric(df.get("成本市值", 0), errors="coerce").fillna(0.0)
        return df[df["股數"] > 0].copy()
    except Exception:
        return pd.DataFrame()

@st.cache_data
def load_latest_prices():
    price_files = sorted(glob.glob("prices/*.csv"))
    if not price_files:
        return {}
    try:
        df = pd.read_csv(price_files[-1], encoding="utf-8-sig")
        df.columns = [str(c).replace("﻿", "").strip() for c in df.columns]
        code_col = next((c for c in df.columns if any(k in c for k in ["股票代號","代號","代碼"])), None)
        price_col = next((c for c in df.columns if any(k in c for k in ["收盤價","收盤","Close"])), None)
        if not code_col or not price_col:
            return {}
        df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
        return dict(zip(df[code_col].astype(str), df[price_col]))
    except Exception:
        return {}

@st.cache_data
def load_realized_gains():
    gains_path = Path("data/realized_gains_log.csv")
    if not gains_path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(gains_path, encoding="utf-8-sig")
        df.columns = [str(c).replace("﻿", "").strip() for c in df.columns]
        return df
    except Exception:
        return pd.DataFrame()

dates = load_available_dates()
if len(dates) < 2:
    st.error("需要至少兩天的 CSV 資料才能進行比較。")
    st.stop()

# 3. 分頁介面
tab_change, tab_cost = st.tabs(["📊 持股變化分析", "💰 成本與損益追蹤"])

# ==================== TAB 1: 持股變化分析 ====================
with tab_change:
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.selectbox("選擇起始日期", dates, index=max(0, len(dates)-2))
    with col2:
        end_date = st.selectbox("選擇結束日期", dates, index=len(dates)-1)

    if start_date >= end_date:
        st.warning("結束日期必須晚於起始日期喔。")
    elif st.button("計算持股變化 & AI 分析", type="primary"):
        df_start = load_data(start_date)
        df_end = load_data(end_date)

        df_s = df_start[['股票代號', '股票名稱', '股數', '持股權重']].rename(columns={'股數':'起始股數', '持股權重':'起始權重'})
        df_e = df_end[['股票代號', '股票名稱', '股數', '持股權重']].rename(columns={'股數':'結束股數', '持股權重':'結束權重'})

        df_merge = pd.merge(df_s, df_e, on=['股票代號', '股票名稱'], how='outer').fillna(0)
        df_merge['股數變化'] = df_merge['結束股數'] - df_merge['起始股數']
        df_merge['權重變化'] = (df_merge['結束權重'] - df_merge['起始權重']).round(3)

        changed_df = df_merge[df_merge['股數變化'] != 0].sort_values("權重變化", ascending=False)

        new_buys = changed_df[changed_df['起始股數'] == 0]
        sold_outs = changed_df[changed_df['結束股數'] == 0]

        st.subheader(f"📊 {start_date} 至 {end_date} 持股變化")
        colA, colB = st.columns(2)
        with colA:
            st.write("🔥 **新增持股**", new_buys[['股票代號', '股票名稱', '結束股數', '結束權重']])
        with colB:
            st.write("💀 **清倉持股**", sold_outs[['股票代號', '股票名稱', '起始股數', '起始權重']])

        st.write("📋 **全部異動排行 (依權重變化)**")
        st.dataframe(changed_df, use_container_width=True)

        if has_api:
            st.subheader("🤖 AI 策略洞察")
            prompt = f"""
            你是一位頂尖的量化 ETF 分析師。以下是 00981A ETF 從 {start_date} 到 {end_date} 的異動數據：
            - 新增持股：{', '.join(new_buys['股票名稱'].tolist()) if not new_buys.empty else '無'}
            - 清倉持股：{', '.join(sold_outs['股票名稱'].tolist()) if not sold_outs.empty else '無'}
            - 前五大加碼：\n{changed_df.head(5)[['股票名稱', '權重變化']].to_string()}
            - 前五大減碼：\n{changed_df.tail(5)[['股票名稱', '權重變化']].to_string()}

            請用繁體中文：
            1. 快速總結該期間的換股邏輯（例如偏好高殖利率、動能、大型權值、或是特定產業輪動）。
            2. 推測經理人對接下來市場的看法。
            3. 結語，語氣簡潔專業，不囉唆。
            """
            with st.spinner("AI 思考中，請稍候..."):
                try:
                    valid_models = [m.name for m in genai.list_models()
                                    if 'generateContent' in m.supported_generation_methods]
                    if not valid_models:
                        st.error("此 API Key 尚未開通文字生成模型權限。")
                    else:
                        target_model = next(
                            (m for m in valid_models if 'gemini-2.5-flash' in m), None
                        ) or next(
                            (m for m in valid_models if 'gemini-2.0-flash' in m), None
                        ) or next(
                            (m for m in valid_models if 'flash' in m and 'lite' not in m and 'preview' not in m), None
                        ) or valid_models[0]
                        model_name = target_model.replace("models/", "")
                        model = genai.GenerativeModel(model_name)
                        res = model.generate_content(prompt)
                        st.success(f"*(本次使用分析模型：{model_name})*")
                        st.info(res.text)
                except Exception as e:
                    st.error(f"AI 呼叫失敗：{e}")

# ==================== TAB 2: 成本與損益追蹤 ====================
with tab_cost:
    st.subheader("💰 成本帳簿與浮動損益")

    df_cost = load_cost_basis()
    price_map = load_latest_prices()

    if df_cost.empty:
        st.info("尚無成本帳簿資料（`data/cost_basis.csv` 不存在）。\n\n"
                "請先執行：`python scripts/backfill_cost_basis.py` 從歷史資料重建，"
                "或等待明日 `daily_fetch` workflow 自動建立。")
    else:
        df_display = df_cost.copy()
        df_display["平均成本(元)"] = (
            df_display["成本市值"] / df_display["股數"].replace(0, pd.NA)
        ).round(2)

        df_display["今日收盤價"] = df_display["股票代號"].astype(str).map(price_map)
        df_display["今日市值(元)"] = (df_display["今日收盤價"] * df_display["股數"]).round(0)
        df_display["浮動損益(元)"] = (df_display["今日市值(元)"] - df_display["成本市值"]).round(0)
        df_display["報酬率"] = (
            df_display["浮動損益(元)"] / df_display["成本市值"].replace(0, pd.NA)
        ).round(4)

        # 總覽指標
        total_cost = df_display["成本市值"].sum()
        total_market = df_display["今日市值(元)"].dropna().sum()
        total_pnl = total_market - total_cost if total_market > 0 else None

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("總投資成本", f"{total_cost:,.0f} 元")
        m2.metric("今日持倉市值", f"{total_market:,.0f} 元" if total_market > 0 else "—")
        if total_pnl is not None:
            m3.metric("浮動損益", f"{total_pnl:+,.0f} 元", delta=f"{total_pnl/total_cost:+.2%}" if total_cost > 0 else None)
            m4.metric("整體報酬率", f"{total_pnl/total_cost:+.2%}" if total_cost > 0 else "—")
        else:
            m3.metric("浮動損益", "—")
            m4.metric("整體報酬率", "—")

        st.write("#### 個股成本明細（依成本市值由大到小）")

        def _color_pnl(val):
            if pd.isna(val) or val == 0:
                return ""
            return "color: #16a34a; font-weight:600" if val > 0 else "color: #dc2626; font-weight:600"

        show_cols = ["股票代號", "股票名稱", "股數", "平均成本(元)",
                     "成本市值", "今日收盤價", "今日市值(元)", "浮動損益(元)", "報酬率"]
        df_show = df_display[show_cols].sort_values("成本市值", ascending=False).reset_index(drop=True)

        styled = df_show.style.applymap(_color_pnl, subset=["浮動損益(元)", "報酬率"])
        st.dataframe(styled, use_container_width=True)

        # 損益長條圖
        valid_pnl = df_display.dropna(subset=["浮動損益(元)"]).copy()
        if not valid_pnl.empty:
            st.write("#### 個股浮動損益排行")
            chart_df = valid_pnl.set_index("股票代號")[["浮動損益(元)"]].sort_values("浮動損益(元)")
            st.bar_chart(chart_df)

    # 實現損益紀錄
    st.write("---")
    st.subheader("📋 實現損益紀錄（完全清倉）")
    df_gains = load_realized_gains()
    if df_gains.empty:
        st.info("目前無完全清倉的實現損益紀錄。")
    else:
        st.dataframe(df_gains, use_container_width=True)
        total_realized = pd.to_numeric(df_gains.get("實現損益", pd.Series(dtype=float)), errors="coerce").sum()
        st.metric("累計實現損益", f"{total_realized:+,.0f} 元")
