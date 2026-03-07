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
    
    # 欄位標準化處理
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
    
    # 去除重複與空值
    if "股票代號" in df.columns:
        df = df.dropna(subset=["股票代號"]).drop_duplicates("股票代號")
    return df

dates = load_available_dates()
if len(dates) < 2:
    st.error("需要至少兩天的 CSV 資料才能進行比較。")
    st.stop()

# 3. UI 篩選介面
col1, col2 = st.columns(2)
with col1:
    start_date = st.selectbox("選擇起始日期", dates, index=max(0, len(dates)-2))
with col2:
    end_date = st.selectbox("選擇結束日期", dates, index=len(dates)-1)

if start_date >= end_date:
    st.warning("結束日期必須晚於起始日期喔。")
    st.stop()

# 4. 點擊按鈕進行分析
if st.button("計算持股變化 & AI 分析", type="primary"):
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
    
    # 顯示數據表格
    st.subheader(f"📊 {start_date} 至 {end_date} 持股變化")
    colA, colB = st.columns(2)
    with colA:
        st.write("🔥 **新增持股**", new_buys[['股票代號', '股票名稱', '結束股數', '結束權重']])
    with colB:
        st.write("💀 **清倉持股**", sold_outs[['股票代號', '股票名稱', '起始股數', '起始權重']])
        
    st.write("📋 **全部異動排行 (依權重變化)**")
    st.dataframe(changed_df, use_container_width=True)
    
    # AI 分析模組
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
                # 動態尋找可用的模型
                valid_models = []
                for m in genai.list_models():
                    if 'generateContent' in m.supported_generation_methods:
                        valid_models.append(m.name)
                
                if not valid_models:
                    st.error("此 API Key 尚未開通文字生成模型權限，請確認是否為有效 Key。")
                else:
                    # 優先順序：2.5-flash -> 2.0-flash -> flash-latest
                    target_model = ""
                    for m in valid_models:
                        if m == 'models/gemini-2.5-flash':
                            target_model = m
                            break
                    if not target_model:
                        for m in valid_models:
                            if m == 'models/gemini-2.0-flash':
                                target_model = m
                                break
                    if not target_model:
                        for m in valid_models:
                            if 'flash' in m and 'lite' not in m and 'preview' not in m:
                                target_model = m
                                break
                    if not target_model:
                        target_model = valid_models[0]
                    
                    # 移除 'models/' 前綴確保相容性
                    model_name = target_model.replace("models/", "")
                    model = genai.GenerativeModel(model_name)
                    res = model.generate_content(prompt)
                    st.success(f"*(本次使用分析模型：{model_name})*")
                    st.info(res.text)
            except Exception as e:
                st.error(f"AI 呼叫失敗，請檢查 API Key 權限或網路狀態：\n{e}\n\n發現以下可用模型：{valid_models if 'valid_models' in locals() else '無法取得清單'}")
