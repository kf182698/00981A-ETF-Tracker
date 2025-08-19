# etf_tracker.py — 抓取 00981A ETF 每日持股明細，存成 CSV + 原始 Excel
import os
import re
import time
import glob
import shutil
from datetime import datetime
from pathlib import Path

import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ===== 路徑設定 =====
DOWNLOAD_DIR = "downloads"
DATA_DIR = "data"
Path(DOWNLOAD_DIR).mkdir(exist_ok=True)
Path(DATA_DIR).mkdir(exist_ok=True)

# ===== ETF 網址（EZMoney 活動頁） =====
ETF_URL = "https://www.ezmoney.com.tw/events/2025TGA/index.html"

def _latest_downloaded_file(folder):
    """取下載資料夾內最後生成的檔案路徑"""
    files = glob.glob(os.path.join(folder, "*"))
    if not files:
        return None
    return max(files, key=os.path.getctime)

def _clean_dataframe(df: pd.DataFrame):
    """清理 ETF 持股 Excel 轉換成統一格式"""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # 嘗試找出欄位
    mapping = {}
    for col in df.columns:
        if re.search("代號", col):
            mapping[col] = "股票代號"
        elif re.search("名稱", col):
            mapping[col] = "股票名稱"
        elif re.search("股數", col):
            mapping[col] = "股數"
        elif re.search("持股比例|權重|占比", col):
            mapping[col] = "持股權重"
        elif re.search("收盤|價格|Close", col, re.IGNORECASE):
            mapping[col] = "收盤價"
    df = df.rename(columns=mapping)

    need_cols = ["股票代號","股票名稱","股數","持股權重"]
    for c in need_cols:
        if c not in df.columns:
            raise ValueError(f"缺少必要欄位: {c}")

    # 清理數值
    df["股票代號"] = df["股票代號"].astype(str).str.strip()
    df["股票名稱"] = df["股票名稱"].astype(str).str.strip()

    df["股數"] = (
        df["股數"].astype(str).str.replace(",", "", regex=False)
    )
    df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)

    df["持股權重"] = (
        df["持股權重"].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False)
    )
    df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)

    if "收盤價" in df.columns:
        df["收盤價"] = pd.to_numeric(df["收盤價"], errors="coerce")

    return df[["股票代號","股票名稱","股數","持股權重"] + (["收盤價"] if "收盤價" in df.columns else [])]

def download_etf_excel():
    """用 Selenium 模擬下載 ETF Excel"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    prefs = {"download.default_directory": str(Path(DOWNLOAD_DIR).resolve())}
    chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(ETF_URL)

    try:
        # 找「下載 Excel」按鈕
        dl_btn = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Excel') or contains(text(), 'EXCEL')]"))
        )
        dl_btn.click()
        time.sleep(8)  # 等待檔案下載完成
    except Exception as e:
        driver.quit()
        raise RuntimeError(f"下載 Excel 失敗: {e}")

    driver.quit()

    # 找下載好的檔案
    f = _latest_downloaded_file(DOWNLOAD_DIR)
    if not f:
        raise FileNotFoundError("下載資料夾沒有檔案")

    return f

def main():
    today = datetime.now().strftime("%Y-%m-%d")

    # ===== Step1. 下載 Excel =====
    raw_excel = download_etf_excel()

    # 固定命名保存一份原始 Excel
    fixed_excel = os.path.join(DOWNLOAD_DIR, f"{today}.xlsx")
    shutil.move(raw_excel, fixed_excel)
    print(f"Saved raw Excel: {fixed_excel}")

    # ===== Step2. 讀取與清洗 =====
    df = pd.read_excel(fixed_excel)
    df = _clean_dataframe(df)

    # ===== Step3. 存成 CSV =====
    csv_out = os.path.join(DATA_DIR, f"{today}.csv")
    df.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"Saved cleaned CSV: {csv_out}")

if __name__ == "__main__":
    main()
