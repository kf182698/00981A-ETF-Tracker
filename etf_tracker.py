# etf_tracker.py — 抓取 00981A ETF 每日持股明細，存成 CSV + 原始 Excel（穩定點擊 Info 頁「匯出XLSX」）
import os
import re
import time
import glob
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ===== 路徑設定 =====
DOWNLOAD_DIR = "downloads"
DATA_DIR = "data"
SCREEN_DIR = "screenshots"
Path(DOWNLOAD_DIR).mkdir(exist_ok=True, parents=True)
Path(DATA_DIR).mkdir(exist_ok=True, parents=True)
Path(SCREEN_DIR).mkdir(exist_ok=True, parents=True)

# ===== 直接使用 Info 頁（而不是活動頁） =====
FUND_CODE = os.environ.get("FUND_CODE", "49YTW")  # 00981A
ETF_URL = os.environ.get("EZMONEY_URL", f"https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode={FUND_CODE}")

def _latest_downloaded_file(folder):
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

    df["股數"] = df["股數"].astype(str).str.replace(",", "", regex=False)
    df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)

    df["持股權重"] = (
        df["持股權重"].astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
    )
    df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)

    if "收盤價" in df.columns:
        df["收盤價"] = pd.to_numeric(df["收盤價"], errors="coerce")

    cols = ["股票代號","股票名稱","股數","持股權重"]
    if "收盤價" in df.columns:
        cols.append("收盤價")
    return df[cols]

def _build_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
    prefs = {
        "download.default_directory": str(Path(DOWNLOAD_DIR).resolve()),
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=chrome_options)

def _screenshot(driver, tag: str):
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    png = os.path.join(SCREEN_DIR, f"{tag}_{ts}.png")
    html = os.path.join(SCREEN_DIR, f"{tag}_{ts}.html")
    try:
        driver.save_screenshot(png)
        with open(html, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print(f"[etf_tracker] Saved screenshot: {png}")
        print(f"[etf_tracker] Saved html: {html}")
    except Exception as e:
        print("[etf_tracker] screenshot/html failed:", e)

def download_etf_excel():
    """到 Info 頁 → 點『基金投資組合』分頁 → 點『匯出XLSX』→ 等待檔案下載"""
    driver = _build_driver()
    driver.get(ETF_URL)
    print(f"[etf_tracker] Open page: {ETF_URL}")

    try:
        wait = WebDriverWait(driver, 25)

        # 1) 點擊「基金投資組合」分頁
        tab = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//a[contains(.,'基金投資組合')]")
        ))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", tab)
        time.sleep(0.5)
        tab.click()
        print("[etf_tracker] Clicked tab: 基金投資組合")

        # 2) 找「匯出XLSX」按鈕（多種選擇器 fallback）
        selectors = [
            (By.XPATH, "//a[contains(.,'匯出') and contains(.,'XLSX')]"),
            (By.XPATH, "//button[contains(.,'匯出') and contains(.,'XLSX')]"),
            (By.CSS_SELECTOR, "a[href*='ExportFundHoldings']"),
        ]
        btn = None
        for by, sel in selectors:
            try:
                btn = wait.until(EC.element_to_be_clickable((by, sel)))
                if btn:
                    break
            except Exception:
                continue
        if not btn:
            _screenshot(driver, "no_export_button")
            raise TimeoutError("找不到『匯出XLSX』按鈕")

        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        time.sleep(0.5)
        btn.click()
        print("[etf_tracker] Clicked: 匯出XLSX")

        # 3) 等下載完成（.crdownload 消失、有 .xlsx 出現）
        deadline = time.time() + 40
        last_seen_cr = last_seen_xlsx = False
        while time.time() < deadline:
            time.sleep(1.0)
            crs = glob.glob(os.path.join(DOWNLOAD_DIR, "*.crdownload"))
            xlsxs = glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx"))
            last_seen_cr = bool(crs)
            last_seen_xlsx = bool(xlsxs)
            if not crs and xlsxs:
                xlsxs.sort(key=os.path.getmtime, reverse=True)
                driver.quit()
                return xlsxs[0]

        _screenshot(driver, "download_timeout")
        raise RuntimeError(f"下載逾時（crdownload={last_seen_cr}, xlsx={last_seen_xlsx}）")

    except Exception as e:
        _screenshot(driver, "exception")
        raise RuntimeError(f"下載 Excel 失敗: {e}") from e
    finally:
        try:
            driver.quit()
        except Exception:
            pass

def main():
    today = datetime.now().strftime("%Y-%m-%d")

    # ===== Step1. 下載 Excel（Info 頁匯出）=====
    raw_excel = download_etf_excel()

    # 固定命名保存一份原始 Excel：downloads/YYYY-MM-DD.xlsx
    fixed_excel = os.path.join(DOWNLOAD_DIR, f"{today}.xlsx")
    try:
        if os.path.exists(fixed_excel):
            os.remove(fixed_excel)
        shutil.move(raw_excel, fixed_excel)
    except Exception as e:
        print("[etf_tracker] rename failed:", e)
        fixed_excel = raw_excel  # fallback
    print(f"[etf_tracker] Saved raw Excel: {fixed_excel}")

    # ===== Step2. 讀取與清洗 =====
    df = pd.read_excel(fixed_excel)
    df = _clean_dataframe(df)

    # ===== Step3. 存成 CSV（供後續比較）=====
    csv_out = os.path.join(DATA_DIR, f"{today}.csv")
    df.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"[etf_tracker] Saved cleaned CSV: {csv_out}")

if __name__ == "__main__":
    main()
