import os
import time
import glob
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller

# 安裝對應版 chromedriver
chromedriver_autoinstaller.install()

TODAY = datetime.today().strftime('%Y-%m-%d')
URL = "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW"

# 下載與輸出目錄（使用絕對路徑較穩）
DOWNLOAD_DIR = os.path.abspath("downloads")
DATA_DIR = os.path.abspath("data")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# 設定 Chrome（headless + 允許自動下載）
options = Options()
options.add_argument("--headless=new")          # 新版 headless
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_experimental_option("prefs", {
    "download.default_directory": DOWNLOAD_DIR,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})
# 降低自動化特徵
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

driver = webdriver.Chrome(options=options)
driver.set_window_size(1280, 900)

try:
    driver.get(URL)

    # 等頁面載入
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    # 嘗試點到「基金投資組合」分頁（若沒有就忽略）
    for locator in [
        (By.XPATH, "//*[contains(text(),'基金投資組合')]"),
        (By.XPATH, "//*[contains(text(),'投資組合')]"),
    ]:
        try:
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable(locator)).click()
            time.sleep(1)
            break
        except Exception:
            pass

    # 找「匯出XLSX」按鈕（多種定位，擇一成功）
    export_locators = [
        (By.XPATH, "//a[contains(., 'XLSX')]"),
        (By.XPATH, "//a[contains(., '匯出')]"),
        (By.XPATH, "//button[contains(., 'XLSX') or contains(., '匯出')]"),
        (By.CSS_SELECTOR, "a[href*='Export'][href*='xlsx'], a[href*='XLSX']"),
    ]

    clicked = False
    for loc in export_locators:
        try:
            btn = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(loc))
            driver.execute_script("arguments[0].click();", btn)  # 用 JS click 比較穩
            clicked = True
            break
        except Exception:
            continue

    if not clicked:
        raise RuntimeError("找不到『匯出XLSX』按鈕，請檢查頁面是否改版。")

    # 等待下載完成：偵測 .crdownload 不見、且有 .xlsx 出現
    def latest_xlsx():
        files = sorted(glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx")), key=os.path.getmtime)
        return files[-1] if files else None

    timeout = time.time() + 90  # 最長等 90 秒
    xlsx_path = None
    while time.time() < timeout:
        if any(name.endswith(".crdownload") for name in os.listdir(DOWNLOAD_DIR)):
            time.sleep(1)
            continue
        xlsx_path = latest_xlsx()
        if xlsx_path:
            break
        time.sleep(1)

    if not xlsx_path:
        raise RuntimeError("未偵測到下載完成的 .xlsx 檔。")

    # 讀 Excel → 取四欄 → 存 CSV
    df = pd.read_excel(xlsx_path)

    # 欄位正規化（清掉全形/半形空白）
    df.columns = [str(c).strip().replace("　", "").replace("\u3000", "") for c in df.columns]

    # 欄位名稱容錯對映
    alias = {
        "股票代號": ["股票代號", "證券代號", "代號"],
        "股票名稱": ["股票名稱", "證券名稱", "名稱"],
        "股數":     ["股數", "持股股數"],
        "持股權重": ["持股權重", "持股比例", "權重", "比重(%)", "占比(%)"]
    }
    def pick(colname):
        for cand in alias[colname]:
            if cand in df.columns:
                return cand
        raise KeyError(f"找不到欄位：{colname}，實際欄位={list(df.columns)}")

    c_code = pick("股票代號")
    c_name = pick("股票名稱")
    c_shares = pick("股數")
    c_weight = pick("持股權重")

    out = df[[c_code, c_name, c_shares, c_weight]].copy()

    # 數值清洗（去逗號、% → 轉數字）
    for col in [c_shares, c_weight]:
        out[col] = (
            out[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
        )
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out.columns = ["股票代號", "股票名稱", "股數", "持股權重"]

    csv_path = os.path.join(DATA_DIR, f"{TODAY}.csv")
    out.to_csv(csv_path, index=False, encoding="utf-8-sig")

finally:
    driver.quit()

