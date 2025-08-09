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

    import pandas as pd

def _normalize_cols(cols):
    return [str(c).strip().replace("　", "").replace("\u3000", "") for c in cols]

alias = {
    "股票代號": ["股票代號", "證券代號", "代號", "證券代號/代碼"],
    "股票名稱": ["股票名稱", "證券名稱", "名稱"],
    "股數":     ["股數", "持股股數", "持有股數"],
    "持股權重": ["持股權重", "持股比例", "權重", "比重(%)", "占比(%)", "占比"]
}

def _pick(df, key):
    for cand in alias[key]:
        if cand in df.columns:
            return cand
    return None

# 逐張工作表嘗試、並嘗試不同表頭列
xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
target_df = None

for sheet in xls.sheet_names:
    # 先讀成無表頭，掃前 20 列找包含「股票代號/名稱」的那一列當 header
    raw = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
    found_header_row = None
    for r in range(min(20, len(raw))):
        row_vals = _normalize_cols(list(raw.iloc[r].fillna("").astype(str)))
        if any(any(k in cell for k in alias["股票代號"]) for cell in row_vals) and \
           any(any(k in cell for k in alias["股票名稱"]) for cell in row_vals):
            found_header_row = r
            break
    if found_header_row is None:
        # 直接跳過這張（很可能是封面/說明）
        continue

    df_try = pd.read_excel(xls, sheet_name=sheet, header=found_header_row)
    df_try.columns = _normalize_cols(df_try.columns)

    c_code = _pick(df_try, "股票代號")
    c_name = _pick(df_try, "股票名稱")
    c_shares = _pick(df_try, "股數")
    c_weight = _pick(df_try, "持股權重")

    if all([c_code, c_name, c_shares, c_weight]):
        target_df = df_try[[c_code, c_name, c_shares, c_weight]].copy()
        target_df.columns = ["股票代號", "股票名稱", "股數", "持股權重"]
        break

if target_df is None:
    raise KeyError(f"在任何工作表都找不到必要欄位，請手動下載檔案檢查欄名。工作表={xls.sheet_names}")

# 數值清洗
for col in ["股數", "持股權重"]:
    target_df[col] = (
        target_df[col].astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
    )
    target_df[col] = pd.to_numeric(target_df[col], errors="coerce")

csv_path = os.path.join(DATA_DIR, f"{TODAY}.csv")
target_df.to_csv(csv_path, index=False, encoding="utf-8-sig")


finally:
    driver.quit()

