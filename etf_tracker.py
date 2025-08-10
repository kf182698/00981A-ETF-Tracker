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


# ========= 基本設定 =========
TODAY = datetime.today().strftime('%Y-%m-%d')
# 這個 URL 對應你提供的 EZMoney 00981A 頁面（fundCode=49YTW）
URL = "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW"

DOWNLOAD_DIR = os.path.abspath("downloads")
DATA_DIR = os.path.abspath("data")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)


def log(msg: str):
    print(f"[etf_tracker] {msg}", flush=True)


# ========= 欄位工具 =========
def _normalize_cols(cols):
    # 去除全形/半形空白
    return [str(c).strip().replace("　", "").replace("\u3000", "") for c in cols]


ALIAS = {
    "股票代號": ["股票代號", "證券代號", "代號", "證券代號/代碼"],
    "股票名稱": ["股票名稱", "證券名稱", "名稱"],
    "股數":     ["股數", "持股股數", "持有股數"],
    "持股權重": ["持股權重", "持股比例", "權重", "比重(%)", "占比(%)", "占比"]
}


def _pick(df: pd.DataFrame, key: str):
    for cand in ALIAS[key]:
        if cand in df.columns:
            return cand
    return None


# ========= 下載等待工具 =========
def list_crdownload():
    try:
        return [n for n in os.listdir(DOWNLOAD_DIR) if n.endswith(".crdownload")]
    except FileNotFoundError:
        return []


def latest_xlsx_path():
    files = sorted(glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx")), key=os.path.getmtime)
    return files[-1] if files else None


def size_stable(path, checks=6, interval=0.8):
    """檔案大小連續 checks 次未變化即視為穩定"""
    if not path or not os.path.exists(path):
        return False
    last = -1
    stable = 0
    for _ in range(checks):
        sz = os.path.getsize(path)
        if sz == last and sz > 0:
            stable += 1
        else:
            stable = 0
        last = sz
        time.sleep(interval)
    return stable >= (checks - 1)


# ========= 啟動 Chrome (Headless) =========
chromedriver_autoinstaller.install()
options = Options()
options.add_argument("--headless=new")          # 新版 headless
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
# 降低自動化特徵（有些站會擋）
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)
# 允許自動下載
options.add_experimental_option("prefs", {
    "download.default_directory": DOWNLOAD_DIR,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

driver = webdriver.Chrome(options=options)
driver.set_window_size(1280, 900)

# 顯式允許 headless 下載（CDP）
try:
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": DOWNLOAD_DIR
    })
except Exception:
    try:
        driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": DOWNLOAD_DIR
        })
    except Exception:
        pass


try:
    # 1) 開頁
    log(f"Open page: {URL}")
    driver.get(URL)
    WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(1)

    # 2) 嘗試點「基金投資組合」分頁（若有）
    for locator in [
        (By.XPATH, "//*[contains(text(),'基金投資組合')]"),
        (By.XPATH, "//*[contains(text(),'投資組合')]"),
    ]:
        try:
            el = WebDriverWait(driver, 5).until(EC.element_to_be_clickable(locator))
            driver.execute_script("arguments[0].click();", el)
            log("Clicked tab: 基金投資組合")
            time.sleep(0.8)
            break
        except Exception:
            pass

    # 3) 找「匯出XLSX」按鈕並嘗試下載
    export_locators = [
        (By.XPATH, "//a[contains(., 'XLSX')]"),
        (By.XPATH, "//a[contains(., '匯出')]"),
        (By.XPATH, "//button[contains(., 'XLSX') or contains(., '匯出')]"),
        (By.CSS_SELECTOR, "a[href*='Export'][href*='xlsx'], a[href*='XLSX']")
    ]
    clicked = False
    href_fallback = None

    for loc in export_locators:
        try:
            btn = WebDriverWait(driver, 12).until(EC.element_to_be_clickable(loc))
            href_fallback = btn.get_attribute("href")
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", btn)  # JS click 較穩
            clicked = True
            log("Clicked: 匯出XLSX")
            break
        except Exception:
            continue

    if not clicked:
        raise RuntimeError("找不到『匯出XLSX』按鈕，請檢查頁面是否改版或權限限制。")

    time.sleep(2)  # 給點時間讓下載開始

    # 若無下載活動也無檔案 → 嘗試以 href 直接導向
    if not list_crdownload() and not latest_xlsx_path() and href_fallback:
        log("No download detected after click, trying direct href navigation…")
        driver.get(href_fallback)
        time.sleep(2)

    # 4) 等待 Excel 下載完成（先等開始 → 再等大小穩定）
    start_deadline = time.time() + 60  # 最多等 60 秒開始
    while time.time() < start_deadline:
        crd = list_crdownload()
        xlsx = latest_xlsx_path()
        log(f"Downloading… (crdownload={len(crd)}, xlsx={'yes' if xlsx else 'no'})")
        if crd or xlsx:
            break
        time.sleep(1)

    xlsx_path = None
    finish_deadline = time.time() + 180  # 最多再等 180 秒完成
    while time.time() < finish_deadline:
        crd = list_crdownload()
        xlsx = latest_xlsx_path()

        status = f"crdownload={len(crd)}, xlsx={'yes' if xlsx else 'no'}"
        log(f"Downloading… ({status})")

        if xlsx and size_stable(xlsx, checks=6, interval=0.8):
            xlsx_path = xlsx
            break

        if not crd and not xlsx:
            time.sleep(1)
            continue

        time.sleep(1)

    # 最後保險一次
    if not xlsx_path:
        x = latest_xlsx_path()
        if x and os.path.getsize(x) > 0:
            log("No perfect finish detected, but XLSX exists with size > 0. Proceeding.")
            xlsx_path = x

    if not xlsx_path:
        raise RuntimeError("未偵測到下載完成的 .xlsx 檔（可能被網站阻擋或按鈕定位失敗）。")

    log(f"Downloaded Excel: {os.path.basename(xlsx_path)}")

    # 5) 解析 Excel：自動定位正確工作表與表頭
    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    target_df = None

    for sheet in xls.sheet_names:
        # 無表頭讀入，掃前 30 列找包含股票代號/名稱的表頭列
        raw = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
        found_header_row = None
        for r in range(min(30, len(raw))):
            row_vals = _normalize_cols(list(raw.iloc[r].fillna("").astype(str)))
            if any(any(k in cell for k in ALIAS["股票代號"]) for cell in row_vals) and \
               any(any(k in cell for k in ALIAS["股票名稱"]) for cell in row_vals):
                found_header_row = r
                break
        if found_header_row is None:
            continue  # 封面或說明頁，跳過

        df_try = pd.read_excel(xls, sheet_name=sheet, header=found_header_row)
        df_try.columns = _normalize_cols(df_try.columns)

        c_code   = _pick(df_try, "股票代號")
        c_name   = _pick(df_try, "股票名稱")
        c_shares = _pick(df_try, "股數")
        c_weight = _pick(df_try, "持股權重")

        if all([c_code, c_name, c_shares, c_weight]):
            target_df = df_try[[c_code, c_name, c_shares, c_weight]].copy()
            target_df.columns = ["股票代號", "股票名稱", "股數", "持股權重"]
            log(f"Parsed sheet: {sheet} (header row={found_header_row})")
            break

    if target_df is None:
        raise KeyError(f"在任何工作表都找不到必要欄位，請手動下載檔案檢查欄名。工作表={xls.sheet_names}")

    # 6) 數值清洗並輸出 CSV
    for col in ["股數", "持股權重"]:
        target_df[col] = (
            target_df[col].astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
        )
    target_df["股數"] = pd.to_numeric(target_df["股數"], errors="coerce")
    target_df["持股權重"] = pd.to_numeric(target_df["持股權重"], errors="coerce")

    csv_path = os.path.join(DATA_DIR, f"{TODAY}.csv")
    target_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    log(f"Saved CSV: {csv_path}")

finally:
    driver.quit()
    log("Driver closed.")
