# etf_tracker.py — Selenium 抓取 + 當日收盤價寫入 data/CSV
import os
import time
import glob
import uuid
from datetime import datetime

import pandas as pd
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller

# ========= 基本設定 =========
TODAY = datetime.today().strftime('%Y-%m-%d')
URL = "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW"  # 00981A
DOWNLOAD_DIR = os.path.abspath("downloads")
DATA_DIR = os.path.abspath("data")
SCREEN_DIR = os.path.abspath("screenshots")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(SCREEN_DIR, exist_ok=True)

def log(msg: str):
    print(f"[etf_tracker] {msg}", flush=True)

# ========= 欄位工具 =========
def _normalize_cols(cols):
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
    if not path or not os.path.exists(path): return False
    last = -1; stable = 0
    for _ in range(checks):
        sz = os.path.getsize(path)
        stable = stable + 1 if (sz == last and sz > 0) else 0
        last = sz
        time.sleep(interval)
    return stable >= (checks - 1)

# ========= 當日收盤價抓取 =========
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/127.0 Safari/537.36"
})

def _to_roc_date(yyyy_mm_dd: str) -> str:
    y, m, d = yyyy_mm_dd.split("-")
    roc = int(y) - 1911
    return f"{roc:03d}/{int(m):02d}/{int(d):02d}"

def _twse_stock_day(code: str, date: str):
    """TWSE 月資料取當日收盤。date: YYYYMMDD"""
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    params = {"response": "json", "date": date, "stockNo": code}
    r = SESSION.get(url, params=params, timeout=12)
    r.raise_for_status()
    j = r.json()
    if j.get("stat") != "OK": return None
    rows = j.get("data", [])
    # rows: [日期(ROC), 開, 高, 低, 收, 漲跌, 成交量, 成交金額, 成交筆數]
    target_roc = _to_roc_date(f"{date[:4]}-{date[4:6]}-{date[6:]}")
    for row in rows:
        if row[0] == target_roc:
            close = str(row[6 if len(row) > 6 and "個股日成交資訊" in j.get("title","") else 4])  # 兼容欄位位移
            # 正常情況是 index 6 是成交量；多數版本 index 4 是收盤
            # 以數字可解析者為準
            try:
                c = float(str(row[4]).replace(",",""))
            except:
                return None
            return c
    return None

def _tpex_daily(code: str, roc_yyy_mm: str, roc_yyy_mm_dd: str):
    """
    櫃買日資料，月表取當日：
    roc_yyy_mm: 113/08
    roc_yyy_mm_dd: 113/08/09
    """
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
    params = {"l": "zh-tw", "d": roc_yyy_mm, "stkno": code}
    r = SESSION.get(url, params=params, timeout=12)
    r.raise_for_status()
    j = r.json()
    rows = j.get("aaData") or j.get("data") or []
    # 典型行：[日期, 成交股數, 成交金額, 開盤, 最高, 最低, 收盤, 漲跌, 筆數, ...]
    for row in rows:
        if str(row[0]).strip() == roc_yyy_mm_dd:
            try:
                return float(str(row[6]).replace(",",""))
            except:
                return None
    return None

def fetch_close_price(code: str, ymd: str) -> float | None:
    """嘗試 TWSE → TPEX。失敗則回傳 None。"""
    ymd_compact = ymd.replace("-", "")
    # 先 TWSE
    try:
        c = _twse_stock_day(code, ymd_compact)
        if c is not None: return round(float(c), 2)
    except Exception:
        pass
    # 再 TPEX（需要 ROC 月份+日期）
    try:
        roc_date = _to_roc_date(ymd)
        roc_month = roc_date.rsplit("/", 1)[0]  # 113/08
        c = _tpex_daily(code, roc_month, roc_date)
        if c is not None: return round(float(c), 2)
    except Exception:
        pass
    return None

# ========= 啟動 Chrome =========
chromedriver_autoinstaller.install()
options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1280,900")
options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)
options.add_experimental_option("prefs", {
    "download.default_directory": DOWNLOAD_DIR,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

driver = webdriver.Chrome(options=options)
driver.set_window_size(1280, 900)

try:
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": DOWNLOAD_DIR})
except Exception:
    try:
        driver.execute_cdp_cmd("Browser.setDownloadBehavior", {"behavior": "allow", "downloadPath": DOWNLOAD_DIR})
    except Exception:
        pass

def snap(tag: str):
    path = os.path.join(SCREEN_DIR, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{tag}_{uuid.uuid4().hex[:6]}.png")
    try:
        driver.save_screenshot(path)
        log(f"Saved screenshot: {path}")
    except Exception as e:
        log(f"screenshot failed: {e}")

def safe_click(locator, tries=3):
    last_err = None
    for _ in range(tries):
        try:
            el = WebDriverWait(driver, 12).until(EC.element_to_be_clickable(locator))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.2)
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception as e:
            last_err = e
            time.sleep(1.2)
    log(f"click failed: {last_err}")
    return False

try:
    # 1) 開頁
    log(f"Open page: {URL}")
    driver.get(URL)
    WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(1)

    # 2) 點「基金投資組合」
    for locator in [
        (By.XPATH, "//*[contains(text(),'基金投資組合')]"),
        (By.XPATH, "//*[contains(text(),'投資組合')]"),
    ]:
        if safe_click(locator, tries=3):
            log("Clicked tab: 基金投資組合")
            time.sleep(0.8)
            break

    # 3) 點「匯出XLSX」
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
            if safe_click(loc, tries=3):
                clicked = True
                log("Clicked: 匯出XLSX")
                break
        except Exception:
            continue

    if not clicked:
        snap("no_export_button")
        raise RuntimeError("找不到『匯出XLSX』按鈕，請檢查頁面是否改版或權限限制。")

    time.sleep(2)

    # 若無下載活動也無檔案 → 嘗試以 href 直接導向
    if not list_crdownload() and not latest_xlsx_path() and href_fallback:
        log("No download detected after click, trying direct href navigation…")
        driver.get(href_fallback)
        time.sleep(2)

    # 4) 等下載完成
    start_deadline = time.time() + 60
    while time.time() < start_deadline:
        if list_crdownload() or latest_xlsx_path(): break
        log("Waiting download start…")
        time.sleep(1)

    xlsx_path = None
    finish_deadline = time.time() + 180
    while time.time() < finish_deadline:
        crd = list_crdownload()
        xlsx = latest_xlsx_path()
        log(f"Downloading… (crdownload={len(crd)}, xlsx={'yes' if xlsx else 'no'})")
        if xlsx and size_stable(xlsx, checks=6, interval=0.8):
            xlsx_path = xlsx
            break
        time.sleep(1)

    if not xlsx_path:
        x = latest_xlsx_path()
        if x and os.path.getsize(x) > 0:
            xlsx_path = x

    if not xlsx_path:
        snap("no_xlsx_detected")
        raise RuntimeError("未偵測到下載完成的 .xlsx 檔。")

    log(f"Downloaded Excel: {os.path.basename(xlsx_path)}")

    # 5) 解析 Excel
    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    target_df = None
    for sheet in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
        found_header_row = None
        for r in range(min(30, len(raw))):
            row_vals = _normalize_cols(list(raw.iloc[r].fillna("").astype(str)))
            if any(any(k in cell for k in ALIAS["股票代號"]) for cell in row_vals) and \
               any(any(k in cell for k in ALIAS["股票名稱"]) for cell in row_vals):
                found_header_row = r
                break
        if found_header_row is None:
            continue
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
        snap("parse_failed")
        raise KeyError(f"在任何工作表都找不到必要欄位，工作表={xls.sheet_names}")

    # 6) 清洗數值
    for col in ["股數", "持股權重"]:
        target_df[col] = (
            target_df[col].astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
        )
    target_df["股數"] = pd.to_numeric(target_df["股數"], errors="coerce")
    target_df["持股權重"] = pd.to_numeric(target_df["持股權重"], errors="coerce")

    # 7) 逐檔補上「收盤價」
    prices = {}
    for code in target_df["股票代號"].astype(str).str.strip():
        if not code or not code.isdigit():
            prices[code] = None
            continue
        try:
            px = fetch_close_price(code, TODAY)
        except Exception:
            px = None
        prices[code] = px
        log(f"close price {code}: {px}")

    target_df["收盤價"] = target_df["股票代號"].astype(str).map(prices)

    # 8) 輸出 CSV
    csv_path = os.path.join(DATA_DIR, f"{TODAY}.csv")
    target_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    log(f"Saved CSV: {csv_path}")

except Exception as e:
    snap("exception")
    raise
finally:
    driver.quit()
    log("Driver closed.")
