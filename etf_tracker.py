# === 取代你檔案內『啟動 Chrome』到『點擊與下載』的那段（含重試與截圖） ===
# ……前略：imports 與常數沿用你原檔……

import uuid

SCREEN_DIR = os.path.abspath("screenshots")
os.makedirs(SCREEN_DIR, exist_ok=True)

# ========= 啟動 Chrome (Headless) =========
chromedriver_autoinstaller.install()
options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1280,900")
options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36")
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

# 允許下載（CDP）
try:
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior":"allow","downloadPath":DOWNLOAD_DIR})
except Exception:
    try:
        driver.execute_cdp_cmd("Browser.setDownloadBehavior", {"behavior":"allow","downloadPath":DOWNLOAD_DIR})
    except Exception:
        pass

def snap(tag):
    path = os.path.join(SCREEN_DIR, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{tag}_{uuid.uuid4().hex[:6]}.png")
    try:
        driver.save_screenshot(path)
        print(f"[etf_tracker] Saved screenshot: {path}")
    except Exception as e:
        print("[etf_tracker] screenshot failed:", e)

def safe_click(locator, tries=3):
    last_err = None
    for i in range(tries):
        try:
            el = WebDriverWait(driver, 12).until(EC.element_to_be_clickable(locator))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception as e:
            last_err = e
            time.sleep(1.2)
    print("[etf_tracker] click failed:", last_err)
    return False

try:
    log(f"Open page: {URL}")
    driver.get(URL)
    WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(1)

    # 點分頁（重試）
    for locator in [
        (By.XPATH, "//*[contains(text(),'基金投資組合')]"),
        (By.XPATH, "//*[contains(text(),'投資組合')]"),
    ]:
        if safe_click(locator, tries=3):
            log("Clicked tab: 基金投資組合")
            time.sleep(0.8)
            break

    # 找匯出按鈕（重試）
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
        raise RuntimeError("找不到『匯出XLSX』按鈕")

    # 以下保留你原本的等待下載（含大小穩定邏輯），遇超時就截圖
    # ……（你的等待下載程式碼原樣保留）……
except Exception as e:
    snap("exception")
    raise
finally:
    driver.quit()
    log("Driver closed.")
