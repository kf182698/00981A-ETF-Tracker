# etf_tracker.py — 抓取 00981A ETF 每日持股，下載 Excel → 轉 CSV（強化表頭偵測/欄位別名/合欄拆解）
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

# ===== 直接使用 Info 頁 =====
FUND_CODE = os.environ.get("FUND_CODE", "49YTW")  # 00981A
ETF_URL = os.environ.get("EZMONEY_URL", f"https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode={FUND_CODE}")

# 欄位別名（盡量放寬）
ALIASES = {
    "code": [
        "股票代號", "證券代號", "代號", "代碼", "股票代碼", "證券代碼", "證券碼",
        "標的代號", "標的代碼", "Symbol", "Ticker", "Code", "Stock Code"
    ],
    "name": [
        "股票名稱", "證券名稱", "名稱", "標的名稱", "Name", "Stock Name", "Security Name"
    ],
    "shares": [
        "股數", "持股股數", "持有股數", "Shares", "Share", "Units", "Quantity", "張數"
    ],
    "weight": [
        "持股權重", "持股比例", "權重", "占比", "比重(%)", "占比(%)", "Weight", "Holding Weight", "Portfolio Weight"
    ],
    "close": [
        "收盤價", "收盤", "價格", "價格(元)", "Close", "Price", "Closing Price"
    ],
}

def _latest_downloaded_file(folder):
    files = glob.glob(os.path.join(folder, "*"))
    if not files:
        return None
    return max(files, key=os.path.getctime)

def _norm(s: str) -> str:
    return str(s).strip().replace("　", "").replace("\u3000", "")

def _find_header_row(df: pd.DataFrame):
    """
    嘗試在整張表中找出「表頭列」：
    規則：該列至少要包含 code/name/weight/shares 中的 2~3 個關鍵欄。
    回傳 (header_index, mapped_columns_dict)；若找不到回傳 (None, {})
    """
    best_idx = None
    best_map = {}
    for ridx in range(min(len(df), 50)):  # 掃前 50 列即可
        row = df.iloc[ridx]
        cand_map = {}
        for cidx, val in enumerate(row):
            label = _norm(val)
            if not label or label.startswith("Unnamed"):
                continue
            low = label.lower()
            def match_any(keys):
                for k in keys:
                    if k.lower() == low:
                        return True
                return False
            # 用別名清單模糊比對（包含子字串）
            def match_alias(alias_list):
                for k in alias_list:
                    if k.lower() in low:
                        return True
                return False

            if match_alias(ALIASES["code"]):
                cand_map["code"] = cidx
            elif match_alias(ALIASES["name"]):
                cand_map["name"] = cidx
            elif match_alias(ALIASES["shares"]):
                cand_map["shares"] = cidx
            elif match_alias(ALIASES["weight"]):
                cand_map["weight"] = cidx
            elif match_alias(ALIASES["close"]):
                cand_map["close"] = cidx

        # 至少需要 code/name/weight 中兩個以上才算合理
        score = sum(k in cand_map for k in ("code", "name", "weight")) + (1 if "shares" in cand_map else 0)
        if score >= 2 and (best_idx is None or score > len(best_map)):
            best_idx = ridx
            best_map = cand_map

    return best_idx, best_map

def _extract_table(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    從原始 DataFrame（可能多表頭/前置說明）抽出正式表格，欄名統一：
    ['股票代號','股票名稱','股數','持股權重'] +（可選）['收盤價']
    """
    # 1) 先嘗試直接用當前 columns
    df = df_raw.copy()
    df.columns = [_norm(c) for c in df.columns]
    mapped = {}

    def map_cols(cols):
        m = {}
        for i, col in enumerate(cols):
            low = col.lower()
            if any(k.lower() in low for k in ALIASES["code"]) and "code" not in m:
                m["code"] = i
            elif any(k.lower() in low for k in ALIASES["name"]) and "name" not in m:
                m["name"] = i
            elif any(k.lower() in low for k in ALIASES["shares"]) and "shares" not in m:
                m["shares"] = i
            elif any(k.lower() in low for k in ALIASES["weight"]) and "weight" not in m:
                m["weight"] = i
            elif any(k.lower() in low for k in ALIASES["close"]) and "close" not in m:
                m["close"] = i
        return m

    mapped = map_cols(df.columns)

    # 2) 若辨識不到足夠欄位，就用 header=None 再掃一次整張表找表頭列
    if sum(k in mapped for k in ("code","name","weight")) < 2:
        df2 = pd.read_excel(xlsx_path_global, header=None)  # 用全域路徑再讀一次無表頭
        df2 = df2.applymap(_norm)
        hdr_idx, hdr_map = _find_header_row(df2)
        if hdr_idx is None:
            # 嘗試另一個常見格式：第一欄是「資料日期：xxx」，實際表頭在後面一兩列
            # 這裡就粗暴掃描所有列找「代號/名稱/權重」關鍵字
            hdr_idx, hdr_map = _find_header_row(df2)

        if hdr_idx is not None:
            cols = df2.iloc[hdr_idx].tolist()
            body = df2.iloc[hdr_idx+1:].reset_index(drop=True)
            body.columns = [_norm(c) for c in cols]
            df = body
            mapped = map_cols(df.columns)

    # 3) 仍然缺 -> 可能「代號+名稱」合在同欄，先找一欄含「代號/名稱」混合字樣或含數字的名稱欄
    if "code" not in mapped and "name" in mapped:
        name_col = df.columns[mapped["name"]]
        # 嘗試從名稱欄拆 code + name
        tmp = df[name_col].astype(str)
        # 兩種常見型態：「2330 台積電」或「台積電(2330)」
        code_first = tmp.str.extract(r"^\s*(\d{4,6})\s*([^\d].*)$")  # 2330 台積電
        code_last  = tmp.str.extract(r"^(.+?)\s*[\(（](\d{4,6})[\)）]\s*$")  # 台積電(2330)
        if code_first.notna().all(axis=1).sum() >= code_last.notna().all(axis=1).sum():
            df["_拆_code"] = code_first[0]
            df["_拆_name"] = code_first[1]
        else:
            df["_拆_code"] = code_last[1]
            df["_拆_name"] = code_last[0]
        mapped["code"] = df.columns.get_loc("_拆_code")
        mapped["name"] = df.columns.get_loc("_拆_name")

    # 4) 組出標準欄位
    need = []
    if "code" in mapped:   need.append(df.columns[mapped["code"]])
    if "name" in mapped:   need.append(df.columns[mapped["name"]])
    if "shares" in mapped: need.append(df.columns[mapped["shares"]])
    if "weight" in mapped: need.append(df.columns[mapped["weight"]])
    if not need or ("code" not in mapped or "name" not in mapped or "weight" not in mapped):
        raise ValueError(f"無法辨識欄位，原始欄={list(df.columns)[:10]}...，mapped={mapped}")

    out = df[need].copy()
    # 正式欄名
    new_cols = []
    for c in out.columns:
        low = str(c).lower()
        if any(k.lower() in low for k in ALIASES["code"]):
            new_cols.append("股票代號")
        elif any(k.lower() in low for k in ALIASES["name"]):
            new_cols.append("股票名稱")
        elif any(k.lower() in low for k in ALIASES["shares"]):
            new_cols.append("股數")
        elif any(k.lower() in low for k in ALIASES["weight"]):
            new_cols.append("持股權重")
        else:
            new_cols.append(c)
    out.columns = new_cols

    # 若 shares 缺欄，補 0；若 close 有找到也補上
    if "股數" not in out.columns and "shares" in mapped:
        out["股數"] = pd.to_numeric(df.iloc[:, mapped["shares"]], errors="coerce")
    if "股數" not in out.columns:
        out["股數"] = 0

    # 嘗試加上收盤價（可選）
    # 注意：不強制要求，若沒找到就略過
    close_val = None
    for cand in ALIASES["close"]:
        for c in df.columns:
            if cand.lower() in str(c).lower():
                close_val = pd.to_numeric(df[c], errors="coerce")
                break
        if close_val is not None:
            break
    if close_val is not None:
        out["收盤價"] = close_val

    # 乾淨化
    out["股票代號"] = out["股票代號"].astype(str).str.strip()
    out["股票名稱"] = out["股票名稱"].astype(str).str.strip()
    out["股數"] = pd.to_numeric(out["股數"].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0).astype(int)
    out["持股權重"] = pd.to_numeric(
        out["持股權重"].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False),
        errors="coerce"
    ).fillna(0.0)

    # 移除空白/表尾摘要列
    out = out[(out["股票代號"].str.len() > 0) & (out["股票名稱"].str.len() > 0)]
    # 去除明顯非代號（若需要可改成更寬鬆）
    out = out[out["股票代號"].str.match(r"^\d{4,6}$")]

    return out.reset_index(drop=True)

def _build_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
    prefs = {
        "download.default_directory": str(Path(DOWNLOAD_DIR).resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
        "download_restrictions": 0,
        "plugins.always_open_pdf_externally": True,
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
    """到 Info 頁 → 點『基金投資組合』分頁 → 點『匯出XLSX』→ 等待檔案下載完成（以大小穩定為準）"""
    driver = _build_driver()
    driver.get(ETF_URL)
    print(f"[etf_tracker] Open page: {ETF_URL}")

    try:
        wait = WebDriverWait(driver, 25)

        tab = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(.,'基金投資組合')]")))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", tab)
        time.sleep(0.5)
        tab.click()
        print("[etf_tracker] Clicked tab: 基金投資組合")

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

        t_click = time.time()
        btn.click()
        print("[etf_tracker] Clicked: 匯出XLSX")

        deadline = time.time() + 90
        quiet_hits = 0
        last_size = None
        cand_path = None

        while time.time() < deadline:
            time.sleep(1.0)
            xlsxs = [p for p in glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx"))
                     if os.path.getmtime(p) >= t_click]
            if xlsxs:
                xlsxs.sort(key=os.path.getmtime, reverse=True)
                cand_path = xlsxs[0]
                size = os.path.getsize(cand_path)
                if last_size is not None and size == last_size:
                    quiet_hits += 1
                else:
                    quiet_hits = 1
                last_size = size
                if quiet_hits >= 3:
                    print(f"[etf_tracker] Detected stable xlsx: {cand_path} (size={size})")
                    driver.quit()
                    return cand_path
            crs = glob.glob(os.path.join(DOWNLOAD_DIR, "*.crdownload"))
            print(f"[etf_tracker] polling... xlsx={len(xlsxs)} crdownload={len(crs)} quiet={quiet_hits}")

        _screenshot(driver, "download_timeout")
        raise RuntimeError("下載逾時，未偵測到穩定完成的 .xlsx 檔")

    except Exception as e:
        _screenshot(driver, "exception")
        raise RuntimeError(f"下載 Excel 失敗: {e}") from e
    finally:
        try:
            driver.quit()
        except Exception:
            pass

# 讓 _extract_table 在 fallback 重讀時能拿到檔案路徑
xlsx_path_global = None

def main():
    global xlsx_path_global
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
    xlsx_path_global = fixed_excel
    df0 = pd.read_excel(fixed_excel)              # 先用推測的表頭讀
    try:
        df = _extract_table(df0)
    except Exception as e:
        print(f"[etf_tracker] direct parse failed: {e}")
        # 換另一個策略：完全不設 header，交給偵測器找表頭列
        df_any = pd.read_excel(fixed_excel, header=None)
        try:
            df = _extract_table(df_any)
        except Exception as e2:
            print(f"[etf_tracker] headerless parse failed: {e2}")
            raise

    # ===== Step3. 存成 CSV（供後續比較）=====
    csv_out = os.path.join(DATA_DIR, f"{today}.csv")
    df.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print(f"[etf_tracker] Saved cleaned CSV: {csv_out}")

if __name__ == "__main__":
    main()