# etf_tracker.py — 下載 00981A 每日持股 → 清洗 → 抓當日收盤價(快取) → 輸出 data/YYYY-MM-DD.csv
import os, re, time, glob, json, shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === 路徑 ===
DOWNLOAD_DIR = "downloads"
DATA_DIR     = "data"
SCREEN_DIR   = "screenshots"
PRICE_DIR    = "prices"
Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
Path(SCREEN_DIR).mkdir(parents=True, exist_ok=True)
Path(PRICE_DIR).mkdir(parents=True, exist_ok=True)

# === 網址 ===
FUND_CODE = os.environ.get("FUND_CODE", "49YTW")  # 00981A
ETF_URL   = os.environ.get("EZMONEY_URL", f"https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode={FUND_CODE}")

# === 欄位別名（放寬） ===
ALIASES = {
    "code":   ["股票代號","證券代號","代號","代碼","股票代碼","證券代碼","Symbol","Ticker","Code","Stock Code"],
    "name":   ["股票名稱","證券名稱","名稱","Name","Stock Name","Security Name"],
    "shares": ["股數","持股股數","持有股數","Shares","Units","Quantity","張數"],
    "weight": ["持股權重","持股比例","權重","占比","比重(%)","占比(%)","Weight","Holding Weight","Portfolio Weight"],
    "close":  ["收盤價","收盤","價格","Price","Close","Closing Price"],
}

def _norm(s): return str(s).strip().replace("　","").replace("\u3000","")

def _build_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    prefs = {
        "download.default_directory": str(Path(DOWNLOAD_DIR).resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
    }
    opts.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=opts)

def _screenshot(driver, tag):
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    png = os.path.join(SCREEN_DIR, f"{tag}_{ts}.png")
    html= os.path.join(SCREEN_DIR, f"{tag}_{ts}.html")
    try:
        driver.save_screenshot(png)
        with open(html,"w",encoding="utf-8") as f: f.write(driver.page_source)
        print("[etf_tracker] screenshot:", png, html)
    except Exception as e:
        print("[etf_tracker] screenshot failed:", e)

def _download_excel():
    d = _build_driver()
    d.get(ETF_URL); print("[etf_tracker] open:", ETF_URL)
    try:
        w = WebDriverWait(d, 25)
        tab = w.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(.,'基金投資組合')]")))
        d.execute_script("arguments[0].scrollIntoView({block:'center'});", tab); time.sleep(0.4); tab.click()
        print("[etf_tracker] tab clicked")

        selectors = [
            (By.XPATH, "//a[contains(.,'匯出') and contains(.,'XLSX')]"),
            (By.CSS_SELECTOR, "a[href*='ExportFundHoldings']"),
            (By.XPATH, "//button[contains(.,'匯出') and contains(.,'XLSX')]"),
        ]
        btn = None
        for by, sel in selectors:
            try:
                btn = w.until(EC.element_to_be_clickable((by, sel))); break
            except Exception: pass
        if not btn:
            _screenshot(d,"no_export_btn"); raise RuntimeError("找不到匯出XLSX按鈕")

        t_click = time.time(); btn.click(); print("[etf_tracker] export clicked")
        # 大小穩定 3 次視為完成
        deadline = time.time()+90; last_size=None; quiet=0; cand=None
        while time.time()<deadline:
            time.sleep(1)
            xlsxs = [p for p in glob.glob(os.path.join(DOWNLOAD_DIR,"*.xlsx"))
                     if os.path.getmtime(p)>=t_click]
            if xlsxs:
                xlsxs.sort(key=os.path.getmtime, reverse=True)
                cand = xlsxs[0]; size = os.path.getsize(cand)
                quiet = quiet+1 if (last_size is not None and size==last_size) else 1
                last_size = size
                if quiet>=3: d.quit(); return cand
            print("[etf_tracker] polling...")
        _screenshot(d,"download_timeout"); raise RuntimeError("下載逾時")
    except Exception as e:
        _screenshot(d,"exception"); raise
    finally:
        try: d.quit()
        except: pass

def _find_header_row(df):
    best_idx, best = None, {}
    for ridx in range(min(50,len(df))):
        row = df.iloc[ridx]
        m={}
        for cidx,val in enumerate(row):
            lab=_norm(val)
            if not lab or lab.startswith("Unnamed"): continue
            low=lab.lower()
            def hit(keys): return any(k.lower() in low for k in keys)
            if hit(ALIASES["code"])   and "code" not in m:   m["code"]=cidx
            if hit(ALIASES["name"])   and "name" not in m:   m["name"]=cidx
            if hit(ALIASES["shares"]) and "shares" not in m: m["shares"]=cidx
            if hit(ALIASES["weight"]) and "weight" not in m: m["weight"]=cidx
            if hit(ALIASES["close"])  and "close" not in m:  m["close"]=cidx
        score = sum(k in m for k in ("code","name","weight")) + (1 if "shares" in m else 0)
        if score>=2 and (best_idx is None or len(m)>len(best)):
            best_idx, best = ridx, m
    return best_idx, best

def _extract_table(xlsx_path):
    df0 = pd.read_excel(xlsx_path)
    df0.columns = [_norm(c) for c in df0.columns]
    def map_cols(cols):
        m={}
        for i,col in enumerate(cols):
            low=str(col).lower()
            if any(k.lower() in low for k in ALIASES["code"])   and "code" not in m:   m["code"]=i
            if any(k.lower() in low for k in ALIASES["name"])   and "name" not in m:   m["name"]=i
            if any(k.lower() in low for k in ALIASES["shares"]) and "shares" not in m: m["shares"]=i
            if any(k.lower() in low for k in ALIASES["weight"]) and "weight" not in m: m["weight"]=i
            if any(k.lower() in low for k in ALIASES["close"])  and "close" not in m:  m["close"]=i
        return m
    mapped = map_cols(df0.columns)
    if sum(k in mapped for k in ("code","name","weight"))<2:
        df1 = pd.read_excel(xlsx_path, header=None).applymap(_norm)
        idx, m2 = _find_header_row(df1)
        if idx is None: raise ValueError("無法辨識表頭")
        cols = df1.iloc[idx].tolist()
        body = df1.iloc[idx+1:].reset_index(drop=True)
        body.columns=[_norm(c) for c in cols]
        df0 = body
        mapped = map_cols(df0.columns)
    # 合欄拆解
    if "code" not in mapped and "name" in mapped:
        name_col = df0.columns[mapped["name"]]
        s = df0[name_col].astype(str)
        a = s.str.extract(r"^\s*(\d{4,6})\s*([^\d].*)$")
        b = s.str.extract(r"^(.+?)\s*[\(（](\d{4,6})[\)）]\s*$")
        if a.notna().all(1).sum() >= b.notna().all(1).sum():
            df0["_code"]=a[0]; df0["_name"]=a[1]
        else:
            df0["_code"]=b[1]; df0["_name"]=b[0]
        mapped["code"]=df0.columns.get_loc("_code"); mapped["name"]=df0.columns.get_loc("_name")

    need=[]
    for k in ("code","name","shares","weight"):
        if k in mapped: need.append(df0.columns[mapped[k]])
    if "code" not in mapped or "name" not in mapped or "weight" not in mapped:
        raise ValueError(f"欄位不足，columns={list(df0.columns)[:10]} mapped={mapped}")

    df = df0[need].copy()
    # 正式欄名
    new=[]
    for c in df.columns:
        low=str(c).lower()
        if any(k.lower() in low for k in ALIASES["code"]):   new.append("股票代號")
        elif any(k.lower() in low for k in ALIASES["name"]): new.append("股票名稱")
        elif any(k.lower() in low for k in ALIASES["shares"]): new.append("股數")
        elif any(k.lower() in low for k in ALIASES["weight"]): new.append("持股權重")
        else: new.append(c)
    df.columns=new

    df["股票代號"]=df["股票代號"].astype(str).str.strip()
    df["股票名稱"]=df["股票名稱"].astype(str).str.strip()
    df["股數"]=pd.to_numeric(df.get("股數",0).astype(str).str.replace(",","",regex=False),errors="coerce").fillna(0).astype(int)
    df["持股權重"]=pd.to_numeric(df["持股權重"].astype(str).str.replace(",","",regex=False).str.replace("%","",regex=False),errors="coerce").fillna(0.0)
    df = df[(df["股票代號"].str.match(r"^\d{4,6}$")) & (df["股票名稱"].str.len()>0)].reset_index(drop=True)
    return df

# === 價格抓取與快取 ===
def _yahoo_quote(codes):
    # 對每個代碼同時試 .TW / .TWO；哪個有價就採用
    out={}
    sess = requests.Session()
    headers={"User-Agent":"Mozilla/5.0"}
    for code in codes:
        syms = [f"{code}.TW", f"{code}.TWO"]
        price=None
        for s in syms:
            url = "https://query1.finance.yahoo.com/v7/finance/quote"
            try:
                r = sess.get(url, params={"symbols": s}, timeout=10, headers=headers)
                if r.status_code!=200: continue
                js=r.json()
                res = js.get("quoteResponse",{}).get("result",[])
                if not res: continue
                p = res[0].get("regularMarketPrice") or res[0].get("postMarketPrice")
                if p: price=float(p); break
            except Exception: continue
        if price is not None: out[code]=price
    return out

def _load_price_cache(ymd):
    p = os.path.join(PRICE_DIR, f"{ymd}.json")
    if os.path.exists(p):
        try:
            with open(p,"r",encoding="utf-8") as f: return json.load(f)
        except: return {}
    return {}

def _save_price_cache(ymd, data):
    p = os.path.join(PRICE_DIR, f"{ymd}.json")
    with open(p,"w",encoding="utf-8") as f: json.dump(data,f,ensure_ascii=False,indent=2)

def _fetch_prices_for(df, ymd):
    # 先讀快取
    cache = _load_price_cache(ymd)
    need = sorted({c for c in df["股票代號"].astype(str) if str(c) not in cache})
    if need:
        got = _yahoo_quote(need)
        cache.update(got)
        _save_price_cache(ymd, cache)
    # 合成價格列，缺就用近日回補（簡單：讀上一個 cache 檔）
    closes=[]
    for code in df["股票代號"].astype(str):
        if code in cache:
            closes.append(cache[code])
        else:
            # 回補：往前找最近一天快取
            prev_files = sorted(glob.glob(os.path.join(PRICE_DIR,"*.json")))
            prev_files = [p for p in prev_files if os.path.basename(p).split(".")[0] < ymd]
            prev_files.sort(reverse=True)
            val=None
            for pf in prev_files:
                try:
                    js=json.load(open(pf,"r",encoding="utf-8"))
                    if str(code) in js: val=js[str(code)]; break
                except: pass
            closes.append(val if val is not None else None)
    return closes

def main():
    ymd = datetime.now().strftime("%Y-%m-%d")
    raw = _download_excel()

    fixed = os.path.join(DOWNLOAD_DIR, f"{ymd}.xlsx")
    try:
        if os.path.exists(fixed): os.remove(fixed)
        shutil.move(raw, fixed)
    except Exception as e:
        print("[etf_tracker] rename failed:", e); fixed = raw
    print("[etf_tracker] saved excel:", fixed)

    df = _extract_table(fixed)

    # 抓價 + 寫入
    closes = _fetch_prices_for(df, ymd.replace("-",""))
    df["收盤價"] = pd.to_numeric(pd.Series(closes), errors="coerce")

    # 輸出
    csv_out = os.path.join(DATA_DIR, f"{ymd}.csv")
    df.to_csv(csv_out, index=False, encoding="utf-8-sig")
    print("[etf_tracker] saved csv:", csv_out)

if __name__ == "__main__":
    main()