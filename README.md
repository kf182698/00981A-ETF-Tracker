# 00981A ETF Tracker (Selenium)

每天台北時間 20:00 自動打開 EZMoney 00981A 頁面，點擊「匯出XLSX」，轉為 CSV 並與昨日比對，輸出差異報表。

### 重要檔案
- `etf_tracker.py`：下載並整理資料（股票代號、股票名稱、股數、持股權重）
- `compare_holdings.py`：與昨日檔案比對，輸出 `diff/diff_YYYY-MM-DD.csv`
- `.github/workflows/main.yml`：GitHub Actions 排程

### 手動測試
Repo → **Actions** → `Daily ETF Tracker (Selenium)` → **Run workflow**  
成功後檢查：
- `data/YYYY-MM-DD.csv`
- 若上一天已有檔，會有 `diff/diff_YYYY-MM-DD.csv`
