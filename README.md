# 00981A ETF Tracker

本專案每天自動從 EZMoney 下載 ETF「00981A」的最新投資組合（成分股），比對昨日與今日差異，並輸出報表。

## 功能

- 自動下載 EZMoney 投資組合（XLSX 檔）
- 擷取欄位：股票代號、名稱、股數、持股權重
- 每日自動比對差異（股數、權重增減或異動）
- 自動存檔至 GitHub repo

## 使用方式

1. Fork 本專案
2. 進入 Settings → Secrets → Actions 新增：

- `EMAIL_USERNAME`: 你的 Gmail 帳號
- `EMAIL_PASSWORD`: 應用程式密碼

## 執行結果

- 每日資料儲存於 `/data/` 資料夾
- 差異分析輸出於 `/diff/`
