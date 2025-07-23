import requests
import pandas as pd
from datetime import datetime
import os
import urllib3

# 關閉 SSL 警告（因為我們要跳過 verify）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TODAY = datetime.today().strftime('%Y-%m-%d')
FUND_CODE = "49YTW"
BASE_URL = "https://www.ezmoney.com.tw/ETF/Ajax/ExportFundHoldings"

os.makedirs("data", exist_ok=True)
os.makedirs("diff", exist_ok=True)

# 加入 headers 模擬 Chrome，避免被網站擋
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

# 加上 verify=False 強制略過憑證錯誤
resp = requests.get(BASE_URL, params={"fundCode": FUND_CODE}, headers=headers, verify=False)

# 檢查是否成功下載 Excel
if not resp.headers.get("Content-Type", "").startswith("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
    raise Exception("下載失敗，EZMoney 可能封鎖請求或回傳非 Excel 格式。")

# 儲存 XLSX 並轉成 CSV
xlsx_path = f"data/{TODAY}.xlsx"
with open(xlsx_path, "wb") as f:
    f.write(resp.content)

df = pd.read_excel(xlsx_path)
df = df[['股票代號', '股票名稱', '股數', '持股權重']]
df.to_csv(f"data/{TODAY}.csv", index=False, encoding='utf-8-sig')
