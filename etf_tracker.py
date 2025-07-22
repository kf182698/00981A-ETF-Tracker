import requests
import pandas as pd
from datetime import datetime
import os

TODAY = datetime.today().strftime('%Y-%m-%d')
FUND_CODE = "49YTW"
BASE_URL = "https://www.ezmoney.com.tw/ETF/Ajax/ExportFundHoldings"

os.makedirs("data", exist_ok=True)
os.makedirs("diff", exist_ok=True)

resp = requests.get(BASE_URL, params={"fundCode": FUND_CODE})
xlsx_path = f"data/{TODAY}.xlsx"
with open(xlsx_path, "wb") as f:
    f.write(resp.content)

df = pd.read_excel(xlsx_path)
df = df[['股票代號', '股票名稱', '股數', '持股權重']]
df.to_csv(f"data/{TODAY}.csv", index=False, encoding='utf-8-sig')
