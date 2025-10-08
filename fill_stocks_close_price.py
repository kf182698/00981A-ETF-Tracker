import pandas as pd
import requests
from datetime import datetime
import os
import time

def get_twse_close_price(stock_no, date_str):
    # TWSE API, 格式 e.g. date=20251008, stockNo=2330
    url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date_str}&stockNo={stock_no}"
    try:
        res = requests.get(url, timeout=10)
        data = res.json()
        # data['data'] 內容 [日期, ...收盤價在第7欄(6)]
        for row in data.get('data', []):
            # 日期格式 "2025/10/08"
            if row[0].replace('/', '') == date_str:
                try:
                    return float(row[6].replace(",", ""))
                except:
                    return None
        return None
    except Exception as e:
        print(f"Error fetching {stock_no} on {date_str}: {e}")
        return None

def auto_fill_csv(date_csv):
    # 解析日期，例如 "2025-10-08.csv" → "20251008"
    base = os.path.basename(date_csv)
    date_str = base.replace('.csv','').replace('-','')
    df = pd.read_csv(date_csv)

    close_prices = []
    for code in df['股票代號']:
        price = get_twse_close_price(str(code).zfill(4), date_str)
        close_prices.append(price)
        time.sleep(1) # 避免API流量過大，可自行調整

    df['收盤價'] = close_prices
    # 存檔於同資料夾，檔案名稱加 "_with_price"
    out_csv = date_csv.replace('.csv', '_with_price.csv')
    df.to_csv(out_csv, index=False, encoding='utf-8-sig')
    print(f"Done: {out_csv}")

if __name__ == "__main__":
    # 自動選取 data 資料夾最新檔
    data_folder = 'data'
    files = sorted([f for f in os.listdir(data_folder) if f.endswith('.csv')])
    if not files:
        print("No csv found.")
    else:
        latest_csv = os.path.join(data_folder, files[-1])
        auto_fill_csv(latest_csv)
