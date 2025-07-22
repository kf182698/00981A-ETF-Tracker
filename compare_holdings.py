import pandas as pd
from datetime import datetime, timedelta
import os

def load_csv(date_str):
    path = f"data/{date_str}.csv"
    return pd.read_csv(path) if os.path.exists(path) else None

today = datetime.today()
yesterday = today - timedelta(days=1)
today_str = today.strftime('%Y-%m-%d')
yesterday_str = yesterday.strftime('%Y-%m-%d')

df_today = load_csv(today_str)
df_yesterday = load_csv(yesterday_str)

if df_today is not None and df_yesterday is not None:
    df_merge = pd.merge(df_yesterday, df_today, on="股票代號", how="outer", suffixes=('_昨', '_今'), indicator=True)
    df_merge['股數變動'] = df_merge['股數_今'].fillna(0) - df_merge['股數_昨'].fillna(0)
    df_merge['持股權重變動'] = df_merge['持股權重_今'].fillna(0) - df_merge['持股權重_昨'].fillna(0)
    df_merge.to_csv(f"diff/diff_{today_str}.csv", index=False, encoding='utf-8-sig')
