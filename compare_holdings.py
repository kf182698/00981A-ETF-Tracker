import os
import pandas as pd
from datetime import datetime, timedelta

def load_csv(date_str):
    path = os.path.join("data", f"{date_str}.csv")
    return pd.read_csv(path) if os.path.exists(path) else None

today = datetime.today()
yesterday = today - timedelta(days=1)
today_str = today.strftime('%Y-%m-%d')
yesterday_str = yesterday.strftime('%Y-%m-%d')

df_today = load_csv(today_str)
df_yesterday = load_csv(yesterday_str)

if df_today is not None and df_yesterday is not None:
    # 以「股票代號」為主鍵合併
    merged = pd.merge(
        df_yesterday, df_today,
        on="股票代號", how="outer",
        suffixes=('_昨', '_今'), indicator=True
    )

    # 欄位可能為 NaN，先補 0 再計算
    for c in ["股數_昨", "股數_今", "持股權重_昨", "持股權重_今"]:
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

    merged["股數變動"] = merged["股數_今"] - merged["股數_昨"]
    merged["持股權重變動"] = merged["持股權重_今"] - merged["持股權重_昨"]

    os.makedirs("diff", exist_ok=True)
    out_path = os.path.join("diff", f"diff_{today_str}.csv")
    merged.to_csv(out_path, index=False, encoding="utf-8-sig")
