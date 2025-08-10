# config.py
# ---- 你可自由調整這些門檻／參數 ----

# 上/下升清單的 Top N
TOP_N = 10

# 噪音門檻（百分比；±此值內視為 0）
THRESH_UPDOWN_EPS = 0.01  # 0.01% = 0.0001 as fraction? 這裡單位就是「百分比」，別改成小數

# 首次新增持股的最低權重（百分比）
NEW_WEIGHT_MIN = 0.5  # 0.5%

# 報表輸出資料夾
REPORT_DIR = "reports"

# 權重顯示小數位
PCT_DECIMALS = 2
