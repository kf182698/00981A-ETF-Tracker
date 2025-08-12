# config.py
# ---- 報表與門檻 ----
TOP_N = 10
PCT_DECIMALS = 2

# 噪音門檻（百分點；例 0.01 代表 0.01%）
THRESH_UPDOWN_EPS = 0.01

# 「首次新增持股」最低權重（百分點；例 0.5 代表 0.5%）
NEW_WEIGHT_MIN = 0.4

# 關鍵賣出警示：若「今日權重 <= 此門檻」且「昨日權重 > 門檻」，觸發（百分點）
SELL_ALERT_THRESHOLD = 0.1

# 報表與圖片輸出資料夾
REPORT_DIR = "reports"
CHART_DIR = "charts"

# 圖表平滑：移動平均視窗（>=1；建議 3~5）
SMOOTH_ROLLING_WINDOW = 3
