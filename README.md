# 00981A ETF Tracker — Clean Architecture

每天台北 20:00 自動下載 EZMoney 00981A 投資組合，補上「收盤價」，
並以「官方快照日」去重，保存原始 XLSX 與乾淨 CSV，避免非交易日重算。
報表與寄信分離為另一個 workflow（可暫時關閉）。
