# build_change_table.py — Clean fixed version
# 產出指定日期的「今 vs 昨」持股變化表與摘要
# - 支援 REPORT_DATE 格式：YYYY-MM-DD / YYYY-M-D / YYYYMMDD
# - 預設優先使用 data_snapshots/（去重後的“真快照序列”），否則回退 data/
# - 尋找比較基期：在相同資料夾內挑選 < 今日 的最近一份（對跨假日友善）

from __future__ import annotations
import os
import re
import glob
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

DATA_DIR        = Path("data")
SNAP_DATA_DIR   = Path("data_snapshots")
REPORT_DIR      = Path("reports")
PRICE_DIR       = Path("prices")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------- 日期工具 ------------------------- #
def _normalize_report_date(raw: str) -> str:
    """
    接受 'YYYY-MM-DD' / 'YYYY-M-D' / 'YYYYMMDD'，回傳 'YYYY-MM-DD'
    """
    if raw is None:
        raise ValueError("REPORT_DATE is None")
    s = str(raw).strip()
    # yyyy-m-d / yyyy-mm-dd
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y, mo, d = m.groups()
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    # yyyymmdd
    m = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    raise ValueError(f"無法解析 REPORT_DATE：{raw}")

def _pick_default_date_and_base():
    """
    未提供 REPORT_DATE 時：
      1) 優先使用 data_snapshots/ 最新一份
      2) 否則使用 data/ 最新一份
    回傳 (date_str, base_dir)
    """
    snaps = sorted(glob.glob(str(SNAP_DATA_DIR / "*.csv")))
    if snaps:
        date_str = Path(snaps[-1]).stem
        return _normalize_report_date(date_str), SNAP_DATA_DIR

    dailies = sorted(glob.glob(str(DATA_DIR / "*.csv")))
    if dailies:
        date_str = Path(dailies[-1]).stem
        return _normalize_report_date(date_str), DATA_DIR

    raise FileNotFoundError("找不到任何 CSV（data_snapshots/ 或 data/）")

def _choose_base_dir(date_str: str) -> Path:
    """
    對於顯式指定的 R
