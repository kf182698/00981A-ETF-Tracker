"""
backfill_cost_basis.py — 從所有歷史 change_table_*.csv 重建成本帳簿

使用方式：
    python scripts/backfill_cost_basis.py
    python scripts/backfill_cost_basis.py --reports-dir reports --output data/cost_basis.csv

邏輯：
  1. 掃描 reports/change_table_*.csv，依日期升序排列
  2. 依序呼叫 update_cost_basis() 累積成本
  3. 輸出最終 data/cost_basis.csv 與 data/realized_gains_log.csv

注意：
  若某日 change_table 缺少「今日收盤價」欄位，該日會跳過成本計算（同 update_cost_basis.py 保護邏輯）。
"""

import argparse
import glob
import re
import sys
from pathlib import Path

import pandas as pd

# 直接 import 同目錄下的 update_cost_basis 模組
sys.path.insert(0, str(Path(__file__).parent))
from update_cost_basis import load_cost_basis, update_cost_basis


REQUIRED_COLS = {"股票代號", "今日股數", "買賣超股數", "首次買進", "股票名稱", "今日收盤價"}


def _load_change_table(path: Path) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
        df.columns = [str(c).replace("﻿", "").strip() for c in df.columns]
        missing = REQUIRED_COLS - set(df.columns)
        if missing:
            print(f"[backfill] 跳過 {path.name}：缺少欄位 {missing}")
            return None
        return df
    except Exception as e:
        print(f"[backfill] 跳過 {path.name}：讀取錯誤 {e}")
        return None


def _extract_date(filename: str) -> str | None:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    return m.group(1) if m else None


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill cost basis from all historical change_table CSVs.")
    parser.add_argument("--reports-dir", type=Path, default=Path("reports"),
                        help="Directory containing change_table_*.csv files")
    parser.add_argument("--output", type=Path, default=Path("data/cost_basis.csv"),
                        help="Output path for cost_basis.csv")
    parser.add_argument("--gains-log", type=Path, default=Path("data/realized_gains_log.csv"),
                        help="Output path for realized_gains_log.csv")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing cost_basis.csv instead of resuming from it")
    args = parser.parse_args()

    # 掃描所有 change_table 檔
    all_files = sorted(glob.glob(str(args.reports_dir / "change_table_*.csv")))
    if not all_files:
        print(f"[backfill] 找不到任何 change_table_*.csv 於 {args.reports_dir}")
        sys.exit(1)

    # 只處理能解析出日期的檔案，依日期排序
    dated = []
    for f in all_files:
        d = _extract_date(Path(f).name)
        if d:
            dated.append((d, Path(f)))
    dated.sort(key=lambda x: x[0])

    print(f"[backfill] 發現 {len(dated)} 個 change_table 檔，將依序重建成本帳")

    # 決定起始 cost_basis
    if args.overwrite or not args.output.exists():
        cost_df = pd.DataFrame(columns=["股票代號", "股票名稱", "股數", "成本市值"])
        if args.overwrite and args.output.exists():
            print(f"[backfill] --overwrite 模式：清除舊帳簿 {args.output}")
    else:
        cost_df = load_cost_basis(args.output)
        # 找出已處理過的最後日期，從後面繼續
        existing_dates = {_extract_date(Path(f).name) for _, f in dated if args.output.exists()}
        print(f"[backfill] 從現有帳簿繼續（已有 {len(cost_df)} 筆）")

    processed = 0
    skipped = 0
    for date_str, fpath in dated:
        change_df = _load_change_table(fpath)
        if change_df is None:
            skipped += 1
            continue
        cost_df = update_cost_basis(cost_df, change_df, date_str, args.gains_log)
        processed += 1
        print(f"[backfill] {date_str} 處理完成（持股數：{len(cost_df[cost_df['股數'].astype(str).str.strip() != '0'])}）")

    # 移除股數為 0 的紀錄（已完全清倉）
    args.output.parent.mkdir(parents=True, exist_ok=True)
    cost_df["股數"] = pd.to_numeric(cost_df["股數"], errors="coerce").fillna(0)
    cost_df.to_csv(args.output, index=False, encoding="utf-8-sig")

    print(f"\n[backfill] 完成：處理 {processed} 筆，跳過 {skipped} 筆")
    print(f"[backfill] 成本帳簿輸出：{args.output}  共 {len(cost_df)} 檔股票")
    if args.gains_log.exists():
        n = len(pd.read_csv(args.gains_log, encoding="utf-8-sig"))
        print(f"[backfill] 實現損益紀錄：{args.gains_log}  共 {n} 筆")


if __name__ == "__main__":
    main()
