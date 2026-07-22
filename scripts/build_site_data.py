#!/usr/bin/env python3
"""Build the single JSON payload consumed by the public 00981A tracker site."""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

SNAPSHOT_DIR = Path("data_snapshots")
OUTPUT = Path("web/etf-tracker.json")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def number(value: str | None, default: float = 0) -> float:
    try:
        return float(str(value or "").replace(",", "").replace("%", "").strip())
    except ValueError:
        return default


def read_snapshot(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for raw in reader:
            code = str(raw.get("股票代號") or raw.get("證券代號") or raw.get("代號") or "").strip()
            match = re.search(r"([1-9]\d{3})", code)
            if not match:
                continue
            rows.append({
                "code": match.group(1),
                "name": str(raw.get("股票名稱") or raw.get("證券名稱") or raw.get("名稱") or "").strip(),
                "units": int(number(raw.get("股數") or raw.get("持有股數"))),
                "weight": round(number(raw.get("持股權重") or raw.get("投資比例") or raw.get("權重")), 6),
            })
    rows.sort(key=lambda row: (-row["weight"], -row["units"], row["code"]))
    for index, row in enumerate(rows, 1):
        row["rank"] = index
    return rows


def event_type(previous: dict | None, current: dict | None, delta_units: int, delta_weight: float) -> str:
    if previous is None and current is not None:
        return "新增持股"
    if previous is not None and current is None:
        return "退出持股"
    if delta_units > 0:
        return "加碼"
    if delta_units < 0:
        return "減碼"
    if delta_weight > 0:
        return "加碼"
    if delta_weight < 0:
        return "減碼"
    return "持平"


def main() -> None:
    files = sorted(
        path for path in SNAPSHOT_DIR.glob("*.csv")
        if DATE_RE.match(path.stem)
    )
    if len(files) < 2:
        raise SystemExit("At least two data_snapshots/YYYY-MM-DD.csv files are required")

    snapshots: list[tuple[str, list[dict]]] = [
        (path.stem, read_snapshot(path)) for path in files
    ]
    maps = [(date, {row["code"]: row for row in rows}) for date, rows in snapshots]
    all_codes = sorted({code for _, rows in maps for code in rows})
    names: dict[str, str] = {}
    history: dict[str, list[dict]] = {code: [] for code in all_codes}
    events: dict[str, list[dict]] = {code: [] for code in all_codes}

    for index, (date, rows) in enumerate(maps):
        previous_rows = maps[index - 1][1] if index else {}
        for code, row in rows.items():
            names[code] = row["name"] or names.get(code, code)
            point = {"date": date, **row}
            history[code].append(point)
            if index:
                previous = previous_rows.get(code)
                delta_units = row["units"] - (previous["units"] if previous else 0)
                delta_weight = round(row["weight"] - (previous["weight"] if previous else 0), 6)
                if previous is None or delta_units or delta_weight:
                    events[code].append({
                        "date": date,
                        "type": event_type(previous, row, delta_units, delta_weight),
                        "deltaUnits": delta_units,
                        "deltaWeight": delta_weight,
                        "rank": row["rank"],
                    })
        if index:
            for code, previous in previous_rows.items():
                if code not in rows:
                    events[code].append({
                        "date": date,
                        "type": "退出持股",
                        "deltaUnits": -previous["units"],
                        "deltaWeight": round(-previous["weight"], 6),
                        "rank": None,
                    })

    latest_date, latest_map = maps[-1]
    previous_date, previous_map = maps[-2]
    stocks = []
    recent_events = []
    stats = {"holdings": len(latest_map), "added": 0, "removed": 0, "increased": 0, "reduced": 0}

    for code in all_codes:
        stock_history = history[code]
        first = stock_history[0]
        current = latest_map.get(code)
        previous = previous_map.get(code)
        delta_units = (current["units"] if current else 0) - (previous["units"] if previous else 0)
        delta_weight = round((current["weight"] if current else 0) - (previous["weight"] if previous else 0), 6)
        prior_units = previous["units"] if previous else 0
        unit_rate = delta_units / prior_units if prior_units else 0

        if previous is None and current is not None:
            flag = "新增持股"
            stats["added"] += 1
        elif previous is not None and current is None:
            flag = "退出持股"
            stats["removed"] += 1
        elif unit_rate >= 0.10 or delta_weight >= 0.25:
            flag = "大幅加碼"
            stats["increased"] += 1
        elif unit_rate <= -0.10 or delta_weight <= -0.25:
            flag = "大幅減持"
            stats["reduced"] += 1
        else:
            flag = "觀察"

        latest_event = events[code][-1] if events[code] else None
        stock = {
            "code": code,
            "name": names.get(code, code),
            "history": stock_history,
            "events": events[code],
            "firstDate": first["date"],
            "firstWeight": first["weight"],
            "firstUnits": first["units"],
            "firstRank": first["rank"],
            "lastDate": stock_history[-1]["date"],
            "isHeld": current is not None,
            "current": current,
            "deltaUnits": delta_units,
            "deltaWeight": delta_weight,
            "maxWeight": max(stock_history, key=lambda point: point["weight"]),
            "maxUnits": max(stock_history, key=lambda point: point["units"]),
            "flag": flag,
            "latestEvent": latest_event,
        }
        stocks.append(stock)

        if latest_event and latest_event["date"] == latest_date:
            recent_events.append({**latest_event, "code": code, "name": stock["name"]})

    stocks.sort(key=lambda stock: (-(stock["current"] or {}).get("weight", 0), stock["code"]))
    recent_events.sort(key=lambda event: (-abs(event["deltaWeight"]), -abs(event["deltaUnits"]), event["code"]))
    latest_holdings = [{"date": latest_date, **row} for row in sorted(latest_map.values(), key=lambda row: row["rank"])]

    payload = {
        "source": {
            "repository": "kf182698/00981A-ETF-Tracker",
            "path": "data_snapshots",
            "generatedBy": "scripts/build_site_data.py",
        },
        "latestDate": latest_date,
        "previousDate": previous_date,
        "snapshotCount": len(snapshots),
        "latestHoldings": latest_holdings,
        "stocks": stocks,
        "recentEvents": recent_events,
        "stats": stats,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"[site-data] wrote {OUTPUT} from {len(snapshots)} snapshots; latest={latest_date}")


if __name__ == "__main__":
    main()
