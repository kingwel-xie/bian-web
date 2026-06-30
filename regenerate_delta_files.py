#!/usr/bin/env python3
"""Regenerate all delta files with the corrected adjacent-snapshot logic."""

from __future__ import annotations

import json
import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(APP_DIR))

from auto_leaderboard import (
    build_delta_outputs,
    find_previous_snapshot,
    enrich_restored_trading_volume,
    read_json,
    write_json,
)


DATA_ROOT = APP_DIR / "data"


def main() -> None:
    for activity_dir in sorted(DATA_ROOT.iterdir()):
        if not activity_dir.is_dir():
            continue
        if activity_dir.name.startswith("."):
            continue

        snapshots = sorted(activity_dir.glob("*_top[0-9]*.json"))
        if not snapshots:
            continue

        print(f"\n=== {activity_dir.name} ({len(snapshots)} snapshots) ===")

        for json_path in snapshots:
            data = read_json(json_path, None)
            if not isinstance(data, dict):
                print(f"  SKIP {json_path.name}: invalid JSON")
                continue
            rows = data.get("rows")
            if not isinstance(rows, list) or not rows:
                print(f"  SKIP {json_path.name}: no rows")
                continue

            name = data.get("name") or activity_dir.name
            ts_prefix = json_path.name.split(f"_{name}_top")[0]
            file_prefix = f"{ts_prefix}_{name}"
            existing_json = activity_dir / f"{file_prefix}_delta_by_nickname.json"

            prev_path = find_previous_snapshot(activity_dir, name, json_path)
            prev_name = prev_path.name if prev_path else "None"
            has_prev_file = existing_json.exists()
            stored_prev = read_json(existing_json, {}).get("previousSnapshot") if has_prev_file else None
            print(f"  {json_path.name}")
            print(f"    previous -> {prev_name}")
            if stored_prev:
                print(f"    stored   -> {Path(stored_prev).name if stored_prev else 'None'}")

            rows_enriched = enrich_restored_trading_volume(rows)

            try:
                result = build_delta_outputs(
                    name, activity_dir, file_prefix, json_path, rows_enriched
                )
                print(f"    -> regenerated OK")
            except Exception as exc:
                print(f"    -> ERROR: {exc}")
                continue

    print("\nDone.")


if __name__ == "__main__":
    main()
