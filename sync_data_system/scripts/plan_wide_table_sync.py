#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plan wide table sync actions from exported YAML specs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from sync_data_system.clickhouse_client import ClickHouseConfig
from sync_data_system.wide_table_sync import load_and_plan_specs, load_and_plan_specs_with_clickhouse


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan wide table sync from YAML specs")
    parser.add_argument("--root", default=str(PROJECT_ROOT))
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument("--clickhouse-live", action="store_true")
    parser.add_argument("--write-state", action="store_true")
    parser.add_argument("--state-database")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.write_state and not args.clickhouse_live:
        raise SystemExit("--write-state requires --clickhouse-live")

    if args.clickhouse_live:
        plans = load_and_plan_specs_with_clickhouse(
            Path(args.root),
            config=ClickHouseConfig.from_env(),
            state_database=args.state_database,
            write_state=args.write_state,
        )
    else:
        plans = load_and_plan_specs(Path(args.root))
    if args.as_json:
        print(json.dumps([plan.__dict__ | {"validation": plan.validation.__dict__} for plan in plans], ensure_ascii=False, indent=2))
        return 0
    for plan in plans:
        print(
            f"{plan.wide_table_name}: action={plan.action} "
            f"target={plan.target_database}.{plan.target_table} "
            f"reason={plan.reason}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
