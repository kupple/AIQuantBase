#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run wide table sync job."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from sync_data_system.clickhouse_client import ClickHouseConfig
from sync_data_system.wide_table_sync import run_wide_table_sync_with_clickhouse


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run wide table sync")
    parser.add_argument("--wide-table-name", action="append", dest="wide_table_names")
    parser.add_argument("--state-database")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    results = run_wide_table_sync_with_clickhouse(
        PROJECT_ROOT,
        config=ClickHouseConfig.from_env(),
        wide_table_names=args.wide_table_names,
        state_database=args.state_database,
    )
    if args.json:
        print(json.dumps([result.__dict__ for result in results], ensure_ascii=False, indent=2))
    else:
        for result in results:
            print(
                f"{result.wide_table_name}: action={result.action} "
                f"status={result.status} message={result.message}"
            )
    return 0 if all(result.status == "success" for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
