#!/usr/bin/env python3
"""Compare AmazingData long_hu_bang source rows with ClickHouse rows.

This is a read-only diagnostic script. It helps answer whether a long_hu_bang
gap comes from the upstream SDK response, our SDK-to-row mapping, or the
ClickHouse persisted table.

Example:
  python sync_data_system/scripts/diagnose_long_hu_bang_source_vs_db.py \
    --code 601360.SH --date 2023-02-27
"""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
for path in (str(REPO_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

import pandas as pd

from aiquantbase.executor import ClickHouseExecutor
from aiquantbase.runtime_config import load_runtime_config
from sync_data_system.amazingdata_sdk_provider import (
    AmazingDataSDKConfig,
    AmazingDataSDKProvider,
    _count_sdk_result_rows,
)


DEFAULT_RUNTIME = REPO_ROOT / "config" / "runtime.local.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare AmazingData get_long_hu_bang source data with ClickHouse ad_long_hu_bang.",
    )
    parser.add_argument("--code", default="601360.SH", help="Market code, for example 601360.SH")
    parser.add_argument("--date", default="2023-02-27", help="Trade date in YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--runtime", default=str(DEFAULT_RUNTIME), help="runtime.local.yaml path")
    parser.add_argument("--max-rows", type=int, default=80, help="Max rows to print per section")
    return parser.parse_args()


def parse_date(value: str) -> date:
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return datetime.strptime(text, "%Y-%m-%d").date()


def yyyymmdd(value: date) -> int:
    return int(value.strftime("%Y%m%d"))


def flatten_sdk_result(obj: Any) -> pd.DataFrame:
    if isinstance(obj, pd.DataFrame):
        return obj.reset_index(drop=False)
    if isinstance(obj, dict):
        frames: list[pd.DataFrame] = []
        for key, value in obj.items():
            if value is None:
                continue
            if isinstance(value, pd.DataFrame):
                frame = value.reset_index(drop=False).copy()
                if not any(col.lower() in {"market_code", "marketcode", "code"} for col in frame.columns):
                    frame.insert(0, "__dict_key__", key)
                frames.append(frame)
                continue
            if isinstance(value, dict):
                child = flatten_sdk_result(value)
                if not child.empty:
                    child.insert(0, "__dict_key__", key)
                    frames.append(child)
                continue
            frames.append(pd.DataFrame([{"__dict_key__": key, "__value__": repr(value)}]))
        if frames:
            return pd.concat(frames, ignore_index=True, sort=False)
        return pd.DataFrame()
    if obj is None:
        return pd.DataFrame()
    return pd.DataFrame([{"__result_type__": type(obj).__name__, "__repr__": repr(obj)}])


def fetch_sdk_raw(provider: AmazingDataSDKProvider, code: str, trade_date: date) -> tuple[Any, pd.DataFrame]:
    kwargs = {
        "code_list": [code],
        "local_path": provider.config.local_path,
        "is_local": False,
        "begin_date": yyyymmdd(trade_date),
        "end_date": yyyymmdd(trade_date),
    }
    raw = provider.session.info.get_long_hu_bang(**kwargs)
    return raw, flatten_sdk_result(raw)


def fetch_sdk_mapped(provider: AmazingDataSDKProvider, code: str, trade_date: date) -> pd.DataFrame:
    rows = list(provider.fetch_long_hu_bang([code], start_date=trade_date, end_date=trade_date) or [])
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([asdict(row) for row in rows])


def quote_sql(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def fetch_db_rows(runtime_path: Path, code: str, trade_date: date) -> pd.DataFrame:
    runtime = load_runtime_config(runtime_path)
    executor = ClickHouseExecutor(runtime.datasource)
    sql = f"""
        SELECT
          market_code,
          trade_date,
          security_name,
          reason_type,
          reason_type_name,
          change_range,
          trader_name,
          buy_amount,
          sell_amount,
          flow_mark,
          total_amount,
          total_volume
        FROM starlight.ad_long_hu_bang
        WHERE market_code = {quote_sql(code)}
          AND trade_date = {quote_sql(trade_date.isoformat())}
        ORDER BY market_code, trade_date, reason_type, trader_name
    """
    return executor.execute_sql_df(sql)


def numeric_sum(df: pd.DataFrame, column: str) -> float | None:
    if df.empty or column not in df.columns:
        return None
    values = pd.to_numeric(df[column], errors="coerce")
    if values.notna().sum() == 0:
        return None
    return float(values.sum(skipna=True))


def has_positive(df: pd.DataFrame, column: str) -> bool:
    value = numeric_sum(df, column)
    return value is not None and value > 0


def same_number(left: float | None, right: float | None) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    return math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-6)


def print_frame(title: str, df: pd.DataFrame, max_rows: int) -> None:
    print(f"\n== {title} ==")
    print(f"rows={len(df)}")
    if df.empty:
        print("<EMPTY>")
        return
    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        240,
        "display.max_colwidth",
        80,
    ):
        print(df.head(max_rows).to_string(index=False))


def print_summary(raw_result: Any, raw_df: pd.DataFrame, mapped_df: pd.DataFrame, db_df: pd.DataFrame) -> None:
    raw_buy_columns = [col for col in raw_df.columns if "buy" in str(col).lower()]
    raw_sell_columns = [col for col in raw_df.columns if "sell" in str(col).lower()]
    mapped_buy_sum = numeric_sum(mapped_df, "buy_amount")
    mapped_sell_sum = numeric_sum(mapped_df, "sell_amount")
    db_buy_sum = numeric_sum(db_df, "buy_amount")
    db_sell_sum = numeric_sum(db_df, "sell_amount")

    print("\n== Summary ==")
    print(f"sdk_result_type={type(raw_result).__name__}")
    print(f"sdk_result_rows={_count_sdk_result_rows(raw_result)}")
    print(f"raw_buy_like_columns={raw_buy_columns}")
    print(f"raw_sell_like_columns={raw_sell_columns}")
    print(f"sdk_mapped_rows={len(mapped_df)} mapped_buy_sum={mapped_buy_sum} mapped_sell_sum={mapped_sell_sum}")
    print(f"db_rows={len(db_df)} db_buy_sum={db_buy_sum} db_sell_sum={db_sell_sum}")

    if has_positive(mapped_df, "buy_amount") and not has_positive(db_df, "buy_amount"):
        verdict = "DB_SYNC_OR_PERSISTENCE_GAP: SDK mapped rows have positive buy_amount, but DB does not."
    elif not has_positive(mapped_df, "buy_amount") and not has_positive(db_df, "buy_amount"):
        if raw_buy_columns:
            verdict = "SOURCE_OR_MAPPING_GAP: DB matches mapped rows with no positive buy_amount; inspect raw buy-like columns."
        else:
            verdict = "SOURCE_GAP: SDK raw/mapped rows do not expose positive buy_amount, and DB matches that."
    elif len(mapped_df) != len(db_df) or not same_number(mapped_buy_sum, db_buy_sum) or not same_number(mapped_sell_sum, db_sell_sum):
        verdict = "DB_DIFFERS_FROM_SDK_MAPPING: mapped SDK rows and DB rows differ."
    else:
        verdict = "SDK_MAPPING_AND_DB_MATCH: persisted rows match the SDK mapping."
    print(f"verdict={verdict}")


def main() -> int:
    args = parse_args()
    trade_date = parse_date(args.date)
    runtime_path = Path(args.runtime).expanduser().resolve()

    print("long_hu_bang diagnostic")
    print(f"repo={REPO_ROOT}")
    print(f"runtime={runtime_path}")
    print(f"code={args.code}")
    print(f"date={trade_date.isoformat()}")

    sdk_config = AmazingDataSDKConfig.from_env(runtime_path=runtime_path)
    provider = AmazingDataSDKProvider(sdk_config)
    try:
        raw_result, raw_df = fetch_sdk_raw(provider, args.code, trade_date)
        mapped_df = fetch_sdk_mapped(provider, args.code, trade_date)
    finally:
        provider.close()

    db_df = fetch_db_rows(runtime_path, args.code, trade_date)

    print_frame("SDK raw get_long_hu_bang result", raw_df, args.max_rows)
    print_frame("SDK mapped LongHuBangRow fields", mapped_df, args.max_rows)
    print_frame("ClickHouse starlight.ad_long_hu_bang rows", db_df, args.max_rows)
    print_summary(raw_result, raw_df, mapped_df, db_df)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
