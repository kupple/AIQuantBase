#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from datetime import datetime

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from sync_data_system.sources.baostock.provider import normalize_baostock_code, normalize_baostock_code_list, to_baostock_code
from sync_data_system.sources.baostock.repository import BaoStockRepository
from sync_data_system.sources.baostock.runner import SyncArgs, resolve_effective_request_meta
from sync_data_system.sources.baostock.specs import BAOSTOCK_TASK_SPECS, camel_to_snake, table_columns_for_spec
from sync_data_system.sync_core.incremental import advance_cursor_value, normalize_request_value


class _FakeClickHouseClient:
    def __init__(self) -> None:
        self.insert_calls: list[tuple[str, list[str], list[tuple]]] = []
        self.query_value_calls: list[tuple[str, dict | None]] = []
        self.query_value_result = 0

    def command(self, sql: str, parameters=None):
        return None

    def insert_rows(self, table: str, column_names, rows):
        self.insert_calls.append((table, list(column_names), list(rows)))

    def query_value(self, sql: str, parameters=None):
        self.query_value_calls.append((sql, parameters))
        return self.query_value_result


class _FakeIncrementalRepository:
    def __init__(self, latest_cursor: str | None = None, has_request_data: bool = False) -> None:
        self.latest_cursor = latest_cursor
        self.has_request_data = has_request_data

    def load_latest_cursor(self, task: str, *, code: str | None = None):
        return self.latest_cursor

    def has_task_data_for_request(self, task: str, request_meta):
        return self.has_request_data


class BaoStockCodeNormalizeTest(unittest.TestCase):
    def test_normalize_baostock_code(self) -> None:
        self.assertEqual(normalize_baostock_code("sh.600000"), "600000.SH")
        self.assertEqual(normalize_baostock_code("sz.000001"), "000001.SZ")
        self.assertEqual(normalize_baostock_code("600000.SH"), "600000.SH")

    def test_to_baostock_code(self) -> None:
        self.assertEqual(to_baostock_code("600000.SH"), "sh.600000")
        self.assertEqual(to_baostock_code("000001.SZ"), "sz.000001")
        self.assertEqual(to_baostock_code("sh.600000"), "sh.600000")

    def test_normalize_baostock_code_list_deduplicates(self) -> None:
        self.assertEqual(
            normalize_baostock_code_list(["sh.600000", "600000.SH", "sz.000001"]),
            ["600000.SH", "000001.SZ"],
        )


class BaoStockSpecTest(unittest.TestCase):
    def test_camel_to_snake(self) -> None:
        self.assertEqual(camel_to_snake("dividOperateDate"), "divid_operate_date")
        self.assertEqual(camel_to_snake("isST"), "is_st")

    def test_table_columns_include_source_code_when_task_has_code_field(self) -> None:
        columns = table_columns_for_spec(BAOSTOCK_TASK_SPECS["stock_basic"])
        self.assertIn("source_code", columns)
        self.assertIn("code", columns)
        self.assertIn("ingested_at", columns)


class BaoStockIncrementalHelperTest(unittest.TestCase):
    def test_normalize_request_value(self) -> None:
        self.assertEqual(normalize_request_value("2024-01-31", "day"), "20240131")
        self.assertEqual(normalize_request_value("2024-01", "month"), "202401")
        self.assertEqual(normalize_request_value("2024", "year"), "2024")

    def test_advance_cursor_value(self) -> None:
        self.assertEqual(advance_cursor_value("2024-01-31", "day"), "20240201")
        self.assertEqual(advance_cursor_value("2024-12", "month"), "202501")
        self.assertEqual(advance_cursor_value("2024", "year"), "2025")

    def test_resolve_effective_request_meta_pushes_begin_date_forward(self) -> None:
        args = SyncArgs(
            task="daily_kline",
            codes_raw="",
            begin_date="20240101",
            end_date="20240131",
            day="",
            year=None,
            quarter=None,
            year_type="",
            adjustflag="3",
            frequency="d",
            limit=0,
            force=False,
            continue_on_error=False,
            runtime_path=None,
            database="baostock",
            log_level="INFO",
        )
        request_meta = {
            "code": "600000.SH",
            "start_date": "20240101",
            "end_date": "20240131",
            "day": "",
            "year": None,
            "quarter": None,
            "year_type": "",
        }

        effective = resolve_effective_request_meta(
            args,
            _FakeIncrementalRepository(latest_cursor="2024-01-10"),
            request_meta,
        )

        self.assertIsNotNone(effective)
        self.assertEqual(effective["start_date"], "20240111")
        self.assertEqual(effective["end_date"], "20240131")

    def test_resolve_effective_request_meta_skips_when_request_already_exists(self) -> None:
        args = SyncArgs(
            task="stock_basic",
            codes_raw="",
            begin_date="",
            end_date="",
            day="",
            year=None,
            quarter=None,
            year_type="",
            adjustflag="3",
            frequency="d",
            limit=0,
            force=False,
            continue_on_error=False,
            runtime_path=None,
            database="baostock",
            log_level="INFO",
        )
        request_meta = {
            "code": "600000.SH",
            "start_date": "",
            "end_date": "",
            "day": "",
            "year": None,
            "quarter": None,
            "year_type": "",
        }

        effective = resolve_effective_request_meta(
            args,
            _FakeIncrementalRepository(latest_cursor=None, has_request_data=True),
            request_meta,
        )

        self.assertIsNone(effective)


@unittest.skipIf(pd is None, "pandas is required")
class BaoStockRepositoryTest(unittest.TestCase):
    def test_save_task_frame_normalizes_code_column(self) -> None:
        client = _FakeClickHouseClient()
        repository = BaoStockRepository(client, database="baostock")
        frame = pd.DataFrame([{"code": "sh.600000", "tradeStatus": "1", "code_name": "浦发银行"}])

        inserted = repository.save_task_frame(
            "all_stock",
            frame,
            request_meta={"day": "20240110", "start_date": "", "end_date": "", "year": None, "quarter": None, "year_type": ""},
        )

        self.assertEqual(inserted, 1)
        table, columns, rows = client.insert_calls[0]
        self.assertEqual(table, "baostock.bs_all_stock")
        self.assertIn("code", columns)
        self.assertIn("source_code", columns)
        row = dict(zip(columns, rows[0]))
        self.assertEqual(row["code"], "600000.SH")
        self.assertEqual(row["source_code"], "sh.600000")
        self.assertEqual(row["query_date"], "20240110")
        self.assertIsInstance(row["ingested_at"], datetime)


if __name__ == "__main__":
    unittest.main()
