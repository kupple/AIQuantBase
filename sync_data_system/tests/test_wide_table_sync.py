#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from pathlib import Path

from sync_data_system.wide_table_sync import (
    WIDE_TABLE_SYNC_STATE_TABLE,
    compute_plan_signature,
    compute_wide_table_signature,
    discover_wide_table_specs,
    load_and_plan_specs,
    load_wide_table_yaml,
    plan_wide_table_sync,
    parse_wide_table_metadata,
    validate_wide_table_payload,
    WideTableSyncStateRepository,
)
from sync_data_system import wide_table_sync


class WideTableSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        self.project_root = Path(__file__).resolve().parent.parent
        specs = discover_wide_table_specs(self.project_root)
        self.assertTrue(specs, "expected at least one wide table spec in repo")
        self.spec_path = specs[0]

    def test_discover_wide_table_specs(self) -> None:
        specs = discover_wide_table_specs(self.project_root)
        self.assertIn(self.spec_path.resolve(), specs)

    def test_parse_metadata(self) -> None:
        payload = load_wide_table_yaml(self.spec_path)
        metadata = parse_wide_table_metadata(self.spec_path, payload)
        self.assertEqual(metadata.spec_name, "stock_daily_real")
        self.assertEqual(metadata.target.database, "starlight")
        self.assertEqual(metadata.target.table, "stock_daily_real")
        self.assertTrue(metadata.fields)

    def test_validation_accepts_materialization_bundle(self) -> None:
        payload = load_wide_table_yaml(self.spec_path)
        metadata = parse_wide_table_metadata(self.spec_path, payload)
        validation = validate_wide_table_payload(metadata, payload)
        self.assertTrue(validation.ok)

    def test_signatures_are_non_empty(self) -> None:
        payload = load_wide_table_yaml(self.spec_path)
        self.assertTrue(compute_wide_table_signature(payload))
        self.assertTrue(compute_plan_signature(payload))

    def test_plan_defaults_to_rebuild_or_sync_for_current_sample(self) -> None:
        plans = load_and_plan_specs(self.project_root)
        target_plan = next(plan for plan in plans if plan.wide_table_name == "stock_daily_real")
        self.assertIn(target_plan.action, {"create_and_sync", "rebuild", "sync"})
        self.assertTrue(target_plan.validation.ok)


class _FakeClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.queries: list[str] = []
        self.insert_calls: list[tuple[str, list[str], list[tuple]]] = []
        self.rows_for_query: list[tuple] = []

    def command(self, sql: str, parameters=None):
        self.commands.append(sql)
        return None

    def query_rows(self, sql: str, parameters=None):
        self.queries.append(sql)
        return list(self.rows_for_query)

    def query_value(self, sql: str, parameters=None):
        return None

    def insert_rows(self, table: str, column_names, rows):
        self.insert_calls.append((table, list(column_names), list(rows)))

    def close(self):
        return None


class WideTableStateRepositoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.project_root = Path(__file__).resolve().parent.parent
        self.spec_path = discover_wide_table_specs(self.project_root)[0]
        self.payload = load_wide_table_yaml(self.spec_path)
        self.metadata = parse_wide_table_metadata(self.spec_path, self.payload)

    def test_repository_ensure_table_and_exists_lookup(self) -> None:
        client = _FakeClickHouseClient()
        client.rows_for_query = [(self.metadata.target.database, self.metadata.target.table)]
        repo = WideTableSyncStateRepository(client, database="default")
        repo.ensure_table()
        lookup = repo.load_target_exists_lookup([(self.metadata.target.database, self.metadata.target.table)])
        self.assertTrue(lookup[(self.metadata.target.database, self.metadata.target.table)])
        self.assertTrue(any(WIDE_TABLE_SYNC_STATE_TABLE in sql for sql in client.commands))

    def test_repository_save_plan_states(self) -> None:
        client = _FakeClickHouseClient()
        repo = WideTableSyncStateRepository(client, database="default")
        plan = plan_wide_table_sync(
            self.metadata,
            self.payload,
            target_exists=False,
            previous_wide_table_signature=None,
            previous_plan_signature=None,
        )
        repo.save_plan_states({self.metadata.spec_path: self.metadata}, [plan])
        self.assertEqual(len(client.insert_calls), 1)
        table, columns, rows = client.insert_calls[0]
        self.assertEqual(table, f"default.{WIDE_TABLE_SYNC_STATE_TABLE}")
        self.assertIn("wide_table_signature", columns)
        self.assertEqual(len(rows), 1)

    def test_run_wide_table_sync_executes_create_and_insert(self) -> None:
        client = _FakeClickHouseClient()
        repo = WideTableSyncStateRepository(client, database="default")
        repo.ensure_table()
        # make target table look missing
        client.rows_for_query = []

        from sync_data_system.wide_table_sync import run_wide_table_sync_with_clickhouse, WideTableMetadata, WideTableTarget
        from unittest.mock import patch

        fake_metadata = WideTableMetadata(
            spec_path="/tmp/demo.yaml",
            spec_name="demo_wide",
            wide_table_id="wide::demo",
            source_node="stock_daily_real",
            target=WideTableTarget(
                database="research",
                table="demo_wide",
                engine="Memory",
                partition_by=(),
                order_by=("code", "trade_time"),
                version_field="",
            ),
            fields=("code", "trade_time", "close"),
            key_fields=("code", "trade_time"),
            status="enabled",
        )
        fake_payload = {
            "wide_table": {
                "id": "wide::demo",
                "name": "demo_wide",
                "source_node": "stock_daily_real",
                "target": {
                    "database": "research",
                    "table": "demo_wide",
                    "engine": "Memory",
                    "partition_by": [],
                    "order_by": ["code", "trade_time"],
                    "version_field": "",
                },
                "fields": ["code", "trade_time", "close"],
                "key_fields": ["code", "trade_time"],
                "status": "enabled",
            },
            "materialization_bundle": {
                "query_plan": {},
                "base_context": {
                    "base_table": "starlight.ad_market_kline_daily",
                },
                "preview_sql": "SELECT code, trade_time, close FROM starlight.ad_market_kline_daily",
            },
        }

        with patch("sync_data_system.wide_table_sync.create_clickhouse_client", return_value=client), patch(
            "sync_data_system.wide_table_sync.load_specs_payloads_and_metadata",
            return_value=({fake_metadata.spec_path: fake_payload}, {fake_metadata.spec_path: fake_metadata}),
        ):
            results = run_wide_table_sync_with_clickhouse(
                self.project_root,
                config=object(),  # not used by patched factory
                wide_table_names=["demo_wide"],
                state_database="default",
            )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, "success")
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS research.demo_wide" in sql for sql in client.commands))
        self.assertTrue(any("INSERT INTO research.demo_wide" in sql for sql in client.commands))

    def test_month_partition_insert_casts_baostock_string_date(self) -> None:
        client = _FakeClickHouseClient()
        client.rows_for_query = [(202401,)]
        select_sql = (
            "SELECT b0.code AS code, toDate(b0.date) AS trade_time "
            "FROM baostock.bs_daily_kline b0 "
            "ANY LEFT JOIN starlight.ad_backward_factor t1 "
            "ON b0.code = t1.code AND toDate(b0.date) = t1.trade_date"
        )

        wide_table_sync._insert_select_by_month(
            client,
            "starlight.stock_daily_real",
            select_sql,
            {
                "base_table": "baostock.bs_daily_kline",
                "time_key": "date",
                "time_key_expression": "toDate(date)",
            },
        )

        self.assertTrue(any("toYYYYMM(toDate(date))" in sql for sql in client.queries))
        self.assertEqual(len(client.commands), 1)
        self.assertIn("INSERT INTO starlight.stock_daily_real", client.commands[0])
        self.assertIn("FROM (SELECT * FROM baostock.bs_daily_kline WHERE toDate(date) >=", client.commands[0])
        self.assertIn("ANY LEFT JOIN (SELECT * FROM starlight.ad_backward_factor", client.commands[0])
        self.assertIn("2024-01-01", client.commands[0])
        self.assertIn("2024-02-01", client.commands[0])

    def test_run_wide_table_sync_drops_existing_target_before_recreate(self) -> None:
        client = _FakeClickHouseClient()
        repo = WideTableSyncStateRepository(client, database="default")
        repo.ensure_table()
        # make target table look existing
        client.rows_for_query = [("research", "demo_wide")]

        from sync_data_system.wide_table_sync import run_wide_table_sync_with_clickhouse, WideTableMetadata, WideTableTarget
        from unittest.mock import patch

        fake_metadata = WideTableMetadata(
            spec_path="/tmp/demo.yaml",
            spec_name="demo_wide",
            wide_table_id="wide::demo",
            source_node="stock_daily_real",
            target=WideTableTarget(
                database="research",
                table="demo_wide",
                engine="Memory",
                partition_by=(),
                order_by=("code", "trade_time"),
                version_field="",
            ),
            fields=("code", "trade_time", "close"),
            key_fields=("code", "trade_time"),
            status="enabled",
        )
        fake_payload = {
            "wide_table": {
                "id": "wide::demo",
                "name": "demo_wide",
                "source_node": "stock_daily_real",
                "target": {
                    "database": "research",
                    "table": "demo_wide",
                    "engine": "Memory",
                    "partition_by": [],
                    "order_by": ["code", "trade_time"],
                    "version_field": "",
                },
                "fields": ["code", "trade_time", "close"],
                "key_fields": ["code", "trade_time"],
                "status": "enabled",
            },
            "materialization_bundle": {
                "query_plan": {},
                "base_context": {
                    "base_table": "starlight.ad_market_kline_daily",
                },
                "preview_sql": "SELECT code, trade_time, close FROM starlight.ad_market_kline_daily",
            },
        }

        with patch("sync_data_system.wide_table_sync.create_clickhouse_client", return_value=client), patch(
            "sync_data_system.wide_table_sync.load_specs_payloads_and_metadata",
            return_value=({fake_metadata.spec_path: fake_payload}, {fake_metadata.spec_path: fake_metadata}),
        ):
            results = run_wide_table_sync_with_clickhouse(
                self.project_root,
                config=object(),  # not used by patched factory
                wide_table_names=["demo_wide"],
                state_database="default",
            )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, "success")
        self.assertTrue(any("DROP TABLE IF EXISTS research.demo_wide" in sql for sql in client.commands))
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS research.demo_wide" in sql for sql in client.commands))
        self.assertTrue(any("INSERT INTO research.demo_wide" in sql for sql in client.commands))


if __name__ == "__main__":
    unittest.main()
