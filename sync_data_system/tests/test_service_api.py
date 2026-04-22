#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from sync_data_system.service.api import app
from sync_data_system.service.job_manager import JobRecord


class ServiceApiTest(unittest.TestCase):
    def test_sync_table_status(self) -> None:
        client = TestClient(app)

        class _FakeClient:
            def query_rows(self, sql, parameters=None):
                if "FROM system.tables" in sql:
                    return [("starlight", "ad_market_kline_daily")]
                if "FROM system.columns" in sql:
                    return [
                        ("starlight", "ad_market_kline_daily", "code"),
                        ("starlight", "ad_market_kline_daily", "trade_time"),
                    ]
                if "FROM system.parts" in sql:
                    return [("starlight", "ad_market_kline_daily", 123, "2026-04-22 10:00:00")]
                return []

            def query_value(self, sql, parameters=None):
                return "2026-04-22 00:00:00"

            def close(self):
                return None

        with patch("sync_data_system.service.api.JOB_MANAGER.list_registered_tasks", return_value=[{"name": "daily_kline", "target": "ad_market_kline_daily"}]), patch(
            "sync_data_system.service.api.ClickHouseConfig.from_env",
            return_value=object(),
        ), patch(
            "sync_data_system.service.api.create_clickhouse_client",
            return_value=_FakeClient(),
        ):
            response = client.get("/api/sync-table-status")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["target"], "ad_market_kline_daily")
        self.assertEqual(payload["items"][0]["latest_date"], "2026-04-22 00:00:00")
        self.assertEqual(payload["items"][0]["row_count"], 123)

    def test_list_wide_table_specs(self) -> None:
        client = TestClient(app)
        with patch(
            "sync_data_system.service.api.list_wide_table_metadata",
            return_value=[],
        ):
            response = client.get("/api/wide-tables/specs")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"specs": []})

    def test_plan_wide_tables_returns_plan_list(self) -> None:
        client = TestClient(app)
        with patch(
            "sync_data_system.service.api.load_and_plan_specs",
            return_value=[],
        ):
            response = client.post("/api/wide-tables/plan", json={})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"plans": []})

    def test_run_wide_tables_creates_job(self) -> None:
        client = TestClient(app)
        fake_job = JobRecord(
            job_id="job_wide",
            kind="wide_table_sync",
            status="running",
            created_at="2026-01-01T00:00:00+00:00",
            started_at="2026-01-01T00:00:00+00:00",
            finished_at=None,
            cwd="/tmp",
            command=["python", "scripts/run_wide_table_sync.py"],
            log_path="/tmp/job_wide.log",
            config_path=None,
            task=None,
            source="wide_table",
            target=None,
            pid=321,
            return_code=None,
            error=None,
            request_payload={"wide_table_names": ["stock_daily_real"]},
        )
        with patch(
            "sync_data_system.service.api.JOB_MANAGER.create_wide_table_job",
            return_value=fake_job,
        ):
            response = client.post(
                "/api/wide-tables/run",
                json={"wide_table_names": ["stock_daily_real"]},
            )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["job_id"], "job_wide")
        self.assertEqual(payload["wide_table_names"], ["stock_daily_real"])

    def test_run_single_wide_table_creates_job(self) -> None:
        client = TestClient(app)
        fake_job = JobRecord(
            job_id="job_wide_one",
            kind="wide_table_sync",
            status="running",
            created_at="2026-01-01T00:00:00+00:00",
            started_at="2026-01-01T00:00:00+00:00",
            finished_at=None,
            cwd="/tmp",
            command=["python", "scripts/run_wide_table_sync.py"],
            log_path="/tmp/job_wide_one.log",
            config_path=None,
            task="stock_daily_real",
            source="wide_table",
            target=None,
            pid=322,
            return_code=None,
            error=None,
            request_payload={"wide_table_names": ["stock_daily_real"]},
        )
        with patch(
            "sync_data_system.service.api.JOB_MANAGER.create_wide_table_job",
            return_value=fake_job,
        ):
            response = client.post("/api/wide-tables/run/stock_daily_real")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["job_id"], "job_wide_one")
        self.assertEqual(payload["wide_table_names"], ["stock_daily_real"])

    def test_get_job_includes_logs_tail(self) -> None:
        client = TestClient(app)
        fake_job = JobRecord(
            job_id="job1",
            kind="task",
            status="running",
            created_at="2026-01-01T00:00:00+00:00",
            started_at="2026-01-01T00:00:00+00:00",
            finished_at=None,
            cwd="/tmp",
            command=["python", "run_sync.py"],
            log_path="/tmp/job1.log",
            config_path=None,
            task="daily_kline",
            source="amazingdata",
            target="ad_market_kline_daily",
            pid=123,
            return_code=None,
            error=None,
        )
        with patch("sync_data_system.service.api.JOB_MANAGER.get_job", return_value=fake_job), patch(
            "sync_data_system.service.api.JOB_MANAGER.read_job_log",
            return_value="line1\nline2",
        ):
            response = client.get("/api/jobs/job1")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["job_id"], "job1")
        self.assertEqual(payload["logs_tail"], "line1\nline2")

    def test_list_jobs_supports_status_filter(self) -> None:
        client = TestClient(app)
        fake_job = JobRecord(
            job_id="job1",
            kind="task",
            status="running",
            created_at="2026-01-01T00:00:00+00:00",
            started_at="2026-01-01T00:00:00+00:00",
            finished_at=None,
            cwd="/tmp",
            command=["python", "run_sync.py"],
            log_path="/tmp/job1.log",
            config_path=None,
            task="daily_kline",
            source="amazingdata",
            target="ad_market_kline_daily",
            pid=123,
            return_code=None,
            error=None,
        )
        with patch("sync_data_system.service.api.JOB_MANAGER.list_jobs", return_value=[fake_job]):
            response = client.get("/api/jobs?status=running")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["jobs"]), 1)
        self.assertEqual(payload["jobs"][0]["status"], "running")

    def test_run_task_returns_409_when_another_job_running(self) -> None:
        client = TestClient(app)
        with patch(
            "sync_data_system.service.api.JOB_MANAGER.create_registered_task_job",
            side_effect=RuntimeError("another sync job is running job_id=job1 task=daily_kline; cancel it first"),
        ), patch(
            "sync_data_system.service.api.JOB_MANAGER.list_registered_tasks",
            return_value=[{"name": "daily_kline"}],
        ):
            response = client.post(
                "/api/jobs/run-task",
                json={"name": "daily_kline"},
            )
        self.assertEqual(response.status_code, 409)
        self.assertIn("another sync job is running", response.json()["detail"])

    def test_run_task_returns_task_metadata(self) -> None:
        client = TestClient(app)
        fake_job = JobRecord(
            job_id="job1",
            kind="registered_task",
            status="running",
            created_at="2026-01-01T00:00:00+00:00",
            started_at="2026-01-01T00:00:00+00:00",
            finished_at=None,
            cwd="/tmp",
            command=["python", "scripts/run_registered_task.py"],
            log_path="/tmp/job1.log",
            config_path=None,
            task="daily_kline",
            source="amazingdata",
            target="ad_market_kline_daily",
            pid=123,
            return_code=None,
            error=None,
            request_payload={"name": "daily_kline"},
        )
        with patch(
            "sync_data_system.service.api.JOB_MANAGER.create_registered_task_job",
            return_value=fake_job,
        ), patch(
            "sync_data_system.service.api.JOB_MANAGER.list_registered_tasks",
            return_value=[
                {
                    "name": "daily_kline",
                    "source": "amazingdata",
                    "target": "ad_market_kline_daily",
                    "input_resolver": "market_kline_defaults",
                    "request_fields": ["name"],
                    "probe_fields": ["name"],
                }
            ],
        ):
            response = client.post("/api/jobs/run-task", json={"name": "daily_kline"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["job_id"], "job1")
        self.assertEqual(payload["task_metadata"]["name"], "daily_kline")


if __name__ == "__main__":
    unittest.main()
