from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from aiquantbase.sync_integration import SyncIntegration


class _FakeJobManager:
    def list_registered_tasks(self):
        return [
            {"name": "daily_kline", "target": "ad_market_kline_daily"},
            {"name": "daily_kline_rebuild", "target": "ad_market_kline_daily"},
        ]


def test_sync_table_status_degrades_when_clickhouse_is_unavailable(tmp_path: Path):
    project_root = tmp_path / "sync_data_system"
    project_root.mkdir()
    integration = SyncIntegration(sync_project_root=project_root)

    with patch.object(SyncIntegration, "_job_manager_instance", return_value=_FakeJobManager()), patch(
        "sync_data_system.clickhouse_client.ClickHouseConfig.from_env",
        return_value=object(),
    ), patch(
        "sync_data_system.clickhouse_client.create_clickhouse_client",
        side_effect=RuntimeError("clickhouse unavailable"),
    ):
        payload = integration.sync_table_status()

    assert payload["detail"] == "clickhouse unavailable"
    assert len(payload["items"]) == 1
    assert payload["items"][0]["target"] == "ad_market_kline_daily"
    assert payload["items"][0]["status"] == "connection_error"
    assert payload["items"][0]["tasks"] == ["daily_kline", "daily_kline_rebuild"]
    assert payload["items"][0]["error"] == "clickhouse unavailable"
