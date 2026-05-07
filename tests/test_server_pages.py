from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from aiquantbase.config import dump_yaml
from aiquantbase.server import create_app


def _write_graph(path: Path) -> None:
    path.write_text(
        dump_yaml(
            {
                "nodes": [
                    {
                        "name": "market_daily",
                        "table": "demo.market_daily",
                        "entity_keys": ["code"],
                        "time_key": "trade_date",
                        "grain": "daily",
                        "fields": ["trade_date", "code", "close"],
                        "description": "daily market data",
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )


def _client(tmp_path: Path):
    graph_path = tmp_path / "graph.yaml"
    fields_path = tmp_path / "fields.yaml"
    sync_root = tmp_path / "sync_project"
    _write_graph(graph_path)
    app = create_app(
        default_graph_path=graph_path,
        default_fields_path=fields_path,
        sync_project_root=sync_root,
    )
    app.config["TESTING"] = True
    return app.test_client(), graph_path, fields_path, sync_root


def test_root_returns_api_summary(tmp_path: Path):
    client, graph_path, fields_path, sync_root = _client(tmp_path)

    response = client.get("/")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["service"] == "AIQuantBase API"
    assert payload["workspace"]["graph_path"] == str(graph_path)
    assert payload["workspace"]["fields_path"] == str(fields_path)
    assert "/api/workspace" in payload["available_routes"]
    assert "/api/capabilities/workspace" in payload["available_routes"]
    assert "/api/sync/jobs" in payload["available_routes"]


def test_old_flask_pages_are_removed(tmp_path: Path):
    client, _, _, _ = _client(tmp_path)

    assert client.get("/nodes").status_code == 404
    assert client.get("/edges").status_code == 404
    assert client.get("/graph/view").status_code == 404
    assert client.get("/llm-query").status_code == 404


def test_health_endpoint_still_works(tmp_path: Path):
    client, _, _, _ = _client(tmp_path)

    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.get_json() == {"ok": True}


def test_workspace_api_uses_configured_graph_path(tmp_path: Path):
    client, graph_path, _, _ = _client(tmp_path)

    response = client.get("/api/workspace")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["workspace"]["graph_path"] == str(graph_path)
    assert len(payload["graph"]["nodes"]) == 1


def test_workspace_api_preserves_range_time_keys(tmp_path: Path):
    client, graph_path, _, _ = _client(tmp_path)
    graph_path.write_text(
        dump_yaml(
            {
                "nodes": [
                    {
                        "name": "index_constituent_real",
                        "table": "starlight.ad_index_constituent",
                        "entity_keys": ["con_code"],
                        "time_key": "in_date",
                        "time_key_mode": "range",
                        "interval_keys": {
                            "start": "in_date",
                            "end": "out_date",
                        },
                        "grain": "daily",
                        "fields": ["index_code", "con_code", "in_date", "out_date"],
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )

    response = client.get("/api/workspace")

    assert response.status_code == 200
    payload = response.get_json()
    node = payload["graph"]["nodes"][0]
    assert node["time_key"] == "in_date"
    assert node["time_key_mode"] == "range"
    assert node["interval_keys"] == {"start": "in_date", "end": "out_date"}


def test_schema_databases_returns_json_error_when_clickhouse_fails(tmp_path: Path):
    client, _, _, _ = _client(tmp_path)

    with patch("aiquantbase.server.ClickHouseExecutor.execute_sql", side_effect=RuntimeError("clickhouse unavailable")):
        response = client.get("/api/schema/databases", query_string={"runtime_path": "config/runtime.example.yaml"})

    assert response.status_code == 400
    assert response.get_json() == {"detail": "clickhouse unavailable"}


def test_sync_config_endpoints_use_integrated_sync_root(tmp_path: Path):
    client, _, _, sync_root = _client(tmp_path)
    config_root = tmp_path / "config" / "sync" / "plans"

    response = client.get("/api/sync-configs")
    assert response.status_code == 200
    assert response.get_json() == {"items": []}

    create_response = client.post(
        "/api/sync-configs",
        json={
            "name": "run_sync.example.toml",
            "content": "task = 'daily_kline'\n",
        },
    )
    assert create_response.status_code == 200
    assert (config_root / "run_sync.example.toml").read_text(encoding="utf-8") == "task = 'daily_kline'\n"

    detail_response = client.get("/api/sync-configs/run_sync.example.toml")
    assert detail_response.status_code == 200
    payload = detail_response.get_json()
    assert payload["name"] == "run_sync.example.toml"
    assert "daily_kline" in payload["content"]


def test_sync_wide_table_export_endpoints_are_removed(tmp_path: Path):
    client, _, _, _ = _client(tmp_path)
    removed_prefix = "/api/" + "sync-" + "wide-tables"

    assert client.get(removed_prefix).status_code == 404
    assert client.post(removed_prefix, json={}).status_code == 404
    assert client.get(f"{removed_prefix}/demo").status_code == 404
