from __future__ import annotations

from pathlib import Path

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
    _write_graph(graph_path)
    app = create_app(default_graph_path=graph_path, default_fields_path=fields_path)
    app.config["TESTING"] = True
    return app.test_client(), graph_path, fields_path


def test_root_returns_api_summary(tmp_path: Path):
    client, graph_path, fields_path = _client(tmp_path)

    response = client.get("/")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["service"] == "AIQuantBase API"
    assert payload["workspace"]["graph_path"] == str(graph_path)
    assert payload["workspace"]["fields_path"] == str(fields_path)
    assert "/api/workspace" in payload["available_routes"]


def test_old_flask_pages_are_removed(tmp_path: Path):
    client, _, _ = _client(tmp_path)

    assert client.get("/nodes").status_code == 404
    assert client.get("/edges").status_code == 404
    assert client.get("/graph/view").status_code == 404
    assert client.get("/llm-query").status_code == 404


def test_health_endpoint_still_works(tmp_path: Path):
    client, _, _ = _client(tmp_path)

    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.get_json() == {"ok": True}


def test_workspace_api_uses_configured_graph_path(tmp_path: Path):
    client, graph_path, _ = _client(tmp_path)

    response = client.get("/api/workspace")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["workspace"]["graph_path"] == str(graph_path)
    assert len(payload["graph"]["nodes"]) == 1
