from __future__ import annotations

from pathlib import Path

from aiquantbase.config import dump_yaml, load_nodes_and_edges
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
                    },
                    {
                        "name": "factor_daily",
                        "table": "demo.factor_daily",
                        "entity_keys": ["code"],
                        "time_key": "trade_date",
                        "grain": "daily_factor",
                        "fields": ["trade_date", "code", "factor_value"],
                        "description": "factor data",
                    },
                    {
                        "name": "orphan_node",
                        "table": "demo.orphan",
                        "entity_keys": ["code"],
                        "time_key": "trade_date",
                        "grain": "daily",
                        "fields": ["trade_date", "code"],
                        "description": "orphan data",
                    },
                ],
                "edges": [
                    {
                        "name": "market_to_factor",
                        "from": "market_daily",
                        "to": "factor_daily",
                        "relation_type": "direct",
                        "join_keys": [{"base": "code", "source": "code"}],
                        "time_binding": {
                            "mode": "same_date",
                            "base_time_field": "trade_date",
                            "source_time_field": "trade_date",
                        },
                        "description": "join daily factor",
                    }
                ],
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
    return app.test_client(), graph_path


def test_root_renders_management_page(tmp_path: Path):
    client, _ = _client(tmp_path)

    response = client.get("/", follow_redirects=True)

    assert response.status_code == 200
    assert "图谱工作台".encode("utf-8") in response.data
    assert "节点列表".encode("utf-8") in response.data
    assert "边列表".encode("utf-8") in response.data
    assert "展示图谱".encode("utf-8") in response.data
    assert "LLM查询数据库".encode("utf-8") in response.data


def test_graph_view_renders_cytoscape_container(tmp_path: Path):
    client, _ = _client(tmp_path)

    response = client.get("/graph/view")

    assert response.status_code == 200
    assert b"graph-canvas" in response.data
    assert b"cytoscape.min.js" in response.data


def test_health_endpoint_still_works(tmp_path: Path):
    client, _ = _client(tmp_path)

    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.get_json() == {"ok": True}


def test_workspace_api_uses_configured_graph_path(tmp_path: Path):
    client, graph_path = _client(tmp_path)

    response = client.get("/api/workspace")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["workspace"]["graph_path"] == str(graph_path)
    assert len(payload["graph"]["nodes"]) == 3


def test_create_node_persists_yaml(tmp_path: Path):
    client, graph_path = _client(tmp_path)

    response = client.post(
        "/graph/node/create",
        data={
            "name": "profile_node",
            "table": "demo.profile",
            "entity_keys": "code",
            "time_key": "trade_date",
            "grain": "daily",
            "fields": "trade_date, code, profile_score",
            "description": "profile node",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    nodes, _ = load_nodes_and_edges(graph_path)
    assert any(node.name == "profile_node" for node in nodes)


def test_update_edge_persists_yaml(tmp_path: Path):
    client, graph_path = _client(tmp_path)

    response = client.post(
        "/graph/edge/update",
        data={
            "original_name": "market_to_factor",
            "name": "market_to_factor_v2",
            "from_node": "market_daily",
            "to_node": "factor_daily",
            "relation_type": "direct",
            "join_keys": "base:code -> source:code",
            "time_mode": "same_date",
            "base_time_field": "trade_date",
            "base_time_cast": "date",
            "source_time_field": "trade_date",
            "source_start_field": "",
            "source_end_field": "",
            "description": "updated edge",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    _, edges = load_nodes_and_edges(graph_path)
    assert any(edge.name == "market_to_factor_v2" and edge.description == "updated edge" for edge in edges)


def test_delete_unreferenced_node_succeeds(tmp_path: Path):
    client, graph_path = _client(tmp_path)

    response = client.post(
        "/graph/node/delete",
        data={"name": "orphan_node"},
        follow_redirects=True,
    )

    assert response.status_code == 200
    nodes, _ = load_nodes_and_edges(graph_path)
    assert all(node.name != "orphan_node" for node in nodes)


def test_delete_referenced_node_fails(tmp_path: Path):
    client, graph_path = _client(tmp_path)

    response = client.post(
        "/graph/node/delete",
        data={"name": "market_daily"},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "仍被边引用".encode("utf-8") in response.data
    nodes, _ = load_nodes_and_edges(graph_path)
    assert any(node.name == "market_daily" for node in nodes)


def test_invalid_join_keys_fails(tmp_path: Path):
    client, graph_path = _client(tmp_path)

    response = client.post(
        "/graph/edge/update",
        data={
            "original_name": "market_to_factor",
            "name": "market_to_factor",
            "from_node": "market_daily",
            "to_node": "factor_daily",
            "relation_type": "direct",
            "join_keys": "code=code",
            "time_mode": "same_date",
            "base_time_field": "trade_date",
            "base_time_cast": "",
            "source_time_field": "trade_date",
            "source_start_field": "",
            "source_end_field": "",
            "description": "broken edge",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "Join Keys".encode("utf-8") in response.data
    _, edges = load_nodes_and_edges(graph_path)
    assert any(edge.name == "market_to_factor" for edge in edges)


def test_invalid_time_binding_fails(tmp_path: Path):
    client, graph_path = _client(tmp_path)

    response = client.post(
        "/graph/edge/update",
        data={
            "original_name": "market_to_factor",
            "name": "market_to_factor",
            "from_node": "market_daily",
            "to_node": "factor_daily",
            "relation_type": "direct",
            "join_keys": "base:code -> source:code",
            "time_mode": "same_date",
            "base_time_field": "trade_date",
            "base_time_cast": "",
            "source_time_field": "",
            "source_start_field": "",
            "source_end_field": "",
            "description": "broken edge",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "requires source_time_field" in response.get_data(as_text=True)
    _, edges = load_nodes_and_edges(graph_path)
    assert any(edge.name == "market_to_factor" for edge in edges)
