from aiquantbase.server import create_app


def test_metadata_catalog_endpoint_returns_chinese_descriptions():
    app = create_app()
    client = app.test_client()

    response = client.get(
        "/api/metadata/catalog",
        query_string={
            "graph_path": "examples/real_combined_graph.yaml",
            "fields_path": "examples/real_combined_fields.yaml",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["nodes"]
    assert payload["edges"]
    assert payload["fields"]
    assert any(item.get("description_zh") for item in payload["nodes"])
    assert any(item.get("description_zh") for item in payload["edges"])
    assert any(item.get("description_zh") for item in payload["fields"])


def test_protocol_summary_endpoint_returns_enabled_entry_summary():
    app = create_app()
    client = app.test_client()

    response = client.get("/api/metadata/protocol-summary")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["code"] == 0
    assert payload["message"] == "success"
    assert payload["summary"]["enabled_real_nodes"] >= 10
    assert payload["summary"]["disabled_real_nodes"] == 0
    assert payload["items"]
    assert any(item["name"] == "stock_daily_real" for item in payload["items"])


def test_disabled_node_cleanup_endpoint_returns_report():
    app = create_app()
    client = app.test_client()

    response = client.get("/api/metadata/disabled-node-cleanup")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["code"] == 0
    assert payload["message"] == "success"
    assert payload["summary"]["disabled_real_nodes"] == 0
    assert "items" in payload
