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
