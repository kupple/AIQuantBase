from aiquantbase import GraphRuntime


def test_graph_runtime_render_and_execute_intent():
    runtime = GraphRuntime.from_defaults()
    intent = {
        "from": "etf_pcf_real",
        "select": [
            "etf_code",
            "etf_trading_day",
            "etf_nav",
        ],
        "where": {
            "mode": "and",
            "items": [
                {"field": "etf_code", "op": "=", "value": "159001.SZ"},
            ],
        },
        "time_range": {
            "field": "trading_day",
            "start": "2026-04-08",
            "end": "2026-04-08",
        },
        "page": 1,
        "page_size": 10,
        "safety": {
            "lookahead_safe": True,
            "strict_mode": True,
        },
    }

    sql = runtime.render_intent(intent)
    assert "FROM starlight.ad_etf_pcf" in sql

    result = runtime.execute_intent(intent)
    assert result["code"] == 0
    assert result["message"] == "success"
    assert "SELECT" in result["sql"]
    assert hasattr(result["df"], "shape")


def test_graph_runtime_metadata_catalog():
    runtime = GraphRuntime.from_defaults()
    catalog = runtime.get_metadata_catalog()

    assert catalog["nodes"]
    assert catalog["edges"]
    assert catalog["fields"]
    assert any(item.get("description_zh") for item in catalog["nodes"])
    assert any(item.get("description_zh") for item in catalog["edges"])
    assert any(item.get("description_zh") for item in catalog["fields"])
    assert any(item.get("time_semantics") for item in catalog["fields"])
    assert any(item.get("is_ai_entry") for item in catalog["nodes"])
    assert any(item.get("node_role") == "query_entry" for item in catalog["nodes"])
    assert "sync_task_log_real" not in [item["name"] for item in catalog["nodes"] if item.get("is_ai_entry")]


def test_graph_runtime_get_real_nodes():
    runtime = GraphRuntime.from_defaults()
    real_nodes = runtime.get_real_nodes()

    assert real_nodes
    assert all(item["name"].endswith("_real") for item in real_nodes)
    assert len(real_nodes) == 54
    assert any(item["description_zh"] for item in real_nodes)
