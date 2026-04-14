from aiquantbase import GraphRuntime
from aiquantbase.executor import QueryExecutionResult


class FakeExecutor:
    def execute_sql(self, sql: str) -> QueryExecutionResult:
        return QueryExecutionResult(
            sql=sql,
            data=[{"etf_code": "159001.SZ", "trading_day": "2026-04-08", "nav": 100000000}],
            rows=1,
            statistics={},
            meta=[],
        )


def test_graph_runtime_render_and_execute_intent():
    runtime = GraphRuntime.from_defaults()
    runtime.executor = FakeExecutor()
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


def test_graph_runtime_execute_sql():
    runtime = GraphRuntime.from_defaults()
    runtime.executor = FakeExecutor()
    result = runtime.execute_sql(
        "SELECT etf_code, trading_day, nav FROM starlight.ad_etf_pcf "
        "WHERE etf_code = '159001.SZ' AND trading_day = '2026-04-08' LIMIT 1"
    )

    assert result["code"] == 0
    assert result["message"] == "success"
    assert "SELECT" in result["sql"]
    assert result["df"].shape[0] == 1


def test_graph_runtime_get_real_fields_json():
    runtime = GraphRuntime.from_defaults()
    result = runtime.get_real_fields_json()

    assert result["code"] == 0
    assert result["message"] == "success"
    assert result["items"]
    stock_daily = next(item for item in result["items"] if item["name"] == "stock_daily_real")
    assert stock_daily["description_zh"]
    assert any(field["field_name"] == "close" for field in stock_daily["fields"])
    assert any(field["field_label_zh"] for field in stock_daily["fields"])


def test_graph_runtime_render_supports_code_in_filter():
    runtime = GraphRuntime.from_defaults()
    intent = {
        "from": "stock_daily_real",
        "select": ["code", "trade_time", "close"],
        "time_range": {
            "field": "trade_time",
            "start": "2024-01-01",
            "end": "2024-01-31",
        },
        "where": {
            "mode": "and",
            "items": [
                {
                    "field": "code",
                    "op": "in",
                    "value": ["000001.SZ", "000002.SZ", "000004.SZ"],
                }
            ],
        },
        "order_by": [
            {"field": "trade_time", "direction": "asc"},
            {"field": "code", "direction": "asc"},
        ],
        "page": 1,
        "page_size": 20,
        "safety": {"lookahead_safe": False, "strict_mode": True},
    }

    sql = runtime.render_intent(intent)
    assert "b0.code IN ('000001.SZ', '000002.SZ', '000004.SZ')" in sql
