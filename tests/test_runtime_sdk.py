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
    assert any(item.get("status") == "enabled" for item in catalog["nodes"])
    assert any(item.get("node_role") == "query_entry" for item in catalog["nodes"])
    enabled_nodes = [item["name"] for item in catalog["nodes"] if item.get("status") == "enabled"]
    assert "sync_task_log_real" not in enabled_nodes


def test_graph_runtime_get_real_nodes():
    runtime = GraphRuntime.from_defaults()
    real_nodes = runtime.get_real_nodes()

    assert real_nodes
    assert all(item["name"].endswith("_real") for item in real_nodes)
    assert len(real_nodes) >= 14
    node_names = {item["name"] for item in real_nodes}
    assert "etf_daily_real" in node_names
    assert "index_daily_real" in node_names
    assert "etf_minute_real" in node_names
    assert "index_minute_real" in node_names
    assert any(item["description_zh"] for item in real_nodes)


def test_graph_runtime_get_protocol_summary():
    runtime = GraphRuntime.from_defaults()
    result = runtime.get_protocol_summary()

    assert result["code"] == 0
    assert result["message"] == "success"
    assert result["summary"]["enabled_real_nodes"] >= 10
    assert result["summary"]["disabled_real_nodes"] == 0
    assert result["summary"]["total_fields_across_enabled_nodes"] > 0
    assert result["items"]

    stock_daily = next(item for item in result["items"] if item["name"] == "stock_daily_real")
    assert stock_daily["field_count"] > 50
    assert "code" in stock_daily["identity_fields"]
    assert "trade_time" in stock_daily["identity_fields"]
    assert stock_daily["sample_fields"]


def test_graph_runtime_disabled_node_cleanup_report():
    runtime = GraphRuntime.from_defaults()
    report = runtime.get_disabled_node_cleanup_report()

    assert report["code"] == 0
    assert report["message"] == "success"
    assert report["summary"]["disabled_real_nodes"] == 0
    assert report["summary"]["safe_to_delete"] == 0
    assert report["summary"]["blocked"] == 0
    assert report["safe_nodes"] == []
    assert report["blocked_nodes"] == []
    assert report["items"] == []


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


def test_resolve_symbols():
    runtime = GraphRuntime.from_defaults()
    result = runtime.resolve_symbols(["000001.SZ", "159102.SZ", "123085.SZ", "000300.SH"])

    assert result["ok"] is True
    items = {item["symbol"]: item for item in result["items"]}
    assert items["000001.SZ"]["asset_type"] == "stock"
    assert items["159102.SZ"]["asset_type"] == "etf"
    assert items["159102.SZ"]["node"] == "etf_daily_real"
    assert items["000300.SH"]["asset_type"] == "index"
    assert items["000300.SH"]["node"] == "index_daily_real"
    assert items["123085.SZ"]["asset_type"] == "kzz"


def test_get_supported_fields():
    runtime = GraphRuntime.from_defaults()
    result = runtime.get_supported_fields(asset_type="stock", freq="1d")

    assert result["ok"] is True
    field_names = {item["name"] for item in result["fields"]}
    assert "close_adj" in field_names
    assert "is_st" in field_names


def test_get_supported_fields_with_node_filter():
    runtime = GraphRuntime.from_defaults()
    result = runtime.get_supported_fields(asset_type="stock", freq="1d", node="stock_daily_real")

    assert result["ok"] is True
    assert result["node"] == "stock_daily_real"
    assert all(item["node"] == "stock_daily_real" for item in result["fields"])


def test_get_supported_fields_with_field_role_filter():
    runtime = GraphRuntime.from_defaults()
    result = runtime.get_supported_fields(asset_type="stock", freq="1d", field_role="financial_field")

    assert result["ok"] is True
    assert result["field_role"] == "financial_field"
    assert result["fields"]
    assert all(item["field_role"] == "financial_field" for item in result["fields"])


def test_get_supported_fields_with_derived_only_filter():
    runtime = GraphRuntime.from_defaults()
    result = runtime.get_supported_fields(asset_type="stock", freq="1d", derived_only=True)

    assert result["ok"] is True
    assert result["derived_only"] is True
    assert result["fields"]
    assert all(item["derived"] is True for item in result["fields"])


def test_validate_query_request():
    runtime = GraphRuntime.from_defaults()
    result = runtime.validate_query_request(
        {
            "symbols": ["159102.SZ"],
            "universe": None,
            "fields": ["close_adj", "open"],
            "start": "2024-01-01",
            "end": "2024-01-31",
            "freq": "1d",
            "asset_type": "auto",
        }
    )

    assert result["ok"] is True
    assert result["resolved"]["asset_type"] == "etf"
    assert result["resolved"]["node"] == "etf_daily_real"
    assert result["issues"] == []


def test_validate_query_request_rejects_etf_unsupported_field():
    runtime = GraphRuntime.from_defaults()
    result = runtime.validate_query_request(
        {
            "symbols": ["159102.SZ"],
            "universe": None,
            "fields": ["close_adj", "is_st"],
            "start": "2024-01-01",
            "end": "2024-01-31",
            "freq": "1d",
            "asset_type": "auto",
        }
    )

    assert result["ok"] is False
    assert result["resolved"]["asset_type"] == "etf"
    assert result["resolved"]["node"] == "etf_daily_real"
    assert any(issue["code"] == "unsupported_field" for issue in result["issues"])


def test_resolve_best_node():
    runtime = GraphRuntime.from_defaults()
    result = runtime.resolve_best_node(
        symbols=["159102.SZ"],
        fields=["close_adj", "open"],
        freq="1d",
        asset_type="auto",
    )

    assert result["ok"] is True
    assert result["asset_type"] == "etf"
    assert result["node"] == "etf_daily_real"
    assert result["issues"] == []


def test_query_daily_for_stock_uses_query_intent_path():
    runtime = GraphRuntime.from_defaults()
    runtime.executor = FakeExecutor()
    result = runtime.query_daily(
        symbols=["000001.SZ"],
        fields=["close_adj", "is_st"],
        start="2024-01-01 00:00:00",
        end="2024-01-31 23:59:59",
    )

    assert result["ok"] is True
    assert result["meta"]["asset_type"] == "stock"
    assert result["meta"]["node"] == "stock_daily_real"
    assert "intent" in result["debug"]
    assert "sql" in result["debug"]


def test_build_intent_from_requirement():
    runtime = GraphRuntime.from_defaults()
    result = runtime.build_intent_from_requirement(
        {
            "fields": ["close_adj", "open"],
            "scope": {
                "symbols": ["159102.SZ"],
                "freq": "1d",
                "start": "2024-01-01",
                "end": "2024-01-31",
            },
        }
    )

    assert result["ok"] is True
    assert result["resolved"]["asset_type"] == "etf"
    assert result["resolved"]["node"] == "etf_daily_real"
    assert result["intent"]["from"] == "etf_daily_real"
    assert any(
        item["field"] == "code" and item["op"] == "=" and item["value"] == "159102.SZ"
        for item in result["intent"]["where"]["items"]
    )


def test_query_daily_for_etf_uses_logical_entry_and_security_type_filter():
    runtime = GraphRuntime.from_defaults()
    runtime.executor = FakeExecutor()
    result = runtime.query_daily(
        symbols=["159102.SZ"],
        fields=["close_adj", "open"],
        start="2024-01-01 00:00:00",
        end="2024-01-31 23:59:59",
    )

    assert result["ok"] is True
    assert result["meta"]["asset_type"] == "etf"
    assert result["meta"]["node"] == "etf_daily_real"
    assert result["debug"]["intent"]["from"] == "etf_daily_real"
    assert "EXTRA_ETF" in result["debug"]["sql"]


def test_query_daily_for_index_uses_logical_entry_and_security_type_filter():
    runtime = GraphRuntime.from_defaults()
    runtime.executor = FakeExecutor()
    result = runtime.query_daily(
        symbols=["000300.SH"],
        fields=["close", "open"],
        start="2024-01-01 00:00:00",
        end="2024-01-31 23:59:59",
    )

    assert result["ok"] is True
    assert result["meta"]["asset_type"] == "index"
    assert result["meta"]["node"] == "index_daily_real"
    assert result["debug"]["intent"]["from"] == "index_daily_real"
    assert "EXTRA_INDEX_A_SH_SZ" in result["debug"]["sql"]


def test_execute_requirement():
    runtime = GraphRuntime.from_defaults()
    runtime.executor = FakeExecutor()
    result = runtime.execute_requirement(
        {
            "fields": ["close_adj", "open"],
            "scope": {
                "symbols": ["000001.SZ"],
                "freq": "1d",
                "start": "2024-01-01",
                "end": "2024-01-31",
            },
        }
    )

    assert result["ok"] is True
    assert hasattr(result["df"], "shape")
    assert result["resolved"]["asset_type"] == "stock"
    assert result["debug"]["intent"]["from"] == "stock_daily_real"


def test_query_minute_for_stock_uses_query_intent_path():
    runtime = GraphRuntime.from_defaults()
    runtime.executor = FakeExecutor()
    result = runtime.query_minute(
        symbols=["000001.SZ"],
        fields=["minute_close", "minute_volume"],
        start="2024-01-02 09:30:00",
        end="2024-01-02 10:30:00",
    )

    assert result["ok"] is True
    assert result["meta"]["asset_type"] == "stock"
    assert result["meta"]["node"] == "stock_minute_real"
    assert result["debug"]["intent"]["from"] == "stock_minute_real"


def test_validate_query_request_rejects_large_minute_query():
    runtime = GraphRuntime.from_defaults()
    result = runtime.validate_query_request(
        {
            "symbols": ["000001.SZ"],
            "universe": None,
            "fields": ["minute_close"],
            "start": "2024-01-02 09:30:00",
            "end": "2024-01-03 18:30:00",
            "freq": "1m",
            "asset_type": "stock",
        }
    )

    assert result["ok"] is False
    assert any(issue["code"] == "minute_time_range_too_large" for issue in result["issues"])


def test_validate_query_request_rejects_large_daily_query_without_symbols():
    runtime = GraphRuntime.from_defaults()
    result = runtime.validate_query_request(
        {
            "symbols": [],
            "universe": None,
            "fields": ["close_adj"],
            "start": "2024-01-01",
            "end": "2024-03-15",
            "freq": "1d",
            "asset_type": "stock",
        }
    )

    assert result["ok"] is False
    assert any(issue["code"] == "missing_symbols" for issue in result["issues"])
