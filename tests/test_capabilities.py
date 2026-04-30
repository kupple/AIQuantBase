from __future__ import annotations

from pathlib import Path

from aiquantbase import load_capability_workspace
from aiquantbase.config import dump_yaml, load_yaml
from aiquantbase.runtime import GraphRuntime
from aiquantbase.server import create_app


def _write_capability_workspace(tmp_path: Path) -> Path:
    root = tmp_path / "aiquantbase_capabilities"
    mode_dir = root / "modes" / "strategy"
    mode_dir.mkdir(parents=True)

    (root / "manifest.yaml").write_text(
        dump_yaml(
            {
                "version": 1,
                "provider": "aiquantbase",
                "capability_registry": {
                    "custom_factor_daily": {
                        "name": "自定义日频因子",
                        "description": "测试用自定义因子",
                        "output_scope": {
                            "scope_type": "daily_panel",
                            "entity_type": "stock",
                            "keys": {"entity": "code", "time": "trade_time"},
                        },
                        "default_slots": ["ranking_fields", "filter_fields"],
                    }
                },
                "nodes": {
                    "stock_daily_real": {
                        "description": "daily panel",
                        "asset_types": ["stock"],
                        "query_profiles": ["panel_time_series"],
                        "keys": {"symbol": "code", "time": "trade_time"},
                        "semantics": {
                            "price.daily": {
                                "fields": {
                                    "open": "open",
                                    "close": "close",
                                }
                            }
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (root / "query_templates.yaml").write_text(
        dump_yaml(
            {
                "version": 1,
                "query_templates": {
                    "stock_daily_panel": {
                        "capability": "price.daily",
                        "query_profile": "panel_time_series",
                        "params_template": {
                            "asset_type": "stock",
                            "freq": "1d",
                            "symbols": [],
                            "fields": [],
                            "start": None,
                            "end": None,
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (root / "mode_capabilities.yaml").write_text(
        dump_yaml(
            {
                "version": 1,
                "mode_config_files": [
                    "modes/strategy/discrete_stock.yaml",
                ],
                "provider_manifest": "manifest.yaml",
                "query_templates": "query_templates.yaml",
            }
        ),
        encoding="utf-8",
    )
    (mode_dir / "discrete_stock.yaml").write_text(
        dump_yaml(
            {
                "version": 1,
                "mode_kind": "strategy_modes",
                "mode_name": "discrete_stock",
                "description": "discrete stock",
                "aiquantbase": {
                    "accepted_output_scopes": [
                        {"scope_type": "daily_panel", "entity_type": "stock"},
                    ],
                    "required_capabilities": [
                        {"capability": "price.daily", "fields": ["open", "close"]},
                    ],
                    "conditional_capabilities": [],
                    "optional_capabilities": [],
                    "extension_slots": [
                        {
                            "slot": "ranking_fields",
                            "name": "排序字段",
                            "query_profile": "panel_time_series",
                            "freq": "1d",
                        },
                        {
                            "slot": "filter_fields",
                            "name": "过滤字段",
                            "query_profile": "panel_time_series",
                            "freq": "1d",
                        },
                    ],
                    "extension_capability_bindings": [],
                },
                "query_needs": [
                    {
                        "id": "daily_market",
                        "use": "stock_daily_panel",
                        "required": True,
                        "derive": {
                            "symbols_from": "scope.symbols",
                            "start_from": "scope.start",
                            "end_from": "scope.end",
                            "fields_from": ["data_requirement.fields"],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return root


def _client(tmp_path: Path):
    app = create_app(
        default_graph_path=tmp_path / "graph.yaml",
        default_fields_path=tmp_path / "fields.yaml",
        sync_project_root=tmp_path / "sync_project",
    )
    app.config["TESTING"] = True
    return app.test_client()


def test_capabilities_workspace_lists_provider_nodes_and_modes(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    client = _client(tmp_path)

    response = client.get("/api/capabilities/workspace", query_string={"capability_root": str(capability_root)})

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["workspace"]["capability_root"] == str(capability_root)
    assert payload["provider_nodes"][0]["name"] == "stock_daily_real"
    assert payload["mode_profiles"][0]["mode_id"] == "strategy_modes.discrete_stock"
    assert payload["mode_profiles"][0]["mode_name"] == "discrete_stock"
    assert payload["mode_profiles"][0]["conditional_capabilities"] == []
    assert payload["mode_profiles"][0]["accepted_output_scopes"] == [
        {"scope_type": "daily_panel", "entity_type": "stock"},
    ]
    assert [item["slot"] for item in payload["mode_profiles"][0]["extension_slots"]] == [
        "ranking_fields",
        "filter_fields",
    ]
    assert payload["capability_registry"][0]["capability"] == "custom_factor_daily"
    assert payload["capability_registry"][0]["name"] == "自定义日频因子"
    assert payload["capability_registry"][0]["output_scope"] == {
        "scope_type": "daily_panel",
        "entity_type": "stock",
        "keys": {"entity": "code", "time": "trade_time"},
    }
    assert payload["capabilities"][0]["capability"] == "price.daily"


def test_discrete_stock_valuation_snapshot_is_bound_through_extension_slots():
    workspace = load_capability_workspace()
    mode = next(
        item for item in workspace["mode_profiles"] if item["mode_id"] == "strategy_modes.discrete_stock"
    )
    binding = next(
        item
        for item in mode["extension_capability_bindings"]
        if item["capability"] == "valuation_snapshot"
    )

    assert binding["fields"] == ["market_cap", "float_market_cap", "turnover_rate"]
    assert binding["slots"] == ["ranking_fields", "filter_fields", "weighting_fields", "report_fields"]
    assert mode["accepted_output_scopes"] == [
        {"scope_type": "daily_panel", "entity_type": "stock"},
        {"scope_type": "linked_daily_panel", "output_entity_type": "stock"},
    ]

    stock_daily = workspace["provider_manifest"]["nodes"]["stock_daily_real"]
    assert stock_daily["semantics"]["valuation_snapshot"]["fields"] == {
        "market_cap": "market_cap",
        "float_market_cap": "float_market_cap",
        "turnover_rate": "turnover_rate",
    }


def test_stock_industry_provider_node_maps_standard_fields_to_physical_columns():
    workspace = load_capability_workspace()
    stock_industry = workspace["provider_manifest"]["nodes"]["stock_industry_daily_real"]
    fields = stock_industry["semantics"]["industry_classification"]["fields"]

    assert stock_industry["description"] == "行业日线行情节点表"
    assert stock_industry["keys"] == {"symbol": "index_code", "time": "trade_date"}
    assert fields == {
        "industry_index_code": "index_code",
        "industry_index_name": "industry_index_name",
        "industry_code": "industry_code",
        "industry_level1_name": "industry_level1_name",
        "industry_level2_name": "industry_level2_name",
        "industry_level3_name": "industry_level3_name",
        "industry_name": "industry_name",
    }

    graph = load_yaml(Path(__file__).resolve().parents[1] / "config" / "graph.yaml")
    graph_node = next(item for item in graph["nodes"] if item["name"] == "stock_industry_daily_real")
    assert graph_node["wide_table"] is None
    assert graph_node["table"] == "starlight.ad_industry_daily"
    assert graph_node["entity_keys"] == ["index_code"]
    assert graph_node["time_key"] == "trade_date"
    assert "index_code" in graph_node["fields"]
    assert "industry_level1_name" not in graph_node["fields"]
    assert "level1_name" not in graph_node["fields"]

    field_catalog = load_yaml(Path(__file__).resolve().parents[1] / "config" / "fields.yaml")["fields"]
    constituent_level1 = next(
        item
        for item in field_catalog
        if item.get("base_node") == "industry_constituent_real"
        and item.get("standard_field") == "industry_level1_name"
    )
    assert constituent_level1["binding_mode"] == "source_table"
    assert constituent_level1["source_table"] == "starlight.ad_industry_base_info"
    assert constituent_level1["source_field"] == "level1_name"
    assert constituent_level1["source_node"] is None
    assert constituent_level1["join_keys"] == [{"base": "index_code", "source": "index_code"}]

    daily_level1 = next(
        item
        for item in field_catalog
        if item.get("base_node") == "stock_industry_daily_real"
        and item.get("standard_field") == "industry_level1_name"
    )
    assert daily_level1["binding_mode"] == "source_table"
    assert daily_level1["source_table"] == "starlight.ad_industry_base_info"
    assert daily_level1["source_field"] == "level1_name"
    assert daily_level1["join_keys"] == [{"base": "index_code", "source": "index_code"}]


def test_discrete_stock_industry_classification_is_bound_as_linked_extension():
    workspace = load_capability_workspace()
    mode = next(
        item for item in workspace["mode_profiles"] if item["mode_id"] == "strategy_modes.discrete_stock"
    )
    binding = next(
        item
        for item in mode["extension_capability_bindings"]
        if item["capability"] == "industry_classification"
    )
    provider = next(
        item
        for item in workspace["capabilities"]
        if item["capability"] == "industry_classification"
    )

    assert binding["fields"] == [
        "industry_code",
        "industry_level1_name",
        "industry_level2_name",
        "industry_level3_name",
        "industry_name",
    ]
    assert binding["slots"] == ["filter_fields", "groupby_fields", "neutralization_fields", "report_fields"]
    assert provider["provider_node"] == "stock_industry_daily_real"
    assert provider["output_scope"] == {
        "scope_type": "linked_daily_panel",
        "base_entity_type": "stock",
        "linked_entity_type": "industry",
        "output_entity_type": "stock",
        "keys": {"entity": "code", "time": "trade_time"},
    }


def test_industry_source_table_bindings_render_expected_sql_paths():
    runtime = GraphRuntime()

    stock_sql = runtime.render_intent(
        {
            "from": "stock_daily_real",
            "select": ["industry_level1_name", "industry_name"],
            "time_range": {
                "field": "trade_time",
                "start": "2024-01-01",
                "end": "2024-01-02",
            },
        }
    )
    assert "FROM (SELECT code, trade_time FROM starlight.stock_daily_real" in stock_sql
    assert "LEFT JOIN starlight.ad_industry_constituent" in stock_sql
    assert "(br2.out_date IS NULL OR toDate(b0.trade_time) < br2.out_date)" in stock_sql
    assert "LEFT JOIN (SELECT * FROM starlight.ad_industry_base_info WHERE level_type = 3 AND is_pub = 1)" in stock_sql
    assert "b0.index_code" not in stock_sql

    industry_daily_sql = runtime.render_intent(
        {
            "from": "stock_industry_daily_real",
            "select": ["industry_level1_name", "industry_index_name"],
            "time_range": {
                "field": "trade_date",
                "start": "2024-01-01",
                "end": "2024-01-02",
            },
        }
    )
    assert "FROM (SELECT index_code, trade_date FROM starlight.ad_industry_daily" in industry_daily_sql
    assert "LEFT JOIN (SELECT * FROM starlight.ad_industry_base_info WHERE level_type = 3 AND is_pub = 1)" in industry_daily_sql
    assert "t1.level1_name AS industry_level1_name" in industry_daily_sql
    assert "t1.index_name AS industry_index_name" in industry_daily_sql


def test_capabilities_provider_node_upsert_registers_custom_fields(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    client = _client(tmp_path)

    response = client.post(
        "/api/capabilities/provider-node",
        json={
            "capability_root": str(capability_root),
            "node_name": "stock_daily_real",
            "capability": "custom_factor_daily",
            "capability_name": "自定义质量因子",
            "capability_description": "用于排序和过滤",
            "default_slots": ["ranking_fields", "filter_fields"],
            "output_scope": {
                "scope_type": "daily_panel",
                "entity_type": "stock",
                "keys": {"entity": "code", "time": "trade_time"},
            },
            "fields": [
                {"semantic_field": "quality_score", "source_field": "quality_score"},
                {"semantic_field": "roe_ttm", "source_field": "roe_ttm"},
            ],
        },
    )

    assert response.status_code == 200
    manifest = load_yaml(capability_root / "manifest.yaml")
    registry = manifest["capability_registry"]["custom_factor_daily"]
    assert registry["name"] == "自定义质量因子"
    assert registry["description"] == "用于排序和过滤"
    assert registry["default_slots"] == ["ranking_fields", "filter_fields"]
    assert registry["output_scope"] == {
        "scope_type": "daily_panel",
        "entity_type": "stock",
        "keys": {"entity": "code", "time": "trade_time"},
    }
    fields = manifest["nodes"]["stock_daily_real"]["semantics"]["custom_factor_daily"]["fields"]
    assert fields == {"quality_score": "quality_score", "roe_ttm": "roe_ttm"}


def test_capabilities_mode_upsert_and_preview_resolve_custom_field(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    client = _client(tmp_path)
    mode_path = capability_root / "modes" / "strategy" / "discrete_stock.yaml"

    provider_response = client.post(
        "/api/capabilities/provider-node",
        json={
            "capability_root": str(capability_root),
            "node_name": "stock_daily_real",
            "capability": "custom_factor_daily",
            "fields": {"quality_score": "quality_score"},
        },
    )
    assert provider_response.status_code == 200

    mode_response = client.post(
        "/api/capabilities/mode-capability",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "section": "extension_capability_bindings",
            "capability": "custom_factor_daily",
            "fields": ["quality_score"],
            "slots": ["ranking_fields"],
        },
    )
    assert mode_response.status_code == 200
    mode_config = load_yaml(mode_path)
    binding = mode_config["aiquantbase"]["extension_capability_bindings"][0]
    assert binding["capability"] == "custom_factor_daily"
    assert binding["slots"] == ["ranking_fields"]

    preview_response = client.post(
        "/api/capabilities/preview",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "symbols": ["603005.SH"],
            "start": "2017-01-01",
            "end": "2017-01-31",
            "fields": ["open", "close", "quality_score"],
            "optional_data": [
                {
                    "capability": "custom_factor_daily",
                    "fields": ["quality_score"],
                    "slots": ["ranking_fields"],
                }
            ],
        },
    )

    assert preview_response.status_code == 200
    payload = preview_response.get_json()
    daily = payload["resolved_queries"][0]
    assert daily["provider_node"] == "stock_daily_real"
    assert daily["requested_field_map"]["quality_score"] == "quality_score"
    assert daily["unmapped_fields"] == []
    extension = payload["resolved_queries"][1]
    assert extension["id"] == "extension_custom_factor_daily"
    assert extension["capability"] == "custom_factor_daily"
    assert extension["output_scope"] == {
        "scope_type": "daily_panel",
        "entity_type": "stock",
        "keys": {"entity": "code", "time": "trade_time"},
    }
    assert extension["requested_field_map"] == {"quality_score": "quality_score"}
    assert extension["slots"] == ["ranking_fields"]


def test_capabilities_preview_resolves_industry_classification_extension():
    client = _client(Path(__file__).resolve().parents[1])

    preview_response = client.post(
        "/api/capabilities/preview",
        json={
            "mode_id": "strategy_modes.discrete_stock",
            "symbols": ["603005.SH"],
            "start": "2017-01-01",
            "end": "2017-01-31",
            "fields": ["open", "close"],
            "optional_data": [
                {
                    "capability": "industry_classification",
                    "fields": ["industry_level1_name", "industry_name"],
                    "slots": ["groupby_fields", "report_fields"],
                }
            ],
        },
    )

    assert preview_response.status_code == 200
    payload = preview_response.get_json()
    assert payload["ok"] is True
    extension = next(
        item for item in payload["resolved_queries"] if item["id"] == "extension_industry_classification"
    )
    assert extension["provider_node"] == "stock_industry_daily_real"
    assert extension["key_schema"] == {"symbol": "index_code", "time": "trade_date"}
    assert extension["output_scope"]["scope_type"] == "linked_daily_panel"
    assert extension["requested_field_map"] == {
        "industry_level1_name": "industry_level1_name",
        "industry_name": "industry_name",
    }
    assert extension["slots"] == ["groupby_fields", "report_fields"]


def test_capabilities_preview_rejects_extension_output_scope_not_accepted(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    client = _client(tmp_path)

    provider_response = client.post(
        "/api/capabilities/provider-node",
        json={
            "capability_root": str(capability_root),
            "node_name": "index_factor_daily_real",
            "capability": "index_factor_daily",
            "capability_name": "指数因子日频",
            "default_slots": ["ranking_fields"],
            "output_scope": {
                "scope_type": "daily_panel",
                "entity_type": "index",
                "keys": {"entity": "index_code", "time": "trade_time"},
            },
            "fields": {"index_score": "index_score"},
        },
    )
    assert provider_response.status_code == 200

    mode_response = client.post(
        "/api/capabilities/mode-capability",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "section": "extension_capability_bindings",
            "capability": "index_factor_daily",
            "fields": ["index_score"],
            "slots": ["ranking_fields"],
        },
    )
    assert mode_response.status_code == 200

    preview_response = client.post(
        "/api/capabilities/preview",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "symbols": ["603005.SH"],
            "start": "2017-01-01",
            "end": "2017-01-31",
            "fields": ["open", "close", "index_score"],
            "optional_data": [
                {
                    "capability": "index_factor_daily",
                    "fields": ["index_score"],
                    "slots": ["ranking_fields"],
                }
            ],
        },
    )

    assert preview_response.status_code == 200
    payload = preview_response.get_json()
    assert payload["ok"] is False
    assert payload["diagnostics"][0]["code"] == "extension_output_scope_not_accepted"


def test_capabilities_mode_extension_binding_can_be_deleted(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    client = _client(tmp_path)
    mode_path = capability_root / "modes" / "strategy" / "discrete_stock.yaml"

    response = client.post(
        "/api/capabilities/mode-capability",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "section": "extension_capability_bindings",
            "capability": "custom_factor_daily",
            "fields": ["quality_score"],
            "slots": ["ranking_fields"],
        },
    )
    assert response.status_code == 200

    delete_response = client.post(
        "/api/capabilities/mode-capability/delete",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "section": "extension_capability_bindings",
            "capability": "custom_factor_daily",
        },
    )

    assert delete_response.status_code == 200
    assert delete_response.get_json()["result"]["removed_count"] == 1
    mode_config = load_yaml(mode_path)
    assert mode_config["aiquantbase"]["extension_capability_bindings"] == []


def test_capabilities_extension_delete_can_purge_provider_registration(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    client = _client(tmp_path)
    mode_path = capability_root / "modes" / "strategy" / "discrete_stock.yaml"
    extra_mode_path = capability_root / "modes" / "strategy" / "custom_strategy.yaml"

    mode_registry = load_yaml(capability_root / "mode_capabilities.yaml")
    mode_registry["mode_config_files"].append("modes/strategy/custom_strategy.yaml")
    (capability_root / "mode_capabilities.yaml").write_text(dump_yaml(mode_registry), encoding="utf-8")

    extra_mode = load_yaml(mode_path)
    extra_mode["mode_name"] = "custom_strategy"
    extra_mode["aiquantbase"]["extension_capability_bindings"] = [
        {
            "capability": "custom_factor_daily",
            "fields": ["quality_score"],
            "slots": ["ranking_fields"],
        }
    ]
    extra_mode_path.write_text(dump_yaml(extra_mode), encoding="utf-8")

    provider_response = client.post(
        "/api/capabilities/provider-node",
        json={
            "capability_root": str(capability_root),
            "node_name": "stock_daily_real",
            "capability": "custom_factor_daily",
            "fields": {"quality_score": "quality_score"},
        },
    )
    assert provider_response.status_code == 200

    mode_response = client.post(
        "/api/capabilities/mode-capability",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "section": "extension_capability_bindings",
            "capability": "custom_factor_daily",
            "fields": ["quality_score"],
            "slots": ["ranking_fields"],
        },
    )
    assert mode_response.status_code == 200

    delete_response = client.post(
        "/api/capabilities/mode-capability/delete",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "section": "extension_capability_bindings",
            "capability": "custom_factor_daily",
            "delete_provider_registration": True,
        },
    )

    assert delete_response.status_code == 200
    result = delete_response.get_json()["result"]
    assert result["removed_count"] == 2
    assert result["provider_registration"]["removed_registry"] is True
    assert result["provider_registration"]["removed_semantics_from"] == ["stock_daily_real"]

    mode_config = load_yaml(mode_path)
    assert mode_config["aiquantbase"]["extension_capability_bindings"] == []
    extra_mode_config = load_yaml(extra_mode_path)
    assert extra_mode_config["aiquantbase"]["extension_capability_bindings"] == []
    manifest = load_yaml(capability_root / "manifest.yaml")
    assert "custom_factor_daily" not in manifest["capability_registry"]
    assert "custom_factor_daily" not in manifest["nodes"]["stock_daily_real"]["semantics"]


def test_capabilities_preview_accepts_new_bound_extension_capability(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    client = _client(tmp_path)

    provider_response = client.post(
        "/api/capabilities/provider-node",
        json={
            "capability_root": str(capability_root),
            "node_name": "sentiment_daily_real",
            "capability": "sentiment_news_daily",
            "capability_name": "新闻情绪日频",
            "fields": {"sentiment_score": "sentiment_score"},
        },
    )
    assert provider_response.status_code == 200

    mode_response = client.post(
        "/api/capabilities/mode-capability",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "section": "extension_capability_bindings",
            "capability": "sentiment_news_daily",
            "fields": ["sentiment_score"],
            "slots": ["ranking_fields"],
        },
    )
    assert mode_response.status_code == 200

    preview_response = client.post(
        "/api/capabilities/preview",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "symbols": ["603005.SH"],
            "start": "2017-01-01",
            "end": "2017-01-31",
            "fields": ["open", "close", "sentiment_score"],
            "optional_data": [
                {
                    "capability": "sentiment_news_daily",
                    "fields": ["sentiment_score"],
                    "slots": ["ranking_fields"],
                }
            ],
        },
    )

    assert preview_response.status_code == 200
    payload = preview_response.get_json()
    extension = payload["resolved_queries"][1]
    assert extension["id"] == "extension_sentiment_news_daily"
    assert extension["capability"] == "sentiment_news_daily"
    assert extension["provider_node"] == "sentiment_daily_real"
    assert extension["requested_field_map"] == {"sentiment_score": "sentiment_score"}
    assert extension["slots"] == ["ranking_fields"]
