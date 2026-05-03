from __future__ import annotations

from pathlib import Path

import pytest

from aiquantbase import build_capability_preview, load_capability_workspace
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
    assert payload["capability_registry"][0]["enabled"] is True
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


def test_factor_research_daily_query_maps_tradeable_fields_from_mode_config():
    client = _client(Path(__file__).resolve().parents[1])

    response = client.post(
        "/api/capabilities/preview",
        json={
            "mode_id": "research_modes.factor_research",
            "universe": "all_a",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "fields": ["close_adj", "dollar_volume", "is_st", "listed_days"],
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    daily = next(item for item in payload["resolved_queries"] if item["id"] == "daily_market")
    assert daily["provider_node"] == "stock_daily_real"
    assert daily["requested_field_map"]["close_adj"] == "close_adj"
    assert daily["requested_field_map"]["dollar_volume"] == "amount"
    assert daily["requested_field_map"]["is_st"] == "is_st"
    assert daily["requested_field_map"]["listed_days"] == {
        "derive": "trade_date - list_date",
        "depends_on": ["list_date"],
    }
    assert daily["unmapped_fields"] == []


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


def test_discrete_stock_stock_pool_membership_is_bound_as_interval_extension():
    workspace = load_capability_workspace()
    mode = next(
        item for item in workspace["mode_profiles"] if item["mode_id"] == "strategy_modes.discrete_stock"
    )
    binding = next(
        item
        for item in mode["extension_capability_bindings"]
        if item["capability"] == "stock_pool_membership"
    )
    provider = next(
        item
        for item in workspace["capabilities"]
        if item["capability"] == "stock_pool_membership"
    )

    assert binding["provider_node"] == "index_constituent_real"
    assert binding["query_profile"] == "interval_membership"
    assert binding["fields"] == ["index_constituent_code", "index_constituent_name", "index_name"]
    assert binding["slots"] == ["universe_fields", "filter_fields", "report_fields"]
    assert provider["provider_node"] == "index_constituent_real"
    assert provider["query_profiles"] == ["interval_membership"]
    assert provider["output_scope"] == {
        "scope_type": "daily_panel",
        "entity_type": "stock",
        "keys": {"entity": "code", "time": "trade_time"},
    }


def test_industry_source_table_bindings_render_expected_sql_paths():
    runtime = GraphRuntime()

    with pytest.raises(ValueError):
        runtime.render_intent(
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

    with pytest.raises(ValueError):
        runtime.render_intent(
            {
                "from": "stock_daily_real",
                "select": ["index_constituent_code", "index_constituent_name"],
                "time_range": {
                    "field": "trade_time",
                    "start": "2024-01-01",
                    "end": "2024-01-02",
                },
            }
        )

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


def test_graph_registered_node_fields_are_not_blocked_by_legacy_asset_allowlist(tmp_path: Path):
    graph_path = tmp_path / "graph.yaml"
    fields_path = tmp_path / "fields.yaml"
    graph_path.write_text(
        dump_yaml(
            {
                "nodes": [
                    {
                        "name": "stock_daily_real",
                        "table": "demo.stock_daily_real",
                        "entity_keys": ["code"],
                        "time_key": "trade_time",
                        "grain": "daily",
                        "asset_type": "stock",
                        "query_freq": "1d",
                        "fields": ["code", "trade_time", "close"],
                    },
                    {
                        "name": "custom_sentiment_real",
                        "table": "demo.custom_sentiment_real",
                        "entity_keys": ["code"],
                        "time_key": "trade_time",
                        "grain": "daily",
                        "asset_type": "stock",
                        "query_freq": "1d",
                        "fields": ["code", "trade_time", "signal_value"],
                    },
                ],
                "edges": [
                    {
                        "name": "stock_daily_real_to_custom_sentiment_real",
                        "from": "stock_daily_real",
                        "to": "custom_sentiment_real",
                        "from_node": "stock_daily_real",
                        "to_node": "custom_sentiment_real",
                        "relation_type": "direct",
                        "join_keys": [{"base": "code", "source": "code"}],
                        "time_binding": {
                            "mode": "same_date",
                            "base_time_field": "trade_time",
                            "source_time_field": "trade_time",
                            "base_time_cast": "date",
                            "source_time_cast": "date",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    fields_path.write_text(
        dump_yaml(
            {
                "fields": [
                    {
                        "standard_field": "custom_sentiment_signal",
                        "source_node": "custom_sentiment_real",
                        "source_field": "signal_value",
                        "field_role": "custom_field",
                        "base_node": "stock_daily_real",
                        "binding_mode": "source_node",
                        "description_zh": "自定义情绪信号",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    runtime = GraphRuntime(graph_path=graph_path, fields_path=fields_path)

    fields = {
        item["name"]
        for item in runtime.get_supported_fields(asset_type="stock", freq="1d", node="stock_daily_real")["fields"]
    }
    assert "custom_sentiment_signal" in fields


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
            "field_usages": {
                "quality_score": {"usages": ["ranking_fields"]},
                "roe_ttm": {"usages": ["filter_fields", "ranking_fields"]},
            },
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
    assert registry["field_usages"] == {
        "quality_score": {"usages": ["ranking_fields"]},
        "roe_ttm": {"usages": ["filter_fields", "ranking_fields"]},
    }
    assert registry["output_scope"] == {
        "scope_type": "daily_panel",
        "entity_type": "stock",
        "keys": {"entity": "code", "time": "trade_time"},
    }
    fields = manifest["nodes"]["stock_daily_real"]["semantics"]["custom_factor_daily"]["fields"]
    assert fields == {"quality_score": "quality_score", "roe_ttm": "roe_ttm"}


def test_capabilities_provider_node_upsert_can_replace_other_provider_nodes(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    manifest_path = capability_root / "manifest.yaml"
    manifest = load_yaml(manifest_path)
    manifest["nodes"]["old_factor_real"] = {
        "description": "old factor node",
        "asset_types": ["stock"],
        "query_profiles": ["panel_time_series"],
        "keys": {"symbol": "code", "time": "trade_time"},
        "semantics": {
            "custom_factor_daily": {
                "fields": {"old_score": "old_score"},
            },
        },
    }
    manifest_path.write_text(dump_yaml(manifest), encoding="utf-8")
    client = _client(tmp_path)

    response = client.post(
        "/api/capabilities/provider-node",
        json={
            "capability_root": str(capability_root),
            "node_name": "stock_daily_real",
            "capability": "custom_factor_daily",
            "replace_provider_nodes": True,
            "fields": {"quality_score": "quality_score"},
        },
    )

    assert response.status_code == 200
    manifest = load_yaml(manifest_path)
    assert "custom_factor_daily" in manifest["nodes"]["stock_daily_real"]["semantics"]
    assert "semantics" not in manifest["nodes"]["old_factor_real"]


def test_capabilities_provider_node_upsert_merges_fields_by_default(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    manifest_path = capability_root / "manifest.yaml"
    manifest = load_yaml(manifest_path)
    manifest["nodes"]["stock_daily_real"]["semantics"]["custom_factor_daily"] = {
        "fields": {"old_score": "old_score"},
    }
    manifest_path.write_text(dump_yaml(manifest), encoding="utf-8")
    client = _client(tmp_path)

    response = client.post(
        "/api/capabilities/provider-node",
        json={
            "capability_root": str(capability_root),
            "node_name": "stock_daily_real",
            "capability": "custom_factor_daily",
            "fields": {"quality_score": "quality_score"},
        },
    )

    assert response.status_code == 200
    fields = load_yaml(manifest_path)["nodes"]["stock_daily_real"]["semantics"]["custom_factor_daily"]["fields"]
    assert fields == {"old_score": "old_score", "quality_score": "quality_score"}


def test_capabilities_provider_node_upsert_can_replace_fields(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    manifest_path = capability_root / "manifest.yaml"
    manifest = load_yaml(manifest_path)
    manifest["nodes"]["stock_daily_real"]["semantics"]["custom_factor_daily"] = {
        "fields": {"old_score": "old_score"},
    }
    manifest_path.write_text(dump_yaml(manifest), encoding="utf-8")
    client = _client(tmp_path)

    response = client.post(
        "/api/capabilities/provider-node",
        json={
            "capability_root": str(capability_root),
            "node_name": "stock_daily_real",
            "capability": "custom_factor_daily",
            "replace_fields": True,
            "fields": {"quality_score": "quality_score"},
        },
    )

    assert response.status_code == 200
    fields = load_yaml(manifest_path)["nodes"]["stock_daily_real"]["semantics"]["custom_factor_daily"]["fields"]
    assert fields == {"quality_score": "quality_score"}


def test_capabilities_mode_capability_upsert_stores_mode_field_map(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    client = _client(tmp_path)

    response = client.post(
        "/api/capabilities/mode-capability",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "section": "required_capabilities",
            "capability": "price.daily",
            "provider_node": "stock_daily_real",
            "fields": ["open", "close"],
            "field_map": {"open": "open_price", "close": "close_price"},
        },
    )

    assert response.status_code == 200
    mode_config = load_yaml(capability_root / "modes" / "strategy" / "discrete_stock.yaml")
    binding = mode_config["aiquantbase"]["required_capabilities"][0]
    assert binding["provider_node"] == "stock_daily_real"
    assert binding["field_map"] == {"open": "open_price", "close": "close_price"}

    manifest = load_yaml(capability_root / "manifest.yaml")
    assert manifest["nodes"]["stock_daily_real"]["semantics"]["price.daily"]["fields"] == {
        "open": "open",
        "close": "close",
    }


def test_capabilities_preview_prefers_mode_field_map(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    client = _client(tmp_path)

    mode_response = client.post(
        "/api/capabilities/mode-capability",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "section": "required_capabilities",
            "capability": "price.daily",
            "provider_node": "stock_daily_real",
            "fields": ["open", "close"],
            "field_map": {"open": "open_price", "close": "close_price", "high_limited": "high_limited"},
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
            "fields": ["open", "close", "high_limited"],
        },
    )

    assert preview_response.status_code == 200
    payload = preview_response.get_json()
    daily = payload["resolved_queries"][0]
    assert daily["provider_node"] == "stock_daily_real"
    assert daily["requested_field_map"] == {
        "open": "open_price",
        "close": "close_price",
        "high_limited": "high_limited",
    }
    assert daily["unmapped_fields"] == []


def test_capabilities_registry_capability_can_be_disabled_without_deleting_definition(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    client = _client(tmp_path)

    response = client.post(
        "/api/capabilities/registry-capability",
        json={
            "capability_root": str(capability_root),
            "capability": "custom_factor_daily",
            "enabled": False,
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["result"]["enabled"] is False
    row = next(
        item
        for item in payload["workspace"]["capability_registry"]
        if item["capability"] == "custom_factor_daily"
    )
    assert row["enabled"] is False

    manifest = load_yaml(capability_root / "manifest.yaml")
    registry = manifest["capability_registry"]["custom_factor_daily"]
    assert registry["enabled"] is False
    assert registry["name"] == "自定义日频因子"

    enable_response = client.post(
        "/api/capabilities/registry-capability",
        json={
            "capability_root": str(capability_root),
            "capability": "custom_factor_daily",
            "enabled": True,
        },
    )

    assert enable_response.status_code == 200
    assert enable_response.get_json()["result"]["enabled"] is True
    assert load_yaml(capability_root / "manifest.yaml")["capability_registry"]["custom_factor_daily"]["enabled"] is True


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
            "field_usages": {
                "quality_score": {"usages": ["ranking_fields"]},
            },
        },
    )
    assert mode_response.status_code == 200
    mode_config = load_yaml(mode_path)
    binding = mode_config["aiquantbase"]["extension_capability_bindings"][0]
    assert binding["capability"] == "custom_factor_daily"
    assert binding["slots"] == ["ranking_fields"]
    assert binding["field_usages"] == {"quality_score": {"usages": ["ranking_fields"]}}

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
    assert extension["field_usages"] == {"quality_score": {"usages": ["ranking_fields"]}}


def test_capabilities_preview_resolves_custom_factor_from_dedicated_provider_node(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    client = _client(tmp_path)

    provider_response = client.post(
        "/api/capabilities/provider-node",
        json={
            "capability_root": str(capability_root),
            "node_name": "quality_factor_daily_real",
            "capability": "custom_factor_daily",
            "capability_name": "质量因子",
            "capability_description": "来自独立自定义因子 real 表",
            "asset_types": ["stock"],
            "query_profiles": ["panel_time_series"],
            "keys": {"symbol": "code", "time": "trade_time"},
            "output_scope": {
                "scope_type": "daily_panel",
                "entity_type": "stock",
                "keys": {"entity": "code", "time": "trade_time"},
            },
            "fields": {
                "quality_score": "quality_score",
                "alpha_score": "alpha_score",
            },
            "replace_provider_nodes": True,
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
            "fields": ["quality_score", "alpha_score"],
            "slots": ["ranking_fields", "filter_fields"],
            "field_usages": {
                "quality_score": {"usages": ["ranking_fields"]},
                "alpha_score": {"usages": ["filter_fields"]},
            },
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
            "fields": ["open", "close"],
            "optional_data": [
                {
                    "capability": "custom_factor_daily",
                    "fields": ["quality_score", "alpha_score"],
                    "slots": ["ranking_fields", "filter_fields"],
                }
            ],
        },
    )

    assert preview_response.status_code == 200
    payload = preview_response.get_json()
    assert payload["ok"] is True
    extension = next(
        item for item in payload["resolved_queries"] if item["id"] == "extension_custom_factor_daily"
    )
    assert extension["provider_node"] == "quality_factor_daily_real"
    assert extension["query_profile"] == "panel_time_series"
    assert extension["key_schema"] == {"symbol": "code", "time": "trade_time"}
    assert extension["requested_field_map"] == {
        "quality_score": "quality_score",
        "alpha_score": "alpha_score",
    }
    assert extension["slots"] == ["filter_fields", "ranking_fields"]
    assert extension["field_usages"] == {
        "alpha_score": {"usages": ["filter_fields"]},
        "quality_score": {"usages": ["ranking_fields"]},
    }


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


def test_capabilities_preview_resolves_stock_pool_membership_extension():
    client = _client(Path(__file__).resolve().parents[1])

    preview_response = client.post(
        "/api/capabilities/preview",
        json={
            "mode_id": "strategy_modes.discrete_stock",
            "symbols": ["000002.SZ", "600036.SH"],
            "start": "2024-01-01",
            "end": "2024-01-31",
            "fields": ["open", "close"],
            "optional_data": [
                {
                    "capability": "stock_pool_membership",
                    "fields": ["index_constituent_code", "index_constituent_name"],
                    "slots": ["filter_fields", "report_fields"],
                }
            ],
        },
    )

    assert preview_response.status_code == 200
    payload = preview_response.get_json()
    assert payload["ok"] is True
    extension = next(
        item for item in payload["resolved_queries"] if item["id"] == "extension_stock_pool_membership"
    )
    assert extension["provider_node"] == "index_constituent_real"
    assert extension["query_profile"] == "interval_membership"
    assert extension["key_schema"] == {"symbol": "con_code", "start": "in_date", "end": "out_date"}
    assert extension["output_scope"] == {
        "scope_type": "daily_panel",
        "entity_type": "stock",
        "keys": {"entity": "code", "time": "trade_time"},
    }
    assert extension["requested_field_map"] == {
        "index_constituent_code": "index_code",
        "index_constituent_name": "index_name",
    }
    assert extension["slots"] == ["filter_fields", "report_fields"]


def test_capabilities_preview_uses_graph_interval_keys_for_interval_membership(tmp_path: Path):
    graph_path = tmp_path / "graph.yaml"
    graph_path.write_text(
        dump_yaml(
            {
                "nodes": [
                    {
                        "name": "index_constituent_real",
                        "table": "starlight.ad_index_constituent",
                        "entity_keys": ["member_code"],
                        "time_key": "effective_from",
                        "time_key_mode": "range",
                        "interval_keys": {
                            "start": "effective_from",
                            "end": "effective_to",
                        },
                        "grain": "daily",
                        "fields": [
                            "pool_code",
                            "member_code",
                            "effective_from",
                            "effective_to",
                            "pool_name",
                        ],
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )

    payload = build_capability_preview(
        {
            "mode_id": "strategy_modes.discrete_stock",
            "graph_path": str(graph_path),
            "symbols": ["000002.SZ", "600036.SH"],
            "start": "2024-01-01",
            "end": "2024-01-31",
            "fields": ["open", "close"],
            "optional_data": [
                {
                    "capability": "stock_pool_membership",
                    "fields": ["index_constituent_code"],
                    "slots": ["filter_fields"],
                }
            ],
        }
    )

    extension = next(
        item for item in payload["resolved_queries"] if item["id"] == "extension_stock_pool_membership"
    )
    assert extension["provider_node"] == "index_constituent_real"
    assert extension["query_profile"] == "interval_membership"
    assert extension["key_schema"] == {
        "symbol": "member_code",
        "start": "effective_from",
        "end": "effective_to",
    }


def test_capabilities_mode_extension_contract_resolves_slots_and_scope(tmp_path: Path):
    capability_root = _write_capability_workspace(tmp_path)
    client = _client(tmp_path)

    response = client.post(
        "/api/capabilities/mode-extension-contract",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "capability": "custom_factor_daily",
            "output_scope": {
                "scope_type": "daily_panel",
                "entity_type": "stock",
            },
            "default_slots": ["ranking_fields", "filter_fields"],
            "fields": ["quality_score", "flag"],
            "default_field_usages": {
                "quality_score": {"usages": ["ranking_fields"]},
                "flag": {"usages": ["filter_fields", "weighting_fields"]},
            },
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["mode_accepts_output_scope"] is True
    assert payload["slots"] == ["filter_fields", "ranking_fields"]
    assert payload["field_usages"] == {
        "flag": {"usages": ["filter_fields"]},
        "quality_score": {"usages": ["ranking_fields"]},
    }
    assert [item["slot"] for item in payload["slot_definitions"]] == ["filter_fields", "ranking_fields"]

    rejected_response = client.post(
        "/api/capabilities/mode-extension-contract",
        json={
            "capability_root": str(capability_root),
            "mode_id": "strategy_modes.discrete_stock",
            "capability": "index_factor_daily",
            "output_scope": {
                "scope_type": "daily_panel",
                "entity_type": "index",
            },
            "default_slots": ["ranking_fields"],
        },
    )

    assert rejected_response.status_code == 200
    rejected = rejected_response.get_json()
    assert rejected["ok"] is False
    assert rejected["mode_accepts_output_scope"] is False
    assert rejected["diagnostics"][0]["code"] == "extension_output_scope_not_accepted"


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
