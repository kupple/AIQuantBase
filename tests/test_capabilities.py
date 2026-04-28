from __future__ import annotations

from pathlib import Path

from aiquantbase.config import dump_yaml, load_yaml
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
                        "default_slots": ["ranking_fields", "filter_fields"],
                    }
                },
                "nodes": {
                    "stock_daily_real": {
                        "description": "daily panel",
                        "asset_types": ["stock"],
                        "access_patterns": ["panel_time_series"],
                        "methods": ["query_daily"],
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
                        "access_pattern": "panel_time_series",
                        "method": "query_daily",
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
                    "required_capabilities": [
                        {"capability": "price.daily", "fields": ["open", "close"]},
                    ],
                    "conditional_capabilities": [],
                    "optional_capabilities": [],
                    "extension_slots": [
                        {
                            "slot": "ranking_fields",
                            "name": "排序字段",
                            "access_pattern": "panel_time_series",
                            "freq": "1d",
                        },
                        {
                            "slot": "filter_fields",
                            "name": "过滤字段",
                            "access_pattern": "panel_time_series",
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
    assert [item["slot"] for item in payload["mode_profiles"][0]["extension_slots"]] == [
        "ranking_fields",
        "filter_fields",
    ]
    assert payload["capability_registry"][0]["capability"] == "custom_factor_daily"
    assert payload["capability_registry"][0]["name"] == "自定义日频因子"
    assert payload["capabilities"][0]["capability"] == "price.daily"


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
    assert extension["requested_field_map"] == {"quality_score": "quality_score"}
    assert extension["slots"] == ["ranking_fields"]


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
