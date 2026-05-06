from __future__ import annotations

from pathlib import Path

from aiquantbase.config import load_yaml
from aiquantbase.server import create_app


def test_data_assets_save_and_load(tmp_path: Path) -> None:
    app = create_app(
        default_graph_path=tmp_path / "graph.yaml",
        default_fields_path=tmp_path / "fields.yaml",
    )
    client = app.test_client()
    data_assets_path = tmp_path / "data_assets.yaml"

    response = client.get("/api/data-assets", query_string={"data_assets_path": str(data_assets_path)})
    assert response.status_code == 200
    assert response.get_json()["assets"] == []

    asset = {
        "capability": "asset_industry_daily_stock_industry_daily_real",
        "name": "行业日线数据",
        "entity_id": "industry",
        "target_entity_id": "stock",
        "access_type": "entity_data",
        "asset_group": "market",
        "market_grain": "daily",
        "data_shape": "time_series",
        "fields": [{"name": "close", "source": "close", "label": "close"}],
        "provider_nodes": ["stock_industry_daily_real"],
        "provider_bindings": [{"provider_node": "stock_industry_daily_real"}],
    }
    response = client.post(
        "/api/data-assets",
        json={"data_assets_path": str(data_assets_path), "asset": asset},
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["workspace"]["assets"][0]["entity_id"] == "industry"

    saved = load_yaml(data_assets_path)
    assert saved["assets"][0]["provider_nodes"] == ["stock_industry_daily_real"]

    response = client.get("/api/data-assets", query_string={"data_assets_path": str(data_assets_path)})
    assert response.status_code == 200
    assert response.get_json()["assets"][0]["capability"] == asset["capability"]

    updated_asset = {
        **asset,
        "capability": "asset_industry_stock_relation_industry_constituent_real",
        "asset_group": "extension",
        "access_type": "relation_data",
        "data_shape": "relation",
        "provider_nodes": ["industry_constituent_real"],
    }
    response = client.post(
        "/api/data-assets",
        json={
            "data_assets_path": str(data_assets_path),
            "replace_asset_id": asset["capability"],
            "asset": updated_asset,
        },
    )
    assert response.status_code == 200
    assets = response.get_json()["workspace"]["assets"]
    assert [item["capability"] for item in assets] == [updated_asset["capability"]]

    response = client.post(
        "/api/data-assets/delete",
        json={"data_assets_path": str(data_assets_path), "capability": updated_asset["capability"]},
    )
    assert response.status_code == 200
    assert response.get_json()["workspace"]["assets"] == []


def test_mode_data_access_save_and_load(tmp_path: Path) -> None:
    app = create_app(
        default_graph_path=tmp_path / "graph.yaml",
        default_fields_path=tmp_path / "fields.yaml",
    )
    client = app.test_client()
    mode_data_access_path = tmp_path / "mode_data_access.yaml"

    response = client.get(
        "/api/mode-data-access",
        query_string={"mode_data_access_path": str(mode_data_access_path)},
    )
    assert response.status_code == 200
    assert response.get_json()["modes"] == {}

    mode = {
        "mode_id": "strategy_modes.discrete_stock_v3",
        "mode_name": "discrete_stock_v3",
        "mode_kind": "strategy_modes",
        "base_entity": "stock",
        "access_groups": {
            "base_market_data": {"enabled": True},
            "reference_entity_data": {"enabled": False},
        },
        "mode_settings": {
            "calendar": {"source": "real_node", "node": "trade_calendar_real"},
            "benchmark": {"symbol": "000300.SH", "name": "沪深300"},
            "costs": {"buy_commission_rate": 0.0003, "min_commission": 5},
        },
        "asset_policies": {
            "asset_stock_daily_stock_daily_real": {
                "enabled": True,
                "fields": {
                    "close": {"enabled": True, "usage": "ranking"},
                    "amount": {"enabled": False, "usage": "auto"},
                },
            }
        },
        "requirement_bindings": {
            "daily_market": {
                "close": {
                    "field": "close",
                    "label": "收盘价",
                    "status": "ready",
                    "real_field": "stock_daily_real.close",
                }
            }
        },
        "runtime_contract": {
            "version": 1,
            "mode_id": "strategy_modes.discrete_stock_v3",
        },
        "validation": {"ok": True},
    }
    response = client.post(
        "/api/mode-data-access",
        json={"mode_data_access_path": str(mode_data_access_path), "mode": mode},
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    saved_mode = payload["workspace"]["modes"]["strategy_modes.discrete_stock_v3"]
    assert saved_mode["base_entity"] == "stock"
    assert saved_mode["mode_settings"]["benchmark"]["symbol"] == "000300.SH"
    assert saved_mode["mode_settings"]["costs"]["min_commission"] == 5
    assert saved_mode["access_groups"]["reference_entity_data"]["enabled"] is False
    assert saved_mode["asset_policies"]["asset_stock_daily_stock_daily_real"]["fields"]["amount"]["enabled"] is False
    assert saved_mode["requirement_bindings"]["daily_market"]["close"]["real_field"] == "stock_daily_real.close"
    assert saved_mode["runtime_contract"]["mode_id"] == "strategy_modes.discrete_stock_v3"
    assert saved_mode["validation"]["ok"] is True

    saved = load_yaml(mode_data_access_path)
    assert "strategy_modes.discrete_stock_v3" in saved["modes"]
