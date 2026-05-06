from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import dump_yaml, load_yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_ASSETS_PATH = PROJECT_ROOT / "config" / "aiquantbase" / "data_assets.yaml"


def load_data_assets(path: str | Path | None = None) -> dict[str, Any]:
    resolved = _resolve_path(path)
    if not resolved.exists():
        return _empty_workspace(resolved)
    payload = load_yaml(resolved)
    assets = payload.get("assets", [])
    if not isinstance(assets, list):
        assets = []
    return {
        "version": payload.get("version", 1),
        "path": str(resolved),
        "assets": [_normalize_asset(item) for item in assets if isinstance(item, dict)],
    }


def upsert_data_asset(payload: dict[str, Any]) -> dict[str, Any]:
    resolved = _resolve_path(payload.get("data_assets_path"))
    asset = payload.get("asset") if isinstance(payload.get("asset"), dict) else payload
    normalized = _normalize_asset(asset)
    asset_id = normalized.get("capability") or normalized.get("asset_id")
    replace_asset_id = str(payload.get("replace_asset_id") or "").strip()
    if not asset_id:
        raise ValueError("数据资产缺少 capability/asset_id")
    if not normalized.get("provider_nodes"):
        raise ValueError("数据资产缺少 provider_nodes")

    workspace = load_data_assets(resolved)
    now = datetime.now(timezone.utc).isoformat()
    existing_assets = workspace["assets"]
    if replace_asset_id and replace_asset_id != asset_id:
        if any((item.get("capability") or item.get("asset_id")) == asset_id for item in existing_assets):
            raise ValueError(f"数据资产已存在：{asset_id}")
    next_assets: list[dict[str, Any]] = []
    replaced = False
    for item in existing_assets:
        current_id = item.get("capability") or item.get("asset_id")
        if current_id == asset_id or (replace_asset_id and current_id == replace_asset_id):
            next_assets.append({
                **item,
                **normalized,
                "created_at": item.get("created_at") or normalized.get("created_at") or now,
                "updated_at": now,
            })
            replaced = True
        else:
            next_assets.append(item)
    if not replaced:
        next_assets.append({
            **normalized,
            "created_at": normalized.get("created_at") or now,
            "updated_at": now,
        })

    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(dump_yaml({"version": 1, "assets": next_assets}), encoding="utf-8")
    saved_asset = next(
        item for item in next_assets
        if (item.get("capability") or item.get("asset_id")) == asset_id
    )
    return {
        "ok": True,
        "asset": saved_asset,
        "workspace": load_data_assets(resolved),
    }


def delete_data_asset(payload: dict[str, Any]) -> dict[str, Any]:
    resolved = _resolve_path(payload.get("data_assets_path"))
    asset_id = str(payload.get("capability") or payload.get("asset_id") or "").strip()
    if not asset_id:
        raise ValueError("缺少要删除的数据资产 ID")
    workspace = load_data_assets(resolved)
    next_assets = [
        item for item in workspace["assets"]
        if (item.get("capability") or item.get("asset_id")) != asset_id
    ]
    if len(next_assets) == len(workspace["assets"]):
        raise ValueError(f"数据资产不存在：{asset_id}")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(dump_yaml({"version": 1, "assets": next_assets}), encoding="utf-8")
    return {
        "ok": True,
        "deleted": asset_id,
        "workspace": load_data_assets(resolved),
    }


def _empty_workspace(path: Path) -> dict[str, Any]:
    return {
        "version": 1,
        "path": str(path),
        "assets": [],
    }


def _resolve_path(path: str | Path | None = None) -> Path:
    if not path:
        return DEFAULT_DATA_ASSETS_PATH
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _normalize_asset(asset: dict[str, Any]) -> dict[str, Any]:
    fields = asset.get("fields") if isinstance(asset.get("fields"), list) else []
    provider_nodes = asset.get("provider_nodes") if isinstance(asset.get("provider_nodes"), list) else []
    provider_bindings = asset.get("provider_bindings") if isinstance(asset.get("provider_bindings"), list) else []
    return {
        "capability": str(asset.get("capability") or asset.get("asset_id") or "").strip(),
        "name": str(asset.get("name") or "").strip(),
        "description": str(asset.get("description") or "").strip(),
        "enabled": bool(asset.get("enabled", True)),
        "entity_id": str(asset.get("entity_id") or "stock").strip(),
        "target_entity_id": str(asset.get("target_entity_id") or "stock").strip(),
        "access_type": str(asset.get("access_type") or "entity_data").strip(),
        "asset_group": str(asset.get("asset_group") or "extension").strip(),
        "market_grain": str(asset.get("market_grain") or "").strip(),
        "data_shape": str(asset.get("data_shape") or "time_series").strip(),
        "fields": [_normalize_field(item) for item in fields if item],
        "field_count": int(asset.get("field_count") or len(fields)),
        "provider_nodes": [str(item).strip() for item in provider_nodes if str(item).strip()],
        "provider_bindings": provider_bindings,
        "query_profiles": list(asset.get("query_profiles") or []),
        "output_scope": dict(asset.get("output_scope") or {}),
        "binding_summary": str(asset.get("binding_summary") or "").strip(),
        "draft": bool(asset.get("draft", False)),
        "created_at": asset.get("created_at"),
        "updated_at": asset.get("updated_at"),
    }


def _normalize_field(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return {"name": value, "source": value, "label": value}
    if not isinstance(value, dict):
        text = str(value)
        return {"name": text, "source": text, "label": text}
    name = str(value.get("name") or value.get("source") or value.get("field") or "").strip()
    return {
        "name": name,
        "source": value.get("source") or name,
        "label": value.get("label") or name,
    }
