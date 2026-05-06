from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import dump_yaml, load_yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MODE_DATA_ACCESS_PATH = PROJECT_ROOT / "config" / "aiquantbase" / "mode_data_access.yaml"


def load_mode_data_access(path: str | Path | None = None) -> dict[str, Any]:
    resolved = _resolve_path(path)
    if not resolved.exists():
        return _empty_workspace(resolved)
    payload = load_yaml(resolved)
    raw_modes = payload.get("modes") if isinstance(payload.get("modes"), dict) else {}
    return {
        "version": payload.get("version", 1),
        "path": str(resolved),
        "modes": {
            mode_id: _normalize_mode_config({"mode_id": mode_id, **value})
            for mode_id, value in raw_modes.items()
            if isinstance(value, dict)
        },
    }


def upsert_mode_data_access(payload: dict[str, Any]) -> dict[str, Any]:
    resolved = _resolve_path(payload.get("mode_data_access_path"))
    mode_config = payload.get("mode") if isinstance(payload.get("mode"), dict) else payload
    normalized = _normalize_mode_config(mode_config)
    normalized = _ensure_runtime_contract(
        normalized,
        data_assets_path=payload.get("data_assets_path"),
    )
    mode_id = normalized.get("mode_id")
    if not mode_id:
        raise ValueError("mode data access requires mode_id")

    workspace = load_mode_data_access(resolved)
    now = datetime.now(timezone.utc).isoformat()
    existing = workspace["modes"].get(mode_id) or {}
    next_mode = {
        **existing,
        **normalized,
        "created_at": existing.get("created_at") or normalized.get("created_at") or now,
        "updated_at": now,
    }
    next_modes = {
        **workspace["modes"],
        mode_id: next_mode,
    }
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(dump_yaml({"version": 1, "modes": next_modes}), encoding="utf-8")
    return {
        "ok": True,
        "mode": next_mode,
        "workspace": load_mode_data_access(resolved),
    }


def _empty_workspace(path: Path) -> dict[str, Any]:
    return {
        "version": 1,
        "path": str(path),
        "modes": {},
    }


def _resolve_path(path: str | Path | None = None) -> Path:
    if not path:
        return DEFAULT_MODE_DATA_ACCESS_PATH
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _normalize_mode_config(value: dict[str, Any]) -> dict[str, Any]:
    mode_id = str(value.get("mode_id") or "").strip()
    access_groups = value.get("access_groups") if isinstance(value.get("access_groups"), dict) else {}
    asset_policies = value.get("asset_policies") if isinstance(value.get("asset_policies"), dict) else {}
    return {
        "mode_id": mode_id,
        "mode_name": str(value.get("mode_name") or "").strip(),
        "mode_kind": str(value.get("mode_kind") or "").strip(),
        "base_entity": str(value.get("base_entity") or "stock").strip() or "stock",
        "description": str(value.get("description") or "").strip(),
        "enabled": bool(value.get("enabled", True)),
        "mode_settings": _copy_dict(value.get("mode_settings")),
        "access_groups": {
            str(group_key): _normalize_group_policy(group_value)
            for group_key, group_value in access_groups.items()
            if str(group_key or "").strip()
        },
        "asset_policies": {
            str(asset_id): _normalize_asset_policy(asset_value)
            for asset_id, asset_value in asset_policies.items()
            if str(asset_id or "").strip()
        },
        "requirement_bindings": _copy_dict(value.get("requirement_bindings")),
        "runtime_contract": _copy_dict(value.get("runtime_contract")),
        "validation": _copy_dict(value.get("validation")),
        "source_snapshot": _copy_dict(value.get("source_snapshot")),
        "created_at": value.get("created_at"),
        "updated_at": value.get("updated_at"),
    }


def _ensure_runtime_contract(
    mode_config: dict[str, Any],
    *,
    data_assets_path: str | Path | None = None,
) -> dict[str, Any]:
    runtime_contract = _copy_dict(mode_config.get("runtime_contract"))
    if not runtime_contract.get("asset_access"):
        runtime_contract = _compile_runtime_contract(mode_config, data_assets_path=data_assets_path)
    elif not runtime_contract.get("data_sources"):
        runtime_contract["data_sources"] = _group_runtime_assets(runtime_contract.get("asset_access") or [])
    return {
        **mode_config,
        "runtime_contract": runtime_contract,
    }


def _compile_runtime_contract(
    mode_config: dict[str, Any],
    *,
    data_assets_path: str | Path | None = None,
) -> dict[str, Any]:
    from .data_assets import load_data_assets

    workspace = load_data_assets(data_assets_path)
    assets = [
        _compile_runtime_asset(asset, mode_config)
        for asset in list(workspace.get("assets") or [])
        if _asset_enabled_for_mode(asset, mode_config)
    ]
    assets = [asset for asset in assets if asset]
    return {
        "version": 1,
        "mode_id": mode_config.get("mode_id") or "",
        "mode_name": mode_config.get("mode_name") or "",
        "mode_kind": mode_config.get("mode_kind") or "",
        "base_entity": mode_config.get("base_entity") or "stock",
        "enabled": bool(mode_config.get("enabled", True)),
        "settings": _copy_dict(mode_config.get("mode_settings")),
        "asset_access": assets,
        "data_sources": _group_runtime_assets(assets),
        "validation_summary": _copy_dict(mode_config.get("validation")).get("summary", {}),
    }


def _compile_runtime_asset(asset: dict[str, Any], mode_config: dict[str, Any]) -> dict[str, Any] | None:
    asset_id = _asset_id(asset)
    if not asset_id:
        return None
    policy = _asset_policy(asset_id, mode_config)
    access_group = _classify_asset_access_group(asset, mode_config)
    fields = [
        _compile_runtime_field(field, policy)
        for field in list(asset.get("fields") or [])
        if _field_enabled_for_mode(field, policy)
    ]
    fields = [field for field in fields if field]
    return {
        "asset_id": asset_id,
        "capability": asset_id,
        "name": str(asset.get("name") or asset_id).strip(),
        "description": str(asset.get("description") or "").strip(),
        "access_group": access_group,
        "entity_id": str(asset.get("entity_id") or "stock").strip(),
        "target_entity_id": str(asset.get("target_entity_id") or "stock").strip(),
        "access_type": str(asset.get("access_type") or "entity_data").strip(),
        "asset_group": str(asset.get("asset_group") or "extension").strip(),
        "market_grain": str(asset.get("market_grain") or "").strip(),
        "data_shape": str(asset.get("data_shape") or "time_series").strip(),
        "provider_nodes": [str(item).strip() for item in list(asset.get("provider_nodes") or []) if str(item).strip()],
        "provider_bindings": [
            dict(item)
            for item in list(asset.get("provider_bindings") or [])
            if isinstance(item, dict)
        ],
        "query_profiles": [str(item).strip() for item in list(asset.get("query_profiles") or []) if str(item).strip()],
        "output_scope": _copy_dict(asset.get("output_scope")),
        "fields": fields,
    }


def _compile_runtime_field(field: Any, policy: dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(field, str):
        name = field.strip()
        source = name
        label = name
        db_type = ""
    elif isinstance(field, dict):
        name = str(field.get("name") or field.get("source") or field.get("field") or "").strip()
        source = str(field.get("source") or name).strip()
        label = str(field.get("label") or name).strip()
        db_type = str(field.get("db_type") or field.get("database_type") or "").strip()
    else:
        return None
    if not name:
        return None
    field_policy = _field_policy(name, policy)
    return {
        "name": name,
        "source": source or name,
        "label": label or name,
        "database_type": db_type,
        "usage": str(field_policy.get("usage") or "auto").strip() or "auto",
        "alias": str(field_policy.get("alias") or "").strip(),
        "notes": str(field_policy.get("notes") or "").strip(),
    }


def _group_runtime_assets(assets: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    return {
        "panels": [dict(asset) for asset in assets if asset.get("data_shape") == "time_series"],
        "events": [dict(asset) for asset in assets if asset.get("data_shape") == "event"],
        "relations": [
            dict(asset)
            for asset in assets
            if asset.get("access_type") == "relation_data" or asset.get("data_shape") == "relation"
        ],
        "references": [dict(asset) for asset in assets if asset.get("access_group") == "reference_entity_data"],
    }


def _asset_enabled_for_mode(asset: dict[str, Any], mode_config: dict[str, Any]) -> bool:
    if not isinstance(asset, dict) or not asset.get("enabled", True):
        return False
    asset_id = _asset_id(asset)
    if not asset_id:
        return False
    policy = _asset_policy(asset_id, mode_config)
    if policy and not bool(policy.get("enabled", True)):
        return False
    policies = mode_config.get("asset_policies") if isinstance(mode_config.get("asset_policies"), dict) else {}
    if policies and asset_id not in policies:
        return False
    access_group = _classify_asset_access_group(asset, mode_config)
    group_policy = (mode_config.get("access_groups") or {}).get(access_group)
    if isinstance(group_policy, dict) and not bool(group_policy.get("enabled", True)):
        return False
    return True


def _field_enabled_for_mode(field: Any, policy: dict[str, Any]) -> bool:
    name = ""
    if isinstance(field, str):
        name = field.strip()
    elif isinstance(field, dict):
        name = str(field.get("name") or field.get("source") or field.get("field") or "").strip()
    if not name:
        return False
    return bool(_field_policy(name, policy).get("enabled", True))


def _asset_policy(asset_id: str, mode_config: dict[str, Any]) -> dict[str, Any]:
    policies = mode_config.get("asset_policies") if isinstance(mode_config.get("asset_policies"), dict) else {}
    policy = policies.get(asset_id)
    return policy if isinstance(policy, dict) else {}


def _field_policy(field_name: str, asset_policy: dict[str, Any]) -> dict[str, Any]:
    fields = asset_policy.get("fields") if isinstance(asset_policy.get("fields"), dict) else {}
    policy = fields.get(field_name)
    return policy if isinstance(policy, dict) else {"enabled": True, "usage": "auto"}


def _classify_asset_access_group(asset: dict[str, Any], mode_config: dict[str, Any]) -> str:
    base_entity = str(mode_config.get("base_entity") or "stock").strip() or "stock"
    entity_id = str(asset.get("entity_id") or base_entity).strip()
    target_entity_id = str(asset.get("target_entity_id") or entity_id or base_entity).strip()
    access_type = str(asset.get("access_type") or "entity_data").strip()
    asset_group = str(asset.get("asset_group") or "").strip()
    if access_type == "relation_data" and target_entity_id == base_entity:
        return "entity_relation"
    if entity_id == base_entity and target_entity_id == base_entity:
        return "base_market_data" if asset_group == "market" else "base_extension_data"
    if target_entity_id == base_entity and entity_id != base_entity:
        return "bound_entity_data"
    if entity_id != base_entity and target_entity_id == entity_id:
        return "reference_entity_data"
    return "base_extension_data"


def _asset_id(asset: dict[str, Any]) -> str:
    return str(asset.get("asset_id") or asset.get("capability") or "").strip()


def _normalize_group_policy(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"enabled": value}
    if not isinstance(value, dict):
        return {"enabled": True}
    return {
        "enabled": bool(value.get("enabled", True)),
        "notes": str(value.get("notes") or "").strip(),
    }


def _normalize_asset_policy(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"enabled": value, "fields": {}}
    if not isinstance(value, dict):
        return {"enabled": True, "fields": {}}
    raw_fields = value.get("fields") if isinstance(value.get("fields"), dict) else {}
    return {
        "enabled": bool(value.get("enabled", True)),
        "notes": str(value.get("notes") or "").strip(),
        "fields": {
            str(field_name): _normalize_field_policy(field_value)
            for field_name, field_value in raw_fields.items()
            if str(field_name or "").strip()
        },
    }


def _normalize_field_policy(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"enabled": value, "usage": "auto"}
    if not isinstance(value, dict):
        return {"enabled": True, "usage": "auto"}
    usage = str(value.get("usage") or "auto").strip() or "auto"
    return {
        "enabled": bool(value.get("enabled", True)),
        "usage": usage,
        "alias": str(value.get("alias") or "").strip(),
        "notes": str(value.get("notes") or "").strip(),
    }


def _copy_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}
