from __future__ import annotations

from copy import deepcopy
import os
from pathlib import Path
from typing import Any

from .config import dump_yaml, load_yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CAPABILITY_ROOT = PROJECT_ROOT / "config" / "aiquantbase"
DEFAULT_GRAPH_PATH = PROJECT_ROOT / "config" / "graph.yaml"


def default_alphablocks_root() -> Path:
    env_value = os.environ.get("ALPHABLOCKS_ROOT")
    if env_value:
        return Path(env_value).expanduser()
    sibling = PROJECT_ROOT.parent / "AlphaBlocks"
    if sibling.exists():
        return sibling
    desktop_default = Path("/Users/zhao/Desktop/git/AlphaBlocks")
    if desktop_default.exists():
        return desktop_default
    return sibling


def default_capability_root() -> Path:
    env_value = os.environ.get("AIQUANTBASE_CAPABILITY_ROOT")
    if env_value:
        return Path(env_value).expanduser()
    return DEFAULT_CAPABILITY_ROOT


def load_capability_workspace(
    *,
    capability_root: str | Path | None = None,
    alphablocks_root: str | Path | None = None,
    provider_manifest_path: str | Path | None = None,
    mode_registry_path: str | Path | None = None,
    query_templates_path: str | Path | None = None,
    graph_path: str | Path | None = None,
) -> dict[str, Any]:
    root = _resolve_capability_root(capability_root=capability_root, alphablocks_root=alphablocks_root)
    manifest_path = _resolve_capability_path(
        provider_manifest_path or "manifest.yaml",
        root=root,
    )
    registry_path = _resolve_capability_path(
        mode_registry_path or "mode_capabilities.yaml",
        root=root,
    )
    templates_path = _resolve_capability_path(
        query_templates_path or "query_templates.yaml",
        root=root,
    )

    diagnostics: list[dict[str, Any]] = []
    manifest = _safe_load_yaml(manifest_path, diagnostics=diagnostics, label="provider_manifest")
    mode_registry = _safe_load_yaml(registry_path, diagnostics=diagnostics, label="mode_registry")
    query_templates = _safe_load_yaml(templates_path, diagnostics=diagnostics, label="query_templates")
    graph_catalog = _load_graph_catalog(graph_path)
    mode_profiles = _load_mode_profiles(mode_registry, root=root, diagnostics=diagnostics)

    return {
        "ok": not any(item.get("severity") == "error" for item in diagnostics),
        "workspace": {
            "capability_root": str(root),
            "provider_manifest_path": str(manifest_path),
            "mode_registry_path": str(registry_path),
            "query_templates_path": str(templates_path),
        },
        "provider_manifest": manifest,
        "provider_nodes": _provider_node_rows(manifest, graph_catalog=graph_catalog),
        "capability_registry": _capability_registry_rows(manifest),
        "capabilities": _capability_rows(manifest),
        "mode_registry": mode_registry,
        "mode_profiles": mode_profiles,
        "query_templates": _query_template_rows(query_templates),
        "diagnostics": diagnostics,
    }


def upsert_provider_node_semantic(
    payload: dict[str, Any],
    *,
    provider_manifest_path: str | Path | None = None,
) -> dict[str, Any]:
    root = _resolve_capability_root(
        capability_root=payload.get("capability_root"),
        alphablocks_root=payload.get("alphablocks_root"),
    )
    manifest_path = _resolve_capability_path(
        provider_manifest_path or payload.get("provider_manifest_path") or "manifest.yaml",
        root=root,
    )
    manifest = load_yaml(manifest_path)
    nodes = manifest.setdefault("nodes", {})
    if not isinstance(nodes, dict):
        raise ValueError("provider manifest nodes must be a mapping")

    node_name = _required_text(payload, "node_name")
    capability = _required_text(payload, "capability")
    fields = _normalize_field_mapping(payload.get("fields") or payload.get("field_map") or {})
    capability_name = str(payload.get("capability_name") or payload.get("name") or "").strip()
    capability_description = str(payload.get("capability_description") or "").strip()
    default_slots = _string_list(payload.get("default_slots") or payload.get("allowed_slots") or payload.get("slots"))

    node = nodes.setdefault(node_name, {})
    if not isinstance(node, dict):
        raise ValueError(f"provider node must be a mapping: {node_name}")
    node.setdefault("description", payload.get("description") or f"{node_name} provider node")
    node.setdefault("asset_types", _string_list(payload.get("asset_types")) or ["stock"])
    node.setdefault("access_patterns", _string_list(payload.get("access_patterns")) or ["panel_time_series"])
    node.setdefault("methods", _string_list(payload.get("methods")) or ["query_daily"])
    node.setdefault("keys", payload.get("keys") or {"symbol": "code", "time": "trade_time"})
    semantics = node.setdefault("semantics", {})
    if not isinstance(semantics, dict):
        raise ValueError(f"provider node semantics must be a mapping: {node_name}")

    semantic = semantics.setdefault(capability, {})
    if not isinstance(semantic, dict):
        semantic = {}
        semantics[capability] = semantic
    if payload.get("capability_description"):
        semantic["description"] = str(payload.get("capability_description"))
    semantic["fields"] = fields
    _upsert_capability_registry_entry(
        manifest,
        capability=capability,
        name=capability_name,
        description=capability_description,
        default_slots=default_slots,
    )

    _write_yaml(manifest_path, manifest)
    return {
        "ok": True,
        "provider_manifest_path": str(manifest_path),
        "node_name": node_name,
        "capability": capability,
        "capability_name": capability_name,
        "capability_description": capability_description,
        "default_slots": default_slots,
        "fields": fields,
    }


def upsert_mode_capability(
    payload: dict[str, Any],
    *,
    mode_file_path: str | Path | None = None,
) -> dict[str, Any]:
    root = _resolve_capability_root(
        capability_root=payload.get("capability_root"),
        alphablocks_root=payload.get("alphablocks_root"),
    )
    registry_path = _resolve_capability_path(
        payload.get("mode_registry_path") or "mode_capabilities.yaml",
        root=root,
    )
    registry = _safe_load_yaml(registry_path, diagnostics=[], label="mode_registry")
    config_path = _resolve_mode_config_path(
        root=root,
        mode_registry=registry,
        mode_file_path=mode_file_path or payload.get("mode_file_path"),
        mode_id=payload.get("mode_id"),
        mode_kind=payload.get("mode_kind"),
        mode_name=payload.get("mode_name"),
    )
    mode_config = load_yaml(config_path)
    aiqb = mode_config.setdefault("aiquantbase", {})
    if not isinstance(aiqb, dict):
        raise ValueError("mode aiquantbase section must be a mapping")

    capability = _required_text(payload, "capability")
    section = str(payload.get("section") or "optional_capabilities").strip()
    if section not in {"required_capabilities", "optional_capabilities", "extension_capabilities"}:
        raise ValueError("section must be required_capabilities, optional_capabilities or extension_capabilities")
    rows = aiqb.setdefault(section, [])
    if not isinstance(rows, list):
        raise ValueError(f"mode aiquantbase.{section} must be a list")

    fields = _string_list(payload.get("fields"))
    slots = _string_list(payload.get("allowed_slots") or payload.get("slots"))
    next_item = {
        "capability": capability,
        "fields": fields,
    }
    if slots:
        if section == "extension_capabilities":
            next_item["allowed_slots"] = slots
        else:
            next_item["slots"] = slots
    for key in ("access_pattern", "method", "query_template", "description"):
        value = payload.get(key)
        if value not in (None, ""):
            next_item[key] = value

    for index, item in enumerate(rows):
        if isinstance(item, dict) and str(item.get("capability") or "").strip() == capability:
            merged = deepcopy(item)
            merged.update(next_item)
            if section == "extension_capabilities":
                merged.pop("slots", None)
            else:
                merged.pop("allowed_slots", None)
            rows[index] = merged
            break
    else:
        rows.append(next_item)

    _write_yaml(config_path, mode_config)
    return {
        "ok": True,
        "mode_file_path": str(config_path),
        "section": section,
        "capability": capability,
        "fields": fields,
        "allowed_slots": slots if section == "extension_capabilities" else [],
        "slots": [] if section == "extension_capabilities" else slots,
    }


def build_capability_preview(payload: dict[str, Any]) -> dict[str, Any]:
    root = _resolve_capability_root(
        capability_root=payload.get("capability_root"),
        alphablocks_root=payload.get("alphablocks_root"),
    )
    manifest_path = _resolve_capability_path(
        payload.get("provider_manifest_path") or "manifest.yaml",
        root=root,
    )
    registry_path = _resolve_capability_path(
        payload.get("mode_registry_path") or "mode_capabilities.yaml",
        root=root,
    )
    templates_path = _resolve_capability_path(
        payload.get("query_templates_path") or "query_templates.yaml",
        root=root,
    )
    registry = load_yaml(registry_path)
    mode_path = _resolve_mode_config_path(
        root=root,
        mode_registry=registry,
        mode_file_path=payload.get("mode_file_path"),
        mode_id=payload.get("mode_id"),
        mode_kind=payload.get("mode_kind"),
        mode_name=payload.get("mode_name"),
    )

    manifest = load_yaml(manifest_path)
    templates_root = load_yaml(templates_path)
    mode_config = load_yaml(mode_path)
    context = _preview_context(payload)
    resolved_queries: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []

    templates = templates_root.get("query_templates") or {}
    for query_need in list(mode_config.get("query_needs") or []):
        if not isinstance(query_need, dict) or not _query_need_enabled(query_need, context):
            continue
        use_name = str(query_need.get("use") or "").strip()
        template = templates.get(use_name)
        if not isinstance(template, dict):
            diagnostics.append(_diagnostic("error", "missing_query_template", f"query template not found: {use_name}"))
            continue
        params = deepcopy(template.get("params_template") or {})
        _apply_derivations(params, query_need.get("derive") or {}, context)
        _finalize_params(params)
        binding = _find_provider_binding(
            manifest=manifest,
            capability=str(template.get("capability") or ""),
            method=str(template.get("method") or ""),
            access_pattern=str(template.get("access_pattern") or ""),
            asset_type=str(params.get("asset_type") or "stock"),
        )
        if binding is None:
            diagnostics.append(
                _diagnostic(
                    "error",
                    "missing_provider_binding",
                    f"provider binding not found for template {use_name}",
                    use=use_name,
                    capability=template.get("capability"),
                )
            )
            continue
        requested_field_map, unmapped_fields = _select_requested_field_map(
            binding.get("node_field_map") or binding.get("field_map") or {},
            params.get("fields") or [],
        )
        resolved_queries.append(
            {
                "id": query_need.get("id"),
                "use": use_name,
                "required": bool(query_need.get("required")),
                "capability": template.get("capability"),
                "access_pattern": template.get("access_pattern"),
                "method": template.get("method"),
                "provider_node": binding.get("node"),
                "params": params,
                "key_schema": binding.get("key_schema") or {},
                "requested_field_map": requested_field_map,
                "unmapped_fields": unmapped_fields,
            }
        )

    resolved_queries.extend(
        _build_extension_queries(
            manifest=manifest,
            mode_config=mode_config,
            context=context,
            diagnostics=diagnostics,
        )
    )

    return {
        "ok": not any(item.get("severity") == "error" for item in diagnostics),
        "version": 1,
        "mode_kind": mode_config.get("mode_kind"),
        "mode_name": mode_config.get("mode_name"),
        "workspace": {
            "capability_root": str(root),
            "provider_manifest_path": str(manifest_path),
            "mode_registry_path": str(registry_path),
            "query_templates_path": str(templates_path),
            "mode_file_path": str(mode_path),
        },
        "context": context,
        "resolved_queries": resolved_queries,
        "diagnostics": diagnostics,
    }


def _load_mode_profiles(
    mode_registry: dict[str, Any],
    *,
    root: Path,
    diagnostics: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    for path in _mode_config_paths(mode_registry, root=root):
        if not path.exists():
            diagnostics.append(_diagnostic("warning", "missing_mode_config", f"mode config not found: {path}"))
            continue
        try:
            payload = load_yaml(path)
        except Exception as exc:
            diagnostics.append(_diagnostic("error", "invalid_mode_config", str(exc), path=str(path)))
            continue
        aiqb = payload.get("aiquantbase") or {}
        profiles.append(
            {
                "mode_id": _mode_id(payload),
                "mode_kind": payload.get("mode_kind"),
                "mode_name": payload.get("mode_name"),
                "description": payload.get("description"),
                "config_path": str(path),
                "required_capabilities": list(aiqb.get("required_capabilities") or []),
                "optional_capabilities": list(aiqb.get("optional_capabilities") or []),
                "extension_capabilities": list(aiqb.get("extension_capabilities") or []),
                "query_needs": list(payload.get("query_needs") or []),
            }
        )
    return profiles


def _mode_config_paths(mode_registry: dict[str, Any], *, root: Path) -> list[Path]:
    values = mode_registry.get("mode_config_files") or mode_registry.get("mode_files") or []
    if isinstance(values, list) and values:
        return [_resolve_capability_path(value, root=root) for value in values if str(value or "").strip()]
    return sorted(root.glob("modes/**/*.yaml"))


def _resolve_mode_config_path(
    *,
    root: Path,
    mode_registry: dict[str, Any],
    mode_file_path: str | Path | None = None,
    mode_id: Any = None,
    mode_kind: Any = None,
    mode_name: Any = None,
) -> Path:
    if mode_file_path not in (None, ""):
        return _resolve_capability_path(mode_file_path, root=root)

    expected_id = str(mode_id or "").strip()
    expected_kind = str(mode_kind or "").strip()
    expected_name = str(mode_name or "").strip()
    if not expected_id and expected_kind and expected_name:
        expected_id = f"{expected_kind}.{expected_name}"
    if not expected_id:
        raise ValueError("mode_id is required")

    diagnostics: list[dict[str, Any]] = []
    for path in _mode_config_paths(mode_registry, root=root):
        if not path.exists():
            continue
        payload = _safe_load_yaml(path, diagnostics=diagnostics, label="mode_config")
        if _mode_id(payload) == expected_id:
            return path
    raise ValueError(f"mode config not found: {expected_id}")


def _mode_id(payload: dict[str, Any]) -> str:
    mode_kind = str(payload.get("mode_kind") or "").strip()
    mode_name = str(payload.get("mode_name") or "").strip()
    return f"{mode_kind}.{mode_name}" if mode_kind and mode_name else mode_name or mode_kind


def _load_graph_catalog(path_like: str | Path | None = None) -> dict[str, dict[str, Any]]:
    path = _resolve_path(path_like or DEFAULT_GRAPH_PATH, base=PROJECT_ROOT)
    if not path.exists():
        return {}
    try:
        payload = load_yaml(path)
    except Exception:
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for item in list(payload.get("nodes") or []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        wide_table = item.get("wide_table") if isinstance(item.get("wide_table"), dict) else {}
        fields = _string_list(wide_table.get("fields") or item.get("fields"))
        rows[name] = {
            "name": name,
            "description": item.get("description_zh") or item.get("description"),
            "table": wide_table.get("target_table") or item.get("table"),
            "database": wide_table.get("target_database"),
            "grain": item.get("grain"),
            "asset_type": item.get("asset_type"),
            "fields": fields,
            "entity_keys": _string_list(item.get("entity_keys")),
            "time_key": item.get("time_key"),
        }
    return rows


def _provider_node_rows(manifest: dict[str, Any], *, graph_catalog: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    graph_catalog = graph_catalog or {}
    nodes = manifest.get("nodes") or {}
    if not isinstance(nodes, dict):
        nodes = {}
    for name, node in nodes.items():
        if not isinstance(node, dict):
            continue
        graph_node = graph_catalog.get(str(name), {})
        rows.append(
            {
                "name": name,
                "description": graph_node.get("description") or node.get("description"),
                "asset_types": list(node.get("asset_types") or []),
                "access_patterns": list(node.get("access_patterns") or []),
                "methods": list(node.get("methods") or []),
                "keys": deepcopy(node.get("keys") or {}),
                "table": graph_node.get("table"),
                "database": graph_node.get("database"),
                "grain": graph_node.get("grain"),
                "source_fields": list(graph_node.get("fields") or []),
                "capabilities": sorted((node.get("semantics") or {}).keys()),
                "registered": True,
            }
        )
    for name, graph_node in sorted(graph_catalog.items()):
        if name in nodes:
            continue
        grain = str(graph_node.get("grain") or "").strip()
        access_pattern = "anchored_intraday_window" if grain == "minute" else "panel_time_series"
        method = "query_minute_window_by_trading_day" if grain == "minute" else "query_daily"
        rows.append(
            {
                "name": name,
                "description": graph_node.get("description"),
                "asset_types": [graph_node.get("asset_type") or "stock"],
                "access_patterns": [access_pattern],
                "methods": [method],
                "keys": _keys_from_graph_node(graph_node),
                "table": graph_node.get("table"),
                "database": graph_node.get("database"),
                "grain": graph_node.get("grain"),
                "source_fields": list(graph_node.get("fields") or []),
                "capabilities": [],
                "registered": False,
            }
        )
    return rows


def _keys_from_graph_node(graph_node: dict[str, Any]) -> dict[str, str]:
    entity_keys = _string_list(graph_node.get("entity_keys"))
    time_key = str(graph_node.get("time_key") or "").strip()
    keys: dict[str, str] = {}
    if entity_keys:
        keys["symbol"] = entity_keys[0]
    if time_key:
        keys["time"] = time_key
    return keys or {"symbol": "code", "time": "trade_time"}


def _capability_registry_map(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw = manifest.get("capability_registry") or {}
    if not isinstance(raw, dict):
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for capability, item in raw.items():
        capability_id = str(capability or "").strip()
        if not capability_id or not isinstance(item, dict):
            continue
        rows[capability_id] = {
            "capability": capability_id,
            "name": str(item.get("name") or item.get("display_name") or capability_id).strip(),
            "description": str(item.get("description") or "").strip(),
            "default_slots": _string_list(item.get("default_slots") or item.get("allowed_slots") or item.get("slots")),
        }
    return rows


def _capability_registry_rows(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    return sorted(_capability_registry_map(manifest).values(), key=lambda item: str(item["capability"]))


def _upsert_capability_registry_entry(
    manifest: dict[str, Any],
    *,
    capability: str,
    name: str = "",
    description: str = "",
    default_slots: list[str] | None = None,
) -> None:
    if not name and not description and not default_slots:
        return
    registry = manifest.setdefault("capability_registry", {})
    if not isinstance(registry, dict):
        raise ValueError("provider manifest capability_registry must be a mapping")
    row = registry.setdefault(capability, {})
    if not isinstance(row, dict):
        row = {}
        registry[capability] = row
    if name:
        row["name"] = name
    if description:
        row["description"] = description
    slots = _string_list(default_slots)
    if slots:
        row["default_slots"] = slots


def _capability_rows(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    nodes = manifest.get("nodes") or {}
    if not isinstance(nodes, dict):
        return rows
    registry = _capability_registry_map(manifest)
    for node_name, node in nodes.items():
        if not isinstance(node, dict):
            continue
        semantics = node.get("semantics") or {}
        if not isinstance(semantics, dict):
            continue
        for capability, semantic in semantics.items():
            fields = (semantic.get("fields") if isinstance(semantic, dict) else {}) or {}
            registry_item = registry.get(str(capability), {})
            rows.append(
                {
                    "capability": capability,
                    "name": registry_item.get("name") or capability,
                    "provider_node": node_name,
                    "description": (semantic.get("description") if isinstance(semantic, dict) else None)
                    or registry_item.get("description")
                    or node.get("description"),
                    "default_slots": list(registry_item.get("default_slots") or []),
                    "asset_types": list(node.get("asset_types") or []),
                    "access_patterns": list(node.get("access_patterns") or []),
                    "methods": list(node.get("methods") or []),
                    "field_count": len(fields) if isinstance(fields, dict) else 0,
                    "fields": deepcopy(fields) if isinstance(fields, dict) else {},
                }
            )
    return sorted(rows, key=lambda item: (str(item["capability"]), str(item["provider_node"])))


def _query_template_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    templates = payload.get("query_templates") or {}
    if not isinstance(templates, dict):
        return rows
    for name, item in templates.items():
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "name": name,
                "capability": item.get("capability"),
                "access_pattern": item.get("access_pattern"),
                "method": item.get("method"),
                "description": item.get("description"),
                "params_template": deepcopy(item.get("params_template") or {}),
            }
        )
    return sorted(rows, key=lambda item: str(item["name"]))


def _preview_context(payload: dict[str, Any]) -> dict[str, Any]:
    fields = _string_list(payload.get("fields"))
    optional_data = _normalize_optional_data(payload.get("optional_data") or payload.get("extension_data") or [])
    scope = {
        "symbols": _string_list(payload.get("symbols")),
        "universe": payload.get("universe") or None,
        "benchmark": payload.get("benchmark") or None,
        "start": payload.get("start") or None,
        "end": payload.get("end") or None,
    }
    scope = {key: value for key, value in scope.items() if value not in (None, [], "")}
    return {
        "task_type": payload.get("task_type") or "strategy",
        "scope": scope,
        "data_requirement": {
            "fields": fields,
            "required_fields": fields,
            "optional_data": optional_data,
            "scope": scope,
        },
        "intraday_requirement": payload.get("intraday_requirement") or {},
        "intraday_anchors": payload.get("intraday_anchors") or [],
    }


def _normalize_optional_data(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        capability = str(item.get("capability") or "").strip()
        fields = _string_list(item.get("fields"))
        if not capability or not fields:
            continue
        row: dict[str, Any] = {
            "capability": capability,
            "fields": fields,
            "slots": _string_list(item.get("slots")),
        }
        for key in ("access_pattern", "method", "query_template", "description"):
            value_text = str(item.get(key) or "").strip()
            if value_text:
                row[key] = value_text
        rows.append(row)
    return rows


def _build_extension_queries(
    *,
    manifest: dict[str, Any],
    mode_config: dict[str, Any],
    context: dict[str, Any],
    diagnostics: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    data_requirement = context.get("data_requirement") or {}
    optional_data = list(data_requirement.get("optional_data") or [])
    if not optional_data:
        return []

    aiqb = mode_config.get("aiquantbase") or {}
    allowed_extensions = {
        str(item.get("capability") or "").strip(): item
        for item in list(aiqb.get("extension_capabilities") or [])
        if isinstance(item, dict) and str(item.get("capability") or "").strip()
    }
    scope = context.get("scope") or {}
    rows: list[dict[str, Any]] = []
    for item in optional_data:
        if not isinstance(item, dict):
            continue
        capability = str(item.get("capability") or "").strip()
        fields = _string_list(item.get("fields"))
        if not capability or not fields:
            continue
        allowed = allowed_extensions.get(capability)
        if not isinstance(allowed, dict):
            diagnostics.append(
                _diagnostic(
                    "error",
                    "extension_capability_not_allowed",
                    f"extension capability is not registered by mode: {capability}",
                    capability=capability,
                )
            )
            continue
        allowed_slots = set(_string_list(allowed.get("allowed_slots") or allowed.get("slots")))
        requested_slots = set(_string_list(item.get("slots")))
        unknown_slots = sorted(requested_slots - allowed_slots) if allowed_slots else []
        if unknown_slots:
            diagnostics.append(
                _diagnostic(
                    "error",
                    "extension_slot_not_allowed",
                    f"extension capability {capability} does not allow slots: {', '.join(unknown_slots)}",
                    capability=capability,
                    slots=unknown_slots,
                )
            )
            continue
        binding = _find_provider_binding(
            manifest=manifest,
            capability=capability,
            method=str(item.get("method") or "").strip(),
            access_pattern=str(item.get("access_pattern") or "").strip(),
            asset_type="stock",
        )
        if binding is None:
            diagnostics.append(
                _diagnostic(
                    "error",
                    "missing_extension_provider_binding",
                    f"provider binding not found for extension capability {capability}",
                    capability=capability,
                )
            )
            continue
        requested_field_map, unmapped_fields = _select_requested_field_map(
            binding.get("node_field_map") or binding.get("field_map") or {},
            fields,
        )
        rows.append(
            {
                "id": f"extension_{capability.replace('.', '_')}",
                "use": capability,
                "required": False,
                "capability": capability,
                "access_pattern": item.get("access_pattern") or binding.get("access_pattern"),
                "method": item.get("method") or binding.get("method"),
                "provider_node": binding.get("node"),
                "params": {
                    "asset_type": "stock",
                    "symbols": list(scope.get("symbols") or []),
                    "universe": scope.get("universe"),
                    "fields": fields,
                    "start": scope.get("start"),
                    "end": scope.get("end"),
                },
                "key_schema": binding.get("key_schema") or {},
                "requested_field_map": requested_field_map,
                "unmapped_fields": unmapped_fields,
                "slots": sorted(requested_slots),
            }
        )
    return rows


def _query_need_enabled(item: dict[str, Any], context: dict[str, Any]) -> bool:
    enabled_when = item.get("enabled_when")
    if not isinstance(enabled_when, dict):
        return True
    all_of_present = enabled_when.get("all_of_present")
    if isinstance(all_of_present, list) and any(not _has_value(_extract(context, path)) for path in all_of_present):
        return False
    any_of_present = enabled_when.get("any_of_present")
    if isinstance(any_of_present, list) and not any(_has_value(_extract(context, path)) for path in any_of_present):
        return False
    return True


def _apply_derivations(params: dict[str, Any], derive: dict[str, Any], context: dict[str, Any]) -> None:
    for key, source in derive.items():
        if key.endswith("_from"):
            target = key[: -len("_from")]
            value = _extract(context, source)
            if target == "symbol":
                params["symbols"] = [value] if value else []
            elif target == "symbols":
                params["symbols"] = list(value or [])
            elif target == "universe":
                params["universe"] = value
            elif target == "start":
                params["start"] = value
            elif target == "end":
                params["end"] = value
            elif target == "fields":
                params["fields"] = _merge_unique(params.get("fields"), value)
            elif target == "anchors":
                params["anchors"] = list(value or [])
            elif target == "hhmm_list":
                params["hhmm_list"] = list(value or [])
            elif target == "start_hhmm":
                params["start_hhmm"] = _hhmm_boundary(value, boundary="min")
            elif target == "end_hhmm":
                params["end_hhmm"] = _hhmm_boundary(value, boundary="max")
            elif target == "trading_days":
                params["trading_days"] = list(value or [])
        elif key == "fields_default":
            params["fields"] = _merge_unique(params.get("fields"), source)
        elif key == "fields_append":
            params["fields"] = _merge_unique(params.get("fields"), source)


def _finalize_params(params: dict[str, Any]) -> None:
    anchors = list(params.get("anchors") or [])
    if anchors:
        symbols = sorted(
            {
                str(item.get("code") or "").strip()
                for item in anchors
                if isinstance(item, dict) and str(item.get("code") or "").strip()
            }
        )
        trading_days = sorted(
            {
                str(item.get("execution_date") or "").strip()
                for item in anchors
                if isinstance(item, dict) and str(item.get("execution_date") or "").strip()
            }
        )
        if not params.get("symbols"):
            params["symbols"] = symbols
        if not params.get("trading_days"):
            params["trading_days"] = trading_days
        params.pop("anchors", None)
    if "fields" in params:
        params["fields"] = _merge_unique([], params.get("fields"))
    for key in list(params):
        value = params[key]
        if value is None:
            params.pop(key)
        elif isinstance(value, list) and not value:
            params.pop(key)


def _hhmm_boundary(value: Any, *, boundary: str) -> Any:
    if not isinstance(value, list):
        return value
    candidates = [str(item).strip() for item in value if str(item).strip()]
    if not candidates:
        return None
    return min(candidates) if boundary == "min" else max(candidates)


def _find_provider_binding(
    *,
    manifest: dict[str, Any],
    capability: str,
    method: str,
    access_pattern: str,
    asset_type: str = "stock",
) -> dict[str, Any] | None:
    nodes = manifest.get("nodes") or {}
    if not isinstance(nodes, dict):
        return None
    for node_name, node in nodes.items():
        if not isinstance(node, dict):
            continue
        semantics = node.get("semantics") or {}
        if not isinstance(semantics, dict) or capability not in semantics:
            continue
        if asset_type and node.get("asset_types") and asset_type not in node.get("asset_types"):
            continue
        if method and node.get("methods") and method not in node.get("methods"):
            continue
        if access_pattern and node.get("access_patterns") and access_pattern not in node.get("access_patterns"):
            continue
        semantic = semantics.get(capability) or {}
        access_patterns = list(node.get("access_patterns") or [])
        methods = list(node.get("methods") or [])
        return {
            "node": node_name,
            "provider": manifest.get("provider") or "aiquantbase",
            "access_pattern": access_pattern or (access_patterns[0] if access_patterns else None),
            "method": method or (methods[0] if methods else None),
            "key_schema": deepcopy(node.get("keys") or {}),
            "field_map": deepcopy((semantic.get("fields") if isinstance(semantic, dict) else {}) or {}),
            "node_field_map": _collect_node_field_map(node),
        }
    return None


def _collect_node_field_map(node: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    semantics = node.get("semantics") or {}
    if not isinstance(semantics, dict):
        return fields
    for semantic in semantics.values():
        if not isinstance(semantic, dict):
            continue
        semantic_fields = semantic.get("fields") or {}
        if not isinstance(semantic_fields, dict):
            continue
        for key, value in semantic_fields.items():
            fields.setdefault(str(key), deepcopy(value))
    return fields


def _select_requested_field_map(field_map: dict[str, Any], fields: list[Any]) -> tuple[dict[str, Any], list[str]]:
    selected: dict[str, Any] = {}
    unmapped: list[str] = []
    for field in fields:
        key = str(field or "").strip()
        if not key:
            continue
        if key in field_map:
            selected[key] = deepcopy(field_map[key])
        else:
            unmapped.append(key)
    return selected, unmapped


def _extract(context: dict[str, Any], path: Any) -> Any:
    if isinstance(path, list):
        values: list[Any] = []
        for item in path:
            value = _extract(context, item)
            if isinstance(value, list):
                values.extend(value)
            elif value is not None:
                values.append(value)
        return values
    if not isinstance(path, str):
        return None
    current: Any = context
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
            continue
        return None
    return current


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return bool(value)
    return True


def _merge_unique(base: Any, extra: Any) -> list[Any]:
    merged: list[Any] = []
    for source in (base, extra):
        items = source if isinstance(source, list) else [source]
        for item in items:
            if item is None or item == "":
                continue
            if item not in merged:
                merged.append(item)
    return merged


def _safe_load_yaml(path: Path, *, diagnostics: list[dict[str, Any]], label: str) -> dict[str, Any]:
    if not path.exists():
        diagnostics.append(_diagnostic("error", "missing_file", f"{label} not found: {path}", path=str(path)))
        return {}
    try:
        return load_yaml(path)
    except Exception as exc:
        diagnostics.append(_diagnostic("error", "invalid_yaml", str(exc), path=str(path)))
        return {}


def _resolve_capability_root(
    *,
    capability_root: str | Path | None = None,
    alphablocks_root: str | Path | None = None,
) -> Path:
    if capability_root not in (None, ""):
        return _resolve_path(capability_root, base=PROJECT_ROOT)
    if alphablocks_root not in (None, ""):
        return _resolve_path(alphablocks_root, base=PROJECT_ROOT) / "config" / "aiquantbase"
    return _resolve_path(default_capability_root(), base=PROJECT_ROOT)


def _resolve_capability_path(path_like: str | Path | None, *, root: Path) -> Path:
    if path_like in (None, ""):
        raise ValueError("path is required")
    path = _resolve_path(path_like, base=root)
    return path


def _resolve_path(path_like: str | Path, *, base: Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = base / path
    return path


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dump_yaml(payload), encoding="utf-8")


def _required_text(payload: dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_items = value.replace("\n", ",").split(",")
    elif isinstance(value, list):
        raw_items = value
    else:
        raw_items = []
    items: list[str] = []
    for item in raw_items:
        text = str(item or "").strip()
        if text and text not in items:
            items.append(text)
    return items


def _normalize_field_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {str(key).strip(): deepcopy(field) for key, field in value.items() if str(key).strip()}
    if isinstance(value, list):
        result: dict[str, Any] = {}
        for item in value:
            if isinstance(item, dict):
                semantic_field = str(item.get("semantic_field") or item.get("field") or "").strip()
                source_field = item.get("source_field")
                if semantic_field and source_field not in (None, ""):
                    result[semantic_field] = source_field
        return result
    return {}


def _diagnostic(severity: str, code: str, message: str, **extra: Any) -> dict[str, Any]:
    payload = {"severity": severity, "code": code, "message": message}
    payload.update({key: value for key, value in extra.items() if value not in (None, "")})
    return payload
