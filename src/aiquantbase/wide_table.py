from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import dump_yaml, load_field_catalog, load_nodes_and_edges, load_yaml
from .models import QueryIntent, SelectField, SafetyOptions
from .planner import GraphRegistry, QueryPlanner
from .sql import SqlRenderer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_GRAPH_PATH = PROJECT_ROOT / 'config' / 'graph.yaml'
DEFAULT_FIELDS_PATH = PROJECT_ROOT / 'config' / 'fields.yaml'
VALID_ENGINES = {'Memory', 'ReplacingMergeTree'}


def resolve_graph_path(path: str | Path | None = None) -> Path:
    target = Path(path or DEFAULT_GRAPH_PATH)
    if not target.is_absolute():
        target = PROJECT_ROOT / target
    return target


def resolve_fields_path(path: str | Path | None = None) -> Path:
    target = Path(path or DEFAULT_FIELDS_PATH)
    if not target.is_absolute():
        target = PROJECT_ROOT / target
    return target


def load_wide_table_workspace(
    path: str | Path | None = None,
    *,
    graph_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved = resolve_graph_path(graph_path or path)
    if not resolved.exists():
        return _default_workspace(resolved)
    data = load_yaml(resolved)
    return _workspace_from_graph_data(data, resolved)


def save_wide_table_workspace(data: dict[str, Any], path: str | Path | None = None) -> dict[str, Any]:
    """Compatibility shim: persist wide-table metadata into graph nodes."""
    resolved = resolve_graph_path(path)
    graph_data = load_yaml(resolved) if resolved.exists() else {'nodes': [], 'edges': []}
    for item in list(data.get('wide_tables') or []):
        upsert_wide_table(item, graph_path=resolved)
    return _workspace_from_graph_data(graph_data if not resolved.exists() else load_yaml(resolved), resolved)


def list_wide_tables(
    path: str | Path | None = None,
    *,
    source_node: str | None = None,
    graph_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    workspace = load_wide_table_workspace(path, graph_path=graph_path)
    items = []
    for item in workspace['wide_tables']:
        if source_node and item['source_node'] != source_node:
            continue
        items.append({**item})
    return items


def upsert_wide_table(
    payload: dict[str, Any],
    path: str | Path | None = None,
    *,
    graph_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved = resolve_graph_path(graph_path or path)
    graph_data = load_yaml(resolved) if resolved.exists() else {'nodes': [], 'edges': []}
    nodes = list(graph_data.get('nodes') or [])
    design = _normalize_design(payload)
    design_id = str(payload.get('id') or '').strip()
    index = _find_wide_table_node_index(nodes, design_id=design_id, name=design['name'], source_node=design['source_node'])
    if index is None:
        if design_id:
            raise ValueError(f'wide table id not found: {design_id}')
        duplicate = next((item for item in _workspace_from_graph_data(graph_data, resolved)['wide_tables'] if item['name'] == design['name']), None)
        if duplicate is not None:
            raise ValueError(f'wide table name already exists: {design["name"]}')
        source_node = next((node for node in nodes if str(node.get('name') or '') == design['source_node']), None)
        nodes.append(_create_node_from_design(design, source_node=source_node))
    else:
        nodes[index] = _apply_design_to_node(nodes[index], design)

    graph_data['nodes'] = nodes
    graph_data.setdefault('edges', [])
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(dump_yaml(graph_data), encoding='utf-8')
    return design


def delete_wide_table(
    design_id: str,
    path: str | Path | None = None,
    *,
    graph_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved = resolve_graph_path(graph_path or path)
    graph_data = load_yaml(resolved)
    nodes = list(graph_data.get('nodes') or [])
    target_id = str(design_id or '').strip()
    if not target_id:
        raise ValueError('wide table id is required')
    index = _find_wide_table_node_index(nodes, design_id=target_id)
    if index is None:
        raise ValueError(f'wide table id not found: {target_id}')
    removed = _node_to_design(nodes[index])
    nodes[index].pop('wide_table', None)
    graph_data['nodes'] = nodes
    resolved.write_text(dump_yaml(graph_data), encoding='utf-8')
    return removed


def export_wide_table_yaml(
    design_id: str,
    path: str | Path | None = None,
    graph_path: str | Path | None = None,
    fields_path: str | Path | None = None,
) -> str:
    payload = build_wide_table_export_payload(
        design_id,
        graph_path=graph_path or path,
        fields_path=fields_path,
    )
    return dump_yaml(payload)


def build_wide_table_export_payload(
    design_id: str,
    path: str | Path | None = None,
    graph_path: str | Path | None = None,
    fields_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_graph_path = resolve_graph_path(graph_path or path)
    workspace = load_wide_table_workspace(graph_path=resolved_graph_path)
    target_id = str(design_id or '').strip()
    design = next((item for item in workspace['wide_tables'] if item['id'] == target_id), None)
    if design is None:
        raise ValueError(f'wide table id not found: {target_id}')

    resolved_fields_path = resolve_fields_path(fields_path)

    nodes, edges = load_nodes_and_edges(resolved_graph_path)
    field_catalog = load_field_catalog(resolved_fields_path)
    graph_bundle = _build_graph_bundle_snapshot(
        source_node_name=design['source_node'],
        selected_fields=list(design['fields']),
        nodes=nodes,
        edges=edges,
        field_catalog=field_catalog,
    )
    materialization_bundle = _build_materialization_bundle(
        source_node_name=design['source_node'],
        selected_fields=list(design['fields']),
        nodes=nodes,
        edges=edges,
        field_catalog=field_catalog,
    )
    return {
        'wide_table': {
            'id': design['id'],
            'name': design['name'],
            'description': design['description'],
            'source_node': design['source_node'],
            'target': {
                'database': design['target_database'],
                'table': design['target_table'],
                'engine': design['engine'],
                'partition_by': list(design['partition_by']),
                'order_by': list(design['order_by']),
                'version_field': design['version_field'],
            },
            'fields': list(design['fields']),
            'key_fields': list(design['key_fields']),
            'status': design['status'],
            'created_at': design['created_at'],
            'updated_at': design['updated_at'],
        },
        'graph_bundle': {
            'graph_path': str(resolved_graph_path),
            'fields_path': str(resolved_fields_path),
            **graph_bundle,
        },
        'materialization_bundle': materialization_bundle,
        'export_meta': {
            'exported_at': _now_iso(),
            'exported_by': 'aiquantbase',
            'format_version': 1,
        },
    }


def get_wide_table_summary(
    path: str | Path | None = None,
    *,
    graph_path: str | Path | None = None,
) -> dict[str, Any]:
    workspace = load_wide_table_workspace(path, graph_path=graph_path)
    return {
        'version': workspace['version'],
        'path': str(workspace['path']),
        'wide_table_count': len(workspace['wide_tables']),
        'enabled_count': sum(1 for item in workspace['wide_tables'] if item['status'] == 'enabled'),
    }


def _default_workspace(path: Path) -> dict[str, Any]:
    return {
        'version': 1,
        'path': path,
        'wide_tables': [],
    }


def _workspace_from_graph_data(data: dict[str, Any], path: Path) -> dict[str, Any]:
    workspace = _default_workspace(path)
    workspace['wide_tables'] = [
        design
        for node in list(data.get('nodes') or [])
        if isinstance(node, dict)
        for design in [_node_to_design(node)]
        if design is not None
    ]
    return workspace


def _normalize_design(payload: dict[str, Any]) -> dict[str, Any]:
    name = str(payload.get('name') or '').strip()
    source_node = str(payload.get('source_node') or '').strip()
    target_database = str(payload.get('target_database') or '').strip()
    target_table = str(payload.get('target_table') or '').strip()
    engine = str(payload.get('engine') or 'Memory').strip() or 'Memory'
    if not name or not source_node or not target_database or not target_table:
        raise ValueError('wide table requires name, source_node, target_database, and target_table')
    if engine not in VALID_ENGINES:
        raise ValueError(f'unsupported wide table engine: {engine}')

    fields = [
        str(item).strip()
        for item in (payload.get('fields') or [])
        if str(item).strip()
    ]
    if not fields:
        raise ValueError('wide table requires at least one field')

    key_fields = [
        str(item).strip()
        for item in (payload.get('key_fields') or [])
        if str(item).strip()
    ]
    if not key_fields:
        raise ValueError('wide table requires at least one key_field')

    partition_by = [
        str(item).strip()
        for item in (payload.get('partition_by') or [])
        if str(item).strip()
    ]
    order_by = [
        str(item).strip()
        for item in (payload.get('order_by') or [])
        if str(item).strip()
    ]
    if not order_by:
        order_by = list(key_fields)

    return {
        'id': str(payload.get('id') or f'wide::{name}'),
        'name': name,
        'description': str(payload.get('description') or '').strip(),
        'source_node': source_node,
        'target_database': target_database,
        'target_table': target_table,
        'engine': engine,
        'fields': fields,
        'key_fields': key_fields,
        'partition_by': partition_by,
        'order_by': order_by,
        'version_field': str(payload.get('version_field') or '').strip(),
        'status': str(payload.get('status') or 'enabled').strip() or 'enabled',
        'created_at': str(payload.get('created_at') or _now_iso()),
        'updated_at': str(payload.get('updated_at') or _now_iso()),
    }


def _find_wide_table_node_index(
    nodes: list[dict[str, Any]],
    *,
    design_id: str | None = None,
    name: str | None = None,
    source_node: str | None = None,
) -> int | None:
    target_id = str(design_id or '').strip()
    target_name = str(name or '').strip()
    target_source = str(source_node or '').strip()
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            continue
        wide_table = node.get('wide_table') if isinstance(node.get('wide_table'), dict) else {}
        if target_id and str(wide_table.get('id') or '') == target_id:
            return index
        node_name = str(node.get('name') or '').strip()
        if not target_id and target_name and node_name == target_name:
            return index
        if not target_id and target_source and node_name == target_source:
            return index
    return None


def _node_to_design(node: dict[str, Any]) -> dict[str, Any] | None:
    wide_table = node.get('wide_table')
    if not isinstance(wide_table, dict):
        return None

    node_name = str(node.get('name') or '').strip()
    database, table = _split_table_ref(str(node.get('table') or ''))
    payload = {
        **wide_table,
        'id': str(wide_table.get('id') or f'wide::{node_name}'),
        'name': str(wide_table.get('name') or node_name),
        'description': str(wide_table.get('description') or node.get('description') or ''),
        'source_node': str(wide_table.get('source_node') or node_name),
        'target_database': str(wide_table.get('target_database') or database),
        'target_table': str(wide_table.get('target_table') or table),
        'engine': str(wide_table.get('engine') or 'Memory'),
        'fields': list(wide_table.get('fields') or node.get('fields') or []),
        'key_fields': list(wide_table.get('key_fields') or _default_key_fields(node)),
        'partition_by': list(wide_table.get('partition_by') or []),
        'order_by': list(wide_table.get('order_by') or wide_table.get('key_fields') or _default_key_fields(node)),
        'version_field': str(wide_table.get('version_field') or ''),
        'status': str(wide_table.get('status') or node.get('status') or 'enabled'),
        'created_at': str(wide_table.get('created_at') or _now_iso()),
        'updated_at': str(wide_table.get('updated_at') or _now_iso()),
    }
    return _normalize_design(payload)


def _create_node_from_design(design: dict[str, Any], *, source_node: dict[str, Any] | None = None) -> dict[str, Any]:
    if source_node is not None:
        time_key = source_node.get('time_key')
        table_ref = source_node.get('table') or f"{design['target_database']}.{design['target_table']}"
        fields = list(source_node.get('fields') or [])
        entity_keys = list(source_node.get('entity_keys') or [])
        base_filters = list(source_node.get('base_filters') or [])
    else:
        time_key = _infer_time_key(design)
        table_ref = f"{design['target_database']}.{design['target_table']}"
        fields = list(design['fields'])
        entity_keys = _entity_keys_from_key_fields(design['key_fields'], time_key=time_key)
        base_filters = []
    return {
        'name': design['name'],
        'table': table_ref,
        'entity_keys': entity_keys,
        'time_key': time_key,
        'grain': (source_node or {}).get('grain'),
        'fields': fields,
        'description': design['description'] or None,
        'description_zh': (source_node or {}).get('description_zh') or design['description'] or None,
        'node_role': (source_node or {}).get('node_role') or 'query_entry',
        'status': design['status'],
        'asset_type': (source_node or {}).get('asset_type'),
        'query_freq': (source_node or {}).get('query_freq'),
        'base_filters': base_filters,
        'wide_table': _design_to_node_meta(design),
    }


def _apply_design_to_node(node: dict[str, Any], design: dict[str, Any]) -> dict[str, Any]:
    next_node = dict(node)
    next_node['name'] = design['name']
    next_node['description'] = design['description'] or next_node.get('description')
    next_node['status'] = design['status']
    next_node['wide_table'] = _design_to_node_meta(design)
    return next_node


def _design_to_node_meta(design: dict[str, Any]) -> dict[str, Any]:
    return {
        'id': design['id'],
        'description': design['description'],
        'source_node': design['source_node'],
        'target_database': design['target_database'],
        'target_table': design['target_table'],
        'engine': design['engine'],
        'fields': list(design['fields']),
        'key_fields': list(design['key_fields']),
        'partition_by': list(design['partition_by']),
        'order_by': list(design['order_by']),
        'version_field': design['version_field'],
        'status': design['status'],
        'created_at': design['created_at'],
        'updated_at': _now_iso(),
    }


def _split_table_ref(table_ref: str) -> tuple[str, str]:
    value = str(table_ref or '').strip()
    if '.' not in value:
        return '', value
    database, table = value.split('.', 1)
    return database, table


def _default_key_fields(node: dict[str, Any]) -> list[str]:
    items = [str(item or '').strip() for item in list(node.get('entity_keys') or []) if str(item or '').strip()]
    time_key = str(node.get('time_key') or '').strip()
    if time_key and time_key not in items:
        items.append(time_key)
    return items


def _infer_time_key(design: dict[str, Any], *, fallback: str | None = None) -> str | None:
    candidates = list(design.get('key_fields') or []) + list(design.get('fields') or [])
    for field_name in candidates:
        field_name = str(field_name or '').strip()
        if field_name in {'trade_time', 'trade_date', 'date', 'ann_date', 'change_date', 'report_date', 'end_date'}:
            return field_name
    return fallback


def _entity_keys_from_key_fields(key_fields: list[str], *, time_key: str | None) -> list[str]:
    return [
        str(item or '').strip()
        for item in key_fields
        if str(item or '').strip() and str(item or '').strip() != str(time_key or '')
    ]


def _build_graph_bundle_snapshot(
    *,
    source_node_name: str,
    selected_fields: list[str],
    nodes: list[Any],
    edges: list[Any],
    field_catalog: list[Any],
) -> dict[str, Any]:
    node_by_name = {node.name: node for node in nodes}
    source_node = node_by_name.get(source_node_name)
    if source_node is None:
        raise ValueError(f'source node not found in graph: {source_node_name}')

    relevant_node_names: set[str] = {source_node_name}
    relevant_field_names: set[str] = set()
    relevant_fields: list[Any] = []

    def register_field(field_name: str) -> None:
        field_name = str(field_name or '').strip()
        if not field_name or field_name in relevant_field_names:
            return
        relevant_field_names.add(field_name)

        matching_entries = [
          entry for entry in field_catalog
          if entry.standard_field == field_name and entry.base_node == source_node_name
        ]
        for entry in matching_entries:
            relevant_fields.append(entry)
            if entry.source_node:
                relevant_node_names.add(str(entry.source_node))
            for dependency in entry.depends_on or []:
                register_field(str(dependency))

    for field_name in selected_fields:
        register_field(field_name)

    raw_source_fields = [
        field_name
        for field_name in source_node.fields
        if field_name in set(selected_fields) or field_name in relevant_field_names
    ]
    relevant_node_names.update(
        str(entry.source_node)
        for entry in relevant_fields
        if getattr(entry, 'source_node', None)
    )

    snapshot_nodes = [
        asdict(node)
        for node in nodes
        if node.name in relevant_node_names
    ]
    snapshot_edges = [
        asdict(edge)
        for edge in edges
        if edge.from_node in relevant_node_names and edge.to_node in relevant_node_names
    ]
    snapshot_fields = [asdict(entry) for entry in relevant_fields]

    return {
        'scope': {
            'source_node': source_node_name,
            'selected_fields': selected_fields,
            'raw_source_fields': raw_source_fields,
        },
        'snapshot': {
            'nodes': snapshot_nodes,
            'edges': snapshot_edges,
            'fields': snapshot_fields,
        },
    }


def _build_materialization_bundle(
    *,
    source_node_name: str,
    selected_fields: list[str],
    nodes: list[Any],
    edges: list[Any],
    field_catalog: list[Any],
) -> dict[str, Any]:
    registry = GraphRegistry(nodes, edges, field_catalog=field_catalog)
    planner = QueryPlanner(registry)
    intent = QueryIntent(
        from_node=source_node_name,
        select=[SelectField(field=item) for item in selected_fields],
        safety=SafetyOptions(lookahead_safe=False, strict_mode=True),
    )
    plan = planner.plan(intent)
    sql = SqlRenderer(registry).render(plan)
    base_node = registry.nodes[plan.base_node]
    return {
        'query_intent': {
            'from': source_node_name,
            'select': selected_fields,
            'safety': {
                'lookahead_safe': False,
                'strict_mode': True,
            },
        },
        'query_plan': asdict(plan),
        'base_context': {
            'base_node': plan.base_node,
            'base_table': base_node.table,
            'entity_keys': list(base_node.entity_keys),
            'time_key': base_node.time_key,
            'grain': base_node.grain,
            'base_filters': list(base_node.base_filters),
        },
        'preview_sql': sql,
    }


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()
