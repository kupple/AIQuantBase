from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from .config import dump_yaml, load_field_catalog, load_nodes_and_edges, load_yaml
from .models import QueryIntent, SelectField, SafetyOptions
from .planner import GraphRegistry, QueryPlanner
from .sql import SqlRenderer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WIDE_TABLE_PATH = PROJECT_ROOT / 'config' / 'wide_tables.yaml'
DEFAULT_GRAPH_PATH = PROJECT_ROOT / 'config' / 'graph.yaml'
DEFAULT_FIELDS_PATH = PROJECT_ROOT / 'config' / 'fields.yaml'
VALID_ENGINES = {'Memory', 'ReplacingMergeTree'}


def resolve_wide_table_path(path: str | Path | None = None) -> Path:
    target = Path(path or DEFAULT_WIDE_TABLE_PATH)
    if not target.is_absolute():
        target = PROJECT_ROOT / target
    return target


def load_wide_table_workspace(path: str | Path | None = None) -> dict[str, Any]:
    resolved = resolve_wide_table_path(path)
    if not resolved.exists():
        return _default_workspace(resolved)
    data = load_yaml(resolved)
    return _normalize_workspace(data, resolved)


def save_wide_table_workspace(data: dict[str, Any], path: str | Path | None = None) -> dict[str, Any]:
    resolved = resolve_wide_table_path(path)
    workspace = _normalize_workspace(data, resolved)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(
        dump_yaml(
            {
                'version': workspace['version'],
                'wide_tables': workspace['wide_tables'],
            }
        ),
        encoding='utf-8',
    )
    return workspace


def list_wide_tables(path: str | Path | None = None, *, source_node: str | None = None) -> list[dict[str, Any]]:
    workspace = load_wide_table_workspace(path)
    items = []
    for item in workspace['wide_tables']:
        if source_node and item['source_node'] != source_node:
            continue
        items.append({**item})
    return items


def upsert_wide_table(payload: dict[str, Any], path: str | Path | None = None) -> dict[str, Any]:
    workspace = load_wide_table_workspace(path)
    design = _normalize_design(payload)
    design_id = str(payload.get('id') or '').strip()
    if design_id:
        index = next((i for i, item in enumerate(workspace['wide_tables']) if item['id'] == design_id), None)
        if index is None:
            raise ValueError(f'wide table id not found: {design_id}')
        workspace['wide_tables'][index] = {**workspace['wide_tables'][index], **design}
    else:
        duplicate = next((item for item in workspace['wide_tables'] if item['name'] == design['name']), None)
        if duplicate is not None:
            raise ValueError(f'wide table name already exists: {design["name"]}')
        workspace['wide_tables'].append(design)
    save_wide_table_workspace(workspace, path)
    return design


def delete_wide_table(design_id: str, path: str | Path | None = None) -> dict[str, Any]:
    workspace = load_wide_table_workspace(path)
    target_id = str(design_id or '').strip()
    if not target_id:
        raise ValueError('wide table id is required')
    index = next((i for i, item in enumerate(workspace['wide_tables']) if item['id'] == target_id), None)
    if index is None:
        raise ValueError(f'wide table id not found: {target_id}')
    removed = workspace['wide_tables'].pop(index)
    save_wide_table_workspace(workspace, path)
    return removed


def export_wide_table_yaml(
    design_id: str,
    path: str | Path | None = None,
    graph_path: str | Path | None = None,
    fields_path: str | Path | None = None,
) -> str:
    workspace = load_wide_table_workspace(path)
    target_id = str(design_id or '').strip()
    design = next((item for item in workspace['wide_tables'] if item['id'] == target_id), None)
    if design is None:
        raise ValueError(f'wide table id not found: {target_id}')

    resolved_graph_path = Path(graph_path or DEFAULT_GRAPH_PATH)
    if not resolved_graph_path.is_absolute():
        resolved_graph_path = PROJECT_ROOT / resolved_graph_path
    resolved_fields_path = Path(fields_path or DEFAULT_FIELDS_PATH)
    if not resolved_fields_path.is_absolute():
        resolved_fields_path = PROJECT_ROOT / resolved_fields_path

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
    payload = {
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
    return dump_yaml(payload)


def get_wide_table_summary(path: str | Path | None = None) -> dict[str, Any]:
    workspace = load_wide_table_workspace(path)
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


def _normalize_workspace(data: dict[str, Any], path: Path) -> dict[str, Any]:
    workspace = _default_workspace(path)
    workspace['version'] = int(data.get('version') or 1)
    workspace['wide_tables'] = [
        _normalize_design(item)
        for item in list(data.get('wide_tables') or [])
        if isinstance(item, dict)
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
        'id': str(payload.get('id') or f'wide::{name}::{uuid4().hex[:8]}'),
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
