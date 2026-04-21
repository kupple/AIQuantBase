from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from .config import dump_yaml, load_yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WIDE_TABLE_PATH = PROJECT_ROOT / 'config' / 'wide_tables.yaml'
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


def export_wide_table_yaml(design_id: str, path: str | Path | None = None) -> str:
    workspace = load_wide_table_workspace(path)
    target_id = str(design_id or '').strip()
    design = next((item for item in workspace['wide_tables'] if item['id'] == target_id), None)
    if design is None:
        raise ValueError(f'wide table id not found: {target_id}')
    payload = {
        'wide_table': {
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
        }
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


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()
