from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml

from .config import dump_yaml, load_yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MEMBERSHIP_PATH = PROJECT_ROOT / 'config' / 'membership.yaml'


def resolve_membership_path(path: str | Path | None = None) -> Path:
    target = Path(path or DEFAULT_MEMBERSHIP_PATH)
    if not target.is_absolute():
        target = PROJECT_ROOT / target
    return target


def load_membership_workspace(path: str | Path | None = None) -> dict[str, Any]:
    resolved = resolve_membership_path(path)
    if not resolved.exists():
        return _default_workspace(resolved)

    data = load_yaml(resolved)
    return _normalize_workspace(data, resolved)


def save_membership_workspace(data: dict[str, Any], path: str | Path | None = None) -> dict[str, Any]:
    resolved = resolve_membership_path(path)
    workspace = _normalize_workspace(data, resolved)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(
        dump_yaml(
            {
                'version': workspace['version'],
                'sources': workspace['sources'],
                'taxonomies': workspace['taxonomies'],
                'members': workspace['members'],
                'relations': workspace['relations'],
            }
        ),
        encoding='utf-8',
    )
    return workspace


def list_domains(path: str | Path | None = None) -> list[dict[str, Any]]:
    workspace = load_membership_workspace(path)
    sources = workspace['sources']
    taxonomies = workspace['taxonomies']
    members = workspace['members']
    relations = workspace['relations']

    domains = sorted(
        {
            item['domain']
            for collection in (sources, taxonomies, members, relations)
            for item in collection
            if item.get('domain')
        }
    )
    items = []
    for domain in domains:
        items.append(
            {
                'domain': domain,
                'source_count': sum(1 for item in sources if item.get('domain') == domain),
                'taxonomy_count': sum(1 for item in taxonomies if item.get('domain') == domain),
                'member_count': sum(1 for item in members if item.get('domain') == domain),
                'relation_count': sum(1 for item in relations if item.get('domain') == domain),
            }
        )
    return items


def list_sources(
    path: str | Path | None = None,
    *,
    domain: str | None = None,
    taxonomy: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    workspace = load_membership_workspace(path)
    items = []
    for source in workspace['sources']:
        if domain and source['domain'] != domain:
            continue
        if taxonomy and source['taxonomy'] != taxonomy:
            continue
        if status and source['status'] != status:
            continue
        items.append({**source})
    return items


def list_taxonomies(path: str | Path | None = None, *, domain: str | None = None) -> list[dict[str, Any]]:
    workspace = load_membership_workspace(path)
    members = workspace['members']
    relations = workspace['relations']
    items = []
    for taxonomy in workspace['taxonomies']:
        if domain and taxonomy['domain'] != domain:
            continue
        items.append(
            {
                **taxonomy,
                'member_count': sum(
                    1
                    for item in members
                    if item['domain'] == taxonomy['domain'] and item['taxonomy'] == taxonomy['taxonomy']
                ),
                'relation_count': sum(
                    1
                    for item in relations
                    if item['domain'] == taxonomy['domain'] and item['taxonomy'] == taxonomy['taxonomy']
                ),
            }
        )
    return items


def list_members(
    path: str | Path | None = None,
    *,
    domain: str | None = None,
    taxonomy: str | None = None,
) -> list[dict[str, Any]]:
    workspace = load_membership_workspace(path)
    relations = workspace['relations']
    items = []
    for member in workspace['members']:
        if domain and member['domain'] != domain:
            continue
        if taxonomy and member['taxonomy'] != taxonomy:
            continue
        items.append(
            {
                **member,
                'relation_count': sum(
                    1
                    for relation in relations
                    if relation['domain'] == member['domain']
                    and relation['taxonomy'] == member['taxonomy']
                    and relation['member_code'] == member['member_code']
                ),
            }
        )
    return items


def list_relations(
    path: str | Path | None = None,
    *,
    security_code: str | None = None,
    domain: str | None = None,
    taxonomy: str | None = None,
    member_code: str | None = None,
    as_of_date: str | None = None,
    security_type: str | None = None,
) -> list[dict[str, Any]]:
    workspace = load_membership_workspace(path)
    items = []
    for relation in workspace['relations']:
        if security_code and relation['security_code'] != security_code:
            continue
        if domain and relation['domain'] != domain:
            continue
        if taxonomy and relation['taxonomy'] != taxonomy:
            continue
        if member_code and relation['member_code'] != member_code:
            continue
        if security_type and relation['security_type'] != security_type:
            continue
        if as_of_date and not _is_relation_active(relation, as_of_date):
            continue
        items.append({**relation})
    return items


def upsert_taxonomy(payload: dict[str, Any], path: str | Path | None = None) -> dict[str, Any]:
    workspace = load_membership_workspace(path)
    taxonomy = _normalize_taxonomy(payload)
    match_index = next(
        (
            index
            for index, item in enumerate(workspace['taxonomies'])
            if item['domain'] == taxonomy['domain'] and item['taxonomy'] == taxonomy['taxonomy']
        ),
        None,
    )
    if match_index is None:
        workspace['taxonomies'].append(taxonomy)
    else:
        workspace['taxonomies'][match_index] = {**workspace['taxonomies'][match_index], **taxonomy}
    save_membership_workspace(workspace, path)
    return taxonomy


def upsert_source(payload: dict[str, Any], path: str | Path | None = None) -> dict[str, Any]:
    workspace = load_membership_workspace(path)
    source = _normalize_source(payload)
    source_id = str(payload.get('id') or '').strip()
    if source_id:
        match_index = next(
            (
                index
                for index, item in enumerate(workspace['sources'])
                if item['id'] == source_id
            ),
            None,
        )
        if match_index is None:
            raise ValueError(f'source id not found: {source_id}')
        workspace['sources'][match_index] = {**workspace['sources'][match_index], **source}
    else:
        duplicated = next(
            (
                item
                for item in workspace['sources']
                if item['source_name'] == source['source_name']
            ),
            None,
        )
        if duplicated is not None:
            raise ValueError(f'source_name already exists: {source["source_name"]}')
        workspace['sources'].append(source)
    save_membership_workspace(workspace, path)
    return source


def delete_source(source_id: str, path: str | Path | None = None) -> dict[str, Any]:
    workspace = load_membership_workspace(path)
    target_id = str(source_id or '').strip()
    if not target_id:
        raise ValueError('source id is required')
    match_index = next((index for index, item in enumerate(workspace['sources']) if item['id'] == target_id), None)
    if match_index is None:
        raise ValueError(f'source id not found: {target_id}')
    removed = workspace['sources'].pop(match_index)
    save_membership_workspace(workspace, path)
    return removed


def upsert_member(payload: dict[str, Any], path: str | Path | None = None) -> dict[str, Any]:
    workspace = load_membership_workspace(path)
    member = _normalize_member(payload)
    match_index = next(
        (
            index
            for index, item in enumerate(workspace['members'])
            if item['domain'] == member['domain']
            and item['taxonomy'] == member['taxonomy']
            and item['member_code'] == member['member_code']
        ),
        None,
    )
    if match_index is None:
        workspace['members'].append(member)
    else:
        workspace['members'][match_index] = {**workspace['members'][match_index], **member}
    save_membership_workspace(workspace, path)
    return member


def upsert_relation(payload: dict[str, Any], path: str | Path | None = None) -> dict[str, Any]:
    workspace = load_membership_workspace(path)
    relation = _normalize_relation(payload, workspace['members'])
    match_index = next(
        (
            index
            for index, item in enumerate(workspace['relations'])
            if item['id'] == relation['id']
        ),
        None,
    )
    if match_index is None:
        workspace['relations'].append(relation)
    else:
        workspace['relations'][match_index] = {**workspace['relations'][match_index], **relation}
    save_membership_workspace(workspace, path)
    return relation


def patch_relation(payload: dict[str, Any], path: str | Path | None = None) -> dict[str, Any]:
    workspace = load_membership_workspace(path)
    relation_id = str(payload.get('id') or '').strip()
    if not relation_id:
        raise ValueError('relation id is required')
    match_index = next((index for index, item in enumerate(workspace['relations']) if item['id'] == relation_id), None)
    if match_index is None:
        raise ValueError(f'relation id not found: {relation_id}')
    updated = _normalize_relation({**workspace['relations'][match_index], **payload}, workspace['members'])
    workspace['relations'][match_index] = updated
    save_membership_workspace(workspace, path)
    return updated


def import_membership_payload(payload: dict[str, Any], path: str | Path | None = None) -> dict[str, Any]:
    workspace = load_membership_workspace(path)
    for source in payload.get('sources', []) or []:
        _upsert_into_list(
            workspace['sources'],
            _normalize_source(source),
            key=lambda item: item['id'],
        )
    for taxonomy in payload.get('taxonomies', []) or []:
        _upsert_into_list(
            workspace['taxonomies'],
            _normalize_taxonomy(taxonomy),
            key=lambda item: (item['domain'], item['taxonomy']),
        )
    for member in payload.get('members', []) or []:
        _upsert_into_list(
            workspace['members'],
            _normalize_member(member),
            key=lambda item: (item['domain'], item['taxonomy'], item['member_code']),
        )
    for relation in payload.get('relations', []) or []:
        _upsert_into_list(
            workspace['relations'],
            _normalize_relation(relation, workspace['members']),
            key=lambda item: item['id'],
        )
    save_membership_workspace(workspace, path)
    return workspace


def parse_import_payload(raw_text: str) -> dict[str, Any]:
    parsed = yaml.safe_load(raw_text) if raw_text.strip() else {}
    if parsed is None:
        parsed = {}
    if not isinstance(parsed, dict):
        raise ValueError('import payload must be a mapping')
    return {
        'sources': list(parsed.get('sources') or []),
        'taxonomies': list(parsed.get('taxonomies') or []),
        'members': list(parsed.get('members') or []),
        'relations': list(parsed.get('relations') or []),
    }


def parse_membership_filter_payload(raw_text: str) -> dict[str, Any]:
    parsed = yaml.safe_load(raw_text) if raw_text.strip() else {}
    if parsed is None:
        parsed = {}
    if not isinstance(parsed, dict):
        raise ValueError('membership filter payload must be a mapping')
    return {
        'include': list(parsed.get('include') or []),
        'exclude': list(parsed.get('exclude') or []),
    }


def query_membership(
    security_code: str,
    *,
    as_of_date: str,
    path: str | Path | None = None,
    security_type: str | None = None,
    executor: Any | None = None,
) -> list[dict[str, Any]]:
    items = list_relations(
        path,
        security_code=security_code,
        as_of_date=as_of_date,
        security_type=security_type,
    )
    if executor is None:
        return items
    source_items = _query_relations_from_sources(
        workspace=load_membership_workspace(path),
        executor=executor,
        as_of_date=as_of_date,
        security_code=security_code,
        security_type=security_type,
    )
    return _merge_relations(items, source_items)


def filter_symbols_by_membership(
    membership_filter: dict[str, Any],
    *,
    as_of_date: str,
    path: str | Path | None = None,
    security_type: str | None = None,
    executor: Any | None = None,
) -> dict[str, Any]:
    workspace = load_membership_workspace(path)
    active_relations = [
        relation
        for relation in workspace['relations']
        if _is_relation_active(relation, as_of_date)
        and (not security_type or relation['security_type'] == security_type)
        and relation['status'] == 'enabled'
    ]
    if executor is not None:
        dynamic_relations = _query_relations_from_sources(
            workspace=workspace,
            executor=executor,
            as_of_date=as_of_date,
            security_type=security_type,
            membership_filter=membership_filter,
        )
        active_relations = _merge_relations(active_relations, dynamic_relations)

    include_groups = list(membership_filter.get('include') or [])
    exclude_groups = list(membership_filter.get('exclude') or [])

    if include_groups:
        include_sets = [
            {
                relation['security_code']
                for relation in active_relations
                if _relation_matches_filter(relation, group)
            }
            for group in include_groups
        ]
        matched = set.intersection(*include_sets) if include_sets else set()
    else:
        matched = {relation['security_code'] for relation in active_relations}

    excluded = {
        relation['security_code']
        for relation in active_relations
        for group in exclude_groups
        if _relation_matches_filter(relation, group)
    }
    symbols = sorted(matched - excluded)
    return {
        'ok': True,
        'symbols': symbols,
        'count': len(symbols),
        'sample_symbols': symbols[:20],
        'as_of_date': as_of_date,
        'include': include_groups,
        'exclude': exclude_groups,
    }


def _query_relations_from_sources(
    *,
    workspace: dict[str, Any],
    executor: Any,
    as_of_date: str,
    security_code: str | None = None,
    security_type: str | None = None,
    membership_filter: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    sources = list(workspace.get('sources') or [])
    relations: list[dict[str, Any]] = []
    include_groups = list((membership_filter or {}).get('include') or [])
    exclude_groups = list((membership_filter or {}).get('exclude') or [])
    requested_groups = include_groups + exclude_groups

    for source in sources:
        if source.get('status') != 'enabled':
            continue
        if source.get('source_kind') != 'relation':
            continue
        if security_type and source.get('security_type') != security_type:
            continue
        if requested_groups:
            if not any(_source_matches_filter(source, item) for item in requested_groups):
                continue
        rows = _query_source_rows(
            executor=executor,
            source=source,
            as_of_date=as_of_date,
            security_code=security_code,
            membership_filter=membership_filter,
        )
        if not rows:
            continue
        preview = _preview_relation_source(source, rows, [], {
            'source_kind': source['source_kind'],
            'domain': source['domain'],
            'taxonomy': source['taxonomy'],
            'display_name': source['description'] or source['taxonomy'],
            'status': source['status'],
        })
        relations.extend(preview.get('relation_preview') or [])
    return relations


def _query_source_rows(
    *,
    executor: Any,
    source: dict[str, Any],
    as_of_date: str,
    security_code: str | None,
    membership_filter: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    select_fields = []
    for key in ('security_code_field', 'member_code_field', 'member_name_field', 'effective_from_field', 'effective_to_field', 'source_system_field'):
        field_name = str(source.get(key) or '').strip()
        if field_name and field_name not in select_fields:
            select_fields.append(field_name)
    for item in source.get('attribute_mappings') or []:
        field_name = str(item.get('field') or '').strip()
        if field_name and field_name not in select_fields:
            select_fields.append(field_name)
    if not select_fields:
        return []

    conditions: list[str] = []
    if security_code:
        conditions.append(f"{source['security_code_field']} = {_sql_quote(security_code)}")
    effective_from_field = str(source.get('effective_from_field') or '').strip()
    if effective_from_field:
        conditions.append(f"{effective_from_field} <= {_sql_quote(as_of_date)}")
    effective_to_field = str(source.get('effective_to_field') or '').strip()
    if effective_to_field:
        conditions.append(
            f"({effective_to_field} IS NULL OR toString({effective_to_field}) = '' OR {effective_to_field} >= {_sql_quote(as_of_date)})"
        )

    requested_groups = list((membership_filter or {}).get('include') or []) + list((membership_filter or {}).get('exclude') or [])
    matching_groups = [item for item in requested_groups if _source_matches_filter(source, item)]
    if matching_groups:
        member_code_field = str(source.get('member_code_field') or '').strip()
        member_code_value = str(source.get('member_code_value') or '').strip()
        requested_codes = sorted({str(item.get('member_code') or '').strip() for item in matching_groups if str(item.get('member_code') or '').strip()})
        if requested_codes:
            if member_code_field:
                if len(requested_codes) == 1:
                    conditions.append(f"{member_code_field} = {_sql_quote(requested_codes[0])}")
                else:
                    items = ", ".join(_sql_quote(value) for value in requested_codes)
                    conditions.append(f"{member_code_field} IN ({items})")
            elif member_code_value and member_code_value not in requested_codes:
                return []

    where = " AND ".join(conditions) if conditions else "1 = 1"
    sql = (
        f"SELECT {', '.join(select_fields)} "
        f"FROM {source['database']}.{source['table']} "
        f"WHERE {where}"
    )
    if hasattr(executor, 'execute_sql_df'):
        df = executor.execute_sql_df(sql)
        return df.to_dict(orient='records')
    result = executor.execute_sql(sql)
    return list(result.data or [])


def _source_matches_filter(source: dict[str, Any], membership_filter: dict[str, Any]) -> bool:
    for key in ('domain', 'taxonomy'):
        value = str(membership_filter.get(key) or '').strip()
        if value and source.get(key) != value:
            return False
    return True


def _merge_relations(*collections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str, str]] = set()
    for items in collections:
        for relation in items or []:
            key = (
                str(relation.get('security_code') or ''),
                str(relation.get('domain') or ''),
                str(relation.get('taxonomy') or ''),
                str(relation.get('member_code') or ''),
                str(relation.get('effective_from') or ''),
                str(relation.get('effective_to') or ''),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(relation)
    return merged


def _sql_quote(value: str) -> str:
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def preview_source_rows(source: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    return preview_source_rows_with_lookup(source, rows, [])


def preview_source_rows_with_lookup(
    source: dict[str, Any],
    rows: list[dict[str, Any]],
    lookup_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    normalized_source = _normalize_source(source)
    taxonomy_preview = {
        'source_kind': normalized_source['source_kind'],
        'domain': normalized_source['domain'],
        'taxonomy': normalized_source['taxonomy'],
        'display_name': normalized_source['description'] or normalized_source['taxonomy'],
        'status': normalized_source['status'],
    }
    if normalized_source['source_kind'] == 'member_dimension':
        return _preview_member_dimension_source(normalized_source, rows, lookup_rows, taxonomy_preview)
    return _preview_relation_source(normalized_source, rows, lookup_rows, taxonomy_preview)


def membership_workspace_summary(path: str | Path | None = None) -> dict[str, Any]:
    workspace = load_membership_workspace(path)
    return {
        'version': workspace['version'],
        'path': str(workspace['path']),
        'domain_count': len(list_domains(path)),
        'source_count': len(workspace['sources']),
        'taxonomy_count': len(workspace['taxonomies']),
        'member_count': len(workspace['members']),
        'relation_count': len(workspace['relations']),
    }


def _preview_relation_source(
    normalized_source: dict[str, Any],
    rows: list[dict[str, Any]],
    lookup_rows: list[dict[str, Any]],
    taxonomy_preview: dict[str, Any],
) -> dict[str, Any]:
    relation_rows: list[dict[str, Any]] = []
    member_map: dict[tuple[str, str], dict[str, Any]] = {}
    attribute_columns = [
        {
            'key': item['key'],
            'label': item['label'],
        }
        for item in normalized_source['attribute_mappings']
    ]
    lookup_map: dict[str, dict[str, Any]] = {}
    if normalized_source['lookup_table'] and normalized_source['lookup_target_field']:
        for row in lookup_rows:
            if not isinstance(row, dict):
                continue
            target_value = str(row.get(normalized_source['lookup_target_field']) or '').strip()
            if target_value and target_value not in lookup_map:
                lookup_map[target_value] = row

    for row in rows:
        if not isinstance(row, dict):
            continue
        security_code = _pick_value(row, normalized_source, 'security_code')
        member_code = _pick_value(row, normalized_source, 'member_code')
        if not security_code or not member_code:
            continue
        lookup_row = lookup_map.get(member_code)
        member_name = (
            _pick_value(row, normalized_source, 'member_name')
            or _pick_lookup_value(lookup_row, normalized_source, 'lookup_member_name_field')
            or member_code
        )
        effective_from = _pick_value(row, normalized_source, 'effective_from')
        effective_to = _pick_value(row, normalized_source, 'effective_to')
        source_system = _pick_value(row, normalized_source, 'source_system') or normalized_source['source_system_value']
        attribute_values = _extract_attribute_values(lookup_row, normalized_source['attribute_mappings'])
        relation = {
            'security_code': security_code,
            'security_type': normalized_source['security_type'],
            'domain': normalized_source['domain'],
            'taxonomy': normalized_source['taxonomy'],
            'member_code': member_code,
            'member_name': member_name,
            'effective_from': effective_from or '',
            'effective_to': effective_to or '',
            'source_system': source_system,
            'status': normalized_source['status'],
            **attribute_values,
        }
        relation_rows.append(relation)
        member_map.setdefault(
            (normalized_source['taxonomy'], member_code),
            {
                'domain': normalized_source['domain'],
                'taxonomy': normalized_source['taxonomy'],
                'member_code': member_code,
                'member_name': member_name,
                'status': normalized_source['status'],
                **attribute_values,
            },
        )

    return {
        'taxonomy_preview': taxonomy_preview,
        'member_preview': list(member_map.values()),
        'relation_preview': relation_rows,
        'attribute_columns': attribute_columns,
        'preview_count': len(relation_rows),
    }


def _preview_member_dimension_source(
    normalized_source: dict[str, Any],
    rows: list[dict[str, Any]],
    lookup_rows: list[dict[str, Any]],
    taxonomy_preview: dict[str, Any],
) -> dict[str, Any]:
    member_rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    attribute_columns = [
        {
            'key': item['key'],
            'label': item['label'],
        }
        for item in normalized_source['attribute_mappings']
    ]
    lookup_map: dict[str, dict[str, Any]] = {}
    if normalized_source['lookup_table'] and normalized_source['lookup_target_field']:
        for row in lookup_rows:
            if not isinstance(row, dict):
                continue
            target_value = str(row.get(normalized_source['lookup_target_field']) or '').strip()
            if target_value and target_value not in lookup_map:
                lookup_map[target_value] = row

    for row in rows:
        if not isinstance(row, dict):
            continue
        member_code = _pick_value(row, normalized_source, 'member_code')
        if not member_code:
            continue
        lookup_row = lookup_map.get(
            _pick_value(row, normalized_source, 'member_code')
            if normalized_source['lookup_table']
            else ''
        )
        member_name = (
            _pick_value(row, normalized_source, 'member_name')
            or _pick_lookup_value(lookup_row, normalized_source, 'lookup_member_name_field')
            or member_code
        )
        member_key = (normalized_source['taxonomy'], member_code)
        if member_key in seen:
            continue
        seen.add(member_key)
        attribute_values = _extract_attribute_values(
            lookup_row if normalized_source['lookup_table'] else row,
            normalized_source['attribute_mappings'],
        )
        member_rows.append(
            {
                'domain': normalized_source['domain'],
                'taxonomy': normalized_source['taxonomy'],
                'member_code': member_code,
                'member_name': member_name,
                'source_system': _pick_value(row, normalized_source, 'source_system') or normalized_source['source_system_value'],
                'status': normalized_source['status'],
                **attribute_values,
            }
        )

    return {
        'taxonomy_preview': taxonomy_preview,
        'member_preview': member_rows,
        'relation_preview': [],
        'attribute_columns': attribute_columns,
        'preview_count': len(member_rows),
    }


def _default_workspace(path: Path) -> dict[str, Any]:
    return {
        'version': 1,
        'path': path,
        'sources': [],
        'taxonomies': [],
        'members': [],
        'relations': [],
    }


def _normalize_workspace(data: dict[str, Any], path: Path) -> dict[str, Any]:
    workspace = _default_workspace(path)
    workspace['version'] = int(data.get('version') or 1)
    workspace['sources'] = [
        _normalize_source(item)
        for item in list(data.get('sources') or [])
        if isinstance(item, dict)
    ]
    workspace['taxonomies'] = [
        _normalize_taxonomy(item)
        for item in list(data.get('taxonomies') or [])
        if isinstance(item, dict)
    ]
    workspace['members'] = [
        _normalize_member(item)
        for item in list(data.get('members') or [])
        if isinstance(item, dict)
    ]
    workspace['relations'] = [
        _normalize_relation(item, workspace['members'])
        for item in list(data.get('relations') or [])
        if isinstance(item, dict)
    ]
    return workspace


def _normalize_source(payload: dict[str, Any]) -> dict[str, Any]:
    source_name = str(payload.get('source_name') or '').strip()
    database = str(payload.get('database') or '').strip()
    table = str(payload.get('table') or '').strip()
    domain = str(payload.get('domain') or '').strip()
    taxonomy = str(payload.get('taxonomy') or '').strip()
    if not source_name or not database or not table or not domain or not taxonomy:
        raise ValueError('source requires source_name, database, table, domain, and taxonomy')

    source_kind = str(payload.get('source_kind') or 'relation').strip() or 'relation'
    if source_kind not in {'relation', 'member_dimension'}:
        raise ValueError('source_kind must be relation or member_dimension')
    mapping = payload.get('mapping') if isinstance(payload.get('mapping'), dict) else {}
    return {
        'id': str(payload.get('id') or f'source::{source_name}'),
        'source_name': source_name,
        'source_kind': source_kind,
        'database': database,
        'table': table,
        'domain': domain,
        'taxonomy': taxonomy,
        'security_type': str(payload.get('security_type') or 'stock').strip() or 'stock',
        'status': str(payload.get('status') or 'enabled').strip() or 'enabled',
        'description': str(payload.get('description') or '').strip(),
        'lookup_database': str(payload.get('lookup_database') or '').strip(),
        'lookup_table': str(payload.get('lookup_table') or '').strip(),
        'lookup_source_field': str(payload.get('lookup_source_field') or '').strip(),
        'lookup_target_field': str(payload.get('lookup_target_field') or '').strip(),
        'lookup_member_name_field': str(payload.get('lookup_member_name_field') or '').strip(),
        'member_code_field': str(payload.get('member_code_field') or '').strip(),
        'member_name_field': str(payload.get('member_name_field') or '').strip(),
        'security_code_field': str(payload.get('security_code_field') or '').strip(),
        'effective_from_field': str(payload.get('effective_from_field') or '').strip(),
        'effective_to_field': str(payload.get('effective_to_field') or '').strip(),
        'source_system_field': str(payload.get('source_system_field') or '').strip(),
        'member_code_value': str(payload.get('member_code_value') or '').strip(),
        'member_name_value': str(payload.get('member_name_value') or '').strip(),
        'source_system_value': str(payload.get('source_system_value') or 'source_mapping').strip() or 'source_mapping',
        'attribute_mappings': _normalize_attribute_mappings(payload.get('attribute_mappings')),
        'mapping': {
            'member_code': str(mapping.get('member_code') or payload.get('member_code_field') or '').strip(),
            'member_name': str(mapping.get('member_name') or payload.get('member_name_field') or '').strip(),
            'security_code': str(mapping.get('security_code') or payload.get('security_code_field') or '').strip(),
            'effective_from': str(mapping.get('effective_from') or payload.get('effective_from_field') or '').strip(),
            'effective_to': str(mapping.get('effective_to') or payload.get('effective_to_field') or '').strip(),
            'source_system': str(mapping.get('source_system') or payload.get('source_system_field') or '').strip(),
        },
        'updated_at': str(payload.get('updated_at') or _now_iso()),
        'extra_payload': payload.get('extra_payload') if isinstance(payload.get('extra_payload'), dict) else {},
    }


def _normalize_taxonomy(payload: dict[str, Any]) -> dict[str, Any]:
    domain = str(payload.get('domain') or '').strip()
    taxonomy = str(payload.get('taxonomy') or '').strip()
    if not domain or not taxonomy:
        raise ValueError('taxonomy requires domain and taxonomy')
    return {
        'id': str(payload.get('id') or f'{domain}::{taxonomy}'),
        'domain': domain,
        'taxonomy': taxonomy,
        'display_name': str(payload.get('display_name') or taxonomy).strip(),
        'description': str(payload.get('description') or '').strip(),
        'status': str(payload.get('status') or 'enabled').strip() or 'enabled',
        'updated_at': str(payload.get('updated_at') or _now_iso()),
        'extra_payload': payload.get('extra_payload') if isinstance(payload.get('extra_payload'), dict) else {},
    }


def _normalize_member(payload: dict[str, Any]) -> dict[str, Any]:
    domain = str(payload.get('domain') or '').strip()
    taxonomy = str(payload.get('taxonomy') or '').strip()
    member_code = str(payload.get('member_code') or '').strip()
    if not domain or not taxonomy or not member_code:
        raise ValueError('member requires domain, taxonomy, and member_code')
    return {
        'id': str(payload.get('id') or f'{domain}::{taxonomy}::{member_code}'),
        'domain': domain,
        'taxonomy': taxonomy,
        'member_code': member_code,
        'member_name': str(payload.get('member_name') or member_code).strip(),
        'sort_order': int(payload.get('sort_order') or 0),
        'status': str(payload.get('status') or 'enabled').strip() or 'enabled',
        'updated_at': str(payload.get('updated_at') or _now_iso()),
        'extra_payload': payload.get('extra_payload') if isinstance(payload.get('extra_payload'), dict) else {},
    }


def _normalize_relation(payload: dict[str, Any], members: list[dict[str, Any]]) -> dict[str, Any]:
    security_code = str(payload.get('security_code') or '').strip()
    domain = str(payload.get('domain') or '').strip()
    taxonomy = str(payload.get('taxonomy') or '').strip()
    member_code = str(payload.get('member_code') or '').strip()
    if not security_code or not domain or not taxonomy or not member_code:
        raise ValueError('relation requires security_code, domain, taxonomy, and member_code')

    member_name = str(payload.get('member_name') or '').strip()
    if not member_name:
        matched_member = next(
            (
                item
                for item in members
                if item['domain'] == domain and item['taxonomy'] == taxonomy and item['member_code'] == member_code
            ),
            None,
        )
        member_name = matched_member['member_name'] if matched_member else member_code

    effective_from = str(payload.get('effective_from') or '').strip()
    effective_to = str(payload.get('effective_to') or '').strip()
    relation_id = str(payload.get('id') or '').strip()
    if not relation_id:
        relation_id = f'rel::{security_code}::{domain}::{taxonomy}::{member_code}::{effective_from or "start"}::{uuid4().hex[:8]}'

    return {
        'id': relation_id,
        'security_code': security_code,
        'security_type': str(payload.get('security_type') or 'stock').strip() or 'stock',
        'domain': domain,
        'taxonomy': taxonomy,
        'member_code': member_code,
        'member_name': member_name,
        'effective_from': effective_from or '',
        'effective_to': effective_to or '',
        'source_system': str(payload.get('source_system') or 'manual').strip() or 'manual',
        'status': str(payload.get('status') or 'enabled').strip() or 'enabled',
        'updated_at': str(payload.get('updated_at') or _now_iso()),
        'extra_payload': payload.get('extra_payload') if isinstance(payload.get('extra_payload'), dict) else {},
    }


def _upsert_into_list(items: list[dict[str, Any]], payload: dict[str, Any], *, key) -> None:
    item_key = key(payload)
    index = next((idx for idx, current in enumerate(items) if key(current) == item_key), None)
    if index is None:
        items.append(payload)
    else:
        items[index] = {**items[index], **payload}


def _relation_matches_filter(relation: dict[str, Any], membership_filter: dict[str, Any]) -> bool:
    for key in ('domain', 'taxonomy', 'member_code'):
        value = str(membership_filter.get(key) or '').strip()
        if value and relation.get(key) != value:
            return False
    return True


def _is_relation_active(relation: dict[str, Any], as_of_date: str) -> bool:
    if relation.get('status') != 'enabled':
        return False
    if relation.get('effective_from') and relation['effective_from'] > as_of_date:
        return False
    if relation.get('effective_to') and relation['effective_to'] < as_of_date:
        return False
    return True


def _pick_value(row: dict[str, Any], source: dict[str, Any], key: str) -> str:
    mapping = source.get('mapping') or {}
    field_name = str(mapping.get(key) or '').strip()
    if field_name:
        value = row.get(field_name)
        if value is None:
            return ''
        return str(value).strip()
    direct_key = f'{key}_value'
    value = source.get(direct_key)
    if value is None:
        return ''
    return str(value).strip()


def _pick_lookup_value(row: dict[str, Any] | None, source: dict[str, Any], field_key: str) -> str:
    if not row or not isinstance(row, dict):
        return ''
    field_name = str(source.get(field_key) or '').strip()
    if not field_name:
        return ''
    value = row.get(field_name)
    if value is None:
        return ''
    return str(value).strip()


def _normalize_attribute_mappings(payload: Any) -> list[dict[str, str]]:
    items = []
    for index, item in enumerate(payload or []):
        if not isinstance(item, dict):
            continue
        field = str(item.get('field') or '').strip()
        if not field:
            continue
        label = str(item.get('label') or field).strip()
        key = str(item.get('key') or f'attr_{index + 1}').strip()
        items.append(
            {
                'key': key,
                'label': label,
                'field': field,
            }
        )
    return items


def _extract_attribute_values(row: dict[str, Any] | None, attribute_mappings: list[dict[str, str]]) -> dict[str, str]:
    if not row or not isinstance(row, dict):
        return {item['key']: '' for item in attribute_mappings}
    values: dict[str, str] = {}
    for item in attribute_mappings:
        value = row.get(item['field'])
        values[item['key']] = '' if value is None else str(value).strip()
    return values


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()
