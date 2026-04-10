from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

import yaml

from .models import (
    Aggregation,
    BridgeStep,
    Edge,
    FieldCatalogEntry,
    FilterCondition,
    FilterExpression,
    FilterGroup,
    Node,
    OrderBy,
    QueryIntent,
    SafetyOptions,
    SelectField,
    TimeBinding,
    TimeRange,
)


def load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML root must be a mapping: {path}")
    return data


def dump_yaml(data: dict[str, Any]) -> str:
    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)


def load_nodes_and_edges(path: str | Path) -> tuple[list[Node], list[Edge]]:
    data = load_yaml(path)
    nodes = [_parse_node(item) for item in data.get("nodes", [])]
    edges = [_parse_edge(item) for item in data.get("edges", [])]
    return nodes, edges


def load_field_catalog(path: str | Path) -> list[FieldCatalogEntry]:
    data = load_yaml(path)
    return [FieldCatalogEntry(**item) for item in data.get("fields", [])]


def load_query_intent(path: str | Path) -> QueryIntent:
    data = load_yaml(path)
    if "from" not in data:
        raise ValueError("Query Intent must contain 'from'")
    page = data.get("page")
    page_size = data.get("page_size")
    limit = data.get("limit")
    offset = data.get("offset")
    if page is not None or page_size is not None:
        if page is None or page_size is None:
            raise ValueError("Query Intent requires both 'page' and 'page_size' when using product pagination")
        if page < 1 or page_size < 1:
            raise ValueError("'page' and 'page_size' must be >= 1")
        expected_limit = page_size
        expected_offset = (page - 1) * page_size
        if limit is not None and limit != expected_limit:
            raise ValueError(
                f"Pagination conflict: page/page_size implies limit={expected_limit}, got limit={limit}"
            )
        if offset is not None and offset != expected_offset:
            raise ValueError(
                f"Pagination conflict: page/page_size implies offset={expected_offset}, got offset={offset}"
            )
        limit = expected_limit
        offset = expected_offset
    elif limit is not None and limit < 1:
        raise ValueError("'limit' must be >= 1")
    elif offset is not None and offset < 0:
        raise ValueError("'offset' must be >= 0")
    return QueryIntent(
        from_node=data["from"],
        select=[_parse_select_item(item) for item in data.get("select", [])],
        aggregations=[Aggregation(**item) for item in data.get("aggregations", [])],
        group_by=list(data.get("group_by", [])),
        where=_parse_where(data.get("where")),
        having=_parse_where(data.get("having")),
        order_by=[OrderBy(**item) for item in data.get("order_by", [])],
        time_range=TimeRange(**data["time_range"]) if data.get("time_range") else None,
        page=page,
        page_size=page_size,
        limit=limit,
        offset=offset,
        safety=SafetyOptions(**data.get("safety", {})),
    )


def intent_to_dict(intent: QueryIntent) -> dict[str, Any]:
    data = asdict(intent)
    data["from"] = data.pop("from_node")
    return data


def _parse_select_item(item: Any) -> SelectField:
    if isinstance(item, str):
        return SelectField(field=item)
    if isinstance(item, dict):
        return SelectField(**item)
    raise ValueError(f"Unsupported select item: {item!r}")


def _parse_where(data: Any) -> FilterGroup:
    if not data:
        return FilterGroup()
    if isinstance(data, list):
        return FilterGroup(mode="and", items=[_parse_filter_expression(item) for item in data])
    if isinstance(data, dict):
        mode = data.get("mode", "and")
        items = [_parse_filter_expression(item) for item in data.get("items", [])]
        return FilterGroup(mode=mode, items=items)
    raise ValueError(f"Unsupported where clause: {data!r}")


def _parse_filter_expression(data: Any) -> FilterExpression:
    if not isinstance(data, dict):
        raise ValueError(f"Unsupported filter expression: {data!r}")
    if "items" in data:
        return FilterGroup(
            mode=data.get("mode", "and"),
            items=[_parse_filter_expression(item) for item in data.get("items", [])],
        )
    return FilterCondition(**data)


def _parse_node(item: dict[str, Any]) -> Node:
    return Node(
        name=item["name"],
        table=item["table"],
        entity_keys=list(item.get("entity_keys", [])),
        time_key=item.get("time_key"),
        grain=item.get("grain"),
        fields=list(item.get("fields", [])),
        description=item.get("description"),
    )


def _parse_edge(item: dict[str, Any]) -> Edge:
    binding = item.get("time_binding")
    bridge_steps = []
    for step in item.get("bridge_steps", []):
        step_binding = step.get("time_binding")
        bridge_steps.append(
            BridgeStep(
                table=step["table"],
                join_keys=list(step.get("join_keys", [])),
                time_binding=TimeBinding(**step_binding) if step_binding else None,
            )
        )
    return Edge(
        name=item["name"],
        from_node=item["from"],
        to_node=item["to"],
        relation_type=item["relation_type"],
        source_table=item.get("source_table"),
        join_keys=list(item.get("join_keys", [])),
        time_binding=TimeBinding(**binding) if binding else None,
        bridge_steps=bridge_steps,
        priority=item.get("priority", 100),
        description=item.get("description"),
    )
