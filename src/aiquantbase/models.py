from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


VALID_BIND_MODES = {
    "same_date",
    "same_timestamp",
    "asof",
    "effective_range",
}

VALID_RELATION_TYPES = {
    "direct",
    "bridge",
}


@dataclass(slots=True)
class Node:
    name: str
    table: str
    entity_keys: list[str]
    time_key: str | None = None
    grain: str | None = None
    fields: list[str] = field(default_factory=list)
    description: str | None = None


@dataclass(slots=True)
class TimeBinding:
    mode: str
    base_time_field: str | None = None
    source_time_field: str | None = None
    source_start_field: str | None = None
    source_end_field: str | None = None
    base_time_cast: str | None = None
    source_time_cast: str | None = None
    available_time_field: str | None = None
    allow_equal: bool = True
    lookahead_safe: bool = True

    def validate(self) -> None:
        if self.mode not in VALID_BIND_MODES:
            raise ValueError(f"Unsupported bind mode: {self.mode}")
        if self.mode in {"same_date", "same_timestamp", "asof"} and not self.source_time_field:
            raise ValueError(f"bind mode {self.mode} requires source_time_field")
        if self.mode == "effective_range":
            if not self.source_start_field or not self.source_end_field:
                raise ValueError("effective_range requires source_start_field and source_end_field")


@dataclass(slots=True)
class BridgeStep:
    table: str
    join_keys: list[dict[str, str]] = field(default_factory=list)
    time_binding: TimeBinding | None = None

    def validate(self) -> None:
        if not self.table:
            raise ValueError("BridgeStep requires table")
        if not self.join_keys:
            raise ValueError("BridgeStep requires join_keys")
        if self.time_binding:
            self.time_binding.validate()


@dataclass(slots=True)
class Edge:
    name: str
    from_node: str
    to_node: str
    relation_type: str
    source_table: str | None = None
    join_keys: list[dict[str, str]] = field(default_factory=list)
    time_binding: TimeBinding | None = None
    bridge_steps: list[BridgeStep] = field(default_factory=list)
    priority: int = 100
    description: str | None = None

    def validate(self) -> None:
        if self.relation_type not in VALID_RELATION_TYPES:
            raise ValueError(f"Unsupported relation type: {self.relation_type}")
        if not self.join_keys and not self.time_binding:
            raise ValueError(f"Edge {self.name} must define join_keys")
        if self.time_binding:
            self.time_binding.validate()
        if self.relation_type == "bridge":
            if not self.bridge_steps:
                raise ValueError(f"Bridge edge {self.name} must define bridge_steps")
            for step in self.bridge_steps:
                step.validate()


@dataclass(slots=True)
class FilterCondition:
    field: str
    op: str
    value: Any


@dataclass(slots=True)
class FilterGroup:
    mode: str = "and"
    items: list["FilterExpression"] = field(default_factory=list)


FilterExpression = FilterCondition | FilterGroup


@dataclass(slots=True)
class OrderBy:
    field: str
    direction: str = "asc"


@dataclass(slots=True)
class SelectField:
    field: str
    alias: str | None = None


@dataclass(slots=True)
class Aggregation:
    field: str
    func: str
    alias: str | None = None


@dataclass(slots=True)
class SafetyOptions:
    lookahead_safe: bool = True
    strict_mode: bool = True


@dataclass(slots=True)
class TimeRange:
    field: str
    start: str
    end: str


@dataclass(slots=True)
class QueryIntent:
    from_node: str
    select: list[SelectField]
    aggregations: list[Aggregation] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    where: FilterGroup = field(default_factory=FilterGroup)
    having: FilterGroup = field(default_factory=FilterGroup)
    order_by: list[OrderBy] = field(default_factory=list)
    time_range: TimeRange | None = None
    page: int | None = None
    page_size: int | None = None
    limit: int | None = None
    offset: int | None = None
    safety: SafetyOptions = field(default_factory=SafetyOptions)


@dataclass(slots=True)
class PlanStep:
    edge_name: str
    from_node: str
    to_node: str
    relation_type: str
    join_keys: list[dict[str, str]]
    time_binding: TimeBinding | None = None
    bridge_steps: list[BridgeStep] = field(default_factory=list)


@dataclass(slots=True)
class QueryPlan:
    base_node: str
    select_fields: list[SelectField]
    aggregations: list[Aggregation]
    group_by: list[str]
    field_bindings: dict[str, str]
    resolved_fields: dict[str, str]
    derived_fields: dict[str, dict[str, Any]]
    steps: list[PlanStep]
    where: FilterGroup = field(default_factory=FilterGroup)
    having: FilterGroup = field(default_factory=FilterGroup)
    order_by: list[OrderBy] = field(default_factory=list)
    time_range: TimeRange | None = None
    limit: int | None = None
    offset: int | None = None
    safety: SafetyOptions = field(default_factory=SafetyOptions)


@dataclass(slots=True)
class FieldCatalogEntry:
    standard_field: str
    source_node: str | None
    source_field: str | None
    field_role: str
    resolver_type: str = "direct"
    depends_on: list[str] = field(default_factory=list)
    formula: str | None = None
    applies_to_grain: str | None = None
    notes: list[str] = field(default_factory=list)
