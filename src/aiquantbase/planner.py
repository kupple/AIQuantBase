from __future__ import annotations

from collections import deque

from .models import Edge, FieldCatalogEntry, FilterCondition, FilterGroup, Node, OrderBy, PlanStep, QueryIntent, QueryPlan


class GraphRegistry:
    def __init__(self, nodes: list[Node], edges: list[Edge], field_catalog: list[FieldCatalogEntry] | None = None) -> None:
        self.nodes = {node.name: node for node in nodes}
        self.edges = edges
        self.field_catalog = field_catalog or []
        self.field_to_node: dict[str, str] = {}
        for node in nodes:
            for field in node.fields:
                self.field_to_node.setdefault(field, node.name)
        self.standard_field_map: dict[str, list[FieldCatalogEntry]] = {}
        for entry in self.field_catalog:
            self.standard_field_map.setdefault(entry.standard_field, []).append(entry)
        self._validate()

    def _validate(self) -> None:
        if not self.nodes:
            raise ValueError("At least one node is required")
        for edge in self.edges:
            edge.validate()
            if edge.from_node not in self.nodes:
                raise ValueError(f"Unknown edge from node: {edge.from_node}")
            if edge.to_node not in self.nodes:
                raise ValueError(f"Unknown edge to node: {edge.to_node}")
        self._validate_field_collisions()

    def _validate_field_collisions(self) -> None:
        field_counts: dict[str, int] = {}
        for node in self.nodes.values():
            for field in node.fields:
                field_counts[field] = field_counts.get(field, 0) + 1
        self.ambiguous_fields = {field for field, count in field_counts.items() if count > 1}

    def resolve_field_node(self, field_name: str, base_node: str) -> str:
        node_name, bare_field = self.split_field_reference(field_name)
        if node_name:
            if node_name not in self.nodes:
                raise ValueError(f"Unknown node in field reference '{field_name}'")
            if bare_field not in self.nodes[node_name].fields:
                raise ValueError(f"Field '{bare_field}' not found on node '{node_name}'")
            return node_name
        field_name = bare_field
        if field_name in self.nodes[base_node].fields:
            return base_node
        catalog_entry = self._resolve_catalog_entry(field_name, base_node)
        if catalog_entry:
            if catalog_entry.resolver_type == "derived":
                return base_node
            return catalog_entry.source_node
        if field_name in self.ambiguous_fields:
            raise ValueError(
                f"Field '{field_name}' exists on multiple nodes and must be disambiguated in a future protocol revision"
            )
        if field_name not in self.field_to_node:
            raise ValueError(f"Field not found in graph registry: {field_name}")
        return self.field_to_node[field_name]

    def resolve_physical_field(self, field_name: str) -> str:
        node_name, bare_field = self.split_field_reference(field_name)
        if node_name:
            return bare_field
        catalog_entry = self._resolve_catalog_entry(field_name, None)
        if catalog_entry:
            if catalog_entry.resolver_type == "derived":
                return field_name
            return catalog_entry.source_field
        return bare_field

    def split_field_reference(self, field_name: str) -> tuple[str | None, str]:
        if "." not in field_name:
            return None, field_name
        node_name, bare_field = field_name.split(".", 1)
        return node_name, bare_field

    def _resolve_catalog_entry(self, standard_field: str, base_node: str | None) -> FieldCatalogEntry | None:
        entries = self.standard_field_map.get(standard_field, [])
        if not entries:
            return None
        if len(entries) == 1:
            return entries[0]
        candidates = list(entries)
        if base_node:
            base_grain = self.nodes[base_node].grain
            if base_grain:
                grain_matches = [entry for entry in candidates if entry.applies_to_grain == base_grain]
                if len(grain_matches) == 1:
                    return grain_matches[0]
                if grain_matches:
                    candidates = grain_matches
            reachable = [entry for entry in candidates if self._is_reachable(base_node, entry.source_node)]
            if len(reachable) == 1:
                return reachable[0]
            if reachable:
                candidates = reachable
            same_node = [entry for entry in candidates if entry.source_node == base_node]
            if len(same_node) == 1:
                return same_node[0]
        raise ValueError(
            f"Standard field '{standard_field}' maps to multiple source fields and could not be resolved uniquely"
        )

    def resolve_catalog_entry(self, standard_field: str, base_node: str | None) -> FieldCatalogEntry | None:
        return self._resolve_catalog_entry(standard_field, base_node)

    def _is_reachable(self, from_node: str, to_node: str) -> bool:
        try:
            self.find_path(from_node, to_node)
        except ValueError:
            return False
        return True

    def find_path(self, from_node: str, to_node: str) -> list[Edge]:
        if from_node == to_node:
            return []
        queue: deque[tuple[str, list[Edge]]] = deque([(from_node, [])])
        visited = {from_node}
        adjacency = self._build_adjacency()

        while queue:
            current, path = queue.popleft()
            for edge in adjacency.get(current, []):
                next_node = edge.to_node
                if next_node in visited:
                    continue
                next_path = path + [edge]
                if next_node == to_node:
                    return next_path
                visited.add(next_node)
                queue.append((next_node, next_path))

        raise ValueError(f"No relation path from {from_node} to {to_node}")

    def _build_adjacency(self) -> dict[str, list[Edge]]:
        adjacency: dict[str, list[Edge]] = {}
        for edge in sorted(self.edges, key=lambda item: item.priority):
            adjacency.setdefault(edge.from_node, []).append(edge)
        return adjacency


class QueryPlanner:
    def __init__(self, registry: GraphRegistry) -> None:
        self.registry = registry

    def plan(self, intent: QueryIntent) -> QueryPlan:
        if intent.from_node not in self.registry.nodes:
            raise ValueError(f"Unknown base node: {intent.from_node}")
        if not intent.select and not intent.aggregations:
            raise ValueError("Query Intent must contain at least one field in select or aggregations")

        aggregation_aliases = {
            item.alias or f"{item.func.lower()}_{self.registry.resolve_physical_field(item.field)}"
            for item in intent.aggregations
        }
        referenced_fields = [item.field for item in intent.select]
        referenced_fields.extend(intent.group_by)
        referenced_fields.extend(item.field for item in intent.aggregations)
        referenced_fields.extend(self._fields_from_filters(intent.where))
        referenced_fields.extend(self._fields_from_filters(intent.having, excluded=aggregation_aliases))
        referenced_fields.extend(self._fields_from_order(intent.order_by))
        if intent.time_range:
            referenced_fields.append(intent.time_range.field)

        field_bindings: dict[str, str] = {}
        resolved_fields: dict[str, str] = {}
        derived_fields: dict[str, dict[str, object]] = {}
        all_steps: list[PlanStep] = []
        seen_edges: set[str] = set()

        for field_name in referenced_fields:
            self._collect_field_requirements(
                field_name,
                intent.from_node,
                field_bindings,
                resolved_fields,
                derived_fields,
                all_steps,
                seen_edges,
            )

        return QueryPlan(
            base_node=intent.from_node,
            select_fields=list(intent.select),
            aggregations=list(intent.aggregations),
            group_by=list(intent.group_by),
            field_bindings=field_bindings,
            resolved_fields=resolved_fields,
            derived_fields=derived_fields,
            steps=all_steps,
            where=intent.where,
            having=intent.having,
            order_by=intent.order_by,
            time_range=intent.time_range,
            limit=intent.limit,
            offset=intent.offset,
            safety=intent.safety,
        )

    def _collect_field_requirements(
        self,
        field_name: str,
        base_node: str,
        field_bindings: dict[str, str],
        resolved_fields: dict[str, str],
        derived_fields: dict[str, dict[str, object]],
        all_steps: list[PlanStep],
        seen_edges: set[str],
    ) -> None:
        if field_name in field_bindings:
            return

        catalog_entry = self.registry.resolve_catalog_entry(field_name, base_node)
        if catalog_entry and catalog_entry.resolver_type == "derived":
            if not catalog_entry.formula:
                raise ValueError(f"Derived field '{field_name}' requires formula")
            for dependency in catalog_entry.depends_on:
                self._collect_field_requirements(
                    dependency,
                    base_node,
                    field_bindings,
                    resolved_fields,
                    derived_fields,
                    all_steps,
                    seen_edges,
                )
            field_bindings[field_name] = base_node
            resolved_fields[field_name] = field_name
            derived_fields[field_name] = {
                "depends_on": list(catalog_entry.depends_on),
                "formula": catalog_entry.formula,
            }
            return

        node_name = self.registry.resolve_field_node(field_name, base_node)
        field_bindings[field_name] = node_name
        resolved_fields[field_name] = self.registry.resolve_physical_field(field_name)
        for edge in self.registry.find_path(base_node, node_name):
            if edge.name in seen_edges:
                continue
            all_steps.append(
                PlanStep(
                    edge_name=edge.name,
                    from_node=edge.from_node,
                    to_node=edge.to_node,
                    relation_type=edge.relation_type,
                    join_keys=edge.join_keys,
                    time_binding=edge.time_binding,
                    bridge_steps=edge.bridge_steps,
                )
            )
            seen_edges.add(edge.name)

    def _fields_from_filters(self, filters: FilterGroup, excluded: set[str] | None = None) -> list[str]:
        excluded = excluded or set()
        result: list[str] = []
        for item in filters.items:
            if isinstance(item, FilterCondition):
                if item.field not in excluded:
                    result.append(item.field)
            else:
                result.extend(self._fields_from_filters(item, excluded=excluded))
        return result

    def _fields_from_order(self, items: list[OrderBy]) -> list[str]:
        return [item.field for item in items]
