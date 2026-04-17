from __future__ import annotations

from collections import deque
import json

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

    def has_direct_source_table_binding(self, entry: FieldCatalogEntry) -> bool:
        return bool(
            entry.source_table
            and entry.binding_mode == "source_table"
            and (entry.join_keys or entry.time_binding or entry.bridge_steps)
        )

    def resolve_source_node_for_entry(
        self,
        entry: FieldCatalogEntry,
        base_node: str | None,
        selected_nodes: set[str] | None = None,
    ) -> str:
        if entry.source_node:
            return entry.source_node
        if entry.source_table:
            candidates = [node.name for node in self.nodes.values() if node.table == entry.source_table]
            if not candidates:
                raise ValueError(f"No node found for source_table '{entry.source_table}'")
            if base_node and self.nodes[base_node].table == entry.source_table:
                return base_node
            if base_node:
                reachable = [name for name in candidates if self._is_reachable(base_node, name)]
                if len(reachable) == 1:
                    return reachable[0]
                if len(reachable) > 1 and selected_nodes:
                    selected_match = [name for name in reachable if name in selected_nodes]
                    if len(selected_match) == 1:
                        return selected_match[0]
            if len(candidates) == 1:
                return candidates[0]
            raise ValueError(
                f"Source table '{entry.source_table}' maps to multiple nodes and could not be resolved uniquely"
            )
        raise ValueError(f"Field '{entry.standard_field}' has neither source_node nor source_table")

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

    def resolve_field_node(
        self,
        field_name: str,
        base_node: str,
        selected_path_groups: dict[str, str] | None = None,
        selected_nodes: set[str] | None = None,
    ) -> str:
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
        catalog_entry = self._resolve_catalog_entry(
            field_name,
            base_node,
            selected_path_groups=selected_path_groups,
            selected_nodes=selected_nodes,
        )
        if catalog_entry:
            if catalog_entry.resolver_type == "derived":
                return base_node
            if self.has_direct_source_table_binding(catalog_entry):
                return base_node
            return self.resolve_source_node_for_entry(
                catalog_entry,
                base_node,
                selected_nodes=selected_nodes,
            )
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

    def _resolve_catalog_entry(
        self,
        standard_field: str,
        base_node: str | None,
        selected_path_groups: dict[str, str] | None = None,
        selected_nodes: set[str] | None = None,
    ) -> FieldCatalogEntry | None:
        entries = self.standard_field_map.get(standard_field, [])
        if not entries:
            return None
        if len(entries) == 1:
            entry = entries[0]
            if not base_node or not entry.base_node or entry.base_node == base_node:
                return entry
        candidates = list(entries)
        selected_path_groups = selected_path_groups or {}
        selected_nodes = selected_nodes or set()
        if base_node:
            exact_base_matches = [entry for entry in candidates if entry.base_node == base_node]
            if len(exact_base_matches) == 1:
                return exact_base_matches[0]
            if exact_base_matches:
                candidates = exact_base_matches
            base_grain = self.nodes[base_node].grain
            if base_grain:
                grain_matches = [entry for entry in candidates if entry.applies_to_grain == base_grain]
                if len(grain_matches) == 1:
                    return grain_matches[0]
                if grain_matches:
                    candidates = grain_matches
            domain_matched = []
            for entry in candidates:
                if not entry.path_domain:
                    continue
                current_group = selected_path_groups.get(entry.path_domain)
                if current_group and entry.path_group == current_group:
                    domain_matched.append(entry)
            if len(domain_matched) == 1:
                return domain_matched[0]
            if domain_matched:
                candidates = domain_matched
            via_matches = [entry for entry in candidates if entry.via_node and entry.via_node in selected_nodes]
            if len(via_matches) == 1:
                return via_matches[0]
            if via_matches:
                candidates = via_matches
            reachable = []
            for entry in candidates:
                try:
                    source_node_name = self.resolve_source_node_for_entry(
                        entry,
                        base_node,
                        selected_nodes=selected_nodes,
                    )
                except ValueError:
                    continue
                if self._is_reachable(base_node, source_node_name):
                    reachable.append(entry)
            if len(reachable) == 1:
                return reachable[0]
            if reachable:
                candidates = reachable
            same_node = []
            for entry in candidates:
                try:
                    source_node_name = self.resolve_source_node_for_entry(
                        entry,
                        base_node,
                        selected_nodes=selected_nodes,
                    )
                except ValueError:
                    continue
                if source_node_name == base_node:
                    same_node.append(entry)
            if len(same_node) == 1:
                return same_node[0]
        raise ValueError(
            f"Standard field '{standard_field}' maps to multiple source fields and could not be resolved uniquely"
        )

    def resolve_catalog_entry(
        self,
        standard_field: str,
        base_node: str | None,
        selected_path_groups: dict[str, str] | None = None,
        selected_nodes: set[str] | None = None,
    ) -> FieldCatalogEntry | None:
        return self._resolve_catalog_entry(
            standard_field,
            base_node,
            selected_path_groups=selected_path_groups,
            selected_nodes=selected_nodes,
        )

    def _is_reachable(self, from_node: str, to_node: str) -> bool:
        try:
            self.find_path(from_node, to_node)
        except ValueError:
            return False
        return True

    def find_path_via(self, from_node: str, to_node: str, via_node: str) -> list[Edge]:
        if via_node == from_node:
            return self.find_path(from_node, to_node)
        if via_node == to_node:
            return self.find_path(from_node, to_node)
        return self.find_path(from_node, via_node) + self.find_path(via_node, to_node)

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
        step_by_edge: dict[str, PlanStep] = {}
        selected_path_groups: dict[str, str] = {}
        selected_nodes: set[str] = {intent.from_node}

        for field_name in referenced_fields:
            self._collect_field_requirements(
                field_name,
                intent.from_node,
                field_bindings,
                resolved_fields,
                derived_fields,
                all_steps,
                step_by_edge,
                selected_path_groups,
                selected_nodes,
                intent.safety.lookahead_safe,
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
        step_by_edge: dict[str, PlanStep],
        selected_path_groups: dict[str, str],
        selected_nodes: set[str],
        lookahead_safe: bool,
    ) -> None:
        if field_name in field_bindings:
            return

        node_name_hint, bare_field = self.registry.split_field_reference(field_name)
        if node_name_hint is None and bare_field in self.registry.nodes[base_node].fields:
            field_bindings[field_name] = base_node
            resolved_fields[field_name] = bare_field
            selected_nodes.add(base_node)
            return

        catalog_entry = self.registry.resolve_catalog_entry(
            field_name,
            base_node,
            selected_path_groups=selected_path_groups,
            selected_nodes=selected_nodes,
        )
        if catalog_entry and catalog_entry.resolver_type == "derived":
            if not catalog_entry.formula:
                raise ValueError(f"Derived field '{field_name}' requires formula")
            self._register_path_group(catalog_entry, selected_path_groups)
            for dependency in catalog_entry.depends_on:
                self._collect_field_requirements(
                    dependency,
                    base_node,
                    field_bindings,
                    resolved_fields,
                    derived_fields,
                    all_steps,
                    step_by_edge,
                    selected_path_groups,
                    selected_nodes,
                    lookahead_safe,
                )
            field_bindings[field_name] = base_node
            resolved_fields[field_name] = field_name
            derived_fields[field_name] = {
                "depends_on": list(catalog_entry.depends_on),
                "formula": catalog_entry.formula,
            }
            return

        if catalog_entry:
            self._register_path_group(catalog_entry, selected_path_groups)
            if catalog_entry.via_node:
                selected_nodes.add(catalog_entry.via_node)
        strict_event_visibility = bool(
            lookahead_safe and catalog_entry and catalog_entry.lookahead_category == "published_event"
        )
        if catalog_entry and self._can_use_direct_source_table_binding(catalog_entry, base_node):
            self._collect_direct_source_table_requirement(
                field_name=field_name,
                base_node=base_node,
                catalog_entry=catalog_entry,
                field_bindings=field_bindings,
                resolved_fields=resolved_fields,
                all_steps=all_steps,
                step_by_edge=step_by_edge,
                selected_nodes=selected_nodes,
                strict_event_visibility=strict_event_visibility,
            )
            return
        node_name = self.registry.resolve_field_node(
            field_name,
            base_node,
            selected_path_groups=selected_path_groups,
            selected_nodes=selected_nodes,
        )
        field_bindings[field_name] = node_name
        if catalog_entry and catalog_entry.source_field:
            resolved_fields[field_name] = catalog_entry.source_field
        else:
            resolved_fields[field_name] = self.registry.resolve_physical_field(field_name)
        selected_nodes.add(node_name)
        path_edges = self.registry.find_path(base_node, node_name)
        if catalog_entry and catalog_entry.via_node and catalog_entry.via_node not in {base_node, node_name}:
            path_edges = self.registry.find_path_via(base_node, node_name, catalog_entry.via_node)
        for edge in path_edges:
            if edge.name in step_by_edge:
                if strict_event_visibility:
                    step_by_edge[edge.name].lookahead_safe_for_event = True
                continue
            step = PlanStep(
                edge_name=edge.name,
                from_node=edge.from_node,
                to_node=edge.to_node,
                relation_type=edge.relation_type,
                join_keys=edge.join_keys,
                time_binding=edge.time_binding,
                bridge_steps=edge.bridge_steps,
                lookahead_safe_for_event=strict_event_visibility,
            )
            all_steps.append(step)
            step_by_edge[edge.name] = step
            selected_nodes.add(edge.to_node)

    def _can_use_direct_source_table_binding(self, entry: FieldCatalogEntry, base_node: str) -> bool:
        if not entry.source_table or entry.binding_mode != "source_table":
            return False
        base = self.registry.nodes[base_node]
        if entry.source_table == base.table:
            return True
        return bool(entry.join_keys or entry.time_binding or entry.bridge_steps)

    def _collect_direct_source_table_requirement(
        self,
        *,
        field_name: str,
        base_node: str,
        catalog_entry: FieldCatalogEntry,
        field_bindings: dict[str, str],
        resolved_fields: dict[str, str],
        all_steps: list[PlanStep],
        step_by_edge: dict[str, PlanStep],
        selected_nodes: set[str],
        strict_event_visibility: bool,
    ) -> None:
        base = self.registry.nodes[base_node]
        resolved_fields[field_name] = catalog_entry.source_field or self.registry.resolve_physical_field(field_name)
        if catalog_entry.source_table == base.table:
            field_bindings[field_name] = base_node
            selected_nodes.add(base_node)
            return

        step_key = self._direct_table_step_key(base_node, catalog_entry)
        field_bindings[field_name] = step_key
        selected_nodes.add(step_key)
        if step_key in step_by_edge:
            if strict_event_visibility:
                step_by_edge[step_key].lookahead_safe_for_event = True
            return

        relation_type = catalog_entry.relation_type or ("bridge" if catalog_entry.bridge_steps else "direct")
        step = PlanStep(
            edge_name=step_key,
            from_node=base_node,
            to_node=step_key,
            to_table=catalog_entry.source_table,
            relation_type=relation_type,
            join_keys=list(catalog_entry.join_keys),
            time_binding=catalog_entry.time_binding,
            bridge_steps=list(catalog_entry.bridge_steps),
            lookahead_safe_for_event=strict_event_visibility,
        )
        all_steps.append(step)
        step_by_edge[step_key] = step

    def _direct_table_step_key(self, base_node: str, entry: FieldCatalogEntry) -> str:
        payload = {
            "base_node": base_node,
            "source_table": entry.source_table,
            "relation_type": entry.relation_type or ("bridge" if entry.bridge_steps else "direct"),
            "join_keys": entry.join_keys,
            "time_binding": self._time_binding_signature(entry.time_binding),
            "bridge_steps": [
                {
                    "table": step.table,
                    "join_keys": step.join_keys,
                    "time_binding": self._time_binding_signature(step.time_binding),
                }
                for step in entry.bridge_steps
            ],
        }
        return "table__" + json.dumps(payload, sort_keys=True, ensure_ascii=True)

    def _time_binding_signature(self, binding) -> dict[str, object] | None:
        if not binding:
            return None
        return {
            "mode": binding.mode,
            "base_time_field": binding.base_time_field,
            "source_time_field": binding.source_time_field,
            "source_start_field": binding.source_start_field,
            "source_end_field": binding.source_end_field,
            "base_time_cast": binding.base_time_cast,
            "source_time_cast": binding.source_time_cast,
            "available_time_field": binding.available_time_field,
            "allow_equal": binding.allow_equal,
            "lookahead_safe": binding.lookahead_safe,
        }

    def _register_path_group(self, entry: FieldCatalogEntry, selected_path_groups: dict[str, str]) -> None:
        if entry.base_node:
            return
        if not entry.path_domain or not entry.path_group:
            return
        current_group = selected_path_groups.get(entry.path_domain)
        if current_group and current_group != entry.path_group:
            raise ValueError(
                f"Conflicting path groups for domain '{entry.path_domain}': "
                f"'{current_group}' and '{entry.path_group}'"
            )
        selected_path_groups[entry.path_domain] = entry.path_group

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
