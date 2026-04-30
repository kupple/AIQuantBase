from __future__ import annotations

from .models import FilterCondition, FilterGroup, OrderBy, QueryPlan
from .planner import GraphRegistry


class SqlRenderer:
    def __init__(self, registry: GraphRegistry) -> None:
        self.registry = registry
        self._base_node_name: str | None = None
        self._base_time_field: str | None = None
        self._base_date_alias: str | None = None
        self._base_time_prefiltered: bool = False

    def render(self, plan: QueryPlan) -> str:
        base_node = self.registry.nodes[plan.base_node]
        self._base_node_name = plan.base_node
        self._base_time_field = plan.time_range.field if plan.time_range else None
        self._base_date_alias = None
        self._base_time_prefiltered = False
        aliases = {plan.base_node: "b0"}
        lines: list[str] = []

        select_exprs: list[str] = []
        for field_name in plan.group_by:
            select_exprs.append(f"{self._expression_for_field(field_name, aliases, plan)} AS {self._label_for_field(field_name)}")
        for select_item in plan.select_fields:
            field_name = select_item.field
            if field_name in plan.group_by:
                continue
            label = select_item.alias or self._label_for_field(field_name)
            select_exprs.append(f"{self._expression_for_field(field_name, aliases, plan)} AS {label}")
        for aggregation in plan.aggregations:
            label = aggregation.alias or f"{aggregation.func.lower()}_{self._label_for_field(aggregation.field)}"
            select_exprs.append(f"{aggregation.func.upper()}({self._expression_for_field(aggregation.field, aliases, plan)}) AS {label}")

        lines.append("SELECT")
        lines.append("  " + ",\n  ".join(select_exprs))
        lines.append(f"FROM {self._render_base_source(base_node.table, plan)} {aliases[plan.base_node]}")

        for step in plan.steps:
            from_alias = aliases[step.from_node]
            to_alias = aliases.setdefault(step.to_node, f"t{len(aliases)}")
            target_table = step.to_table or self.registry.nodes[step.to_node].table
            join_source = self._render_join_source(target_table, step.time_binding, plan)
            join_target = f"{join_source} {to_alias}"
            if step.relation_type == "bridge" and step.bridge_steps:
                current_alias = from_alias
                for bridge_step in step.bridge_steps:
                    bridge_alias = f"br{len(aliases)}"
                    aliases[f"{step.edge_name}:{bridge_alias}"] = bridge_alias
                    conditions = [
                        self._render_pair(current_alias, bridge_alias, pair) for pair in bridge_step.join_keys
                    ]
                    if bridge_step.time_binding:
                        conditions.extend(
                            self._render_time_binding(
                                current_alias,
                                bridge_alias,
                                bridge_step.table,
                                bridge_step.join_keys,
                                bridge_step.time_binding,
                                lookahead_safe_for_event=step.lookahead_safe_for_event,
                            )
                        )
                    bridge_source = self._render_join_source(bridge_step.table, bridge_step.time_binding, plan)
                    lines.append(f"LEFT JOIN {bridge_source} {bridge_alias} ON " + " AND ".join(conditions))
                    current_alias = bridge_alias

                conditions = [self._render_pair(current_alias, to_alias, pair) for pair in step.join_keys]
                if step.time_binding:
                    conditions.extend(
                        self._render_time_binding(
                            current_alias,
                            to_alias,
                            target_table,
                            step.join_keys,
                            step.time_binding,
                            lookahead_safe_for_event=step.lookahead_safe_for_event,
                        )
                    )
                lines.append(f"LEFT JOIN {join_target} ON " + " AND ".join(conditions))
                continue

            conditions = [self._render_pair(from_alias, to_alias, pair) for pair in step.join_keys]
            join_prefix = "LEFT JOIN"
            if step.relation_type == "any":
                join_prefix = "ANY LEFT JOIN"
            if step.time_binding:
                if step.time_binding.mode == "asof":
                    join_prefix = "ASOF LEFT JOIN"
                conditions.extend(
                    self._render_time_binding(
                        from_alias,
                        to_alias,
                        target_table,
                        step.join_keys,
                        step.time_binding,
                        lookahead_safe_for_event=step.lookahead_safe_for_event,
                    )
                )
            lines.append(f"{join_prefix} {join_target} ON " + " AND ".join(conditions))

        where_clauses: list[str] = []
        if plan.time_range and not self._base_time_prefiltered:
            base_alias = aliases[plan.base_node]
            where_clauses.append(
                f"{base_alias}.{plan.resolved_fields[plan.time_range.field]} "
                f"BETWEEN '{plan.time_range.start}' AND '{plan.time_range.end}'"
            )
        rendered_where = self._render_filter_group(plan.where, aliases, plan)
        if rendered_where:
            where_clauses.append(rendered_where)
        if where_clauses:
            lines.append("WHERE " + " AND ".join(where_clauses))

        if plan.group_by:
            lines.append(
                "GROUP BY " + ", ".join(
                    self._expression_for_field(field_name, aliases, plan)
                    for field_name in plan.group_by
                )
            )
        rendered_having = self._render_filter_group(plan.having, aliases, plan, use_aggregation_aliases=True)
        if rendered_having:
            lines.append("HAVING " + rendered_having)
        if plan.order_by:
            lines.append("ORDER BY " + ", ".join(self._render_order(item, aliases, plan) for item in plan.order_by))
        if plan.limit is not None:
            lines.append(f"LIMIT {plan.limit}")
        if plan.offset is not None:
            lines.append(f"OFFSET {plan.offset}")
        return "\n".join(lines)

    def _render_join_keys(self, left_alias: str, right_alias: str, join_keys: list[dict[str, str]]) -> str:
        if not join_keys:
            return "1 = 1"
        return " AND ".join(self._render_pair(left_alias, right_alias, pair) for pair in join_keys)

    def _render_pair(self, left_alias: str, right_alias: str, pair: dict[str, str]) -> str:
        return f"{left_alias}.{pair['base']} = {right_alias}.{pair['source']}"

    def _render_time_binding(
        self,
        from_alias: str,
        to_alias: str,
        to_table: str,
        join_keys: list[dict[str, str]],
        binding,
        lookahead_safe_for_event: bool = False,
    ) -> list[str]:
        mode = binding.mode
        if mode == "same_date" or mode == "same_timestamp":
            left = self._render_time_ref(from_alias, binding.base_time_field, binding.base_time_cast)
            right = self._render_time_ref(to_alias, binding.source_time_field, binding.source_time_cast)
            if lookahead_safe_for_event:
                return [f"{left} > {right}"]
            return [f"{left} = {right}"]
        if mode == "asof":
            left = self._render_time_ref(from_alias, binding.base_time_field, binding.base_time_cast)
            right = self._render_time_ref(to_alias, binding.source_time_field, binding.source_time_cast)
            operator = ">" if lookahead_safe_for_event else (">=" if binding.allow_equal else ">")
            clauses = [f"{left} {operator} {right}"]
            if binding.available_time_field and binding.available_time_field != binding.source_time_field:
                clauses.append(
                    f"{left} >= {to_alias}.{binding.available_time_field}"
                )
            return clauses
        if mode == "effective_range":
            left = self._render_time_ref(from_alias, binding.base_time_field, binding.base_time_cast)
            start = self._render_time_ref(to_alias, binding.source_start_field, binding.source_time_cast)
            end = self._render_time_ref(to_alias, binding.source_end_field, binding.source_time_cast)
            return [
                f"{left} > {start}" if lookahead_safe_for_event else f"{left} >= {start}",
                f"({end} IS NULL OR {left} < {end})",
            ]
        raise ValueError(f"Unsupported bind mode: {mode}")

    def _render_filter_group(
        self,
        group: FilterGroup,
        aliases: dict[str, str],
        plan: QueryPlan,
        use_aggregation_aliases: bool = False,
    ) -> str:
        if not group.items:
            return ""
        rendered_items: list[str] = []
        for item in group.items:
            if isinstance(item, FilterCondition):
                rendered_items.append(self._render_filter(item, aliases, plan, use_aggregation_aliases))
            else:
                nested = self._render_filter_group(item, aliases, plan, use_aggregation_aliases)
                if nested:
                    rendered_items.append(f"({nested})")
        if not rendered_items:
            return ""
        operator = " AND " if group.mode.lower() == "and" else " OR "
        return operator.join(rendered_items)

    def _render_filter(
        self,
        condition: FilterCondition,
        aliases: dict[str, str],
        plan: QueryPlan,
        use_aggregation_aliases: bool = False,
    ) -> str:
        aggregation_aliases = {
            aggregation.alias or f"{aggregation.func.lower()}_{plan.resolved_fields[aggregation.field]}": aggregation
            for aggregation in plan.aggregations
        }
        if use_aggregation_aliases and condition.field in aggregation_aliases:
            return self._render_operator(condition.field, condition.op, condition.value)
        if condition.field in plan.derived_fields:
            return self._render_operator(self._expression_for_field(condition.field, aliases, plan), condition.op, condition.value)
        node_name = plan.field_bindings.get(condition.field, plan.base_node)
        physical_field = plan.resolved_fields.get(condition.field, condition.field)
        alias = aliases[node_name]
        return self._render_operator(f"{alias}.{physical_field}", condition.op, condition.value)

    def _render_order(self, order: OrderBy, aliases: dict[str, str], plan: QueryPlan) -> str:
        return f"{self._expression_for_field(order.field, aliases, plan)} {order.direction.upper()}"

    def _quote(self, value) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, (int, float)):
            return str(value)
        return f"'{value}'"

    def _render_time_ref(self, alias: str, field_name: str | None, cast_name: str | None) -> str:
        if not field_name:
            raise ValueError("Time binding field is required")
        if (
            alias == "b0"
            and self._base_date_alias
            and self._base_time_field == field_name
            and cast_name == "date"
        ):
            return f"{alias}.{self._base_date_alias}"
        ref = f"{alias}.{field_name}"
        if cast_name == "date":
            return f"toDate({ref})"
        if cast_name == "yyyymmdd":
            return f"toDate(parseDateTimeBestEffortOrNull(toString({ref})))"
        return ref

    def _render_join_source(self, table: str, binding, plan: QueryPlan) -> str:
        stripped = table.strip()
        if stripped.startswith("("):
            return table

        clauses = self._base_filter_clauses_for_table(stripped)
        if binding and plan.time_range and binding.mode in {"same_date", "same_timestamp"}:
            source_time_field = binding.source_time_field
            if source_time_field:
                cast_name = binding.source_time_cast or binding.base_time_cast
                start_literal = self._render_time_literal(plan.time_range.start, cast_name)
                end_literal = self._render_time_literal(plan.time_range.end, cast_name)
                clauses.append(f"{source_time_field} BETWEEN {start_literal} AND {end_literal}")

        if not clauses:
            return table
        return f"(SELECT * FROM {table} WHERE {' AND '.join(clauses)})"

    def _base_filter_clauses_for_table(self, table: str) -> list[str]:
        matches = [
            node
            for node in self.registry.nodes.values()
            if node.table == table and node.base_filters
        ]
        if len(matches) != 1:
            return []
        clauses = []
        for item in matches[0].base_filters:
            field = str(item.get("field") or "").strip()
            operator = str(item.get("op") or "").strip()
            if not field or not operator:
                continue
            clauses.append(self._render_operator(field, operator, item.get("value")))
        return clauses

    def _render_base_source(self, table: str, plan: QueryPlan) -> str:
        if not plan.time_range:
            return table
        stripped = table.strip()
        if stripped.startswith("("):
            return table

        required_fields = self._collect_base_required_fields(plan)
        time_field = plan.resolved_fields.get(plan.time_range.field, plan.time_range.field)
        required_fields.add(time_field)
        select_fields = sorted(required_fields)
        select_parts = [field for field in select_fields]

        if self._needs_base_date_alias(plan):
            self._base_date_alias = f"__base_{time_field}_date"
            select_parts.append(f"toDate({time_field}) AS {self._base_date_alias}")

        self._base_time_prefiltered = True
        rendered = ", ".join(select_parts)
        return (
            f"(SELECT {rendered} FROM {table} "
            f"WHERE {time_field} BETWEEN '{plan.time_range.start}' AND '{plan.time_range.end}')"
        )

    def _needs_base_date_alias(self, plan: QueryPlan) -> bool:
        if not plan.time_range:
            return False
        time_field = plan.time_range.field
        for step in plan.steps:
            binding = step.time_binding
            if not binding:
                continue
            if binding.base_time_field == time_field and binding.base_time_cast == "date":
                return True
        return False

    def _collect_base_required_fields(self, plan: QueryPlan) -> set[str]:
        required: set[str] = set()

        def collect_field(field_name: str) -> None:
            if field_name in plan.derived_fields:
                for dependency in plan.derived_fields[field_name].get("depends_on") or []:
                    collect_field(str(dependency))
                return
            node_name = plan.field_bindings.get(field_name)
            if node_name == plan.base_node:
                required.add(plan.resolved_fields.get(field_name, field_name))

        def collect_filter_group(group: FilterGroup) -> None:
            for item in group.items:
                if isinstance(item, FilterCondition):
                    collect_field(item.field)
                else:
                    collect_filter_group(item)

        for field_name in plan.group_by:
            collect_field(field_name)
        for select_item in plan.select_fields:
            collect_field(select_item.field)
        for aggregation in plan.aggregations:
            collect_field(aggregation.field)
        for order_by in plan.order_by:
            collect_field(order_by.field)
        collect_filter_group(plan.where)
        collect_filter_group(plan.having)

        for step in plan.steps:
            if step.from_node == plan.base_node:
                if step.bridge_steps:
                    first_bridge = step.bridge_steps[0]
                    for pair in first_bridge.join_keys:
                        required.add(pair["base"])
                    if first_bridge.time_binding and first_bridge.time_binding.base_time_field:
                        required.add(first_bridge.time_binding.base_time_field)
                else:
                    for pair in step.join_keys:
                        required.add(pair["base"])
                    if step.time_binding and step.time_binding.base_time_field:
                        required.add(step.time_binding.base_time_field)
        return required

    def _render_time_literal(self, value: str, cast_name: str | None) -> str:
        if cast_name == "date":
            return f"toDate({self._quote(value)})"
        if cast_name == "yyyymmdd":
            return f"toUInt32(formatDateTime(toDate({self._quote(value)}), '%Y%m%d'))"
        return self._quote(value)

    def _label_for_field(self, field_name: str) -> str:
        if "." in field_name:
            return field_name.split(".", 1)[1]
        return field_name

    def _expression_for_field(self, field_name: str, aliases: dict[str, str], plan: QueryPlan) -> str:
        if field_name in plan.derived_fields:
            expression = str(plan.derived_fields[field_name]["formula"])
            for dependency in plan.derived_fields[field_name]["depends_on"]:
                expression = expression.replace(
                    "{" + dependency + "}",
                    self._expression_for_field(str(dependency), aliases, plan),
                )
            return f"({expression})"
        node_name = plan.field_bindings[field_name]
        physical_field = plan.resolved_fields[field_name]
        alias = aliases.setdefault(node_name, f"t{len(aliases)}")
        return f"{alias}.{physical_field}"

    def _render_operator(self, left: str, operator: str, value) -> str:
        op = operator.lower()
        if op == "in":
            values = ", ".join(self._quote(item) for item in value)
            return f"{left} IN ({values})"
        if op == "not_in":
            values = ", ".join(self._quote(item) for item in value)
            return f"{left} NOT IN ({values})"
        if op == "between":
            start, end = value
            return f"{left} BETWEEN {self._quote(start)} AND {self._quote(end)}"
        if op == "like":
            return f"{left} LIKE {self._quote(value)}"
        if op == "not_like":
            return f"{left} NOT LIKE {self._quote(value)}"
        if op == "ilike":
            return f"lower({left}) LIKE lower({self._quote(value)})"
        if op == "not_ilike":
            return f"lower({left}) NOT LIKE lower({self._quote(value)})"
        if op == "contains":
            return f"{left} LIKE {self._quote('%' + str(value) + '%')}"
        if op == "starts_with":
            return f"{left} LIKE {self._quote(str(value) + '%')}"
        if op == "ends_with":
            return f"{left} LIKE {self._quote('%' + str(value))}"
        if op == "is_null":
            return f"{left} IS NULL"
        if op == "is_not_null":
            return f"{left} IS NOT NULL"
        return f"{left} {operator} {self._quote(value)}"
