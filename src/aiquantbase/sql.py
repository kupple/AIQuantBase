from __future__ import annotations

from .models import FilterCondition, FilterGroup, OrderBy, QueryPlan
from .planner import GraphRegistry


class SqlRenderer:
    def __init__(self, registry: GraphRegistry) -> None:
        self.registry = registry

    def render(self, plan: QueryPlan) -> str:
        base_node = self.registry.nodes[plan.base_node]
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
        lines.append(f"FROM {base_node.table} {aliases[plan.base_node]}")

        for step in plan.steps:
            from_alias = aliases[step.from_node]
            to_alias = aliases.setdefault(step.to_node, f"t{len(aliases)}")
            target_table = step.to_table or self.registry.nodes[step.to_node].table
            join_target = f"{target_table} {to_alias}"
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
                    lines.append(f"LEFT JOIN {bridge_step.table} {bridge_alias} ON " + " AND ".join(conditions))
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
        if plan.time_range:
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
                f"{left} < {end}",
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
        ref = f"{alias}.{field_name}"
        if cast_name == "date":
            return f"toDate({ref})"
        if cast_name == "yyyymmdd":
            return f"toDate(parseDateTimeBestEffortOrNull(toString({ref})))"
        return ref

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
