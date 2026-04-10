from __future__ import annotations

from typing import Any

from .executor import ClickHouseExecutor
from .runtime_config import DiscoveryConfig


class SchemaDiscoveryService:
    def __init__(self, executor: ClickHouseExecutor) -> None:
        self.executor = executor

    def discover(self, config: DiscoveryConfig) -> dict[str, Any]:
        if not config.allow_databases and not config.allow_tables:
            raise ValueError("Schema discovery requires allow_databases or allow_tables")

        tables = self._fetch_tables(config)
        columns = self._fetch_columns(config)
        return {
            "scope": {
                "allow_databases": config.allow_databases,
                "allow_tables": config.allow_tables,
            },
            "tables": tables,
            "columns": columns,
        }

    def _fetch_tables(self, config: DiscoveryConfig) -> list[dict[str, Any]]:
        conditions = []
        if config.allow_databases:
            conditions.append(self._in_clause("database", config.allow_databases))
        if config.allow_tables:
            conditions.append(self._in_clause("name", config.allow_tables))
        where = " AND ".join(conditions) if conditions else "1 = 1"
        sql = (
            "SELECT database, name, engine, create_table_query "
            f"FROM system.tables WHERE {where} ORDER BY database, name"
        )
        return self.executor.execute_sql(sql).data

    def _fetch_columns(self, config: DiscoveryConfig) -> list[dict[str, Any]]:
        conditions = []
        if config.allow_databases:
            conditions.append(self._in_clause("database", config.allow_databases))
        if config.allow_tables:
            conditions.append(self._in_clause("table", config.allow_tables))
        where = " AND ".join(conditions) if conditions else "1 = 1"
        sql = (
            "SELECT database, table, name, type, default_kind, default_expression, comment, position "
            f"FROM system.columns WHERE {where} ORDER BY database, table, position"
        )
        return self.executor.execute_sql(sql).data

    def _in_clause(self, field_name: str, values: list[str]) -> str:
        quoted = ", ".join(self._quote(value) for value in values)
        return f"{field_name} IN ({quoted})"

    def _quote(self, value: str) -> str:
        escaped = value.replace("'", "\\'")
        return f"'{escaped}'"


class NodeInferenceService:
    def infer_nodes(self, schema_payload: dict[str, Any]) -> dict[str, Any]:
        columns_by_table: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for column in schema_payload.get("columns", []):
            key = (column["database"], column["table"])
            columns_by_table.setdefault(key, []).append(column)

        inferred_nodes: list[dict[str, Any]] = []
        for table in schema_payload.get("tables", []):
            key = (table["database"], table["name"])
            columns = columns_by_table.get(key, [])
            inferred_nodes.append(self._infer_single_node(table, columns))

        return {
            "scope": schema_payload.get("scope", {}),
            "observed_tables": schema_payload.get("tables", []),
            "inferred_nodes": inferred_nodes,
        }

    def _infer_single_node(self, table: dict[str, Any], columns: list[dict[str, Any]]) -> dict[str, Any]:
        column_names = [column["name"] for column in columns]
        node_name = self._infer_node_name(table["name"])
        entity_keys = self._infer_entity_keys(column_names)
        time_key = self._infer_time_key(column_names)
        grain = self._infer_grain(table["name"], column_names)
        node_type = self._infer_node_type(table["name"], column_names)

        return {
            "name": node_name,
            "source": {
                "database": table["database"],
                "table": table["name"],
                "engine": table.get("engine"),
            },
            "candidate_node_type": node_type,
            "candidate_grain": grain,
            "candidate_entity_keys": entity_keys,
            "candidate_time_key": time_key,
            "fields": column_names,
            "notes": self._build_notes(table["name"], column_names, node_type, grain),
        }

    def _infer_node_name(self, table_name: str) -> str:
        if table_name.startswith("ad_"):
            return table_name.removeprefix("ad_")
        return table_name

    def _infer_entity_keys(self, column_names: list[str]) -> list[str]:
        for candidate in ("code", "market_code", "industry_code", "concept_id", "benchmark_code"):
            if candidate in column_names:
                return [candidate]
        return []

    def _infer_time_key(self, column_names: list[str]) -> str | None:
        for candidate in ("trade_time", "trade_date", "announce_date", "effective_date"):
            if candidate in column_names:
                return candidate
        return None

    def _infer_grain(self, table_name: str, column_names: list[str]) -> str:
        if "minute" in table_name:
            return "minute"
        if "daily" in table_name:
            return "daily"
        if any(name.startswith("is_") for name in column_names):
            return "daily_status"
        if "factor_value" in column_names:
            return "daily_factor"
        return "unknown"

    def _infer_node_type(self, table_name: str, column_names: list[str]) -> str:
        if "kline" in table_name and "minute" in table_name:
            return "market_timeseries"
        if "kline" in table_name or "daily" in table_name:
            return "market_timeseries"
        if "tot_share" in column_names and "float_share" in column_names:
            return "equity_structure"
        if "factor_value" in column_names:
            return "adjustment_factor"
        if any(name in column_names for name in ("is_st_sec", "is_susp_sec", "is_xr_sec")):
            return "status_snapshot"
        return "generic_table"

    def _build_notes(
        self,
        table_name: str,
        column_names: list[str],
        node_type: str,
        grain: str,
    ) -> list[str]:
        notes = [
            f"Detected node_type={node_type}",
            f"Detected grain={grain}",
        ]
        if "factor_value" in column_names:
            notes.append("Likely factor table; for this project ad_adj_factor is treated as forward adjustment factor")
        if any(name in column_names for name in ("is_st_sec", "is_susp_sec", "is_xr_sec")):
            notes.append("Contains status flags that may attach to stock daily data")
        if "market_code" in column_names and "code" not in column_names:
            notes.append("Potential key mismatch: market_code may need mapping to stock code in edge configuration")
        if table_name.endswith("_daily") and "trade_time" in column_names:
            notes.append("Daily table uses DateTime trade_time; downstream graph may normalize to trade_date semantics")
        return notes


class EdgeInferenceService:
    def infer_edges(self, inferred_payload: dict[str, Any]) -> dict[str, Any]:
        nodes = inferred_payload.get("inferred_nodes", [])
        edges: list[dict[str, Any]] = []

        by_name = {node["name"]: node for node in nodes}
        market_daily = by_name.get("market_kline_daily")
        adj_factor = by_name.get("adj_factor")
        status = by_name.get("history_stock_status")

        if market_daily and adj_factor:
            edges.append(
                {
                    "name": "market_kline_daily_to_adj_factor",
                    "from": market_daily["name"],
                    "to": adj_factor["name"],
                    "relation_type": "direct",
                    "candidate_confidence": "high",
                    "join_keys": [
                        {"base": "code", "source": "code"},
                    ],
                    "time_binding": {
                        "mode": "same_date",
                        "base_time_field": "trade_time",
                        "base_time_cast": "date",
                        "source_time_field": "trade_date",
                    },
                    "notes": [
                        "Daily kline and adjustment factor share stock code",
                        "trade_time should be normalized to trade_date when joining daily factor table",
                        "For this project ad_adj_factor is treated as forward adjustment factor",
                    ],
                }
            )

        if market_daily and status:
            edges.append(
                {
                    "name": "market_kline_daily_to_history_stock_status",
                    "from": market_daily["name"],
                    "to": status["name"],
                    "relation_type": "direct",
                    "candidate_confidence": "high",
                    "join_keys": [
                        {"base": "code", "source": "market_code"},
                    ],
                    "time_binding": {
                        "mode": "same_date",
                        "base_time_field": "trade_time",
                        "base_time_cast": "date",
                        "source_time_field": "trade_date",
                    },
                    "notes": [
                        "Status snapshot appears attachable to stock daily data by trade date",
                        "market_code has been observed to use the same A-share code format as stock_daily.code",
                        "trade_time should be normalized to trade_date when joining status snapshot table",
                    ],
                }
            )

        return {
            "scope": inferred_payload.get("scope", {}),
            "observed_tables": inferred_payload.get("observed_tables", []),
            "inferred_nodes": nodes,
            "inferred_edges": edges,
        }


class GraphInferenceService:
    def infer_graph(self, edge_payload: dict[str, Any]) -> dict[str, Any]:
        graph_nodes: list[dict[str, Any]] = []
        for node in edge_payload.get("inferred_nodes", []):
            graph_nodes.append(
                {
                    "name": node["name"],
                    "table": f"{node['source']['database']}.{node['source']['table']}",
                    "entity_keys": node.get("candidate_entity_keys", []),
                    "time_key": node.get("candidate_time_key"),
                    "fields": node.get("fields", []),
                    "description": "; ".join(node.get("notes", [])),
                    "candidate_node_type": node.get("candidate_node_type"),
                    "candidate_grain": node.get("candidate_grain"),
                }
            )

        graph_edges: list[dict[str, Any]] = []
        for edge in edge_payload.get("inferred_edges", []):
            graph_edges.append(
                {
                    "name": edge["name"],
                    "from": edge["from"],
                    "to": edge["to"],
                    "relation_type": edge["relation_type"],
                    "join_keys": edge.get("join_keys", []),
                    "time_binding": edge.get("time_binding"),
                    "description": "; ".join(edge.get("notes", [])),
                    "candidate_confidence": edge.get("candidate_confidence", "unknown"),
                }
            )

        return {
            "scope": edge_payload.get("scope", {}),
            "candidate_graph": {
                "nodes": graph_nodes,
                "edges": graph_edges,
            },
            "observed_tables": edge_payload.get("observed_tables", []),
            "inferred_nodes": edge_payload.get("inferred_nodes", []),
            "inferred_edges": edge_payload.get("inferred_edges", []),
        }


class GraphExportService:
    def export_graph_yaml_payload(self, graph_payload: dict[str, Any]) -> dict[str, Any]:
        candidate_graph = graph_payload.get("candidate_graph", {})
        return {
            "nodes": [
                {
                    "name": node["name"],
                    "table": node["table"],
                    "entity_keys": node.get("entity_keys", []),
                    "time_key": node.get("time_key"),
                    "grain": node.get("candidate_grain"),
                    "fields": node.get("fields", []),
                    "description": node.get("description"),
                }
                for node in candidate_graph.get("nodes", [])
            ],
            "edges": [
                {
                    "name": edge["name"],
                    "from": edge["from"],
                    "to": edge["to"],
                    "relation_type": edge["relation_type"],
                    "join_keys": edge.get("join_keys", []),
                    "time_binding": edge.get("time_binding"),
                    "description": edge.get("description"),
                }
                for edge in candidate_graph.get("edges", [])
            ],
        }


class FieldCatalogInferenceService:
    def infer_field_catalog(self, graph_payload: dict[str, Any]) -> dict[str, Any]:
        candidate_graph = graph_payload.get("candidate_graph", {})
        catalog: list[dict[str, Any]] = []

        for node in candidate_graph.get("nodes", []):
            node_name = node["name"]
            fields = set(node.get("fields", []))
            node_type = node.get("candidate_node_type")
            grain = node.get("candidate_grain")

            catalog.extend(self._infer_common_market_fields(node_name, fields, grain))
            if node_type == "adjustment_factor":
                catalog.extend(self._infer_adjustment_fields(node_name, fields))
            if node_type == "equity_structure":
                catalog.extend(self._infer_equity_structure_fields(node_name, fields))
            if node_type == "status_snapshot":
                catalog.extend(self._infer_status_fields(node_name, fields))

        return {
            "scope": graph_payload.get("scope", {}),
            "candidate_field_catalog": catalog,
            "candidate_graph": candidate_graph,
        }

    def _infer_common_market_fields(self, node_name: str, fields: set[str], grain: str) -> list[dict[str, Any]]:
        mappings = [
            ("open", "open", "raw_market_field"),
            ("high", "high", "raw_market_field"),
            ("low", "low", "raw_market_field"),
            ("close", "close", "raw_market_field"),
            ("volume", "volume", "raw_market_field"),
            ("amount", "amount", "raw_market_field"),
        ]
        result: list[dict[str, Any]] = []
        for source_field, standard_field, role in mappings:
            if source_field in fields:
                result.append(
                    {
                        "standard_field": standard_field,
                        "source_node": node_name,
                        "source_field": source_field,
                        "field_role": role,
                        "resolver_type": "direct",
                        "applies_to_grain": grain,
                        "notes": ["Direct passthrough market field"],
                    }
                )
        return result

    def _infer_adjustment_fields(self, node_name: str, fields: set[str]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        if "factor_value" in fields:
            result.append(
                {
                    "standard_field": "forward_adj_factor",
                    "source_node": node_name,
                    "source_field": "factor_value",
                    "field_role": "adjustment_factor",
                    "resolver_type": "direct",
                    "applies_to_grain": "daily",
                    "notes": ["Mapped from ad_adj_factor", "Interpreted as forward adjustment factor"],
                }
            )
            result.append(
                {
                    "standard_field": "close_adj",
                    "source_node": None,
                    "source_field": None,
                    "field_role": "derived_field",
                    "resolver_type": "derived",
                    "depends_on": ["close", "forward_adj_factor"],
                    "formula": "{close} * {forward_adj_factor}",
                    "applies_to_grain": "daily",
                    "notes": ["Derived adjusted close based on raw close and forward adjustment factor"],
                }
            )
            result.extend(
                [
                    {
                        "standard_field": "open_adj",
                        "source_node": None,
                        "source_field": None,
                        "field_role": "derived_field",
                        "resolver_type": "derived",
                        "depends_on": ["open", "forward_adj_factor"],
                        "formula": "{open} * {forward_adj_factor}",
                        "applies_to_grain": "daily",
                        "notes": ["Derived adjusted open based on raw open and forward adjustment factor"],
                    },
                    {
                        "standard_field": "high_adj",
                        "source_node": None,
                        "source_field": None,
                        "field_role": "derived_field",
                        "resolver_type": "derived",
                        "depends_on": ["high", "forward_adj_factor"],
                        "formula": "{high} * {forward_adj_factor}",
                        "applies_to_grain": "daily",
                        "notes": ["Derived adjusted high based on raw high and forward adjustment factor"],
                    },
                    {
                        "standard_field": "low_adj",
                        "source_node": None,
                        "source_field": None,
                        "field_role": "derived_field",
                        "resolver_type": "derived",
                        "depends_on": ["low", "forward_adj_factor"],
                        "formula": "{low} * {forward_adj_factor}",
                        "applies_to_grain": "daily",
                        "notes": ["Derived adjusted low based on raw low and forward adjustment factor"],
                    },
                    {
                        "standard_field": "gap_open_return",
                        "source_node": None,
                        "source_field": None,
                        "field_role": "derived_field",
                        "resolver_type": "derived",
                        "depends_on": ["open", "pre_close"],
                        "formula": "({open} / nullIf({pre_close}, 0)) - 1",
                        "applies_to_grain": "daily",
                        "notes": ["Derived gap open return based on raw open and previous close"],
                    },
                ]
            )
        return result

    def _infer_status_fields(self, node_name: str, fields: set[str]) -> list[dict[str, Any]]:
        mappings = [
            ("preclose", "pre_close", "status_snapshot_field"),
            ("high_limited", "high_limited", "status_snapshot_field"),
            ("low_limited", "low_limited", "status_snapshot_field"),
            ("is_st_sec", "is_st", "status_flag"),
            ("is_susp_sec", "is_suspended", "status_flag"),
            ("is_wd_sec", "is_wd", "status_flag"),
            ("is_xr_sec", "is_xr", "status_flag"),
        ]
        result: list[dict[str, Any]] = []
        for source_field, standard_field, role in mappings:
            if source_field in fields:
                result.append(
                    {
                        "standard_field": standard_field,
                        "source_node": node_name,
                        "source_field": source_field,
                        "field_role": role,
                        "resolver_type": "direct",
                        "applies_to_grain": "daily",
                        "notes": ["Mapped from status snapshot table"],
                    }
                )
        return result

    def _infer_equity_structure_fields(self, node_name: str, fields: set[str]) -> list[dict[str, Any]]:
        mappings = [
            ("tot_share", "tot_share", "equity_structure_field"),
            ("float_share", "float_share", "equity_structure_field"),
            ("float_a_share", "float_a_share", "equity_structure_field"),
        ]
        result: list[dict[str, Any]] = []
        for source_field, standard_field, role in mappings:
            if source_field in fields:
                result.append(
                    {
                        "standard_field": standard_field,
                        "source_node": node_name,
                        "source_field": source_field,
                        "field_role": role,
                        "resolver_type": "direct",
                        "applies_to_grain": "daily",
                        "notes": ["Mapped from equity structure table", "Share unit is 10k shares"],
                    }
                )
        if "float_share" in fields:
            result.append(
                {
                    "standard_field": "turnover_rate",
                    "source_node": None,
                    "source_field": None,
                    "field_role": "derived_field",
                    "resolver_type": "derived",
                    "depends_on": ["volume", "float_share"],
                    "formula": "{volume} / nullIf(({float_share} * 10000), 0)",
                    "applies_to_grain": "daily",
                    "notes": ["Derived daily turnover rate", "float_share is in 10k-share units so denominator multiplies by 10000"],
                }
            )
        if "tot_share" in fields:
            result.append(
                {
                    "standard_field": "market_cap",
                    "source_node": None,
                    "source_field": None,
                    "field_role": "derived_field",
                    "resolver_type": "derived",
                    "depends_on": ["close", "tot_share"],
                    "formula": "{close} * ({tot_share} * 10000)",
                    "applies_to_grain": "daily",
                    "notes": ["Derived market cap", "tot_share is in 10k-share units so denominator multiplies by 10000"],
                }
            )
        if "float_share" in fields:
            result.append(
                {
                    "standard_field": "float_market_cap",
                    "source_node": None,
                    "source_field": None,
                    "field_role": "derived_field",
                    "resolver_type": "derived",
                    "depends_on": ["close", "float_share"],
                    "formula": "{close} * ({float_share} * 10000)",
                    "applies_to_grain": "daily",
                    "notes": ["Derived float market cap", "float_share is in 10k-share units so denominator multiplies by 10000"],
                }
            )
        return result


class FieldCatalogExportService:
    def export_field_catalog_yaml_payload(self, field_payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "fields": [
                {
                    "standard_field": item["standard_field"],
                    "source_node": item["source_node"],
                    "source_field": item["source_field"],
                    "field_role": item["field_role"],
                    "resolver_type": item.get("resolver_type", "direct"),
                    "depends_on": item.get("depends_on", []),
                    "formula": item.get("formula"),
                    "applies_to_grain": item["applies_to_grain"],
                    "notes": item.get("notes", []),
                }
                for item in field_payload.get("candidate_field_catalog", [])
            ]
        }


class ResearchPackageService:
    def build_candidate_package(
        self,
        graph_payload: dict[str, Any],
        field_payload: dict[str, Any],
    ) -> dict[str, Any]:
        candidate_graph = graph_payload.get("candidate_graph", {})
        candidate_fields = field_payload.get("candidate_field_catalog", [])
        return {
            "scope": graph_payload.get("scope", {}),
            "summary": {
                "node_count": len(candidate_graph.get("nodes", [])),
                "edge_count": len(candidate_graph.get("edges", [])),
                "field_count": len(candidate_fields),
            },
            "candidate_graph": candidate_graph,
            "candidate_field_catalog": candidate_fields,
        }
