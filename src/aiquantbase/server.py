from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

from .config import dump_yaml, load_field_catalog, load_nodes_and_edges
from .discovery import SchemaDiscoveryService
from .executor import ClickHouseExecutor
from .llm import DeepSeekClient
from .nl_intent import enrich_query_intent_with_aliases, normalize_query_intent_defaults, validate_query_intent_fields
from .planner import GraphRegistry, QueryPlanner
from .runtime_config import DEFAULT_RUNTIME_CONFIG_PATH, load_runtime_config
from .sql import SqlRenderer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STUDIO_DIR = PROJECT_ROOT / "studio"
DEFAULT_GRAPH_PATH = PROJECT_ROOT / "out" / "graph.yaml"
DEFAULT_FIELDS_PATH = PROJECT_ROOT / "out" / "fields.yaml"


def create_app() -> Flask:
    app = Flask(__name__, static_folder=str(STUDIO_DIR), static_url_path="/studio")

    @app.after_request
    def add_cors_headers(response):
        response.headers["Access-Control-Allow-Origin"] = "http://127.0.0.1:3010"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        return response

    @app.get("/")
    def index():
        return send_from_directory(STUDIO_DIR, "index.html")

    @app.get("/studio/<path:filename>")
    def studio_assets(filename: str):
        return send_from_directory(STUDIO_DIR, filename)

    @app.get("/api/workspace")
    def get_workspace():
        graph_path = Path(request.args.get("graph_path") or DEFAULT_GRAPH_PATH)
        fields_path = Path(request.args.get("fields_path") or DEFAULT_FIELDS_PATH)
        runtime_path = Path(request.args.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH)
        runtime = load_runtime_config(runtime_path)

        nodes, edges = _safe_load_graph(graph_path)
        fields = _safe_load_fields(fields_path)
        return jsonify(
            {
                "workspace": {
                    "graph_path": str(graph_path),
                    "fields_path": str(fields_path),
                    "runtime_path": str(runtime_path),
                },
                "runtime": {
                    "datasource": {
                        "id": runtime.datasource.id,
                        "name": runtime.datasource.name,
                        "db_type": runtime.datasource.db_type,
                        "host": runtime.datasource.host,
                        "port": runtime.datasource.port,
                        "database": runtime.datasource.database,
                    },
                    "discovery": {
                        "allow_databases": runtime.discovery.allow_databases,
                        "allow_tables": runtime.discovery.allow_tables,
                    },
                },
                "graph": {
                    "nodes": [asdict(node) for node in nodes],
                    "edges": [asdict(edge) for edge in edges],
                },
                "fields": [asdict(field) for field in fields],
            }
        )

    @app.post("/api/workspace")
    def save_workspace():
        payload = request.get_json(force=True)
        graph_path = Path(payload["workspace"]["graph_path"])
        fields_path = Path(payload["workspace"]["fields_path"])

        graph_yaml = dump_yaml(
            {
                "nodes": payload.get("graph", {}).get("nodes", []),
                "edges": payload.get("graph", {}).get("edges", []),
            }
        )
        fields_yaml = dump_yaml({"fields": payload.get("fields", [])})

        graph_path.parent.mkdir(parents=True, exist_ok=True)
        fields_path.parent.mkdir(parents=True, exist_ok=True)
        graph_path.write_text(graph_yaml, encoding="utf-8")
        fields_path.write_text(fields_yaml, encoding="utf-8")

        return jsonify({"ok": True})

    @app.get("/api/schema/databases")
    def list_databases():
        runtime = load_runtime_config(request.args.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH)
        executor = ClickHouseExecutor(runtime.datasource)
        rows = executor.execute_sql("SELECT name FROM system.databases ORDER BY name").data
        return jsonify({"items": rows})

    @app.get("/api/schema/tables")
    def list_tables():
        runtime = load_runtime_config(request.args.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH)
        database = request.args.get("database")
        if not database:
            return jsonify({"items": []})
        executor = ClickHouseExecutor(runtime.datasource)
        sql = (
            "SELECT database, name, engine FROM system.tables "
            f"WHERE database = '{database}' ORDER BY name"
        )
        rows = executor.execute_sql(sql).data
        return jsonify({"items": rows})

    @app.get("/api/schema/columns")
    def list_columns():
        runtime = load_runtime_config(request.args.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH)
        database = request.args.get("database")
        table = request.args.get("table")
        if not database or not table:
            return jsonify({"items": []})
        executor = ClickHouseExecutor(runtime.datasource)
        sql = (
            "SELECT database, table, name, type, comment, position "
            f"FROM system.columns WHERE database = '{database}' AND table = '{table}' "
            "ORDER BY position"
        )
        rows = executor.execute_sql(sql).data
        return jsonify({"items": rows})

    @app.post("/api/query/execute")
    def execute_query():
        payload = request.get_json(force=True)
        runtime = load_runtime_config(payload.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH)
        executor = ClickHouseExecutor(runtime.datasource)
        result = executor.execute_sql(payload["sql"])
        return jsonify(
            {
                "rows": result.rows,
                "meta": result.meta,
                "statistics": result.statistics,
                "data": result.data,
                "sql": result.sql,
            }
        )

    @app.post("/api/query/nl")
    def execute_nl_query():
        payload = request.get_json(force=True)
        runtime = load_runtime_config(payload.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH)
        graph_path = Path(payload.get("graph_path") or DEFAULT_GRAPH_PATH)
        fields_path = Path(payload.get("fields_path") or DEFAULT_FIELDS_PATH)
        natural_query = payload["query"]

        nodes, edges = _safe_load_graph(graph_path)
        field_catalog = _safe_load_fields(fields_path)
        available_fields = {item.standard_field for item in field_catalog}
        available_fields.update(
            field_name
            for node in nodes
            for field_name in node.fields
            if field_name not in {"code", "trade_time", "trade_date", "market_code"}
        )

        few_shots = [
            {
                "nl": "查询 2026年4月1日到2026年4月7日 000004.SZ 的收盘价、前复权收盘价、昨收和是否ST，按日期升序返回",
                "intent": {
                    "from": "stock_daily_real",
                    "select": ["close", "close_adj", "pre_close", "is_st"],
                    "where": {"mode": "and", "items": [{"field": "code", "op": "=", "value": "000004.SZ"}]},
                    "order_by": [{"field": "trade_time", "direction": "asc"}],
                    "time_range": {"field": "trade_time", "start": "2026-04-01 00:00:00", "end": "2026-04-07 23:59:59"},
                    "page": 1,
                    "page_size": 20,
                    "safety": {"lookahead_safe": True, "strict_mode": True},
                },
            },
            {
                "nl": "查询 2026年4月1日到2026年4月7日 000004.SZ 的换手率、流通股本和成交量",
                "intent": {
                    "from": "stock_daily_real",
                    "select": ["turnover_rate", "float_share", "volume"],
                    "where": {"mode": "and", "items": [{"field": "code", "op": "=", "value": "000004.SZ"}]},
                    "order_by": [{"field": "trade_time", "direction": "asc"}],
                    "time_range": {"field": "trade_time", "start": "2026-04-01 00:00:00", "end": "2026-04-07 23:59:59"},
                    "page": 1,
                    "page_size": 20,
                    "safety": {"lookahead_safe": True, "strict_mode": True},
                },
            },
            {
                "nl": "查询 2026年4月1日到2026年4月7日 000004.SZ 的市值、流通市值、换手率",
                "intent": {
                    "from": "stock_daily_real",
                    "select": ["market_cap", "float_market_cap", "turnover_rate"],
                    "where": {"mode": "and", "items": [{"field": "code", "op": "=", "value": "000004.SZ"}]},
                    "order_by": [{"field": "trade_time", "direction": "asc"}],
                    "time_range": {"field": "trade_time", "start": "2026-04-01 00:00:00", "end": "2026-04-07 23:59:59"},
                    "page": 1,
                    "page_size": 20,
                    "safety": {"lookahead_safe": True, "strict_mode": True},
                },
            },
        ]

        system_prompt = (
            "You convert natural language A-share research requests into Query Intent JSON. "
            "Only use fields from the provided field catalog. "
            "Do not write SQL. "
            "Return strict JSON with keys: from, select, where, order_by, time_range, page, page_size, safety. "
            "Use standard fields in select/where/order_by. "
            "Keep where as a FilterGroup object. "
            "Preserve all explicitly requested metrics whenever they exist in the field catalog. "
            "If the user asks for both raw and adjusted versions, include both. "
            "If time range is implied but not explicit, choose a reasonable recent range and mention nothing else. "
            "If a field is not available in the catalog, do not invent it."
        )
        user_prompt = {
            "task": "Convert natural language to Query Intent",
            "natural_language_query": natural_query,
            "available_nodes": [
                {"name": node.name, "grain": node.grain, "table": node.table}
                for node in nodes
            ],
            "available_standard_fields": [
                {
                    "standard_field": item.standard_field,
                    "field_role": item.field_role,
                    "resolver_type": item.resolver_type,
                    "applies_to_grain": item.applies_to_grain,
                    "notes": item.notes,
                }
                for item in field_catalog
            ]
            + [
                {
                    "standard_field": field_name,
                    "field_role": "raw_graph_field",
                    "resolver_type": "direct",
                    "applies_to_grain": node.grain,
                    "notes": ["Direct raw field from graph node"],
                }
                for node in nodes
                for field_name in node.fields
                if field_name not in {"code", "trade_time", "trade_date", "market_code"}
            ],
            "few_shot_examples": few_shots,
        }

        llm_response = DeepSeekClient(runtime.llm).chat_json(system_prompt, json.dumps(user_prompt, ensure_ascii=False))
        query_intent = enrich_query_intent_with_aliases(
            llm_response.parsed_json if isinstance(llm_response.parsed_json, dict) else None,
            natural_query,
            available_fields,
        )
        query_intent = normalize_query_intent_defaults(query_intent)
        query_intent = validate_query_intent_fields(query_intent, available_fields)

        intent_yaml = dump_yaml(query_intent)
        from tempfile import NamedTemporaryFile

        with NamedTemporaryFile("w+", suffix=".yaml", encoding="utf-8", delete=True) as handle:
            handle.write(intent_yaml)
            handle.flush()
            from .config import load_query_intent

            intent = load_query_intent(handle.name)

        registry = GraphRegistry(nodes, edges, field_catalog=field_catalog)
        plan = QueryPlanner(registry).plan(intent)
        sql = SqlRenderer(registry).render(plan)
        result = ClickHouseExecutor(runtime.datasource).execute_sql(sql)
        return jsonify(
            {
                "model": llm_response.model,
                "query_intent": query_intent,
                "sql": result.sql,
                "rows": result.rows,
                "meta": result.meta,
                "statistics": result.statistics,
                "data": result.data,
            }
        )

    @app.get("/api/health")
    def health():
        return jsonify({"ok": True})

    return app


def run_server(host: str = "127.0.0.1", port: int = 8000, debug: bool = True) -> None:
    app = create_app()
    app.run(host=host, port=port, debug=debug)


def _safe_load_graph(path: Path):
    if not path.exists():
        return [], []
    return load_nodes_and_edges(path)


def _safe_load_fields(path: Path):
    if not path.exists():
        return []
    return load_field_catalog(path)
