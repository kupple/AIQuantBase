from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from flask import (
    Flask,
    current_app,
    jsonify,
    request,
)

from .config import dump_yaml, load_field_catalog, load_nodes_and_edges
from .discovery import SchemaDiscoveryService
from .executor import ClickHouseExecutor
from .llm import DeepSeekClient
from .nl_intent import enrich_query_intent_with_aliases, normalize_query_intent_defaults, validate_query_intent_fields
from .planner import GraphRegistry, QueryPlanner
from .runtime import GraphRuntime
from .runtime_config import DEFAULT_RUNTIME_CONFIG_PATH, load_runtime_config
from .sql import SqlRenderer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_GRAPH_PATH = PROJECT_ROOT / "config" / "graph.yaml"
DEFAULT_FIELDS_PATH = PROJECT_ROOT / "config" / "fields.yaml"



def create_app(
    default_graph_path: Path | None = None,
    default_fields_path: Path | None = None,
) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = "aiquantbase-dev-secret"
    app.config["DEFAULT_GRAPH_PATH"] = Path(default_graph_path or DEFAULT_GRAPH_PATH)
    app.config["DEFAULT_FIELDS_PATH"] = Path(default_fields_path or DEFAULT_FIELDS_PATH)

    @app.after_request
    def add_cors_headers(response):
        response.headers["Access-Control-Allow-Origin"] = "http://127.0.0.1:3010"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        return response

    @app.get("/")
    def index():
        return jsonify(
            {
                "ok": True,
                "service": "AIQuantBase API",
                "ui": "Nuxt node workbench",
                "workspace": {
                    "graph_path": str(_default_graph_path()),
                    "fields_path": str(_default_fields_path()),
                    "runtime_path": str(DEFAULT_RUNTIME_CONFIG_PATH),
                },
                "available_routes": [
                    "/api/health",
                    "/api/workspace",
                    "/api/schema/databases",
                    "/api/schema/tables",
                    "/api/schema/columns",
                    "/api/metadata/catalog",
                    "/api/metadata/protocol-summary",
                    "/api/metadata/disabled-node-cleanup",
                    "/api/fields/ai-notes",
                    "/api/query/execute",
                    "/api/query/nl",
                ],
            }
        )

    @app.get("/api/workspace")
    def get_workspace():
        graph_path = _resolve_workspace_path(request.args.get("graph_path") or _default_graph_path())
        fields_path = _resolve_workspace_path(request.args.get("fields_path") or _default_fields_path())
        runtime_path = _resolve_workspace_path(request.args.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH)
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
                    "llm": asdict(runtime.llm),
                    "datasource": asdict(runtime.datasource),
                    "discovery": asdict(runtime.discovery),
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
        graph_path = _resolve_workspace_path(payload["workspace"]["graph_path"])
        fields_path = _resolve_workspace_path(payload["workspace"]["fields_path"])
        runtime_path = _resolve_workspace_path(payload["workspace"]["runtime_path"])
        current_runtime = load_runtime_config(runtime_path)

        graph_yaml = dump_yaml(
            {
                "nodes": payload.get("graph", {}).get("nodes", []),
                "edges": payload.get("graph", {}).get("edges", []),
            }
        )
        fields_yaml = dump_yaml({"fields": payload.get("fields", [])})
        runtime_yaml = dump_yaml(
            {
                "llm": payload.get("runtime", {}).get("llm") or asdict(current_runtime.llm),
                "datasource": payload.get("runtime", {}).get("datasource") or asdict(current_runtime.datasource),
                "discovery": payload.get("runtime", {}).get("discovery") or asdict(current_runtime.discovery),
            }
        )

        graph_path.parent.mkdir(parents=True, exist_ok=True)
        fields_path.parent.mkdir(parents=True, exist_ok=True)
        runtime_path.parent.mkdir(parents=True, exist_ok=True)
        graph_path.write_text(graph_yaml, encoding="utf-8")
        fields_path.write_text(fields_yaml, encoding="utf-8")
        runtime_path.write_text(runtime_yaml, encoding="utf-8")

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

    @app.get("/api/metadata/catalog")
    def metadata_catalog():
        graph_path = Path(request.args.get("graph_path") or _default_graph_path())
        fields_path = Path(request.args.get("fields_path") or _default_fields_path())
        nodes, edges = _safe_load_graph(graph_path)
        fields = _safe_load_fields(fields_path)
        return jsonify(
            {
                "nodes": [
                    {
                        "name": node.name,
                        "table": node.table,
                        "grain": node.grain,
                        "description": node.description,
                        "description_zh": node.description_zh,
                        "node_role": node.node_role,
                        "status": node.status,
                    }
                    for node in nodes
                ],
                "edges": [
                    {
                        "name": edge.name,
                        "from": edge.from_node,
                        "to": edge.to_node,
                        "relation_type": edge.relation_type,
                        "description": edge.description,
                        "description_zh": edge.description_zh,
                    }
                    for edge in edges
                ],
                "fields": [
                    {
                        "standard_field": field.standard_field,
                        "source_node": field.source_node,
                        "source_field": field.source_field,
                        "field_role": field.field_role,
                        "description_zh": field.description_zh,
                        "path_domain": field.path_domain,
                        "path_group": field.path_group,
                        "via_node": field.via_node,
                        "time_semantics": field.time_semantics,
                        "lookahead_category": field.lookahead_category,
                        "notes": list(field.notes),
                    }
                    for field in fields
                ],
            }
        )

    @app.get("/api/metadata/protocol-summary")
    def protocol_summary():
        graph_path = Path(request.args.get("graph_path") or _default_graph_path())
        fields_path = Path(request.args.get("fields_path") or _default_fields_path())
        runtime_path = Path(request.args.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH)
        runtime = GraphRuntime(
            graph_path=graph_path,
            fields_path=fields_path,
            runtime_path=runtime_path,
        )
        return jsonify(runtime.get_protocol_summary())

    @app.get("/api/metadata/disabled-node-cleanup")
    def disabled_node_cleanup():
        graph_path = Path(request.args.get("graph_path") or _default_graph_path())
        fields_path = Path(request.args.get("fields_path") or _default_fields_path())
        runtime_path = Path(request.args.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH)
        runtime = GraphRuntime(
            graph_path=graph_path,
            fields_path=fields_path,
            runtime_path=runtime_path,
        )
        return jsonify(runtime.get_disabled_node_cleanup_report())

    @app.post("/api/fields/ai-notes")
    def ai_field_notes():
        payload = request.get_json(force=True)
        try:
            response = _generate_ai_field_notes(
                runtime_path=_resolve_workspace_path(payload.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH),
                node_name=str(payload.get("node_name") or "").strip(),
                items=list(payload.get("items") or []),
            )
            return jsonify({"ok": True, **response})
        except Exception as exc:
            return jsonify(
                {
                    "ok": False,
                    "error": str(exc),
                    "items": [],
                }
            )

    @app.post("/api/query/execute")
    def execute_query():
        payload = request.get_json(force=True)
        try:
            runtime = load_runtime_config(payload.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH)
            executor = ClickHouseExecutor(runtime.datasource)
            result = executor.execute_sql(payload["sql"])
            return jsonify(
                {
                    "ok": True,
                    "rows": result.rows,
                    "meta": result.meta,
                    "statistics": result.statistics,
                    "data": result.data,
                    "sql": result.sql,
                }
            )
        except Exception as exc:
            return jsonify(
                {
                    "ok": False,
                    "error": str(exc),
                    "rows": 0,
                    "meta": [],
                    "statistics": {},
                    "data": [],
                    "sql": payload.get("sql", ""),
                }
            )

    @app.post("/api/query/nl")
    def execute_nl_query():
        payload = request.get_json(force=True)
        try:
            response = _run_nl_query(
                natural_query=payload["query"],
                graph_path=Path(payload.get("graph_path") or _default_graph_path()),
                fields_path=Path(payload.get("fields_path") or _default_fields_path()),
                runtime_path=Path(payload.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH),
            )
            response["ok"] = True
            return jsonify(response)
        except Exception as exc:
            return jsonify(
                {
                    "ok": False,
                    "error": str(exc),
                    "query_intent": None,
                    "sql": "",
                    "rows": 0,
                    "meta": [],
                    "statistics": {},
                    "data": [],
                }
            )

    @app.get("/api/health")
    def health():
        return jsonify({"ok": True})

    return app


def run_server(host: str = "127.0.0.1", port: int = 8000, debug: bool = True) -> None:
    app = create_app()
    app.run(host=host, port=port, debug=debug)


def _default_graph_path() -> Path:
    return Path(current_app.config["DEFAULT_GRAPH_PATH"])


def _default_fields_path() -> Path:
    return Path(current_app.config["DEFAULT_FIELDS_PATH"])


def _resolve_workspace_path(path_like: str | Path) -> Path:
    path = Path(path_like)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _safe_load_graph(path: Path):
    resolved = _resolve_workspace_path(path)
    if not resolved.exists():
        return [], []
    return load_nodes_and_edges(resolved)


def _safe_load_fields(path: Path):
    resolved = _resolve_workspace_path(path)
    if not resolved.exists():
        return []
    return load_field_catalog(resolved)


def _run_nl_query(
    natural_query: str,
    graph_path: Path,
    fields_path: Path,
    runtime_path: Path,
) -> dict[str, Any]:
    runtime = load_runtime_config(runtime_path)
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
    with NamedTemporaryFile("w+", suffix=".yaml", encoding="utf-8", delete=True) as handle:
        handle.write(intent_yaml)
        handle.flush()
        from .config import load_query_intent

        intent = load_query_intent(handle.name)

    registry = GraphRegistry(nodes, edges, field_catalog=field_catalog)
    plan = QueryPlanner(registry).plan(intent)
    sql = SqlRenderer(registry).render(plan)
    result = ClickHouseExecutor(runtime.datasource).execute_sql(sql)
    return {
        "model": llm_response.model,
        "query_intent": query_intent,
        "sql": result.sql,
        "rows": result.rows,
        "meta": result.meta,
        "statistics": result.statistics,
        "data": result.data,
    }


def _generate_ai_field_notes(
    runtime_path: Path,
    node_name: str,
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    runtime = load_runtime_config(runtime_path)
    if not items:
        return {"items": []}

    system_prompt = (
        "You write concise Chinese field notes for a data-query workbench. "
        "Return strict JSON with one top-level key: items. "
        "Each item must contain row_key and notes. "
        "notes must be an array of 1 to 2 short Chinese strings. "
        "Explain what the field means or how it is derived. "
        "For derived fields, mention dependencies and the calculation idea briefly. "
        "Do not write SQL or mention prompts. "
        "Do not use template phrases like '直接来自源表字段', '直接来自主表字段', '挂载自', '来源表字段', or similar implementation wording."
    )
    user_prompt = {
        "task": "generate field notes",
        "node_name": node_name,
        "items": [
            {
                "row_key": str(item.get("row_key") or ""),
                "standard_field": item.get("standard_field"),
                "binding_type": item.get("binding_type"),
                "binding_mode": item.get("binding_mode"),
                "source_table": item.get("source_table"),
                "source_field": item.get("source_field"),
                "relation_mode": item.get("relation_mode"),
                "depends_on": item.get("depends_on") or [],
                "formula": item.get("formula"),
                "existing_notes": item.get("notes") or [],
            }
            for item in items
        ],
    }

    llm_response = DeepSeekClient(runtime.llm).chat_json(system_prompt, json.dumps(user_prompt, ensure_ascii=False))
    parsed = llm_response.parsed_json if isinstance(llm_response.parsed_json, dict) else {}
    valid_row_keys = {str(item.get("row_key") or "") for item in items}
    response_items = []
    for item in parsed.get("items", []) if isinstance(parsed.get("items"), list) else []:
        row_key = str(item.get("row_key") or "")
        if not row_key or row_key not in valid_row_keys:
            continue
        notes = item.get("notes")
        if isinstance(notes, str):
            notes = [notes]
        notes = [_clean_ai_note_text(str(note)) for note in (notes or [])]
        notes = [note for note in notes if note]
        if not notes:
            continue
        response_items.append(
            {
                "row_key": row_key,
                "notes": notes[:2],
            }
        )

    return {
        "model": llm_response.model,
        "items": response_items,
    }


def _clean_ai_note_text(text: str) -> str:
    note = text.strip()
    banned_fragments = [
        "直接来自源表字段",
        "直接来自主表字段",
        "直接来自源表",
        "直接来自主表",
        "来源表字段",
        "挂载自",
        "挂载自主表字段",
        "来自节点主表字段",
    ]
    for fragment in banned_fragments:
        if fragment in note:
            return ""
    return note
