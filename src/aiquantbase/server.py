from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from flask import (
    Flask,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from .config import dump_yaml, load_field_catalog, load_nodes_and_edges
from .discovery import SchemaDiscoveryService
from .executor import ClickHouseExecutor
from .llm import DeepSeekClient
from .models import Edge, Node, TimeBinding
from .nl_intent import enrich_query_intent_with_aliases, normalize_query_intent_defaults, validate_query_intent_fields
from .planner import GraphRegistry, QueryPlanner
from .runtime_config import DEFAULT_RUNTIME_CONFIG_PATH, load_runtime_config
from .sql import SqlRenderer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_GRAPH_PATH = PROJECT_ROOT / "config" / "graph.yaml"
DEFAULT_FIELDS_PATH = PROJECT_ROOT / "config" / "fields.yaml"
VALID_RELATION_TYPES = ("direct", "bridge")
VALID_TIME_MODES = ("", "same_date", "same_timestamp", "asof", "effective_range")
APP_MENU_ITEMS = (
    ("node_list", "节点列表"),
    ("edge_list", "边列表"),
    ("graph_view", "展示图谱"),
    ("llm_query_page", "LLM查询数据库"),
)


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
        return redirect(url_for("node_list"))

    @app.get("/nodes")
    def node_list():
        graph_path = _default_graph_path()
        nodes, edges = _safe_load_graph(graph_path)
        return render_template(
            "graph_nodes.html",
            **_build_manage_context(
                graph_path=graph_path,
                nodes=nodes,
                edges=edges,
                query=request.args.get("q", "").strip(),
                selected_node_name=request.args.get("node"),
                selected_edge_name=None,
                mode=request.args.get("mode", "node"),
                panel="nodes",
            ),
        )

    @app.get("/edges")
    def edge_list():
        graph_path = _default_graph_path()
        nodes, edges = _safe_load_graph(graph_path)
        return render_template(
            "graph_edges.html",
            **_build_manage_context(
                graph_path=graph_path,
                nodes=nodes,
                edges=edges,
                query=request.args.get("q", "").strip(),
                selected_node_name=None,
                selected_edge_name=request.args.get("edge"),
                mode=request.args.get("mode", "edge"),
                panel="edges",
            ),
        )

    @app.get("/graph/view")
    def graph_view():
        graph_path = _default_graph_path()
        nodes, edges = _safe_load_graph(graph_path)
        return render_template(
            "graph_view.html",
            graph_path=str(graph_path),
            graph_summary=_graph_summary(nodes, edges),
            graph_elements=_build_graph_elements(nodes, edges),
            node_lookup={node.name: _node_detail(node, nodes, edges) for node in nodes},
            edge_lookup={edge.name: _edge_detail(edge) for edge in edges},
            menu_items=APP_MENU_ITEMS,
        )

    @app.route("/llm-query", methods=["GET", "POST"])
    def llm_query_page():
        graph_path = _default_graph_path()
        fields_path = _default_fields_path()
        natural_query = ""
        query_intent = None
        sql = ""
        result = None
        error = ""
        if request.method == "POST":
            natural_query = request.form.get("query", "").strip()
            if natural_query:
                try:
                    payload = _run_nl_query(
                        natural_query=natural_query,
                        graph_path=graph_path,
                        fields_path=fields_path,
                        runtime_path=Path(request.form.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH),
                    )
                    query_intent = payload["query_intent"]
                    sql = payload["sql"]
                    result = payload
                except Exception as exc:  # noqa: BLE001
                    error = str(exc)
                    flash(f"LLM 查询失败: {error}", "error")
            else:
                error = "自然语言查询不能为空"
                flash(error, "error")
        return render_template(
            "graph_llm_query.html",
            graph_path=str(graph_path),
            menu_items=APP_MENU_ITEMS,
            natural_query=natural_query,
            runtime_path=str(DEFAULT_RUNTIME_CONFIG_PATH),
            query_intent=query_intent,
            sql=sql,
            result=result,
            error=error,
        )

    @app.post("/graph/node/create")
    def create_node():
        graph_path = _default_graph_path()
        nodes, edges = _safe_load_graph(graph_path)
        try:
            node = _node_from_form(request.form)
            if any(existing.name == node.name for existing in nodes):
                raise ValueError(f"节点名已存在: {node.name}")
            nodes.append(node)
            _save_graph(graph_path, nodes, edges)
            flash(f"节点已创建: {node.name}", "success")
            return redirect(url_for("node_list", node=node.name))
        except ValueError as error:
            flash(str(error), "error")
            return redirect(url_for("node_list", mode="new-node"))

    @app.post("/graph/node/update")
    def update_node():
        graph_path = _default_graph_path()
        nodes, edges = _safe_load_graph(graph_path)
        original_name = request.form.get("original_name", "").strip()
        try:
            node = _node_from_form(request.form)
            target_index = next((index for index, item in enumerate(nodes) if item.name == original_name), None)
            if target_index is None:
                raise ValueError(f"未找到节点: {original_name}")
            if node.name != original_name and any(existing.name == node.name for existing in nodes):
                raise ValueError(f"节点名已存在: {node.name}")

            nodes[target_index] = node
            if node.name != original_name:
                edges = [_rename_edge_reference(edge, original_name, node.name) for edge in edges]
            _save_graph(graph_path, nodes, edges)
            flash(f"节点已更新: {node.name}", "success")
            return redirect(url_for("node_list", node=node.name))
        except ValueError as error:
            flash(str(error), "error")
            return redirect(url_for("node_list", node=original_name or request.form.get("name", "").strip()))

    @app.post("/graph/node/delete")
    def delete_node():
        graph_path = _default_graph_path()
        nodes, edges = _safe_load_graph(graph_path)
        node_name = request.form.get("name", "").strip()
        try:
            if not node_name:
                raise ValueError("缺少待删除节点")
            if any(edge.from_node == node_name or edge.to_node == node_name for edge in edges):
                raise ValueError(f"节点 {node_name} 仍被边引用，请先删除相关边")
            nodes = [node for node in nodes if node.name != node_name]
            _save_graph(graph_path, nodes, edges)
            flash(f"节点已删除: {node_name}", "success")
        except ValueError as error:
            flash(str(error), "error")
        return redirect(url_for("node_list"))

    @app.post("/graph/edge/create")
    def create_edge():
        graph_path = _default_graph_path()
        nodes, edges = _safe_load_graph(graph_path)
        try:
            edge = _edge_from_form(request.form)
            if any(existing.name == edge.name for existing in edges):
                raise ValueError(f"边名已存在: {edge.name}")
            edges.append(edge)
            _save_graph(graph_path, nodes, edges)
            flash(f"边已创建: {edge.name}", "success")
            return redirect(url_for("edge_list", edge=edge.name))
        except ValueError as error:
            flash(str(error), "error")
            return redirect(url_for("edge_list", mode="new-edge"))

    @app.post("/graph/edge/update")
    def update_edge():
        graph_path = _default_graph_path()
        nodes, edges = _safe_load_graph(graph_path)
        original_name = request.form.get("original_name", "").strip()
        existing_edge = next((item for item in edges if item.name == original_name), None)
        try:
            if existing_edge is None:
                raise ValueError(f"未找到边: {original_name}")
            edge = _edge_from_form(request.form, existing_edge=existing_edge)
            target_index = next((index for index, item in enumerate(edges) if item.name == original_name), None)
            if target_index is None:
                raise ValueError(f"未找到边: {original_name}")
            if edge.name != original_name and any(existing.name == edge.name for existing in edges):
                raise ValueError(f"边名已存在: {edge.name}")
            edges[target_index] = edge
            _save_graph(graph_path, nodes, edges)
            flash(f"边已更新: {edge.name}", "success")
            return redirect(url_for("edge_list", edge=edge.name))
        except ValueError as error:
            flash(str(error), "error")
            return redirect(url_for("edge_list", edge=original_name or request.form.get("name", "").strip()))

    @app.post("/graph/edge/delete")
    def delete_edge():
        graph_path = _default_graph_path()
        nodes, edges = _safe_load_graph(graph_path)
        edge_name = request.form.get("name", "").strip()
        try:
            if not edge_name:
                raise ValueError("缺少待删除边")
            edges = [edge for edge in edges if edge.name != edge_name]
            _save_graph(graph_path, nodes, edges)
            flash(f"边已删除: {edge_name}", "success")
        except ValueError as error:
            flash(str(error), "error")
        return redirect(url_for("edge_list"))

    @app.get("/api/workspace")
    def get_workspace():
        graph_path = Path(request.args.get("graph_path") or _default_graph_path())
        fields_path = Path(request.args.get("fields_path") or _default_fields_path())
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
                        "is_ai_entry": node.is_ai_entry,
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
                    }
                    for field in fields
                ],
            }
        )

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
        response = _run_nl_query(
            natural_query=payload["query"],
            graph_path=Path(payload.get("graph_path") or _default_graph_path()),
            fields_path=Path(payload.get("fields_path") or _default_fields_path()),
            runtime_path=Path(payload.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH),
        )
        return jsonify(response)

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


def _safe_load_graph(path: Path):
    if not path.exists():
        return [], []
    return load_nodes_and_edges(path)


def _safe_load_fields(path: Path):
    if not path.exists():
        return []
    return load_field_catalog(path)


def _build_manage_context(
    graph_path: Path,
    nodes: list[Node],
    edges: list[Edge],
    query: str,
    selected_node_name: str | None,
    selected_edge_name: str | None,
    mode: str,
    panel: str,
) -> dict[str, Any]:
    node_details = {node.name: _node_detail(node, nodes, edges) for node in nodes}
    edge_details = {edge.name: _edge_detail(edge) for edge in edges}
    node_summaries = [_node_summary(node, nodes, edges) for node in nodes]
    edge_summaries = [_edge_summary(edge) for edge in edges]
    filtered_nodes = _filter_nodes(node_summaries, query)
    filtered_edges = _filter_edges(edge_summaries, query)

    selected_type = "new-node"
    selected_payload: dict[str, Any]
    if selected_edge_name and selected_edge_name in edge_details:
        selected_type = "edge"
        selected_payload = edge_details[selected_edge_name]
    elif selected_node_name and selected_node_name in node_details:
        selected_type = "node"
        selected_payload = node_details[selected_node_name]
    elif mode == "new-edge":
        selected_type = "new-edge"
        selected_payload = _blank_edge_payload(nodes)
    else:
        selected_type = "new-node" if not nodes else "node"
        if nodes:
            selected_payload = node_details[nodes[0].name]
        else:
            selected_type = "new-node"
            selected_payload = _blank_node_payload()

    return {
        "graph_path": str(graph_path),
        "graph_summary": _graph_summary(nodes, edges),
        "nodes": filtered_nodes,
        "edges": filtered_edges,
        "query": query,
        "selected_type": selected_type,
        "selected_payload": selected_payload,
        "node_options": [node.name for node in nodes],
        "relation_types": VALID_RELATION_TYPES,
        "time_modes": VALID_TIME_MODES,
        "menu_items": APP_MENU_ITEMS,
        "panel": panel,
    }


def _graph_summary(nodes: list[Node], edges: list[Edge]) -> dict[str, int]:
    return {
        "node_count": len(nodes),
        "edge_count": len(edges),
        "field_count": sum(len(node.fields) for node in nodes),
    }


def _node_summary(node: Node, nodes: list[Node], edges: list[Edge]) -> dict[str, Any]:
    incoming = sum(1 for edge in edges if edge.to_node == node.name)
    outgoing = sum(1 for edge in edges if edge.from_node == node.name)
    return {
        "name": node.name,
        "table": node.table,
        "grain": node.grain or "-",
        "field_count": len(node.fields),
        "incoming_count": incoming,
        "outgoing_count": outgoing,
    }


def _edge_summary(edge: Edge) -> dict[str, Any]:
    return {
        "name": edge.name,
        "from_node": edge.from_node,
        "to_node": edge.to_node,
        "relation_type": edge.relation_type,
        "time_mode": edge.time_binding.mode if edge.time_binding else "-",
    }


def _node_detail(node: Node, nodes: list[Node], edges: list[Edge]) -> dict[str, Any]:
    detail = _node_summary(node, nodes, edges)
    detail.update(
        {
            "entity_keys_text": ", ".join(node.entity_keys),
            "time_key": node.time_key or "",
            "fields_text": ", ".join(node.fields),
            "description": node.description or "",
            "incoming_edges": [edge.name for edge in edges if edge.to_node == node.name],
            "outgoing_edges": [edge.name for edge in edges if edge.from_node == node.name],
        }
    )
    return detail


def _edge_detail(edge: Edge) -> dict[str, Any]:
    binding = edge.time_binding
    return {
        "name": edge.name,
        "from_node": edge.from_node,
        "to_node": edge.to_node,
        "relation_type": edge.relation_type,
        "join_keys_text": "\n".join(f"base:{item['base']} -> source:{item['source']}" for item in edge.join_keys),
        "time_mode": binding.mode if binding else "",
        "base_time_field": binding.base_time_field if binding else "",
        "base_time_cast": binding.base_time_cast if binding else "",
        "source_time_field": binding.source_time_field if binding else "",
        "source_start_field": binding.source_start_field if binding else "",
        "source_end_field": binding.source_end_field if binding else "",
        "description": edge.description or "",
    }


def _filter_nodes(nodes: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    keyword = query.lower().strip()
    if not keyword:
        return nodes
    return [
        node
        for node in nodes
        if keyword in node["name"].lower()
        or keyword in node["table"].lower()
        or keyword in str(node["grain"]).lower()
    ]


def _filter_edges(edges: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    keyword = query.lower().strip()
    if not keyword:
        return edges
    return [
        edge
        for edge in edges
        if keyword in edge["name"].lower()
        or keyword in edge["from_node"].lower()
        or keyword in edge["to_node"].lower()
        or keyword in edge["relation_type"].lower()
        or keyword in edge["time_mode"].lower()
    ]


def _blank_node_payload() -> dict[str, Any]:
    return {
        "name": "",
        "table": "",
        "grain": "",
        "field_count": 0,
        "incoming_count": 0,
        "outgoing_count": 0,
        "entity_keys_text": "",
        "time_key": "",
        "fields_text": "",
        "description": "",
        "incoming_edges": [],
        "outgoing_edges": [],
    }


def _blank_edge_payload(nodes: list[Node]) -> dict[str, Any]:
    default_node = nodes[0].name if nodes else ""
    return {
        "name": "",
        "from_node": default_node,
        "to_node": default_node,
        "relation_type": "direct",
        "join_keys_text": "",
        "time_mode": "",
        "base_time_field": "",
        "base_time_cast": "",
        "source_time_field": "",
        "source_start_field": "",
        "source_end_field": "",
        "description": "",
    }


def _node_from_form(form: Any) -> Node:
    name = form.get("name", "").strip()
    table = form.get("table", "").strip()
    if not name:
        raise ValueError("节点名不能为空")
    if not table:
        raise ValueError("表名不能为空")
    return Node(
        name=name,
        table=table,
        entity_keys=_split_csv(form.get("entity_keys", "")),
        time_key=form.get("time_key", "").strip() or None,
        grain=form.get("grain", "").strip() or None,
        fields=_split_csv(form.get("fields", "")),
        description=form.get("description", "").strip() or None,
    )


def _edge_from_form(form: Any, existing_edge: Edge | None = None) -> Edge:
    name = form.get("name", "").strip()
    from_node = form.get("from_node", "").strip()
    to_node = form.get("to_node", "").strip()
    relation_type = form.get("relation_type", "direct").strip()
    if not name:
        raise ValueError("边名不能为空")
    if not from_node or not to_node:
        raise ValueError("边的起点和终点不能为空")
    if relation_type not in VALID_RELATION_TYPES:
        raise ValueError(f"不支持的关系类型: {relation_type}")

    time_mode = form.get("time_mode", "").strip()
    if time_mode not in VALID_TIME_MODES:
        raise ValueError(f"不支持的时间绑定模式: {time_mode}")

    time_binding = None
    if time_mode:
        existing_binding = existing_edge.time_binding if existing_edge else None
        time_binding = TimeBinding(
            mode=time_mode,
            base_time_field=form.get("base_time_field", "").strip() or None,
            source_time_field=form.get("source_time_field", "").strip() or None,
            source_start_field=form.get("source_start_field", "").strip() or None,
            source_end_field=form.get("source_end_field", "").strip() or None,
            base_time_cast=form.get("base_time_cast", "").strip() or None,
            source_time_cast=existing_binding.source_time_cast if existing_binding else None,
            available_time_field=existing_binding.available_time_field if existing_binding else None,
            allow_equal=existing_binding.allow_equal if existing_binding else True,
            lookahead_safe=existing_binding.lookahead_safe if existing_binding else True,
        )
        time_binding.validate()

    edge = Edge(
        name=name,
        from_node=from_node,
        to_node=to_node,
        relation_type=relation_type,
        source_table=existing_edge.source_table if existing_edge else None,
        join_keys=_parse_join_keys_text(form.get("join_keys", "")),
        time_binding=time_binding,
        bridge_steps=list(existing_edge.bridge_steps) if existing_edge else [],
        priority=existing_edge.priority if existing_edge else 100,
        description=form.get("description", "").strip() or None,
    )
    edge.validate()
    return edge


def _rename_edge_reference(edge: Edge, old_name: str, new_name: str) -> Edge:
    return Edge(
        name=edge.name,
        from_node=new_name if edge.from_node == old_name else edge.from_node,
        to_node=new_name if edge.to_node == old_name else edge.to_node,
        relation_type=edge.relation_type,
        source_table=edge.source_table,
        join_keys=list(edge.join_keys),
        time_binding=edge.time_binding,
        bridge_steps=list(edge.bridge_steps),
        priority=edge.priority,
        description=edge.description,
    )


def _save_graph(graph_path: Path, nodes: list[Node], edges: list[Edge]) -> None:
    _validate_graph(nodes, edges)
    payload = {
        "nodes": [_node_to_yaml_payload(node) for node in nodes],
        "edges": [_edge_to_yaml_payload(edge) for edge in edges],
    }
    graph_path.parent.mkdir(parents=True, exist_ok=True)
    graph_path.write_text(dump_yaml(payload), encoding="utf-8")


def _validate_graph(nodes: list[Node], edges: list[Edge]) -> None:
    node_names = [node.name for node in nodes]
    edge_names = [edge.name for edge in edges]
    if len(node_names) != len(set(node_names)):
        raise ValueError("节点名必须唯一")
    if len(edge_names) != len(set(edge_names)):
        raise ValueError("边名必须唯一")

    node_name_set = set(node_names)
    for edge in edges:
        if edge.from_node not in node_name_set or edge.to_node not in node_name_set:
            raise ValueError(f"边 {edge.name} 引用了不存在的节点")
        edge.validate()

    GraphRegistry(nodes, edges)


def _node_to_yaml_payload(node: Node) -> dict[str, Any]:
    return {
        "name": node.name,
        "table": node.table,
        "entity_keys": list(node.entity_keys),
        "time_key": node.time_key,
        "grain": node.grain,
        "fields": list(node.fields),
        "description": node.description,
    }


def _edge_to_yaml_payload(edge: Edge) -> dict[str, Any]:
    payload = {
        "name": edge.name,
        "from": edge.from_node,
        "to": edge.to_node,
        "relation_type": edge.relation_type,
        "source_table": edge.source_table,
        "join_keys": list(edge.join_keys),
        "priority": edge.priority,
        "description": edge.description,
    }
    if edge.time_binding:
        payload["time_binding"] = asdict(edge.time_binding)
    if edge.bridge_steps:
        payload["bridge_steps"] = [asdict(step) for step in edge.bridge_steps]
    return payload


def _build_graph_elements(nodes: list[Node], edges: list[Edge]) -> dict[str, Any]:
    return {
        "nodes": [
            {
                "data": {
                    "id": node.name,
                    "label": node.name,
                    "table": node.table,
                    "grain": node.grain or "-",
                }
            }
            for node in nodes
        ],
        "edges": [
            {
                "data": {
                    "id": edge.name,
                    "source": edge.from_node,
                    "target": edge.to_node,
                    "label": edge.time_binding.mode if edge.time_binding else edge.relation_type,
                    "relation_type": edge.relation_type,
                }
            }
            for edge in edges
        ],
    }


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


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _parse_join_keys_text(text: str) -> list[dict[str, str]]:
    join_keys: list[dict[str, str]] = []
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "->" not in line or "base:" not in line.lower() or "source:" not in line.lower():
            raise ValueError("Join Keys 格式必须为 base:xxx -> source:yyy")
        base_part, source_part = line.split("->", 1)
        base = base_part.split(":", 1)[1].strip() if ":" in base_part else ""
        source = source_part.split(":", 1)[1].strip() if ":" in source_part else ""
        if not base or not source:
            raise ValueError("Join Keys 中的 base 和 source 不能为空")
        join_keys.append({"base": base, "source": source})
    return join_keys
