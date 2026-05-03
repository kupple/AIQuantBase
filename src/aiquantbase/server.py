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

from .capabilities import (
    build_capability_preview,
    load_capability_workspace,
    resolve_mode_extension_contract,
    set_capability_registry_enabled,
    upsert_mode_capability,
    upsert_provider_node_semantic,
    delete_mode_capability,
)
from .config import dump_yaml, load_field_catalog, load_nodes_and_edges, load_yaml
from .discovery import SchemaDiscoveryService
from .executor import ClickHouseExecutor
from .llm import DeepSeekClient
from .membership import (
    DEFAULT_MEMBERSHIP_PATH,
    delete_source,
    filter_symbols_by_membership,
    import_membership_payload,
    list_domains,
    list_members,
    list_relations,
    list_sources,
    list_taxonomies,
    load_membership_workspace,
    membership_workspace_summary,
    parse_import_payload,
    parse_membership_filter_payload,
    patch_relation,
    preview_source_rows_with_lookup,
    query_membership,
    upsert_member,
    upsert_relation,
    upsert_source,
    upsert_taxonomy,
)
from .nl_intent import enrich_query_intent_with_aliases, normalize_query_intent_defaults, validate_query_intent_fields
from .planner import GraphRegistry, QueryPlanner
from .runtime import GraphRuntime
from .runtime_config import DEFAULT_RUNTIME_CONFIG_PATH, load_runtime_config
from .sql import SqlRenderer
from .sync.service import build_sync_service
from .sync_integration import DEFAULT_SYNC_PROJECT_ROOT, register_sync_routes
from .wide_table import (
    delete_wide_table,
    export_wide_table_yaml,
    get_wide_table_summary,
    list_wide_tables,
    upsert_wide_table,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_GRAPH_PATH = PROJECT_ROOT / "config" / "graph.yaml"
DEFAULT_FIELDS_PATH = PROJECT_ROOT / "config" / "fields.yaml"



def create_app(
    default_graph_path: Path | None = None,
    default_fields_path: Path | None = None,
    sync_project_root: Path | None = None,
) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = "aiquantbase-dev-secret"
    app.config["DEFAULT_GRAPH_PATH"] = Path(default_graph_path or DEFAULT_GRAPH_PATH)
    app.config["DEFAULT_FIELDS_PATH"] = Path(default_fields_path or DEFAULT_FIELDS_PATH)
    app.config["DEFAULT_SYNC_PROJECT_ROOT"] = Path(sync_project_root or DEFAULT_SYNC_PROJECT_ROOT)

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
                    "membership_path": str(DEFAULT_MEMBERSHIP_PATH),
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
                    "/api/capabilities/workspace",
                    "/api/capabilities/provider-node",
                    "/api/capabilities/registry-capability",
                    "/api/capabilities/mode-capability",
                    "/api/capabilities/mode-capability/delete",
                    "/api/capabilities/mode-extension-contract",
                    "/api/capabilities/preview",
                    "/api/membership/workspace",
                    "/api/membership/domains",
                    "/api/membership/sources",
                    "/api/membership/taxonomies",
                    "/api/membership/members",
                    "/api/membership/relations",
                    "/api/wide-tables",
                    "/api/wide-tables/export",
                    "/api/fields/ai-notes",
                    "/api/query/execute",
                    "/api/query/nl",
                    "/api/sync-configs",
                    "/api/sync-wide-tables",
                    "/api/sync-table-status",
                    "/api/sync/meta/tasks",
                    "/api/sync/jobs",
                ],
            }
        )

    @app.get("/api/wide-tables")
    def wide_tables():
        graph_path = _resolve_workspace_path(request.args.get("graph_path") or _default_graph_path())
        return jsonify(
            {
                "workspace": {
                    "graph_path": str(graph_path),
                },
                "summary": get_wide_table_summary(graph_path=graph_path),
                "items": list_wide_tables(
                    graph_path=graph_path,
                    source_node=request.args.get("source_node") or None,
                ),
            }
        )

    @app.post("/api/wide-tables")
    def save_wide_table():
        payload = request.get_json(force=True)
        graph_path = _resolve_workspace_path(payload.get("graph_path") or _default_graph_path())
        item = upsert_wide_table(payload.get("wide_table") or payload, graph_path=graph_path)
        return jsonify({"ok": True, "item": item, "summary": get_wide_table_summary(graph_path=graph_path)})

    @app.delete("/api/wide-tables")
    def remove_wide_table():
        graph_path = _resolve_workspace_path(request.args.get("graph_path") or _default_graph_path())
        design_id = request.args.get("id") or ""
        item = delete_wide_table(design_id, graph_path=graph_path)
        return jsonify({"ok": True, "item": item, "summary": get_wide_table_summary(graph_path=graph_path)})

    @app.get("/api/wide-tables/export")
    def export_wide_table():
        design_id = request.args.get("id") or ""
        yaml_text = export_wide_table_yaml(
            design_id,
            graph_path=request.args.get("graph_path") or _default_graph_path(),
            fields_path=request.args.get("fields_path") or _default_fields_path(),
        )
        return jsonify({"ok": True, "yaml": yaml_text})

    @app.get("/api/membership/workspace")
    def membership_workspace():
        membership_path = _resolve_membership_path(request.args.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        workspace = load_membership_workspace(membership_path)
        return jsonify(
            {
                "workspace": {
                    "membership_path": str(membership_path),
                },
                "summary": membership_workspace_summary(membership_path),
                "domains": list_domains(membership_path),
                "sources": list_sources(membership_path),
                "taxonomies": list_taxonomies(membership_path),
                "members": list_members(membership_path),
                "relations": list_relations(membership_path),
            }
        )

    @app.get("/api/membership/domains")
    def membership_domains():
        membership_path = _resolve_membership_path(request.args.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        return jsonify({"items": list_domains(membership_path)})

    @app.get("/api/membership/taxonomies")
    def membership_taxonomies():
        membership_path = _resolve_membership_path(request.args.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        domain = request.args.get("domain") or None
        return jsonify({"items": list_taxonomies(membership_path, domain=domain)})

    @app.get("/api/membership/sources")
    def membership_sources():
        membership_path = _resolve_membership_path(request.args.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        return jsonify(
            {
                "items": list_sources(
                    membership_path,
                    domain=request.args.get("domain") or None,
                    taxonomy=request.args.get("taxonomy") or None,
                    status=request.args.get("status") or None,
                )
            }
        )

    @app.get("/api/membership/members")
    def membership_members():
        membership_path = _resolve_membership_path(request.args.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        domain = request.args.get("domain") or None
        taxonomy = request.args.get("taxonomy") or None
        return jsonify({"items": list_members(membership_path, domain=domain, taxonomy=taxonomy)})

    @app.get("/api/membership/relations")
    def membership_relations():
        membership_path = _resolve_membership_path(request.args.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        return jsonify(
            {
                "items": list_relations(
                    membership_path,
                    security_code=request.args.get("security_code") or None,
                    domain=request.args.get("domain") or None,
                    taxonomy=request.args.get("taxonomy") or None,
                    member_code=request.args.get("member_code") or None,
                    as_of_date=request.args.get("as_of_date") or None,
                    security_type=request.args.get("security_type") or None,
                )
            }
        )

    @app.get("/api/membership/query")
    def membership_query():
        membership_path = _resolve_membership_path(request.args.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        security_code = str(request.args.get("security_code") or "").strip()
        as_of_date = str(request.args.get("as_of_date") or "").strip()
        if not security_code or not as_of_date:
            return jsonify({"ok": False, "message": "security_code and as_of_date are required", "items": []}), 400
        items = query_membership(
            security_code,
            as_of_date=as_of_date,
            path=membership_path,
            security_type=request.args.get("security_type") or None,
        )
        return jsonify({"ok": True, "items": items, "count": len(items)})

    @app.post("/api/membership/filter")
    def membership_filter():
        payload = request.get_json(force=True)
        membership_path = _resolve_membership_path(payload.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        as_of_date = str(payload.get("as_of_date") or "").strip()
        if not as_of_date:
            return jsonify({"ok": False, "message": "as_of_date is required", "symbols": []}), 400
        membership_filter_payload = payload.get("filter") or {}
        if payload.get("raw_text"):
            membership_filter_payload = parse_membership_filter_payload(str(payload.get("raw_text") or ""))
        result = filter_symbols_by_membership(
            membership_filter_payload,
            as_of_date=as_of_date,
            path=membership_path,
            security_type=payload.get("security_type") or None,
        )
        return jsonify(result)

    @app.post("/api/membership/sources")
    def membership_upsert_source():
        payload = request.get_json(force=True)
        membership_path = _resolve_membership_path(payload.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        item = upsert_source(payload.get("source") or payload, membership_path)
        return jsonify({"ok": True, "item": item, "summary": membership_workspace_summary(membership_path)})

    @app.delete("/api/membership/sources")
    def membership_delete_source():
        membership_path = _resolve_membership_path(request.args.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        source_id = request.args.get("id") or ""
        item = delete_source(source_id, membership_path)
        return jsonify({"ok": True, "item": item, "summary": membership_workspace_summary(membership_path)})

    @app.post("/api/membership/source-preview")
    def membership_source_preview():
        payload = request.get_json(force=True)
        source = payload.get("source") or {}
        runtime_path = _resolve_workspace_path(payload.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH)
        runtime = load_runtime_config(runtime_path)
        database = str(source.get("database") or "").strip()
        table = str(source.get("table") or "").strip()
        if not database or not table:
            return jsonify({"ok": False, "message": "source.database and source.table are required"}), 400

        limit = int(payload.get("limit") or 100)
        sql = f"SELECT * FROM {database}.{table} LIMIT {max(1, min(limit, 200))}"
        executor = ClickHouseExecutor(runtime.datasource)
        result = executor.execute_sql(sql)

        lookup_rows: list[dict[str, Any]] = []
        lookup_sql = ""
        lookup_database = str(source.get("lookup_database") or "").strip()
        lookup_table = str(source.get("lookup_table") or "").strip()
        lookup_target_field = str(source.get("lookup_target_field") or "").strip()
        main_member_field = str(source.get("lookup_source_field") or source.get("member_code_field") or "").strip()
        if lookup_database and lookup_table and lookup_target_field and main_member_field:
            member_codes = sorted(
                {
                    str(row.get(main_member_field) or "").strip()
                    for row in result.data
                    if isinstance(row, dict) and str(row.get(main_member_field) or "").strip()
                }
            )
            if member_codes:
                in_values = ", ".join(_quote_clickhouse_string(value) for value in member_codes)
                lookup_sql = (
                    f"SELECT * FROM {lookup_database}.{lookup_table} "
                    f"WHERE {lookup_target_field} IN ({in_values}) "
                    f"LIMIT {max(1, min(len(member_codes) * 5, 1000))}"
                )
                lookup_result = executor.execute_sql(lookup_sql)
                lookup_rows = lookup_result.data

        preview = preview_source_rows_with_lookup(source, result.data, lookup_rows)
        return jsonify(
            {
                "ok": True,
                "sql": result.sql,
                "lookup_sql": lookup_sql,
                "rows": result.rows,
                "raw_rows": result.data,
                "lookup_rows": lookup_rows,
                **preview,
            }
        )

    @app.post("/api/membership/taxonomies")
    def membership_upsert_taxonomy():
        payload = request.get_json(force=True)
        membership_path = _resolve_membership_path(payload.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        item = upsert_taxonomy(payload.get("taxonomy") or payload, membership_path)
        return jsonify({"ok": True, "item": item, "summary": membership_workspace_summary(membership_path)})

    @app.post("/api/membership/members")
    def membership_upsert_member():
        payload = request.get_json(force=True)
        membership_path = _resolve_membership_path(payload.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        item = upsert_member(payload.get("member") or payload, membership_path)
        return jsonify({"ok": True, "item": item, "summary": membership_workspace_summary(membership_path)})

    @app.post("/api/membership/relations")
    def membership_upsert_relation():
        payload = request.get_json(force=True)
        membership_path = _resolve_membership_path(payload.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        item = upsert_relation(payload.get("relation") or payload, membership_path)
        return jsonify({"ok": True, "item": item, "summary": membership_workspace_summary(membership_path)})

    @app.patch("/api/membership/relations")
    def membership_patch_relation():
        payload = request.get_json(force=True)
        membership_path = _resolve_membership_path(payload.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        item = patch_relation(payload.get("relation") or payload, membership_path)
        return jsonify({"ok": True, "item": item, "summary": membership_workspace_summary(membership_path)})

    @app.post("/api/membership/import")
    def membership_import():
        payload = request.get_json(force=True)
        membership_path = _resolve_membership_path(payload.get("membership_path") or DEFAULT_MEMBERSHIP_PATH)
        import_payload = parse_import_payload(str(payload.get("raw_text") or ""))
        workspace = import_membership_payload(import_payload, membership_path)
        return jsonify(
            {
                "ok": True,
                "summary": membership_workspace_summary(membership_path),
                "counts": {
                    "taxonomies": len(workspace["taxonomies"]),
                    "members": len(workspace["members"]),
                    "relations": len(workspace["relations"]),
                },
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
                    "runtime_state": asdict(runtime.runtime_state),
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
        runtime_payload = load_yaml(runtime_path) if runtime_path.exists() else {}
        if not isinstance(runtime_payload, dict):
            runtime_payload = {}
        runtime_payload.update(
            {
                "llm": payload.get("runtime", {}).get("llm") or asdict(current_runtime.llm),
                "datasource": payload.get("runtime", {}).get("datasource") or asdict(current_runtime.datasource),
                "discovery": payload.get("runtime", {}).get("discovery") or asdict(current_runtime.discovery),
                "runtime_state": payload.get("runtime", {}).get("runtime_state") or asdict(current_runtime.runtime_state),
            }
        )
        runtime_yaml = dump_yaml(runtime_payload)

        graph_path.parent.mkdir(parents=True, exist_ok=True)
        fields_path.parent.mkdir(parents=True, exist_ok=True)
        runtime_path.parent.mkdir(parents=True, exist_ok=True)
        graph_path.write_text(graph_yaml, encoding="utf-8")
        fields_path.write_text(fields_yaml, encoding="utf-8")
        runtime_path.write_text(runtime_yaml, encoding="utf-8")

        return jsonify({"ok": True})

    @app.get("/api/schema/databases")
    def list_databases():
        try:
            runtime = load_runtime_config(request.args.get("runtime_path") or DEFAULT_RUNTIME_CONFIG_PATH)
            executor = ClickHouseExecutor(runtime.datasource)
            rows = executor.execute_sql("SELECT name FROM system.databases ORDER BY name").data
            return jsonify({"items": rows})
        except Exception as exc:
            return jsonify({"detail": str(exc).strip() or exc.__class__.__name__}), 400

    @app.get("/api/schema/tables")
    def list_tables():
        try:
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
        except Exception as exc:
            return jsonify({"detail": str(exc).strip() or exc.__class__.__name__}), 400

    @app.get("/api/schema/columns")
    def list_columns():
        try:
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
        except Exception as exc:
            return jsonify({"detail": str(exc).strip() or exc.__class__.__name__}), 400

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

    @app.get("/api/capabilities/workspace")
    def capabilities_workspace():
        return jsonify(
            load_capability_workspace(
                capability_root=request.args.get("capability_root") or None,
                alphablocks_root=request.args.get("alphablocks_root") or None,
                provider_manifest_path=request.args.get("provider_manifest_path") or None,
                mode_registry_path=request.args.get("mode_registry_path") or None,
                query_templates_path=request.args.get("query_templates_path") or None,
                graph_path=request.args.get("graph_path") or None,
            )
        )

    @app.post("/api/capabilities/provider-node")
    def capabilities_upsert_provider_node():
        payload = request.get_json(force=True)
        try:
            result = upsert_provider_node_semantic(payload)
            workspace = load_capability_workspace(
                capability_root=payload.get("capability_root") or None,
                alphablocks_root=payload.get("alphablocks_root") or None,
                provider_manifest_path=payload.get("provider_manifest_path") or None,
                mode_registry_path=payload.get("mode_registry_path") or None,
                query_templates_path=payload.get("query_templates_path") or None,
                graph_path=payload.get("graph_path") or None,
            )
            return jsonify({"ok": True, "result": result, "workspace": workspace})
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

    @app.post("/api/capabilities/registry-capability")
    def capabilities_set_registry_capability():
        payload = request.get_json(force=True)
        try:
            result = set_capability_registry_enabled(payload)
            workspace = load_capability_workspace(
                capability_root=payload.get("capability_root") or None,
                alphablocks_root=payload.get("alphablocks_root") or None,
                provider_manifest_path=payload.get("provider_manifest_path") or None,
                mode_registry_path=payload.get("mode_registry_path") or None,
                query_templates_path=payload.get("query_templates_path") or None,
                graph_path=payload.get("graph_path") or None,
            )
            return jsonify({"ok": True, "result": result, "workspace": workspace})
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

    @app.post("/api/capabilities/mode-capability")
    def capabilities_upsert_mode_capability():
        payload = request.get_json(force=True)
        try:
            result = upsert_mode_capability(payload)
            workspace = load_capability_workspace(
                capability_root=payload.get("capability_root") or None,
                alphablocks_root=payload.get("alphablocks_root") or None,
                provider_manifest_path=payload.get("provider_manifest_path") or None,
                mode_registry_path=payload.get("mode_registry_path") or None,
                query_templates_path=payload.get("query_templates_path") or None,
                graph_path=payload.get("graph_path") or None,
            )
            return jsonify({"ok": True, "result": result, "workspace": workspace})
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

    @app.post("/api/capabilities/mode-capability/delete")
    def capabilities_delete_mode_capability():
        payload = request.get_json(force=True)
        try:
            result = delete_mode_capability(payload)
            workspace = load_capability_workspace(
                capability_root=payload.get("capability_root") or None,
                alphablocks_root=payload.get("alphablocks_root") or None,
                provider_manifest_path=payload.get("provider_manifest_path") or None,
                mode_registry_path=payload.get("mode_registry_path") or None,
                query_templates_path=payload.get("query_templates_path") or None,
                graph_path=payload.get("graph_path") or None,
            )
            return jsonify({"ok": True, "result": result, "workspace": workspace})
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

    @app.post("/api/capabilities/mode-extension-contract")
    def capabilities_mode_extension_contract():
        payload = request.get_json(force=True)
        try:
            return jsonify(resolve_mode_extension_contract(payload))
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

    @app.post("/api/capabilities/preview")
    def capabilities_preview():
        payload = request.get_json(force=True)
        try:
            return jsonify(build_capability_preview(payload))
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc), "resolved_queries": []}), 400

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

    register_sync_routes(
        app,
        build_sync_service(app.config["DEFAULT_SYNC_PROJECT_ROOT"]),
    )

    return app


def run_server(
    host: str = "127.0.0.1",
    port: int = 8000,
    debug: bool = False,
    sync_project_root: str | Path | None = None,
) -> None:
    app = create_app(sync_project_root=Path(sync_project_root) if sync_project_root else None)
    app.run(
        host=host,
        port=port,
        debug=debug,
        use_reloader=bool(debug),
    )


def _default_graph_path() -> Path:
    return Path(current_app.config["DEFAULT_GRAPH_PATH"])


def _default_fields_path() -> Path:
    return Path(current_app.config["DEFAULT_FIELDS_PATH"])


def _resolve_workspace_path(path_like: str | Path) -> Path:
    path = Path(path_like)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _resolve_membership_path(path_like: str | Path) -> Path:
    return _resolve_workspace_path(path_like)


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


def _quote_clickhouse_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"
