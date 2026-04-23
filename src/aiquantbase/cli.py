from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from .config import dump_yaml, intent_to_dict, load_field_catalog, load_nodes_and_edges, load_query_intent
from .discovery import (
    EdgeInferenceService,
    FieldCatalogExportService,
    FieldCatalogInferenceService,
    GraphExportService,
    GraphInferenceService,
    NodeInferenceService,
    ResearchPackageService,
    SchemaDiscoveryService,
)
from .executor import ClickHouseExecutor
from .llm import DeepSeekClient
from .nl_intent import enrich_query_intent_with_aliases, normalize_query_intent_defaults, validate_query_intent_fields
from .planner import GraphRegistry, QueryPlanner
from .runtime_config import DEFAULT_RUNTIME_CONFIG_PATH, DiscoveryConfig, load_runtime_config
from .sql import SqlRenderer
from .sync.service import build_sync_service


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AIQuantBase demo CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_graph = subparsers.add_parser("validate-graph", help="Validate graph config")
    validate_graph.add_argument("graph", help="Path to graph YAML")

    show_intent = subparsers.add_parser("show-intent", help="Show parsed Query Intent")
    show_intent.add_argument("intent", help="Path to Query Intent YAML")

    plan_cmd = subparsers.add_parser("plan", help="Build Query Plan from graph and intent")
    plan_cmd.add_argument("graph", help="Path to graph YAML")
    plan_cmd.add_argument("intent", help="Path to Query Intent YAML")
    plan_cmd.add_argument("--fields", help="Optional field catalog YAML")

    render_cmd = subparsers.add_parser("render-sql", help="Render SQL from graph and intent")
    render_cmd.add_argument("graph", help="Path to graph YAML")
    render_cmd.add_argument("intent", help="Path to Query Intent YAML")
    render_cmd.add_argument("--fields", help="Optional field catalog YAML")

    ping_cmd = subparsers.add_parser("ping-datasource", help="Ping ClickHouse datasource")
    ping_cmd.add_argument(
        "--runtime",
        default=str(DEFAULT_RUNTIME_CONFIG_PATH),
        help="Path to runtime YAML",
    )

    studio_cmd = subparsers.add_parser("studio", help="Run local Graph Studio workbench")
    studio_cmd.add_argument("--host", default="127.0.0.1")
    studio_cmd.add_argument("--port", type=int, default=8000)
    studio_cmd.add_argument("--sync-project-root", help="Path to bundled sync project root")
    studio_cmd.add_argument("--debug", action=argparse.BooleanOptionalAction, default=False)

    sync_configs_cmd = subparsers.add_parser("sync-list-configs", help="List bundled sync config files")
    sync_configs_cmd.add_argument("--sync-project-root", help="Path to bundled sync project root")

    sync_tasks_cmd = subparsers.add_parser("sync-list-tasks", help="List bundled sync tasks")
    sync_tasks_cmd.add_argument("--sync-project-root", help="Path to bundled sync project root")

    sync_jobs_cmd = subparsers.add_parser("sync-list-jobs", help="List sync job records")
    sync_jobs_cmd.add_argument("--sync-project-root", help="Path to bundled sync project root")
    sync_jobs_cmd.add_argument("--status")
    sync_jobs_cmd.add_argument("--task")
    sync_jobs_cmd.add_argument("--kind")

    sync_job_cmd = subparsers.add_parser("sync-job", help="Show a sync job record")
    sync_job_cmd.add_argument("job_id")
    sync_job_cmd.add_argument("--sync-project-root", help="Path to bundled sync project root")
    sync_job_cmd.add_argument("--tail-lines", type=int, default=100)

    sync_cancel_cmd = subparsers.add_parser("sync-cancel-job", help="Cancel a running sync job")
    sync_cancel_cmd.add_argument("job_id")
    sync_cancel_cmd.add_argument("--sync-project-root", help="Path to bundled sync project root")

    sync_run_config_cmd = subparsers.add_parser("sync-run-config", help="Run a bundled sync config")
    sync_run_config_cmd.add_argument("config")
    sync_run_config_cmd.add_argument("--sync-project-root", help="Path to bundled sync project root")
    sync_run_config_cmd.add_argument("--log-level")

    sync_run_task_cmd = subparsers.add_parser("sync-run-task", help="Run a bundled sync task")
    sync_run_task_cmd.add_argument("name")
    sync_run_task_cmd.add_argument("--sync-project-root", help="Path to bundled sync project root")
    sync_run_task_cmd.add_argument("--codes", default="")
    sync_run_task_cmd.add_argument("--begin-date", type=int)
    sync_run_task_cmd.add_argument("--end-date", type=int)
    sync_run_task_cmd.add_argument("--limit", type=int, default=0)
    sync_run_task_cmd.add_argument("--force", action="store_true")
    sync_run_task_cmd.add_argument("--resume", action="store_true")
    sync_run_task_cmd.add_argument("--log-level")

    sync_run_wide_table_cmd = subparsers.add_parser("sync-run-wide-tables", help="Run bundled wide table sync jobs")
    sync_run_wide_table_cmd.add_argument("--sync-project-root", help="Path to bundled sync project root")
    sync_run_wide_table_cmd.add_argument("--wide-table-name", action="append", dest="wide_table_names", default=[])
    sync_run_wide_table_cmd.add_argument("--state-database")

    exec_cmd = subparsers.add_parser("execute-intent", help="Plan, render and execute a Query Intent")
    exec_cmd.add_argument("graph", help="Path to graph YAML")
    exec_cmd.add_argument("intent", help="Path to Query Intent YAML")
    exec_cmd.add_argument(
        "--runtime",
        default=str(DEFAULT_RUNTIME_CONFIG_PATH),
        help="Path to runtime YAML",
    )
    exec_cmd.add_argument(
        "--include-sql",
        action="store_true",
        help="Include rendered SQL in output",
    )
    exec_cmd.add_argument("--fields", help="Optional field catalog YAML")

    discover_cmd = subparsers.add_parser("discover-schema", help="Scan ClickHouse schema with allow lists")
    discover_cmd.add_argument(
        "--runtime",
        default=str(DEFAULT_RUNTIME_CONFIG_PATH),
        help="Path to runtime YAML",
    )
    discover_cmd.add_argument(
        "--allow-database",
        action="append",
        default=[],
        help="Database allowed for scanning; can be repeated",
    )
    discover_cmd.add_argument(
        "--allow-table",
        action="append",
        default=[],
        help="Table allowed for scanning; can be repeated",
    )

    infer_nodes_cmd = subparsers.add_parser("infer-nodes-demo", help="Infer candidate nodes from schema payload")
    infer_nodes_cmd.add_argument(
        "--schema-json",
        help="Path to schema JSON exported from discover-schema",
    )
    infer_nodes_cmd.add_argument(
        "--runtime",
        default=str(DEFAULT_RUNTIME_CONFIG_PATH),
        help="Path to runtime YAML",
    )
    infer_nodes_cmd.add_argument(
        "--allow-database",
        action="append",
        default=[],
        help="Database allowed for scanning; can be repeated",
    )
    infer_nodes_cmd.add_argument(
        "--allow-table",
        action="append",
        default=[],
        help="Table allowed for scanning; can be repeated",
    )

    infer_edges_cmd = subparsers.add_parser("infer-edges-demo", help="Infer candidate edges from inferred nodes")
    infer_edges_cmd.add_argument(
        "--inferred-json",
        help="Path to inferred node JSON; if omitted, infer nodes first from runtime discovery",
    )
    infer_edges_cmd.add_argument(
        "--runtime",
        default=str(DEFAULT_RUNTIME_CONFIG_PATH),
        help="Path to runtime YAML",
    )
    infer_edges_cmd.add_argument(
        "--allow-database",
        action="append",
        default=[],
        help="Database allowed for scanning; can be repeated",
    )
    infer_edges_cmd.add_argument(
        "--allow-table",
        action="append",
        default=[],
        help="Table allowed for scanning; can be repeated",
    )

    infer_graph_cmd = subparsers.add_parser("infer-graph-demo", help="Infer a candidate graph from discovery scope")
    infer_graph_cmd.add_argument(
        "--edge-json",
        help="Path to inferred edge JSON; if omitted, infer schema, nodes and edges first",
    )
    infer_graph_cmd.add_argument(
        "--runtime",
        default=str(DEFAULT_RUNTIME_CONFIG_PATH),
        help="Path to runtime YAML",
    )
    infer_graph_cmd.add_argument(
        "--allow-database",
        action="append",
        default=[],
        help="Database allowed for scanning; can be repeated",
    )
    infer_graph_cmd.add_argument(
        "--allow-table",
        action="append",
        default=[],
        help="Table allowed for scanning; can be repeated",
    )

    export_graph_cmd = subparsers.add_parser("export-graph-yaml", help="Export candidate graph as graph YAML")
    export_graph_cmd.add_argument(
        "--graph-json",
        help="Path to candidate graph JSON; if omitted, infer graph first from runtime discovery",
    )
    export_graph_cmd.add_argument(
        "--runtime",
        default=str(DEFAULT_RUNTIME_CONFIG_PATH),
        help="Path to runtime YAML",
    )
    export_graph_cmd.add_argument(
        "--allow-database",
        action="append",
        default=[],
        help="Database allowed for scanning; can be repeated",
    )
    export_graph_cmd.add_argument(
        "--allow-table",
        action="append",
        default=[],
        help="Table allowed for scanning; can be repeated",
    )
    export_graph_cmd.add_argument(
        "--output",
        help="Optional output file path for graph YAML",
    )

    infer_fields_cmd = subparsers.add_parser("infer-fields-demo", help="Infer candidate field catalog from candidate graph")
    infer_fields_cmd.add_argument(
        "--graph-json",
        help="Path to candidate graph JSON; if omitted, infer graph first from runtime discovery",
    )
    infer_fields_cmd.add_argument(
        "--runtime",
        default=str(DEFAULT_RUNTIME_CONFIG_PATH),
        help="Path to runtime YAML",
    )
    infer_fields_cmd.add_argument(
        "--allow-database",
        action="append",
        default=[],
        help="Database allowed for scanning; can be repeated",
    )
    infer_fields_cmd.add_argument(
        "--allow-table",
        action="append",
        default=[],
        help="Table allowed for scanning; can be repeated",
    )

    export_fields_cmd = subparsers.add_parser("export-fields-yaml", help="Export candidate field catalog as YAML")
    export_fields_cmd.add_argument(
        "--fields-json",
        help="Path to candidate field JSON; if omitted, infer fields first from runtime discovery",
    )
    export_fields_cmd.add_argument(
        "--runtime",
        default=str(DEFAULT_RUNTIME_CONFIG_PATH),
        help="Path to runtime YAML",
    )
    export_fields_cmd.add_argument(
        "--allow-database",
        action="append",
        default=[],
        help="Database allowed for scanning; can be repeated",
    )
    export_fields_cmd.add_argument(
        "--allow-table",
        action="append",
        default=[],
        help="Table allowed for scanning; can be repeated",
    )
    export_fields_cmd.add_argument(
        "--output",
        help="Optional output file path for fields YAML",
    )

    export_package_cmd = subparsers.add_parser(
        "export-research-package",
        help="Export candidate research package with graph and fields",
    )
    export_package_cmd.add_argument(
        "--runtime",
        default=str(DEFAULT_RUNTIME_CONFIG_PATH),
        help="Path to runtime YAML",
    )
    export_package_cmd.add_argument(
        "--allow-database",
        action="append",
        default=[],
        help="Database allowed for scanning; can be repeated",
    )
    export_package_cmd.add_argument(
        "--allow-table",
        action="append",
        default=[],
        help="Table allowed for scanning; can be repeated",
    )
    export_package_cmd.add_argument(
        "--output-dir",
        help="Optional directory to write graph.yaml, fields.yaml and package.json",
    )

    ai_graph_cmd = subparsers.add_parser(
        "infer-graph-ai-demo",
        help="Use LLM to refine discovered schema into a candidate graph package",
    )
    ai_graph_cmd.add_argument(
        "--runtime",
        default=str(DEFAULT_RUNTIME_CONFIG_PATH),
        help="Path to runtime YAML",
    )
    ai_graph_cmd.add_argument(
        "--allow-database",
        action="append",
        default=[],
        help="Database allowed for scanning; can be repeated",
    )
    ai_graph_cmd.add_argument(
        "--allow-table",
        action="append",
        default=[],
        help="Table allowed for scanning; can be repeated",
    )

    nl_intent_cmd = subparsers.add_parser(
        "nl-to-intent-demo",
        help="Use LLM to convert natural language into standard-field Query Intent",
    )
    nl_intent_cmd.add_argument("query", help="Natural language research query")
    nl_intent_cmd.add_argument("graph", help="Path to graph YAML")
    nl_intent_cmd.add_argument("--fields", required=True, help="Field catalog YAML")
    nl_intent_cmd.add_argument(
        "--runtime",
        default=str(DEFAULT_RUNTIME_CONFIG_PATH),
        help="Path to runtime YAML",
    )

    nl_query_cmd = subparsers.add_parser(
        "nl-query-demo",
        help="Convert natural language to Query Intent and execute it",
    )
    nl_query_cmd.add_argument("query", help="Natural language research query")
    nl_query_cmd.add_argument("graph", help="Path to graph YAML")
    nl_query_cmd.add_argument("--fields", required=True, help="Field catalog YAML")
    nl_query_cmd.add_argument(
        "--runtime",
        default=str(DEFAULT_RUNTIME_CONFIG_PATH),
        help="Path to runtime YAML",
    )
    nl_query_cmd.add_argument(
        "--include-sql",
        action="store_true",
        help="Include rendered SQL in output",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "validate-graph":
        nodes, edges = load_nodes_and_edges(args.graph)
        GraphRegistry(nodes, edges)
        print(f"Graph is valid: {len(nodes)} nodes, {len(edges)} edges")
        return

    if args.command == "show-intent":
        intent = load_query_intent(args.intent)
        print(dump_yaml(intent_to_dict(intent)).strip())
        return

    if args.command == "ping-datasource":
        runtime = load_runtime_config(args.runtime)
        ok = ClickHouseExecutor(runtime.datasource).ping()
        print("Datasource is reachable" if ok else "Datasource ping failed")
        return

    if args.command == "studio":
        from .server import run_server

        run_server(host=args.host, port=args.port, debug=args.debug, sync_project_root=args.sync_project_root)
        return

    if args.command == "sync-list-configs":
        integration = build_sync_service(args.sync_project_root)
        print(json.dumps({"configs": integration.list_configs()}, indent=2, ensure_ascii=False))
        return

    if args.command == "sync-list-tasks":
        integration = build_sync_service(args.sync_project_root)
        print(json.dumps(integration.list_tasks(), indent=2, ensure_ascii=False))
        return

    if args.command == "sync-list-jobs":
        integration = build_sync_service(args.sync_project_root)
        print(
            json.dumps(
                integration.list_jobs(status=args.status, task=args.task, kind=args.kind),
                indent=2,
                ensure_ascii=False,
            )
        )
        return

    if args.command == "sync-job":
        integration = build_sync_service(args.sync_project_root)
        print(json.dumps(integration.get_job(args.job_id, tail_lines=args.tail_lines), indent=2, ensure_ascii=False))
        return

    if args.command == "sync-cancel-job":
        integration = build_sync_service(args.sync_project_root)
        print(json.dumps(integration.cancel_job(args.job_id), indent=2, ensure_ascii=False))
        return

    if args.command == "sync-run-config":
        integration = build_sync_service(args.sync_project_root)
        print(
            json.dumps(
                integration.run_config(config=args.config, log_level=args.log_level),
                indent=2,
                ensure_ascii=False,
            )
        )
        return

    if args.command == "sync-run-task":
        integration = build_sync_service(args.sync_project_root)
        print(
            json.dumps(
                integration.run_task(
                    name=args.name,
                    codes=[item.strip() for item in args.codes.split(",") if item.strip()],
                    begin_date=args.begin_date,
                    end_date=args.end_date,
                    limit=args.limit,
                    force=args.force,
                    resume=args.resume,
                    log_level=args.log_level,
                ),
                indent=2,
                ensure_ascii=False,
            )
        )
        return

    if args.command == "sync-run-wide-tables":
        integration = build_sync_service(args.sync_project_root)
        print(
            json.dumps(
                integration.run_wide_tables(
                    wide_table_names=list(args.wide_table_names or []),
                    state_database=args.state_database,
                ),
                indent=2,
                ensure_ascii=False,
            )
        )
        return

    if args.command == "discover-schema":
        runtime = load_runtime_config(args.runtime)
        executor = ClickHouseExecutor(runtime.datasource)
        config = _build_discovery_config(runtime.discovery, args.allow_database, args.allow_table)
        payload = SchemaDiscoveryService(executor).discover(config)
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return

    if args.command == "infer-nodes-demo":
        if args.schema_json:
            with open(args.schema_json, "r", encoding="utf-8") as handle:
                schema_payload = json.load(handle)
        else:
            runtime = load_runtime_config(args.runtime)
            executor = ClickHouseExecutor(runtime.datasource)
            config = _build_discovery_config(runtime.discovery, args.allow_database, args.allow_table)
            schema_payload = SchemaDiscoveryService(executor).discover(config)
        inferred = NodeInferenceService().infer_nodes(schema_payload)
        print(json.dumps(inferred, indent=2, ensure_ascii=False))
        return

    if args.command == "infer-edges-demo":
        if args.inferred_json:
            with open(args.inferred_json, "r", encoding="utf-8") as handle:
                inferred_payload = json.load(handle)
        else:
            runtime = load_runtime_config(args.runtime)
            executor = ClickHouseExecutor(runtime.datasource)
            config = _build_discovery_config(runtime.discovery, args.allow_database, args.allow_table)
            schema_payload = SchemaDiscoveryService(executor).discover(config)
            inferred_payload = NodeInferenceService().infer_nodes(schema_payload)
        inferred_edges = EdgeInferenceService().infer_edges(inferred_payload)
        print(json.dumps(inferred_edges, indent=2, ensure_ascii=False))
        return

    if args.command == "infer-graph-demo":
        if args.edge_json:
            with open(args.edge_json, "r", encoding="utf-8") as handle:
                edge_payload = json.load(handle)
        else:
            runtime = load_runtime_config(args.runtime)
            executor = ClickHouseExecutor(runtime.datasource)
            config = _build_discovery_config(runtime.discovery, args.allow_database, args.allow_table)
            schema_payload = SchemaDiscoveryService(executor).discover(config)
            inferred_payload = NodeInferenceService().infer_nodes(schema_payload)
            edge_payload = EdgeInferenceService().infer_edges(inferred_payload)
        graph_payload = GraphInferenceService().infer_graph(edge_payload)
        print(json.dumps(graph_payload, indent=2, ensure_ascii=False))
        return

    if args.command == "export-graph-yaml":
        if args.graph_json:
            with open(args.graph_json, "r", encoding="utf-8") as handle:
                graph_payload = json.load(handle)
        else:
            runtime = load_runtime_config(args.runtime)
            executor = ClickHouseExecutor(runtime.datasource)
            config = _build_discovery_config(runtime.discovery, args.allow_database, args.allow_table)
            schema_payload = SchemaDiscoveryService(executor).discover(config)
            inferred_payload = NodeInferenceService().infer_nodes(schema_payload)
            edge_payload = EdgeInferenceService().infer_edges(inferred_payload)
            graph_payload = GraphInferenceService().infer_graph(edge_payload)
        exported = GraphExportService().export_graph_yaml_payload(graph_payload)
        yaml_text = dump_yaml(exported)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as handle:
                handle.write(yaml_text)
        print(yaml_text.strip())
        return

    if args.command == "infer-fields-demo":
        if args.graph_json:
            with open(args.graph_json, "r", encoding="utf-8") as handle:
                graph_payload = json.load(handle)
        else:
            runtime = load_runtime_config(args.runtime)
            executor = ClickHouseExecutor(runtime.datasource)
            config = _build_discovery_config(runtime.discovery, args.allow_database, args.allow_table)
            schema_payload = SchemaDiscoveryService(executor).discover(config)
            inferred_payload = NodeInferenceService().infer_nodes(schema_payload)
            edge_payload = EdgeInferenceService().infer_edges(inferred_payload)
            graph_payload = GraphInferenceService().infer_graph(edge_payload)
        field_payload = FieldCatalogInferenceService().infer_field_catalog(graph_payload)
        print(json.dumps(field_payload, indent=2, ensure_ascii=False))
        return

    if args.command == "export-fields-yaml":
        if args.fields_json:
            with open(args.fields_json, "r", encoding="utf-8") as handle:
                field_payload = json.load(handle)
        else:
            runtime = load_runtime_config(args.runtime)
            executor = ClickHouseExecutor(runtime.datasource)
            config = _build_discovery_config(runtime.discovery, args.allow_database, args.allow_table)
            schema_payload = SchemaDiscoveryService(executor).discover(config)
            inferred_payload = NodeInferenceService().infer_nodes(schema_payload)
            edge_payload = EdgeInferenceService().infer_edges(inferred_payload)
            graph_payload = GraphInferenceService().infer_graph(edge_payload)
            field_payload = FieldCatalogInferenceService().infer_field_catalog(graph_payload)
        exported = FieldCatalogExportService().export_field_catalog_yaml_payload(field_payload)
        yaml_text = dump_yaml(exported)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as handle:
                handle.write(yaml_text)
        print(yaml_text.strip())
        return

    if args.command == "export-research-package":
        runtime = load_runtime_config(args.runtime)
        executor = ClickHouseExecutor(runtime.datasource)
        config = _build_discovery_config(runtime.discovery, args.allow_database, args.allow_table)
        schema_payload = SchemaDiscoveryService(executor).discover(config)
        inferred_payload = NodeInferenceService().infer_nodes(schema_payload)
        edge_payload = EdgeInferenceService().infer_edges(inferred_payload)
        graph_payload = GraphInferenceService().infer_graph(edge_payload)
        field_payload = FieldCatalogInferenceService().infer_field_catalog(graph_payload)

        graph_yaml = dump_yaml(GraphExportService().export_graph_yaml_payload(graph_payload))
        fields_yaml = dump_yaml(FieldCatalogExportService().export_field_catalog_yaml_payload(field_payload))
        package_json = json.dumps(
            ResearchPackageService().build_candidate_package(graph_payload, field_payload),
            indent=2,
            ensure_ascii=False,
        )

        if args.output_dir:
            from pathlib import Path

            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / "graph.yaml").write_text(graph_yaml, encoding="utf-8")
            (output_dir / "fields.yaml").write_text(fields_yaml, encoding="utf-8")
            (output_dir / "package.json").write_text(package_json, encoding="utf-8")

        output = {
            "graph_yaml": graph_yaml,
            "fields_yaml": fields_yaml,
            "package_json": json.loads(package_json),
        }
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return

    if args.command == "infer-graph-ai-demo":
        runtime = load_runtime_config(args.runtime)
        executor = ClickHouseExecutor(runtime.datasource)
        config = _build_discovery_config(runtime.discovery, args.allow_database, args.allow_table)
        schema_payload = SchemaDiscoveryService(executor).discover(config)
        inferred_payload = NodeInferenceService().infer_nodes(schema_payload)
        edge_payload = EdgeInferenceService().infer_edges(inferred_payload)
        graph_payload = GraphInferenceService().infer_graph(edge_payload)
        field_payload = FieldCatalogInferenceService().infer_field_catalog(graph_payload)
        package_payload = ResearchPackageService().build_candidate_package(graph_payload, field_payload)

        system_prompt = (
            "You are helping build an A-share research graph package. "
            "Refine the candidate graph and field catalog into a cleaner JSON package. "
            "Keep output strictly as JSON."
        )
        user_prompt = json.dumps(
            {
                "task": "Refine this candidate research package for an A-share query middleware",
                "requirements": [
                    "Keep nodes, edges and field catalog aligned with observed schema",
                    "Preserve direct relationships already validated by rule-based inference",
                    "Do not invent tables outside observed schema",
                    "You may improve descriptions, confidence notes and field semantics",
                    "Return JSON with keys: summary, candidate_graph, candidate_field_catalog, notes",
                ],
                "candidate_package": package_payload,
            },
            ensure_ascii=False,
        )
        llm_response = DeepSeekClient(runtime.llm).chat_json(system_prompt, user_prompt)
        output = {
            "model": llm_response.model,
            "candidate_package": llm_response.parsed_json,
            "raw_text": llm_response.raw_text,
        }
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return

    if args.command == "nl-to-intent-demo":
        runtime = load_runtime_config(args.runtime)
        nodes, edges = load_nodes_and_edges(args.graph)
        field_catalog = load_field_catalog(args.fields)
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
        user_prompt = json.dumps(
            {
                "task": "Convert natural language to Query Intent",
                "natural_language_query": args.query,
                "available_nodes": [
                    {
                        "name": node.name,
                        "grain": node.grain,
                        "table": node.table,
                    }
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
                "query_intent_shape": {
                    "from": "node_name",
                    "select": ["standard_field_a", "standard_field_b"],
                    "where": {
                        "mode": "and",
                        "items": [
                            {"field": "code", "op": "=", "value": "000001.SZ"}
                        ],
                    },
                    "order_by": [
                        {"field": "trade_time", "direction": "asc"}
                    ],
                    "time_range": {
                        "field": "trade_time",
                        "start": "2026-04-01 00:00:00",
                        "end": "2026-04-07 23:59:59",
                    },
                    "page": 1,
                    "page_size": 20,
                    "safety": {"lookahead_safe": True, "strict_mode": True},
                },
                "few_shot_examples": few_shots,
            },
            ensure_ascii=False,
        )
        llm_response = DeepSeekClient(runtime.llm).chat_json(system_prompt, user_prompt)
        available_fields = {item.standard_field for item in field_catalog}
        available_fields.update(
            field_name
            for node in nodes
            for field_name in node.fields
            if field_name not in {"code", "trade_time", "trade_date", "market_code"}
        )
        query_intent = enrich_query_intent_with_aliases(
            llm_response.parsed_json if isinstance(llm_response.parsed_json, dict) else None,
            args.query,
            available_fields,
        )
        query_intent = normalize_query_intent_defaults(query_intent)
        query_intent = validate_query_intent_fields(query_intent, available_fields)
        output = {
            "model": llm_response.model,
            "query_intent": query_intent,
            "raw_text": llm_response.raw_text,
        }
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return

    if args.command == "nl-query-demo":
        runtime = load_runtime_config(args.runtime)
        nodes, edges = load_nodes_and_edges(args.graph)
        field_catalog = load_field_catalog(args.fields)
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
        user_prompt = json.dumps(
            {
                "task": "Convert natural language to Query Intent",
                "natural_language_query": args.query,
                "available_nodes": [
                    {
                        "name": node.name,
                        "grain": node.grain,
                        "table": node.table,
                    }
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
            },
            ensure_ascii=False,
        )
        llm_response = DeepSeekClient(runtime.llm).chat_json(system_prompt, user_prompt)
        available_fields = {item.standard_field for item in field_catalog}
        available_fields.update(
            field_name
            for node in nodes
            for field_name in node.fields
            if field_name not in {"code", "trade_time", "trade_date", "market_code"}
        )
        query_intent = enrich_query_intent_with_aliases(
            llm_response.parsed_json if isinstance(llm_response.parsed_json, dict) else None,
            args.query,
            available_fields,
        )
        query_intent = normalize_query_intent_defaults(query_intent)
        query_intent = validate_query_intent_fields(query_intent, available_fields)
        from tempfile import NamedTemporaryFile

        with NamedTemporaryFile("w+", suffix=".yaml", encoding="utf-8", delete=True) as handle:
            handle.write(dump_yaml(query_intent))
            handle.flush()
            intent = load_query_intent(handle.name)
        registry = GraphRegistry(nodes, edges, field_catalog=field_catalog)
        plan = QueryPlanner(registry).plan(intent)
        sql = SqlRenderer(registry).render(plan)
        result = ClickHouseExecutor(runtime.datasource).execute_sql(sql)
        output: dict[str, object] = {
            "model": llm_response.model,
            "query_intent": query_intent,
            "rows": result.rows,
            "meta": result.meta,
            "statistics": result.statistics,
            "data": result.data,
        }
        if args.include_sql:
            output["sql"] = result.sql
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return

    nodes, edges = load_nodes_and_edges(args.graph)
    field_catalog = load_field_catalog(args.fields) if getattr(args, "fields", None) else []
    registry = GraphRegistry(nodes, edges, field_catalog=field_catalog)
    planner = QueryPlanner(registry)
    intent = load_query_intent(args.intent)

    if args.command == "plan":
        plan = planner.plan(intent)
        print(json.dumps(asdict(plan), indent=2, ensure_ascii=False))
        return

    if args.command == "render-sql":
        plan = planner.plan(intent)
        print(SqlRenderer(registry).render(plan))
        return

    if args.command == "execute-intent":
        plan = planner.plan(intent)
        sql = SqlRenderer(registry).render(plan)
        runtime = load_runtime_config(args.runtime)
        result = ClickHouseExecutor(runtime.datasource).execute_sql(sql)
        output: dict[str, object] = {
            "rows": result.rows,
            "meta": result.meta,
            "statistics": result.statistics,
            "data": result.data,
        }
        if args.include_sql:
            output["sql"] = result.sql
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return

def _build_discovery_config(
    runtime_discovery: DiscoveryConfig,
    allow_databases: list[str],
    allow_tables: list[str],
) -> DiscoveryConfig:
    return DiscoveryConfig(
        allow_databases=list(allow_databases) or list(runtime_discovery.allow_databases),
        allow_tables=list(allow_tables) or list(runtime_discovery.allow_tables),
    )


if __name__ == "__main__":
    main()
