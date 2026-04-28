from __future__ import annotations

import re
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import jsonify, request

from sync_data_system.config_paths import resolve_sync_plan_root, resolve_sync_spec_dir
from .wide_table import build_wide_table_export_payload, export_wide_table_yaml, list_wide_tables


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SYNC_PROJECT_ROOT = PROJECT_ROOT / "sync_data_system"
DATE_FIELD_CANDIDATES = (
    "trade_time",
    "trade_date",
    "ann_date",
    "end_date",
    "report_date",
    "change_date",
    "list_date",
    "in_date",
    "out_date",
    "date",
)
CONFIG_FILE_RE = re.compile(r"^run_sync.*\.toml$", re.IGNORECASE)
SPEC_FILE_RE = re.compile(r"^[\w.-]+$")


class SyncIntegration:
    def __init__(self, sync_project_root: str | Path | None = None) -> None:
        self.sync_project_root = Path(sync_project_root or DEFAULT_SYNC_PROJECT_ROOT).resolve()
        self.sync_config_root = resolve_sync_plan_root(self.sync_project_root)
        self.sync_spec_dir = resolve_sync_spec_dir(self.sync_project_root)
        self._job_manager: Any | None = None

    def ensure_sync_project_root(self) -> Path:
        if not self.sync_project_root.exists():
            raise FileNotFoundError(f"sync project root not found: {self.sync_project_root}")
        return self.sync_project_root

    def _ensure_vendor_path(self) -> Path:
        root = self.ensure_sync_project_root()
        parent_str = str(root.parent)
        if parent_str not in sys.path:
            sys.path.insert(0, parent_str)
        return root

    def _job_manager_instance(self):
        if self._job_manager is None:
            self._ensure_vendor_path()
            from sync_data_system.service.job_manager import SyncJobManager

            self._job_manager = SyncJobManager(self.sync_project_root)
        return self._job_manager

    def list_configs(self) -> list[str]:
        if not self.sync_config_root.exists():
            return []
        return sorted(
            [
                path.name
                for path in self.sync_config_root.glob("run_sync*.toml")
                if path.is_file()
            ]
        )

    def read_config(self, name: str) -> dict[str, Any]:
        file_path = self._resolve_config_path(name)
        return {"name": file_path.name, "content": file_path.read_text(encoding="utf-8")}

    def write_config(self, name: str, content: str) -> dict[str, Any]:
        self.sync_config_root.mkdir(parents=True, exist_ok=True)
        file_path = self._resolve_config_path(name)
        file_path.write_text(content, encoding="utf-8")
        return {"ok": True, "name": file_path.name}

    def delete_config(self, name: str) -> dict[str, Any]:
        file_path = self._resolve_config_path(name)
        if not file_path.is_file():
            raise FileNotFoundError(f"sync config not found: {name}")
        file_path.unlink()
        return {"ok": True, "name": file_path.name}

    def list_exported_wide_tables(
        self,
        graph_path: str | Path | None = None,
    ) -> dict[str, Any]:
        self.sync_spec_dir.mkdir(parents=True, exist_ok=True)
        items = []
        for item in list_wide_tables(graph_path=graph_path):
            file_path = self.sync_spec_dir / f"{item['name']}.yaml"
            exported = file_path.is_file()
            items.append(
                {
                    **item,
                    "exported": exported,
                    "exported_path": str(file_path) if exported else "",
                    "exported_at": _mtime_iso(file_path) if exported else "",
                }
            )
        return {
            "items": items,
            "sync_spec_dir": str(self.sync_spec_dir),
        }

    def export_wide_table_spec(
        self,
        *,
        design_id: str,
        name: str,
        graph_path: str | Path | None = None,
        fields_path: str | Path | None = None,
    ) -> dict[str, Any]:
        clean_name = self._validate_spec_name(name)
        self.sync_spec_dir.mkdir(parents=True, exist_ok=True)
        yaml_text = export_wide_table_yaml(
            design_id,
            graph_path=graph_path,
            fields_path=fields_path,
        )
        file_path = self.sync_spec_dir / f"{clean_name}.yaml"
        file_path.write_text(yaml_text, encoding="utf-8")
        return {
            "ok": True,
            "exported_path": str(file_path),
            "content": yaml_text,
        }

    def run_wide_table_inline(
        self,
        *,
        design_id: str,
        graph_path: str | Path | None = None,
        fields_path: str | Path | None = None,
        state_database: str | None = None,
    ) -> dict[str, Any]:
        self._ensure_vendor_path()
        from sync_data_system.clickhouse_client import ClickHouseConfig
        from sync_data_system.wide_table_sync import build_wide_table_metadata, run_wide_table_sync_payloads_with_clickhouse

        payload = build_wide_table_export_payload(
            design_id,
            graph_path=graph_path,
            fields_path=fields_path,
        )
        spec_name = str(((payload.get("wide_table") or {}).get("name")) or design_id).strip() or design_id
        spec_path = f"inline://{spec_name}.yaml"
        metadata = build_wide_table_metadata(payload, spec_path=spec_path)
        results = run_wide_table_sync_payloads_with_clickhouse(
            {spec_path: payload},
            {spec_path: metadata},
            config=ClickHouseConfig.from_env(),
            state_database=state_database,
        )
        return {
            "ok": all(item.status == "success" for item in results),
            "results": [item.__dict__ for item in results],
        }

    def read_wide_table_spec_file(self, name: str) -> dict[str, Any]:
        file_path = self._resolve_spec_path(name)
        return {
            "name": file_path.name,
            "content": file_path.read_text(encoding="utf-8"),
            "exported_path": str(file_path),
            "exported_at": _mtime_iso(file_path),
        }

    def sync_table_status(self) -> dict[str, Any]:
        self._ensure_vendor_path()
        from sync_data_system.clickhouse_client import ClickHouseConfig, create_clickhouse_client

        job_manager = self._job_manager_instance()
        task_items = job_manager.list_registered_tasks()
        tasks_by_target: dict[str, list[str]] = {}
        for item in task_items:
            target = str(item.get("target") or "").strip()
            if not target:
                continue
            tasks_by_target.setdefault(target, []).append(str(item.get("name") or ""))
        targets = sorted(tasks_by_target)
        if not targets:
            return {"items": []}

        def build_connection_error(detail: str) -> dict[str, Any]:
            return {
                "detail": detail,
                "items": [
                    {
                        "target": target,
                        "database": "",
                        "latest_date": "",
                        "row_count": 0,
                        "last_update_time": "",
                        "status": "connection_error",
                        "tasks": tasks_by_target.get(target, []),
                        "error": detail,
                    }
                    for target in targets
                ],
            }

        connection = None
        try:
            config = ClickHouseConfig.from_env()
            connection = create_clickhouse_client(config)
            table_rows = connection.query_rows(
                """
                SELECT database, name
                FROM system.tables
                WHERE name IN {targets:Array(String)}
                ORDER BY database, name
                """,
                {"targets": targets},
            )
            target_lookup: dict[str, tuple[str, str]] = {}
            for row in table_rows:
                if len(row) < 2:
                    continue
                database = str(row[0])
                table = str(row[1])
                target_lookup.setdefault(table, (database, table))

            items: list[dict[str, Any]] = []
            resolved_targets = [(database, table) for _, (database, table) in target_lookup.items()]
            columns_by_table: dict[tuple[str, str], list[str]] = {}
            parts_by_table: dict[tuple[str, str], tuple[Any, ...]] = {}
            if resolved_targets:
                dbs = sorted({database for database, _ in resolved_targets})
                table_names = sorted({table for _, table in resolved_targets})
                column_rows = connection.query_rows(
                    """
                    SELECT database, table, name
                    FROM system.columns
                    WHERE database IN {databases:Array(String)}
                      AND table IN {tables:Array(String)}
                    ORDER BY database, table, position
                    """,
                    {"databases": dbs, "tables": table_names},
                )
                for row in column_rows:
                    if len(row) < 3:
                        continue
                    key = (str(row[0]), str(row[1]))
                    columns_by_table.setdefault(key, []).append(str(row[2]))

                part_rows = connection.query_rows(
                    """
                    SELECT
                      database,
                      table,
                      sum(rows) AS row_count,
                      max(modification_time) AS last_update_time
                    FROM system.parts
                    WHERE active = 1
                      AND database IN {databases:Array(String)}
                      AND table IN {tables:Array(String)}
                    GROUP BY database, table
                    """,
                    {"databases": dbs, "tables": table_names},
                )
                parts_by_table = {
                    (str(row[0]), str(row[1])): row
                    for row in part_rows
                    if len(row) >= 4
                }

            for target in targets:
                task_names = tasks_by_target.get(target, [])
                if target not in target_lookup:
                    items.append(
                        {
                            "target": target,
                            "database": "",
                            "latest_date": "",
                            "row_count": 0,
                            "last_update_time": "",
                            "status": "missing",
                            "tasks": task_names,
                        }
                    )
                    continue

                database, table = target_lookup[target]
                columns = columns_by_table.get((database, table), [])
                latest_field = next((field for field in DATE_FIELD_CANDIDATES if field in columns), None)
                latest_date = ""
                if latest_field:
                    latest_value = connection.query_value(f"SELECT toString(max({latest_field})) FROM {database}.{table}")
                    latest_date = str(latest_value or "")

                part_row = parts_by_table.get((database, table))
                row_count = int(part_row[2]) if part_row else 0
                last_update_time = str(part_row[3]) if part_row and part_row[3] is not None else ""
                items.append(
                    {
                        "target": target,
                        "database": database,
                        "latest_date": latest_date,
                        "row_count": row_count,
                        "last_update_time": last_update_time,
                        "status": "ready" if latest_date else "warning",
                        "tasks": task_names,
                        }
                    )
            return {"items": items}
        except Exception as exc:
            detail = str(exc).strip() or exc.__class__.__name__
            return build_connection_error(detail)
        finally:
            if connection is not None:
                connection.close()

    def list_tasks(self) -> dict[str, Any]:
        job_manager = self._job_manager_instance()
        return {
            "tasks": job_manager.list_tasks(),
            "registered_tasks": job_manager.list_registered_tasks(),
        }

    def get_task_metadata(self, task_name: str) -> dict[str, Any]:
        job_manager = self._job_manager_instance()
        items = {item["name"]: item for item in job_manager.list_registered_tasks()}
        if task_name in items:
            return items[task_name]
        if task_name in job_manager.list_tasks():
            return {
                "name": task_name,
                "source": None,
                "target": None,
                "input_resolver": None,
                "request_fields": ["name", "codes", "begin_date", "end_date", "limit", "force", "resume", "log_level"],
                "probe_fields": [],
            }
        raise KeyError(task_name)

    def list_wide_table_specs(self) -> dict[str, Any]:
        self._ensure_vendor_path()
        from sync_data_system.wide_table_sync import list_wide_table_metadata, wide_table_metadata_to_dict

        return {
            "specs": [wide_table_metadata_to_dict(item) for item in list_wide_table_metadata(self.sync_project_root)]
        }

    def get_wide_table_spec(self, name: str) -> dict[str, Any]:
        self._ensure_vendor_path()
        from sync_data_system.wide_table_sync import get_wide_table_metadata, wide_table_metadata_to_dict

        metadata = get_wide_table_metadata(self.sync_project_root, name)
        if metadata is None:
            raise FileNotFoundError(f"wide table spec not found: {name}")
        return wide_table_metadata_to_dict(metadata)

    def plan_wide_tables(
        self,
        *,
        clickhouse_live: bool = False,
        write_state: bool = False,
        state_database: str | None = None,
    ) -> dict[str, Any]:
        self._ensure_vendor_path()
        if write_state and not clickhouse_live:
            raise ValueError("write_state requires clickhouse_live")
        from sync_data_system.clickhouse_client import ClickHouseConfig
        from sync_data_system.wide_table_sync import load_and_plan_specs, load_and_plan_specs_with_clickhouse, wide_table_plan_to_dict

        if clickhouse_live:
            plans = load_and_plan_specs_with_clickhouse(
                self.sync_project_root,
                config=ClickHouseConfig.from_env(),
                state_database=state_database,
                write_state=write_state,
            )
        else:
            plans = load_and_plan_specs(self.sync_project_root)
        return {"plans": [wide_table_plan_to_dict(plan) for plan in plans]}

    def list_wide_table_states(self, state_database: str | None = None) -> dict[str, Any]:
        self._ensure_vendor_path()
        from sync_data_system.clickhouse_client import ClickHouseConfig, create_clickhouse_client
        from sync_data_system.wide_table_sync import WideTableSyncStateRepository, wide_table_state_to_dict

        config = ClickHouseConfig.from_env()
        connection = create_clickhouse_client(config)
        try:
            repository = WideTableSyncStateRepository(
                connection,
                database=state_database or config.database,
            )
            repository.ensure_table()
            states = repository.load_states()
            return {"states": [wide_table_state_to_dict(state) for state in states]}
        finally:
            connection.close()

    def get_wide_table_state(self, name: str, state_database: str | None = None) -> dict[str, Any]:
        self._ensure_vendor_path()
        from sync_data_system.clickhouse_client import ClickHouseConfig, create_clickhouse_client
        from sync_data_system.wide_table_sync import WideTableSyncStateRepository, wide_table_state_to_dict

        config = ClickHouseConfig.from_env()
        connection = create_clickhouse_client(config)
        try:
            repository = WideTableSyncStateRepository(
                connection,
                database=state_database or config.database,
            )
            repository.ensure_table()
            state = repository.load_state(name)
            if state is None:
                raise FileNotFoundError(f"wide table state not found: {name}")
            return wide_table_state_to_dict(state)
        finally:
            connection.close()

    def run_wide_tables(self, *, wide_table_names: list[str] | None = None, state_database: str | None = None) -> dict[str, Any]:
        job_manager = self._job_manager_instance()
        job = job_manager.create_wide_table_job(
            wide_table_names=wide_table_names,
            state_database=state_database,
        )
        return {
            **asdict(job),
            "wide_table_names": list(wide_table_names or []),
        }

    def list_jobs(self, *, status: str | None = None, task: str | None = None, kind: str | None = None) -> dict[str, Any]:
        job_manager = self._job_manager_instance()
        return {
            "jobs": [
                asdict(job)
                for job in job_manager.list_jobs(status=status, task=task, kind=kind)
            ]
        }

    def get_job(self, job_id: str, tail_lines: int = 100) -> dict[str, Any]:
        job_manager = self._job_manager_instance()
        job = job_manager.get_job(job_id)
        return {
            **asdict(job),
            "logs_tail": job_manager.read_job_log(job_id, tail_lines=tail_lines),
        }

    def get_job_logs(self, job_id: str, tail_lines: int = 200) -> dict[str, Any]:
        job_manager = self._job_manager_instance()
        return {"job_id": job_id, "logs": job_manager.read_job_log(job_id, tail_lines=tail_lines)}

    def run_config(self, *, config: str, log_level: str | None = None) -> dict[str, Any]:
        job_manager = self._job_manager_instance()
        job = job_manager.create_config_job(config, log_level=log_level)
        return {
            **asdict(job),
            "config": config,
        }

    def run_task(
        self,
        *,
        name: str,
        codes: list[str] | None = None,
        day: int | None = None,
        begin_date: int | None = None,
        end_date: int | None = None,
        year: int | None = None,
        quarter: int | None = None,
        year_type: str | None = None,
        limit: int = 0,
        force: bool = False,
        resume: bool = False,
        adjustflag: str | None = None,
        frequency: str | None = None,
        log_level: str | None = None,
    ) -> dict[str, Any]:
        task_name = str(name or "").strip()
        if not task_name:
            raise ValueError("name 不能为空。")
        job_manager = self._job_manager_instance()
        registered_tasks = {item["name"]: item for item in job_manager.list_registered_tasks()}
        if task_name in registered_tasks:
            job = job_manager.create_registered_task_job(
                task=task_name,
                codes=list(codes or []),
                day=day,
                begin_date=begin_date,
                end_date=end_date,
                year=year,
                quarter=quarter,
                year_type=year_type,
                limit=limit,
                force=force,
                resume=resume,
                adjustflag=adjustflag,
                frequency=frequency,
                log_level=log_level,
            )
            task_metadata = registered_tasks[task_name]
        else:
            job = job_manager.create_task_job(
                task=task_name,
                codes=list(codes or []),
                begin_date=begin_date,
                end_date=end_date,
                limit=limit,
                force=force,
                resume=resume,
                log_level=log_level,
            )
            task_metadata = None
        return {
            **asdict(job),
            "task_metadata": task_metadata,
        }

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        job_manager = self._job_manager_instance()
        return asdict(job_manager.cancel_job(job_id))

    def _resolve_config_path(self, name: str) -> Path:
        self.sync_config_root.mkdir(parents=True, exist_ok=True)
        file_name = str(name or "").strip()
        if not CONFIG_FILE_RE.match(file_name):
            raise ValueError("invalid config file name")
        return self.sync_config_root / file_name

    def _resolve_spec_path(self, name: str) -> Path:
        clean_name = self._validate_spec_name(name)
        candidates = [self.sync_spec_dir / clean_name]
        if not clean_name.endswith(".yaml") and not clean_name.endswith(".yml"):
            candidates.extend([self.sync_spec_dir / f"{clean_name}.yaml", self.sync_spec_dir / f"{clean_name}.yml"])
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        raise FileNotFoundError(f"wide table sync spec not found: {name}")

    def _validate_spec_name(self, name: str) -> str:
        clean_name = str(name or "").strip()
        if not SPEC_FILE_RE.match(clean_name):
            raise ValueError("invalid file name")
        return clean_name


def build_sync_integration(sync_project_root: str | Path | None = None) -> SyncIntegration:
    return SyncIntegration(sync_project_root=sync_project_root)


def register_sync_routes(app, integration: SyncIntegration) -> None:
    @app.get("/api/sync-configs")
    def sync_configs():
        try:
            return jsonify({"items": integration.list_configs()})
        except Exception as exc:
            return _json_error(exc)

    @app.post("/api/sync-configs")
    def sync_configs_save():
        try:
            payload = request.get_json(force=True) or {}
            return jsonify(
                integration.write_config(
                    str(payload.get("name") or ""),
                    str(payload.get("content") or ""),
                )
            )
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync-configs/<path:name>")
    def sync_config_detail(name: str):
        try:
            return jsonify(integration.read_config(name))
        except Exception as exc:
            return _json_error(exc)

    @app.delete("/api/sync-configs/<path:name>")
    def sync_config_delete(name: str):
        try:
            return jsonify(integration.delete_config(name))
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync-wide-tables")
    def sync_wide_tables():
        try:
            return jsonify(
                integration.list_exported_wide_tables(
                    graph_path=request.args.get("graph_path") or None,
                )
            )
        except Exception as exc:
            return _json_error(exc)

    @app.post("/api/sync-wide-tables")
    def sync_wide_tables_export():
        try:
            payload = request.get_json(force=True) or {}
            design_id = str(payload.get("id") or "").strip()
            name = str(payload.get("name") or "").strip()
            if not design_id or not name:
                raise ValueError("id and name are required")
            return jsonify(
                integration.export_wide_table_spec(
                    design_id=design_id,
                    name=name,
                    graph_path=payload.get("graph_path") or None,
                    fields_path=payload.get("fields_path") or None,
                )
            )
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync-wide-tables/<path:name>")
    def sync_wide_table_file(name: str):
        try:
            return jsonify(integration.read_wide_table_spec_file(name))
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync-table-status")
    def sync_table_status():
        try:
            return jsonify(integration.sync_table_status())
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync/meta/tasks")
    def sync_meta_tasks():
        try:
            return jsonify(integration.list_tasks())
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync/meta/tasks/<task_name>")
    def sync_meta_task_detail(task_name: str):
        try:
            return jsonify(integration.get_task_metadata(task_name))
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync/meta/configs")
    def sync_meta_configs():
        try:
            return jsonify({"configs": integration.list_configs()})
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync/wide-tables/specs")
    def sync_specs():
        try:
            return jsonify(integration.list_wide_table_specs())
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync/wide-tables/specs/<wide_table_name>")
    def sync_spec_detail(wide_table_name: str):
        try:
            return jsonify(integration.get_wide_table_spec(wide_table_name))
        except Exception as exc:
            return _json_error(exc)

    @app.post("/api/sync/wide-tables/plan")
    def sync_plan():
        try:
            payload = request.get_json(force=True) or {}
            return jsonify(
                integration.plan_wide_tables(
                    clickhouse_live=bool(payload.get("clickhouse_live")),
                    write_state=bool(payload.get("write_state")),
                    state_database=payload.get("state_database") or None,
                )
            )
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync/wide-tables/states")
    def sync_states():
        try:
            return jsonify(integration.list_wide_table_states(state_database=request.args.get("state_database") or None))
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync/wide-tables/states/<wide_table_name>")
    def sync_state_detail(wide_table_name: str):
        try:
            return jsonify(
                integration.get_wide_table_state(
                    wide_table_name,
                    state_database=request.args.get("state_database") or None,
                )
            )
        except Exception as exc:
            return _json_error(exc)

    @app.post("/api/sync/wide-tables/run")
    def sync_run_wide_tables():
        try:
            payload = request.get_json(force=True) or {}
            return jsonify(
                integration.run_wide_tables(
                    wide_table_names=list(payload.get("wide_table_names") or []),
                    state_database=payload.get("state_database") or None,
                )
            )
        except Exception as exc:
            return _json_error(exc)

    @app.post("/api/sync/wide-tables/run-inline")
    def sync_run_wide_table_inline():
        try:
            payload = request.get_json(force=True) or {}
            design_id = str(payload.get("id") or "").strip()
            if not design_id:
                raise ValueError("id is required")
            return jsonify(
                integration.run_wide_table_inline(
                    design_id=design_id,
                    graph_path=payload.get("graph_path") or None,
                    fields_path=payload.get("fields_path") or None,
                    state_database=payload.get("state_database") or None,
                )
            )
        except Exception as exc:
            return _json_error(exc)

    @app.post("/api/sync/wide-tables/run/<wide_table_name>")
    def sync_run_single_wide_table(wide_table_name: str):
        try:
            return jsonify(
                integration.run_wide_tables(
                    wide_table_names=[wide_table_name],
                    state_database=request.args.get("state_database") or None,
                )
            )
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync/jobs")
    def sync_jobs():
        try:
            return jsonify(
                integration.list_jobs(
                    status=request.args.get("status") or None,
                    task=request.args.get("task") or None,
                    kind=request.args.get("kind") or None,
                )
            )
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync/jobs/<job_id>")
    def sync_job_detail(job_id: str):
        try:
            tail_lines = int(request.args.get("tail_lines") or 100)
            return jsonify(integration.get_job(job_id, tail_lines=max(1, min(tail_lines, 2000))))
        except Exception as exc:
            return _json_error(exc)

    @app.get("/api/sync/jobs/<job_id>/logs")
    def sync_job_logs(job_id: str):
        try:
            tail_lines = int(request.args.get("tail_lines") or 200)
            return jsonify(integration.get_job_logs(job_id, tail_lines=max(1, min(tail_lines, 5000))))
        except Exception as exc:
            return _json_error(exc)

    @app.post("/api/sync/jobs/run-config")
    def sync_run_config():
        try:
            payload = request.get_json(force=True) or {}
            return jsonify(
                integration.run_config(
                    config=str(payload.get("config") or ""),
                    log_level=(str(payload.get("log_level")) if payload.get("log_level") is not None else None),
                )
            )
        except Exception as exc:
            return _json_error(exc)

    @app.post("/api/sync/jobs/run-task")
    def sync_run_task():
        try:
            payload = request.get_json(force=True) or {}
            return jsonify(
                integration.run_task(
                    name=str(payload.get("name") or payload.get("task") or ""),
                    codes=list(payload.get("codes") or []),
                    day=payload.get("day"),
                    begin_date=payload.get("begin_date"),
                    end_date=payload.get("end_date"),
                    year=payload.get("year"),
                    quarter=payload.get("quarter"),
                    year_type=(str(payload.get("year_type")) if payload.get("year_type") is not None else None),
                    limit=int(payload.get("limit") or 0),
                    force=bool(payload.get("force")),
                    resume=bool(payload.get("resume")),
                    adjustflag=(str(payload.get("adjustflag")) if payload.get("adjustflag") is not None else None),
                    frequency=(str(payload.get("frequency")) if payload.get("frequency") is not None else None),
                    log_level=(str(payload.get("log_level")) if payload.get("log_level") is not None else None),
                )
            )
        except Exception as exc:
            return _json_error(exc)

    @app.post("/api/sync/jobs/<job_id>/cancel")
    def sync_cancel_job(job_id: str):
        try:
            return jsonify(integration.cancel_job(job_id))
        except Exception as exc:
            return _json_error(exc)


def _mtime_iso(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).astimezone().isoformat()


def _json_error(exc: Exception):
    detail = str(exc)
    if isinstance(exc, KeyError):
        return jsonify({"detail": detail or "not found"}), 404
    if isinstance(exc, FileNotFoundError):
        return jsonify({"detail": detail}), 404
    if "another sync job is running" in detail:
        return jsonify({"detail": detail}), 409
    if isinstance(exc, ValueError):
        return jsonify({"detail": detail}), 400
    return jsonify({"detail": detail or exc.__class__.__name__}), 400
