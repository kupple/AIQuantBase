#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Decorator-based sync task registry for API execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from sync_data_system.amazingdata_sdk_provider import AmazingDataSDKConfig, AmazingDataSDKProvider
from sync_data_system.base_data import BaseData
from sync_data_system.clickhouse_client import ClickHouseConfig
from sync_data_system.data_models import normalize_code_list
from sync_data_system.info_data import InfoData
from sync_data_system.market_data import MarketData
from sync_data_system import run_sync as run_sync_module


RUN_TASK_REQUEST_FIELDS = (
    "name",
    "codes",
    "begin_date",
    "end_date",
    "limit",
    "force",
    "resume",
    "log_level",
)

PROBE_PUBLIC_FIELDS = (
    "name",
    "source",
    "target",
    "job_id",
    "runtime_path",
    "input_codes",
    "input_begin_date",
    "input_end_date",
    "limit",
    "force",
    "resume",
    "log_level",
    "codes",
    "begin_date",
    "end_date",
    "row_count",
    "status",
    "message",
    "log_path",
)


@dataclass(frozen=True)
class TaskDefinition:
    name: str
    source: str
    target: str
    input_resolver: Optional[str]
    handler: Callable[["SyncTaskProbe"], Any]


@dataclass
class SyncTaskProbe:
    name: str
    source: str
    target: str
    job_id: str
    project_root: Path
    log_path: Path
    runtime_path: Optional[str] = None
    input_codes: list[str] = field(default_factory=list)
    input_begin_date: Optional[int] = None
    input_end_date: Optional[int] = None
    limit: int = 0
    force: bool = False
    resume: bool = False
    log_level: Optional[str] = None
    codes: list[str] = field(default_factory=list)
    begin_date: Optional[int] = None
    end_date: Optional[int] = None
    row_count: int = 0
    status: str = "created"
    message: str = ""
    context: Any = None

    def log(self, message: str) -> None:
        timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(f"{timestamp} {message}\n")

    def set_status(self, status: str, message: str = "") -> None:
        self.status = status
        if message:
            self.message = message
            self.log(message)

    def set_row_count(self, row_count: int) -> None:
        self.row_count = int(row_count)

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source": self.source,
            "target": self.target,
            "job_id": self.job_id,
            "runtime_path": self.runtime_path,
            "input_codes": list(self.input_codes),
            "input_begin_date": self.input_begin_date,
            "input_end_date": self.input_end_date,
            "limit": self.limit,
            "force": self.force,
            "resume": self.resume,
            "log_level": self.log_level,
            "codes": list(self.codes),
            "begin_date": self.begin_date,
            "end_date": self.end_date,
            "row_count": self.row_count,
            "status": self.status,
            "message": self.message,
            "log_path": str(self.log_path),
        }


class SyncTaskRegistry:
    def __init__(self) -> None:
        self._tasks: dict[str, TaskDefinition] = {}
        self._resolvers: dict[str, Callable[[SyncTaskProbe], None]] = {}

    def register_task(self, name: str, source: str, target: str, input_resolver: Optional[str], handler) -> Callable:
        if name in self._tasks:
            raise ValueError(f"duplicate task definition: {name}")
        self._tasks[name] = TaskDefinition(
            name=name,
            source=source,
            target=target,
            input_resolver=input_resolver,
            handler=handler,
        )
        return handler

    def register_resolver(self, name: str, resolver: Callable[[SyncTaskProbe], None]) -> Callable[[SyncTaskProbe], None]:
        if name in self._resolvers:
            raise ValueError(f"duplicate input resolver: {name}")
        self._resolvers[name] = resolver
        return resolver

    def get_task(self, name: str) -> TaskDefinition:
        if name not in self._tasks:
            raise KeyError(name)
        return self._tasks[name]

    def list_tasks(self) -> list[TaskDefinition]:
        return [self._tasks[key] for key in sorted(self._tasks.keys())]

    def list_task_metadata(self) -> list[dict[str, Any]]:
        return [
            {
                "name": task.name,
                "source": task.source,
                "target": task.target,
                "input_resolver": task.input_resolver,
                "request_fields": list(RUN_TASK_REQUEST_FIELDS),
                "probe_fields": list(PROBE_PUBLIC_FIELDS),
            }
            for task in self.list_tasks()
        ]

    def get_task_metadata(self, name: str) -> dict[str, Any]:
        task = self.get_task(name)
        return {
            "name": task.name,
            "source": task.source,
            "target": task.target,
            "input_resolver": task.input_resolver,
            "request_fields": list(RUN_TASK_REQUEST_FIELDS),
            "probe_fields": list(PROBE_PUBLIC_FIELDS),
        }

    def resolve_inputs(self, probe: SyncTaskProbe) -> SyncTaskProbe:
        task = self.get_task(probe.name)
        if not task.input_resolver:
            probe.codes = list(probe.input_codes)
            probe.begin_date = probe.input_begin_date
            probe.end_date = probe.input_end_date
            return probe
        resolver = self._resolvers[task.input_resolver]
        resolver(probe)
        return probe


TASK_REGISTRY = SyncTaskRegistry()


def sync_task(name: str, source: str, target: str, input_resolver: Optional[str] = None):
    def decorator(handler):
        return TASK_REGISTRY.register_task(name=name, source=source, target=target, input_resolver=input_resolver, handler=handler)

    return decorator


def register_input_resolver(name: str):
    def decorator(resolver):
        return TASK_REGISTRY.register_resolver(name=name, resolver=resolver)

    return decorator


@dataclass
class ApiSyncExecutionContext:
    sdk_config: AmazingDataSDKConfig
    provider: AmazingDataSDKProvider
    base_data: BaseData
    info_data: InfoData
    market_data: MarketData

    def close(self) -> None:
        try:
            self.market_data.close()
        except Exception:
            pass
        try:
            self.info_data.close()
        except Exception:
            pass
        try:
            self.base_data.close()
        except Exception:
            pass
        self.provider.close()


def build_amazingdata_context(runtime_path: Optional[str] = None) -> ApiSyncExecutionContext:
    sdk_config = AmazingDataSDKConfig.from_env(runtime_path=runtime_path)
    clickhouse_config = ClickHouseConfig.from_env()
    provider = AmazingDataSDKProvider(sdk_config)
    base_data = BaseData.from_clickhouse_config(clickhouse_config, sync_provider=provider)
    info_data = InfoData.from_clickhouse_config(clickhouse_config, sync_provider=provider)
    market_data = MarketData.from_clickhouse_config(clickhouse_config, sync_provider=provider)
    return ApiSyncExecutionContext(
        sdk_config=sdk_config,
        provider=provider,
        base_data=base_data,
        info_data=info_data,
        market_data=market_data,
    )


@register_input_resolver("run_sync_defaults")
def resolve_run_sync_defaults(probe: SyncTaskProbe) -> None:
    if probe.context is None:
        raise RuntimeError("probe.context is required for run_sync_defaults")

    task = probe.name
    ignores_date_range = run_sync_module.task_ignores_date_range(task)
    begin_date, end_date = run_sync_module.resolve_date_window(
        provider=probe.context.provider,
        begin_date=None if ignores_date_range else probe.input_begin_date,
        end_date=None if ignores_date_range else probe.input_end_date,
    )

    codes: list[str] = []
    if run_sync_module.task_requires_code_list(task):
        if task == "backward_factor":
            codes = run_sync_module.resolve_backward_factor_code_list(
                base_data=probe.context.base_data,
                raw_codes=",".join(probe.input_codes),
                limit=probe.limit,
            )
        elif task in {"industry_constituent", "industry_weight", "industry_daily"}:
            codes = run_sync_module.resolve_industry_code_list(
                info_data=probe.context.info_data,
                raw_codes=",".join(probe.input_codes),
                limit=probe.limit,
            )
        else:
            codes = run_sync_module.resolve_code_list(
                base_data=probe.context.base_data,
                task=task,
                raw_codes=",".join(probe.input_codes),
                limit=probe.limit,
                local_path=probe.context.sdk_config.local_path,
                end_date=end_date,
            )
        task_spec = run_sync_module.TaskRunSpec(
            task=task,
            codes_raw=",".join(probe.input_codes),
            begin_date=None if ignores_date_range else probe.input_begin_date,
            end_date=None if ignores_date_range else probe.input_end_date,
            limit=probe.limit,
            force=probe.force,
            resume=probe.resume,
        )
        codes = run_sync_module.filter_code_list_for_resume(
            context=probe.context,
            task_spec=task_spec,
            code_list=codes,
            begin_date=begin_date,
            end_date=end_date,
        )

    probe.codes = codes
    probe.begin_date = begin_date
    probe.end_date = end_date


@register_input_resolver("market_kline_defaults")
def resolve_market_kline_defaults(probe: SyncTaskProbe) -> None:
    if probe.context is None:
        raise RuntimeError("probe.context is required for market_kline_defaults")

    probe.begin_date, probe.end_date = run_sync_module.resolve_date_window(
        provider=probe.context.provider,
        begin_date=probe.input_begin_date,
        end_date=probe.input_end_date,
    )
    if probe.input_codes:
        codes = normalize_code_list(probe.input_codes)
        if probe.limit and probe.limit > 0:
            codes = codes[: probe.limit]
    else:
        codes = run_sync_module.resolve_market_kline_code_list(
            base_data=probe.context.base_data,
            task=probe.name,
            limit=probe.limit,
        )
    task_spec = run_sync_module.TaskRunSpec(
        task=probe.name,
        codes_raw=",".join(probe.input_codes),
        begin_date=probe.input_begin_date,
        end_date=probe.input_end_date,
        limit=probe.limit,
        force=probe.force,
        resume=probe.resume,
    )
    probe.codes = run_sync_module.filter_code_list_for_resume(
        context=probe.context,
        task_spec=task_spec,
        code_list=codes,
        begin_date=probe.begin_date,
        end_date=probe.end_date,
    )


def _execute_via_run_sync(probe: SyncTaskProbe) -> int:
    task_spec = run_sync_module.TaskRunSpec(
        task=probe.name,
        codes_raw=",".join(probe.codes),
        begin_date=probe.begin_date,
        end_date=probe.end_date,
        limit=probe.limit,
        force=probe.force,
        resume=probe.resume,
    )
    return run_sync_module.execute_task_spec(probe.context, task_spec)


def _register_run_sync_task(name: str, target: str, input_resolver: str = "run_sync_defaults") -> None:
    @sync_task(
        name=name,
        source="amazingdata",
        target=target,
        input_resolver=input_resolver,
    )
    def _generated_task(probe: SyncTaskProbe) -> int:
        inserted = _execute_via_run_sync(probe)
        probe.set_row_count(inserted)
        return inserted


for _task_name in run_sync_module.TASK_CHOICES:
    _register_run_sync_task(
        _task_name,
        run_sync_module.TASK_TARGET_TABLE_MAP[_task_name],
        input_resolver=run_sync_module.TASK_INPUT_RESOLVER_MAP.get(_task_name, "run_sync_defaults"),
    )


def create_probe(
    *,
    task_name: str,
    job_id: str,
    project_root: Path,
    log_path: Path,
    runtime_path: Optional[str] = None,
    codes: Optional[list[str]] = None,
    begin_date: Optional[int] = None,
    end_date: Optional[int] = None,
    limit: int = 0,
    force: bool = False,
    resume: bool = False,
    log_level: Optional[str] = None,
) -> SyncTaskProbe:
    definition = TASK_REGISTRY.get_task(task_name)
    return SyncTaskProbe(
        name=definition.name,
        source=definition.source,
        target=definition.target,
        job_id=job_id,
        project_root=project_root,
        log_path=log_path,
        runtime_path=runtime_path,
        input_codes=list(codes or []),
        input_begin_date=begin_date,
        input_end_date=end_date,
        limit=limit,
        force=force,
        resume=resume,
        log_level=log_level,
    )


__all__ = [
    "ApiSyncExecutionContext",
    "SyncTaskProbe",
    "TASK_REGISTRY",
    "TaskDefinition",
    "build_amazingdata_context",
    "create_probe",
    "register_input_resolver",
    "sync_task",
]
