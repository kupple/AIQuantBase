from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .runtime_config import DatasourceConfig


@dataclass(slots=True)
class QueryExecutionResult:
    sql: str
    data: list[dict[str, Any]]
    rows: int
    statistics: dict[str, Any]
    meta: list[dict[str, Any]]


class ClickHouseExecutor:
    def __init__(self, datasource: DatasourceConfig) -> None:
        if datasource.db_type.lower() != "clickhouse":
            raise ValueError(f"Unsupported datasource type: {datasource.db_type}")
        self.datasource = datasource

    def ping(self) -> bool:
        response = self._post("SELECT 1")
        payload = json.loads(response)
        return bool(payload.get("data"))

    def execute_sql(self, sql: str) -> QueryExecutionResult:
        rendered_sql = self._normalize_sql(sql)
        response = self._post(rendered_sql)
        payload = json.loads(response)
        return QueryExecutionResult(
            sql=rendered_sql,
            data=list(payload.get("data", [])),
            rows=payload.get("rows", len(payload.get("data", []))),
            statistics=dict(payload.get("statistics", {})),
            meta=list(payload.get("meta", [])),
        )

    def _post(self, sql: str) -> str:
        url = self._build_url(default_format="JSON")
        headers = {
            "Content-Type": "text/plain; charset=utf-8",
            "X-ClickHouse-User": self.datasource.username,
            "X-ClickHouse-Key": self.datasource.password,
        }
        request = Request(
            url=url,
            data=sql.encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=30) as response:
                return response.read().decode("utf-8")
        except HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode("utf-8", errors="replace").strip()
            except Exception:
                detail = ""
            message = detail or str(exc)
            raise RuntimeError(message) from exc

    def _build_url(self, default_format: str | None = None) -> str:
        scheme = "https" if self.datasource.secure else "http"
        params: dict[str, Any] = dict(self.datasource.extra_params)
        if self.datasource.database:
            params["database"] = self.datasource.database
        if default_format and "default_format" not in params:
            params["default_format"] = default_format
        query = urlencode(params)
        base = f"{scheme}://{self.datasource.host}:{self.datasource.port}/"
        return f"{base}?{query}" if query else base

    def _normalize_sql(self, sql: str) -> str:
        return sql.strip().rstrip(";")
