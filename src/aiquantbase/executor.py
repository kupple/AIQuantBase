from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
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
        response = self._post("SELECT 1 FORMAT JSON")
        payload = json.loads(response)
        return bool(payload.get("data"))

    def execute_sql(self, sql: str) -> QueryExecutionResult:
        rendered_sql = self._ensure_json_format(sql)
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
        url = self._build_url()
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
        with urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8")

    def _build_url(self) -> str:
        scheme = "https" if self.datasource.secure else "http"
        params: dict[str, Any] = dict(self.datasource.extra_params)
        if self.datasource.database:
            params["database"] = self.datasource.database
        query = urlencode(params)
        base = f"{scheme}://{self.datasource.host}:{self.datasource.port}/"
        return f"{base}?{query}" if query else base

    def _ensure_json_format(self, sql: str) -> str:
        stripped = sql.strip().rstrip(";")
        upper = stripped.upper()
        if " FORMAT " in upper:
            return stripped
        return stripped + "\nFORMAT JSON"
