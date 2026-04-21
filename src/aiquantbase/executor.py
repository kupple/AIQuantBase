from __future__ import annotations

from io import BytesIO
import json
from dataclasses import dataclass
from time import perf_counter
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

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

    def execute_sql_df(self, sql: str) -> pd.DataFrame:
        return self.execute_sql_df_timed(sql)['df']

    def execute_sql_df_timed(self, sql: str) -> dict[str, Any]:
        rendered_sql = self._normalize_sql(sql)
        started = perf_counter()
        response = self._post_bytes(
            f"{rendered_sql}\nFORMAT CSVWithNames",
            default_format=None,
        )
        fetched = perf_counter()
        if not response.strip():
            return {
                'sql': rendered_sql,
                'df': pd.DataFrame(),
                'timings': {
                    'request_seconds': round(fetched - started, 6),
                    'parse_seconds': 0.0,
                    'total_seconds': round(fetched - started, 6),
                },
                'response_bytes': len(response),
                'row_count': 0,
                'parser_engine': None,
            }
        df, parser_engine = self._read_csv_bytes(response)
        parsed = perf_counter()
        return {
            'sql': rendered_sql,
            'df': df,
            'timings': {
                'request_seconds': round(fetched - started, 6),
                'parse_seconds': round(parsed - fetched, 6),
                'total_seconds': round(parsed - started, 6),
            },
            'response_bytes': len(response),
            'row_count': int(len(df.index)),
            'parser_engine': parser_engine,
        }

    def execute_sql_raw(self, sql: str) -> str:
        rendered_sql = self._normalize_sql(sql)
        return self._post(rendered_sql)

    def _post(self, sql: str) -> str:
        response = self._post_bytes(sql, default_format="JSON")
        return response.decode("utf-8")

    def _post_bytes(self, sql: str, *, default_format: str | None) -> bytes:
        url = self._build_url(default_format=default_format)
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
                return response.read()
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

    def _read_csv_bytes(self, response: bytes) -> tuple[pd.DataFrame, str]:
        common_kwargs = {
            'na_values': ["\\N"],
            'keep_default_na': True,
        }
        try:
            df = pd.read_csv(
                BytesIO(response),
                engine='pyarrow',
                **common_kwargs,
            )
            return df, 'pyarrow'
        except Exception:
            df = pd.read_csv(
                BytesIO(response),
                low_memory=False,
                **common_kwargs,
            )
            return df, 'c'
