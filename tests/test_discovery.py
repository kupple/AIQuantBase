from aiquantbase.discovery import SchemaDiscoveryService
from aiquantbase.executor import QueryExecutionResult
from aiquantbase.runtime_config import DiscoveryConfig, load_runtime_config


class FakeExecutor:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute_sql(self, sql: str) -> QueryExecutionResult:
        self.queries.append(sql)
        if "FROM system.tables" in sql:
            data = [{"database": "starlight", "name": "ad_market_kline_daily"}]
        else:
            data = [{"database": "starlight", "table": "ad_market_kline_daily", "name": "close"}]
        return QueryExecutionResult(sql=sql, data=data, rows=len(data), statistics={}, meta=[])


def test_load_discovery_config_from_runtime():
    config = load_runtime_config("config/runtime.example.yaml").discovery

    assert "starlight" in config.allow_databases
    assert "ad_adj_factor" in config.allow_tables


def test_schema_discovery_service_builds_queries():
    service = SchemaDiscoveryService(FakeExecutor())
    result = service.discover(
        DiscoveryConfig(
            allow_databases=["starlight"],
            allow_tables=["ad_market_kline_daily"],
        )
    )

    assert result["tables"][0]["name"] == "ad_market_kline_daily"
    assert result["columns"][0]["name"] == "close"
