from aiquantbase import ApplicationRuntime
from aiquantbase.executor import QueryExecutionResult


class FakeExecutor:
    def execute_sql(self, sql: str) -> QueryExecutionResult:
        return QueryExecutionResult(
            sql=sql,
            data=[{"code": "000001.SZ", "trade_time": "2024-01-02 00:00:00", "close": 10.0}],
            rows=1,
            statistics={},
            meta=[],
        )


def test_application_runtime_resolve_symbols():
    runtime = ApplicationRuntime.from_defaults()
    result = runtime.resolve_symbols(["000001.SZ", "159102.SZ", "000300.SH"])

    assert result["ok"] is True
    items = {item["symbol"]: item for item in result["items"]}
    assert items["000001.SZ"]["asset_type"] == "stock"
    assert items["159102.SZ"]["node"] == "etf_daily_real"
    assert items["000300.SH"]["node"] == "index_daily_real"


def test_application_runtime_query_daily():
    runtime = ApplicationRuntime.from_defaults()
    runtime.graph_runtime.executor = FakeExecutor()
    result = runtime.query_daily(
        symbols=["000001.SZ"],
        fields=["close_adj"],
        start="2024-01-01 00:00:00",
        end="2024-01-31 23:59:59",
    )

    assert result["ok"] is True
    assert result["meta"]["node"] == "stock_daily_real"
    assert hasattr(result["df"], "shape")


def test_application_runtime_execute_requirement():
    runtime = ApplicationRuntime.from_defaults()
    runtime.graph_runtime.executor = FakeExecutor()
    result = runtime.execute_requirement(
        {
            "fields": ["close_adj"],
            "scope": {
                "symbols": ["000001.SZ"],
                "freq": "1d",
                "start": "2024-01-01",
                "end": "2024-01-31",
            },
        }
    )

    assert result["ok"] is True
    assert result["resolved"]["node"] == "stock_daily_real"


def test_application_runtime_query_daily_for_etf():
    runtime = ApplicationRuntime.from_defaults()
    runtime.graph_runtime.executor = FakeExecutor()
    result = runtime.query_daily(
        symbols=["159102.SZ"],
        fields=["close_adj"],
        start="2024-01-01 00:00:00",
        end="2024-01-31 23:59:59",
    )

    assert result["ok"] is True
    assert result["meta"]["node"] == "etf_daily_real"
    assert "EXTRA_ETF" in result["debug"]["sql"]
