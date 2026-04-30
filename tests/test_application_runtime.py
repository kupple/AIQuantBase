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


class EmptyExecutor:
    def execute_sql(self, sql: str) -> QueryExecutionResult:
        return QueryExecutionResult(
            sql=sql,
            data=[],
            rows=0,
            statistics={},
            meta=[],
        )


class IntradayExecutor:
    def execute_sql(self, sql: str) -> QueryExecutionResult:
        if "FROM starlight.ad_market_kline_minute" in sql:
            return QueryExecutionResult(
                sql=sql,
                data=[
                    {"code": "002545.SZ", "trade_time": "2024-03-04 14:30:00", "open": 11.90, "close": 11.90},
                    {"code": "002545.SZ", "trade_time": "2024-03-04 14:31:00", "open": 12.00, "close": 12.00},
                ],
                rows=2,
                statistics={},
                meta=[],
            )
        if "high_limited" in sql and (
            "starlight.ad_market_kline_daily" in sql
            or "starlight.stock_daily_real" in sql
        ):
            return QueryExecutionResult(
                sql=sql,
                data=[
                    {"code": "002545.SZ", "trade_time": "2024-03-04 00:00:00", "high_limited": 12.00, "low_limited": 10.80},
                ],
                rows=1,
                statistics={},
                meta=[],
            )
        return QueryExecutionResult(
            sql=sql,
            data=[],
            rows=0,
            statistics={},
            meta=[],
        )


def test_application_runtime_resolve_symbols():
    runtime = ApplicationRuntime.from_defaults()
    result = runtime.resolve_symbols(["000001.SZ", "000002.SZ", "603005.SH"])

    assert result["ok"] is True
    items = {item["symbol"]: item for item in result["items"]}
    assert items["000001.SZ"]["asset_type"] == "stock"
    assert items["000002.SZ"]["asset_type"] == "stock"
    assert items["603005.SH"]["asset_type"] == "stock"


def test_application_runtime_execute_panel_time_series_profile():
    runtime = ApplicationRuntime.from_defaults()
    runtime.graph_runtime.executor = FakeExecutor()
    result = runtime.execute_query_profile(
        {
            "query_profile": "panel_time_series",
            "symbols": ["000001.SZ"],
            "fields": ["close_adj"],
            "start": "2024-01-01 00:00:00",
            "end": "2024-01-31 23:59:59",
        }
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


def test_application_runtime_execute_requirement_supports_all_a():
    runtime = ApplicationRuntime.from_defaults()
    runtime.graph_runtime.executor = FakeExecutor()
    result = runtime.execute_requirement(
        {
            "fields": ["close_adj", "open"],
            "scope": {
                "universe": "all_a",
                "freq": "1d",
                "start": "2024-01-01",
                "end": "2024-01-31",
            },
        }
    )

    assert result["ok"] is True
    assert result["resolved"]["node"] == "stock_daily_real"


def test_application_runtime_execute_membership_profile_resolves_target(tmp_path):
    membership_path = tmp_path / "membership.yaml"
    membership_path.write_text(
        """
version: 1
sources:
  - source_name: index_source
    source_kind: relation
    database: starlight
    table: ad_index_constituent
    domain: index
    taxonomy: csi_index
    security_code_field: con_code
    member_code_field: index_code
    member_name_field: index_name
members:
  - domain: index
    taxonomy: csi_index
    member_code: 399101.SZ
    member_name: 中小综指
    status: enabled
        """.strip(),
        encoding="utf-8",
    )

    runtime = ApplicationRuntime.from_defaults()
    result = runtime.execute_query_profile(
        {
            "query_profile": "membership",
            "operation": "resolve_target",
            "domain": "index",
            "member_code": "399101.SZ",
            "membership_path": membership_path,
        }
    )

    assert result["ok"] is True
    assert result["item"]["taxonomy"] == "csi_index"


def test_application_runtime_execute_panel_time_series_profile_reports_unsupported_stock_field():
    runtime = ApplicationRuntime.from_defaults()
    runtime.graph_runtime.executor = FakeExecutor()
    result = runtime.execute_query_profile(
        {
            "query_profile": "panel_time_series",
            "symbols": ["000001.SZ"],
            "fields": ["not_registered_field"],
            "start": "2024-01-01 00:00:00",
            "end": "2024-01-31 23:59:59",
        }
    )

    assert result["ok"] is False
    assert any(issue["code"] == "unsupported_field" for issue in result["issues"])
    assert result["meta"]["asset_type"] == "stock"


def test_application_runtime_execute_panel_time_series_profile_empty_result():
    runtime = ApplicationRuntime.from_defaults()
    runtime.graph_runtime.executor = EmptyExecutor()
    result = runtime.execute_query_profile(
        {
            "query_profile": "panel_time_series",
            "symbols": ["000001.SZ"],
            "fields": ["close_adj"],
            "start": "2024-01-01 00:00:00",
            "end": "2024-01-31 23:59:59",
        }
    )

    assert result["ok"] is False
    assert any(issue["code"] == "empty_result" for issue in result["issues"])
    assert result["meta"]["node"] == "stock_daily_real"


def test_application_runtime_execute_anchored_intraday_window_profile():
    runtime = ApplicationRuntime.from_defaults()
    runtime.graph_runtime.executor = IntradayExecutor()
    result = runtime.execute_query_profile(
        {
            "query_profile": "anchored_intraday_window",
            "symbols": ["002545.SZ"],
            "trading_days": ["2024-03-04"],
            "start_hhmm": "14:30",
            "end_hhmm": "14:31",
            "hhmm_list": ["14:30", "14:31"],
            "fields": ["open", "is_limit_up"],
            "asset_type": "stock",
        }
    )

    assert result["ok"] is True
    assert result["meta"]["freq"] == "1m"
    assert result["df"].shape[0] == 2
    assert result["df"]["is_limit_up"].tolist() == [0, 1]
    assert result["debug"]["intent"]["hhmm_list"] == ["14:30", "14:31"]


def test_application_runtime_execute_next_trading_day_window_profile():
    runtime = ApplicationRuntime.from_defaults()
    runtime.graph_runtime.executor = IntradayExecutor()
    result = runtime.execute_query_profile(
        {
            "query_profile": "next_trading_day_window",
            "anchors": [
                {
                    "anchor_id": "002545.SZ__2024-03-01__2024-03-04",
                    "code": "002545.SZ",
                    "signal_date": "2024-03-01",
                    "execution_date": "2024-03-04",
                }
            ],
            "start_hhmm": "14:30",
            "end_hhmm": "14:31",
            "hhmm_list": ["14:30", "14:31"],
            "fields": ["open", "is_limit_up"],
            "asset_type": "stock",
        }
    )

    assert result["ok"] is True
    assert result["meta"]["anchor_count"] == 1
    assert "execution_date" in result["df"].columns
    assert result["debug"]["intent"]["hhmm_list"] == ["14:30", "14:31"]
