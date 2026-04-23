from aiquantbase.executor import ClickHouseExecutor
from aiquantbase.runtime_config import DatasourceConfig


def make_datasource() -> DatasourceConfig:
    return DatasourceConfig(
        id="primary",
        name="Primary",
        db_type="clickhouse",
        host="127.0.0.1",
        port=8123,
        database="demo",
        username="default",
        password="secret",
        secure=False,
        extra_params={"session_id": "abc"},
    )


def test_executor_builds_clickhouse_url():
    executor = ClickHouseExecutor(make_datasource())

    assert executor._build_url() == "http://127.0.0.1:8123/?session_id=abc&database=demo"


def test_executor_appends_json_format():
    executor = ClickHouseExecutor(make_datasource())

    assert executor._ensure_json_format("SELECT 1") == "SELECT 1\nFORMAT JSON"
    assert executor._ensure_json_format("SELECT 1 FORMAT JSON") == "SELECT 1 FORMAT JSON"


def test_executor_requires_host():
    datasource = make_datasource()
    datasource.host = ""
    executor = ClickHouseExecutor(datasource)

    try:
        executor._build_url()
    except RuntimeError as exc:
        assert "datasource.host is empty" in str(exc)
    else:
        raise AssertionError("Expected missing host to raise RuntimeError")
