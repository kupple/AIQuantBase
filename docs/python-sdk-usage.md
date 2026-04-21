# Python SDK Usage

## 目标

这份文档说明 AIQuantBase 在本地 Python 项目中的使用方式。

当前推荐分两层理解：

1. 上层应用优先使用 `ApplicationRuntime`
2. 底层图谱能力调试时再使用 `GraphRuntime`

## 1. 安装方式

如果是本地项目直接引用，推荐使用 editable install：

```bash
/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m pip install -e /Users/zhao/Desktop/git/AIQuantBase
```

如果已经在当前仓库中开发，可以直接：

```bash
/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m pip install -e .
```

### 说明

1. 使用 `-e` 后，源码变更通常会直接生效
2. 如果修改了依赖，请重新执行一次安装
3. 已运行的 Python 进程一般需要重启才能拿到最新代码

## 2. 初始化

### 上层应用推荐初始化

```python
from aiquantbase import ApplicationRuntime

runtime = ApplicationRuntime.from_defaults()
```

默认读取：

1. `config/graph.yaml`
2. `config/fields.yaml`
3. `config/runtime.local.yaml`

### 显式传路径初始化

```python
from aiquantbase import ApplicationRuntime

runtime = ApplicationRuntime(
    graph_path="/Users/zhao/Desktop/git/AIQuantBase/config/graph.yaml",
    fields_path="/Users/zhao/Desktop/git/AIQuantBase/config/fields.yaml",
    runtime_path="/Users/zhao/Desktop/git/AIQuantBase/config/runtime.local.yaml",
)
```

### 底层调试初始化

```python
from aiquantbase import GraphRuntime

runtime = GraphRuntime.from_defaults()
```

## 3. 上层应用推荐接口

当前 `ApplicationRuntime` 推荐优先使用：

1. `resolve_symbols(symbols)`
2. `resolve_best_node(...)`
3. `get_supported_fields(...)`
4. `validate_query_request(request)`
5. `query_daily(...)`
6. `query_minute(...)`
7. `query_minute_window_by_trading_day(...)`
8. `query_next_trading_day_intraday_windows(...)`
9. `build_intent_from_requirement(data_requirement)`
10. `execute_requirement(data_requirement)`

如果需要先快速理解当前启用协议规模，再决定查哪个入口，推荐补充使用：

9. `GraphRuntime.get_protocol_summary()`

## 4. 典型调用方式

### 先解析 symbol

```python
resolved = runtime.resolve_symbols(["000001.SZ", "159102.SZ"])
```

### 看字段支持清单

```python
fields = runtime.get_supported_fields(asset_type="stock", freq="1d")
```

### 校验请求

```python
validation = runtime.validate_query_request(
    {
        "symbols": ["000001.SZ"],
        "universe": None,
        "fields": ["close_adj", "market_cap"],
        "start": "2025-01-01",
        "end": "2025-01-31",
        "freq": "1d",
        "asset_type": "auto",
    }
)
```

当前也支持：

```python
validation = runtime.validate_query_request(
    {
        "symbols": [],
        "universe": "all_a",
        "fields": ["close_adj", "open", "is_st", "listed_days"],
        "start": "2024-01-01",
        "end": "2024-06-30",
        "freq": "1d",
        "asset_type": "auto",
    }
)
```

### 直接执行日频查询

```python
result = runtime.query_daily(
    symbols=["000001.SZ"],
    fields=["close_adj", "market_cap", "industry_name", "listed_days"],
    start="2025-01-01 00:00:00",
    end="2025-01-31 23:59:59",
)
```

### 直接执行分钟查询

```python
result = runtime.query_minute(
    symbols=["000001.SZ"],
    fields=["minute_close", "minute_volume"],
    start="2024-01-02 09:30:00",
    end="2024-01-02 10:30:00",
)
```

### 按交易日批量拉分钟执行窗口

```python
result = runtime.query_minute_window_by_trading_day(
    symbols=["002545.SZ"],
    trading_days=["2024-03-04"],
    start_hhmm="14:30",
    end_hhmm="14:31",
    fields=["open", "is_limit_up", "limit_up_price"],
    asset_type="stock",
)
```

### 按锚点拉下一交易日分钟执行窗口

```python
result = runtime.query_next_trading_day_intraday_windows(
    anchors=[
        {
            "anchor_id": "002545.SZ__2024-03-01__2024-03-04",
            "code": "002545.SZ",
            "signal_date": "2024-03-01",
            "execution_date": "2024-03-04",
        }
    ],
    start_hhmm="14:30",
    end_hhmm="14:31",
    fields=["open", "is_limit_up"],
    asset_type="stock",
)
```

### 执行 requirement

```python
data_requirement = {
    "fields": ["close_adj", "open", "listed_days"],
    "scope": {
        "freq": "1d",
        "start": "2024-01-01",
        "end": "2024-01-31",
        "universe": "all_a",
    },
}

result = runtime.execute_requirement(data_requirement)
```

## 5. 返回结构

### `query_daily()` / `query_minute()` / `execute_requirement()`

```python
{
    "ok": True,
    "df": DataFrame(...),
    "issues": [],
    "meta": {
        "asset_type": "...",
        "node": "...",
        "fields": [...],
        "row_count": 0,
        "symbol_count": 0,
        "empty": False,
        "empty_reason": None,
        "elapsed_ms": 123,
    },
    "debug": {
        "request": {...},
        "validation": {...},
        "resolved": {...},
        "intent": {...},
        "sql": "...",
    },
}
```

### 补充说明

当前运行时主路径已经优先走：

1. `ClickHouse -> DataFrame`

而不是：

1. `ClickHouse -> JSON -> list[dict] -> DataFrame`

所以在 Python SDK 中，最应该直接消费的是：

1. `result["df"]`

而不是先依赖中间 JSON 结构。

这对下面这些场景尤其重要：

1. `all_a` 大结果查询
2. 多字段多表 join 查询
3. 日线研究 / 回测数据准备

### 推荐使用方式

```python
result = runtime.query_daily(
    universe="all_a",
    fields=["close_adj", "open", "is_st", "listed_days"],
    start="2024-01-01 00:00:00",
    end="2024-01-31 23:59:59",
)

df = result["df"]
```

如果你只是调试或做 HTTP / CLI 输出，再把 `df` 转成记录列表即可。

### `query_minute_window_by_trading_day()` / `query_next_trading_day_intraday_windows()`

```python
{
    "ok": True,
    "df": DataFrame(...),
    "issues": [],
    "meta": {
        "asset_type": "stock",
        "freq": "1m",
        "node": "stock_minute_real",
        "fields": [...],
        "row_count": 0,
        "symbol_count": 0,
        "trading_day_count": 0,
        "empty": False,
        "empty_reason": None,
        "elapsed_ms": 0,
    },
    "debug": {
        "request": {...},
        "validation": {...},
        "resolved": {...},
        "intent": {...},
        "sql": "...",
        "sqls": [...],
    },
}
```

### 错误时

```python
{
    "ok": False,
    "df": DataFrame(),
    "issues": [...],
    "meta": {...},
    "debug": {
        "intent": None,
        "sql": "",
    },
}
```

## 6. 字段能力接口

### 获取字段清单

```python
result = runtime.get_supported_fields(asset_type="stock", freq="1d")
```

### 按节点过滤

```python
result = runtime.get_supported_fields(
    asset_type="stock",
    freq="1d",
    node="stock_daily_real",
)
```

### 按字段角色过滤

```python
result = runtime.get_supported_fields(
    asset_type="stock",
    freq="1d",
    field_role="financial_field",
)
```

### 只看派生字段

```python
result = runtime.get_supported_fields(
    asset_type="stock",
    freq="1d",
    derived_only=True,
)
```

## 7. 底层调试接口

如果你需要直接看图谱底层元数据或手动打 SQL，可以使用 `GraphRuntime`。

### 当前协议摘要

```python
from aiquantbase import GraphRuntime

runtime = GraphRuntime.from_defaults()
summary = runtime.get_protocol_summary()
```

这个接口适合先快速查看：

1. 当前启用的入口节点数量
2. 每个入口节点的粒度、资产类型、字段数量
3. 每个入口节点的示例字段

### 全量元数据目录

```python
from aiquantbase import GraphRuntime

runtime = GraphRuntime.from_defaults()
catalog = runtime.get_metadata_catalog()
```

### real 节点字段 JSON

```python
result = runtime.get_real_fields_json()
```

推荐顺序：

1. 先 `get_protocol_summary()`
2. 再 `get_real_fields_json()`
3. 最后按需 `get_supported_fields(...)`

### 渲染 Query Intent

```python
sql = runtime.render_intent(intent)
```

### 执行原生 SQL

```python
result = runtime.execute_sql("SELECT 1 AS x")
```

返回：

```python
{
    "code": 0,
    "message": "success",
    "sql": "...",
    "df": DataFrame(...),
}
```

## 8. 当前 lookahead 规则

当前 `lookahead_safe` 第一阶段只对公告/事件类字段生效。

### 目前不影响：

1. `close`
2. `high`
3. `low`
4. `volume`
5. `amount`
6. `market_cap`
7. `turnover_rate`

## 9. 最佳实践

### 推荐

1. 上层应用用 `ApplicationRuntime`
2. 先 `resolve_symbols()` / `get_supported_fields()` / `validate_query_request()`
3. 再 `query_daily()` / `query_minute()` / `execute_requirement()`
4. 保留 `debug.sql` 做审计和排查

### 不推荐

1. 上层业务直接使用底层表名
2. 上层自己拼复杂 Query Intent 作为默认路径
3. 上层自己处理图谱 join

## 10. 一句话用法

```python
from aiquantbase import ApplicationRuntime

runtime = ApplicationRuntime.from_defaults()
result = runtime.execute_requirement(data_requirement)
df = result["df"]
sql = result["debug"]["sql"]
```

## 11. 多个代码过滤

如果底层最终构造成 Query Intent，要查询多个代码，推荐使用：

```python
{
    "field": "code",
    "op": "in",
    "value": ["000001.SZ", "000002.SZ", "000004.SZ"]
}
```

## 12. 常用过滤写法

### 单值等于

```python
{"field": "code", "op": "=", "value": "000001.SZ"}
```

### 多值 in

```python
{"field": "code", "op": "in", "value": ["000001.SZ", "000002.SZ", "000004.SZ"]}
```

### 多值 not_in

```python
{"field": "code", "op": "not_in", "value": ["000001.SZ", "000002.SZ"]}
```

### 区间 between

```python
{"field": "market_cap", "op": "between", "value": [3000000, 6000000]}
```

### 判空 is_null

```python
{"field": "kzz_forced_conv_reason", "op": "is_null", "value": None}
```

### 非空 is_not_null

```python
{"field": "industry_name", "op": "is_not_null", "value": None}
```
