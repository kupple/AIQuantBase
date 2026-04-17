# ApplicationRuntime Examples

## 目标

这份文档提供面向上层应用的 `ApplicationRuntime` 调用示例。

适合：

1. `AlphaBlocks`
2. `protocol_core`
3. 其他需要直接消费 `sql + DataFrame` 的本地 Python 项目

## 1. 初始化

```python
from aiquantbase import ApplicationRuntime

runtime = ApplicationRuntime.from_defaults()
```

如果需要显式指定配置：

```python
from aiquantbase import ApplicationRuntime

runtime = ApplicationRuntime(
    graph_path="/Users/zhao/Desktop/git/AIQuantBase/config/graph.yaml",
    fields_path="/Users/zhao/Desktop/git/AIQuantBase/config/fields.yaml",
    runtime_path="/Users/zhao/Desktop/git/AIQuantBase/config/runtime.local.yaml",
)
```

## 2. 先解析 symbol

```python
result = runtime.resolve_symbols(["000001.SZ", "159102.SZ", "123085.SZ"])
print(result)
```

适合在上层先判断：

1. 这是股票还是 ETF
2. 应该走什么查询入口

## 3. 查询股票日频

```python
result = runtime.query_daily(
    symbols=["000001.SZ"],
    fields=["close_adj", "market_cap", "industry_name"],
    start="2025-01-01 00:00:00",
    end="2025-01-31 23:59:59",
)

print(result["ok"])
print(result["meta"])
print(result["debug"]["sql"])
print(result["df"])
```

## 4. 查询股票分钟

```python
result = runtime.query_minute(
    symbols=["000001.SZ"],
    fields=["minute_close", "minute_volume"],
    start="2024-01-02 09:30:00",
    end="2024-01-02 10:30:00",
)

print(result["ok"])
print(result["meta"])
print(result["debug"]["sql"])
print(result["df"])
```

## 5. 查询前先看字段支持清单

### 按资产类型

```python
result = runtime.get_supported_fields(asset_type="stock", freq="1d")
print(result["fields"][:10])
```

### 按节点过滤

```python
result = runtime.get_supported_fields(
    asset_type="stock",
    freq="1d",
    node="stock_daily_real",
)
```

### 只看财务字段

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

## 6. 查询前先校验请求

```python
request = {
    "symbols": ["159102.SZ"],
    "universe": None,
    "fields": ["close_adj", "open", "is_st"],
    "start": "2024-01-01",
    "end": "2024-01-31",
    "freq": "1d",
    "asset_type": "auto",
}

result = runtime.validate_query_request(request)
print(result)
```

这个例子里，ETF + `is_st` 会被提前拦住。

## 7. 从 requirement 构造 intent

```python
data_requirement = {
    "fields": ["close_adj", "open"],
    "scope": {
        "symbols": ["159102.SZ"],
        "freq": "1d",
        "start": "2024-01-01",
        "end": "2024-01-31",
    },
}

result = runtime.build_intent_from_requirement(data_requirement)
print(result["resolved"])
print(result["intent"])
```

## 8. 直接执行 requirement

```python
data_requirement = {
    "fields": ["close_adj", "open"],
    "scope": {
        "symbols": ["000001.SZ"],
        "freq": "1d",
        "start": "2024-01-01",
        "end": "2024-01-31",
    },
}

result = runtime.execute_requirement(data_requirement)
print(result["ok"])
print(result["resolved"])
print(result["debug"]["sql"])
print(result["df"])
```

## 9. 原生 SQL 调试

如果需要直接打底层 SQL：

```python
result = runtime.graph_runtime.execute_sql("SELECT 1 AS x")
print(result)
```

更推荐只在排错或调试时这样用。

## 10. 推荐调用顺序

上层项目最推荐的顺序：

1. `resolve_symbols()`
2. `get_supported_fields()`
3. `validate_query_request()`
4. `build_intent_from_requirement()`
5. `execute_requirement()`

如果场景比较简单，也可以直接：

1. `query_daily()`
2. `query_minute()`

## 11. 当前边界

当前 `ApplicationRuntime` 已支持：

1. 股票日频
2. 股票分钟
3. ETF / 可转债等资产类型识别
4. 基于字段和资产类型的入口推荐
5. requirement -> intent -> execution

当前仍然不建议：

1. 让上层自己拼底层图谱节点
2. 让上层直接拼底层 SQL 作为主路径
3. 自动跨多个业务主体做复杂合并查询
