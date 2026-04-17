# ApplicationRuntime Quick Start

## 目标

这份文档只做一件事：

**让上层应用最快接通 `ApplicationRuntime`。**

如果你只想知道：

1. 怎么初始化
2. 怎么查日频
3. 怎么查分钟
4. 怎么执行 requirement

看这一份就够了。

## 1. 安装

在项目根目录执行：

```bash
/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m pip install -e .
```

如果是在别的本地项目里引用：

```bash
/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m pip install -e /Users/zhao/Desktop/git/AIQuantBase
```

## 2. 初始化

```python
from aiquantbase import ApplicationRuntime

runtime = ApplicationRuntime.from_defaults()
```

默认读取：

1. `config/graph.yaml`
2. `config/fields.yaml`
3. `config/runtime.local.yaml`

## 3. 查股票日频

```python
result = runtime.query_daily(
    symbols=["000001.SZ"],
    fields=["close_adj", "market_cap", "industry_name"],
    start="2025-01-01 00:00:00",
    end="2025-01-31 23:59:59",
)

if result["ok"]:
    df = result["df"]
    sql = result["debug"]["sql"]
    print(sql)
    print(df.head())
else:
    print(result["issues"])
```

## 4. 查股票分钟

```python
result = runtime.query_minute(
    symbols=["000001.SZ"],
    fields=["minute_close", "minute_volume"],
    start="2024-01-02 09:30:00",
    end="2024-01-02 10:30:00",
)

if result["ok"]:
    print(result["debug"]["sql"])
    print(result["df"].head())
else:
    print(result["issues"])
```

## 5. 先看 symbol 类型

```python
result = runtime.resolve_symbols(["000001.SZ", "159102.SZ", "000300.SH", "123085.SZ"])
print(result)
```

你会看到：

1. `000001.SZ -> stock_daily_real`
2. `159102.SZ -> etf_daily_real`
3. `000300.SH -> index_daily_real`

这些是逻辑入口名。
底层 SQL 仍然会复用同一批行情物理表，并自动补上资产类型过滤条件。

## 6. 先看字段支持清单

```python
result = runtime.get_supported_fields(asset_type="stock", freq="1d")
print(result["fields"][:20])
```

## 7. 先校验请求

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

validation = runtime.validate_query_request(request)
print(validation)
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

if result["ok"]:
    print(result["debug"]["sql"])
    print(result["df"].head())
else:
    print(result["issues"])
```

## 9. 返回结构

### 查询成功

```python
{
    "ok": True,
    "df": DataFrame(...),
    "issues": [],
    "meta": {...},
    "debug": {
        "intent": {...},
        "sql": "...",
    },
}
```

### 查询失败

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

## 10. 当前最推荐的使用顺序

### 简单场景

1. `query_daily(...)`
2. `query_minute(...)`

### 稍稳场景

1. `resolve_symbols(...)`
2. `get_supported_fields(...)`
3. `validate_query_request(...)`
4. `execute_requirement(...)`

## 11. 一句话上手

```python
from aiquantbase import ApplicationRuntime
runtime = ApplicationRuntime.from_defaults()
result = runtime.query_daily(...)
```
