# Query Intent Templates

## 目标

这份文档提供一组可以直接复制使用的 `Query Intent` 模板。

适用于：

1. Python SDK 调用
2. 外部模块拼装查询
3. 人工调试与排查 SQL

## 1. 最小模板

```python
intent = {
    "from": "stock_daily_real",
    "select": ["code", "trade_time", "close"],
    "time_range": {
        "field": "trade_time",
        "start": "2025-01-01 00:00:00",
        "end": "2025-01-31 23:59:59"
    },
    "page": 1,
    "page_size": 20,
    "safety": {
        "lookahead_safe": False,
        "strict_mode": True
    }
}
```

## 2. 单股票日线查询

```python
intent = {
    "from": "stock_daily_real",
    "select": ["code", "trade_time", "close", "market_cap", "industry_name"],
    "where": {
        "mode": "and",
        "items": [
            {"field": "code", "op": "=", "value": "000001.SZ"}
        ]
    },
    "time_range": {
        "field": "trade_time",
        "start": "2025-01-01 00:00:00",
        "end": "2025-01-31 23:59:59"
    },
    "order_by": [
        {"field": "trade_time", "direction": "asc"}
    ],
    "page": 1,
    "page_size": 50,
    "safety": {
        "lookahead_safe": False,
        "strict_mode": True
    }
}
```

## 3. 多股票查询（code in）

```python
intent = {
    "from": "stock_daily_real",
    "select": ["code", "trade_time", "close"],
    "where": {
        "mode": "and",
        "items": [
            {
                "field": "code",
                "op": "in",
                "value": ["000001.SZ", "000002.SZ", "000004.SZ"]
            }
        ]
    },
    "time_range": {
        "field": "trade_time",
        "start": "2024-01-01 00:00:00",
        "end": "2024-01-31 23:59:59"
    },
    "order_by": [
        {"field": "trade_time", "direction": "asc"},
        {"field": "code", "direction": "asc"}
    ],
    "page": 1,
    "page_size": 100,
    "safety": {
        "lookahead_safe": False,
        "strict_mode": True
    }
}
```

## 4. 市值区间筛选

```python
intent = {
    "from": "stock_daily_real",
    "select": ["code", "trade_time", "market_cap"],
    "where": {
        "mode": "and",
        "items": [
            {"field": "market_cap", "op": "between", "value": [3000000, 6000000]},
            {"field": "is_st", "op": "=", "value": 0}
        ]
    },
    "time_range": {
        "field": "trade_time",
        "start": "2025-01-01 00:00:00",
        "end": "2025-12-31 23:59:59"
    },
    "order_by": [
        {"field": "trade_time", "direction": "asc"},
        {"field": "code", "direction": "asc"}
    ],
    "page": 1,
    "page_size": 100,
    "safety": {
        "lookahead_safe": False,
        "strict_mode": True
    }
}
```

## 5. 股票分钟查询

```python
intent = {
    "from": "stock_minute_real",
    "select": ["code", "trade_time", "minute_close", "minute_volume"],
    "where": {
        "mode": "and",
        "items": [
            {"field": "code", "op": "=", "value": "000001.SZ"}
        ]
    },
    "time_range": {
        "field": "trade_time",
        "start": "2025-04-10 09:30:00",
        "end": "2025-04-10 10:30:00"
    },
    "order_by": [
        {"field": "trade_time", "direction": "asc"}
    ],
    "page": 1,
    "page_size": 200,
    "safety": {
        "lookahead_safe": False,
        "strict_mode": True
    }
}
```

## 6. 行业日线查询

```python
intent = {
    "from": "industry_daily_real",
    "select": ["index_code", "trade_date", "industry_daily_close", "industry_pe", "industry_name"],
    "where": {
        "mode": "and",
        "items": [
            {"field": "index_code", "op": "=", "value": "801010.SI"}
        ]
    },
    "time_range": {
        "field": "trade_date",
        "start": "2025-01-01",
        "end": "2025-01-31"
    },
    "order_by": [
        {"field": "trade_date", "direction": "asc"}
    ],
    "page": 1,
    "page_size": 50,
    "safety": {
        "lookahead_safe": False,
        "strict_mode": True
    }
}
```

## 7. 公告/事件类查询

```python
intent = {
    "from": "stock_daily_real",
    "select": ["code", "trade_time", "dividend_ann_date", "dividend_progress"],
    "where": {
        "mode": "and",
        "items": [
            {"field": "dividend_ann_date", "op": "is_not_null", "value": None}
        ]
    },
    "time_range": {
        "field": "trade_time",
        "start": "2025-01-01 00:00:00",
        "end": "2025-12-31 23:59:59"
    },
    "page": 1,
    "page_size": 50,
    "safety": {
        "lookahead_safe": True,
        "strict_mode": True
    }
}
```

## 8. 原生 SQL 调用

如果不走 `Query Intent`，可以直接执行 SQL：

```python
result = runtime.execute_sql("SELECT 1 AS x")
```

返回：

```python
{
    "code": 0,
    "message": "success",
    "sql": "SELECT 1 AS x\nFORMAT JSON",
    "df": DataFrame(...),
}
```

## 9. where 常用操作符

当前常用支持：

1. `=`
2. `in`
3. `not_in`
4. `between`
5. `like`
6. `ilike`
7. `contains`
8. `starts_with`
9. `ends_with`
10. `is_null`
11. `is_not_null`

## 10. page / page_size

推荐统一用：

```python
"page": 1,
"page_size": 20
```

系统会自动映射到：

1. `limit`
2. `offset`

## 11. lookahead_safe

### `lookahead_safe = False`
不额外做防未来约束。

### `lookahead_safe = True`
当前第一阶段只对公告/事件类字段生效。

### 当前明确不影响：

1. `close`
2. `high`
3. `low`
4. `volume`
5. `amount`
6. `market_cap`
7. `turnover_rate`

## 12. 返回结构

### execute_intent

```python
{
    "code": 0,
    "message": "success",
    "sql": "...",
    "df": DataFrame(...),
}
```

### get_real_fields_json

```python
{
    "code": 0,
    "message": "success",
    "items": [...]
}
```
