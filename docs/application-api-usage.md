# Application API Usage

## 目标

这份文档说明面向上层应用的第一批正式接口如何使用。

当前推荐优先使用：

1. `resolve_symbols(...)`
2. `resolve_best_node(...)`
3. `get_supported_fields(...)`
4. `validate_query_request(...)`
5. `query_daily(...)`
6. `query_minute(...)`

这些接口的目标是让上层应用不需要自己猜：

1. symbol 对应什么资产类型
2. 某类资产支持哪些字段
3. 某份查询请求是否合理
4. 最终该怎么发起日频查询

## 1. resolve_symbols

```python
result = runtime.resolve_symbols(["000001.SZ", "159102.SZ", "000300.SH", "123085.SZ"])
```

返回示例：

```python
{
    "ok": True,
    "items": [
        {"symbol": "000001.SZ", "asset_type": "stock", "market": "SZ", "node": "stock_daily_real"},
        {"symbol": "159102.SZ", "asset_type": "etf", "market": "SZ", "node": "etf_daily_real", "graph_node": "stock_daily_real"},
        {"symbol": "000300.SH", "asset_type": "index", "market": "SH", "node": "index_daily_real", "graph_node": "stock_daily_real"},
        {"symbol": "123085.SZ", "asset_type": "kzz", "market": "SZ", "node": "kzz_conv_real"},
    ],
    "issues": [],
}
```



## 2. resolve_best_node

```python
result = runtime.resolve_best_node(
    symbols=["159102.SZ"],
    fields=["close_adj", "open"],
    freq="1d",
    asset_type="auto",
)
```

返回示例：

```python
{
    "ok": True,
    "node": "etf_daily_real",
    "asset_type": "etf",
    "freq": "1d",
    "candidates": [
        {
            "node": "etf_daily_real",
            "supported_fields": ["close_adj", "open"],
            "unsupported_fields": [],
            "score": 2,
        }
    ],
    "issues": [],
}
```



### 候选节点说明

`resolve_best_node(...)` 会返回 `candidates`，每个候选节点会说明：

1. `supported_fields`
2. `unsupported_fields`
3. `score`

当前规则是：

1. 先根据 `asset_type + freq` 缩小候选范围
2. 再看每个候选节点能否覆盖请求字段
3. 选择能够完全覆盖字段且得分最高的节点

### 说明

例如 ETF 日频如果请求的是：

1. `close_adj`
2. `open`
3. `is_st`

当前节点会先稳定落到 `etf_daily_real`；
但最终字段是否合法，仍然由 `validate_query_request(...)` 决定。

## 3. get_supported_fields

```python
result = runtime.get_supported_fields(asset_type="stock", freq="1d")
```

返回示例：

```python
{
    "ok": True,
    "asset_type": "stock",
    "freq": "1d",
    "fields": [
        {
            "name": "close_adj",
            "description_zh": "前复权收盘价",
            "asset_types": ["stock"],
            "derived": True,
            "node": "stock_daily_real",
        }
    ]
}
```



### get_supported_fields 可选过滤参数

除了 `asset_type` 和 `freq`，还支持：

1. `node`
2. `field_role`
3. `derived_only`

例如：

```python
runtime.get_supported_fields(asset_type="stock", freq="1d", node="stock_daily_real")
runtime.get_supported_fields(asset_type="stock", freq="1d", field_role="financial_field")
runtime.get_supported_fields(asset_type="stock", freq="1d", derived_only=True)
```

## 4. validate_query_request

```python
result = runtime.validate_query_request({
    "symbols": ["159102.SZ"],
    "universe": None,
    "fields": ["close_adj", "open", "is_st"],
    "start": "2024-01-01",
    "end": "2024-01-31",
    "freq": "1d",
    "asset_type": "auto",
})
```

如果字段和资产类型不匹配，会返回：

```python
{
    "ok": False,
    "issues": [
        {
            "path": "fields.is_st",
            "code": "unsupported_field",
            "message": "is_st is not supported for asset_type=etf, freq=1d"
        }
    ],
    "resolved": {
        "asset_type": "etf",
        "node": "etf_daily_real"
    }
}
```

补充：

1. `universe="all_a"` 现在已支持原生日频校验路径
2. 当前仅支持 `freq="1d"`

例如：

```python
result = runtime.validate_query_request({
    "symbols": [],
    "universe": "all_a",
    "fields": ["close_adj", "open", "is_st", "listed_days"],
    "start": "2024-01-01",
    "end": "2024-06-30",
    "freq": "1d",
    "asset_type": "auto",
})
```

## 5. query_daily

```python
result = runtime.query_daily(
    symbols=["000001.SZ"],
    fields=["close_adj", "is_st", "listed_days"],
    start="2024-01-01 00:00:00",
    end="2024-01-31 23:59:59",
)
```

返回示例：

```python
{
    "ok": True,
    "df": DataFrame(...),
    "issues": [],
    "meta": {
        "asset_type": "stock",
        "node": "stock_daily_real",
        "fields": ["close_adj", "is_st", "listed_days"],
        "row_count": 22,
        "symbol_count": 1,
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

如果查询合法但没有任何数据，现在会返回结构化空结果：

```python
{
    "ok": False,
    "issues": [{"code": "empty_result", "message": "..."}],
    "meta": {
        "empty": True,
        "empty_reason": "no_rows",
    },
    "debug": {
        "intent": {...},
        "sql": "...",
    },
}
```



## 6. query_minute

```python
result = runtime.query_minute(
    symbols=["000001.SZ"],
    fields=["minute_close", "minute_volume"],
    start="2024-01-02 09:30:00",
    end="2024-01-02 10:30:00",
)
```

返回结构和 `query_daily(...)` 保持一致：

1. `ok`
2. `df`
3. `issues`
4. `meta`
5. `debug.intent`
6. `debug.sql`

## 7. 当前范围说明

当前第一批正式接口仍然是最小可用版本：

1. 以稳定为主
2. 以日频查询为主
3. 先支持最常见 symbol 识别
4. 先支持常见资产类型到节点的稳定映射

当前还不是：

1. 全自动任意字段驱动查询
2. 全自动跨多业务主体合并查询
3. 全自动最佳入口选择系统

## 8. 推荐顺序

上层应用建议这样调用：

1. 先 `resolve_symbols()`
2. 再 `get_supported_fields()`
3. 再 `validate_query_request()`
4. 最后 `query_daily()`


## 9. build_intent_from_requirement

```python
result = runtime.build_intent_from_requirement({
    "fields": ["close_adj", "open", "listed_days"],
    "scope": {
        "freq": "1d",
        "start": "2024-01-01",
        "end": "2024-01-31",
        "universe": "all_a",
    }
})
```

返回：

1. `ok`
2. `issues`
3. `resolved`
4. `intent`

## 10. execute_requirement

```python
result = runtime.execute_requirement({
    "fields": ["close_adj", "open", "listed_days"],
    "scope": {
        "freq": "1d",
        "start": "2024-01-01",
        "end": "2024-01-31",
        "universe": "all_a",
    }
})
```

返回：

1. `ok`
2. `df`
3. `issues`
4. `resolved`
5. `debug.intent`
6. `debug.sql`
7. `meta`


## 11. 分钟与大范围查询保护

当前接口层已经增加保护规则：

1. 分钟查询必须带 `symbols`
2. 分钟查询时间窗当前限制为 8 小时以内
3. 日频无 symbols 查询不允许长时间范围扫描
