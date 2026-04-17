# AIQuantBase API Checklist

> 目标：给 `AIQuantBase` 提供一份可以直接照着整改的函数清单，帮助当前 `AlphaBlocks` 项目稳定完成“合同 -> data_requirement -> 查询 -> data view”这条链路。

## 1. 文档定位

这份文档不是协议设计稿，而是：

**开发任务清单**

重点回答 4 个问题：

1. 当前项目到底需要哪些函数
2. 哪些是必须先做的
3. 每个函数建议返回什么结构
4. 怎么判断这次整改已经够用了

---

## 2. 当前项目最小依赖

如果只想先把当前 `AlphaBlocks` 跑顺，最少先做这 4 个函数：

1. `query_daily`
2. `resolve_symbols`
3. `get_supported_fields`
4. `validate_query_request`

这 4 个做完，当前项目的数据层稳定性会提升很多。

---

## 3. 必须先做的函数

### 3.1 `query_daily(...)`

#### 作用

作为当前项目最主要的数据入口，覆盖：

1. 股票横截面研究
2. ETF / 单标的事件策略
3. 日频研究和日频回测

#### 建议签名

```python
query_daily(
    *,
    symbols: list[str] | None = None,
    universe: str | None = None,
    fields: list[str],
    start: str,
    end: str,
    asset_type: str = "auto",
    freq: str = "1d",
) -> dict
```

#### 建议返回

```python
{
    "ok": True,
    "df": DataFrame,
    "issues": [],
    "meta": {
        "asset_type": "stock",
        "node": "stock_daily_real",
        "fields": ["close_adj", "open", "is_st", "listed_days"],
        "row_count": 12345,
        "symbol_count": 5000,
        "empty": False,
    },
    "debug": {
        "intent": {...},
        "sql": "...",
    },
}
```

#### 当前项目为什么需要它

`AlphaBlocks` 最终只希望做：

```python
result = aiqb.query_daily(...)
data_view = to_standardized_data_view(result["df"])
```

而不希望自己再拼 node、拼 where、猜 symbol 类型。

---

### 3.2 `resolve_symbols(symbols)`

#### 作用

给一组 symbol 判定资产类型。

#### 建议签名

```python
resolve_symbols(symbols: list[str]) -> dict
```

#### 建议返回

```python
{
    "ok": True,
    "items": [
        {
            "symbol": "159102.SZ",
            "asset_type": "etf",
            "market": "SZ",
            "node": "etf_daily_real",
        }
    ],
    "issues": [],
}
```

#### 当前项目为什么需要它

当前最典型的问题就是：

`159102.SZ` 这类 ETF 不应该默认查 `stock_daily_real`。

---

### 3.3 `get_supported_fields(...)`

#### 作用

告诉上层某类资产、某个频率下能查哪些标准字段。

#### 建议签名

```python
get_supported_fields(
    *,
    asset_type: str | None = None,
    freq: str = "1d",
) -> dict
```

#### 建议返回

```python
{
    "ok": True,
    "fields": [
        {
            "name": "close_adj",
            "kind": "numeric",
            "asset_types": ["stock", "etf"],
            "derived": True,
        },
        {
            "name": "is_st",
            "kind": "bool",
            "asset_types": ["stock"],
            "derived": True,
        },
    ]
}
```

#### 当前项目为什么需要它

例如 ETF 未必支持：

1. `is_st`
2. `listed_days`

如果没有字段能力清单，AI 层和 adapter 层会一直踩坑。

---

### 3.4 `validate_query_request(...)`

#### 作用

在真正查数据库之前，先判断这份查询请求是否合理。

#### 建议签名

```python
validate_query_request(request: dict) -> dict
```

#### 输入建议

```python
{
    "symbols": ["159102.SZ"],
    "universe": None,
    "fields": ["close_adj", "open", "is_st"],
    "start": "2024-01-01",
    "end": "2024-01-31",
    "freq": "1d",
    "asset_type": "auto",
}
```

#### 建议返回

```python
{
    "ok": False,
    "issues": [
        {
            "path": "fields.is_st",
            "code": "unsupported_field",
            "message": "is_st is not supported for asset_type=etf",
        }
    ],
    "resolved": {
        "asset_type": "etf",
        "node": "etf_daily_real",
    },
}
```

#### 当前项目为什么需要它

这样很多错误能在查库前就拦住。

---

## 4. 强烈建议做的函数

### 4.1 `resolve_best_node(...)`

#### 作用

根据 symbols、fields、freq，返回最适合的 node。

#### 建议签名

```python
resolve_best_node(
    *,
    symbols: list[str] | None,
    universe: str | None,
    fields: list[str],
    freq: str = "1d",
    asset_type: str = "auto",
) -> dict
```

#### 建议返回

```python
{
    "ok": True,
    "node": "etf_daily_real",
    "asset_type": "etf",
    "unsupported_fields": [],
    "mapped_fields": {
        "close_adj": "close_adj",
        "open": "open",
    }
}
```

---

### 4.2 `build_intent_from_requirement(...)`

#### 作用

把中立的 query request 翻成最终 QueryIntent。

#### 建议签名

```python
build_intent_from_requirement(request: dict) -> dict
```

#### 建议返回

```python
{
    "ok": True,
    "intent": {
        "from": "etf_daily_real",
        "select": ["code", "trade_time", "close_adj", "open"],
        "where": {
            "mode": "and",
            "items": [
                {"field": "code", "op": "=", "value": "159102.SZ"}
            ]
        },
        "time_range": {
            "field": "trade_time",
            "start": "2024-01-01",
            "end": "2024-01-31"
        }
    },
    "resolved": {
        "asset_type": "etf",
        "node": "etf_daily_real"
    },
    "issues": [],
}
```

---

### 4.3 `query_daily_bundle(...)`

#### 作用

作为 `query_daily` 的增强版，专门给调试和归档使用。

#### 建议签名

```python
query_daily_bundle(...) -> dict
```

#### 建议返回

```python
{
    "ok": True,
    "df": DataFrame,
    "meta": {...},
    "debug": {
        "intent": {...},
        "sql": "...",
        "row_count": 22,
        "empty": False,
    },
    "issues": [],
}
```

#### 当前项目为什么需要它

当前 `AlphaBlocks` 会把这些内容归档到：

1. `query_intent.yaml`
2. `sql_debug.yaml`
3. `dataframe_meta.yaml`

---

## 5. 可后续再做的函数

### 5.1 `query_minute(...)`

用途：

未来做分钟级事件驱动和更细的执行研究。

---

### 5.2 `get_trading_calendar(...)`

用途：

补齐交易日历、处理日期轴对齐。

---

### 5.3 `get_supported_universes()`

用途：

告诉上层 `all_a / csi300 / csi500 / etf_main` 这些 universe 名称如何解释。

---

### 5.4 `get_field_dependencies(...)`

用途：

告诉上层某些字段是不是派生字段，以及依赖哪些底层列。

---

## 6. 当前项目最常用字段优先级

### 6.1 第一优先级

1. `close_adj`
2. `open`

这是事件策略和大多数回测的最低必需字段。

### 6.2 第二优先级

1. `is_st`
2. `listed_days`

这是当前股票横截面研究常用字段。

### 6.3 第三优先级

1. `close`
2. `high`
3. `low`
4. `volume`
5. `amount`
6. `turnover_rate`
7. `market_cap`

---

## 7. 推荐统一返回格式

建议所有对外函数统一返回：

```python
{
    "ok": True,
    "issues": [],
    "data": ...,
    "meta": ...,
}
```

失败时：

```python
{
    "ok": False,
    "issues": [
        {
            "path": "...",
            "code": "...",
            "message": "...",
        }
    ],
    "data": None,
    "meta": {},
}
```

这样 AI 层和 workflow 层都能直接消费。

---

## 8. 最关键的元数据建议

### 8.1 节点元数据

建议每个 node 至少明确暴露：

1. `supported_asset_types`
2. `freq`
3. `symbol_field`
4. `time_field`

例如：

```yaml
node: stock_daily_real
supported_asset_types:
  - stock
freq: 1d
symbol_field: code
time_field: trade_time
```

---

### 8.2 字段元数据

建议每个字段至少明确暴露：

1. `asset_types`
2. `available_nodes`
3. `derived`
4. `depends_on`

例如：

```yaml
field: close_adj
asset_types: [stock, etf]
available_nodes: [stock_daily_real, etf_daily_real]
derived: true
```

```yaml
field: is_st
asset_types: [stock]
available_nodes: [stock_daily_real]
derived: true
```

---

## 9. 空结果语义必须和执行失败分开

这一点非常重要。

建议 `query_daily()` / `query_daily_bundle()` / `execute_intent()` 在成功但无数据时明确返回：

```python
{
    "ok": True,
    "df": empty_df,
    "meta": {
        "row_count": 0,
        "empty": True,
    },
    "issues": [],
}
```

不要把“空结果”混成“查询失败”。

---

## 10. AlphaBlocks 这边的理想调用方式

如果 `AIQuantBase` 提供的是上面这套 API，当前项目最理想的调用会是：

```python
result = aiqb.query_daily(
    symbols=scope.get("symbols"),
    universe=scope.get("universe"),
    fields=data_requirement["fields"],
    start=scope["start"],
    end=scope["end"],
    asset_type="auto",
)

if not result["ok"]:
    return {
        "stage": "data_access",
        "issues": result["issues"],
        "meta": result["meta"],
    }

data_view = to_standardized_data_view(result["df"])
```

这样：

1. `protocol_core` 保持纯协议语义
2. `AIQuantBase` 负责资产识别、节点选择和查询构造
3. `AlphaBlocks` workflow 负责调度、归档和结果解释

---

## 11. 建议的整改优先级

### 必须先做

1. `resolve_symbols`
2. `get_supported_fields`
3. `validate_query_request`
4. `query_daily`

### 强烈建议做

5. `resolve_best_node`
6. `build_intent_from_requirement`
7. `query_daily_bundle`

### 以后再做

8. `query_minute`
9. `get_trading_calendar`
10. `get_supported_universes`
11. `get_field_dependencies`

---

## 12. 验收标准

可以按下面这些最小 case 验收：

### 股票横截面

```python
query_daily(
    universe="all_a",
    fields=["close_adj", "open", "is_st", "listed_days"],
    start="2024-01-01",
    end="2024-01-31",
)
```

预期：

1. `ok = True`
2. `row_count > 0`
3. `empty = False`

### ETF 单标的

```python
query_daily(
    symbols=["159102.SZ"],
    fields=["close_adj", "open"],
    start="2024-01-01",
    end="2024-01-31",
)
```

预期：

1. `ok = True`
2. `asset_type = etf`
3. `empty = False`

### ETF 不支持字段

```python
validate_query_request(
    {
        "symbols": ["159102.SZ"],
        "fields": ["close_adj", "open", "is_st"],
        "start": "2024-01-01",
        "end": "2024-01-31",
        "freq": "1d",
    }
)
```

预期：

1. `ok = False`
2. issue code 明确指出字段不支持

---

## 13. 一句话总结

当前 `AlphaBlocks` 最希望 `AIQuantBase` 提供的，不是更多 SQL 能力，而是：

**稳定、简单、标准字段导向的数据 API。**
