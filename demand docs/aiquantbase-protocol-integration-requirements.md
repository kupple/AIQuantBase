# AIQuantBase Protocol Integration Requirements

> 目标：明确 `AlphaBlocks` / `protocol_core` 希望 `AIQuantBase` 提供哪些“协议友好”的接口，减少上层在 symbol 类型、节点选择、字段支持和 QueryIntent 构造上的猜测。

## 1. 先说结论

当前 `AlphaBlocks` 最需要的，不是更多 SQL 能力，而是：

**从 `data_requirement` 稳定映射到正确 `QueryIntent` 的能力。**

也就是说：

1. `protocol_core`
   只负责表达“我要什么数据”
2. `AIQuantBase`
   应该负责回答“这些数据到底该去哪个 node 查、字段支不支持、QueryIntent 怎么构造”

AI 不应该参与这一层判断。

这一层应尽量规则化、元数据化、可测试。

---

## 2. 为什么不是 protocol_core 直接对数据库

`protocol_core` 只知道：

1. 需要哪些字段
2. 时间范围是什么
3. 标的是谁
4. 频率是什么

它不应该知道：

1. `159102.SZ` 是股票还是 ETF
2. 应该查 `stock_daily_real` 还是 `etf_daily_real`
3. 哪些字段在哪个 node 上可用
4. 查不到结果时是“空结果”还是“资产类型错了”

所以中间必须存在一层：

**Data Adapter / Symbol Resolver / Node Resolver**

这层最好由 `AIQuantBase` 提供稳定接口支持。

---

## 3. 我们最希望 AIQuantBase 暴露的接口

### 3.1 `resolve_symbol_type(symbols)`

作用：

给一组代码判定资产类型。

例如：

```python
runtime.resolve_symbol_type(["159102.SZ"])
```

理想返回：

```json
{
  "ok": true,
  "items": [
    {
      "symbol": "159102.SZ",
      "asset_type": "etf",
      "market": "SZ"
    }
  ]
}
```

为什么需要：

这一步不应该靠 AI 猜，也不应该靠 `AlphaBlocks` 硬编码。

---

### 3.2 `resolve_best_node(symbols, fields, freq="1d")`

作用：

根据 symbol 类型和字段需求，返回最适合的 node。

例如：

```python
runtime.resolve_best_node(
    symbols=["159102.SZ"],
    fields=["close_adj", "open"],
    freq="1d",
)
```

理想返回：

```json
{
  "ok": true,
  "node": "etf_daily_real",
  "asset_type": "etf",
  "unsupported_fields": [],
  "mapped_fields": {
    "close_adj": "close_adj",
    "open": "open"
  }
}
```

为什么需要：

当前 `AlphaBlocks` 不应该自己硬编码 `from: stock_daily_real`。

---

### 3.3 `get_fields_for_asset_type(asset_type, freq="1d")`

作用：

告诉上层某种资产类型支持哪些标准字段。

例如：

```python
runtime.get_fields_for_asset_type("etf", freq="1d")
```

理想返回：

```json
{
  "ok": true,
  "asset_type": "etf",
  "freq": "1d",
  "fields": ["open", "close", "close_adj", "volume", "amount"]
}
```

为什么需要：

ETF 不一定支持：

1. `is_st`
2. `listed_days`

这类字段如果没有资产类型级别元数据，AI 层和 adapter 层会一直踩坑。

---

### 3.4 `validate_data_requirement(data_requirement)`

作用：

在真正查数据库前，先判断这份数据需求是否合理。

输入：

```json
{
  "fields": ["close_adj", "open", "is_st"],
  "scope": {
    "symbols": ["159102.SZ"],
    "freq": "1d",
    "start": "2024-01-01",
    "end": "2024-01-31"
  }
}
```

理想返回：

```json
{
  "ok": false,
  "issues": [
    {
      "path": "fields.is_st",
      "code": "unsupported_field_for_asset_type",
      "message": "is_st is not supported for asset_type=etf"
    }
  ],
  "resolved": {
    "asset_type": "etf",
    "node": "etf_daily_real"
  }
}
```

为什么需要：

这能在查库前就拦住大量错误。

---

### 3.5 `build_intent_from_requirement(data_requirement)`

作用：

把协议侧的 `data_requirement` 直接翻成标准 QueryIntent。

理想调用：

```python
runtime.build_intent_from_requirement(data_requirement)
```

理想返回：

```json
{
  "ok": true,
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
    },
    "order_by": [
      {"field": "trade_time", "direction": "asc"},
      {"field": "code", "direction": "asc"}
    ]
  },
  "resolved": {
    "asset_type": "etf",
    "node": "etf_daily_real"
  }
}
```

为什么需要：

这能让当前项目的 adapter 变薄很多。

---

### 3.6 `execute_requirement(data_requirement)`

作用：

一步完成：

`data_requirement -> node resolve -> QueryIntent -> SQL -> DataFrame`

理想返回：

```json
{
  "ok": true,
  "intent": {...},
  "sql": "...",
  "df": "DataFrame",
  "meta": {
    "asset_type": "etf",
    "node": "etf_daily_real",
    "row_count": 22,
    "symbol_count": 1,
    "empty_result": false
  }
}
```

为什么需要：

这是最理想的最终对接接口。

---

## 4. 最推荐的整改优先级

如果不想一次做太多，建议先做这 5 个：

1. `resolve_symbol_type(symbols)`
2. `resolve_best_node(symbols, fields, freq)`
3. `validate_data_requirement(data_requirement)`
4. `build_intent_from_requirement(data_requirement)`
5. `execute_requirement(data_requirement)`

这 5 个做完，`AlphaBlocks` 上层稳定性会提升非常明显。

---

## 5. 推荐统一返回结构

建议这些接口统一返回：

```json
{
  "ok": true,
  "issues": [],
  "resolved": {},
  "data": {}
}
```

失败时：

```json
{
  "ok": false,
  "issues": [
    {
      "path": "scope.symbols.0",
      "code": "unknown_symbol",
      "message": "symbol 159102.SZ cannot be resolved"
    }
  ],
  "resolved": {},
  "data": null
}
```

这样 AI 层和 workflow 层都可以直接消费 `issues`。

---

## 6. 最关键的元数据建议

### 6.1 节点级资产类型支持

建议每个 node 明确暴露：

```yaml
node: stock_daily_real
supported_code_types:
  - stock
```

```yaml
node: etf_daily_real
supported_code_types:
  - etf
```

---

### 6.2 字段级资产兼容性

建议字段目录明确暴露：

```yaml
field: is_st
asset_types: [stock]
available_on_nodes: [stock_daily_real]
```

```yaml
field: close_adj
asset_types: [stock, etf]
available_on_nodes: [stock_daily_real, etf_daily_real]
```

这样上层就能提前过滤非法字段组合。

---

### 6.3 派生字段责任边界

建议明确区分：

1. 原生字段
2. AIQuantBase 内置派生字段
3. 上层 adapter 派生字段

例如：

```yaml
field: listed_days
kind: derived
owner: adapter
depends_on:
  - list_date
  - trade_time
```

这样大家都知道这个字段该由谁负责。

---

## 7. 空结果语义必须和执行失败分开

这点非常重要。

当前像：

```text
AIQuantBase query failed across all chunks: full_range: success
```

说明“执行成功但没数据”被误当成了失败。

建议 `execute_intent()` 或未来 `execute_requirement()` 明确返回：

```json
{
  "code": 0,
  "message": "success",
  "empty_result": true,
  "row_count": 0
}
```

这样 workflow 层才能正确诊断：

1. 是没数据
2. 还是查错节点

---

## 8. 我们为什么不希望 AI 参与这一层

因为这一层本质是确定性工作：

1. symbol 是什么资产类型
2. 该走哪个 node
3. 字段支不支持
4. SQL 怎么构造

这些都应该由规则和元数据决定，而不是让 AI 猜。

AI 更适合负责：

1. 写研究目标
2. 生成合同
3. 修合同
4. 解释结果

而不是决定数据库节点。

---

## 9. 最终理想调用方式

当前项目最终最希望只写：

```python
result = runtime.execute_requirement(data_requirement)

if not result["ok"]:
    return {
        "stage": "data_access",
        "issues": result["issues"],
        "resolved": result["resolved"],
    }

data_view = adapter.to_standardized_data_view(result["df"])
```

这样：

1. `protocol_core` 保持纯协议语义
2. `AIQuantBase` 负责资产识别、节点选择和查询构造
3. `AlphaBlocks` workflow 只负责调度、归档和结果解释

---

## 10. 一句话总结

`AlphaBlocks` 现在真正希望 `AIQuantBase` 提供的，不是更多 SQL 能力，而是：

**协议友好的 symbol / node / field / requirement 解析能力。**
