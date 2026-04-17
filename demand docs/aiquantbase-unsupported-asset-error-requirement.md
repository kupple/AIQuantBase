# AIQuantBase Unsupported Asset Error Requirement

> 目标：让 `AIQuantBase.query_daily(...)` 在遇到 ETF / 指数 / 基金等当前不支持的数据类型时，前置返回明确错误，而不是继续落到错误 node 查询后返回空结果。

## 1. 问题背景

当前在 `AlphaBlocks` 里测试事件驱动策略时：

1. `002545.SZ` 这种正常股票可以成功查询并跑通
2. `159102.SZ` 这种 ETF，当前数据库没有对应 ETF/指数数据

现状问题是：

1. 系统已经能识别出 `159102.SZ` 不是普通股票
2. 但 `query_daily(...)` 仍然继续走了 `stock_daily_real`
3. 最后返回空结果

这样会让上层误以为是：

1. 时间范围没数据
2. symbol 写错
3. 数据库偶发空结果

但真正原因其实是：

**当前数据层不支持该资产类型的日频查询。**

---

## 2. 希望修改后的行为

正确流程应该是：

1. 先识别 symbol 的资产类型
2. 再判断当前是否存在对应的查询 node
3. 如果不存在，直接返回结构化错误
4. 不继续执行 SQL

也就是说：

**资产类型不支持 > 字段不支持 > 空结果**

不要把“资产类型不支持”伪装成“空结果”。

---

## 3. 必须修改的函数

### 3.1 `query_daily(...)`

当传入：

```python
query_daily(
    symbols=["159102.SZ"],
    fields=["close_adj", "open"],
    start="2024-01-01",
    end="2024-01-31",
)
```

如果当前系统没有可用的 ETF 日线 node，
或者当前配置里 ETF 不支持 `1d` 查询，
应直接返回：

```python
{
    "ok": False,
    "df": pd.DataFrame(),
    "issues": [
        {
            "path": "symbols.0",
            "code": "unsupported_asset_type",
            "message": "symbol 159102.SZ is resolved as asset_type=etf, but current AIQuantBase daily query does not support etf"
        }
    ],
    "meta": {
        "asset_type": "etf",
        "node": None,
        "fields": ["close_adj", "open"],
        "row_count": 0,
        "symbol_count": 1,
        "empty": True
    },
    "debug": {
        "intent": None,
        "sql": ""
    }
}
```

不允许：

1. fallback 到 `stock_daily_real`
2. 执行错误 node 的查询
3. 把“资产类型不支持”包装成“查询成功但空结果”

---

### 3.2 `resolve_best_node(...)`

当前如果 symbol 已经被识别成 ETF，但没有可用 node，
应返回：

```python
{
    "ok": False,
    "node": None,
    "asset_type": "etf",
    "freq": "1d",
    "candidates": [],
    "issues": [
        {
            "code": "missing_query_node",
            "message": "asset_type=etf has no supported 1d query node"
        }
    ]
}
```

不允许默认给：

```python
node = "stock_daily_real"
```

---

### 3.3 `validate_query_request(...)`

如果：

1. `symbols` 已识别为 ETF
2. 但当前版本没有支持 ETF 的日频 node

应直接在 validation 阶段返回失败：

```python
{
    "ok": False,
    "issues": [
        {
            "path": "symbols.0",
            "code": "unsupported_asset_type",
            "message": "symbol 159102.SZ is resolved as etf, but no supported daily node exists"
        }
    ],
    "resolved": {
        "asset_type": "etf",
        "node": None
    }
}
```

这样上层在 query 前就能知道是“资产类型不支持”。

---

## 4. 建议增加的错误码

建议统一使用结构化 `issues`，至少支持：

1. `unknown_symbol_type`
   无法识别 symbol 属于什么资产类型

2. `unsupported_asset_type`
   已识别资产类型，但当前版本不支持该类型查询

3. `missing_query_node`
   已识别资产类型，但当前配置中没有对应 node

4. `unsupported_field`
   资产类型或节点不支持该字段

---

## 5. 关键原则

请在实现里保证：

### 原则 1

先判：

1. symbol 类型
2. 是否存在可用 node
3. 字段是否合法

最后才执行查询。

### 原则 2

“资产类型不支持”和“空结果”必须分开。

### 原则 3

`query_daily` / `resolve_best_node` / `validate_query_request`
这三个入口的错误语义要一致。

---

## 6. 当前最关键的复现 case

### case 1: 股票正常

```python
query_daily(
    symbols=["002545.SZ"],
    fields=["close_adj", "open"],
    start="2024-01-01",
    end="2024-01-31",
)
```

预期：

1. `ok = True`
2. 正常返回数据

### case 2: ETF 当前未接入

```python
query_daily(
    symbols=["159102.SZ"],
    fields=["close_adj", "open"],
    start="2024-01-01",
    end="2024-01-31",
)
```

预期：

1. `ok = False`
2. 返回 `unsupported_asset_type` 或 `missing_query_node`
3. 不继续执行错误 node 的查询
4. 不把它伪装成“空结果即成功”

---

## 7. 验收标准

修改完成后，至少满足：

1. `002545.SZ` 正常股票查询不受影响
2. `159102.SZ` 不再落到 `stock_daily_real`
3. ETF 不支持时能前置报错
4. `query_daily` / `validate_query_request` / `resolve_best_node` 三个入口的错误语义一致
5. `issues.code` 可被上层稳定消费

---

## 8. 一句话总结

当 symbol 已经被识别成某种资产类型，但当前数据源不支持时：

**必须前置报错，不允许掉到空结果。**
