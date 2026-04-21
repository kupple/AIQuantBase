# ApplicationRuntime API Reference

## 目标

这份文档定义 `ApplicationRuntime` 作为上层应用正式接口层的 API 边界。

核心原则：

1. 上层应用优先使用 `ApplicationRuntime`
2. `GraphRuntime` 继续保留为底层图谱能力层
3. 不把底层图谱实现细节直接暴露给业务调用方

## 1. 推荐使用的正式接口

### 1. `ApplicationRuntime.from_defaults()`

作用：

1. 使用正式默认配置初始化应用层运行时

默认读取：

1. `config/graph.yaml`
2. `config/fields.yaml`
3. `config/runtime.local.yaml`

示例：

```python
from aiquantbase import ApplicationRuntime

runtime = ApplicationRuntime.from_defaults()
```

---

### 2. `resolve_symbols(symbols)`

作用：

1. 把 symbol 解析成资产类型和推荐节点

输入：

```python
symbols: list[str]
```

返回：

```python
{
    "ok": True,
    "items": [...],
    "issues": [...],
}
```

推荐场景：

1. 上层应用先判断一组 symbol 属于什么资产
2. UI 先做 symbol 类型预检查

---

### 3. `resolve_best_node(...)`

作用：

1. 根据 symbol、字段和频率推荐最合适的查询节点

输入：

1. `symbols`
2. `fields`
3. `freq`
4. `asset_type`

返回：

```python
{
    "ok": True,
    "node": "stock_daily_real",
    "asset_type": "stock",
    "freq": "1d",
    "candidates": [...],
    "issues": [],
}
```

推荐场景：

1. 调试字段组合最终会落到哪个节点
2. 给上层显示“为什么选这个入口”

---

### 4. `get_supported_fields(...)`

作用：

1. 获取当前资产/频率下可支持的字段清单

支持过滤：

1. `asset_type`
2. `freq`
3. `node`
4. `field_role`
5. `derived_only`

返回：

```python
{
    "ok": True,
    "fields": [...],
}
```

推荐场景：

1. 前端字段选择器
2. AI 字段推荐
3. 上层请求构建前的能力检查

---

### 5. `validate_query_request(request)`

作用：

1. 在真正查询前校验请求是否合法

当前会检查：

1. symbols 是否存在
2. asset_type 是否可解析
3. 字段是否支持
4. 频率是否支持
5. 查询规模是否超出保护阈值

补充：

1. `universe="all_a"` 当前已支持原生日频校验
2. 当前仅支持 `freq="1d"`

返回：

```python
{
    "ok": False,
    "issues": [...],
    "resolved": {...},
}
```

推荐场景：

1. 查库前预检查
2. 给上层返回结构化错误

---

### 6. `query_daily(...)`

作用：

1. 执行日频查询

返回：

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

推荐场景：

1. 股票/ETF/可转债等日频数据查询
2. 日频研究与回测数据准备
3. `all_a` universe 日频查询

当前执行特性：

1. 运行时主路径优先走 `DataFrame` 直读
2. 不再要求先把大结果集反序列化成 Python `list[dict]` 再进入业务层
3. 对 `all_a` 或大结果查询，这一条尤其重要

当前 `meta` 至少包含：

1. `asset_type`
2. `node`
3. `fields`
4. `row_count`
5. `symbol_count`
6. `empty`
7. `empty_reason`
8. `elapsed_ms`

当前 `debug` 至少包含：

1. `request`
2. `validation`
3. `resolved`
4. `intent`
5. `sql`

---

### 7. `query_minute(...)`

作用：

1. 执行分钟查询

返回结构与 `query_daily(...)` 一致。

当前也优先返回：

1. `df: pandas.DataFrame`

这意味着分钟执行类接口与日线接口在上层调用方式上已经统一。

---

### 8. `query_minute_window_by_trading_day(...)`

作用：

1. 按交易日批量拉取分钟窗口
2. 适合固定时点观察 + 下一分钟执行这类策略

推荐场景：

1. `14:30` 观察，`14:31 open` 执行
2. 单标的或小股票池分钟执行
3. `AlphaBlocks` 当前 `intraday_rules` 对接

---

### 9. `query_next_trading_day_intraday_windows(...)`

作用：

1. 按 anchor 批量拉取“下一交易日”分钟执行窗口
2. 最贴近日线信号 + 分钟执行规则的业务语义

推荐场景：

1. “涨停次日 14:30 未涨停卖出”
2. “次日 09:35 买入”
3. 日线事件信号触发后的盘中执行

对接说明：

当前 `AlphaBlocks` 已经会优先调用这个接口。

推荐场景：

1. 股票分钟走势
2. 分钟级量价数据获取

---

### 8. `query_minute_window_by_trading_day(...)`

作用：

1. 按 `symbols + trading_days + HH:MM 窗口` 拉取分钟执行窗口
2. 面向“日线信号 + 分钟执行”场景直接返回分钟 bars

输入：

1. `symbols`
2. `trading_days`
3. `start_hhmm`
4. `end_hhmm`
5. `fields`
6. `asset_type`

当前第一版支持的分钟执行补充字段：

1. `is_limit_up`
2. `is_limit_down`
3. `limit_up_price`
4. `limit_down_price`

说明：

1. 这些字段当前在运行时里做派生，不要求上层自己补日线限价表
2. 部分 `trading_days` 没数据时，只要整体仍有数据，就返回 `ok=True`
3. 全部窗口为空时，返回 `ok=False + empty_result`

推荐场景：

1. 上层已经知道执行日
2. 想直接批量拉某几个交易日的分钟执行窗口

---

### 9. `query_next_trading_day_intraday_windows(...)`

作用：

1. 面向锚点列表直接查询“下一交易日分钟执行窗口”
2. 优先兼容当前 `AlphaBlocks` adapter

输入示意：

```python
anchors = [
    {
        "anchor_id": "002545.SZ__2024-03-01__2024-03-04",
        "code": "002545.SZ",
        "signal_date": "2024-03-01",
        "execution_date": "2024-03-04",
    }
]
```

说明：

1. 如果 `execution_date` 已给出，则直接使用
2. 如果缺少 `execution_date`，运行时会尝试用 `discovery.trading_calendar_table` 推导下一交易日
3. 返回结构与 `query_minute_window_by_trading_day(...)` 一致，但 `meta` 额外带：
   `anchor_count / missing_anchor_count / missing_anchor_ids`

推荐场景：

1. `AlphaBlocks` 的 `intraday_requirement`
2. 日线信号转分钟执行窗口

---

### 10. `build_intent_from_requirement(data_requirement)`

作用：

1. 把 requirement 直接翻译成 Query Intent

返回：

```python
{
    "ok": True,
    "issues": [],
    "resolved": {...},
    "intent": {...},
}
```

推荐场景：

1. 调试 requirement -> intent 的转换结果
2. 协议层与执行层之间做中间审计

---

### 11. `execute_requirement(data_requirement)`

作用：

1. 从 requirement 直接执行到 DataFrame

返回：

```python
{
    "ok": True,
    "df": DataFrame(...),
    "issues": [],
    "resolved": {...},
    "debug": {
        "intent": {...},
        "sql": "...",
    },
}
```

推荐场景：

1. 上层应用主执行入口
2. 协议驱动的数据层调用
3. `all_a` requirement 执行

## 2. 暂不推荐上层直接使用的底层接口

这些接口仍然存在，但不建议作为业务主入口：

1. `GraphRuntime.render_intent(...)`
2. `GraphRuntime.execute_intent(...)`
3. `GraphRuntime.execute_sql(...)`
4. `GraphRuntime.get_metadata_catalog()`
5. `GraphRuntime.get_real_fields_json()`
6. `GraphRuntime.get_protocol_summary()`

原因：

1. 它们更偏底层图谱能力
2. 适合调试、管理和排查
3. 不适合作为协议层主入口长期暴露给业务代码

补充说明：

1. `get_protocol_summary()` 比 `get_metadata_catalog()` 更轻
2. 如果上层只是想快速知道当前有哪些入口节点，可以优先用它
3. 真正执行查询仍然建议回到 `ApplicationRuntime`

## 3. 当前可信使用方式

推荐顺序：

1. `resolve_symbols()`
2. `get_supported_fields()`
3. `validate_query_request()`
4. `build_intent_from_requirement()`
5. `execute_requirement()`

如果业务场景简单，也可以直接：

1. `query_daily()`
2. `query_minute()`
3. `query_minute_window_by_trading_day()`
4. `query_next_trading_day_intraday_windows()`

## 4. 当前不承诺的能力

以下能力当前仍不作为正式接口承诺：

1. 全自动字段驱动选入口
2. 全自动跨多个业务主体拼接查询
3. 完全自动拆查询
4. 无边界全图谱自由查询

## 5. 一句话边界

**`ApplicationRuntime` 是上层应用正式接口层；`GraphRuntime` 是底层图谱能力层。**
