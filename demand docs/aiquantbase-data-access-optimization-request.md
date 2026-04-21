# AIQuantBase Data Access Optimization Request

> 目标：把 `AlphaBlocks` 当前最关键的 `contract -> data_requirement -> AIQuantBase -> data_view` 这层稳定下来，优先解决性能、空结果语义、以及回归验收问题。

## 1. 背景

当前 `AlphaBlocks` 已经把 `data_access` 节点单独拆出来测试：

1. 固定合同样例库：
   `tests/data_access/contracts/`
2. 固定冒烟入口：
   `tools/run_aiqb_contract_smoke.py`
3. 固定产物目录：
   `tests/data_access/artifacts/runs/`

我们现在要优先稳定的是：

1. 合同编译出的 `data_requirement`
2. `AIQuantBase` 如何把它变成查询请求
3. 查询结果如何稳定回到 `Standardized Data View`

这一步稳定后，后面的 research / backtest / report 都只是上层消费问题。

---

## 2. 当前已确认状态

### 2.1 已经成功的链路

截至 `2026-04-18`，下面这些真实链路已经成功跑通：

1. `all_a` 横截面研究 + 回测
2. `all_a` 反转研究 + 回测
3. 单股票事件研究 + 回测
4. `all_a` 事件驱动研究 + 回测
5. 首板低开事件回测

代表性成功 run：

1. `tests/data_access/artifacts/runs/20260418_100149__mom_minus_vol_all_a_smoke__mom_minus_vol_all_a_weekly_smoke`

这次 run 的主查询已经能稳定拿到：

1. `close_adj`
2. `is_st`
3. `open`

代表 SQL：

```sql
SELECT
  b0.code AS code,
  b0.trade_time AS trade_time,
  (b0.close * t1.factor_value) AS close_adj,
  t2.is_st_sec AS is_st,
  b0.open AS open
FROM starlight.ad_market_kline_daily b0
LEFT JOIN starlight.ad_adj_factor t1
  ON b0.code = t1.code
 AND toDate(b0.trade_time) = t1.trade_date
LEFT JOIN starlight.ad_history_stock_status t2
  ON b0.code = t2.market_code
 AND toDate(b0.trade_time) = t2.trade_date
LEFT JOIN starlight.ad_code_info t3
  ON b0.code = t3.code
WHERE b0.trade_time BETWEEN '2024-01-01' AND '2024-06-30'
  AND t3.security_type = 'EXTRA_STOCK_A'
ORDER BY b0.trade_time ASC, b0.code ASC
```

这说明：

1. `close_adj` 已经可以内聚在主查询里
2. `is_st` 也已经内聚在主查询里
3. 当前真正剩下的主要瓶颈不是这两个字段

### 2.2 当前剩余问题

现在还剩两类问题：

#### 问题 A：`listed_days` 仍然依赖额外 fallback SQL

当前 `AlphaBlocks` 适配层为了支持 `listed_days`，会额外跑一轮：

```sql
SELECT
  market_code AS code,
  argMax(list_date, snapshot_date) AS list_date
FROM starlight.ad_stock_basic
WHERE market_code IN (...)
GROUP BY market_code
```

而且是按批次拼超长 `IN (...)` 查询。

这会带来几个问题：

1. 一次主查询后还要再扫一轮基础表
2. 查询性能不稳定
3. debug 结构会出现主查询成功但 fallback 很重
4. 长周期 / 大范围回测时，这层会成为明显瓶颈

#### 问题 B：ETF 日线链路当前返回空结果，但语义还不够好

当前失败 case：

1. `tests/data_access/contracts/strategy_backtest/theme_rotation_broad_top3_smoke.yaml`
2. 对应 run：
   `tests/data_access/artifacts/runs/20260418_095953__research__strategy`

当前错误：

```text
AIQuantBase query_daily returned empty result for node=etf_daily_real asset_type=etf
```

这里至少要区分清楚三种情况：

1. 资产类型不支持
2. node 存在，但当前库里没有数据
3. 查询本身失败

现在这三者在上层看来还不够稳定。

---

## 3. AlphaBlocks 当前传给 AIQuantBase 的请求长什么样

当前 `AlphaBlocks` 在 symbol 模式下，是通过 `ApplicationRuntime.query_daily(...)` 调 `AIQuantBase`。

### 3.1 当前 query request 形态

```yaml
symbols:
  - 002545.SZ
universe: null
fields:
  - close_adj
  - open
  - is_st
  - listed_days
start: 2024-01-01
end: 2024-06-30
asset_type: auto
freq: 1d
```

ETF 轮动类请求长这样：

```yaml
symbols:
  - 510050.SH
  - 510300.SH
  - 510500.SH
  - 159915.SZ
universe: null
fields:
  - close_adj
  - open
start: 2024-01-01
end: 2024-06-30
asset_type: auto
freq: 1d
```

### 3.2 当前 `data_requirement` 形态

`AlphaBlocks` 真实输出给数据层的核心结构是：

```yaml
fields:
  - close_adj
  - is_st
  - listed_days
  - open
required_fields:
  - close_adj
  - is_st
  - listed_days
  - open
lookback_bars: 6
freq: 1d
scope:
  universe: all_a
  benchmark: 000300.SH
  freq: 1d
  start: 2024-01-01
  end: 2024-06-30
```

这意味着 AIQuantBase 侧如果能把：

1. `query_daily`
2. `build_intent_from_requirement`
3. `execute_requirement`

这三条路径收敛好，后面两边的边界会更稳。

---

## 4. 这次整改的目标边界

这次不要做大的重构，只做对当前链路最有价值的整改：

1. 优先优化日频查询路径
2. 优先优化 `stock` / `etf` 两类资产
3. 优先优化 `close_adj / open / is_st / list_date / listed_days`
4. 优先保证错误语义和 debug 产物稳定

这次不要求：

1. 重写 protocol
2. 接入 LLM
3. 改 AlphaBlocks 的研究层 / 回测层语义

---

## 5. P0 必改项

### 5.1 让 `listed_days` 不再依赖 AlphaBlocks 侧 raw SQL fallback

这是当前最重要的一项。

#### 当前问题

`AlphaBlocks` 适配层里还存在：

1. `_fill_list_date_via_sql(...)`
2. `debug_bundle["sql_debug"]["fallback_queries"]["listed_days"]`

也就是说，`listed_days` 还不是 AIQuantBase 主能力。

#### 目标

以下两种方案任选一种，但最终效果要一样：

##### 方案 A：主查询直接支持 `list_date`

让股票日频主查询路径直接支持：

1. `close_adj`
2. `open`
3. `is_st`
4. `list_date`

这样 `AlphaBlocks` 只需用：

1. `trade_time`
2. `list_date`

就能本地算出 `listed_days`，不再额外扫 `ad_stock_basic`。

##### 方案 B：AIQuantBase 直接支持 `listed_days`

也可以直接把 `listed_days` 做成标准字段或 query-time 派生字段。

但无论怎么实现，要求都是：

1. `query_daily(... fields=["close_adj", "open", "is_st", "listed_days"])`
   不再触发第二轮 raw SQL fallback
2. `execute_requirement(...)`
   也能直接覆盖这个能力

#### 验收标准

改完后，下面这种请求不应再出现：

1. `query_intent.fallback.listed_days`
2. `sql_debug.fallback_queries.listed_days`

也就是说：

**`listed_days` 必须并入 AIQuantBase 的主查询能力，而不是让 AlphaBlocks 自己补。**

---

### 5.2 空结果必须有明确语义，不能只给 `ok=true + empty`

#### 当前问题

现在上层最怕的是：

1. 请求合法
2. node 也存在
3. 但是结果空了

这时候如果只返回空表，上层很难判断到底是：

1. 这个 symbol 本来就没数据
2. 时间范围没数据
3. node 配对错了
4. asset_type 推断错了

#### 目标

当 query 已经通过 validation，但执行后没有任何行时，建议返回：

```python
{
    "ok": False,
    "df": pd.DataFrame(),
    "issues": [
        {
            "code": "empty_result",
            "path": "query",
            "message": "query succeeded but returned no rows for node=etf_daily_real asset_type=etf"
        }
    ],
    "meta": {
        "asset_type": "etf",
        "node": "etf_daily_real",
        "fields": ["close_adj", "open"],
        "row_count": 0,
        "symbol_count": 4,
        "empty": True,
        "empty_reason": "no_rows"
    },
    "debug": {
        "intent": {...},
        "sql": "..."
    }
}
```

关键点：

1. `unsupported_asset_type`
2. `missing_query_node`
3. `empty_result`
4. `query_failed`

这几类语义必须能稳定区分。

---

### 5.3 `query_daily` / `resolve_best_node` / `validate_query_request` 错误语义要完全一致

这一点不是新需求，但这次需要一起收口。

要求：

1. 如果资产类型不支持：
   返回 `unsupported_asset_type`
2. 如果资产类型支持，但没有 node：
   返回 `missing_query_node`
3. 如果字段不支持：
   返回 `unsupported_field`
4. 如果执行成功但无数据：
   返回 `empty_result`

不要出现：

1. `validate_query_request` 说支持
2. `resolve_best_node` 说有 node
3. `query_daily` 最后却只能给一个泛化空表

---

### 5.4 query 结果里必须带足够的 debug 元信息

当前 `AlphaBlocks` 很依赖：

1. `intent`
2. `sql`
3. `meta.asset_type`
4. `meta.node`

建议统一保证下面这些字段始终存在：

```python
meta = {
    "asset_type": "...",
    "node": "...",
    "fields": [...],
    "row_count": 0,
    "symbol_count": 0,
    "empty": True,
    "elapsed_ms": 1234,
}

debug = {
    "intent": {...},
    "sql": "...",
}
```

如果后面 AIQuantBase 要继续演进，建议再补：

1. `debug.request`
2. `debug.validation`
3. `debug.resolved`

这样 AlphaBlocks 的冒烟归档就不需要再推断。

---

## 6. P1 建议项

### 6.1 原生支持 `universe=all_a`

当前 `ApplicationRuntime.validate_query_request(...)` 里仍然直接把 `universe` 判成：

1. `unsupported_universe`

所以 `AlphaBlocks` 在 `all_a` 路径上只能绕回 `GraphRuntime.execute_intent(...)`。

这不是当前最急的问题，但它会导致：

1. symbol 模式和 universe 模式走两套路径
2. debug 结构不统一
3. 上层适配器逻辑更重

建议后续补上：

1. `query_daily(... universe="all_a")`
2. `build_intent_from_requirement(...)`
3. `execute_requirement(...)`

对 `all_a` 的原生支持。

### 6.2 如果 AIQuantBase 后续愿意，补一个 `query_daily_bundle(...)`

这不是本次硬要求，但很适合集成层。

理想输出：

1. `request`
2. `validation`
3. `resolved`
4. `intent`
5. `sql`
6. `df`
7. `elapsed_ms`
8. `issues`

这样 `AlphaBlocks` 可以直接归档，不用二次拼 debug bundle。

---

## 7. AIQuantBase 仓库内的测试建议

建议在 `AIQuantBase` 仓库里至少补下面这些测试。

### 7.1 单元测试

#### case 1：股票日频支持 `listed_days`

```python
result = runtime.query_daily(
    symbols=["000001.SZ"],
    fields=["close_adj", "open", "is_st", "listed_days"],
    start="2024-01-01",
    end="2024-01-31",
)
```

要求：

1. `ok is True`
2. `meta.node == "stock_daily_real"`
3. 返回列中已经能支撑 `listed_days`
4. 不依赖 AlphaBlocks 再补 raw SQL

#### case 2：ETF 合法字段正常解析

```python
result = runtime.validate_query_request(
    {
        "symbols": ["159915.SZ"],
        "fields": ["close_adj", "open"],
        "start": "2024-01-01",
        "end": "2024-06-30",
        "freq": "1d",
        "asset_type": "auto",
    }
)
```

要求：

1. `ok is True`
2. `resolved.asset_type == "etf"`
3. `resolved.node == "etf_daily_real"`

#### case 3：ETF 空结果必须抛结构化错误

如果数据库里没有对应 ETF 数据，要求：

1. `ok is False`
2. `issues` 里包含 `empty_result`
3. `meta.node == "etf_daily_real"`
4. `debug.sql` 必须存在

#### case 4：不支持字段仍然提前拦截

```python
result = runtime.validate_query_request(
    {
        "symbols": ["159915.SZ"],
        "fields": ["close_adj", "is_st"],
        "start": "2024-01-01",
        "end": "2024-06-30",
        "freq": "1d",
        "asset_type": "auto",
    }
)
```

要求：

1. `ok is False`
2. `issues` 里包含 `unsupported_field`

### 7.2 requirement 入口测试

如果这次一起补 `execute_requirement(...)` 能力，建议加：

```python
result = runtime.execute_requirement(
    {
        "fields": ["close_adj", "open", "is_st", "listed_days"],
        "scope": {
            "symbols": ["000001.SZ"],
            "freq": "1d",
            "start": "2024-01-01",
            "end": "2024-01-31",
        },
    }
)
```

要求：

1. `ok is True`
2. `debug.intent` 存在
3. `debug.sql` 存在

---

## 8. 改完后在 AlphaBlocks 这边怎么验收

AIQuantBase 改完后，先回到 `AlphaBlocks` 做这两层验收。

### 8.1 先装本地最新版 AIQuantBase

```bash
python3 -m pip install -e /Users/zhao/Desktop/git/AIQuantBase
```

### 8.2 先跑 AlphaBlocks 的 data_access 单测

```bash
python3 -m pytest \
  tests/data_access/test_aiquantbase_debug_bundle.py \
  tests/data_access/test_contract_catalog.py \
  tests/contract_compile/test_contract_compile_stage.py
```

这里有一个重点：

如果 AIQuantBase 已经把 `listed_days` 做进主查询能力，
那么 `tests/data_access/test_aiquantbase_debug_bundle.py`
里关于：

1. `fallback_queries.listed_days`
2. `query_intent.fallback.listed_days`

的旧断言要同步改掉。

也就是说，AlphaBlocks 这边的测试预期也要跟着升级：

1. 以前是“允许 fallback”
2. 改完后应该变成“禁止 `listed_days` fallback”

### 8.3 再跑 data_access 真实冒烟

```bash
python3 tools/run_aiqb_contract_smoke.py \
  --cases-path tests/data_access/contracts/contract_smoke_cases.yaml \
  --aiqb-root /Users/zhao/Desktop/git/AIQuantBase \
  --output-root tests/data_access/artifacts/runs \
  --output /tmp/alphablocks_data_access_smoke_post_aiqb.yaml \
  --allow-failures
```

### 8.4 冒烟通过标准

至少要满足下面几点：

1. 原来已经成功的股票 / 全市场 case 不能退化
2. `listed_days` 不再出现 fallback raw SQL
3. `query_intent / sql_debug / dataframe_meta` 仍然完整
4. ETF case 如果仍然无数据，失败原因必须是结构化 `empty_result`
5. 不允许再出现“只是空表，但不知道为什么空”的情况

### 8.5 重点看哪些产物

改完后优先检查：

1. `run_meta.yaml`
2. `query_intent.yaml`
3. `sql_debug.yaml`
4. `dataframe_meta.yaml`
5. `summary.yaml`

尤其要看：

1. `sql_debug.main_query`
2. `sql_debug.fallback_queries`
3. `dataframe_meta.query_meta`

---

## 9. 这次整改完成的定义

满足下面条件，就可以认为这轮 `AIQuantBase` 整改达标：

1. `listed_days` 已经从 AlphaBlocks 的 fallback 逻辑回收到 AIQuantBase 主能力
2. 空结果语义已经结构化，能稳定区分 `unsupported_asset_type / missing_query_node / empty_result / query_failed`
3. `AlphaBlocks` 的 `data_access` 冒烟能稳定复跑
4. debug 产物足够完整，后续出现数据问题可以直接定位 SQL 和 resolved node

## 10. 一句话结论

这次最应该优先改的，不是更多字段，也不是更多 AI 能力，而是：

**把 `listed_days` 并入主查询能力，并把空结果语义彻底结构化。**
