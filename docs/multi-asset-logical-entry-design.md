# Multi-Asset Logical Entry Design

## 目标

这份文档说明：

1. 为什么当前日线/分钟入口应该逻辑拆分
2. 如何在不复制物理表的前提下实现 `stock / etf / index` 入口分离
3. 这样改动对图谱、字段协议和应用层接口意味着什么

## 当前状态

当前已经先按第一阶段落地到应用层：

1. 对外逻辑入口已经拆成 `stock / etf / index`
2. 日频与分钟频都会返回对应逻辑 node
3. 底层仍复用同一批行情物理表
4. 日频会自动补 `security_type` 过滤条件
5. 分钟当前因强制带 `symbols`，先不额外补 `security_type` 过滤
6. 这些逻辑入口和过滤条件现在已经下沉到 [config/graph.yaml](/Users/zhao/Desktop/git/AIQuantBase/config/graph.yaml) 配置，而不是写死在 `runtime.py`

## 1. 当前问题

当前 `ad_market_kline_daily` / `ad_market_kline_minute` 物理表里混存：

1. 股票
2. ETF
3. 指数
4. 其他证券类型

如果逻辑上只保留一个：

1. `stock_daily_real`
2. `stock_minute_real`

会带来几个问题：

1. 资产主体语义不清
2. 调用方误以为 ETF / 指数也支持股票专属字段
3. 文档和错误提示会越来越绕
4. `get_supported_fields()` 需要持续解释“同入口不同资产支持不同字段”

## 2. 设计原则

推荐采用：

### 逻辑分开

1. `stock_daily_real`
2. `etf_daily_real`
3. `index_daily_real`
4. `stock_minute_real`
5. `etf_minute_real`
6. `index_minute_real`

### 物理复用

这些逻辑节点可以继续复用同一张物理表：

1. 日频都可复用 `ad_market_kline_daily`
2. 分钟都可复用 `ad_market_kline_minute`

差异不通过“新建物理表”实现，而通过：

1. `security_type` 过滤
2. 字段支持矩阵
3. 应用层入口区分

## 3. 推荐映射关系

### 日频

1. `stock_daily_real`
   - table: `starlight.ad_market_kline_daily`
   - scope: `security_type = EXTRA_STOCK_A`

2. `etf_daily_real`
   - table: `starlight.ad_market_kline_daily`
   - scope: `security_type = EXTRA_ETF`

3. `index_daily_real`
   - table: `starlight.ad_market_kline_daily`
   - scope: `security_type = EXTRA_INDEX_A_SH_SZ`

### 分钟

1. `stock_minute_real`
   - table: `starlight.ad_market_kline_minute`
   - scope: `security_type = EXTRA_STOCK_A`

2. `etf_minute_real`
   - table: `starlight.ad_market_kline_minute`
   - scope: `security_type = EXTRA_ETF`

3. `index_minute_real`
   - table: `starlight.ad_market_kline_minute`
   - scope: `security_type = EXTRA_INDEX_A_SH_SZ`

## 4. 图谱层怎么表达

推荐在 node 上新增一个最小约束字段，例如：

1. `entity_scope`
或
2. `base_filters`

例如：

```yaml
- name: stock_daily_real
  table: starlight.ad_market_kline_daily
  entity_scope: stock
```

或者：

```yaml
- name: stock_daily_real
  table: starlight.ad_market_kline_daily
  base_filters:
    - field: security_type
      op: '='
      value: EXTRA_STOCK_A
```

## 5. 应用层怎么受益

拆分后，应用层会更清楚：

### `resolve_symbols(...)`

可以直接返回：

1. `stock -> stock_daily_real`
2. `etf -> etf_daily_real`
3. `index -> index_daily_real`

### `get_supported_fields(...)`

字段支持矩阵按入口主体会更自然：

1. 股票字段给 `stock_daily_real`
2. ETF 字段给 `etf_daily_real`
3. 指数字段给 `index_daily_real`

### `validate_query_request(...)`

不需要再依赖“资产识别正确，但最后又回落到 stock 入口”的弯路。

## 6. 为什么这比“只保留一个入口”更好

### 优点

1. 主体语义清楚
2. 字段支持矩阵更好维护
3. 错误提示更自然
4. AI 更不容易混淆
5. 不引入物理数据冗余

### 成本

1. 逻辑节点数会上升
2. 字段支持需要按资产主体重新梳理
3. `resolve_symbols / resolve_best_node / validate_query_request` 需要同步调整

## 7. 推荐落地顺序

### 第一阶段

先做日频：

1. `stock_daily_real`
2. `etf_daily_real`
3. `index_daily_real`

### 第二阶段

再做分钟：

1. `stock_minute_real`
2. `etf_minute_real`
3. `index_minute_real`

### 第三阶段

再回头梳理：

1. `get_supported_fields()`
2. `resolve_best_node()`
3. `validate_query_request()`
4. `query_daily()` / `query_minute()`

## 8. 一句话结论

**建议逻辑上分开 `stock / etf / index` 入口，但继续复用同一张物理日线/分钟表。**
