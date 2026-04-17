# Direct-Field Runtime Protocol

## 目标

这份文档描述当前 AIQuantBase 已经稳定下来的运行时协议形态。

当前版本的核心变化是：

1. `disabled relation real nodes` 已经全部从正式图谱中移除
2. 关系不再依赖中间节点跳转
3. 查询主要由字段级关系配置直接驱动

可以把现在的运行时理解成：

1. `enabled real entry nodes`
2. `field protocol`
3. `Query Intent`
4. `SQL renderer`

## 1. 当前图谱形态

当前正式图谱只保留启用的 `*_real` 主入口节点。

特点：

1. 没有 `disabled real nodes`
2. 没有必须先经过 relation-support node 才能拿到字段的查询依赖
3. 字段关系直接写在 `config/fields.yaml`

也就是说，现在的主逻辑是：

1. 先选入口节点
2. 再选标准字段
3. 字段自己携带 join 规则
4. 规划器直接生成 SQL

## 2. 字段协议

当前字段协议主要依赖下面这些配置项：

1. `standard_field`
2. `base_node`
3. `binding_mode`
4. `source_table`
5. `source_field`
6. `join_keys`
7. `time_binding`
8. `bridge_steps`
9. `resolver_type`
10. `depends_on`

### 2.1 直接字段

直接字段一般长这样：

```yaml
standard_field: pre_close
base_node: stock_daily_real
binding_mode: source_table
source_table: starlight.ad_history_stock_status
source_field: preclose
relation_type: direct
join_keys:
  - base: code
    source: market_code
time_binding:
  mode: same_date
  base_time_field: trade_time
  base_time_cast: date
  source_time_field: trade_date
```

### 2.2 桥接字段

桥接字段通过 `bridge_steps` 表达：

```yaml
standard_field: industry_code
base_node: stock_daily_real
binding_mode: source_table
source_table: starlight.ad_industry_base_info
source_field: industry_code
relation_type: bridge
bridge_steps:
  - table: starlight.ad_industry_weight
    join_keys:
      - base: code
        source: con_code
    time_binding:
      mode: same_date
      base_time_field: trade_time
      base_time_cast: date
      source_time_field: trade_date
join_keys:
  - base: index_code
    source: index_code
```

### 2.3 派生字段

派生字段只依赖其他标准字段：

```yaml
standard_field: market_cap
base_node: stock_daily_real
resolver_type: derived
depends_on:
  - close
  - tot_share
formula: "{close} * ({tot_share} * 10000)"
```

## 3. 运行时元数据接口

当前推荐按下面顺序理解协议：

1. `GraphRuntime.get_protocol_summary()`
2. `GraphRuntime.get_real_fields_json()`
3. `GraphRuntime.get_supported_fields(...)`

### 3.1 `get_protocol_summary()`

作用：

1. 返回当前启用入口节点摘要
2. 返回字段规模
3. 返回每个节点的示例字段

适合：

1. 上层模块先快速了解当前协议规模
2. AI 先看入口摘要，不先吃完整 catalog

### 3.2 `get_real_fields_json()`

作用：

1. 按 real 节点返回字段英文名和中文名
2. 给前端字段选择器或 AI 做下一步精确字段选择

### 3.3 `get_metadata_catalog()`

作用：

1. 返回完整底层元数据
2. 适合调试，不适合默认直接喂给上层模块

## 4. 当前正式入口节点

当前正式入口节点以 `get_protocol_summary()` 和 `get_real_fields_json()` 返回结果为准。

通用上可以分成：

1. 股票日线/分钟
2. ETF 日线/分钟
3. 指数日线/分钟
4. 行业日线
5. 基金/宏观/可转债/融资融券专项入口

## 5. 推荐调用顺序

推荐上层调用顺序：

1. `resolve_symbols(...)`
2. `get_protocol_summary()`
3. `get_real_fields_json()`
4. `get_supported_fields(...)`
5. `validate_query_request(...)`
6. `query_daily(...)` / `query_minute(...)`

## 6. 当前结论

当前版本已经从“节点关系图驱动”进一步收敛成：

1. 入口节点稳定
2. 字段关系显式
3. SQL 可审计
4. 图谱更轻
5. 上层更容易接入
