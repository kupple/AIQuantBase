# 研究数据标准化系统需求文档

## 1. 背景

当前量化研究系统已经具备：

1. `Phase 1`：研究课题 -> 研究合同
2. `Phase 2`：研究合同 -> 查询计划 / SQL
3. `Phase 3`：执行研究并输出结果

但随着研究维度增加，系统遇到一个核心问题：

很多研究字段并不是某张日线表直接提供的字段，而是需要：****

1. 跨表取值
2. 不同粒度数据对齐
3. 非对称日期对齐
4. 派生公式计算

典型例子：

1. `market_cap = close * total_shares`
2. `float_market_cap = close * float_shares`
3. `turnover_rate = volume_shares / float_shares`
4. `gap_open_return = open / prev_close - 1`

其中：

1. 行情表通常是日频或分钟频
2. 股本表往往不是每天都有记录
3. 行业分类、财务、状态表常常是快照表或区间生效表

如果每次研究都在 SQL 层手工写 join 和公式，系统会越来越难维护，也不利于 AI 自动研究。

因此需要构建一个独立的新项目，专门负责：

**将原始数据库表标准化为可研究的标准字段系统。**

---

## 2. 目标

新项目的核心目标是：

1. 定义统一的标准研究键
2. 定义统一的标准研究字段
3. 通过简单配置描述字段来源、对齐规则和推导公式
4. 让系统根据规则自动生成连表和字段表达式
5. 最终让上层研究系统尽量不直接依赖 SQL 和底层表名

最终效果希望达到：

1. 用户或 AI 只需要声明要什么字段
2. 系统自动决定从哪些表获取
3. 系统自动决定如何对齐日期
4. 系统自动生成标准研究数据

---

## 3. 非目标

第一版不追求：

1. 完整的通用数据湖系统
2. 所有市场和所有资产类别统一支持
3. 复杂的任意公式 DSL
4. 完全开源友好的超泛化实现

第一版优先目标是：

1. 先服务股票研究
2. 先服务日频和分钟频
3. 先把高频使用的标准字段打通
4. 先把规则稳定下来

---

## 4. 设计原则

### 4.1 统一键模型

所有字段必须先归一化到统一主键。

建议固定两类键：

1. `stock_daily_key = (code, trade_date)`
2. `stock_minute_key = (code, trade_time)`

只有能够归一化到这两类键的字段，才允许进入标准研究层。

### 4.2 研究层只认标准字段名

研究系统不应直接依赖：

1. 表名
2. 字段原名
3. 某个项目私有数据库结构

研究层只认标准字段，例如：

1. `open_raw`
2. `close_raw`
3. `close_adj`
4. `market_cap`
5. `float_market_cap`
6. `turnover_rate`
7. `industry_code`
8. `gap_open_return`

### 4.3 连表逻辑必须配置化

所有跨表 join 和时间对齐规则必须通过配置驱动，不应分散写在业务逻辑中。

### 4.4 原始字段和派生字段要区分

标准字段需要区分：

1. 原始字段
2. 派生字段

例如：

1. `close_raw` 是原始字段
2. `market_cap` 是派生字段
3. `turnover_rate` 是派生字段

### 4.5 原始价与复权价并存

标准研究层中建议同时保留：

1. 原始价：`open_raw/high_raw/low_raw/close_raw`
2. 复权价：`open_adj/high_adj/low_adj/close_adj`

原则：

1. 规则判断优先使用原始价
2. 收益研究优先使用复权价

---

## 5. 标准字段系统的核心对象

建议新项目至少包含三个核心模块：

1. `source registry`
2. `field rules`
3. `field resolver`

### 5.1 Source Registry

作用：

描述系统有哪些原始数据源。

每个 source 需要描述：

1. `source_name`
2. `table`
3. `entity_type`
4. `grain`
5. `symbol_col`
6. `time_col`
7. `fields`

示例：

```json
{
  "sources": [
    {
      "source_name": "daily_price",
      "table": "starlight.ad_market_kline_daily",
      "entity_type": "stock",
      "grain": "daily",
      "symbol_col": "code",
      "time_col": "trade_time",
      "fields": {
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume"
      }
    }
  ]
}
```

### 5.2 Field Rules

作用：

定义标准字段如何解析。

每个字段需要描述：

1. `field_name`
2. `key_type`
3. `resolver_type`
4. `depends_on`
5. `source`
6. `alignment_rule`
7. `formula`

示例：

```json
{
  "field_name": "market_cap",
  "key_type": "stock_daily",
  "resolver_type": "derived",
  "depends_on": ["close_raw", "total_shares"],
  "formula": "{close_raw} * {total_shares}"
}
```

### 5.3 Field Resolver

作用：

输入标准字段名，输出：

1. SQL 表达式
2. 依赖字段
3. 所需 source
4. 对齐规则
5. 诊断信息

---

## 6. 配置层要求

配置层必须尽量简单，不希望用户维护过于复杂的 DSL。

建议配置拆成两份：

1. `research_sources.json`
2. `research_fields.json`

设计要求：

1. 尽量 JSON 化
2. 规则扁平
3. 少嵌套
4. 先覆盖常见场景
5. 不要求第一版支持所有复杂表达式

---

## 7. 字段解析方式

第一版建议只支持四类解析方式：

### 7.1 `direct`

原表直接字段。

适用：

1. `close_raw`
2. `open_raw`
3. `volume_raw`

### 7.2 `source_field`

来自某个外部 source 的字段，需要按规则 join。

适用：

1. `total_shares`
2. `float_shares`
3. `industry_code`

### 7.3 `derived`

由其他标准字段计算。

适用：

1. `market_cap`
2. `float_market_cap`
3. `turnover_rate`
4. `gap_open_return`

### 7.4 `sql_expr`

复杂场景兜底，允许直接给 SQL 模板。

适用：

1. 第一版难以结构化表达的复杂字段

---

## 8. 日期对齐规则

这是系统设计的重点。

建议第一版固定支持这些对齐类型：

### 8.1 `same_trade_date`

适用：

1. 日频表和日频表同日对齐
2. 分钟表引用日频字段，但只按交易日对齐

示例：

```text
toDate(base.trade_time) = toDate(source.trade_date)
```

### 8.2 `same_timestamp`

适用：

1. 分钟表和分钟表严格时间戳对齐

示例：

```text
base.trade_time = source.trade_time
```

### 8.3 `asof_trade_date`

适用：

1. 股本表
2. 快照型财务表
3. 非日频但可按最近日期生效的表

语义：

```text
取同 code 下，小于等于 trade_date 的最近一条记录
```

建议实现方式：

1. 先把快照表转成区间表
2. 再使用 `effective_range` 方式 join

### 8.4 `effective_range`

适用：

1. 区间生效表
2. 行业映射表
3. 成分股区间表
4. 状态区间表

示例：

```text
base.trade_date >= effective_start
AND base.trade_date < effective_end
```

### 8.5 `same_symbol_only`

适用：

1. 静态维表
2. 当前版本里不强依赖时间的辅助属性

---

## 9. 非对称日期对齐的处理方式

像股本表不是每天都有，就是典型的非对称日期对齐问题。

推荐标准做法：

### 9.1 原始快照表

```text
code, effective_date, total_shares, float_shares
```

### 9.2 转换成区间表

```text
code, effective_start, effective_end, total_shares, float_shares
```

其中：

1. `effective_start = effective_date`
2. `effective_end = next_effective_date`

### 9.3 日线使用方式

```text
trade_date >= effective_start
AND trade_date < effective_end
```

这一步完成后，后续公式就非常简单：

```text
market_cap = close_raw * total_shares
float_market_cap = close_raw * float_shares
turnover_rate = volume_shares / float_shares
```

---

## 10. 分钟数据怎么处理

分钟问题不建议混入日频标准层。

建议明确分成两套标准键：

1. `stock_daily_key = (code, trade_date)`
2. `stock_minute_key = (code, trade_time)`

并设计两套标准研究层：

1. `research_stock_daily`
2. `research_stock_minute`

分钟场景下的字段支持策略：

### 10.1 分钟直接字段

可直接支持：

1. `open`
2. `high`
3. `low`
4. `close`
5. `volume`

### 10.2 分钟引用日频字段

如果分钟研究需要日频辅助字段，比如：

1. `industry_code`
2. `market_cap`
3. `float_shares`
4. `is_st`

则使用：

```text
same_trade_date
```

即按分钟对应的交易日去引用日频字段。

### 10.3 分钟引用快照字段

如需分钟场景下引用股本等快照字段：

1. 先用 `asof_trade_date` 对齐到分钟对应交易日
2. 再在分钟研究中使用

即：

```text
code + toDate(trade_time)
```

而不是按分钟级快照逐条匹配。

第一版不建议支持：

1. 分钟级快照字段的 `asof_timestamp`
2. 高频微结构级别的复杂对齐

---

## 11. 推荐的标准字段

第一版建议优先支持这些字段：

### 11.1 行情基础字段

1. `open_raw`
2. `high_raw`
3. `low_raw`
4. `close_raw`
5. `volume_raw`
6. `amount_raw`
7. `prev_close`

### 11.2 复权字段

1. `backward_factor`
2. `open_adj`
3. `high_adj`
4. `low_adj`
5. `close_adj`

### 11.3 股本相关

1. `total_shares`
2. `float_shares`
3. `market_cap`
4. `float_market_cap`
5. `turnover_rate`

### 11.4 分类字段

1. `industry_code`
2. `industry_name`

### 11.5 常见派生字段

1. `gap_open_return`
2. `next_day_open_return`
3. `next_day_close_return`

---

## 12. 第一版配置示例

### 12.1 `research_sources.json`

```json
{
  "sources": [
    {
      "source_name": "daily_price",
      "table": "starlight.ad_market_kline_daily",
      "entity_type": "stock",
      "grain": "daily",
      "symbol_col": "code",
      "time_col": "trade_time",
      "fields": {
        "open_raw": "open",
        "high_raw": "high",
        "low_raw": "low",
        "close_raw": "close",
        "volume_raw": "volume"
      }
    },
    {
      "source_name": "capital_snapshot",
      "table": "starlight.ad_stock_capital",
      "entity_type": "stock",
      "grain": "snapshot",
      "symbol_col": "code",
      "time_col": "effective_date",
      "fields": {
        "total_shares": "total_shares",
        "float_shares": "float_shares"
      }
    }
  ]
}
```

### 12.2 `research_fields.json`

```json
{
  "fields": [
    {
      "field_name": "close_raw",
      "key_type": "stock_daily",
      "resolver_type": "direct",
      "source_name": "daily_price",
      "field": "close_raw"
    },
    {
      "field_name": "total_shares",
      "key_type": "stock_daily",
      "resolver_type": "source_field",
      "source_name": "capital_snapshot",
      "field": "total_shares",
      "alignment_rule": "asof_trade_date"
    },
    {
      "field_name": "market_cap",
      "key_type": "stock_daily",
      "resolver_type": "derived",
      "depends_on": ["close_raw", "total_shares"],
      "formula": "{close_raw} * {total_shares}"
    }
  ]
}
```

---

## 13. 第一版实现范围

第一版只要求完成：

1. 配置读取
2. 标准字段规则校验
3. `direct / source_field / derived` 三类解析
4. `same_trade_date / same_timestamp / asof_trade_date / effective_range` 四类对齐
5. 日频字段解析优先
6. 分钟只支持：
   - 分钟直接字段
   - 引用日频字段

---

## 14. 与现有研究系统的集成方式

建议集成方式：

### 14.1 Phase 1

合同中继续只写标准字段名：

```json
"grouping_spec": {
  "dimension": "market_cap",
  "method": "quantile",
  "params": { "buckets": 5 }
}
```

### 14.2 Phase 2

读取标准字段名后，调用字段解析器：

1. 解析字段
2. 自动补 source
3. 自动补 join
4. 自动生成 SQL 表达式

### 14.3 Phase 3

Phase 3 不需要关心底层连表逻辑，只消费已经标准化后的研究数据。

---

## 15. 第一阶段优先落地建议

最优先建议先打通：

1. `market_cap`
2. `float_market_cap`
3. `turnover_rate`
4. `gap_open_return`

原因：

1. `cross_section` 研究强依赖这些维度
2. `event_study` 分组研究也会大量用到它们
3. 这些字段最能检验标准字段系统是否正确

---

## 16. 一句话总结

新项目的核心，不是“写更多 SQL”，而是：

**把原始数据库表，归一化成标准研究字段系统，并通过统一的 `code + date / code + time` 规则来驱动自动连表和字段解析。**

只要这层建立起来，后续：

1. 研究课题表达会更稳定
2. AI 更容易自动研究
3. 新维度接入成本会显著下降
4. 查询页可以尽量不直接暴露 SQL

