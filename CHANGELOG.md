# Changelog

## V1 - ApplicationRuntime 接口层

### 版本定位

V1 标志着 AIQuantBase 从底层图谱能力项目，进入面向上层应用的稳定数据接口层。

本版本主入口为：

```python
from aiquantbase import ApplicationRuntime

runtime = ApplicationRuntime.from_defaults()
```

## 新增能力

### 1. ApplicationRuntime

新增上层应用 facade：

1. `resolve_symbols(...)`
2. `resolve_best_node(...)`
3. `get_supported_fields(...)`
4. `validate_query_request(...)`
5. `query_daily(...)`
6. `query_minute(...)`
7. `build_intent_from_requirement(...)`
8. `execute_requirement(...)`

### 2. Query Intent 图谱执行能力

保留底层能力：

1. 图谱加载
2. 字段协议加载
3. Query Intent 规划
4. SQL 渲染
5. ClickHouse 执行
6. DataFrame 返回

### 3. 字段协议增强

字段协议已具备：

1. `description_zh`
2. `field_role`
3. `path_domain`
4. `path_group`
5. `via_node`
6. `time_semantics`
7. `lookahead_category`

### 4. 主入口节点收口

当前推荐对外主入口：

1. `stock_daily_real`
2. `stock_minute_real`
3. `industry_daily_real`
4. `treasury_yield_real`
5. `fund_iopv_real`
6. `fund_share_real`
7. `etf_pcf_real`
8. `kzz_issuance_real`
9. `kzz_conv_real`
10. `margin_summary_real`

### 5. lookahead 第一阶段

`lookahead_safe = true` 当前只对公告/事件/条款类字段生效。

当前明确不影响：

1. `close`
2. `high`
3. `low`
4. `volume`
5. `amount`
6. `market_cap`
7. `turnover_rate`

### 6. 查询保护

新增应用层查询保护：

1. 分钟查询必须带 `symbols`
2. 分钟查询限制时间窗
3. 日频无 symbols 查询限制时间范围

### 7. 常用过滤操作符

当前已明确支持：

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

### 8. SSR 网页管理台

新增/保留：

1. `/nodes`
2. `/edges`
3. `/graph/view`
4. `/llm-query`

## 默认配置

正式默认配置已经收口到：

1. `config/graph.yaml`
2. `config/fields.yaml`
3. `config/runtime.local.yaml`

## 推荐使用方式

### 上层应用

```python
from aiquantbase import ApplicationRuntime

runtime = ApplicationRuntime.from_defaults()
result = runtime.execute_requirement(data_requirement)
```

### 底层调试

```python
from aiquantbase import GraphRuntime

runtime = GraphRuntime.from_defaults()
```

## 当前可信边界

当前可信前提：

1. 主入口明确
2. 标准字段明确
3. 图谱路径明确
4. SQL 可审计

当前不承诺：

1. 全自动任意字段驱动查询
2. 全自动跨业务主体合并查询
3. 完全自动拆查询
4. 无边界自由 Text2SQL

## 文档入口

推荐先看：

1. `README.md`
2. `docs/application-runtime-quickstart.md`
3. `docs/application-runtime-api-reference.md`
4. `docs/alphablocks-integration-example.md`
5. `docs/trusted-query-boundary.md`
