# Handoff Summary

## 1. 当前版本定位

当前版本可以视为：

**AIQuantBase V1 - 面向上层应用的稳定数据接口层**

它已经不只是底层图谱能力项目，而是具备：

1. 图谱能力层
2. 应用层 facade
3. Python SDK
4. SSR 图谱管理网页
5. 文档与边界说明

## 2. 当前正式默认配置

当前正式默认配置已收口到 `config/`：

1. `config/graph.yaml`
2. `config/fields.yaml`
3. `config/runtime.local.yaml`

默认情况下：

1. `GraphRuntime` 读取 `config/`
2. `ApplicationRuntime` 读取 `config/`
3. 服务端读取 `config/`
4. 网页管理台读取 `config/`

## 3. 当前对外正式入口

### 推荐上层应用使用

```python
from aiquantbase import ApplicationRuntime

runtime = ApplicationRuntime.from_defaults()
```

### 当前推荐正式接口

1. `resolve_symbols(...)`
2. `resolve_best_node(...)`
3. `get_supported_fields(...)`
4. `validate_query_request(...)`
5. `query_daily(...)`
6. `query_minute(...)`
7. `build_intent_from_requirement(...)`
8. `execute_requirement(...)`

## 4. 当前仍保留但不建议默认暴露的底层接口

通过 `GraphRuntime` 仍然可以使用：

1. `render_intent(...)`
2. `execute_intent(...)`
3. `execute_sql(...)`
4. `get_metadata_catalog()`
5. `get_real_nodes()`
6. `get_real_fields_json()`

这些更适合：

1. 调试
2. 图谱管理
3. 排查 SQL
4. 高级使用场景

## 5. 当前推荐主入口节点

当前默认推荐对外暴露的主入口节点：

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

## 6. 当前字段协议能力

字段协议当前已经具备：

1. `description_zh`
2. `field_role`
3. `path_domain`
4. `path_group`
5. `via_node`
6. `time_semantics`
7. `lookahead_category`

## 7. 当前 lookahead 状态

当前 `lookahead_safe` 第一阶段已接入执行层。

### 已生效范围

1. 公告类字段
2. 事件类字段
3. 条款生效字段

### 当前明确不影响

1. `close`
2. `high`
3. `low`
4. `volume`
5. `amount`
6. `market_cap`
7. `turnover_rate`

## 8. 当前应用层保护规则

已加入：

1. 分钟查询必须带 `symbols`
2. 分钟查询时间窗限制
3. 日频无 symbols 长时间范围限制
4. ETF 等资产类型字段支持校验

## 9. 当前测试状态

最近一轮完整测试已通过。

当前项目已具备：

1. 应用层 facade 测试
2. SDK 测试
3. 页面测试
4. planner / inference / discovery 测试

## 10. 当前文档体系

### 项目入口

1. `README.md`
2. `docs/README-usage.md`

### 上层应用接口

1. `docs/application-api-usage.md`
2. `docs/application-runtime-examples.md`
3. `docs/application-runtime-api-reference.md`
4. `docs/application-runtime-quickstart.md`
5. `docs/alphablocks-integration-example.md`
6. `docs/alphablocks-query-optimization-summary.md`

### 迁移与发布

1. `docs/release-notes-v1.md`
2. `docs/migration-notes.md`
3. `CHANGELOG.md`
4. `docs/go-live-checklist.md`

### 底层能力与边界

1. `docs/python-sdk-usage.md`
2. `docs/query-intent-templates.md`
3. `docs/public-entry-nodes.md`
4. `docs/trusted-query-boundary.md`
5. `docs/final-graph-relationship.md`
6. `docs/field-description-catalog.md`
7. `docs/node-field-coverage.md`

## 11. 当前可信边界

当前可信前提：

1. 主入口明确
2. 标准字段明确
3. 图谱路径明确
4. SQL 可审计

当前不承诺：

1. 全自动任意字段驱动查询
2. 全自动跨多业务主体合并查询
3. 完全自动拆查询
4. 无边界自由 Text2SQL

## 12. 当前建议的后续方向

如果继续推进，优先建议：

1. 用真实上层应用做联调
2. 根据联调结果修 `ApplicationRuntime` 的边界和错误码
3. 参考 `docs/alphablocks-query-optimization-summary.md` 继续推进查询性能优化
4. 再决定是否扩展更多自动化能力

## 13. 一句话总结

当前项目已经适合作为：

**面向上层应用的 A 股研究数据接口层 V1 正式启用。**
