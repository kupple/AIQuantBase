# Release Notes V1

## 版本定位

这一版标志着 AIQuantBase 从“图谱底层能力集合”进入“可供上层应用正式调用的数据接口层 V1”。

当前版本重点不是继续扩 SQL 能力，而是：

1. 明确默认配置入口
2. 明确主入口节点
3. 明确字段协议
4. 明确应用层 facade
5. 明确可信边界

## 本版重点更新

### 1. 默认配置收口到 `config/`

正式默认配置统一到：

1. `config/graph.yaml`
2. `config/fields.yaml`
3. `config/runtime.local.yaml`

默认情况下：

1. Python SDK 读取这组配置
2. 服务端读取这组配置
3. 网页管理台读取这组配置

### 2. 图谱字段协议增强

字段协议当前已经包含：

1. `description_zh`
2. `field_role`
3. `path_domain`
4. `path_group`
5. `via_node`
6. `time_semantics`
7. `lookahead_category`

这使得字段不再只是“来自某张表”，而是具备：

1. 中文说明
2. 时间语义
3. 路径约束
4. lookahead 分类

### 3. `lookahead_safe` 第一阶段落地

当前 `lookahead_safe = true` 第一阶段已经接入执行层。

当前只作用于：

1. 公告类字段
2. 事件类字段
3. 条款生效类字段

当前明确不影响：

1. `close`
2. `high`
3. `low`
4. `volume`
5. `amount`
6. `market_cap`
7. `turnover_rate`

### 4. 主入口节点边界明确

当前推荐的主入口节点为：

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

其他节点继续保留在内部图谱中，但不建议默认直接对外暴露。

### 5. 网页管理台切换到 SSR 方案

网页层当前采用 SSR 页面方案，主要入口：

1. `/nodes`
2. `/edges`
3. `/graph/view`
4. `/llm-query`

同时保留当前底层图谱与元数据能力。

### 6. Python SDK 可正式使用

底层 SDK：

1. `GraphRuntime`

应用层 facade：

1. `ApplicationRuntime`

### 7. 应用层第一批正式接口上线

当前 `ApplicationRuntime` 已提供：

1. `resolve_symbols(...)`
2. `resolve_best_node(...)`
3. `get_supported_fields(...)`
4. `validate_query_request(...)`
5. `query_daily(...)`
6. `query_minute(...)`
7. `build_intent_from_requirement(...)`
8. `execute_requirement(...)`

### 8. 常用查询过滤能力补齐

当前已明确支持：

1. `=`
2. `in`
3. `not_in`
4. `between`
5. `like`
6. `ilike`
7. `contains`
8. `is_null`
9. `is_not_null`

### 9. 查询规模保护加入应用层

当前已加的保护包括：

1. 分钟查询必须带 `symbols`
2. 分钟查询时间窗限制
3. 日频无 symbols 长时间范围查询限制

### 10. 文档体系基本成型

当前已经补齐：

1. 项目入口文档
2. Python SDK 使用文档
3. Query Intent 模板文档
4. 主入口节点说明
5. 可信边界说明
6. AlphaBlocks 对接示例
7. ApplicationRuntime API Reference
8. 上线前检查清单

## 当前状态

当前版本更适合作为：

1. 图谱约束查询中间件
2. 上层应用数据接口层
3. Python SDK
4. 网页图谱管理后台

## 当前不追求的目标

当前版本仍然不承诺：

1. 全自动任意字段驱动查询
2. 全自动跨多业务主体拼接查询
3. 完全自动拆查询
4. 无边界自由 Text2SQL

## 当前测试状态

当前全量测试通过。
