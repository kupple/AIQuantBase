# 能力接入 output_scope 落地计划

## 目标

AIQuantBase 负责维护能力注册和真实数据节点映射，AlphaBlocks 的不同模式只声明自己接受什么输出维度。用户在能力接入页面新增扩展能力时，不需要理解 Provider 配置路径，只需要选择能力字段、使用位置和输出维度。

## 当前约定

- `daily_panel`：日频面板，典型键为 `entity + time`，例如股票 `code + trade_time`。
- `intraday_panel`：盘中面板，典型键为 `entity + time`，例如股票分钟 `code + trade_time`。
- `event_stream`：事件流，典型键为 `entity + event_time`，例如分红、公告、龙虎榜事件。
- `linked_daily_panel`：关联日频，数据来自关联实体但最终映射回当前实体，例如股票通过行业成分映射行业日线。

## discrete_stock 当前规则

- 必需能力先固定，避免运行基础能力被配置成不完整状态。
- 扩展能力由策略合同引用时才查询。
- 当前接受 `daily_panel(stock)` 和 `linked_daily_panel(output_entity_type=stock)`。
- 估值、自定义因子、龙虎榜日频默认是股票日频面板。
- 行业分类默认是关联日频，后续可通过行业成分关系映射回股票。

## 实施顺序

1. 在 AIQuantBase capability registry 写入 `output_scope`。
2. 在模式配置写入 `accepted_output_scopes`。
3. AIQuantBase 能力预览校验扩展能力输出维度是否被当前模式接受。
4. AlphaBlocks SDK 读取能力工作区时透传 `output_scope`。
5. `/capabilities` 页面在新增/编辑扩展能力弹窗内配置输出维度。
6. 补回归测试，保证配置、预览和 AlphaBlocks query plan 都能拿到输出维度。

## 后续

- 行业 real 表不强行并入 `stock_daily_real`，用 `linked_daily_panel` 表达映射关系。
- 财务、行业、龙虎榜、自定义因子继续作为扩展能力，由 slot 和 output_scope 共同约束。
- 暂不改 `/database` 页面，字段目录和节点表仍由现有 AIQuantBase 配置维护。

## 当前落地状态

- `industry_classification` 已注册为 `linked_daily_panel`，来源节点是 `stock_industry_daily_real`，输出维度是股票日频。
- AlphaBlocks Adapter 已支持该输出维度：运行时按股票 `code + trade_time` 面板输出，并通过 AIQuantBase 图谱桥接行业字段。
- 字符串扩展字段已支持进入标准化 data view，例如 `industry_level1_name` 会以 `kind: string` 输出。
- `effective_range` 关系已支持空结束日期，表示关系仍然有效。
- source table join 会应用图谱节点 `base_filters`，例如行业基础信息限定 `level_type=3`、`is_pub=1`。
