# Path Constraint And Asset Field Layering

## 本次改动目标

这次调整集中处理三类事情：

1. 把多路径字段的查询冲突显式约束住，避免结果集被无意放大。
2. 把 `ETF / 可转债 / 期权` 从“按表接入”继续推进到“按语义分层 + 第一批派生字段层”。
3. 把新增的 `龙虎榜 / 融资融券` 表也纳入更细的 `path/field_role` 语义层。

## 1. 已纳入 path 机制的业务域

当前已经纳入 `path_domain / path_group / via_node` 机制的业务域包括：

1. `industry`
2. `index`
3. `fund`
4. `macro`
5. `margin`
6. `trading_event`

这套机制的核心作用是：

1. 让共享字段在多条可达路径里稳定选中正确路径
2. 当一条查询混用了互斥路径时，直接报错
3. 让 SQL 规划显式经过指定中间节点，而不是只按最短路径猜测

## 2. Industry 路径约束

### 可用路径

当前行业相关字段存在 3 条可用路径：

1. `stock_daily_real -> industry_weight_real -> industry_base_info_real`
2. `stock_daily_real -> industry_constituent_real -> industry_base_info_real`
3. `industry_daily_real -> industry_base_info_real`

### 路径组

`industry` 域当前分为：

1. `weight`
2. `constituent`
3. `daily`

### 当前落地

1. `industry_index_code`、`industry_weight` 绑定到 `industry.weight`
2. `industry_constituent_index_code`、`industry_constituent_index_name` 绑定到 `industry.constituent`
3. `industry_code`、`industry_level1_name`、`industry_level2_name`、`industry_level3_name`、`industry_name` 做成三版本共享字段

## 3. Index / Fund / Macro / Margin / Trading Event 路径约束

### Index

`index` 域分成两组：

1. `weight`
2. `constituent`

当前字段归属：

1. `index_code`、`index_weight`、`index_component_close` 属于 `index.weight`
2. `index_name`、`index_constituent_code` 属于 `index.constituent`

### Fund

`fund` 域当前分成两组：

1. `iopv`
2. `share`

### Macro

`macro` 域当前先落一个组：

1. `treasury`

### Margin

`margin` 域现在分成两组：

1. `detail`
   对应 `margin_detail_real`
2. `summary`
   对应 `margin_summary_real`

这层的意义不是为了处理当前的多路径冲突，而是先把融资融券字段放进统一的路径协议里，后面扩到更多融资融券来源表时可以直接复用。

### Trading Event

`trading_event` 域当前先落一个组：

1. `long_hu_bang`
   对应 `long_hu_bang_real`

这让龙虎榜字段也具备与其他业务域一致的路径元信息。

## 4. ETF 标准字段分层

ETF 字段按语义拆成：

1. `etf_overview_field`
2. `etf_status_field`
3. `etf_cash_nav_field`
4. `etf_limit_field`
5. `etf_structure_field`
6. `etf_underlying_ref_field`
7. `etf_constituent_identity_field`
8. `etf_constituent_position_field`
9. `etf_constituent_cash_substitute_field`

### 第一批 ETF 派生字段

1. `etf_cash_component_ratio`
2. `etf_limit_spread`
3. `etf_net_limit_spread`
4. `etf_constituent_cash_substitute_diff`
5. `etf_constituent_premium_discount_spread`

## 5. 可转债标准字段分层

可转债字段细分为：

1. `kzz_call_field`
2. `kzz_call_clause_field`
3. `kzz_conversion_field`
4. `kzz_conversion_detail_field`
5. `kzz_correction_clause_field`
6. `kzz_put_field`
7. `kzz_put_clause_field`
8. `kzz_put_call_item_field`
9. `kzz_balance_field`
10. `kzz_suspend_field`
11. `kzz_issuance_overview_field`
12. `kzz_clause_field`
13. `kzz_listing_subscription_field`
14. `kzz_coupon_field`
15. `kzz_issuance_misc_field`

### 第一批可转债派生字段

1. `kzz_call_price_premium_rate`
2. `kzz_put_price_premium_rate`
3. `kzz_balance_conversion_ratio`
4. `kzz_coupon_rate_decimal`
5. `kzz_clause_ini_conv_premium_ratio_decimal`

## 6. 期权标准字段分层

期权字段按“合约身份、生命周期、规格、调整”拆层：

1. `option_contract_identity_field`
2. `option_contract_lifecycle_field`
3. `option_contract_pricing_field`
4. `option_contract_spec_field`
5. `option_contract_adjustment_field`

### 第一批期权派生字段

1. `option_basic_contract_notional`
2. `option_mon_exercise_price_change`
3. `option_mon_unit_change`
4. `option_std_contract_value_per_multiplier`
5. `option_std_option_strike_price_num`

## 7. 龙虎榜与融资融券语义层

### 龙虎榜字段层

新增 `long_hu_bang_real` 后，字段继续细分为：

1. `long_hu_identity_field`
   例如：`long_hu_trade_date`、`long_hu_security_name`

2. `long_hu_reason_field`
   例如：`long_hu_reason_type`、`long_hu_reason_type_name`、`long_hu_change_range`

3. `long_hu_trader_field`
   例如：`long_hu_trader_name`、`long_hu_flow_mark`

4. `long_hu_amount_field`
   例如：`long_hu_buy_amount`、`long_hu_sell_amount`、`long_hu_total_amount`

5. `long_hu_derived_field`
   例如：`long_hu_net_amount`

### 融资融券字段层

`margin_detail_real` 细分为：

1. `margin_detail_identity_field`
2. `margin_detail_balance_field`
3. `margin_detail_flow_field`
4. `margin_detail_derived_field`

`margin_summary_real` 细分为：

1. `margin_summary_identity_field`
2. `margin_summary_balance_field`
3. `margin_summary_flow_field`
4. `margin_summary_derived_field`

## 8. 对系统的直接收益

### 查询正确性更稳

1. 多路径字段不再依赖最短路径碰运气
2. 混用互斥路径时会直接失败，而不是生成含糊 SQL

### 字段语义更清晰

1. 现在不仅知道字段来自哪张表
2. 还知道它属于哪个路径域、哪个路径组、哪个语义层
3. 派生字段已经开始形成标准协议

### 更利于后续图谱管理

后面无论是 CLI 管理还是图形化工作台，`path_domain / path_group / via_node / field_role` 都可以直接作为可维护配置项展示出来。

## 9. 当前限制

1. `fund / macro / margin / trading_event` 当前虽然已经纳入同一机制，但还没有全部出现明显的多路径冲突场景
2. 当前冲突处理策略仍然是“直接报错”，还没有做自动拆查询
3. 派生字段目前主要还是单节点或轻依赖字段，尚未扩到更复杂的跨节点派生协议

## 10. 下一步建议

1. 给 `margin` 扩更多来源表时继续沿用 `detail / summary` 的 path 约束
2. 给 `long_hu_bang` 增加事件强度、净买卖占比等派生字段
3. 继续把新业务表都纳入统一的 `path/field_role` 配置协议

## 11. Index 与 KZZ 语义别名层

为了降低“字段名看起来差不多，但实际来自不同路径/不同语义来源”的误用风险，当前又补了一层显式别名字段。

### Index 显式别名

新增：

1. `index_weight_code`
   明确表示来自 `index.weight` 路径的指数代码

2. `index_weight_name`
   明确表示需要通过成分路径拿到的指数名称

3. `index_constituent_name`
   明确表示来自 `index.constituent` 路径的指数名称

这层别名的目标不是替换原字段，而是让上层在写 Query Intent 或做自然语言归一化时，可以优先落到更明确的名字。

### KZZ 语义别名

新增的别名主要按 4 种来源做区分：

1. `event`
   例如：
   - `kzz_call_event_date`
   - `kzz_call_event_price`
   - `kzz_put_event_price`

2. `explanation`
   例如：
   - `kzz_call_explanation_announce_date`
   - `kzz_put_explanation_announce_date`

3. `clause`
   例如：
   - `kzz_call_clause_start_date`
   - `kzz_call_clause_trigger_ratio`
   - `kzz_put_clause_start_date`
   - `kzz_put_clause_trigger_ratio`

4. `rule`
   例如：
   - `kzz_conv_rule_announce_date`
   - `kzz_conv_rule_price`
   - `kzz_conv_rule_start_date`
   - `kzz_conv_rule_end_date`

以及针对 `put_call_item` 的条款细项别名：

1. `kzz_put_item_call_trigger_period`
2. `kzz_put_item_call_condition_text`

### 这层别名的意义

1. 原字段继续保留，兼容已有调用
2. 新别名字段给上层更稳定的语义入口
3. 后续 AI 生成 Query Intent 时，可以优先产出这些更明确的名字

## 12. 字段中文说明层

当前字段目录已经补充为“每个标准字段都带 `description_zh` 中文说明”。

### 设计原则

1. `description_zh` 作为正式字段元数据存在，不再只依赖 `notes`
2. 原有 `notes` 保留，兼容已有英文或补充性备注
3. 上层如果需要给 AI、CLI、图谱管理界面暴露字段说明，优先使用 `description_zh`

### 当前效果

1. 现有字段目录中的全部标准字段都已经补齐中文说明
2. 即使原始备注是英文，也已经补到中文描述
3. 后续新增字段时，应该同步填写 `description_zh`

## 13. 时间语义标签层

为了进一步降低日期字段被误用的风险，当前字段目录已经开始补充 `time_semantics` 标签。

### 当前使用的标签示例

1. `market_trade_date`
   用于交易日、市场日频日期

2. `financial_announce_date`
   用于财报、业绩类公告日期

3. `corporate_action_announce_date`
   用于分红、配股、质押等公司行为公告日期

4. `fund_announce_date`
   用于基金份额公告日期

5. `holder_announce_date`
   用于股东户数、股东明细公告日期

6. `kzz_announce_date`
   用于可转债相关公告日期

7. `effective_start_date`
   用于生效起始日期

8. `effective_end_date`
   用于生效结束日期

9. `event_date`
   用于事件发生日期

10. `listing_start_date`
    用于上市或交易开始日期

11. `listing_end_date`
    用于退市或交易结束日期

12. `expiry_date`
13. `exercise_date`
14. `exercise_end_date`
15. `delivery_date`
16. `settlement_date`
17. `last_trade_date`
18. `system_run_date`
19. `system_checkpoint_date`
20. `system_start_date`
21. `system_end_date`

### 这层标签的作用

1. 帮助 AI 区分“交易日”和“公告日”
2. 帮助上层在相似日期字段之间做稳定选择
3. 为后续自然语言转 Query Intent 提供更明确的时间语义约束

## 14. Lookahead 第一阶段约束

当前 `lookahead_safe` 的第一阶段设计已经确定为：

1. 只作用在公告/事件类字段
2. 不影响常规行情终值字段

### 当前新增字段标签

字段目录新增：

1. `lookahead_category`

当前第一阶段只使用两类：

1. `published_event`
2. `none`

### 规则

当 `lookahead_safe = true` 时：

1. `lookahead_category = published_event` 的字段进入防未来约束范围
2. `lookahead_category = none` 的字段不受这一阶段约束影响

### 第一阶段明确不影响的字段

当前明确不受这一阶段影响的常规字段包括：

1. `close`
2. `high`
3. `low`
4. `volume`
5. `amount`
6. `market_cap`
7. `turnover_rate`

### 第一阶段重点约束对象

例如：

1. 财报公告日期
2. 分红配股公告日期
3. 股东公告日期
4. 质押冻结公告日期
5. 可转债公告/说明/条款日期
6. 事件发生日期与条款生效日期
