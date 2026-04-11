# Node Field Coverage

当前文档按 node 统计：

1. 节点基础信息
2. 节点自身原始字段
3. 以该节点为查询入口时可用的标准字段

## `stock_daily_real`

- 中文说明：股票日线行情主表
- 表：`starlight.ad_market_kline_daily`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`True`
- 自身原始字段数：`8`
- 可用标准字段数：`396`

### 原始字段

`code`, `trade_time`, `open`, `high`, `low`, `close`, `volume`, `amount`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `ann_date` | 公告日期 | `financial_field` | `income_real` | `financial_announce_date` |
| `backward_adj_factor` | 后复权因子 | `adjustment_factor` | `backward_factor_real` | `-` |
| `basic_eps` | 基础每股收益 | `financial_field` | `income_real` | `-` |
| `block_trade_amount` | 大宗交易amount | `block_trading_field` | `block_trading_real` | `-` |
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `block_trade_avg_volume` | 大宗交易AVGvolume | `block_trading_field` | `block_trading_real` | `-` |
| `block_trade_buyer_name` | 大宗交易买方名称 | `block_trading_field` | `block_trading_real` | `-` |
| `block_trade_date` | 大宗交易日期 | `block_trading_field` | `block_trading_real` | `market_trade_date` |
| `block_trade_frequency` | 大宗交易frequency | `block_trading_field` | `block_trading_real` | `-` |
| `block_trade_price` | 大宗交易price | `block_trading_field` | `block_trading_real` | `-` |
| `block_trade_seller_name` | 大宗交易卖方名称 | `block_trading_field` | `block_trading_real` | `-` |
| `block_trade_volume` | 大宗交易volume | `block_trading_field` | `block_trading_real` | `-` |
| `bs_ann_date` | 资产负债表公告日期 | `financial_field` | `balance_sheet_real` | `financial_announce_date` |
| `bs_reporting_period` | 资产负债表报告期period | `financial_field` | `balance_sheet_real` | `-` |
| `calendar_trade_date` | 交易日历日期 | `calendar_field` | `trade_calendar_real` | `market_trade_date` |
| `cash_end_bal` | 现金ENDBAL | `financial_field` | `cash_flow_real` | `-` |
| `cf_ann_date` | 现金流公告日期 | `financial_field` | `cash_flow_real` | `financial_announce_date` |
| `cf_reporting_period` | 现金流报告期period | `financial_field` | `cash_flow_real` | `-` |
| `close_adj` | 基于原始收盘价与前复权因子计算的复权收盘价 | `derived_field` | `-` | `-` |
| `comp_name` | 公司名称 | `basic_info_field` | `stock_basic_real` | `-` |
| `comp_name_eng` | 公司名称英文 | `basic_info_field` | `stock_basic_real` | `-` |
| `delist_date` | delist日期 | `basic_info_field` | `stock_basic_real` | `listing_end_date` |
| `dividend_ann_date` | 分红公告日期 | `corporate_action_field` | `dividend_real` | `corporate_action_announce_date` |
| `dividend_cash_after_tax` | 分红现金afterTAX | `corporate_action_field` | `dividend_real` | `-` |
| `dividend_cash_pre_tax` | 分红现金前值TAX | `corporate_action_field` | `dividend_real` | `-` |
| `dividend_payout_date` | 分红派付日期 | `corporate_action_field` | `dividend_real` | `event_date` |
| `dividend_progress` | 分红progress | `corporate_action_field` | `dividend_real` | `-` |
| `dividend_report_period` | 分红reportperiod | `corporate_action_field` | `dividend_real` | `-` |
| `dividend_stock_per_share` | 分红股票PER股本 | `corporate_action_field` | `dividend_real` | `-` |
| `equity_record_date` | 股权记录日期 | `corporate_action_field` | `dividend_real` | `event_date` |
| `etf_cash_component` | 现金差额 | `etf_cash_nav_field` | `etf_pcf_real` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_code` | ETF 代码 | `etf_overview_field` | `etf_pcf_real` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_component_share` | 成分证券数量 | `etf_constituent_position_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_creation_cash_substitute` | 申购现金替代金额 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_discount_ratio` | 赎回折价比例 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_etf_code` | ETF 代码 | `etf_constituent_identity_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_constituent_premium_ratio` | 申购溢价比例 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_redemption_cash_substitute` | 赎回现金替代金额 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_substitute_flag` | 替代标志 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_substitution_cash_amount` | 现金替代补款金额 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_trading_day` | PCF 交易日 | `etf_constituent_identity_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_underlying_security_id` | 成分证券 ID | `etf_constituent_position_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_underlying_symbol` | 成分证券代码 | `etf_constituent_identity_field` | `etf_pcf_constituent_real` | `-` |
| `etf_creation_flag` | 是否允许申购 | `etf_status_field` | `etf_pcf_real` | `-` |
| `etf_creation_limit` | 申购上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_creation_limit_per_user` | 单用户申购上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_creation_redemption_switch` | 申赎开关 | `etf_status_field` | `etf_pcf_real` | `-` |
| `etf_creation_redemption_unit` | 申赎单位 | `etf_structure_field` | `etf_pcf_real` | `-` |
| `etf_dividend_per_cu` | 每申赎单位分红 | `etf_cash_nav_field` | `etf_pcf_real` | `-` |
| `etf_estimate_cash_component` | 预估现金差额 | `etf_cash_nav_field` | `etf_pcf_real` | `-` |
| `etf_fund_management_company` | 基金管理人 | `etf_overview_field` | `etf_pcf_real` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_max_cash_ratio` | 最大现金替代比例 | `etf_cash_nav_field` | `etf_pcf_real` | `-` |
| `etf_nav` | 基金净值 | `etf_cash_nav_field` | `etf_pcf_real` | `-` |
| `etf_nav_per_cu` | 每申赎单位资产净值 | `etf_cash_nav_field` | `etf_pcf_real` | `-` |
| `etf_net_creation_limit` | 净申购上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_net_creation_limit_per_user` | 单用户净申购上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_redemption_limit` | 净赎回上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_net_redemption_limit_per_user` | 单用户净赎回上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_pre_trading_day` | 前一交易日 | `etf_overview_field` | `etf_pcf_real` | `-` |
| `etf_publish_flag` | 是否发布 PCF | `etf_status_field` | `etf_pcf_real` | `-` |
| `etf_record_num` | 现金替代记录数 | `etf_structure_field` | `etf_pcf_real` | `-` |
| `etf_redemption_flag` | 是否允许赎回 | `etf_status_field` | `etf_pcf_real` | `-` |
| `etf_redemption_limit` | 赎回上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_redemption_limit_per_user` | 单用户赎回上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_symbol` | ETF 名称 | `etf_overview_field` | `etf_pcf_real` | `-` |
| `etf_total_record_num` | 总记录数 | `etf_structure_field` | `etf_pcf_real` | `-` |
| `etf_trading_day` | PCF 交易日 | `etf_overview_field` | `etf_pcf_real` | `-` |
| `etf_underlying_security_id` | 标的证券 ID | `etf_underlying_ref_field` | `etf_pcf_real` | `-` |
| `etf_underlying_security_id_source` | 标的证券 ID 来源 | `etf_underlying_ref_field` | `etf_pcf_real` | `-` |
| `ex_dividend_date` | 除权除息分红日期 | `corporate_action_field` | `dividend_real` | `event_date` |
| `express_ann_date` | 快报公告日期 | `financial_field` | `profit_express_real` | `financial_announce_date` |
| `express_basic_eps` | 快报基础每股收益 | `financial_field` | `profit_express_real` | `-` |
| `express_net_profit` | 快报NET利润 | `financial_field` | `profit_express_real` | `-` |
| `express_oper_profit` | 快报经营利润 | `financial_field` | `profit_express_real` | `-` |
| `express_performance_summary` | 快报performance汇总 | `financial_field` | `profit_express_real` | `-` |
| `express_reporting_period` | 快报报告期period | `financial_field` | `profit_express_real` | `-` |
| `express_revenue` | 快报营收 | `financial_field` | `profit_express_real` | `-` |
| `express_roe_weighted` | 快报ROEweighted | `financial_field` | `profit_express_real` | `-` |
| `express_total_assets` | 快报总资产 | `financial_field` | `profit_express_real` | `-` |
| `express_total_profit` | 快报总利润 | `financial_field` | `profit_express_real` | `-` |
| `float_a_share` | 来自股本结构表 | `equity_structure_field` | `equity_structure_real` | `-` |
| `float_market_cap` | 派生流通市值 | `derived_field` | `-` | `-` |
| `float_share` | 来自股本结构表 | `equity_structure_field` | `equity_structure_real` | `-` |
| `forward_adj_factor` | 前复权因子 | `adjustment_factor` | `adj_factor_real` | `-` |
| `free_cash_flow` | free现金流向 | `financial_field` | `cash_flow_real` | `-` |
| `gap_open_return` | 基于开盘价与前收盘价计算的跳空收益率 | `derived_field` | `-` | `-` |
| `high_adj` | 基于原始最高价与前复权因子计算的复权最高价 | `derived_field` | `-` | `-` |
| `high_limited` | 来自状态快照表 | `status_snapshot_field` | `history_stock_status_real` | `-` |
| `hist_security_type` | 历史证券类型快照 | `historical_code_field` | `hist_code_daily_real` | `-` |
| `holder_enddate` | 持有人enddate | `holder_field` | `holder_num_real` | `-` |
| `holder_num` | 持有人数量 | `holder_field` | `holder_num_real` | `-` |
| `holder_num_ann_date` | 持有人数量公告日期 | `holder_field` | `holder_num_real` | `holder_announce_date` |
| `holder_total_num` | 持有人总数量 | `holder_field` | `holder_num_real` | `-` |
| `index_code` | 日度指数成分权重映射 | `index_weight_field` | `index_weight_real` | `-` |
| `index_component_close` | 指数权重表中的成分收盘价 | `index_weight_snapshot_field` | `index_weight_real` | `-` |
| `index_constituent_code` | 指数成分代码 | `index_constituent_field` | `index_constituent_real` | `-` |
| `index_constituent_name` | 指数成分路径名称 | `index_constituent_field` | `index_constituent_real` | `-` |
| `index_name` | 指数成分映射名称 | `index_constituent_field` | `index_constituent_real` | `-` |
| `index_weight` | 日度指数成分权重 | `index_weight_field` | `index_weight_real` | `-` |
| `index_weight_code` | 指数权重代码 | `index_weight_field` | `index_weight_real` | `-` |
| `index_weight_name` | 指数权重路径名称 | `index_constituent_field` | `index_constituent_real` | `-` |
| `industry_code` | 行业代码 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_constituent_con_code` | 行业成分证券代码 | `industry_constituent_field` | `industry_constituent_real` | `-` |
| `industry_constituent_in_date` | 纳入日期 | `industry_constituent_field` | `industry_constituent_real` | `effective_start_date` |
| `industry_constituent_index_code` | 行业指数代码 | `industry_constituent_field` | `industry_constituent_real` | `-` |
| `industry_constituent_index_name` | 行业指数名称 | `industry_constituent_field` | `industry_constituent_real` | `-` |
| `industry_constituent_out_date` | 剔除日期 | `industry_constituent_field` | `industry_constituent_real` | `effective_end_date` |
| `industry_index_code` | 行业指数代码 | `industry_field` | `industry_weight_real` | `-` |
| `industry_level1_name` | 行业一级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_level2_name` | 行业二级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_level3_name` | 行业三级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_weight` | 行业权重 | `industry_field` | `industry_weight_real` | `-` |
| `is_listed` | 是否已上市 | `basic_info_field` | `stock_basic_real` | `-` |
| `is_st` | 来自状态快照表 | `status_flag` | `history_stock_status_real` | `-` |
| `is_suspended` | 来自状态快照表 | `status_flag` | `history_stock_status_real` | `-` |
| `kzz_ann_dt` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_bond_share` | 债券余额 | `kzz_balance_field` | `kzz_share_real` | `-` |
| `kzz_call_amount` | 赎回数量 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_announcement_date` | 赎回公告日期 | `kzz_call_field` | `kzz_call_explanation_real` | `kzz_announce_date` |
| `kzz_call_begin_date` | 可转债赎回begin日期 | `kzz_call_clause_field` | `kzz_call_real` | `effective_start_date` |
| `kzz_call_clause_end_date` | 可转债赎回条款END日期 | `kzz_call_clause_field` | `kzz_call_real` | `effective_end_date` |
| `kzz_call_clause_start_date` | 可转债赎回条款start日期 | `kzz_call_clause_field` | `kzz_call_real` | `effective_start_date` |
| `kzz_call_clause_trigger_ratio` | 可转债赎回条款triggerratio | `kzz_call_clause_field` | `kzz_call_real` | `-` |
| `kzz_call_date` | 可转债赎回日期 | `kzz_call_field` | `kzz_call_explanation_real` | `event_date` |
| `kzz_call_date_pub` | 赎回实施日期 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_end_date` | 可转债赎回END日期 | `kzz_call_clause_field` | `kzz_call_real` | `effective_end_date` |
| `kzz_call_event_date` | 可转债赎回事发日期 | `kzz_call_field` | `kzz_call_explanation_real` | `event_date` |
| `kzz_call_event_price` | 可转债赎回事发价格 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_explanation_announce_date` | 可转债赎回说明announce日期 | `kzz_call_field` | `kzz_call_explanation_real` | `kzz_announce_date` |
| `kzz_call_ful_res_ann_date` | 赎回结果公告日期 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_fund_arrival_date` | 赎回到账日期 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_outstanding_amount` | 赎回后余额 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_price` | 可转债赎回价格 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_call_reason` | 赎回原因 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_record_day` | 赎回登记日 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_trigger_ratio` | 可转债赎回triggerratio | `kzz_call_clause_field` | `kzz_call_real` | `-` |
| `kzz_clause_call_item` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_com_int_desc` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_com_int_rate` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_conv_adj_item` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_conv_period_item` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_ini_conv_premium_ratio` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_price` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_init_conv_price_item` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_is_com_int` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_is_int_cha_de_po_rate` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_orig_ration_arr_item` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_put_item` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_spec_down_adj` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_conv_ann_date` | 转股价调整公告日 | `kzz_conversion_field` | `kzz_conv_change_real` | `kzz_announce_date` |
| `kzz_conv_change_announce_date` | 可转债转股变动announce日期 | `kzz_conversion_field` | `kzz_conv_change_real` | `kzz_announce_date` |
| `kzz_conv_change_date` | 转股价调整生效日 | `kzz_conversion_field` | `kzz_conv_change_real` | `-` |
| `kzz_conv_change_price` | 可转债转股变动price | `kzz_conversion_field` | `kzz_conv_change_real` | `-` |
| `kzz_conv_change_reason` | 转股价调整原因 | `kzz_conversion_field` | `kzz_conv_change_real` | `-` |
| `kzz_conv_code` | 可转债转股代码 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_currency_code` | 可转债转股currency代码 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_end_date` | 可转债转股END日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `effective_end_date` |
| `kzz_conv_name` | 可转债转股名称 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_price` | 转股价格 | `kzz_conversion_field` | `kzz_conv_change_real` | `-` |
| `kzz_conv_rule_ann_date` | 可转债转股规则公告日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `kzz_announce_date` |
| `kzz_conv_rule_announce_date` | 可转债转股规则announce日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_rule_end_date` | 可转债转股规则END日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_rule_price` | 可转债转股规则价格 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_rule_start_date` | 可转债转股规则start日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_share` | 累计转股数量 | `kzz_conversion_field` | `kzz_share_real` | `-` |
| `kzz_conv_start_date` | 可转债转股start日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `effective_start_date` |
| `kzz_conv_trade_date_last` | 可转债转股交易日期最新价 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_corr_conv_price_floor_desc` | 下修价格下限描述 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_end_date` | 转股价下修结束日 | `kzz_correction_clause_field` | `kzz_corr_real` | `effective_end_date` |
| `kzz_corr_is_spec_down_corr_clause_flag` | 是否特殊下修条款 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_is_timepoint_corr_clause_flag` | 是否时间点下修条款 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_ref_price_is_avg_price` | 参考价是否均价 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_spec_corr_range` | 特殊下修区间 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_spec_corr_trig_ratio` | 触发比例 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_start_date` | 转股价下修起始日 | `kzz_correction_clause_field` | `kzz_corr_real` | `effective_start_date` |
| `kzz_corr_timepoint_corr_text_clause` | 时间点条款描述 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_timepoint_count` | 时间点数量 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_times_limit` | 下修次数限制 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_trig_calc_max_period` | 触发计算最大周期 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_trig_calc_period` | 触发计算周期 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_coupon_rate` | 可转债发行字段 | `kzz_coupon_field` | `kzz_issuance_real` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_txt` | 可转债发行字段 | `kzz_coupon_field` | `kzz_issuance_real` | `-` |
| `kzz_crncy_code` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `-` |
| `kzz_forced_conv_date` | 可转债强赎转股日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `event_date` |
| `kzz_forced_conv_price` | 可转债强赎转股price | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_forced_conv_reason` | 可转债强赎转股原因 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_interest_fre_quency` | 可转债发行字段 | `kzz_coupon_field` | `kzz_issuance_real` | `-` |
| `kzz_interest_type` | 可转债发行字段 | `kzz_coupon_field` | `kzz_issuance_real` | `-` |
| `kzz_is_conv_bonds` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_is_forced` | 可转债是否强赎 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_is_separation` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_item_add_put_con` | 可转债条款项ADD回售CON | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_add_put_price_ins` | 可转债条款项ADD回售priceINS | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_call_pro_period` | 可转债条款项赎回PROperiod | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_call_tri_con_ins` | 可转债条款项赎回TRICONINS | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_call_tri_per` | 可转债条款项赎回TRIPER | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_con_call_end_date` | 可转债条款项CON赎回END日期 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `effective_end_date` |
| `kzz_item_con_call_start_date` | 可转债条款项CON赎回start日期 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `effective_start_date` |
| `kzz_item_con_put_end_date` | 可转债条款项CON回售END日期 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `effective_end_date` |
| `kzz_item_con_put_start_date` | 可转债条款项CON回售start日期 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `effective_start_date` |
| `kzz_item_expired_redemption_price` | 可转债条款项expired赎回price | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_is_call_item` | 可转债条款项是否赎回条款项 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_is_put_item` | 可转债条款项是否回售条款项 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_mand_put_end_date` | 可转债条款项mand回售END日期 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `effective_end_date` |
| `kzz_item_mand_put_period` | 可转债条款项mand回售period | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_mand_put_price` | 可转债条款项mand回售price | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_mand_put_start_date` | 可转债条款项mand回售start日期 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `effective_start_date` |
| `kzz_item_mand_put_text` | 可转债条款项mand回售text | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_put_num_ins` | 可转债条款项回售数量INS | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_time_call_text` | 可转债条款项time赎回text | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_list_ann_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_list_code_onl` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_date_inst_off` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_eff_pc_h_of` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_effect_pc_hvol_off` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_excess_pch_onl` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_fee` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_household` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_issue_quantity` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_issue_size` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_issue_vol_onl` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_onl_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_pass_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_pch_name_onl` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_pch_price_onl` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_pchase_code_onl` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_permit_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_code` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_name` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_paymt_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_price` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_ratio_de` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_ratio_mo` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_reg_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_vol` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_result_ann_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_list_suc_rate_off` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_type` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_vol_inst_off` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_listed_ann_date` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_listed_date` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `listing_start_date` |
| `kzz_max_off_inst_subscr_qty` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_max_unline_public` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_min_off_inst_subscr_qty` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_min_unline_public` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_off_inst_dep_ratio` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_off_subscr_unit_inc_desc` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_plan_schedule` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `-` |
| `kzz_pre_plan_date` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_pre_ration_vol` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_put_amount` | 回售数量 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_announcement_date` | 回售公告日期 | `kzz_put_field` | `kzz_put_explanation_real` | `kzz_announce_date` |
| `kzz_put_begin_date` | 可转债回售begin日期 | `kzz_put_clause_field` | `kzz_put_real` | `effective_start_date` |
| `kzz_put_clause_end_date` | 可转债回售条款END日期 | `kzz_put_clause_field` | `kzz_put_real` | `effective_end_date` |
| `kzz_put_clause_start_date` | 可转债回售条款start日期 | `kzz_put_clause_field` | `kzz_put_real` | `effective_start_date` |
| `kzz_put_clause_trigger_ratio` | 可转债回售条款triggerratio | `kzz_put_clause_field` | `kzz_put_real` | `-` |
| `kzz_put_end_date` | 可转债回售END日期 | `kzz_put_clause_field` | `kzz_put_real` | `effective_end_date` |
| `kzz_put_event_price` | 可转债回售价 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_ex_date` | 回售除权日 | `kzz_put_field` | `kzz_put_explanation_real` | `event_date` |
| `kzz_put_explanation_announce_date` | 可转债回售说明announce日期 | `kzz_put_field` | `kzz_put_explanation_real` | `kzz_announce_date` |
| `kzz_put_fund_arrival_date` | 回售到账日 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_fund_end_date` | 资金截止日 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_item_call_condition_text` | 可转债回售条款项赎回conditiontext | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_put_item_call_trigger_period` | 可转债回售条款项赎回triggerperiod | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_put_outstanding` | 回售后余额 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_price` | 回售价格 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `kzz_put_repurchase_code` | 回售代码 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_repurchase_end_date` | 回售申报结束日 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_repurchase_start_date` | 回售申报开始日 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_resale_amount` | 再售数量 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_resale_end_date` | 再售结束日 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_resale_imp_amount` | 实施再售数量 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_resale_start_date` | 再售开始日 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_trigger_ratio` | 可转债回售triggerratio | `kzz_put_clause_field` | `kzz_put_real` | `-` |
| `kzz_ratio_annce_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_ratio_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_recommender` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `-` |
| `kzz_rel_conv_month` | 可转债REL转股month | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_result_ef_subscr_p_off` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_result_suc_rate_off` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_result_suc_rate_on` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_result_suc_rate_on2` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_resump_date` | 复牌日期 | `kzz_suspend_field` | `kzz_suspend_real` | `event_date` |
| `kzz_resump_time` | 复牌时间段 | `kzz_suspend_field` | `kzz_suspend_real` | `-` |
| `kzz_share_ann_date` | 可转债余额变动公告日 | `kzz_balance_field` | `kzz_share_real` | `-` |
| `kzz_share_change_date` | 可转债余额变动生效日 | `kzz_balance_field` | `kzz_share_real` | `event_date` |
| `kzz_share_change_reason` | 余额变动原因 | `kzz_balance_field` | `kzz_share_real` | `-` |
| `kzz_smtg_ann_date` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_stock_code` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `-` |
| `kzz_suspend_date` | 停牌日期 | `kzz_suspend_field` | `kzz_suspend_real` | `event_date` |
| `kzz_suspend_reason` | 停牌原因 | `kzz_suspend_field` | `kzz_suspend_real` | `-` |
| `kzz_suspend_reason_code` | 停牌原因代码 | `kzz_suspend_field` | `kzz_suspend_real` | `-` |
| `kzz_suspend_type` | 停牌类型 | `kzz_suspend_field` | `kzz_suspend_real` | `-` |
| `kzz_term_year` | 可转债发行字段 | `kzz_coupon_field` | `kzz_issuance_real` | `-` |
| `list_date` | 上市日期 | `basic_info_field` | `stock_basic_real` | `listing_start_date` |
| `listplate_name` | listplate名称 | `basic_info_field` | `stock_basic_real` | `-` |
| `long_hu_buy_amount` | 龙虎榜买入金额 | `long_hu_amount_field` | `long_hu_bang_real` | `-` |
| `long_hu_change_range` | 龙虎榜涨跌幅 | `long_hu_reason_field` | `long_hu_bang_real` | `-` |
| `long_hu_flow_mark` | 龙虎榜流向标识 | `long_hu_trader_field` | `long_hu_bang_real` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `long_hu_reason_type` | 龙虎榜原因类型 | `long_hu_reason_field` | `long_hu_bang_real` | `-` |
| `long_hu_reason_type_name` | 龙虎榜原因名称 | `long_hu_reason_field` | `long_hu_bang_real` | `-` |
| `long_hu_security_name` | 龙虎榜证券名称 | `long_hu_identity_field` | `long_hu_bang_real` | `-` |
| `long_hu_sell_amount` | 龙虎榜卖出金额 | `long_hu_amount_field` | `long_hu_bang_real` | `-` |
| `long_hu_total_amount` | 龙虎榜成交额 | `long_hu_amount_field` | `long_hu_bang_real` | `-` |
| `long_hu_total_volume` | 龙虎榜成交量 | `long_hu_amount_field` | `long_hu_bang_real` | `-` |
| `long_hu_trade_date` | 龙虎榜交易日 | `long_hu_identity_field` | `long_hu_bang_real` | `market_trade_date` |
| `long_hu_trader_name` | 龙虎榜营业部名称 | `long_hu_trader_field` | `long_hu_bang_real` | `-` |
| `low_adj` | 基于原始最低价与前复权因子计算的复权最低价 | `derived_field` | `-` | `-` |
| `low_limited` | 来自状态快照表 | `status_snapshot_field` | `history_stock_status_real` | `-` |
| `margin_borrow_money_balance` | 融资余额 | `margin_detail_balance_field` | `margin_detail_real` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_purch_with_borrow_money` | 融资买入额 | `margin_detail_flow_field` | `margin_detail_real` | `-` |
| `margin_repayment_of_borrow_money` | 融资偿还额 | `margin_detail_flow_field` | `margin_detail_real` | `-` |
| `margin_repayment_of_borrow_sec` | 融券偿还量 | `margin_detail_flow_field` | `margin_detail_real` | `-` |
| `margin_sales_of_borrowed_sec` | 融券卖出量 | `margin_detail_flow_field` | `margin_detail_real` | `-` |
| `margin_sec_lending_balance` | 融券余额 | `margin_detail_balance_field` | `margin_detail_real` | `-` |
| `margin_sec_lending_balance_vol` | 融券余量 | `margin_detail_balance_field` | `margin_detail_real` | `-` |
| `margin_security_name` | 融资融券证券名称 | `margin_detail_identity_field` | `margin_detail_real` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `margin_trade_balance` | 融资融券余额 | `margin_detail_balance_field` | `margin_detail_real` | `-` |
| `margin_trade_date` | 融资融券交易日 | `margin_detail_identity_field` | `margin_detail_real` | `market_trade_date` |
| `market_cap` | 派生总市值 | `derived_field` | `-` | `-` |
| `net_cash_flows_fin_act` | NET现金流量融资ACT | `financial_field` | `cash_flow_real` | `-` |
| `net_cash_flows_inv_act` | NET现金流量投资ACT | `financial_field` | `cash_flow_real` | `-` |
| `net_cash_flows_oper_act` | NET现金流量经营ACT | `financial_field` | `cash_flow_real` | `-` |
| `net_incr_cash_and_cash_equ` | NET增加现金AND现金等价物 | `financial_field` | `cash_flow_real` | `-` |
| `notice_ann_date` | 预告公告日期 | `financial_field` | `profit_notice_real` | `financial_announce_date` |
| `notice_reporting_period` | 预告报告期period | `financial_field` | `profit_notice_real` | `-` |
| `open_adj` | 基于原始开盘价与前复权因子计算的复权开盘价 | `derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `pinyin` | 字段 pinyin | `basic_info_field` | `stock_basic_real` | `-` |
| `pledge_ann_date` | 质押公告日期 | `equity_event_field` | `equity_pledge_freeze_real` | `corporate_action_announce_date` |
| `pledge_begin_date` | 质押begin日期 | `equity_event_field` | `equity_pledge_freeze_real` | `effective_start_date` |
| `pledge_end_date` | 质押END日期 | `equity_event_field` | `equity_pledge_freeze_real` | `effective_end_date` |
| `pledge_freeze_type` | 质押冻结type | `equity_event_field` | `equity_pledge_freeze_real` | `-` |
| `pledge_fro_shares` | 质押FROshares | `equity_event_field` | `equity_pledge_freeze_real` | `-` |
| `pledge_frozen_institution` | 质押frozeninstitution | `equity_event_field` | `equity_pledge_freeze_real` | `-` |
| `pledge_holder_name` | 质押持有人名称 | `equity_event_field` | `equity_pledge_freeze_real` | `-` |
| `pledge_is_disfrozen` | 质押是否disfrozen | `equity_event_field` | `equity_pledge_freeze_real` | `-` |
| `pledge_total_holding_shr` | 质押总holdingSHR | `equity_event_field` | `equity_pledge_freeze_real` | `-` |
| `pledge_total_pledge_shr` | 质押总质押SHR | `equity_event_field` | `equity_pledge_freeze_real` | `-` |
| `pre_close` | 来自状态快照表 | `status_snapshot_field` | `history_stock_status_real` | `-` |
| `price_tick` | 字段 price_tick | `code_info_field` | `code_info_real` | `-` |
| `profit_notice_change_max` | 利润预告变动上限 | `financial_field` | `profit_notice_real` | `-` |
| `profit_notice_change_min` | 利润预告变动下限 | `financial_field` | `profit_notice_real` | `-` |
| `profit_notice_net_profit_max` | 利润预告NET利润上限 | `financial_field` | `profit_notice_real` | `-` |
| `profit_notice_net_profit_min` | 利润预告NET利润下限 | `financial_field` | `profit_notice_real` | `-` |
| `profit_notice_summary` | 利润预告汇总 | `financial_field` | `profit_notice_real` | `-` |
| `reporting_period` | 报告期period | `financial_field` | `income_real` | `-` |
| `restricted_event_close_price` | 限售事件收盘价price | `equity_event_field` | `equity_restricted_real` | `-` |
| `restricted_list_date` | 限售上市日期 | `equity_event_field` | `equity_restricted_real` | `event_date` |
| `restricted_share_is_ann` | 限售股本是否公告 | `equity_event_field` | `equity_restricted_real` | `-` |
| `restricted_share_lst` | 限售股本LST | `equity_event_field` | `equity_restricted_real` | `-` |
| `restricted_share_market_value` | 限售股本市场数值 | `equity_event_field` | `equity_restricted_real` | `-` |
| `restricted_share_ratio` | 限售股本ratio | `equity_event_field` | `equity_restricted_real` | `-` |
| `restricted_share_type_name` | 限售股本type名称 | `equity_event_field` | `equity_restricted_real` | `-` |
| `revenue` | 营收 | `financial_field` | `income_real` | `-` |
| `right_issue_amt_plan` | 配股发行AMTplan | `corporate_action_field` | `right_issue_real` | `-` |
| `right_issue_ann_date` | 配股发行公告日期 | `corporate_action_field` | `right_issue_real` | `corporate_action_announce_date` |
| `right_issue_collection_fund` | 配股发行collection基金 | `corporate_action_field` | `right_issue_real` | `-` |
| `right_issue_ex_dividend_date` | 配股发行除权除息分红日期 | `corporate_action_field` | `right_issue_real` | `-` |
| `right_issue_expected_fund_raising` | 配股发行expected基金raising | `corporate_action_field` | `right_issue_real` | `-` |
| `right_issue_price` | 配股发行price | `corporate_action_field` | `right_issue_real` | `-` |
| `right_issue_progress` | 配股发行progress | `corporate_action_field` | `right_issue_real` | `-` |
| `right_issue_ratio` | 配股发行ratio | `corporate_action_field` | `right_issue_real` | `-` |
| `security_name` | 证券名称 | `basic_info_field` | `stock_basic_real` | `-` |
| `security_status_raw` | 证券状态原始 | `code_info_field` | `code_info_real` | `-` |
| `security_type` | 证券type | `code_info_field` | `code_info_real` | `-` |
| `share_holder_ann_date` | 股本持有人公告日期 | `share_holder_field` | `share_holder_real` | `holder_announce_date` |
| `share_holder_enddate` | 股本持有人enddate | `share_holder_field` | `share_holder_real` | `-` |
| `share_holder_float_qty` | 股本持有人流通QTY | `share_holder_field` | `share_holder_real` | `-` |
| `share_holder_name` | 股本持有人名称 | `share_holder_field` | `share_holder_real` | `-` |
| `share_holder_pct` | 股本持有人PCT | `share_holder_field` | `share_holder_real` | `-` |
| `share_holder_quantity` | 股本持有人quantity | `share_holder_field` | `share_holder_real` | `-` |
| `symbol` | 简称 | `code_info_field` | `code_info_real` | `-` |
| `tot_share` | 来自股本结构表 | `equity_structure_field` | `equity_structure_real` | `-` |
| `total_assets` | 总资产 | `financial_field` | `balance_sheet_real` | `-` |
| `total_equity_ex_min_int` | 总股权除权除息下限INT | `financial_field` | `balance_sheet_real` | `-` |
| `total_liab` | 总负债 | `financial_field` | `balance_sheet_real` | `-` |
| `turnover_rate` | 按日计算的换手率 | `derived_field` | `-` | `-` |

## `stock_minute_real`

- 中文说明：股票分钟行情主表
- 表：`starlight.ad_market_kline_minute`
- 粒度：`minute`
- 节点角色：`query_entry`
- AI 主入口：`True`
- 自身原始字段数：`8`
- 可用标准字段数：`33`

### 原始字段

`code`, `trade_time`, `open`, `high`, `low`, `close`, `volume`, `amount`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `close_adj` | 基于原始收盘价与前复权因子计算的复权收盘价 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `float_market_cap` | 派生流通市值 | `derived_field` | `-` | `-` |
| `gap_open_return` | 基于开盘价与前收盘价计算的跳空收益率 | `derived_field` | `-` | `-` |
| `high_adj` | 基于原始最高价与前复权因子计算的复权最高价 | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `low_adj` | 基于原始最低价与前复权因子计算的复权最低价 | `derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `market_cap` | 派生总市值 | `derived_field` | `-` | `-` |
| `minute_amount` | 分钟amount | `raw_market_field` | `stock_minute_real` | `-` |
| `minute_close` | 分钟收盘价 | `raw_market_field` | `stock_minute_real` | `-` |
| `minute_high` | 分钟最高价 | `raw_market_field` | `stock_minute_real` | `-` |
| `minute_low` | 分钟最低价 | `raw_market_field` | `stock_minute_real` | `-` |
| `minute_open` | 分钟开盘价 | `raw_market_field` | `stock_minute_real` | `-` |
| `minute_volume` | 分钟volume | `raw_market_field` | `stock_minute_real` | `-` |
| `open_adj` | 基于原始开盘价与前复权因子计算的复权开盘价 | `derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `turnover_rate` | 按日计算的换手率 | `derived_field` | `-` | `-` |

## `adj_factor_real`

- 中文说明：前复权因子表
- 表：`starlight.ad_adj_factor`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`1`
- 可用标准字段数：`20`

### 原始字段

`factor_value`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `forward_adj_factor` | 前复权因子 | `adjustment_factor` | `adj_factor_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `backward_factor_real`

- 中文说明：后复权因子表
- 表：`starlight.ad_backward_factor`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`1`
- 可用标准字段数：`20`

### 原始字段

`factor_value`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `backward_adj_factor` | 后复权因子 | `adjustment_factor` | `backward_factor_real` | `-` |
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `history_stock_status_real`

- 中文说明：股票历史状态表
- 表：`starlight.ad_history_stock_status`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`7`
- 可用标准字段数：`24`

### 原始字段

`preclose`, `high_limited`, `low_limited`, `is_st_sec`, `is_susp_sec`, `is_wd_sec`, `is_xr_sec`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `high_limited` | 来自状态快照表 | `status_snapshot_field` | `history_stock_status_real` | `-` |
| `is_st` | 来自状态快照表 | `status_flag` | `history_stock_status_real` | `-` |
| `is_suspended` | 来自状态快照表 | `status_flag` | `history_stock_status_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `low_limited` | 来自状态快照表 | `status_snapshot_field` | `history_stock_status_real` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `pre_close` | 来自状态快照表 | `status_snapshot_field` | `history_stock_status_real` | `-` |

## `equity_structure_real`

- 中文说明：股本结构表
- 表：`starlight.ad_equity_structure`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`3`
- 可用标准字段数：`22`

### 原始字段

`tot_share`, `float_share`, `float_a_share`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `float_a_share` | 来自股本结构表 | `equity_structure_field` | `equity_structure_real` | `-` |
| `float_share` | 来自股本结构表 | `equity_structure_field` | `equity_structure_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `tot_share` | 来自股本结构表 | `equity_structure_field` | `equity_structure_real` | `-` |

## `industry_weight_real`

- 中文说明：行业权重映射表
- 表：`starlight.ad_industry_weight`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`2`
- 可用标准字段数：`25`

### 原始字段

`index_code`, `weight`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `industry_code` | 行业代码 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_index_code` | 行业指数代码 | `industry_field` | `industry_weight_real` | `-` |
| `industry_level1_name` | 行业一级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_level2_name` | 行业二级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_level3_name` | 行业三级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_weight` | 行业权重 | `industry_field` | `industry_weight_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `industry_base_info_real`

- 中文说明：行业基础信息表
- 表：`starlight.ad_industry_base_info`
- 粒度：`daily`
- 节点角色：`reference`
- AI 主入口：`False`
- 自身原始字段数：`5`
- 可用标准字段数：`23`

### 原始字段

`index_code`, `industry_code`, `level1_name`, `level2_name`, `level3_name`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `industry_code` | 行业代码 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_level1_name` | 行业一级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_level2_name` | 行业二级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_level3_name` | 行业三级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `industry_daily_real`

- 中文说明：行业日线行情表
- 表：`starlight.ad_industry_daily`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`True`
- 自身原始字段数：`13`
- 可用标准字段数：`42`

### 原始字段

`index_code`, `trade_date`, `open`, `high`, `close`, `low`, `amount`, `volume`, `pb`, `pe`, `total_cap`, `a_float_cap`, `pre_close`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `close_adj` | 基于原始收盘价与前复权因子计算的复权收盘价 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `float_market_cap` | 派生流通市值 | `derived_field` | `-` | `-` |
| `gap_open_return` | 基于开盘价与前收盘价计算的跳空收益率 | `derived_field` | `-` | `-` |
| `high_adj` | 基于原始最高价与前复权因子计算的复权最高价 | `derived_field` | `-` | `-` |
| `industry_a_float_cap` | 行业A流通市值 | `industry_daily_field` | `industry_daily_real` | `-` |
| `industry_code` | 行业代码 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_daily_amount` | 行业日频amount | `industry_daily_field` | `industry_daily_real` | `-` |
| `industry_daily_close` | 行业日频收盘价 | `industry_daily_field` | `industry_daily_real` | `-` |
| `industry_daily_high` | 行业日频最高价 | `industry_daily_field` | `industry_daily_real` | `-` |
| `industry_daily_low` | 行业日频最低价 | `industry_daily_field` | `industry_daily_real` | `-` |
| `industry_daily_open` | 行业日频开盘价 | `industry_daily_field` | `industry_daily_real` | `-` |
| `industry_daily_volume` | 行业日频volume | `industry_daily_field` | `industry_daily_real` | `-` |
| `industry_level1_name` | 行业一级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_level2_name` | 行业二级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_level3_name` | 行业三级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_pb` | 行业市净率 | `industry_daily_field` | `industry_daily_real` | `-` |
| `industry_pe` | 行业市盈率 | `industry_daily_field` | `industry_daily_real` | `-` |
| `industry_pre_close` | 行业前值收盘价 | `industry_daily_field` | `industry_daily_real` | `-` |
| `industry_total_cap` | 行业总市值 | `industry_daily_field` | `industry_daily_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `low_adj` | 基于原始最低价与前复权因子计算的复权最低价 | `derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `market_cap` | 派生总市值 | `derived_field` | `-` | `-` |
| `open_adj` | 基于原始开盘价与前复权因子计算的复权开盘价 | `derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `turnover_rate` | 按日计算的换手率 | `derived_field` | `-` | `-` |

## `index_weight_real`

- 中文说明：指数成分权重表
- 表：`starlight.ad_index_weight`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`3`
- 可用标准字段数：`26`

### 原始字段

`index_code`, `weight`, `close`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `close_adj` | 基于原始收盘价与前复权因子计算的复权收盘价 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `float_market_cap` | 派生流通市值 | `derived_field` | `-` | `-` |
| `index_code` | 日度指数成分权重映射 | `index_weight_field` | `index_weight_real` | `-` |
| `index_component_close` | 指数权重表中的成分收盘价 | `index_weight_snapshot_field` | `index_weight_real` | `-` |
| `index_weight` | 日度指数成分权重 | `index_weight_field` | `index_weight_real` | `-` |
| `index_weight_code` | 指数权重代码 | `index_weight_field` | `index_weight_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `market_cap` | 派生总市值 | `derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `index_constituent_real`

- 中文说明：指数成分关系表
- 表：`starlight.ad_index_constituent`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`4`
- 可用标准字段数：`23`

### 原始字段

`index_code`, `index_name`, `in_date`, `out_date`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `index_constituent_code` | 指数成分代码 | `index_constituent_field` | `index_constituent_real` | `-` |
| `index_constituent_name` | 指数成分路径名称 | `index_constituent_field` | `index_constituent_real` | `-` |
| `index_name` | 指数成分映射名称 | `index_constituent_field` | `index_constituent_real` | `-` |
| `index_weight_name` | 指数权重路径名称 | `index_constituent_field` | `index_constituent_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `income_real`

- 中文说明：利润表
- 表：`starlight.ad_income`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`4`
- 可用标准字段数：`23`

### 原始字段

`reporting_period`, `ann_date`, `opera_rev`, `basic_eps`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `ann_date` | 公告日期 | `financial_field` | `income_real` | `financial_announce_date` |
| `basic_eps` | 基础每股收益 | `financial_field` | `income_real` | `-` |
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `reporting_period` | 报告期period | `financial_field` | `income_real` | `-` |
| `revenue` | 营收 | `financial_field` | `income_real` | `-` |

## `balance_sheet_real`

- 中文说明：资产负债表
- 表：`starlight.ad_balance_sheet`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`5`
- 可用标准字段数：`24`

### 原始字段

`reporting_period`, `ann_date`, `total_assets`, `total_liab`, `tot_share_equity_excl_min_int`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `bs_ann_date` | 资产负债表公告日期 | `financial_field` | `balance_sheet_real` | `financial_announce_date` |
| `bs_reporting_period` | 资产负债表报告期period | `financial_field` | `balance_sheet_real` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `total_assets` | 总资产 | `financial_field` | `balance_sheet_real` | `-` |
| `total_equity_ex_min_int` | 总股权除权除息下限INT | `financial_field` | `balance_sheet_real` | `-` |
| `total_liab` | 总负债 | `financial_field` | `balance_sheet_real` | `-` |

## `cash_flow_real`

- 中文说明：现金流量表
- 表：`starlight.ad_cash_flow`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`8`
- 可用标准字段数：`27`

### 原始字段

`reporting_period`, `ann_date`, `net_cash_flows_opera_act`, `net_cash_flows_inv_act`, `net_cash_flows_fin_act`, `net_incr_cash_and_cash_equ`, `free_cash_flow`, `cash_end_bal`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `cash_end_bal` | 现金ENDBAL | `financial_field` | `cash_flow_real` | `-` |
| `cf_ann_date` | 现金流公告日期 | `financial_field` | `cash_flow_real` | `financial_announce_date` |
| `cf_reporting_period` | 现金流报告期period | `financial_field` | `cash_flow_real` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `free_cash_flow` | free现金流向 | `financial_field` | `cash_flow_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `net_cash_flows_fin_act` | NET现金流量融资ACT | `financial_field` | `cash_flow_real` | `-` |
| `net_cash_flows_inv_act` | NET现金流量投资ACT | `financial_field` | `cash_flow_real` | `-` |
| `net_cash_flows_oper_act` | NET现金流量经营ACT | `financial_field` | `cash_flow_real` | `-` |
| `net_incr_cash_and_cash_equ` | NET增加现金AND现金等价物 | `financial_field` | `cash_flow_real` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `profit_notice_real`

- 中文说明：业绩预告表
- 表：`starlight.ad_profit_notice`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`11`
- 可用标准字段数：`26`

### 原始字段

`reporting_period`, `ann_date`, `p_change_max`, `p_change_min`, `net_profit_max`, `net_profit_min`, `first_ann_date`, `p_reason`, `p_summary`, `p_net_parent_firm`, `report_type`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `notice_ann_date` | 预告公告日期 | `financial_field` | `profit_notice_real` | `financial_announce_date` |
| `notice_reporting_period` | 预告报告期period | `financial_field` | `profit_notice_real` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `profit_notice_change_max` | 利润预告变动上限 | `financial_field` | `profit_notice_real` | `-` |
| `profit_notice_change_min` | 利润预告变动下限 | `financial_field` | `profit_notice_real` | `-` |
| `profit_notice_net_profit_max` | 利润预告NET利润上限 | `financial_field` | `profit_notice_real` | `-` |
| `profit_notice_net_profit_min` | 利润预告NET利润下限 | `financial_field` | `profit_notice_real` | `-` |
| `profit_notice_summary` | 利润预告汇总 | `financial_field` | `profit_notice_real` | `-` |

## `profit_express_real`

- 中文说明：业绩快报表
- 表：`starlight.ad_profit_express`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`12`
- 可用标准字段数：`29`

### 原始字段

`reporting_period`, `ann_date`, `total_assets`, `net_pro_excl_min_int_inc`, `tot_opera_rev`, `total_profit`, `opera_profit`, `eps_basic`, `roe_weighted`, `performance_summary`, `yoy_gr_gross_rev`, `yoy_gr_net_profit_parent`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `express_ann_date` | 快报公告日期 | `financial_field` | `profit_express_real` | `financial_announce_date` |
| `express_basic_eps` | 快报基础每股收益 | `financial_field` | `profit_express_real` | `-` |
| `express_net_profit` | 快报NET利润 | `financial_field` | `profit_express_real` | `-` |
| `express_oper_profit` | 快报经营利润 | `financial_field` | `profit_express_real` | `-` |
| `express_performance_summary` | 快报performance汇总 | `financial_field` | `profit_express_real` | `-` |
| `express_reporting_period` | 快报报告期period | `financial_field` | `profit_express_real` | `-` |
| `express_revenue` | 快报营收 | `financial_field` | `profit_express_real` | `-` |
| `express_roe_weighted` | 快报ROEweighted | `financial_field` | `profit_express_real` | `-` |
| `express_total_assets` | 快报总资产 | `financial_field` | `profit_express_real` | `-` |
| `express_total_profit` | 快报总利润 | `financial_field` | `profit_express_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `stock_basic_real`

- 中文说明：股票基础信息快照表
- 表：`starlight.ad_stock_basic`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`9`
- 可用标准字段数：`27`

### 原始字段

`snapshot_date`, `security_name`, `comp_name`, `pinyin`, `comp_name_eng`, `list_date`, `delist_date`, `listplate_name`, `is_listed`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `comp_name` | 公司名称 | `basic_info_field` | `stock_basic_real` | `-` |
| `comp_name_eng` | 公司名称英文 | `basic_info_field` | `stock_basic_real` | `-` |
| `delist_date` | delist日期 | `basic_info_field` | `stock_basic_real` | `listing_end_date` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `is_listed` | 是否已上市 | `basic_info_field` | `stock_basic_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `list_date` | 上市日期 | `basic_info_field` | `stock_basic_real` | `listing_start_date` |
| `listplate_name` | listplate名称 | `basic_info_field` | `stock_basic_real` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `pinyin` | 字段 pinyin | `basic_info_field` | `stock_basic_real` | `-` |
| `security_name` | 证券名称 | `basic_info_field` | `stock_basic_real` | `-` |

## `stock_snapshot_real`

- 中文说明：股票盘口快照表
- 表：`starlight.ad_market_snapshot`
- 粒度：`minute`
- 节点角色：`query_entry`
- AI 主入口：`True`
- 自身原始字段数：`50`
- 可用标准字段数：`88`

### 原始字段

`trade_time`, `code`, `pre_close`, `last`, `open`, `high`, `low`, `close`, `volume`, `amount`, `num_trades`, `high_limited`, `low_limited`, `ask_price1`, `ask_price2`, `ask_price3`, `ask_price4`, `ask_price5`, `ask_volume1`, `ask_volume2`, `ask_volume3`, `ask_volume4`, `ask_volume5`, `bid_price1`, `bid_price2`, `bid_price3`, `bid_price4`, `bid_price5`, `bid_volume1`, `bid_volume2`, `bid_volume3`, `bid_volume4`, `bid_volume5`, `iopv`, `trading_phase_code`, `total_long_position`, `pre_settle`, `auction_price`, `auction_volume`, `settle`, `contract_type`, `expire_date`, `underlying_security_code`, `exercise_price`, `action_day`, `trading_day`, `pre_open_interest`, `open_interest`, `average_price`, `nominal_price`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `close_adj` | 基于原始收盘价与前复权因子计算的复权收盘价 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `float_market_cap` | 派生流通市值 | `derived_field` | `-` | `-` |
| `gap_open_return` | 基于开盘价与前收盘价计算的跳空收益率 | `derived_field` | `-` | `-` |
| `high_adj` | 基于原始最高价与前复权因子计算的复权最高价 | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `low_adj` | 基于原始最低价与前复权因子计算的复权最低价 | `derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `market_cap` | 派生总市值 | `derived_field` | `-` | `-` |
| `open_adj` | 基于原始开盘价与前复权因子计算的复权开盘价 | `derived_field` | `-` | `-` |
| `option_basic_contract_adjust_flag` | 期权基础信息字段 | `option_contract_pricing_field` | `option_basic_info_real` | `-` |
| `option_basic_contract_full_name` | 期权基础信息字段 | `option_contract_identity_field` | `option_basic_info_real` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_basic_contract_type` | 期权基础信息字段 | `option_contract_identity_field` | `option_basic_info_real` | `-` |
| `option_basic_contract_unit` | 期权基础信息字段 | `option_contract_pricing_field` | `option_basic_info_real` | `-` |
| `option_basic_delivery_date` | 期权基础信息字段 | `option_contract_lifecycle_field` | `option_basic_info_real` | `delivery_date` |
| `option_basic_delivery_month` | 期权基础信息字段 | `option_contract_identity_field` | `option_basic_info_real` | `-` |
| `option_basic_exchange_code` | 期权基础信息字段 | `option_contract_identity_field` | `option_basic_info_real` | `-` |
| `option_basic_exchange_short_name` | 期权基础信息字段 | `option_contract_identity_field` | `option_basic_info_real` | `-` |
| `option_basic_exercise_end_date` | 期权基础信息字段 | `option_contract_lifecycle_field` | `option_basic_info_real` | `exercise_end_date` |
| `option_basic_exercise_price` | 期权基础信息字段 | `option_contract_pricing_field` | `option_basic_info_real` | `-` |
| `option_basic_expiry_date` | 期权基础信息字段 | `option_contract_lifecycle_field` | `option_basic_info_real` | `expiry_date` |
| `option_basic_is_trade` | 期权基础信息字段 | `option_contract_lifecycle_field` | `option_basic_info_real` | `-` |
| `option_basic_last_trade_date` | 期权基础信息字段 | `option_contract_lifecycle_field` | `option_basic_info_real` | `last_trade_date` |
| `option_basic_listing_ref_price` | 期权基础信息字段 | `option_contract_pricing_field` | `option_basic_info_real` | `-` |
| `option_basic_start_trade_date` | 期权基础信息字段 | `option_contract_lifecycle_field` | `option_basic_info_real` | `listing_start_date` |
| `option_mon_change_date` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_change_reason` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_code_new` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_code_old` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_new` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_exercise_price_old` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_name_new` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_name_old` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_new` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_unit_old` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_std_contract_multiplier` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_contract_unit` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_contract_unit_dimension` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_contract_value` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_delist_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `listing_end_date` |
| `option_std_delivery_method` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_exchange_name` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_exercise_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `exercise_date` |
| `option_std_exercise_method` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_final_settlement_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `settlement_date` |
| `option_std_final_settlement_price` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_is_simulation` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_is_simulation_trade` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_last_trading_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `last_trade_date` |
| `option_std_listed_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `listing_start_date` |
| `option_std_market_code` | 期权STD市场代码 | `option_contract_identity_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_min_price_unit` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_notional_value` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_option_en_name` | 期权标准合约字段 | `option_contract_identity_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_option_name` | 期权标准合约字段 | `option_contract_identity_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_option_strike_price` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `option_std_option_type` | 期权标准合约字段 | `option_contract_identity_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_position_declare_min` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_position_limit` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_premium` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_quote_currency_unit` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_settlement_month` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_trading_fee` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_trading_hours_desc` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `snapshot_ask_price1` | 最优卖价 | `snapshot_field` | `stock_snapshot_real` | `-` |
| `snapshot_ask_volume1` | 最优卖量 | `snapshot_field` | `stock_snapshot_real` | `-` |
| `snapshot_bid_price1` | 最优买价 | `snapshot_field` | `stock_snapshot_real` | `-` |
| `snapshot_bid_volume1` | 最优买量 | `snapshot_field` | `stock_snapshot_real` | `-` |
| `snapshot_last` | 市场快照最新成交价 | `snapshot_field` | `stock_snapshot_real` | `-` |
| `snapshot_num_trades` | 市场快照成交笔数 | `snapshot_field` | `stock_snapshot_real` | `-` |
| `snapshot_trading_phase_code` | 交易阶段代码 | `snapshot_field` | `stock_snapshot_real` | `-` |
| `turnover_rate` | 按日计算的换手率 | `derived_field` | `-` | `-` |

## `hist_code_daily_real`

- 中文说明：历史证券代码日表
- 表：`starlight.ad_hist_code_daily`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`3`
- 可用标准字段数：`20`

### 原始字段

`trade_date`, `security_type`, `code`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `hist_security_type` | 历史证券类型快照 | `historical_code_field` | `hist_code_daily_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `treasury_yield_real`

- 中文说明：国债收益率表
- 表：`starlight.ad_treasury_yield`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`True`
- 自身原始字段数：`3`
- 可用标准字段数：`22`

### 原始字段

`term`, `trade_date`, `yield_value`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `treasury_term` | 国债收益率期限 | `macro_treasury_identity_field` | `treasury_yield_real` | `-` |
| `treasury_trade_date` | 国债收益率交易日 | `macro_treasury_identity_field` | `treasury_yield_real` | `market_trade_date` |
| `treasury_yield` | 国债收益率数值 | `macro_treasury_value_field` | `treasury_yield_real` | `-` |

## `fund_iopv_real`

- 中文说明：基金 IOPV 表
- 表：`starlight.ad_fund_iopv`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`True`
- 自身原始字段数：`3`
- 可用标准字段数：`21`

### 原始字段

`market_code`, `price_date`, `iopv_nav`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `fund_iopv` | 基金盘中参考净值 | `fund_iopv_field` | `fund_iopv_real` | `-` |
| `fund_iopv_date` | 基金IOPV日期 | `fund_iopv_identity_field` | `fund_iopv_real` | `market_trade_date` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `fund_share_real`

- 中文说明：基金份额表
- 表：`starlight.ad_fund_share`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`True`
- 自身原始字段数：`8`
- 可用标准字段数：`25`

### 原始字段

`market_code`, `fund_share`, `change_reason`, `is_consolidated_data`, `ann_date`, `total_share`, `change_date`, `float_share`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `fund_change_reason` | 基金份额变动原因 | `fund_share_event_field` | `fund_share_real` | `-` |
| `fund_float_share` | 基金流通份额 | `fund_share_balance_field` | `fund_share_real` | `-` |
| `fund_share` | 基金份额 | `fund_share_balance_field` | `fund_share_real` | `-` |
| `fund_share_ann_date` | 基金份额公告日 | `fund_share_event_field` | `fund_share_real` | `fund_announce_date` |
| `fund_share_change_date` | 基金份额变动日 | `fund_share_event_field` | `fund_share_real` | `-` |
| `fund_total_share` | 基金总份额 | `fund_share_balance_field` | `fund_share_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `code_info_real`

- 中文说明：证券代码信息表
- 表：`starlight.ad_code_info`
- 粒度：`daily`
- 节点角色：`reference`
- AI 主入口：`False`
- 自身原始字段数：`7`
- 可用标准字段数：`23`

### 原始字段

`security_type`, `symbol`, `security_status_raw`, `pre_close`, `high_limited`, `low_limited`, `price_tick`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `price_tick` | 字段 price_tick | `code_info_field` | `code_info_real` | `-` |
| `security_status_raw` | 证券状态原始 | `code_info_field` | `code_info_real` | `-` |
| `security_type` | 证券type | `code_info_field` | `code_info_real` | `-` |
| `symbol` | 简称 | `code_info_field` | `code_info_real` | `-` |

## `dividend_real`

- 中文说明：分红送配表
- 表：`starlight.ad_dividend`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`13`
- 可用标准字段数：`28`

### 原始字段

`ann_date`, `report_period`, `div_progress`, `dvd_per_share_stk`, `dvd_per_share_pre_tax_cash`, `dvd_per_share_after_tax_cash`, `date_eqy_record`, `date_ex`, `date_dvd_payout`, `div_bonusrate`, `div_conversedrate`, `remark`, `div_target`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `dividend_ann_date` | 分红公告日期 | `corporate_action_field` | `dividend_real` | `corporate_action_announce_date` |
| `dividend_cash_after_tax` | 分红现金afterTAX | `corporate_action_field` | `dividend_real` | `-` |
| `dividend_cash_pre_tax` | 分红现金前值TAX | `corporate_action_field` | `dividend_real` | `-` |
| `dividend_payout_date` | 分红派付日期 | `corporate_action_field` | `dividend_real` | `event_date` |
| `dividend_progress` | 分红progress | `corporate_action_field` | `dividend_real` | `-` |
| `dividend_report_period` | 分红reportperiod | `corporate_action_field` | `dividend_real` | `-` |
| `dividend_stock_per_share` | 分红股票PER股本 | `corporate_action_field` | `dividend_real` | `-` |
| `equity_record_date` | 股权记录日期 | `corporate_action_field` | `dividend_real` | `event_date` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `ex_dividend_date` | 除权除息分红日期 | `corporate_action_field` | `dividend_real` | `event_date` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `holder_num_real`

- 中文说明：股东户数表
- 表：`starlight.ad_holder_num`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`4`
- 可用标准字段数：`23`

### 原始字段

`ann_dt`, `holder_enddate`, `holder_total_num`, `holder_num`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `holder_enddate` | 持有人enddate | `holder_field` | `holder_num_real` | `-` |
| `holder_num` | 持有人数量 | `holder_field` | `holder_num_real` | `-` |
| `holder_num_ann_date` | 持有人数量公告日期 | `holder_field` | `holder_num_real` | `holder_announce_date` |
| `holder_total_num` | 持有人总数量 | `holder_field` | `holder_num_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `share_holder_real`

- 中文说明：股东明细表
- 表：`starlight.ad_share_holder`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`9`
- 可用标准字段数：`25`

### 原始字段

`ann_date`, `holder_enddate`, `holder_type`, `qty_num`, `holder_name`, `holder_quantity`, `holder_pct`, `holder_sharecategoryname`, `float_qty`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `share_holder_ann_date` | 股本持有人公告日期 | `share_holder_field` | `share_holder_real` | `holder_announce_date` |
| `share_holder_enddate` | 股本持有人enddate | `share_holder_field` | `share_holder_real` | `-` |
| `share_holder_float_qty` | 股本持有人流通QTY | `share_holder_field` | `share_holder_real` | `-` |
| `share_holder_name` | 股本持有人名称 | `share_holder_field` | `share_holder_real` | `-` |
| `share_holder_pct` | 股本持有人PCT | `share_holder_field` | `share_holder_real` | `-` |
| `share_holder_quantity` | 股本持有人quantity | `share_holder_field` | `share_holder_real` | `-` |

## `right_issue_real`

- 中文说明：配股表
- 表：`starlight.ad_right_issue`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`29`
- 可用标准字段数：`27`

### 原始字段

`progress`, `price`, `ratio`, `amt_plan`, `amt_real`, `collection_fund`, `shareb_reg_date`, `ex_dividend_date`, `listed_date`, `pay_start_date`, `pay_end_date`, `preplan_date`, `smtg_ann_date`, `pass_date`, `approved_date`, `execute_date`, `result_date`, `list_ann_date`, `guarantor`, `guartype`, `rightsissue_code`, `ann_date`, `rightsissue_year`, `rightsissue_desc`, `rightsissue_name`, `ratio_denominator`, `ratio_molecular`, `subs_method`, `expected_fund_raising`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `right_issue_amt_plan` | 配股发行AMTplan | `corporate_action_field` | `right_issue_real` | `-` |
| `right_issue_ann_date` | 配股发行公告日期 | `corporate_action_field` | `right_issue_real` | `corporate_action_announce_date` |
| `right_issue_collection_fund` | 配股发行collection基金 | `corporate_action_field` | `right_issue_real` | `-` |
| `right_issue_ex_dividend_date` | 配股发行除权除息分红日期 | `corporate_action_field` | `right_issue_real` | `-` |
| `right_issue_expected_fund_raising` | 配股发行expected基金raising | `corporate_action_field` | `right_issue_real` | `-` |
| `right_issue_price` | 配股发行price | `corporate_action_field` | `right_issue_real` | `-` |
| `right_issue_progress` | 配股发行progress | `corporate_action_field` | `right_issue_real` | `-` |
| `right_issue_ratio` | 配股发行ratio | `corporate_action_field` | `right_issue_real` | `-` |

## `equity_restricted_real`

- 中文说明：限售解禁表
- 表：`starlight.ad_equity_restricted`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`7`
- 可用标准字段数：`26`

### 原始字段

`list_date`, `share_ratio`, `share_lst_type_name`, `share_lst`, `share_lst_is_ann`, `close_price`, `share_lst_market_value`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `restricted_event_close_price` | 限售事件收盘价price | `equity_event_field` | `equity_restricted_real` | `-` |
| `restricted_list_date` | 限售上市日期 | `equity_event_field` | `equity_restricted_real` | `event_date` |
| `restricted_share_is_ann` | 限售股本是否公告 | `equity_event_field` | `equity_restricted_real` | `-` |
| `restricted_share_lst` | 限售股本LST | `equity_event_field` | `equity_restricted_real` | `-` |
| `restricted_share_market_value` | 限售股本市场数值 | `equity_event_field` | `equity_restricted_real` | `-` |
| `restricted_share_ratio` | 限售股本ratio | `equity_event_field` | `equity_restricted_real` | `-` |
| `restricted_share_type_name` | 限售股本type名称 | `equity_event_field` | `equity_restricted_real` | `-` |

## `trade_calendar_real`

- 中文说明：交易日历表
- 表：`starlight.ad_trade_calendar`
- 粒度：`daily`
- 节点角色：`reference`
- AI 主入口：`False`
- 自身原始字段数：`1`
- 可用标准字段数：`20`

### 原始字段

`trade_date`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `calendar_trade_date` | 交易日历日期 | `calendar_field` | `trade_calendar_real` | `market_trade_date` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `equity_pledge_freeze_real`

- 中文说明：股权质押冻结表
- 表：`starlight.ad_equity_pledge_freeze`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`17`
- 可用标准字段数：`29`

### 原始字段

`ann_date`, `holder_name`, `holder_type_code`, `total_holding_shr`, `total_holding_shr_ratio`, `fro_shares`, `fro_shr_to_total_holding_ratio`, `fro_shr_to_total_ratio`, `total_pledge_shr`, `is_equity_pledge_repo`, `begin_date`, `end_date`, `is_disfrozen`, `frozen_institution`, `disfrozen_time`, `shr_category_code`, `freeze_type`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `pledge_ann_date` | 质押公告日期 | `equity_event_field` | `equity_pledge_freeze_real` | `corporate_action_announce_date` |
| `pledge_begin_date` | 质押begin日期 | `equity_event_field` | `equity_pledge_freeze_real` | `effective_start_date` |
| `pledge_end_date` | 质押END日期 | `equity_event_field` | `equity_pledge_freeze_real` | `effective_end_date` |
| `pledge_freeze_type` | 质押冻结type | `equity_event_field` | `equity_pledge_freeze_real` | `-` |
| `pledge_fro_shares` | 质押FROshares | `equity_event_field` | `equity_pledge_freeze_real` | `-` |
| `pledge_frozen_institution` | 质押frozeninstitution | `equity_event_field` | `equity_pledge_freeze_real` | `-` |
| `pledge_holder_name` | 质押持有人名称 | `equity_event_field` | `equity_pledge_freeze_real` | `-` |
| `pledge_is_disfrozen` | 质押是否disfrozen | `equity_event_field` | `equity_pledge_freeze_real` | `-` |
| `pledge_total_holding_shr` | 质押总holdingSHR | `equity_event_field` | `equity_pledge_freeze_real` | `-` |
| `pledge_total_pledge_shr` | 质押总质押SHR | `equity_event_field` | `equity_pledge_freeze_real` | `-` |

## `bj_code_mapping_real`

- 中文说明：北交所代码映射表
- 表：`starlight.ad_bj_code_mapping`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`False`
- 自身原始字段数：`4`
- 可用标准字段数：`23`

### 原始字段

`old_code`, `new_code`, `security_name`, `listing_date`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `bj_listing_date` | 北交所上市日期 | `reference_field` | `bj_code_mapping_real` | `listing_start_date` |
| `bj_new_code` | 北交所新代码 | `reference_field` | `bj_code_mapping_real` | `-` |
| `bj_old_code` | 北交所旧代码 | `reference_field` | `bj_code_mapping_real` | `-` |
| `bj_security_name` | 北交所证券名称 | `reference_field` | `bj_code_mapping_real` | `-` |
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `etf_pcf_real`

- 中文说明：ETF PCF 主表
- 表：`starlight.ad_etf_pcf`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`True`
- 自身原始字段数：`28`
- 可用标准字段数：`58`

### 原始字段

`etf_code`, `creation_redemption_unit`, `max_cash_ratio`, `publish`, `creation`, `redemption`, `creation_redemption_switch`, `record_num`, `total_record_num`, `estimate_cash_component`, `trading_day`, `pre_trading_day`, `cash_component`, `nav_per_cu`, `nav`, `symbol`, `fund_management_company`, `underlying_security_id`, `underlying_security_id_source`, `dividend_per_cu`, `creation_limit`, `redemption_limit`, `creation_limit_per_user`, `redemption_limit_per_user`, `net_creation_limit`, `net_redemption_limit`, `net_creation_limit_per_user`, `net_redemption_limit_per_user`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component` | 现金差额 | `etf_cash_nav_field` | `etf_pcf_real` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_code` | ETF 代码 | `etf_overview_field` | `etf_pcf_real` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_component_share` | 成分证券数量 | `etf_constituent_position_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_creation_cash_substitute` | 申购现金替代金额 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_discount_ratio` | 赎回折价比例 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_etf_code` | ETF 代码 | `etf_constituent_identity_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_constituent_premium_ratio` | 申购溢价比例 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_redemption_cash_substitute` | 赎回现金替代金额 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_substitute_flag` | 替代标志 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_substitution_cash_amount` | 现金替代补款金额 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_trading_day` | PCF 交易日 | `etf_constituent_identity_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_underlying_security_id` | 成分证券 ID | `etf_constituent_position_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_underlying_symbol` | 成分证券代码 | `etf_constituent_identity_field` | `etf_pcf_constituent_real` | `-` |
| `etf_creation_flag` | 是否允许申购 | `etf_status_field` | `etf_pcf_real` | `-` |
| `etf_creation_limit` | 申购上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_creation_limit_per_user` | 单用户申购上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_creation_redemption_switch` | 申赎开关 | `etf_status_field` | `etf_pcf_real` | `-` |
| `etf_creation_redemption_unit` | 申赎单位 | `etf_structure_field` | `etf_pcf_real` | `-` |
| `etf_dividend_per_cu` | 每申赎单位分红 | `etf_cash_nav_field` | `etf_pcf_real` | `-` |
| `etf_estimate_cash_component` | 预估现金差额 | `etf_cash_nav_field` | `etf_pcf_real` | `-` |
| `etf_fund_management_company` | 基金管理人 | `etf_overview_field` | `etf_pcf_real` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_max_cash_ratio` | 最大现金替代比例 | `etf_cash_nav_field` | `etf_pcf_real` | `-` |
| `etf_nav` | 基金净值 | `etf_cash_nav_field` | `etf_pcf_real` | `-` |
| `etf_nav_per_cu` | 每申赎单位资产净值 | `etf_cash_nav_field` | `etf_pcf_real` | `-` |
| `etf_net_creation_limit` | 净申购上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_net_creation_limit_per_user` | 单用户净申购上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_redemption_limit` | 净赎回上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_net_redemption_limit_per_user` | 单用户净赎回上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_pre_trading_day` | 前一交易日 | `etf_overview_field` | `etf_pcf_real` | `-` |
| `etf_publish_flag` | 是否发布 PCF | `etf_status_field` | `etf_pcf_real` | `-` |
| `etf_record_num` | 现金替代记录数 | `etf_structure_field` | `etf_pcf_real` | `-` |
| `etf_redemption_flag` | 是否允许赎回 | `etf_status_field` | `etf_pcf_real` | `-` |
| `etf_redemption_limit` | 赎回上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_redemption_limit_per_user` | 单用户赎回上限 | `etf_limit_field` | `etf_pcf_real` | `-` |
| `etf_symbol` | ETF 名称 | `etf_overview_field` | `etf_pcf_real` | `-` |
| `etf_total_record_num` | 总记录数 | `etf_structure_field` | `etf_pcf_real` | `-` |
| `etf_trading_day` | PCF 交易日 | `etf_overview_field` | `etf_pcf_real` | `-` |
| `etf_underlying_security_id` | 标的证券 ID | `etf_underlying_ref_field` | `etf_pcf_real` | `-` |
| `etf_underlying_security_id_source` | 标的证券 ID 来源 | `etf_underlying_ref_field` | `etf_pcf_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `etf_pcf_constituent_real`

- 中文说明：ETF PCF 成分表
- 表：`starlight.ad_etf_pcf_constituent`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`11`
- 可用标准字段数：`30`

### 原始字段

`etf_code`, `trading_day`, `underlying_symbol`, `component_share`, `substitute_flag`, `premium_ratio`, `discount_ratio`, `creation_cash_substitute`, `redemption_cash_substitute`, `substitution_cash_amount`, `underlying_security_id`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_component_share` | 成分证券数量 | `etf_constituent_position_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_creation_cash_substitute` | 申购现金替代金额 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_discount_ratio` | 赎回折价比例 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_etf_code` | ETF 代码 | `etf_constituent_identity_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_constituent_premium_ratio` | 申购溢价比例 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_redemption_cash_substitute` | 赎回现金替代金额 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_substitute_flag` | 替代标志 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_substitution_cash_amount` | 现金替代补款金额 | `etf_constituent_cash_substitute_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_trading_day` | PCF 交易日 | `etf_constituent_identity_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_underlying_security_id` | 成分证券 ID | `etf_constituent_position_field` | `etf_pcf_constituent_real` | `-` |
| `etf_constituent_underlying_symbol` | 成分证券代码 | `etf_constituent_identity_field` | `etf_pcf_constituent_real` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `industry_constituent_real`

- 中文说明：行业成分关系表
- 表：`starlight.ad_industry_constituent`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`5`
- 可用标准字段数：`28`

### 原始字段

`index_code`, `con_code`, `in_date`, `out_date`, `index_name`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `industry_code` | 行业代码 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_constituent_con_code` | 行业成分证券代码 | `industry_constituent_field` | `industry_constituent_real` | `-` |
| `industry_constituent_in_date` | 纳入日期 | `industry_constituent_field` | `industry_constituent_real` | `effective_start_date` |
| `industry_constituent_index_code` | 行业指数代码 | `industry_constituent_field` | `industry_constituent_real` | `-` |
| `industry_constituent_index_name` | 行业指数名称 | `industry_constituent_field` | `industry_constituent_real` | `-` |
| `industry_constituent_out_date` | 剔除日期 | `industry_constituent_field` | `industry_constituent_real` | `effective_end_date` |
| `industry_level1_name` | 行业一级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_level2_name` | 行业二级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `industry_level3_name` | 行业三级名称 | `industry_field` | `industry_base_info_real` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `kzz_call_explanation_real`

- 中文说明：可转债赎回说明表
- 表：`starlight.ad_kzz_call_explanation`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`11`
- 可用标准字段数：`32`

### 原始字段

`market_code`, `call_date`, `call_price`, `call_announcement_date`, `call_ful_res_ann_date`, `call_amount`, `call_outstanding_amount`, `call_date_pub`, `call_fund_arrival_date`, `call_record_day`, `call_reason`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_amount` | 赎回数量 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_announcement_date` | 赎回公告日期 | `kzz_call_field` | `kzz_call_explanation_real` | `kzz_announce_date` |
| `kzz_call_date` | 可转债赎回日期 | `kzz_call_field` | `kzz_call_explanation_real` | `event_date` |
| `kzz_call_date_pub` | 赎回实施日期 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_event_date` | 可转债赎回事发日期 | `kzz_call_field` | `kzz_call_explanation_real` | `event_date` |
| `kzz_call_event_price` | 可转债赎回事发价格 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_explanation_announce_date` | 可转债赎回说明announce日期 | `kzz_call_field` | `kzz_call_explanation_real` | `kzz_announce_date` |
| `kzz_call_ful_res_ann_date` | 赎回结果公告日期 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_fund_arrival_date` | 赎回到账日期 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_outstanding_amount` | 赎回后余额 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_price` | 可转债赎回价格 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_call_reason` | 赎回原因 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_call_record_day` | 赎回登记日 | `kzz_call_field` | `kzz_call_explanation_real` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `kzz_conv_change_real`

- 中文说明：可转债转股价变更表
- 表：`starlight.ad_kzz_conv_change`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`5`
- 可用标准字段数：`25`

### 原始字段

`market_code`, `change_date`, `ann_date`, `conv_price`, `change_reason`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_conv_ann_date` | 转股价调整公告日 | `kzz_conversion_field` | `kzz_conv_change_real` | `kzz_announce_date` |
| `kzz_conv_change_announce_date` | 可转债转股变动announce日期 | `kzz_conversion_field` | `kzz_conv_change_real` | `kzz_announce_date` |
| `kzz_conv_change_date` | 转股价调整生效日 | `kzz_conversion_field` | `kzz_conv_change_real` | `-` |
| `kzz_conv_change_price` | 可转债转股变动price | `kzz_conversion_field` | `kzz_conv_change_real` | `-` |
| `kzz_conv_change_reason` | 转股价调整原因 | `kzz_conversion_field` | `kzz_conv_change_real` | `-` |
| `kzz_conv_price` | 转股价格 | `kzz_conversion_field` | `kzz_conv_change_real` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `kzz_corr_real`

- 中文说明：可转债下修条款表
- 表：`starlight.ad_kzz_corr`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`14`
- 可用标准字段数：`32`

### 原始字段

`market_code`, `start_date`, `end_date`, `corr_trig_calc_max_period`, `corr_trig_calc_period`, `spec_corr_trig_ratio`, `corr_conv_price_floor_desc`, `ref_price_is_avg_price`, `corr_times_limit`, `is_timepoint_corr_clause_flag`, `timepoint_count`, `timepoint_corr_text_clause`, `spec_corr_range`, `is_spec_down_corr_clause_flag`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_corr_conv_price_floor_desc` | 下修价格下限描述 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_end_date` | 转股价下修结束日 | `kzz_correction_clause_field` | `kzz_corr_real` | `effective_end_date` |
| `kzz_corr_is_spec_down_corr_clause_flag` | 是否特殊下修条款 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_is_timepoint_corr_clause_flag` | 是否时间点下修条款 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_ref_price_is_avg_price` | 参考价是否均价 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_spec_corr_range` | 特殊下修区间 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_spec_corr_trig_ratio` | 触发比例 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_start_date` | 转股价下修起始日 | `kzz_correction_clause_field` | `kzz_corr_real` | `effective_start_date` |
| `kzz_corr_timepoint_corr_text_clause` | 时间点条款描述 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_timepoint_count` | 时间点数量 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_times_limit` | 下修次数限制 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_trig_calc_max_period` | 触发计算最大周期 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_corr_trig_calc_period` | 触发计算周期 | `kzz_correction_clause_field` | `kzz_corr_real` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `kzz_issuance_real`

- 中文说明：可转债发行表
- 表：`starlight.ad_kzz_issuance`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`True`
- 自身原始字段数：`73`
- 可用标准字段数：`91`

### 原始字段

`market_code`, `stock_code`, `crncy_code`, `ann_dt`, `pre_plan_date`, `smtg_ann_date`, `listed_ann_date`, `listed_date`, `plan_schedule`, `is_separation`, `recommender`, `clause_is_int_cha_de_po_rate`, `clause_is_com_int`, `clause_com_int_rate`, `clause_com_int_desc`, `clause_init_conv_price_item`, `clause_conv_adj_item`, `clause_conv_period_item`, `clause_ini_conv_price`, `clause_ini_conv_premium_ratio`, `clause_put_item`, `clause_call_item`, `clause_spec_down_adj`, `clause_orig_ration_arr_item`, `list_pass_date`, `list_permit_date`, `list_ann_date`, `list_result_ann_date`, `list_type`, `list_fee`, `list_ration_date`, `list_ration_reg_date`, `list_ration_paymt_date`, `list_ration_code`, `list_ration_name`, `list_ration_price`, `list_ration_ratio_de`, `list_ration_ratio_mo`, `list_ration_vol`, `list_household`, `list_onl_date`, `list_pchase_code_onl`, `list_pch_name_onl`, `list_pch_price_onl`, `list_issue_vol_onl`, `list_code_onl`, `list_excess_pch_onl`, `result_ef_subscr_p_off`, `result_suc_rate_off`, `list_date_inst_off`, `list_vol_inst_off`, `result_suc_rate_on`, `list_effect_pc_hvol_off`, `list_eff_pc_h_of`, `list_suc_rate_off`, `pre_ration_vol`, `list_issue_size`, `list_issue_quantity`, `min_off_inst_subscr_qty`, `off_inst_dep_ratio`, `max_off_inst_subscr_qty`, `off_subscr_unit_inc_desc`, `is_conv_bonds`, `min_unline_public`, `max_unline_public`, `term_year`, `interest_type`, `coupon_rate`, `interest_fre_quency`, `result_suc_rate_on2`, `coupon_txt`, `ratio_annce_date`, `ratio_date`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_ann_dt` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_call_item` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_com_int_desc` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_com_int_rate` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_conv_adj_item` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_conv_period_item` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_ini_conv_premium_ratio` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_price` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_init_conv_price_item` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_is_com_int` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_is_int_cha_de_po_rate` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_orig_ration_arr_item` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_put_item` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_clause_spec_down_adj` | 可转债发行字段 | `kzz_clause_field` | `kzz_issuance_real` | `-` |
| `kzz_coupon_rate` | 可转债发行字段 | `kzz_coupon_field` | `kzz_issuance_real` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_txt` | 可转债发行字段 | `kzz_coupon_field` | `kzz_issuance_real` | `-` |
| `kzz_crncy_code` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `-` |
| `kzz_interest_fre_quency` | 可转债发行字段 | `kzz_coupon_field` | `kzz_issuance_real` | `-` |
| `kzz_interest_type` | 可转债发行字段 | `kzz_coupon_field` | `kzz_issuance_real` | `-` |
| `kzz_is_conv_bonds` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_is_separation` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ann_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_list_code_onl` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_date_inst_off` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_eff_pc_h_of` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_effect_pc_hvol_off` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_excess_pch_onl` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_fee` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_household` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_issue_quantity` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_issue_size` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_issue_vol_onl` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_onl_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_pass_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_pch_name_onl` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_pch_price_onl` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_pchase_code_onl` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_permit_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_code` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_name` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_paymt_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_price` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_ratio_de` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_ratio_mo` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_reg_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_ration_vol` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_result_ann_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_list_suc_rate_off` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_type` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_list_vol_inst_off` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_listed_ann_date` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_listed_date` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `listing_start_date` |
| `kzz_max_off_inst_subscr_qty` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_max_unline_public` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_min_off_inst_subscr_qty` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_min_unline_public` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_off_inst_dep_ratio` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_off_subscr_unit_inc_desc` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_plan_schedule` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `-` |
| `kzz_pre_plan_date` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_pre_ration_vol` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `kzz_ratio_annce_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_ratio_date` | 可转债发行字段 | `kzz_listing_subscription_field` | `kzz_issuance_real` | `-` |
| `kzz_recommender` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `-` |
| `kzz_result_ef_subscr_p_off` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_result_suc_rate_off` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_result_suc_rate_on` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_result_suc_rate_on2` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `-` |
| `kzz_smtg_ann_date` | 可转债发行字段 | `kzz_issuance_misc_field` | `kzz_issuance_real` | `kzz_announce_date` |
| `kzz_stock_code` | 可转债发行字段 | `kzz_issuance_overview_field` | `kzz_issuance_real` | `-` |
| `kzz_term_year` | 可转债发行字段 | `kzz_coupon_field` | `kzz_issuance_real` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `kzz_put_explanation_real`

- 中文说明：可转债回售说明表
- 表：`starlight.ad_kzz_put_explanation`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`15`
- 可用标准字段数：`35`

### 原始字段

`market_code`, `put_fund_arrival_date`, `put_price`, `put_announcement_date`, `put_ex_date`, `put_amount`, `put_outstanding`, `repurchase_start_date`, `repurchase_end_date`, `resale_start_date`, `fund_end_date`, `repurchase_code`, `resale_amount`, `resale_imp_amount`, `resale_end_date`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_amount` | 回售数量 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_announcement_date` | 回售公告日期 | `kzz_put_field` | `kzz_put_explanation_real` | `kzz_announce_date` |
| `kzz_put_event_price` | 可转债回售价 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_ex_date` | 回售除权日 | `kzz_put_field` | `kzz_put_explanation_real` | `event_date` |
| `kzz_put_explanation_announce_date` | 可转债回售说明announce日期 | `kzz_put_field` | `kzz_put_explanation_real` | `kzz_announce_date` |
| `kzz_put_fund_arrival_date` | 回售到账日 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_fund_end_date` | 资金截止日 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_outstanding` | 回售后余额 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_price` | 回售价格 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `kzz_put_repurchase_code` | 回售代码 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_repurchase_end_date` | 回售申报结束日 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_repurchase_start_date` | 回售申报开始日 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_resale_amount` | 再售数量 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_resale_end_date` | 再售结束日 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_resale_imp_amount` | 实施再售数量 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `kzz_put_resale_start_date` | 再售开始日 | `kzz_put_field` | `kzz_put_explanation_real` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `kzz_share_real`

- 中文说明：可转债余额变动表
- 表：`starlight.ad_kzz_share`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`6`
- 可用标准字段数：`24`

### 原始字段

`change_date`, `ann_date`, `market_code`, `bond_share`, `conv_share`, `change_reason`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_bond_share` | 债券余额 | `kzz_balance_field` | `kzz_share_real` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_conv_share` | 累计转股数量 | `kzz_conversion_field` | `kzz_share_real` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `kzz_share_ann_date` | 可转债余额变动公告日 | `kzz_balance_field` | `kzz_share_real` | `-` |
| `kzz_share_change_date` | 可转债余额变动生效日 | `kzz_balance_field` | `kzz_share_real` | `event_date` |
| `kzz_share_change_reason` | 余额变动原因 | `kzz_balance_field` | `kzz_share_real` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `kzz_suspend_real`

- 中文说明：可转债停复牌表
- 表：`starlight.ad_kzz_suspend`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`7`
- 可用标准字段数：`25`

### 原始字段

`market_code`, `suspend_date`, `suspend_type`, `resump_date`, `change_reason`, `change_reason_code`, `resump_time`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `kzz_resump_date` | 复牌日期 | `kzz_suspend_field` | `kzz_suspend_real` | `event_date` |
| `kzz_resump_time` | 复牌时间段 | `kzz_suspend_field` | `kzz_suspend_real` | `-` |
| `kzz_suspend_date` | 停牌日期 | `kzz_suspend_field` | `kzz_suspend_real` | `event_date` |
| `kzz_suspend_reason` | 停牌原因 | `kzz_suspend_field` | `kzz_suspend_real` | `-` |
| `kzz_suspend_reason_code` | 停牌原因代码 | `kzz_suspend_field` | `kzz_suspend_real` | `-` |
| `kzz_suspend_type` | 停牌类型 | `kzz_suspend_field` | `kzz_suspend_real` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `option_basic_info_real`

- 中文说明：期权基础信息表
- 表：`starlight.ad_option_basic_info`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`False`
- 自身原始字段数：`16`
- 可用标准字段数：`73`

### 原始字段

`contract_full_name`, `contract_type`, `delivery_month`, `expiry_date`, `exercise_price`, `exercise_end_date`, `start_trade_date`, `listing_ref_price`, `last_trade_date`, `exchange_code`, `delivery_date`, `contract_unit`, `is_trade`, `exchange_short_name`, `contract_adjust_flag`, `market_code`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_adjust_flag` | 期权基础信息字段 | `option_contract_pricing_field` | `option_basic_info_real` | `-` |
| `option_basic_contract_full_name` | 期权基础信息字段 | `option_contract_identity_field` | `option_basic_info_real` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_basic_contract_type` | 期权基础信息字段 | `option_contract_identity_field` | `option_basic_info_real` | `-` |
| `option_basic_contract_unit` | 期权基础信息字段 | `option_contract_pricing_field` | `option_basic_info_real` | `-` |
| `option_basic_delivery_date` | 期权基础信息字段 | `option_contract_lifecycle_field` | `option_basic_info_real` | `delivery_date` |
| `option_basic_delivery_month` | 期权基础信息字段 | `option_contract_identity_field` | `option_basic_info_real` | `-` |
| `option_basic_exchange_code` | 期权基础信息字段 | `option_contract_identity_field` | `option_basic_info_real` | `-` |
| `option_basic_exchange_short_name` | 期权基础信息字段 | `option_contract_identity_field` | `option_basic_info_real` | `-` |
| `option_basic_exercise_end_date` | 期权基础信息字段 | `option_contract_lifecycle_field` | `option_basic_info_real` | `exercise_end_date` |
| `option_basic_exercise_price` | 期权基础信息字段 | `option_contract_pricing_field` | `option_basic_info_real` | `-` |
| `option_basic_expiry_date` | 期权基础信息字段 | `option_contract_lifecycle_field` | `option_basic_info_real` | `expiry_date` |
| `option_basic_is_trade` | 期权基础信息字段 | `option_contract_lifecycle_field` | `option_basic_info_real` | `-` |
| `option_basic_last_trade_date` | 期权基础信息字段 | `option_contract_lifecycle_field` | `option_basic_info_real` | `last_trade_date` |
| `option_basic_listing_ref_price` | 期权基础信息字段 | `option_contract_pricing_field` | `option_basic_info_real` | `-` |
| `option_basic_start_trade_date` | 期权基础信息字段 | `option_contract_lifecycle_field` | `option_basic_info_real` | `listing_start_date` |
| `option_mon_change_date` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_change_reason` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_code_new` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_code_old` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_new` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_exercise_price_old` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_name_new` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_name_old` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_new` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_unit_old` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_std_contract_multiplier` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_contract_unit` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_contract_unit_dimension` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_contract_value` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_delist_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `listing_end_date` |
| `option_std_delivery_method` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_exchange_name` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_exercise_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `exercise_date` |
| `option_std_exercise_method` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_final_settlement_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `settlement_date` |
| `option_std_final_settlement_price` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_is_simulation` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_is_simulation_trade` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_last_trading_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `last_trade_date` |
| `option_std_listed_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `listing_start_date` |
| `option_std_market_code` | 期权STD市场代码 | `option_contract_identity_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_min_price_unit` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_notional_value` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_option_en_name` | 期权标准合约字段 | `option_contract_identity_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_option_name` | 期权标准合约字段 | `option_contract_identity_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_option_strike_price` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `option_std_option_type` | 期权标准合约字段 | `option_contract_identity_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_position_declare_min` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_position_limit` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_premium` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_quote_currency_unit` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_settlement_month` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_trading_fee` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_trading_hours_desc` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |

## `option_mon_ctr_specs_real`

- 中文说明：期权月度合约调整表
- 表：`starlight.ad_option_mon_ctr_specs`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`11`
- 可用标准字段数：`29`

### 原始字段

`code_old`, `change_date`, `market_code`, `name_new`, `exercise_price_new`, `name_old`, `code_new`, `exercise_price_old`, `unit_old`, `unit_new`, `change_reason`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_change_date` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_change_reason` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_code_new` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_code_old` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_new` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_exercise_price_old` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_name_new` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_name_old` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_new` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_mon_unit_old` | 期权合约变更字段 | `option_contract_adjustment_field` | `option_mon_ctr_specs_real` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `option_std_ctr_specs_real`

- 中文说明：期权标准合约规格表
- 表：`starlight.ad_option_std_ctr_specs`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`False`
- 自身原始字段数：`29`
- 可用标准字段数：`48`

### 原始字段

`exercise_date`, `contract_unit`, `position_declare_min`, `quote_currency_unit`, `last_trading_date`, `position_limit`, `delist_date`, `notional_value`, `exercise_method`, `delivery_method`, `settlement_month`, `trading_fee`, `exchange_name`, `option_en_name`, `contract_value`, `is_simulation`, `contract_unit_dimension`, `option_strike_price`, `is_simulation_trade`, `listed_date`, `option_name`, `premium`, `option_type`, `trading_hours_desc`, `final_settlement_date`, `final_settlement_price`, `min_price_unit`, `market_code`, `contract_multiplier`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_multiplier` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_contract_unit` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_contract_unit_dimension` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_contract_value` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_delist_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `listing_end_date` |
| `option_std_delivery_method` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_exchange_name` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_exercise_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `exercise_date` |
| `option_std_exercise_method` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_final_settlement_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `settlement_date` |
| `option_std_final_settlement_price` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_is_simulation` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_is_simulation_trade` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_last_trading_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `last_trade_date` |
| `option_std_listed_date` | 期权标准合约字段 | `option_contract_lifecycle_field` | `option_std_ctr_specs_real` | `listing_start_date` |
| `option_std_market_code` | 期权STD市场代码 | `option_contract_identity_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_min_price_unit` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_notional_value` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_option_en_name` | 期权标准合约字段 | `option_contract_identity_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_option_name` | 期权标准合约字段 | `option_contract_identity_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_option_strike_price` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `option_std_option_type` | 期权标准合约字段 | `option_contract_identity_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_position_declare_min` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_position_limit` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_premium` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_quote_currency_unit` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_settlement_month` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_trading_fee` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |
| `option_std_trading_hours_desc` | 期权标准合约字段 | `option_contract_spec_field` | `option_std_ctr_specs_real` | `-` |

## `sync_checkpoint_real`

- 中文说明：同步检查点表
- 表：`starlight.ad_sync_checkpoint`
- 粒度：`daily`
- 节点角色：`system`
- AI 主入口：`False`
- 自身原始字段数：`9`
- 可用标准字段数：`28`

### 原始字段

`task_name`, `scope_key`, `run_date`, `status`, `target_table`, `checkpoint_date`, `row_count`, `message`, `finished_at`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `sync_checkpoint_checkpoint_date` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `system_checkpoint_date` |
| `sync_checkpoint_finished_at` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |
| `sync_checkpoint_message` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |
| `sync_checkpoint_row_count` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |
| `sync_checkpoint_run_date` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `system_run_date` |
| `sync_checkpoint_scope_key` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |
| `sync_checkpoint_status` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |
| `sync_checkpoint_target_table` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |
| `sync_checkpoint_task_name` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |

## `sync_task_log_real`

- 中文说明：同步任务日志表
- 表：`starlight.ad_sync_task_log`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`True`
- 自身原始字段数：`11`
- 可用标准字段数：`39`

### 原始字段

`task_name`, `scope_key`, `run_date`, `status`, `target_table`, `start_date`, `end_date`, `row_count`, `message`, `started_at`, `finished_at`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
| `sync_checkpoint_checkpoint_date` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `system_checkpoint_date` |
| `sync_checkpoint_finished_at` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |
| `sync_checkpoint_message` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |
| `sync_checkpoint_row_count` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |
| `sync_checkpoint_run_date` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `system_run_date` |
| `sync_checkpoint_scope_key` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |
| `sync_checkpoint_status` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |
| `sync_checkpoint_target_table` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |
| `sync_checkpoint_task_name` | 同步检查点字段 | `system_field` | `sync_checkpoint_real` | `-` |
| `sync_task_end_date` | 同步任务日志字段 | `system_field` | `sync_task_log_real` | `system_end_date` |
| `sync_task_finished_at` | 同步任务日志字段 | `system_field` | `sync_task_log_real` | `-` |
| `sync_task_message` | 同步任务日志字段 | `system_field` | `sync_task_log_real` | `-` |
| `sync_task_row_count` | 同步任务日志字段 | `system_field` | `sync_task_log_real` | `-` |
| `sync_task_run_date` | 同步任务日志字段 | `system_field` | `sync_task_log_real` | `system_run_date` |
| `sync_task_scope_key` | 同步任务日志字段 | `system_field` | `sync_task_log_real` | `-` |
| `sync_task_start_date` | 同步任务日志字段 | `system_field` | `sync_task_log_real` | `system_start_date` |
| `sync_task_started_at` | 同步任务日志字段 | `system_field` | `sync_task_log_real` | `-` |
| `sync_task_status` | 同步任务日志字段 | `system_field` | `sync_task_log_real` | `-` |
| `sync_task_target_table` | 同步任务日志字段 | `system_field` | `sync_task_log_real` | `-` |
| `sync_task_task_name` | 同步任务日志字段 | `system_field` | `sync_task_log_real` | `-` |

## `block_trading_real`

- 中文说明：大宗交易表
- 表：`starlight.ad_block_trading`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`False`
- 自身原始字段数：`9`
- 可用标准字段数：`27`

### 原始字段

`market_code`, `trade_date`, `bshare_price`, `bshare_volume`, `b_frequency`, `block_avg_volume`, `bshare_amount`, `bbuyer_name`, `bseller_name`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_amount` | 大宗交易amount | `block_trading_field` | `block_trading_real` | `-` |
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `block_trade_avg_volume` | 大宗交易AVGvolume | `block_trading_field` | `block_trading_real` | `-` |
| `block_trade_buyer_name` | 大宗交易买方名称 | `block_trading_field` | `block_trading_real` | `-` |
| `block_trade_date` | 大宗交易日期 | `block_trading_field` | `block_trading_real` | `market_trade_date` |
| `block_trade_frequency` | 大宗交易frequency | `block_trading_field` | `block_trading_real` | `-` |
| `block_trade_price` | 大宗交易price | `block_trading_field` | `block_trading_real` | `-` |
| `block_trade_seller_name` | 大宗交易卖方名称 | `block_trading_field` | `block_trading_real` | `-` |
| `block_trade_volume` | 大宗交易volume | `block_trading_field` | `block_trading_real` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `kzz_call_real`

- 中文说明：可转债赎回条款表
- 表：`starlight.ad_kzz_call`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`5`
- 可用标准字段数：`25`

### 原始字段

`market_code`, `call_price`, `begin_date`, `end_date`, `tri_ratio`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_begin_date` | 可转债赎回begin日期 | `kzz_call_clause_field` | `kzz_call_real` | `effective_start_date` |
| `kzz_call_clause_end_date` | 可转债赎回条款END日期 | `kzz_call_clause_field` | `kzz_call_real` | `effective_end_date` |
| `kzz_call_clause_start_date` | 可转债赎回条款start日期 | `kzz_call_clause_field` | `kzz_call_real` | `effective_start_date` |
| `kzz_call_clause_trigger_ratio` | 可转债赎回条款triggerratio | `kzz_call_clause_field` | `kzz_call_real` | `-` |
| `kzz_call_end_date` | 可转债赎回END日期 | `kzz_call_clause_field` | `kzz_call_real` | `effective_end_date` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_call_trigger_ratio` | 可转债赎回triggerratio | `kzz_call_clause_field` | `kzz_call_real` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `kzz_conv_real`

- 中文说明：可转债转股规则表
- 表：`starlight.ad_kzz_conv`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`True`
- 自身原始字段数：`14`
- 可用标准字段数：`35`

### 原始字段

`market_code`, `ann_date`, `conv_code`, `conv_name`, `conv_price`, `currency_code`, `conv_start_date`, `conv_end_date`, `trade_date_last`, `forced_conv_date`, `forced_conv_price`, `rel_conv_month`, `is_forced`, `forced_conv_reason`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_conv_code` | 可转债转股代码 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_currency_code` | 可转债转股currency代码 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_end_date` | 可转债转股END日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `effective_end_date` |
| `kzz_conv_name` | 可转债转股名称 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_rule_ann_date` | 可转债转股规则公告日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `kzz_announce_date` |
| `kzz_conv_rule_announce_date` | 可转债转股规则announce日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_rule_end_date` | 可转债转股规则END日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_rule_price` | 可转债转股规则价格 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_rule_start_date` | 可转债转股规则start日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_conv_start_date` | 可转债转股start日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `effective_start_date` |
| `kzz_conv_trade_date_last` | 可转债转股交易日期最新价 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_forced_conv_date` | 可转债强赎转股日期 | `kzz_conversion_detail_field` | `kzz_conv_real` | `event_date` |
| `kzz_forced_conv_price` | 可转债强赎转股price | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_forced_conv_reason` | 可转债强赎转股原因 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_is_forced` | 可转债是否强赎 | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `kzz_rel_conv_month` | 可转债REL转股month | `kzz_conversion_detail_field` | `kzz_conv_real` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `kzz_put_real`

- 中文说明：可转债回售条款表
- 表：`starlight.ad_kzz_put`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`5`
- 可用标准字段数：`25`

### 原始字段

`market_code`, `put_price`, `begin_date`, `end_date`, `tri_ratio`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_begin_date` | 可转债回售begin日期 | `kzz_put_clause_field` | `kzz_put_real` | `effective_start_date` |
| `kzz_put_clause_end_date` | 可转债回售条款END日期 | `kzz_put_clause_field` | `kzz_put_real` | `effective_end_date` |
| `kzz_put_clause_start_date` | 可转债回售条款start日期 | `kzz_put_clause_field` | `kzz_put_real` | `effective_start_date` |
| `kzz_put_clause_trigger_ratio` | 可转债回售条款triggerratio | `kzz_put_clause_field` | `kzz_put_real` | `-` |
| `kzz_put_end_date` | 可转债回售END日期 | `kzz_put_clause_field` | `kzz_put_real` | `effective_end_date` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `kzz_put_trigger_ratio` | 可转债回售triggerratio | `kzz_put_clause_field` | `kzz_put_real` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `kzz_put_call_item_real`

- 中文说明：可转债回售赎回条款项表
- 表：`starlight.ad_kzz_put_call_item`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`37`
- 可用标准字段数：`40`

### 原始字段

`market_code`, `mand_put_period`, `mand_put_price`, `mand_put_start_date`, `mand_put_end_date`, `mand_put_text`, `is_mand_put_contain_current`, `con_put_start_date`, `con_put_end_date`, `max_put_triper`, `put_triperiod`, `add_put_con`, `add_put_price_ins`, `put_num_ins`, `put_pro_period`, `put_no_pery`, `is_put_item`, `is_term_put_item`, `is_mand_put_item`, `is_time_put_item`, `time_put_no`, `time_put_item`, `term_put_price`, `con_call_start_date`, `con_call_end_date`, `call_tri_con_ins`, `max_call_triper`, `call_tri_per`, `call_num_ber_ins`, `is_call_item`, `call_pro_period`, `call_no_pery`, `is_time_call_item`, `time_call_no`, `time_call_text`, `expired_redemption_price`, `put_tri_cond_desc`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_item_add_put_con` | 可转债条款项ADD回售CON | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_add_put_price_ins` | 可转债条款项ADD回售priceINS | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_call_pro_period` | 可转债条款项赎回PROperiod | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_call_tri_con_ins` | 可转债条款项赎回TRICONINS | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_call_tri_per` | 可转债条款项赎回TRIPER | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_con_call_end_date` | 可转债条款项CON赎回END日期 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `effective_end_date` |
| `kzz_item_con_call_start_date` | 可转债条款项CON赎回start日期 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `effective_start_date` |
| `kzz_item_con_put_end_date` | 可转债条款项CON回售END日期 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `effective_end_date` |
| `kzz_item_con_put_start_date` | 可转债条款项CON回售start日期 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `effective_start_date` |
| `kzz_item_expired_redemption_price` | 可转债条款项expired赎回price | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_is_call_item` | 可转债条款项是否赎回条款项 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_is_put_item` | 可转债条款项是否回售条款项 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_mand_put_end_date` | 可转债条款项mand回售END日期 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `effective_end_date` |
| `kzz_item_mand_put_period` | 可转债条款项mand回售period | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_mand_put_price` | 可转债条款项mand回售price | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_mand_put_start_date` | 可转债条款项mand回售start日期 | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `effective_start_date` |
| `kzz_item_mand_put_text` | 可转债条款项mand回售text | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_put_num_ins` | 可转债条款项回售数量INS | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_item_time_call_text` | 可转债条款项time赎回text | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_put_item_call_condition_text` | 可转债回售条款项赎回conditiontext | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_put_item_call_trigger_period` | 可转债回售条款项赎回triggerperiod | `kzz_put_call_item_field` | `kzz_put_call_item_real` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `long_hu_bang_real`

- 中文说明：龙虎榜表
- 表：`starlight.ad_long_hu_bang`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`False`
- 自身原始字段数：`12`
- 可用标准字段数：`30`

### 原始字段

`market_code`, `trade_date`, `security_name`, `reason_type`, `reason_type_name`, `change_range`, `trader_name`, `buy_amount`, `sell_amount`, `flow_mark`, `total_amount`, `total_volume`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_buy_amount` | 龙虎榜买入金额 | `long_hu_amount_field` | `long_hu_bang_real` | `-` |
| `long_hu_change_range` | 龙虎榜涨跌幅 | `long_hu_reason_field` | `long_hu_bang_real` | `-` |
| `long_hu_flow_mark` | 龙虎榜流向标识 | `long_hu_trader_field` | `long_hu_bang_real` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `long_hu_reason_type` | 龙虎榜原因类型 | `long_hu_reason_field` | `long_hu_bang_real` | `-` |
| `long_hu_reason_type_name` | 龙虎榜原因名称 | `long_hu_reason_field` | `long_hu_bang_real` | `-` |
| `long_hu_security_name` | 龙虎榜证券名称 | `long_hu_identity_field` | `long_hu_bang_real` | `-` |
| `long_hu_sell_amount` | 龙虎榜卖出金额 | `long_hu_amount_field` | `long_hu_bang_real` | `-` |
| `long_hu_total_amount` | 龙虎榜成交额 | `long_hu_amount_field` | `long_hu_bang_real` | `-` |
| `long_hu_total_volume` | 龙虎榜成交量 | `long_hu_amount_field` | `long_hu_bang_real` | `-` |
| `long_hu_trade_date` | 龙虎榜交易日 | `long_hu_identity_field` | `long_hu_bang_real` | `market_trade_date` |
| `long_hu_trader_name` | 龙虎榜营业部名称 | `long_hu_trader_field` | `long_hu_bang_real` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `margin_detail_real`

- 中文说明：融资融券明细表
- 表：`starlight.ad_margin_detail`
- 粒度：`daily`
- 节点角色：`relation_support`
- AI 主入口：`False`
- 自身原始字段数：`11`
- 可用标准字段数：`29`

### 原始字段

`market_code`, `security_name`, `trade_date`, `borrow_money_balance`, `purch_with_borrow_money`, `repayment_of_borrow_money`, `sec_lending_balance`, `sales_of_borrowed_sec`, `repayment_of_borrow_sec`, `sec_lending_balance_vol`, `margin_trade_balance`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_borrow_money_balance` | 融资余额 | `margin_detail_balance_field` | `margin_detail_real` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_purch_with_borrow_money` | 融资买入额 | `margin_detail_flow_field` | `margin_detail_real` | `-` |
| `margin_repayment_of_borrow_money` | 融资偿还额 | `margin_detail_flow_field` | `margin_detail_real` | `-` |
| `margin_repayment_of_borrow_sec` | 融券偿还量 | `margin_detail_flow_field` | `margin_detail_real` | `-` |
| `margin_sales_of_borrowed_sec` | 融券卖出量 | `margin_detail_flow_field` | `margin_detail_real` | `-` |
| `margin_sec_lending_balance` | 融券余额 | `margin_detail_balance_field` | `margin_detail_real` | `-` |
| `margin_sec_lending_balance_vol` | 融券余量 | `margin_detail_balance_field` | `margin_detail_real` | `-` |
| `margin_security_name` | 融资融券证券名称 | `margin_detail_identity_field` | `margin_detail_real` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `margin_trade_balance` | 融资融券余额 | `margin_detail_balance_field` | `margin_detail_real` | `-` |
| `margin_trade_date` | 融资融券交易日 | `margin_detail_identity_field` | `margin_detail_real` | `market_trade_date` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |

## `margin_summary_real`

- 中文说明：融资融券市场汇总表
- 表：`starlight.ad_margin_summary`
- 粒度：`daily`
- 节点角色：`query_entry`
- AI 主入口：`True`
- 自身原始字段数：`7`
- 可用标准字段数：`26`

### 原始字段

`trade_date`, `sum_borrow_money_balance`, `sum_purch_with_borrow_money`, `sum_repayment_of_borrow_money`, `sum_sec_lending_balance`, `sum_sales_of_borrowed_sec`, `sum_margin_trade_balance`

### 标准字段

| 标准字段 | 中文说明 | 字段角色 | 来源节点 | 时间语义 |
|---|---|---|---|---|
| `block_trade_avg_amount_per_freq` | 单次大宗交易平均金额 | `derived_field` | `-` | `-` |
| `etf_cash_component_ratio` | ETF现金成分ratio | `derived_field` | `-` | `-` |
| `etf_constituent_cash_substitute_diff` | ETF成分现金替代diff | `derived_field` | `-` | `-` |
| `etf_constituent_premium_discount_spread` | ETF成分溢价折价spread | `derived_field` | `-` | `-` |
| `etf_limit_spread` | 字段 etf_limit_spread | `derived_field` | `-` | `-` |
| `etf_net_limit_spread` | 字段 etf_net_limit_spread | `derived_field` | `-` | `-` |
| `kzz_balance_conversion_ratio` | 可转债转股比例 | `derived_field` | `-` | `-` |
| `kzz_call_price_premium_rate` | 可转债赎回溢价率 | `derived_field` | `-` | `-` |
| `kzz_clause_ini_conv_premium_ratio_decimal` | 可转债初始转股溢价率小数值 | `derived_field` | `-` | `-` |
| `kzz_coupon_rate_decimal` | 可转债票息小数值 | `derived_field` | `-` | `-` |
| `kzz_put_price_premium_rate` | 可转债回售溢价率 | `derived_field` | `-` | `-` |
| `long_hu_net_amount` | 龙虎榜净买入额 | `long_hu_derived_field` | `-` | `-` |
| `margin_net_borrow_money` | 融资净买入额 | `margin_detail_derived_field` | `-` | `-` |
| `margin_summary_borrow_money_balance` | 市场融资余额 | `margin_summary_balance_field` | `margin_summary_real` | `-` |
| `margin_summary_net_borrow_money` | 市场融资净买入额 | `margin_summary_derived_field` | `-` | `-` |
| `margin_summary_purch_with_borrow_money` | 市场融资买入额 | `margin_summary_flow_field` | `margin_summary_real` | `-` |
| `margin_summary_repayment_of_borrow_money` | 市场融资偿还额 | `margin_summary_flow_field` | `margin_summary_real` | `-` |
| `margin_summary_sales_of_borrowed_sec` | 市场融券卖出量 | `margin_summary_flow_field` | `margin_summary_real` | `-` |
| `margin_summary_sec_lending_balance` | 市场融券余额 | `margin_summary_balance_field` | `margin_summary_real` | `-` |
| `margin_summary_trade_balance` | 市场融资融券余额 | `margin_summary_balance_field` | `margin_summary_real` | `-` |
| `margin_summary_trade_date` | 融资融券汇总交易日 | `margin_summary_identity_field` | `margin_summary_real` | `market_trade_date` |
| `option_basic_contract_notional` | 期权基础合约名义价值 | `derived_field` | `-` | `-` |
| `option_mon_exercise_price_change` | 期权月度行权价变动值 | `derived_field` | `-` | `-` |
| `option_mon_unit_change` | 期权月度合约单位变动值 | `derived_field` | `-` | `-` |
| `option_std_contract_value_per_multiplier` | 期权标准合约单位乘数价值 | `derived_field` | `-` | `-` |
| `option_std_option_strike_price_num` | 期权标准行权价数值 | `derived_field` | `-` | `-` |
