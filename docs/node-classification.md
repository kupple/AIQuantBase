# Node Classification

## 目标

当前图谱共有 `54` 个节点。

这些节点不是都应该直接暴露给 AI 或其他模块作为查询入口。

因此，节点正式分成两层：

1. 图谱内部节点
2. 对外主查询入口节点

并在配置中增加：

1. `node_role`
2. `is_ai_entry`

## 1. 分类规则

### `query_entry`
适合直接作为 `from_node` 的主查询入口。

### `relation_support`
主要用于挂接、扩展、补充信息，不建议默认作为 AI 入口。

### `reference`
静态或参考型节点，主要提供映射、基础信息、日历等。

### `system`
系统运维类节点。

## 2. 当前 AI 主入口节点

当前标记为 `is_ai_entry = true` 的主入口节点包括：

1. `stock_daily_real`
2. `stock_minute_real`
3. `stock_snapshot_real`
4. `industry_daily_real`
5. `fund_iopv_real`
6. `fund_share_real`
7. `treasury_yield_real`
8. `etf_pcf_real`
9. `kzz_conv_real`
10. `kzz_issuance_real`
11. `margin_summary_real`

这 11 个节点已经足够覆盖大多数外部查询场景。

## 3. 节点分组清单

### A. 主查询入口节点 `query_entry`

1. `stock_daily_real`
2. `stock_minute_real`
3. `stock_snapshot_real`
4. `industry_daily_real`
5. `fund_iopv_real`
6. `fund_share_real`
7. `treasury_yield_real`
8. `etf_pcf_real`
9. `kzz_conv_real`
10. `kzz_issuance_real`
11. `margin_summary_real`
12. `sync_task_log_real`
13. `option_basic_info_real`
14. `option_std_ctr_specs_real`
15. `bj_code_mapping_real`
16. `block_trading_real`
17. `long_hu_bang_real`

### B. 关系支撑节点 `relation_support`

1. `adj_factor_real`
2. `backward_factor_real`
3. `history_stock_status_real`
4. `equity_structure_real`
5. `industry_weight_real`
6. `index_weight_real`
7. `index_constituent_real`
8. `income_real`
9. `balance_sheet_real`
10. `cash_flow_real`
11. `profit_notice_real`
12. `profit_express_real`
13. `stock_basic_real`
14. `hist_code_daily_real`
15. `dividend_real`
16. `holder_num_real`
17. `share_holder_real`
18. `right_issue_real`
19. `equity_restricted_real`
20. `equity_pledge_freeze_real`
21. `etf_pcf_constituent_real`
22. `industry_constituent_real`
23. `kzz_call_explanation_real`
24. `kzz_conv_change_real`
25. `kzz_corr_real`
26. `kzz_put_explanation_real`
27. `kzz_share_real`
28. `kzz_suspend_real`
29. `kzz_call_real`
30. `kzz_put_real`
31. `kzz_put_call_item_real`
32. `margin_detail_real`
33. `option_mon_ctr_specs_real`

### C. 参考节点 `reference`

1. `industry_base_info_real`
2. `code_info_real`
3. `trade_calendar_real`

### D. 系统节点 `system`

1. `sync_checkpoint_real`

## 4. 使用建议

### 给 AI

默认只暴露：

1. `is_ai_entry = true` 的节点

### 给其他 Python 模块

优先暴露：

1. `query_entry` 节点
2. 如果需要高级查询，再允许访问 `relation_support` 节点

### 图谱内部

内部仍保留全部 `54` 个节点，不做裁剪。

## 5. 一句话结论

**图谱内部保留 54 个节点，对外默认只暴露 11 个 AI 主入口节点。**
