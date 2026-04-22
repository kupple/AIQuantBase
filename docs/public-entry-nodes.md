# Public Entry Nodes

## 当前对外默认开放的主入口节点

当前建议对外默认开放的主入口节点如下：

1. `stock_daily_real`
2. `etf_daily_real`
3. `index_daily_real`
4. `stock_minute_real`
5. `etf_minute_real`
6. `index_minute_real`
7. `industry_daily_real`
8. `treasury_yield_real`
9. `fund_iopv_real`
10. `fund_share_real`
11. `etf_pcf_real`
12. `kzz_issuance_real`
13. `kzz_conv_real`
14. `margin_summary_real`

## 说明

这些节点的特点是：

1. 有明确的业务主体
2. 查询语义清楚
3. 适合作为外部模块和 AI 的主查询入口
4. 不需要依赖太多中间节点才能理解其用途
5. `stock / etf / index` 在逻辑入口层已经拆分，但底层继续复用同一批日线/分钟行情物理表

### 当前运行时补充

对于和宽表节点同名、且已经启用的入口节点，当前运行时会优先走宽表目标表。

以 `stock_daily_real` 为例：

1. 对外入口名仍然是 `stock_daily_real`
2. 同步导出链路仍然保留图谱里的源节点定义
3. 但 SDK / Query Runtime 默认会把它覆盖到宽表目标表
4. 当前默认实际查询表通常是 `starlight.stock_daily_real`

也就是说：

1. 对外协议名保持稳定
2. 底层查询可以被宽表平替
3. 上层模块不需要感知源表和宽表切换细节

## 暂不默认开放的节点

以下节点通常作为内部支撑节点保留，不建议默认暴露：

1. `adj_factor_real`
2. `history_stock_status_real`
3. `industry_weight_real`
4. `index_weight_real`
5. `index_constituent_real`
6. `kzz_put_call_item_real`
7. `margin_detail_real`
8. `sync_task_log_real`
9. `sync_checkpoint_real`

## 原则

### 默认开放

1. 主入口节点
2. 面向业务主体的节点

### 默认不开放

1. 中间关系节点
2. 说明/条款细项节点
3. 系统运维节点
4. 仅作为补充挂接的节点
