# AlphaBlocks Query Optimization Summary

> 日期：2026-04-19
>
> 目标：记录这轮围绕 `AlphaBlocks -> AIQuantBase -> data_view` 主链所做的语义修正、查询优化、验证结果，以及已测试但未保留的方向。

## 1. 边界说明

这轮改动没有改变 `AlphaBlocks` 的主流程顺序。

当前主流程仍然是：

1. 合同
2. compile
3. AIQuantBase 查数
4. data_view
5. research
6. backtest
7. reporting

这轮调整的是：

1. `AIQuantBase` 的字段语义
2. `AIQuantBase` 的 SQL 生成细节
3. `AlphaBlocks` 的数据标准化实现

没有调整：

1. 合同协议格式
2. compile 语义
3. research / backtest 的业务顺序

---

## 2. 本轮正式保留的修改

### 2.1 复权语义修正

将：

1. `close_adj`
2. `open_adj`
3. `high_adj`
4. `low_adj`

从前复权因子改为：

**后复权因子 `ad_backward_factor`**

配置文件：

1. `config/fields.yaml`

当前语义：

1. `forward_adj_factor` 仍保留
2. `backward_adj_factor` 新增为正式字段
3. `*_adj` 默认改吃 `backward_adj_factor`

---

### 2.2 状态快照类字段统一支持 `ANY LEFT JOIN`

对于来自：

`starlight.ad_history_stock_status`

且按同日关联的状态快照字段，支持：

**`relation_type: any`**

这轮已经把相关字段切到：

**`ANY LEFT JOIN`**

原因：

状态表在真实库里存在重复的 `market_code + trade_date` 记录，普通 `LEFT JOIN` 会放大结果集。

配置/实现文件：

1. `config/fields.yaml`
2. `src/aiquantbase/models.py`
3. `src/aiquantbase/sql.py`

---

### 2.3 右表时间范围下推

对 `same_date / same_timestamp` 类型的右表 join，SQL renderer 现在会自动做：

**按当前 `time_range` 的右表裁剪**

即把：

`LEFT JOIN some_table t ON ...`

渲染为：

`LEFT JOIN (SELECT * FROM some_table WHERE trade_date BETWEEN start AND end) t ON ...`

适用目标：

1. `ad_backward_factor`
2. `ad_history_stock_status`
3. 同类 `same_date / same_timestamp` 右表

实现文件：

1. `src/aiquantbase/sql.py`

---

### 2.4 base table 时间预过滤与 `trade_date` 预计算

对带 `time_range` 的 base query，SQL renderer 现在会把 base table 改写成一个时间窗口子查询，并提前计算：

`toDate(trade_time) AS __base_trade_time_date`

这可以减少：

1. 重复 `toDate(...)` 计算
2. base table 无关时间窗口扫描

实现文件：

1. `src/aiquantbase/sql.py`

---

### 2.5 executor 增加原始 SQL 执行能力

增加：

`execute_sql_raw(...)`

这是为了支持上层实验性 staged/materialized 查询原型。

实现文件：

1. `src/aiquantbase/executor.py`

说明：

1. 当前默认主路径不依赖 staged
2. 该能力主要用于上层实验路径

---

### 2.6 大结果查询主路径改为 DataFrame 直读

这轮进一步调整了 `AIQuantBase` 的执行主路径：

1. `GraphRuntime.execute_sql(...)`
2. `GraphRuntime.execute_intent(...)`
3. `ApplicationRuntime.query_daily(...)`
4. `ApplicationRuntime.execute_requirement(...)`

当前默认不再优先依赖：

`ClickHouse JSON -> json.loads -> list[dict] -> DataFrame`

而是优先改成：

`ClickHouse -> CSVWithNames -> pandas.DataFrame`

也就是说：

1. 运行时主路径优先直接拿 `DataFrame`
2. 上层协议执行链不再把大结果集先反序列化成 Python `list[dict]`
3. JSON 结果更多保留给 CLI / Server 输出边界，而不是主执行路径

实现文件：

1. `src/aiquantbase/executor.py`
2. `src/aiquantbase/runtime.py`

当前这轮调整主要就是为了解决：

1. `all_a`
2. 多字段
3. 多表 join
4. 大结果返回

时，JSON 序列化/反序列化链过重的问题。

---

## 3. 联动的 AlphaBlocks 侧优化

虽然这份文档属于 `AIQuantBase`，但需要记录本轮联动修改的另一半。

联动文件：

1. `AlphaBlocks/integrations/aiquantbase/adapter.py`
2. `AlphaBlocks/protocol_core/protocol_core/research/data_view.py`

主要内容：

1. 长窗口 `custom symbols` 不再走 `query_daily` 的 `page_size=500` 截断路径
2. data_view 去重改成只处理重复 `(trade_date, code)` 子集
3. `numpy` backend 下不再逐字段 `pivot`
4. `normalize_data_view_payload(...)` 用 `np.asarray(...)` 避免多余 copy

这部分是本轮大幅提速的主要来源之一。

---

## 4. 当前已验证的结果

### 4.1 AIQuantBase 自测

通过：

1. `tests/test_runtime_sdk.py`
2. `tests/test_application_runtime.py`

最近一轮结果：

**36 passed**

### 4.2 AlphaBlocks quick 回归

最近一轮结果：

**3 / 3 passed**

覆盖：

1. `all_a + listed_days` 主线
2. 单标的事件链路
3. A 股自选池趋势链路

### 4.3 性能结果

两年 `all_a` 主链当前大致为：

1. `fetch_seconds ~ 28s`
2. `standardize_seconds ~ 4s`
3. `total_seconds ~ 33s`

对比早期阶段大约：

1. `total_seconds ~ 306s`

也就是说：

**主链 data_access 耗时从约 306 秒下降到了约 33 秒。**

### 4.4 2026-04-20 新一轮验证

对 `AlphaBlocks` 当前这条课题：

1. `all_a`
2. `close_adj`
3. `is_st`
4. `is_star_board`
5. `is_gem_board`
6. `listed_days`
7. `market_cap`
8. `open`

在 `2024-02-01 ~ 2024-02-29` 这一月窗口下做了对比：

1. 旧路径：
   大约 `94s`
2. 新路径：
   大约 `32s`

这轮验证说明：

`all_a` 大结果返回的主要瓶颈，确实在：

1. 结果 materialization
2. JSON 序列化 / 反序列化

而不是单纯 SQL 语义本身。

---

## 5. 本轮测试过但未保留的优化方向

以下方向都做过实测，但当前不保留为正式默认路径。

### 5.1 `code_info` 改 `INNER JOIN`

结论：

1. 没有比当前 `LEFT JOIN + WHERE` 更快

### 5.2 `code IN (subquery)` / 大字面量 `IN (...)`

结论：

1. baseline 无明显收益
2. full query 容易触发内存限制

### 5.3 full query 拆成两条 SQL 再 Python merge

结论：

1. 比单条 full query 更慢

### 5.4 `backward_factor` 改 `ANY LEFT JOIN`

结论：

1. 没收益
2. 原因是 `ad_backward_factor` 本身不存在 `code + trade_date` 重复

### 5.5 `is_st / listed_days` 直接做 SQL pushdown

例如：

1. `is_st = 0`
2. `listed_days > 120`

结论：

1. 在当前 ClickHouse 上默认推到 SQL 里反而略慢
2. 所以未作为默认主路径开启

### 5.6 staged/materialized all_a 路径

结论：

1. 原型可行
2. 但对当前优化后的默认路径没有稳定优势
3. 因此保留实验能力，不默认开启

---

## 6. 当前最优默认组合

当前建议的默认组合是：

1. `backward_factor`: 普通 `LEFT JOIN`
2. `status snapshot`: `ANY LEFT JOIN`
3. `same_date` 右表：时间范围下推
4. base table：时间预过滤 + `trade_date` 预计算
5. Python 侧：优化后的 ndarray 标准化路径

---

## 7. 下一步仍值得继续做的事情

如果继续优化，优先建议：

1. 继续拆 `ad_backward_factor` 的成本
2. 继续拆 `listed_days` 的 `stock_basic` 聚合子查询
3. 如果允许动数据组织，再考虑辅助表 / 宽表

当前不建议继续优先尝试：

1. `INNER JOIN code_info`
2. `IN (subquery)` 半连接
3. Python 侧再小修小补

---

## 8. 一句话总结

这轮不是只修了一个字段，也不是只修了一个 case，而是：

**对 `AIQuantBase` 的字段语义、join 语义、SQL 生成、以及上层 data_view 标准化链路做了一轮系统性优化，同时保持了 `AlphaBlocks` 主流程不变。**
