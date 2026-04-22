# Wide Table Sync Implementation Handoff

## 目标

这份文档是交付给同步程序开发者的实现说明。

目标是让同步程序基于 AIQuantBase 导出的宽表 YAML，完成：

1. 扫描宽表配置目录
2. 读取宽表配置
3. 判断目标表是否存在
4. 判断配置是否变化
5. 创建表 / 重建表 / 重刷数据 / 普通同步
6. 回写同步状态

---

## 1. 当前对接方式

当前不走 API 下发，采用：

**目录 + YAML 文件协议**

AIQuantBase 负责：

1. 设计宽表节点
2. 导出 YAML 到同步项目目录

同步程序负责：

1. 扫描目录
2. 读取 YAML
3. 执行同步

---

## 2. 约定目录

建议目录：

```text
<sync_project_root>/wide_table_specs/
```

每个宽表节点一个文件，例如：

```text
wide_table_specs/
  stock_daily_real.yaml
  etf_daily_real.yaml
  industry_daily_real.yaml
```

---

## 3. YAML 文件结构

当前导出的 YAML 结构包含四部分：

1. `wide_table`
2. `graph_bundle`
3. `materialization_bundle`
4. `export_meta`

### 3.1 `wide_table`

用于描述目标表定义。

示例：

```yaml
wide_table:
  id: wide::stock_daily_real::c83034b0
  name: stock_daily_real
  description: 由普通节点 stock_daily_real 转换生成
  source_node: stock_daily_real
  target:
    database: starlight
    table: ad_market_kline_daily
    engine: Memory
    partition_by: []
    order_by:
      - code
      - trade_time
    version_field: ""
  fields:
    - code
    - trade_time
    - open
    - high
    - low
    - close
    - pre_close
    - volume
    - amount
    - open_adj
    - high_adj
    - low_adj
    - close_adj
    - is_st
    - is_suspended
    - is_wd_sec
    - is_xr_sec
    - is_kcb
    - is_cyb
    - is_bjs
    - high_limited
    - low_limited
    - market_cap
    - float_market_cap
    - turnover_rate
    - security_name
    - security_type
  key_fields:
    - code
    - trade_time
  status: enabled
```

### 3.2 `graph_bundle`

这是宽表所依赖的**最小图谱快照**，不是全量图谱。

主要用途：

1. 辅助排查
2. 必要时回放字段来源关系

但同步程序第一版不建议优先依赖它来推导全部逻辑。

### 3.3 `materialization_bundle`

这是当前最关键的部分。

它包含：

1. `query_intent`
2. `query_plan`
3. `base_context`
4. `preview_sql`

同步程序第一版建议优先消费：

1. `wide_table`
2. `materialization_bundle`

而不是自己重新从 `graph_bundle` 全量推导。

### 3.4 `export_meta`

用于记录导出时间和格式版本。

---

## 4. 同步程序建议优先消费哪些字段

### 必须消费

#### `wide_table.target`

决定目标表信息：

1. `database`
2. `table`
3. `engine`
4. `partition_by`
5. `order_by`
6. `version_field`

#### `wide_table.fields`

宽表最终需要输出哪些字段。

#### `wide_table.key_fields`

宽表主键。

#### `materialization_bundle.base_context`

至少需要：

1. `base_node`
2. `base_table`
3. `entity_keys`
4. `time_key`
5. `grain`
6. `base_filters`

#### `materialization_bundle.query_plan`

这是实现同步宽表最关键的执行上下文。

重点字段：

1. `field_bindings`
2. `resolved_fields`
3. `derived_fields`
4. `steps`
5. `where`
6. `time_range`
7. `safety`

#### `materialization_bundle.preview_sql`

当前建议作为：

1. 调试辅助
2. 日志输出

不建议第一版直接把 `preview_sql` 当最终执行 SQL 原样跑，因为后续同步程序通常还要加：

1. 增量条件
2. 分区条件
3. 插入语句
4. 重刷逻辑

---

## 5. 同步程序第一版实现建议

## 5.1 扫描目录

建议实现一个扫描器：

1. 读取 `wide_table_specs/*.yaml`
2. 每个文件解析成一个宽表任务对象

## 5.2 计算配置签名

建议为每个宽表文件计算两个签名：

### `wide_table_signature`

基于：

1. `target`
2. `fields`
3. `key_fields`
4. `partition_by`
5. `order_by`
6. `version_field`

### `plan_signature`

基于：

1. `materialization_bundle.query_plan`
2. `materialization_bundle.base_context`

理由：

即使目标表定义没变，只要字段绑定、join、派生公式变化，也应该视为需要重建或重刷。

---

## 5.3 目标表存在性检查

对于每个宽表任务，先检查：

1. `target.database`
2. `target.table`

### 如果不存在

动作：

1. 生成建表 SQL
2. 创建目标表
3. 执行首轮同步

### 如果存在

继续做签名比对。

---

## 5.4 签名比对策略

### 情况 1：表存在，签名未变

动作：

1. 普通同步

### 情况 2：表存在，但签名变化

动作建议：

1. 默认执行 `rebuild`

也就是：

1. 创建临时表
2. 用新配置同步
3. 校验成功后 rename 替换

不建议直接 destructive 修改原表。

### 当前已实现的执行语义

上面是理想设计。

当前已经接通的同步实现里，真实执行语义进一步收敛成：

1. `create_and_sync`
2. `sync`
3. `rebuild`

这三种动作在真正落库前都会先检查目标表是否存在。

当前真实执行流程：

1. 查询 `system.tables` 检查目标表
2. 如果目标表已存在，先执行 `DROP TABLE IF EXISTS target`
3. 重新 `CREATE TABLE`
4. 再执行整表 `INSERT INTO target SELECT ...`

当前限制：

1. 当前是删表重建语义
2. 还不是增量同步语义
3. 也不是 rename-swap 临时表切换语义
4. `action` 名称继续保留，用于表达规划层结果和状态表记录

所以当前应该把同步宽表理解成：

1. 已支持真实执行
2. 但执行方式是“全量重建”
3. 更适合当前的 Memory 宽表使用模式

---

## 6. 建表建议

同步程序需要根据 `wide_table.target.engine` 构造建表 SQL。

### 6.1 `Memory`

建议用于：

1. 小规模
2. 临时
3. 快速验证

### 6.2 `ReplacingMergeTree`

建议用于：

1. 正式宽表
2. 需要重刷
3. 需要版本替换

如果引擎是 `ReplacingMergeTree`：

1. 优先使用 `version_field`
2. 如果没有 `version_field`，同步程序可以：
   - 明确报错
   - 或 fallback 到无版本的 ReplacingMergeTree

建议第一版直接报错更稳。

---

## 7. 宽表 SQL 的实现建议

这里有两种策略：

### 策略 A：直接消费 `query_plan`

最推荐。

同步程序自己根据：

1. `base_context`
2. `query_plan.steps`
3. `query_plan.resolved_fields`
4. `query_plan.derived_fields`

构建最终物化查询。

优点：

1. 结构化
2. 可插入增量条件
3. 可控

### 策略 B：先用 `preview_sql` 落地

适合快速接通，但只适合第一版验证。

如果使用这个方式：

1. 同步程序要在外层包：
   - `CREATE TABLE AS`
   - 或 `INSERT INTO ... SELECT ...`

2. 后续再逐步切换到结构化 plan 消费

我的建议：

**第一版可以先用 `preview_sql` 验证链路，正式版切到 `query_plan`。**

---

## 8. 增量 / 重刷建议

### 第一版

可以先做：

1. 全量重刷

也就是：

1. truncate 目标表（或临时表重建）
2. 重新写入

### 后续

如果你要支持增量，同步程序应该基于：

1. `base_context.time_key`
2. 最新目标表时间
3. 源表最新时间

做时间窗口增量。

---

## 9. 状态记录建议

建议 Sync 项目维护一张状态表：

`wide_table_sync_state`

字段建议：

1. `wide_table_id`
2. `wide_table_name`
3. `source_node`
4. `target_database`
5. `target_table`
6. `wide_table_signature`
7. `plan_signature`
8. `last_status`
9. `last_action`
10. `last_message`
11. `last_started_at`
12. `last_finished_at`
13. `updated_at`

建议状态值：

1. `pending`
2. `creating`
3. `syncing`
4. `rebuilding`
5. `success`
6. `failed`

---

## 10. 当前最小必须做的实现项

同步程序第一版至少要完成：

1. 扫描 `wide_table_specs/*.yaml`
2. 解析 `wide_table`
3. 解析 `materialization_bundle`
4. 检查目标表是否存在
5. 检查签名是否变化
6. 创建表
7. 普通同步
8. 重建表
9. 记录状态

---

## 11. AIQuantBase 当前已经提供什么

当前 AIQuantBase 已经提供：

1. 宽表节点配置管理
2. 宽表节点 YAML 导出
3. 导出内容包含：
   - `wide_table`
   - `graph_bundle`
   - `materialization_bundle`
   - `export_meta`

也就是说：

**同步程序现在不需要自己重新发明一套宽表配置协议。**

只需要实现：

1. 读取
2. 校验
3. 执行

---

## 12. 推荐实施顺序

### P0

1. 支持扫描 YAML
2. 支持创建表
3. 支持全量同步
4. 支持状态记录

### P1

1. 支持签名变更检测
2. 支持自动重建

### P2

1. 支持增量同步
2. 支持按时间窗口重刷

---

## 13. 一句话结论

同步程序实现宽表同步时，建议：

**优先消费 AIQuantBase 导出的 `wide_table + materialization_bundle`，而不是只靠宽表定义或者自己重新推图谱。**

这样实现成本最低，也最不容易和 AIQuantBase 的字段解析逻辑偏离。

补充当前状态：

1. 这条链路已经支持真实 ClickHouse 执行
2. 当前执行语义是“目标表存在则先删表，再重建并整表重刷”
3. 如果继续推进，下一步重点应放在增量同步和非 destructive rebuild
