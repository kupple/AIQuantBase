# Wide Table Sync File Handoff Design

## 目标

这份文档定义宽表节点和同步程序之间的第一版集成方式：

**不走 API 下发，而是由 AIQuantBase 把宽表同步配置导出成 YAML 文件，放到一个约定目录里，由同步程序扫描这个目录并读取配置执行同步。**

这个方案的核心目标：

1. AIQuantBase 负责设计宽表节点
2. AIQuantBase 负责导出宽表物化配置
3. Sync 项目负责扫描目录、读取 YAML、执行建表/重建/同步
4. 两个项目先通过“文件协议”对接，不先做 HTTP 编排

---

## 1. 一句话结论

第一版不要先做：

1. `POST /api/wide-table-jobs/run`
2. 前端点击后直接请求 Sync API

而是改成：

1. AIQuantBase 导出 YAML 到约定目录
2. Sync 项目扫描该目录
3. 发现新的或变更的配置文件后执行同步

也就是说：

**宽表同步能力先通过“目录 + YAML 文件协议”实现。**

---

## 2. 为什么先用文件协议

对你当前阶段来说，文件协议比 API 更合适。

原因：

1. Sync 项目本来就是独立运行的
2. 宽表节点本质上是“同步任务配置”
3. 目录扫描实现成本低
4. 更容易调试
5. 不需要先定义复杂的跨项目 API 状态机

这个模式更像：

1. AIQuantBase 是设计器
2. Sync 项目是执行器

---

## 3. 功能边界

### AIQuantBase 负责

1. 宽表节点设计
2. 宽表节点配置持久化
3. 宽表节点配置导出为 YAML
4. 导出到约定目录
5. 展示该宽表节点是否已经导出

### Sync 项目负责

1. 扫描目录
2. 读取宽表 YAML
3. 检查目标表是否存在
4. 检查配置是否变更
5. 执行：
   - 创建表
   - 普通同步
   - 重建
   - 重刷
6. 记录执行状态

---

## 4. 目录协议

建议定义一个固定目录，例如：

```text
sync_project/
  wide_table_specs/
```

AIQuantBase 负责往这里写文件。

建议每个宽表节点一个文件：

```text
wide_table_specs/
  stock_daily_real.yaml
  etf_daily_real.yaml
  industry_daily_real.yaml
```

文件名建议和宽表节点名一致。

---

## 5. YAML 文件内容

单个文件建议结构：

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
  status: disabled

graph_bundle:
  graph_path: config/graph.yaml
  fields_path: config/fields.yaml
  scope:
    source_node: stock_daily_real
    selected_fields:
      - code
      - trade_time
      - open
      - high
      - low
      - close
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
  snapshot:
    nodes: []
    edges: []
    fields: []

export_meta:
  exported_at: 2026-04-21T22:30:00
  exported_by: aiquantbase
  format_version: 1
```

---

## 6. 为什么还是要带图谱

即使用文件协议，还是不能只放宽表定义。

原因：

1. `fields` 只告诉 Sync 项目“要哪些字段”
2. 但没有告诉它：
   - 字段来自哪张表
   - join 怎么做
   - 时间关系是什么
   - 派生公式是什么

所以 YAML 里仍然需要带：

1. `wide_table`
2. `graph_bundle`

也就是：

**宽表定义 + 图谱快照**

只是传输方式从 HTTP body 改成了 YAML 文件。

---

## 7. Sync 项目怎么处理

### 7.1 扫描目录

Sync 项目启动时或定时执行时：

1. 扫描 `wide_table_specs/`
2. 找到所有 `*.yaml`
3. 逐个读取

### 7.2 对每个 YAML 做解析

1. 读取 `wide_table`
2. 读取 `graph_bundle`
3. 恢复当前宽表所需的最小图谱上下文
4. 生成物化 SQL / 同步计划

### 7.3 检查目标表

如果目标表不存在：

1. 创建表
2. 执行首轮同步

### 7.4 检查配置是否变化

建议 Sync 项目为每个 YAML 生成两个签名：

1. `wide_table_signature`
2. `graph_signature`

如果签名变化，说明：

1. 宽表定义变了
2. 或图谱关系变了

都应视为配置已变更。

---

## 8. 配置签名建议

### 8.1 宽表定义签名

```text
sha256(
  source_node +
  target.database +
  target.table +
  engine +
  sorted(fields) +
  sorted(key_fields) +
  partition_by +
  order_by +
  version_field
)
```

### 8.2 图谱签名

```text
sha256(
  snapshot.nodes +
  snapshot.edges +
  snapshot.fields +
  scope.source_node +
  scope.selected_fields
)
```

---

## 9. 配置变化后的处理策略

### 9.1 表不存在

动作：

1. 创建表
2. 首轮同步

### 9.2 表存在且签名未变

动作：

1. 普通同步

### 9.3 表存在但签名已变

动作建议：

1. 默认进入 `rebuild`
2. 使用临时表安全重建
3. 完成后 rename 替换原表

---

## 10. 状态回写建议

虽然不走 API 下发，但仍建议 Sync 项目把状态回写到统一位置。

建议维护一张状态表：

`wide_table_sync_state`

字段建议：

1. `wide_table_id`
2. `wide_table_name`
3. `source_node`
4. `target_database`
5. `target_table`
6. `wide_table_signature`
7. `graph_signature`
8. `last_status`
9. `last_action`
10. `last_message`
11. `last_started_at`
12. `last_finished_at`
13. `updated_at`

这样 AIQuantBase 前端后面仍然能显示：

1. 是否已同步
2. 是否配置已变更
3. 最近一次执行结果

---

## 11. AIQuantBase 前端需要做什么

### 第一版

建议只做：

1. `导出同步配置`
2. `覆盖导出到目录`

而不是立刻做：

1. `同步表` 按钮直接触发 Sync

### 第二版

如果你后面希望在前端一键触发，也可以做成：

1. 先导出 YAML
2. 再调用 Sync 项目的“立即扫描”接口

但第一版没必要。

---

## 12. 推荐的前端交互

宽表节点详情里建议有两个按钮：

1. `导出 YAML`
2. `导出到同步目录`

### `导出 YAML`

作用：

1. 仅预览
2. 给人检查配置

### `导出到同步目录`

作用：

1. 直接把当前宽表配置写入 Sync 项目约定目录
2. 由 Sync 项目后续扫描并执行

---

## 13. 第一版建议范围

### 必做

1. AIQuantBase 可以导出完整 YAML
2. YAML 包含：
   - `wide_table`
   - `graph_bundle`
   - `export_meta`
3. AIQuantBase 可以把 YAML 写到约定目录
4. Sync 项目可以扫描目录
5. Sync 项目根据 YAML 创建 / 重建 / 同步

### 暂缓

1. API 下发
2. 前端直接一键触发 Sync 执行
3. 复杂审批流
4. 多版本管理

---

## 14. 一句话结论

宽表节点和同步程序的第一版对接，最适合用：

**目录 + YAML 文件协议**

而不是先做 API。

也就是：

1. AIQuantBase 设计宽表节点
2. AIQuantBase 导出 YAML 到同步目录
3. Sync 项目扫描目录并读取配置
4. Sync 项目执行创建 / 重建 / 同步

这样实现最简单、最稳定，也最符合你当前项目阶段。*** End Patch

