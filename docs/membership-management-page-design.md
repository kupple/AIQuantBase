# Membership Management Page Design

> 状态：第二阶段设计。
>
> 当前优先级不是先做页面，而是先做：
>
> 1. membership 统一数据模型
> 2. membership discovery / enumeration API
> 3. `query_daily(..., memberships=...)` 配置式过滤
>
> 这份文档保留，作为后续前端阶段设计输入。

## 目标

这份文档定义 `AIQuantBase` 后续需要新增的：

**股票归属关系管理页**

这个页面不是为了单独解决：

1. 中小板
2. 行业
3. 指数

而是为了统一管理下面这些归属能力：

1. 板块
2. 行业
3. 指数
4. 概念
5. 主题
6. 标签 / 自定义股票池

核心原则：

1. 所有归属统一按 membership 管理
2. 页面既是维护入口，也是能力发现入口
3. `query_daily(...)` 等运行时接口最终要消费这层能力

在当前阶段，这份文档只描述未来页面应该怎么接，不代表当前立刻实现。

---

## 1. 页面定位

建议页面名称：

1. `股票归属关系管理`
2. `Security Membership Management`

这个页面的职责不是“看某张表”，而是：

1. 管理归属域
2. 管理分类体系
3. 管理成员
4. 管理股票与成员的时间关系
5. 提供给运行时做统一发现和过滤

---

## 2. 页面要解决的问题

如果没有这个页面，后续会持续遇到这些问题：

1. 哪些板块 / 行业 / 指数已经接入，没人说得清
2. 新增一个指数或概念要改代码或 YAML
3. AlphaBlocks 不知道系统现在到底支持什么
4. `query_daily(...)` 无法做真正配置式过滤

所以页面必须承担：

1. 人工维护
2. 结构化发现
3. 对外统一接口

---

## 3. 统一模型

页面背后的数据模型建议统一成 membership 关系。

最小字段建议：

1. `security_code`
2. `security_type`
3. `domain`
4. `taxonomy`
5. `member_code`
6. `member_name`
7. `effective_from`
8. `effective_to`
9. `source_system`
10. `status`
11. `extra_payload`

说明：

### `domain`

表示大类：

1. `board`
2. `industry`
3. `index`
4. `concept`
5. `theme`
6. `tag`

### `taxonomy`

表示某大类下的体系：

1. `exchange_board`
2. `sw2021_l1`
3. `sw2021_l2`
4. `citic_l1`
5. `csi_index`
6. `concept_internal`

### `member_code`

表示体系中的具体成员：

1. `sme`
2. `gem`
3. `star`
4. `000300.SH`
5. `electronics`

---

## 4. 页面模块划分

建议拆成 4 个主要区域。

## 4.1 Domain / Taxonomy 管理

作用：

1. 管理有哪些归属域
2. 每个归属域下有哪些 taxonomy

表格建议字段：

1. `domain`
2. `taxonomy`
3. `display_name`
4. `description`
5. `status`
6. `updated_at`

页面动作：

1. 新增 taxonomy
2. 启停 taxonomy
3. 编辑展示名和描述

---

## 4.2 Member 管理

作用：

1. 管理某个 taxonomy 下有哪些 member

表格建议字段：

1. `domain`
2. `taxonomy`
3. `member_code`
4. `member_name`
5. `sort_order`
6. `status`
7. `extra_payload`

页面动作：

1. 新增 member
2. 编辑 member
3. 批量导入 member
4. 启停 member

典型示例：

### 板块

1. `domain=board`
2. `taxonomy=exchange_board`
3. `member_code=sme`
4. `member_name=中小板`

### 指数

1. `domain=index`
2. `taxonomy=csi_index`
3. `member_code=000300.SH`
4. `member_name=沪深300`

---

## 4.3 Membership 关系管理

作用：

1. 管理股票和 member 之间的归属关系
2. 管理时间生效区间

表格建议字段：

1. `security_code`
2. `security_type`
3. `domain`
4. `taxonomy`
5. `member_code`
6. `member_name`
7. `effective_from`
8. `effective_to`
9. `status`
10. `source_system`

页面动作：

1. 新增单条关系
2. 批量导入关系
3. 按股票查看历史关系
4. 按 member 查看当前成分
5. 按时间过滤关系

---

## 4.4 查询验证 / 调试区

作用：

让开发和数据同学能直接验证：

1. 某天某股票属于什么
2. 某个 taxonomy 当前有哪些 member
3. 某个过滤条件会筛出哪些股票

建议提供 3 个调试面板：

### A. 股票归属查询

输入：

1. 股票代码
2. 日期

输出：

1. 当日所有归属关系

### B. 成员反查

输入：

1. `domain`
2. `taxonomy`
3. `member_code`
4. 日期

输出：

1. 当前成员里的股票清单

### C. 过滤表达式验证

输入：

```yaml
include:
  - domain: board
    taxonomy: exchange_board
    member_code: sme
exclude:
  - domain: board
    taxonomy: exchange_board
    member_code: gem
```

输出：

1. 符合条件的股票数量
2. 样例股票

---

## 5. 后端接口建议

页面不应该直接读表，而应该有一层正式 API。

推荐最小接口：

## 5.1 Discovery API

### `GET /api/membership/domains`

返回当前支持的 domain 列表。

### `GET /api/membership/taxonomies`

支持：

1. 按 `domain` 过滤

### `GET /api/membership/members`

支持：

1. 按 `domain`
2. 按 `taxonomy`

### `GET /api/membership/relations`

支持：

1. 按 `security_code`
2. 按 `domain`
3. 按 `taxonomy`
4. 按 `member_code`
5. 按 `as_of_date`

---

## 5.2 Mutation API

### `POST /api/membership/taxonomies`

新增 taxonomy。

### `POST /api/membership/members`

新增 member。

### `POST /api/membership/relations`

新增归属关系。

### `POST /api/membership/import`

批量导入：

1. members
2. relations

### `PATCH /api/membership/relations`

更新生效时间 / 状态。

---

## 5.3 Runtime-facing API

这是最重要的。

页面不是终点，运行时也要用。

推荐：

### `query_membership(...)`

输入股票和日期，返回归属。

### `filter_symbols_by_membership(...)`

输入 membership 条件，返回股票集合。

### `query_daily(..., memberships=...)`

让 `query_daily` 直接支持 membership 配置式过滤。

---

## 6. 过滤协议建议

页面管理的数据，最终要让运行时能直接消费。

建议统一过滤格式：

```yaml
include:
  - domain: board
    taxonomy: exchange_board
    member_code: sme
exclude:
  - domain: board
    taxonomy: exchange_board
    member_code: gem
```

未来也可以自然扩成：

```yaml
include:
  - domain: industry
    taxonomy: sw2021_l1
    member_code: electronics
  - domain: index
    taxonomy: csi_index
    member_code: 000300.SH
```

这样：

1. `query_daily(...)` 参数不写死类型
2. 新增概念/主题时不改接口协议

---

## 7. 为什么页面必须支持时间维度

这点不能省。

原因：

1. 指数成分会变
2. 行业口径可能会变
3. 概念归属也可能有时间变化

所以页面里每条关系最好都显式有：

1. `effective_from`
2. `effective_to`

如果当前原始数据只有每日快照，也建议在展示和运行时抽象成这个语义。

---

## 8. 与 AlphaBlocks 的关系

这个页面做完后，AlphaBlocks 不应该再：

1. 自己猜中小板定义
2. 自己硬编码指数名单
3. 自己知道系统里支持哪些行业口径

AlphaBlocks 只需要：

1. 发现有哪些 domain/taxonomy/member
2. 构造 membership filter
3. 把 filter 交给 AIQuantBase

也就是说，这个页面和它背后的 API 是：

**归属能力的唯一真实来源**

---

## 9. 页面第一版建议范围

不建议一上来就把所有 UI 做复杂。

第一版建议只做：

### 必做

1. Taxonomy 列表
2. Member 列表
3. Relation 列表
4. 单条新增 / 编辑
5. 批量导入
6. 查询验证区

### 暂缓

1. 太复杂的可视化图谱
2. 复杂审批流
3. 多租户隔离

---

## 10. 页面完成后的成功标准

做完以后，应该能直接支持这些业务：

1. 只看中小板
2. 过滤创业板 / 科创板
3. 只看申万一级电子
4. 只看沪深300
5. 只看中证500
6. 以后新增概念时，不改 API 协议

---

## 11. 一句话结论

这个页面不是附属工具，而是：

**AIQuantBase 归属关系能力的正式管理入口和发现入口。**

做完它之后：

1. 维护归属信息会清晰很多
2. AlphaBlocks 才能真正走配置式过滤
3. 板块 / 行业 / 指数 / 概念才能统一接入  
