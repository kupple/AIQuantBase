# Field Protocol For Integrations

## 目标

这份文档是写给“其他模块如何接入 AIQuantBase”的。

核心原则只有一句：

1. 不要直接使用底层表名
2. 不要直接依赖图谱内部节点关系
3. 只通过标准字段目录与 Query Intent 协议接入

## 1. 其他模块应该怎么接

推荐接入方式：

```python
from aiquantbase import GraphRuntime

runtime = GraphRuntime.from_defaults()
```

### 先拿字段目录

```python
catalog = runtime.get_metadata_catalog()
```

这个 `catalog` 会返回：

1. `nodes`
2. `edges`
3. `fields`

其中其他模块最应该使用的是：

1. `catalog["fields"]`

## 2. 其他模块应该认什么，不该认什么

### 应该认

1. `standard_field`
2. `description_zh`
3. `field_role`
4. `path_domain`
5. `path_group`
6. `time_semantics`
7. `applies_to_grain`

### 不应该认

1. 底层数据库表名
2. `source_field` 作为查询入口
3. 图谱内部 join 细节
4. ClickHouse SQL 细节

也就是说，对外协议层应当只使用：

1. `standard_field`
2. `Query Intent`

## 3. metadata catalog 长什么样

示例：

```python
catalog = runtime.get_metadata_catalog()
```

字段项大致长这样：

```python
{
    "standard_field": "market_cap",
    "source_node": None,
    "source_field": None,
    "field_role": "derived_field",
    "resolver_type": "derived",
    "applies_to_grain": "daily",
    "path_domain": None,
    "path_group": None,
    "via_node": None,
    "time_semantics": None,
    "description_zh": "总市值",
    "depends_on": ["close", "tot_share"],
    "formula": "{close} * ({tot_share} * 10000)",
}
```

## 4. 其他模块如何发查询

其他模块不要传 SQL。

统一传 `Query Intent`：

```python
intent = {
    "from": "stock_daily_real",
    "select": ["close", "market_cap", "industry_name"],
    "where": {
        "mode": "and",
        "items": [
            {"field": "code", "op": "=", "value": "000001.SZ"}
        ]
    },
    "time_range": {
        "field": "trade_time",
        "start": "2026-01-01 00:00:00",
        "end": "2026-01-31 23:59:59"
    },
    "page": 1,
    "page_size": 20,
    "safety": {
        "lookahead_safe": True,
        "strict_mode": True
    }
}
```

然后执行：

```python
result = runtime.execute_intent(intent)
```

返回：

```python
{
    "code": 0,
    "message": "success",
    "sql": "...",
    "df": DataFrame(...),
}
```

## 5. 其他模块如何选字段

推荐顺序：

1. 先看 `description_zh`
2. 再看 `field_role`
3. 再看 `path_domain/path_group`
4. 如果是日期字段，再看 `time_semantics`

### 例子

如果其他模块想找“公告日”，不能直接乱猜 `ann_date`。

应该从字段目录里区分：

1. `ann_date`
   `financial_announce_date`

2. `dividend_ann_date`
   `corporate_action_announce_date`

3. `fund_share_ann_date`
   `fund_announce_date`

4. `kzz_conv_rule_ann_date`
   `kzz_announce_date`

## 6. 为什么不能直接用表名

因为图谱的价值就在于：

1. 把多表关系隐藏在中间层里
2. 把可查询字段标准化成统一协议
3. 把路径冲突、时间语义、派生逻辑统一管理

如果其他模块直接用表名：

1. 会绕过图谱
2. 会绕过路径约束
3. 会绕过时间语义层
4. 会让 SQL 生成重新变得不稳定

## 7. 最佳实践

### 最推荐

1. `runtime.get_metadata_catalog()` 拉字段目录
2. 其他模块只保存 `standard_field`
3. 查询时只构造 `Query Intent`
4. 通过 `runtime.execute_intent()` 获取 `sql + df`

### 不推荐

1. 其他模块自己记 `ad_xxx` 表名
2. 其他模块自己记 join 规则
3. 其他模块直接拼 SQL

## 8. 当前可以直接给其他模块的接口

### Python SDK

```python
from aiquantbase import GraphRuntime

runtime = GraphRuntime.from_defaults()
```

高频方法：

1. `runtime.get_metadata_catalog()`
2. `runtime.get_real_fields_json()`
3. `runtime.render_intent(intent)`
4. `runtime.execute_intent(intent)`
5. `runtime.execute_sql(sql)`

## 9. 一句话接入原则

其他模块只认：

1. `standard_field`
2. `description_zh`
3. `Query Intent`

不要认：

1. 表名
2. 原字段名
3. 底层 join 关系

## 默认图谱配置位置

当前系统的正式默认图谱配置已经收口到 `config/` 目录：

1. `config/graph.yaml`
2. `config/fields.yaml`

默认行为：

1. 不传路径时，SDK、服务端、Studio 都默认读取这两份文件
2. 如果显式传入 `graph_path` / `fields_path`，则优先读取传入路径

这意味着：

1. `config/` 下的是“正式默认配置”
2. `examples/` 下的是“示例与历史演化文件”

## 10. 补充入口

### 直接执行原生 SQL

```python
result = runtime.execute_sql("SELECT ...")
```

返回：

```python
{
    "code": 0,
    "message": "success",
    "sql": "...",
    "df": DataFrame(...),
}
```

### 获取 real 节点字段 JSON

```python
result = runtime.get_real_fields_json()
```

返回：

1. `code`
2. `message`
3. `items`

其中 `items` 会按 `*_real` 节点分组返回字段，每个字段只带英文名 `field_name` 和中文名 `field_label_zh`。
