# Python SDK Usage

## 目标

这份文档说明 AIQuantBase 在本地 Python 项目中的使用方式。

当前推荐的使用方式是：

1. 通过 Python SDK 引入
2. 使用正式默认配置 `config/graph.yaml` 与 `config/fields.yaml`
3. 通过 `Query Intent` 或原生 SQL 获取 `sql + df`

## 1. 安装方式

如果是本地项目直接引用，推荐使用 editable install：

```bash
/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m pip install -e /Users/zhao/Desktop/git/AIQuantBase
```

如果已经在当前仓库中开发，可以直接：

```bash
/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m pip install -e .
```

### 说明

1. 使用 `-e` 后，源码变更通常会直接生效
2. 如果修改了依赖，请重新执行一次安装
3. 已运行的 Python 进程一般需要重启才能拿到最新代码

## 2. 初始化

### 默认初始化

```python
from aiquantbase import GraphRuntime

runtime = GraphRuntime.from_defaults()
```

默认读取：

1. `config/graph.yaml`
2. `config/fields.yaml`
3. `config/runtime.local.yaml`

### 显式传路径初始化

```python
from aiquantbase import GraphRuntime

runtime = GraphRuntime(
    graph_path="/Users/zhao/Desktop/git/AIQuantBase/config/graph.yaml",
    fields_path="/Users/zhao/Desktop/git/AIQuantBase/config/fields.yaml",
    runtime_path="/Users/zhao/Desktop/git/AIQuantBase/config/runtime.local.yaml",
)
```

如果你的调用方不在项目根目录运行，建议显式传绝对路径。

## 3. 当前推荐暴露的主入口节点

当前对外默认主入口节点为：

1. `stock_daily_real`
2. `stock_minute_real`
3. `industry_daily_real`
4. `treasury_yield_real`
5. `fund_iopv_real`
6. `fund_share_real`
7. `etf_pcf_real`
8. `kzz_issuance_real`
9. `kzz_conv_real`
10. `margin_summary_real`

说明：

1. 这批主入口已经足够支撑大多数查询场景
2. 其他 `*_real` 节点仍保留在内部图谱中，但不建议默认暴露给外部模块

## 4. 获取字段目录

### 全量元数据目录

```python
catalog = runtime.get_metadata_catalog()
```

返回：

1. `nodes`
2. `edges`
3. `fields`

字段项里会包含：

1. `standard_field`
2. `description_zh`
3. `field_role`
4. `path_domain`
5. `path_group`
6. `time_semantics`
7. `lookahead_category`

### 获取 real 节点字段 JSON

```python
result = runtime.get_real_fields_json()
```

返回：

```python
{
    "code": 0,
    "message": "success",
    "items": [
        {
            "name": "stock_daily_real",
            "table": "starlight.ad_market_kline_daily",
            "grain": "daily",
            "description_zh": "股票日线行情主表",
            "is_ai_entry": True,
            "fields": [
                {"field_name": "code", "field_label_zh": "代码"},
                {"field_name": "trade_time", "field_label_zh": "交易时间"},
                {"field_name": "close", "field_label_zh": "收盘价"}
            ]
        }
    ]
}
```

这个接口适合给外部模块先做字段选择。

## 5. 执行 Query Intent

### 渲染 SQL

```python
sql = runtime.render_intent(intent)
```

### 执行 Query Intent

```python
result = runtime.execute_intent(intent)
```

返回结构：

```python
{
    "code": 0,
    "message": "success",
    "sql": "...",
    "df": DataFrame(...),
}
```

### 最小示例

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
        "start": "2025-01-01 00:00:00",
        "end": "2025-12-31 23:59:59"
    },
    "page": 1,
    "page_size": 20,
    "safety": {
        "lookahead_safe": False,
        "strict_mode": True
    }
}

result = runtime.execute_intent(intent)
```

## 6. 执行原生 SQL

```python
result = runtime.execute_sql("SELECT 1 AS x")
```

返回结构和 `execute_intent()` 一致：

```python
{
    "code": 0,
    "message": "success",
    "sql": "SELECT 1 AS x\nFORMAT JSON",
    "df": DataFrame(...),
}
```

## 7. 错误处理

当前最小错误码规则：

1. `code = 0`
   成功

2. `code = 1`
   执行失败
   `message` 中会带异常信息

### 示例

```python
if result["code"] != 0:
    print(result["message"])
```

## 8. 当前 lookahead 规则

当前 `lookahead_safe` 第一阶段只对公告/事件类字段生效。

### 目前不影响：

1. `close`
2. `high`
3. `low`
4. `volume`
5. `amount`
6. `market_cap`
7. `turnover_rate`

### 当前主要影响：

1. 财报公告字段
2. 分红配股公告字段
3. 股东公告字段
4. 可转债公告/条款字段

## 9. 最佳实践

### 推荐

1. 先用 `get_real_fields_json()` 给外部模块选字段
2. 再用 `execute_intent()` 执行查询
3. 保留返回的 `sql` 做审计和排查

### 不推荐

1. 外部模块自己记底层表名
2. 外部模块自己拼 join
3. 外部模块直接推测原始字段名

## 10. 一句话用法

```python
from aiquantbase import GraphRuntime

runtime = GraphRuntime.from_defaults()
result = runtime.execute_intent(intent)
sql = result["sql"]
df = result["df"]
```

## 11. 多个代码过滤

如果要查询多个股票代码，不要写多个 `=`，推荐直接使用：

```python
intent = {
    "from": "stock_daily_real",
    "select": ["code", "trade_time", "close"],
    "where": {
        "mode": "and",
        "items": [
            {
                "field": "code",
                "op": "in",
                "value": ["000001.SZ", "000002.SZ", "000004.SZ"]
            }
        ]
    },
    "time_range": {
        "field": "trade_time",
        "start": "2024-01-01",
        "end": "2024-01-31"
    },
    "page": 1,
    "page_size": 20,
    "safety": {
        "lookahead_safe": False,
        "strict_mode": True
    }
}
```

这会渲染成：

```sql
code IN ('000001.SZ', '000002.SZ', '000004.SZ')
```


## 12. 常用过滤写法

### 单值等于

```python
{"field": "code", "op": "=", "value": "000001.SZ"}
```

### 多值 in

```python
{"field": "code", "op": "in", "value": ["000001.SZ", "000002.SZ", "000004.SZ"]}
```

### 多值 not_in

```python
{"field": "code", "op": "not_in", "value": ["000001.SZ", "000002.SZ"]}
```

### 区间 between

```python
{"field": "market_cap", "op": "between", "value": [3000000, 6000000]}
```

### 判空 is_null

```python
{"field": "kzz_forced_conv_reason", "op": "is_null", "value": None}
```

### 非空 is_not_null

```python
{"field": "industry_name", "op": "is_not_null", "value": None}
```

### 模糊匹配 contains

```python
{"field": "security_name", "op": "contains", "value": "银行"}
```

### like

```python
{"field": "symbol", "op": "like", "value": "000%"}
```

### 不区分大小写 ilike

```python
{"field": "comp_name_eng", "op": "ilike", "value": "%bank%"}
```
