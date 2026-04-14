# AIQuantBase

AIQuantBase 是一个面向 A 股研究场景的图谱驱动查询中间件。

当前项目已经具备：

1. 图谱驱动的 Query Intent 查询规划
2. 标准字段协议与中文说明
3. 返回 `sql + DataFrame` 的 Python SDK
4. `lookahead_safe` 第一阶段控制
5. SSR 图谱管理网页

## 快速开始

### 1. 安装本地 SDK

```bash
/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m pip install -e .
```

### 2. Python 中使用

```python
from aiquantbase import GraphRuntime

runtime = GraphRuntime.from_defaults()

result = runtime.execute_intent({
    "from": "stock_daily_real",
    "select": ["code", "trade_time", "close"],
    "where": {
        "mode": "and",
        "items": [
            {"field": "code", "op": "=", "value": "000001.SZ"}
        ]
    },
    "time_range": {
        "field": "trade_time",
        "start": "2025-01-01 00:00:00",
        "end": "2025-01-31 23:59:59"
    },
    "page": 1,
    "page_size": 20,
    "safety": {
        "lookahead_safe": False,
        "strict_mode": True
    }
})

print(result["sql"])
print(result["df"])
```

### 3. 启动网页

```bash
PYTHONPATH=src /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m aiquantbase.cli studio --host 127.0.0.1 --port 8000
```

打开：

- [http://127.0.0.1:8000](http://127.0.0.1:8000)

## 默认配置

当前正式默认配置位于：

1. [config/graph.yaml](/Users/zhao/Desktop/git/AIQuantBase/config/graph.yaml)
2. [config/fields.yaml](/Users/zhao/Desktop/git/AIQuantBase/config/fields.yaml)
3. [config/runtime.local.yaml](/Users/zhao/Desktop/git/AIQuantBase/config/runtime.local.yaml)

## 当前推荐主入口节点

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

## 当前对外推荐函数

1. `GraphRuntime.from_defaults()`
2. `runtime.get_real_fields_json()`
3. `runtime.execute_intent(intent)`
4. `runtime.execute_sql(sql)`

## 文档导航

1. [docs/README-usage.md](/Users/zhao/Desktop/git/AIQuantBase/docs/README-usage.md)
2. [docs/python-sdk-usage.md](/Users/zhao/Desktop/git/AIQuantBase/docs/python-sdk-usage.md)
3. [docs/query-intent-templates.md](/Users/zhao/Desktop/git/AIQuantBase/docs/query-intent-templates.md)
4. [docs/public-entry-nodes.md](/Users/zhao/Desktop/git/AIQuantBase/docs/public-entry-nodes.md)
5. [docs/trusted-query-boundary.md](/Users/zhao/Desktop/git/AIQuantBase/docs/trusted-query-boundary.md)
6. [docs/final-graph-relationship.md](/Users/zhao/Desktop/git/AIQuantBase/docs/final-graph-relationship.md)
7. [docs/field-description-catalog.md](/Users/zhao/Desktop/git/AIQuantBase/docs/field-description-catalog.md)

## 当前可信边界

当前系统可信的前提是：

1. 主入口明确
2. 标准字段明确
3. 图谱路径明确
4. SQL 可审计

当前不做：

1. 全自动选入口
2. 全自动跨多个业务主体拼接查询
3. 完全自动拆查询

## 测试

```bash
PYTHONPATH=src /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m pytest -q
```
