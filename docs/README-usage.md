# AIQuantBase Usage Guide

## 1. 这个项目现在能做什么

AIQuantBase 当前已经具备：

1. 图谱驱动的 Query Intent 查询规划
2. 基于标准字段协议的 SQL 生成
3. 返回 `sql + DataFrame` 的 Python SDK
4. 中文字段说明、时间语义、路径约束、lookahead 第一阶段控制
5. SSR 网页图谱管理台

## 2. 默认配置位置

当前正式默认配置都在 `config/`：

1. `config/graph.yaml`
2. `config/fields.yaml`
3. `config/runtime.local.yaml`

默认情况下：

1. Python SDK 读这里
2. Flask 服务端也读这里
3. 网页工作台默认也读这里

## 3. Python SDK 怎么用

### 初始化

```python
from aiquantbase import GraphRuntime

runtime = GraphRuntime.from_defaults()
```

### 获取字段清单

```python
result = runtime.get_real_fields_json()
```

### 执行 Query Intent

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

### 执行原生 SQL

```python
result = runtime.execute_sql("SELECT ...")
```

## 4. 当前建议默认开放的主入口节点

当前建议对外默认开放：

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

## 5. 网页怎么启动

```bash
PYTHONPATH=src /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m aiquantbase.cli studio --host 127.0.0.1 --port 8000
```

打开：

1. `http://127.0.0.1:8000/`
2. `http://127.0.0.1:8000/nodes`
3. `http://127.0.0.1:8000/edges`
4. `http://127.0.0.1:8000/graph/view`
5. `http://127.0.0.1:8000/llm-query`

## 6. 当前可信边界

当前系统可信的前提是：

1. 主入口明确
2. 标准字段明确
3. 图谱路径明确
4. SQL 可审计

当前不做：

1. 全自动选入口
2. 全自动跨多个业务主体拼接查询
3. 完全自动拆查询

## 7. lookahead 当前规则

当前 `lookahead_safe = true` 第一阶段只对：

1. 公告类字段
2. 事件类字段
3. 条款生效类字段

生效。

当前明确不影响：

1. `close`
2. `high`
3. `low`
4. `volume`
5. `amount`
6. `market_cap`
7. `turnover_rate`

## 8. 推荐先看的文档

1. [python-sdk-usage.md](/Users/zhao/Desktop/git/AIQuantBase/docs/python-sdk-usage.md)
2. [query-intent-templates.md](/Users/zhao/Desktop/git/AIQuantBase/docs/query-intent-templates.md)
3. [public-entry-nodes.md](/Users/zhao/Desktop/git/AIQuantBase/docs/public-entry-nodes.md)
4. [trusted-query-boundary.md](/Users/zhao/Desktop/git/AIQuantBase/docs/trusted-query-boundary.md)
5. [field-description-catalog.md](/Users/zhao/Desktop/git/AIQuantBase/docs/field-description-catalog.md)

## 9. 一句话怎么理解现在的项目

当前阶段，AIQuantBase 已经可以作为：

1. 图谱约束查询中间件
2. Python 查询 SDK
3. 图谱管理网页后台

稳定使用。
