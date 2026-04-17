# AIQuantBase Usage Guide

## 1. 这个项目现在能做什么

AIQuantBase 当前已经具备：

1. 图谱驱动的 Query Intent 查询规划
2. 基于标准字段协议的 SQL 生成
3. 面向上层应用的 `ApplicationRuntime`
4. 中文字段说明、时间语义、路径约束、lookahead 第一阶段控制
5. Nuxt 工作台
6. 纯字段直连图谱协议

## 2. 默认配置位置

当前正式默认配置都在 `config/`：

1. `config/graph.yaml`
2. `config/fields.yaml`
3. `config/runtime.local.yaml`

## 3. 上层应用怎么用

### 推荐入口

```python
from aiquantbase import ApplicationRuntime

runtime = ApplicationRuntime.from_defaults()
```

### 推荐调用方式

1. `resolve_symbols(symbols)`
2. `get_supported_fields(...)`
3. `validate_query_request(...)`
4. `query_daily(...)` / `query_minute(...)`
5. `build_intent_from_requirement(...)`
6. `execute_requirement(...)`

## 4. 底层能力怎么用

如果需要直接调图谱底层能力，再用：

```python
from aiquantbase import GraphRuntime

runtime = GraphRuntime.from_defaults()
```

适合：

1. 调试图谱
2. 查看元数据
3. 渲染底层 Query Intent
4. 执行原生 SQL

如果只想先看当前协议摘要，推荐优先调用：

```python
runtime.get_protocol_summary()
```

## 5. 网页怎么启动

当前工作台默认是前后端两个开发进程：

### 启动 Python backend

```bash
PYTHONPATH=src /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m aiquantbase.cli studio --host 127.0.0.1 --port 8011
```

### 启动 Nuxt frontend

```bash
cd studio
npm run dev
```

常用页面：

1. `http://localhost:3000/database`
2. `http://localhost:3000/query`
3. `http://localhost:3000/settings`

## 6. 当前推荐主入口节点

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
11. `etf_daily_real`
12. `index_daily_real`
13. `etf_minute_real`
14. `index_minute_real`

## 7. 文档导航

1. [docs/application-api-usage.md](/Users/zhao/Desktop/git/AIQuantBase/docs/application-api-usage.md)
2. [docs/application-runtime-examples.md](/Users/zhao/Desktop/git/AIQuantBase/docs/application-runtime-examples.md)
3. [docs/application-runtime-api-reference.md](/Users/zhao/Desktop/git/AIQuantBase/docs/application-runtime-api-reference.md)
4. [docs/application-runtime-quickstart.md](/Users/zhao/Desktop/git/AIQuantBase/docs/application-runtime-quickstart.md)
5. [docs/alphablocks-integration-example.md](/Users/zhao/Desktop/git/AIQuantBase/docs/alphablocks-integration-example.md)
6. [docs/python-sdk-usage.md](/Users/zhao/Desktop/git/AIQuantBase/docs/python-sdk-usage.md)
7. [docs/query-intent-templates.md](/Users/zhao/Desktop/git/AIQuantBase/docs/query-intent-templates.md)
8. [docs/public-entry-nodes.md](/Users/zhao/Desktop/git/AIQuantBase/docs/public-entry-nodes.md)
9. [docs/trusted-query-boundary.md](/Users/zhao/Desktop/git/AIQuantBase/docs/trusted-query-boundary.md)
10. [docs/release-notes-v1.md](/Users/zhao/Desktop/git/AIQuantBase/docs/release-notes-v1.md)
11. [docs/migration-notes.md](/Users/zhao/Desktop/git/AIQuantBase/docs/migration-notes.md)
12. [docs/go-live-checklist.md](/Users/zhao/Desktop/git/AIQuantBase/docs/go-live-checklist.md)
13. [docs/direct-field-runtime-protocol.md](/Users/zhao/Desktop/git/AIQuantBase/docs/direct-field-runtime-protocol.md)

## 8. 当前可信边界

当前系统可信的前提是：

1. 主入口明确
2. 标准字段明确
3. 图谱路径明确
4. SQL 可审计

当前不做：

1. 全自动选入口
2. 全自动跨多个业务主体拼接查询
3. 完全自动拆查询
