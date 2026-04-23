# AIQuantBase

AIQuantBase 是一个面向 A 股研究场景的图谱驱动查询中间件。

当前项目已经具备：

1. 图谱驱动的 Query Intent 查询规划
2. 标准字段协议与中文说明
3. 返回 `sql + DataFrame` 的 Python SDK
4. `lookahead_safe` 第一阶段控制
5. Nuxt 图谱管理工作台
6. 内置 `sync_data_system` 同步服务与宽表同步执行能力
7. `stock / etf / index` 逻辑入口分离，底层继续复用同一批行情物理表
8. 逻辑入口条件由 [config/graph.yaml](/Users/zhao/Desktop/git/AIQuantBase/config/graph.yaml) 配置驱动
9. 纯字段直连图谱协议

## 快速开始

### 1. 安装本地 SDK

```bash
/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m pip install -r requirements.txt
```

或：

```bash
/Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m pip install -e .
```

### 2. Python 中使用

```python
from aiquantbase import ApplicationRuntime

runtime = ApplicationRuntime.from_defaults()

result = runtime.query_daily(
    symbols=["000001.SZ"],
    fields=["close_adj", "market_cap", "industry_name"],
    start="2025-01-01 00:00:00",
    end="2025-01-31 23:59:59",
)

print(result["debug"]["sql"])
print(result["df"])
```

### 3. 启动工作台

先启动统一 Python backend：

```bash
PYTHONPATH=src /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m aiquantbase.cli studio --host 127.0.0.1 --port 8011 --debug
```

再启动 Nuxt frontend：

```bash
cd studio
npm run dev
```

打开：

- [http://localhost:3000/database](http://localhost:3000/database)
- [http://localhost:3000/query](http://localhost:3000/query)
- [http://localhost:3000/settings](http://localhost:3000/settings)
- [http://localhost:3000/sync](http://localhost:3000/sync)

当前 `studio` 的查询页和同步页都默认走同一个 Python backend，不再要求单独启动另一个同步 API 服务。

服务器使用 `pm2` 时，建议：

1. 后端直接运行 `python -m aiquantbase.cli studio --host 0.0.0.0 --port 8000`
2. 前端先执行 `npm run build`
3. 再用 `npm run preview -- --host 0.0.0.0 --port 3000`

当前根目录已提供 [ecosystem.config.js](/Users/zhao/Desktop/git/AIQuantBase/ecosystem.config.js)。

### 4. 仓库内置同步项目

同步能力已经直接并入当前仓库，默认同步项目根目录为：

`sync_data_system/`

其中包含：

1. `run_sync.py`
2. `service/`
3. `tests/`

同步相关配置已经统一收敛到外层：

`config/sync/`

其中包含：

1. `plans/run_sync*.toml`
2. `wide_table_specs/*.yaml`
3. [config/sync/README.md](/Users/zhao/Desktop/git/AIQuantBase/config/sync/README.md)

同步程序与主程序公用同一个运行配置文件：

`config/runtime.local.yaml`

其中：

1. `datasource` 作为统一 ClickHouse 连接配置
2. `sync.amazingdata` 保存 AmazingData 同步所需凭据
3. `sync.baostock` 保存 BaoStock 同步所需凭据
4. 运行说明见 [docs/sync-runbook.md](/Users/zhao/Desktop/git/AIQuantBase/docs/sync-runbook.md)

如需覆盖默认路径，可在启动 backend 时传：

```bash
PYTHONPATH=src /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m aiquantbase.cli studio --sync-project-root /path/to/sync_project
```

常用同步 CLI 也已经统一收进 `aiquantbase.cli`：

```bash
PYTHONPATH=src python3 -m aiquantbase.cli sync-list-configs
PYTHONPATH=src python3 -m aiquantbase.cli sync-list-tasks
PYTHONPATH=src python3 -m aiquantbase.cli sync-run-task daily_kline --codes 000001.SZ,600000.SH --begin-date 20240101
PYTHONPATH=src python3 -m aiquantbase.cli sync-list-jobs
```

## 默认配置

当前正式默认配置位于：

1. [config/graph.yaml](/Users/zhao/Desktop/git/AIQuantBase/config/graph.yaml)
2. [config/fields.yaml](/Users/zhao/Desktop/git/AIQuantBase/config/fields.yaml)
3. [config/runtime.local.yaml](/Users/zhao/Desktop/git/AIQuantBase/config/runtime.local.yaml)

## 当前推荐主入口节点

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

## 当前对外推荐函数

1. `ApplicationRuntime.from_defaults()`
2. `runtime.resolve_symbols(symbols)`
3. `runtime.get_supported_fields(...)`
4. `runtime.query_daily(...)`
5. `runtime.query_minute(...)`
6. `runtime.execute_requirement(data_requirement)`
7. `GraphRuntime.get_protocol_summary()`

## 文档导航

1. [docs/README-usage.md](/Users/zhao/Desktop/git/AIQuantBase/docs/README-usage.md)
2. [docs/python-sdk-usage.md](/Users/zhao/Desktop/git/AIQuantBase/docs/python-sdk-usage.md)
3. [docs/query-intent-templates.md](/Users/zhao/Desktop/git/AIQuantBase/docs/query-intent-templates.md)
4. [docs/public-entry-nodes.md](/Users/zhao/Desktop/git/AIQuantBase/docs/public-entry-nodes.md)
5. [docs/trusted-query-boundary.md](/Users/zhao/Desktop/git/AIQuantBase/docs/trusted-query-boundary.md)
6. [docs/final-graph-relationship.md](/Users/zhao/Desktop/git/AIQuantBase/docs/final-graph-relationship.md)
7. [docs/field-description-catalog.md](/Users/zhao/Desktop/git/AIQuantBase/docs/field-description-catalog.md)
8. [docs/application-runtime-examples.md](/Users/zhao/Desktop/git/AIQuantBase/docs/application-runtime-examples.md)
9. [docs/application-runtime-api-reference.md](/Users/zhao/Desktop/git/AIQuantBase/docs/application-runtime-api-reference.md)
10. [docs/alphablocks-integration-example.md](/Users/zhao/Desktop/git/AIQuantBase/docs/alphablocks-integration-example.md)
11. [docs/application-runtime-quickstart.md](/Users/zhao/Desktop/git/AIQuantBase/docs/application-runtime-quickstart.md)
12. [docs/direct-field-runtime-protocol.md](/Users/zhao/Desktop/git/AIQuantBase/docs/direct-field-runtime-protocol.md)
13. [docs/release-notes-v1.md](/Users/zhao/Desktop/git/AIQuantBase/docs/release-notes-v1.md)
14. [docs/migration-notes.md](/Users/zhao/Desktop/git/AIQuantBase/docs/migration-notes.md)
15. [CHANGELOG.md](/Users/zhao/Desktop/git/AIQuantBase/CHANGELOG.md)
16. [docs/handoff-summary.md](/Users/zhao/Desktop/git/AIQuantBase/docs/handoff-summary.md)

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
