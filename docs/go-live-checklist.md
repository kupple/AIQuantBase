# Go-Live Checklist

## 目标

这份清单用于在 AIQuantBase 正式启用前，逐项确认系统状态、默认配置、查询边界和使用方式是否已经准备好。

## 1. 默认配置检查

确认以下文件存在并为当前正式版本：

1. [config/graph.yaml](/Users/zhao/Desktop/git/AIQuantBase/config/graph.yaml)
2. [config/fields.yaml](/Users/zhao/Desktop/git/AIQuantBase/config/fields.yaml)
3. [config/runtime.local.yaml](/Users/zhao/Desktop/git/AIQuantBase/config/runtime.local.yaml)

确认默认行为：

1. Python SDK 默认读 `config/graph.yaml`
2. Python SDK 默认读 `config/fields.yaml`
3. 服务端默认读 `config/graph.yaml`
4. 服务端默认读 `config/fields.yaml`
5. 网页管理台默认也读 `config/` 下配置

## 2. SDK 检查

确认以下函数可用：

1. `GraphRuntime.from_defaults()`
2. `runtime.execute_intent(intent)`
3. `runtime.execute_sql(sql)`
4. `runtime.get_real_fields_json()`

确认 `execute_intent()` / `execute_sql()` 返回结构一致：

1. `code`
2. `message`
3. `sql`
4. `df`

## 3. 主入口节点检查

确认当前默认对外开放的主入口节点为：

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

确认以下节点不默认对外开放：

1. `sync_task_log_real`
2. `stock_snapshot_real`
3. 其他支撑节点

## 4. 字段协议检查

确认字段协议中已经具备：

1. `description_zh`
2. `field_role`
3. `path_domain`
4. `path_group`
5. `time_semantics`
6. `lookahead_category`

确认 `get_real_fields_json()` 返回：

1. 节点英文名
2. 节点中文说明
3. 表名
4. 字段英文名 `field_name`
5. 字段中文名 `field_label_zh`

## 5. Query Intent 检查

确认当前 Query Intent 使用规范：

1. 明确 `from`
2. 明确 `select`
3. 明确 `where`
4. 明确 `time_range`
5. 明确分页 `page/page_size`

确认常用 where 操作符已验证：

1. `=`
2. `in`
3. `not_in`
4. `between`
5. `like`
6. `contains`
7. `is_null`
8. `is_not_null`

## 6. lookahead 检查

确认当前 `lookahead_safe` 已生效的范围：

1. 公告类字段
2. 事件类字段
3. 条款生效字段

确认当前明确不影响：

1. `close`
2. `high`
3. `low`
4. `volume`
5. `amount`
6. `market_cap`
7. `turnover_rate`

## 7. 数据库查询规模检查

确认当前已知约束：

1. 全市场大面板查询容易超时
2. 大时间范围查询建议分页
3. 原生 SQL 查询建议带 `LIMIT`
4. 分钟查询建议缩小时间窗

如果要正式开放给其他模块，建议额外确认：

1. 是否需要限制默认 `page_size`
2. 是否需要限制分钟查询范围
3. 是否需要后续实现分片查询与阈值中止机制

## 8. 网页管理台检查

确认可以成功启动：

```bash
PYTHONPATH=src /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m aiquantbase.cli studio --host 127.0.0.1 --port 8000
```

确认以下页面可访问：

1. `/`
2. `/nodes`
3. `/edges`
4. `/graph/view`
5. `/llm-query`

## 9. 文档检查

确认以下文档已经更新并可交付：

1. [README.md](/Users/zhao/Desktop/git/AIQuantBase/README.md)
2. [docs/python-sdk-usage.md](/Users/zhao/Desktop/git/AIQuantBase/docs/python-sdk-usage.md)
3. [docs/query-intent-templates.md](/Users/zhao/Desktop/git/AIQuantBase/docs/query-intent-templates.md)
4. [docs/public-entry-nodes.md](/Users/zhao/Desktop/git/AIQuantBase/docs/public-entry-nodes.md)
5. [docs/trusted-query-boundary.md](/Users/zhao/Desktop/git/AIQuantBase/docs/trusted-query-boundary.md)
6. [docs/field-description-catalog.md](/Users/zhao/Desktop/git/AIQuantBase/docs/field-description-catalog.md)
7. [docs/final-graph-relationship.md](/Users/zhao/Desktop/git/AIQuantBase/docs/final-graph-relationship.md)

## 10. 测试检查

正式启用前，建议至少执行一次：

```bash
PYTHONPATH=src /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 -m pytest -q
```

当前目标状态：

1. 全量测试通过
2. 无关键回归
3. SDK 测试通过
4. 网页测试通过

## 11. 一句话上线判断

当以下 4 点都满足时，可以认为当前版本适合正式启用：

1. 默认配置已收口到 `config/`
2. 主入口节点边界已明确
3. SDK 返回结构已稳定
4. 查询可信边界和文档已明确
