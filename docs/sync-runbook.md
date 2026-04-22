# Sync Runbook

同步系统当前由仓库内的 [sync_data_system](/Users/zhao/Desktop/git/AIQuantBase/sync_data_system) 提供实现。

运行入口：

1. Python CLI: `python -m aiquantbase.cli sync-*`
2. Studio Sync 页面
3. 同步内部任务入口：`python -m sync_data_system.run_sync`

配置入口：

1. 共享运行配置：[config/runtime.local.yaml](/Users/zhao/Desktop/git/AIQuantBase/config/runtime.local.yaml)
2. 同步计划目录：[config/sync/plans](/Users/zhao/Desktop/git/AIQuantBase/config/sync/plans)
3. 宽表导出目录：[config/sync/wide_table_specs](/Users/zhao/Desktop/git/AIQuantBase/config/sync/wide_table_specs)

说明：

- 不再使用 `.env`
- `datasource` 为主程序和同步程序共用 ClickHouse 配置
- `sync.amazingdata` / `sync.baostock` 为同步数据源专用配置
