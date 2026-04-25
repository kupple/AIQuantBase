# Sync Config Layout

`config/sync/` 是同步系统的外层配置目录。

当前约定：

1. `plans/`
   存放同步任务计划文件，文件名形如 `run_sync*.toml`。
2. `wide_table_specs/`
   存放宽表导出的同步 YAML。
3. `../runtime.local.yaml`
   主程序与同步程序共用的运行配置文件。

说明：

- 同步程序不再读取 `.env`
- ClickHouse 与数据源账号统一从 [runtime.local.yaml](/Users/zhao/Desktop/git/AIQuantBase/config/runtime.local.yaml) 读取
- AmazingData 同步任务额外依赖 `sync.amazingdata.username/password/host/port/local_path`
- `plans/` 只描述“跑哪些任务、时间范围、限额、是否继续执行”等任务计划
