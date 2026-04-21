# Error Code Specification

## 目标

这份文档说明当前 AIQuantBase 面向上层应用接口的错误码规范。

当前错误码分两层：

1. 顶层执行结果 `code`
2. `issues` 列表中的 `code + error_code`

## 1. 顶层执行结果 code

### `0`
表示成功。

### `1000`
表示执行失败。

当前典型包括：

1. 意图渲染失败
2. SQL 执行失败
3. 网络或数据库异常

示例：

```python
{
    "code": 1000,
    "message": "<urlopen error timed out>",
    "sql": "SELECT ...",
    "df": DataFrame(),
}
```

## 2. issues 中的错误码

`validate_query_request(...)`、`resolve_symbols(...)`、`resolve_best_node(...)` 等接口会返回 `issues`。

每条 issue 当前结构：

```python
{
    "code": "unsupported_field",
    "error_code": 1109,
    "message": "..."
}
```

## 3. 当前 issue code 映射

1. `unknown_symbol_type` -> `1101`
2. `unsupported_universe` -> `1102`
3. `missing_symbols` -> `1103`
4. `symbols_required_for_minute` -> `1104`
5. `minute_time_range_too_large` -> `1105`
6. `daily_universe_time_range_too_large` -> `1106`
7. `mixed_asset_types` -> `1107`
8. `unsupported_freq` -> `1108`
9. `unsupported_field` -> `1109`
10. `unsupported_asset_type` -> `1110`
11. `missing_query_node` -> `1111`
12. `empty_result` -> `1112`
13. `query_failed` -> `1113`
14. `missing_anchors` -> `1114`
15. `missing_trading_days` -> `1115`
16. `invalid_intraday_time` -> `1116`
17. `trading_calendar_unavailable` -> `1117`
18. `missing_execution_date` -> `1118`

## 4. 推荐用法

### 判断顶层执行是否成功

```python
if result["code"] != 0:
    print(result["message"])
```

### 判断校验问题类型

```python
for issue in result["issues"]:
    print(issue["code"], issue["error_code"], issue["message"])
```

## 5. 当前原则

1. 顶层 `code` 用数字，适合上层统一判断
2. `issues.code` 保留字符串，适合调试和可读性
3. `issues.error_code` 用数字，适合上层程序化处理
4. 分钟执行接口的时间窗口、锚点、交易日历问题也统一放进 `issues`
