# Error Code Specification

## 当前最小错误码规范

当前 SDK 和执行接口采用最小错误码规范：

1. `0` 成功
2. `1` 失败

## 1. code = 0

表示：

1. 查询或执行成功
2. `message` 一般为 `success`
3. 返回中包含有效 `sql` 与 `df`

示例：

```python
{
    "code": 0,
    "message": "success",
    "sql": "SELECT ...",
    "df": DataFrame(...)
}
```

## 2. code = 1

表示：

1. 解析失败
2. 图谱规划失败
3. SQL 执行失败
4. 底层网络或数据库错误

示例：

```python
{
    "code": 1,
    "message": "Field not found in graph registry: xxx",
    "sql": "",
    "df": DataFrame()
}
```

## 3. 当前建议

当前阶段先保持简单：

1. 只区分成功与失败
2. 通过 `message` 看具体错误原因

后续如果需要更精细的错误治理，再扩展到：

1. 意图解析失败
2. 字段冲突失败
3. 图谱路径失败
4. SQL 执行失败
