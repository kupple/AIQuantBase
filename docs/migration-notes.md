# Migration Notes

## 目标

这份文档用于说明：

1. 旧使用方式和新使用方式的差别
2. 上层应用应如何迁移到当前正式接口层
3. 哪些旧接口保留但不再建议作为主入口使用

## 1. 总体迁移方向

### 旧方式

更多围绕：

1. `GraphRuntime`
2. 直接 `Query Intent`
3. 图谱底层能力

### 新方式

推荐迁移到：

1. `ApplicationRuntime`
2. requirement / request 驱动接口
3. 应用层 facade

## 2. 推荐迁移方式

### 旧初始化方式

```python
from aiquantbase import GraphRuntime
runtime = GraphRuntime.from_defaults()
```

### 新初始化方式

```python
from aiquantbase import ApplicationRuntime
runtime = ApplicationRuntime.from_defaults()
```

## 3. 查询迁移

### 旧方式：直接执行 Query Intent

```python
result = runtime.execute_intent(intent)
```

### 新方式 1：简单场景

```python
result = runtime.query_daily(...)
```

### 新方式 2：推荐场景

```python
result = runtime.execute_requirement(data_requirement)
```

## 4. 字段能力获取迁移

### 旧方式

```python
catalog = runtime.get_metadata_catalog()
```

### 新方式

```python
result = runtime.get_supported_fields(...)
```

如果只是想给外部模块选字段，也可以：

```python
result = runtime.get_real_fields_json()
```

## 5. 节点选择迁移

### 旧方式

上层自己决定 `from`。

### 新方式

推荐先通过：

```python
runtime.resolve_symbols(...)
runtime.resolve_best_node(...)
```

由 AIQuantBase 帮上层解释：

1. 资产类型
2. 推荐主入口
3. 字段是否支持

## 6. 旧接口现在的地位

以下接口仍然保留：

1. `GraphRuntime.render_intent(...)`
2. `GraphRuntime.execute_intent(...)`
3. `GraphRuntime.execute_sql(...)`
4. `GraphRuntime.get_metadata_catalog()`
5. `GraphRuntime.get_real_nodes()`
6. `GraphRuntime.get_real_fields_json()`

但这些接口现在更适合：

1. 调试
2. 图谱管理
3. 底层排查
4. 高级使用场景

而不是上层业务代码的默认主入口。

## 7. 推荐迁移后的调用顺序

### 推荐顺序

1. `resolve_symbols()`
2. `get_supported_fields()`
3. `validate_query_request()`
4. `build_intent_from_requirement()`
5. `execute_requirement()`

### 简化顺序

1. `query_daily()`
2. `query_minute()`

## 8. 迁移时的注意点

1. 不要再让上层直接记底层表名
2. 不要让上层自己拼复杂 Query Intent 作为默认路径
3. 不要让上层自己猜 ETF、股票、可转债应该查哪个 node
4. 上层应把“我要什么数据”交给 `ApplicationRuntime`

## 9. 一句话迁移结论

**旧接口保留，主入口迁移到 `ApplicationRuntime`。**
