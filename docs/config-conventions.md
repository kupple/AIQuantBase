# Config Conventions

## 目标

这份文档用于约定 AIQuantBase 项目里配置文件、示例文件和文档文件的职责边界。

目标是避免后续继续迭代时出现：

1. 正式配置和示例配置混在一起
2. 应用默认读取旧文件
3. 不同模块各自读取不同图谱版本

## 1. 总体原则

项目中的配置分成 3 类：

1. `config/`
   正式运行配置

2. `examples/`
   示例与测试资产

3. `docs/`
   协议、说明与设计文档

## 2. config/ 目录职责

`config/` 是当前系统的正式默认配置目录。

### 当前正式默认文件

1. `config/runtime.local.yaml`
   本地运行时配置
   包含：
   - 数据源连接
   - LLM 配置
   - schema discovery 白名单

2. `config/runtime.example.yaml`
   运行时配置示例

3. `config/graph.yaml`
   当前正式默认图谱关系配置

4. `config/fields.yaml`
   当前正式默认字段协议配置

### 规则

1. SDK 默认读取 `config/graph.yaml` 与 `config/fields.yaml`
2. 服务端默认读取 `config/graph.yaml` 与 `config/fields.yaml`
3. Studio 默认工作区也应读取 `config/graph.yaml` 与 `config/fields.yaml`
4. 如果调用方显式传入路径，则优先使用传入路径

### 一句话

`config/` 下的文件，代表系统当前正式生效版本。

## 3. examples/ 目录职责

`examples/` 只放示例与测试资产，不作为正式默认配置入口。

### 适合放在 examples/ 的内容

1. 示例 Query Intent
2. demo graph
3. demo field catalog
4. schema discovery 示例输出
5. 测试样例文件
6. 过往迭代过程中保留的参考图谱

### 不适合继续作为默认入口的内容

1. 正式运行图谱
2. 正式字段协议
3. 生产默认读取配置

### 一句话

`examples/` 是给人看、给测试跑、给演示用的，不是正式运行入口。

## 4. docs/ 目录职责

`docs/` 用来记录：

1. 系统设计
2. 图谱关系说明
3. 字段协议说明
4. 接入协议说明
5. 配置约定

### 当前关键文档

1. `docs/query-planning-middleware-design.md`
   中间件整体设计与接入表清单

2. `docs/path-constraint-and-asset-field-layering.md`
   路径约束、字段分层、时间语义等协议设计

3. `docs/final-graph-relationship.md`
   当前最终图谱关系总览

4. `docs/field-description-catalog.md`
   字段中文说明总表

5. `docs/field-protocol-for-integrations.md`
   其他模块如何接入字段协议

6. `docs/config-conventions.md`
   本文档，约定配置文件职责边界

## 5. 默认读取规则

### SDK

```python
from aiquantbase import GraphRuntime
runtime = GraphRuntime.from_defaults()
```

默认应读取：

1. `config/graph.yaml`
2. `config/fields.yaml`
3. `config/runtime.local.yaml`

### 服务端

默认应读取：

1. `config/graph.yaml`
2. `config/fields.yaml`
3. `config/runtime.local.yaml`

### Studio

默认工作区应指向：

1. `config/graph.yaml`
2. `config/fields.yaml`
3. `config/runtime.local.yaml`

## 6. 覆盖规则

默认配置只是“默认”。

如果调用方显式传入路径，则优先使用传入路径。

例如：

1. SDK 构造时传入 `graph_path`
2. 服务端接口传入 `graph_path` / `fields_path`
3. Studio 工作区选择其他路径

这类显式传参都应覆盖默认值。

## 7. 维护建议

后续继续开发时，建议遵循以下规则：

1. 新版本正式图谱先更新 `config/graph.yaml`
2. 新版本正式字段协议先更新 `config/fields.yaml`
3. 如果需要保留实验版，再额外放到 `examples/`
4. 不要再把 `examples/real_combined_graph.yaml` 当成默认入口
5. `examples/` 可以保留历史文件，但默认入口必须统一收口到 `config/`

## 8. 最终结论

当前项目的职责边界应该固定为：

1. `config/` = 正式默认配置
2. `examples/` = 示例与测试资产
3. `docs/` = 协议与说明文档

这样以后无论是：

1. SDK 调用
2. 服务端调用
3. Studio 使用
4. 其他模块集成

都只需要记住一条规则：

**默认读 `config/`，示例看 `examples/`，说明查 `docs/`。**
