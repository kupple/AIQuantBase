# AIQuantBase 分钟执行接口整改需求

> 目标：让 `AlphaBlocks` 当前已落地的“日线信号 + 分钟执行规则”主线，能够直接通过 `AIQuantBase` 拉取分钟窗口数据并进入回测。

## 1. 一句话结论

当前 `AlphaBlocks` 已经支持下面这条链：

1. 合同里声明 `execution.intraday_rules`
2. `protocol_core` 编译出 `intraday_requirement`
3. workflow 根据 `intraday_requirement + intraday_anchors` 调 `AIQuantBase`
4. `AIQuantBase` 返回分钟窗口数据
5. `AlphaBlocks` 组装成 `intraday_execution_view`
6. `backtest_runtime` 使用分钟窗口执行：
   `固定时刻观察 + 下一分钟执行`

现在缺的是：

**AIQuantBase 暴露稳定的分钟窗口查询接口**

---

## 2. 当前 AlphaBlocks 已经实现到哪里

当前主项目已经实现了：

1. 合同层：
   `execution.intraday_rules`
2. 编译层：
   `intraday_requirement`
3. data access 层：
   `build_intraday_execution_request(...)`
   `build_intraday_execution_view_bundle(...)`
4. 回测层：
   `run_backtest(..., intraday_execution_view=...)`
5. workflow 层：
   `run_live_task(...)` 会自动在需要时拉分钟窗口并落盘

也就是说，SDK 这边只要按本文档把接口做出来，当前主项目就能直接接上。

---

## 3. 当前代码实际会调用什么接口

当前 adapter 的优先级是：

### 优先接口

```python
query_next_trading_day_intraday_windows(
    anchors: list[dict],
    start_hhmm: str,
    end_hhmm: str,
    fields: list[str],
    asset_type: str = "stock",
)
```

### 兼容接口

如果没有上面这个接口，则会退回：

```python
query_minute_window_by_trading_day(
    symbols: list[str],
    trading_days: list[str],
    start_hhmm: str,
    end_hhmm: str,
    fields: list[str],
    asset_type: str = "stock",
)
```

当前 adapter 代码位置：

1. [adapter.py](/Users/zhao/Desktop/git/AlphaBlocks/integrations/aiquantbase/adapter.py)

建议：

**优先实现 `query_next_trading_day_intraday_windows`**

因为它最贴近当前业务语义。

---

## 4. 推荐实现接口

## 4.1 推荐接口签名

```python
def query_next_trading_day_intraday_windows(
    self,
    *,
    anchors: list[dict],
    start_hhmm: str,
    end_hhmm: str,
    fields: list[str],
    asset_type: str = "stock",
) -> dict:
    ...
```

### `anchors` 的输入格式

每个 anchor 当前至少会包含：

```yaml
- anchor_id: "002545.SZ__2024-03-01__2024-03-04"
  code: "002545.SZ"
  signal_date: "2024-03-01"
  execution_date: "2024-03-04"
```

其中：

1. `anchor_id`
   当前主要用于 debug / 对账
2. `code`
   股票代码
3. `signal_date`
   日线信号日
4. `execution_date`
   分钟执行日

### `start_hhmm`

例如：

1. `09:35`
2. `14:30`

### `end_hhmm`

当前 `AlphaBlocks` 会自动把 `next_bar_open` 的执行时点考虑进去。

例如：

1. 如果观察时点有 `14:30`
2. 执行时点是 `next_bar_open`
3. 那么会把 `end_hhmm` 推到 `14:31`

也就是说，SDK 只要把 `[start_hhmm, end_hhmm]` 的分钟 bars 返回即可。

### `fields`

例如：

```yaml
- is_limit_up
- open
```

当前 `AlphaBlocks` 会把：

1. 规则条件用到的分钟字段
2. 执行价格字段

一起放进 `fields`

### `asset_type`

第一版默认会传：

`stock`

---

## 5. 兼容实现接口

如果你暂时不想做事件窗口接口，也可以先做这个：

```python
def query_minute_window_by_trading_day(
    self,
    *,
    symbols: list[str],
    trading_days: list[str],
    start_hhmm: str,
    end_hhmm: str,
    fields: list[str],
    asset_type: str = "stock",
) -> dict:
    ...
```

这种方式当前也能接上，只是语义没那么贴业务。

如果只实现这个接口，`AlphaBlocks` 当前会传：

```yaml
symbols:
  - 002545.SZ
trading_days:
  - 2024-03-04
start_hhmm: "09:35"
end_hhmm: "14:31"
fields:
  - is_limit_up
  - open
asset_type: stock
```

---

## 6. 返回结果格式要求

无论是推荐接口还是兼容接口，返回结果都建议统一成：

```python
{
    "ok": True,
    "df": pandas.DataFrame(...),
    "issues": [],
    "meta": {...},
    "debug": {...},
}
```

### 6.1 `ok`

1. `True`
   查询成功
2. `False`
   查询失败

### 6.2 `df`

必须返回 `pandas.DataFrame`

如果成功但没有任何数据：

1. 可以返回空 `DataFrame`
2. 但当前 `AlphaBlocks` 会把“整体为空”当作错误

所以更推荐：

1. 整体完全查不到时返回 `ok=False`
2. 部分 anchor 没数据时返回 `ok=True + 非空 df`

### 6.3 `issues`

失败时建议返回：

```python
[
    {"message": "xxx"}
]
```

当前 adapter 会把这些 message 拼成错误信息。

### 6.4 `meta`

建议至少返回：

```python
{
    "asset_type": "stock",
    "freq": "1m",
    "row_count": 1234,
    "symbol_count": 12,
    "empty": False,
}
```

### 6.5 `debug`

建议至少返回：

```python
{
    "sql": "SELECT ...",
    "intent": {...},
}
```

其中：

1. `sql`
   当前会直接归档到 workflow 产物里
2. `intent`
   可选，但建议保留

---

## 7. DataFrame 列名要求

当前 `AlphaBlocks` 最终只强依赖两类基础列：

### 必需列

1. `code`
2. `trade_time`

### 请求字段列

以及 `fields` 里请求到的分钟字段，例如：

1. `open`
2. `is_limit_up`

### 当前 adapter 还支持的列名别名

如果你不想直接返回 `code` / `trade_time`，当前 adapter 也能识别这些别名：

#### 代码列别名

1. `symbol`
2. `market_code`
3. `etf_code`

#### 时间列别名

1. `datetime`
2. `bar_time`
3. `minute_time`

但建议你直接返回标准列名：

1. `code`
2. `trade_time`

这样最稳。

---

## 8. `trade_time` 格式要求

当前 adapter 会做：

```python
pd.to_datetime(df["trade_time"], errors="coerce")
```

所以推荐格式：

1. `2024-03-04 14:30:00`
2. `2024-03-04T14:30:00`

都可以。

建议统一返回：

`YYYY-MM-DD HH:MM:SS`

---

## 9. 当前第一版最小字段要求

当前这一版要接通主链，**最小只需要 2 个分钟字段**：

1. `open`
2. `is_limit_up`

只要这两个字段能返回，当前这条课题就能跑：

“涨停后，次日 `14:30` 若未涨停，则 `14:31 open` 卖出”

### 推荐一起做的字段

建议顺手补上：

1. `high`
2. `low`
3. `close`
4. `volume`
5. `amount`
6. `is_limit_down`
7. `limit_up_price`
8. `limit_down_price`

这样后面不用再频繁改接口。

---

## 10. 返回 DataFrame 示例

推荐接口返回的 `df` 可以长这样：

| code | signal_date | execution_date | trade_time | open | is_limit_up |
|---|---|---|---|---:|---:|
| 002545.SZ | 2024-03-01 | 2024-03-04 | 2024-03-04 09:35:00 | 10.80 | 0 |
| 002545.SZ | 2024-03-01 | 2024-03-04 | 2024-03-04 09:36:00 | 11.00 | 0 |
| 002545.SZ | 2024-03-01 | 2024-03-04 | 2024-03-04 14:30:00 | 11.90 | 0 |
| 002545.SZ | 2024-03-01 | 2024-03-04 | 2024-03-04 14:31:00 | 12.00 | 0 |

其中：

1. `signal_date`
2. `execution_date`

当前 adapter 不强依赖，但**强烈建议保留**，方便 debug 和后续扩展。

---

## 11. 推荐接口成功返回示例

```python
{
    "ok": True,
    "df": df,
    "issues": [],
    "meta": {
        "asset_type": "stock",
        "freq": "1m",
        "row_count": 4,
        "symbol_count": 1,
        "empty": False,
    },
    "debug": {
        "sql": "SELECT ... FROM stock_minute_bar ...",
        "intent": {
            "kind": "next_trading_day_intraday_windows",
            "anchor_count": 1,
            "start_hhmm": "09:35",
            "end_hhmm": "14:31",
            "fields": ["is_limit_up", "open"],
        },
    },
}
```

---

## 12. 推荐接口失败返回示例

```python
{
    "ok": False,
    "df": None,
    "issues": [
        {"message": "minute data source is unavailable"},
    ],
    "meta": {
        "asset_type": "stock",
        "freq": "1m",
        "row_count": 0,
        "symbol_count": 0,
        "empty": True,
    },
    "debug": {
        "sql": "SELECT ...",
    },
}
```

---

## 13. 业务语义要求

SDK 这边只需要负责：

1. 根据 `code + execution_date + time window`
2. 返回该执行日的分钟 bars

SDK 不需要负责：

1. 判断信号是否成立
2. 判断何时买卖
3. 处理未来函数
4. 计算回测结果

这些都在 `AlphaBlocks` 端处理。

---

## 14. 当前 AlphaBlocks 的执行语义

为了让 SDK 实现时不误解，这里把当前语义写清楚。

例如用户课题：

“涨停后，次日 `14:30` 股票不涨停就卖出”

在当前系统里的正式定义是：

1. 日线层先确定 `signal_date`
2. SDK 返回 `execution_date` 这一天的分钟窗口 bars
3. `AlphaBlocks` 在 `14:30` 这一根分钟 bar 观察条件
4. 若条件成立，则按 **下一分钟 `open`**
   也就是 `14:31 open` 成交

所以 SDK 只要把：

1. `14:30`
2. `14:31`

这两根 bar 都返回，就够了。

---

## 15. 第一版必须支持的交易时段

第一版建议支持标准 A 股连续竞价分钟 bars：

1. `09:30` - `11:30`
2. `13:00` - `15:00`

当前 `AlphaBlocks` validator 也按这个时段做校验。

---

## 16. 兼容性建议

为了减少后面反复改接口，建议分钟接口在一开始就支持：

1. 多 `symbols`
2. 多 `execution_date`
3. 任意分钟窗口
4. 任意字段白名单

不要做成只支持单标的单日。

---

## 17. 当前代码最关心的几个点

如果只考虑“能不能让当前代码直接跑起来”，你这边只要满足下面几点：

1. 至少实现一个接口：
   `query_next_trading_day_intraday_windows`
   或
   `query_minute_window_by_trading_day`
2. 返回标准结果字典：
   `ok / df / issues / meta / debug`
3. DataFrame 里必须有：
   `code`
   `trade_time`
   请求字段
4. `trade_time` 能被 `pd.to_datetime` 正常解析
5. 至少支持字段：
   `open`
   `is_limit_up`

做到这 5 条，当前 AlphaBlocks 的分钟执行链就能接上。

---

## 18. 建议的实现顺序

推荐顺序：

### 第 1 步

先实现：

`query_minute_window_by_trading_day`

因为实现成本最低。

### 第 2 步

再补：

`query_next_trading_day_intraday_windows`

这样语义更完整。

### 第 3 步

补齐更多分钟字段：

1. `close`
2. `high`
3. `low`
4. `is_limit_down`

---

## 19. AlphaBlocks 当前对应代码入口

如果你需要对照当前主项目代码，重点看：

1. [adapter.py](/Users/zhao/Desktop/git/AlphaBlocks/integrations/aiquantbase/adapter.py)
2. [intraday-execution-mvp-design.md](/Users/zhao/Desktop/git/AlphaBlocks/docs/current/guides/intraday-execution-mvp-design.md)
3. [runtime.py](/Users/zhao/Desktop/git/AlphaBlocks/protocol_core/protocol_core/backtest/runtime.py)
4. [runner.py](/Users/zhao/Desktop/git/AlphaBlocks/workflow/tasks/runner.py)

---

## 20. 最终目标

SDK 这边实现完本接口后，当前项目的目标是直接支持下面这条真实链路：

1. 日线合同生成信号
2. 编译出 `intraday_requirement`
3. workflow 自动拉分钟窗口
4. 生成 `intraday_execution_view.yaml`
5. 回测按分钟执行规则完成成交

第一版重点不是分钟研究，而是：

**先把分钟执行规则跑通**
