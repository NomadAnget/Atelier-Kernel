# 观察能力

按检测范围与职能分四组：**消息生命周期**（`kernel.observer`）、**执行进度**（`kernel.logger`）、**运行指标**（`kernel.telemetry`）、**通知告警**（`kernel.notify`）。

优先使用官方 SDK，不要在业务域里手拼 `kernel.*` 协议。

## 共同前提：订阅必须显式

`on_observation` / `on_log` / `on_telemetry` / `on_notify` **只登记一个本地回调**，不会向内核订阅。**必须先调 `subscribe_*` 才收得到事件**，用完调 `unsubscribe_*`：

```python
domain.on_notify(lambda alert: print(alert))   # 登记回调，此时还收不到
await domain.subscribe_notify()                # 真正订阅
# ...
await domain.unsubscribe_notify()
```

订阅生命周期绑定进程代际，进程退出即回收；新代际需要重新订阅。

## 一、消息生命周期：`kernel.observer`

| 操作 | Python | Node.js |
| :--- | :--- | :--- |
| 订阅 | `await subscribe_observer()` | `await subscribeObserver()` |
| 取消订阅 | `await unsubscribe_observer()` | `await unsubscribeObserver()` |
| 查询近期记录 | `await query_observer(**filters)` | `await queryObserver(filters)` |

过滤参数：`message_id`、`target`、`source_domain`、`limit`（默认 100，上限 1000）；Node 同时接受 `messageId` / `sourceDomain`。

事件字段：`event_id`、`timestamp`（Unix 毫秒）、`logical_id`、`message_id`、`source_domain`、`target`、`seq`、`message_type`、`status`、`details`。

状态沿 `Sent → Received → Started → Completed` 推进；错误可能是 `TargetInvalid` 或 `ExecutionFailed`。`logical_id` 关联一次提交，`message_id` 标识拆分后的独立目标消息。可以用同一 `message_id` 各阶段时间差估算耗时，但**阶段缺失不能自行判成成功或失败**。

## 二、执行进度：`kernel.logger`

进度不另设处理器，复用日志通道，用 `ephemeral` 标记：

| 操作 | Python | Node.js |
| :--- | :--- | :--- |
| 上报进度 | `logger.info(msg, ephemeral=True, key=..., frac=..., waiting=..., done=..., state=...)` | `logger.info(msg, { ephemeral: true, key, frac, waiting, done, state })` |
| 接收进度更新 | `await subscribe_logs()` + `on_log` | `await subscribeLogs()` + `onLog` |
| 查询当前活动 | `await query_progress()` | `await queryProgress()` |

身份是 `(内核绑定的 source_domain, key)`，不同域的同名 `key` 互不干扰。`frac` 是比例，`waiting` 表示排队，`state` 取 `running` / `completed` / `failed` / `cancelled`。终态更新必须复用原 `key`。`done` 只是终止兼容字段，**不能当成功标志**。详见 [进度状态](progress.md)。

`query_progress()` 返回当前活动条目，不是完成历史；终态会移出活动集合，**查不到不代表成功**。

实时副本允许丢失；进度 `key` 与消息 ID 之间没有内核自动映射，需上报方自己提供业务关联信息。

## 三、运行指标：`kernel.telemetry`

| 操作 | Python | Node.js |
| :--- | :--- | :--- |
| 订阅 | `await subscribe_telemetry()` + `on_telemetry` | `await subscribeTelemetry()` + `onTelemetry` |
| 取消订阅 | `await unsubscribe_telemetry()` | `await unsubscribeTelemetry()` |

**SDK 没有上报遥测的方法**：遥测事件由内核侧发出，域只能在订阅端消费。事件包含 `metric_name`、`source_domain` 与发送方数据。

内核不提供 GPU/CPU/吞吐量采集或阈值判定，也没有专用历史指标查询、聚合或趋势存储。观察副本允许丢失，消费方如需趋势要自行保留有限展示窗口。

## 四、通知与告警：`kernel.notify`

| 操作 | Python | Node.js |
| :--- | :--- | :--- |
| 发通知（等内核处理结果） | `await notify(level, message, title, details)` | `await notify(level, message, title, details)` |
| 订阅 | `await subscribe_notify()` + `on_notify` | `await subscribeNotify()` + `onNotify` |
| 取消订阅 | `await unsubscribe_notify()` | `await unsubscribeNotify()` |

事件字段：`notification_type`、`source_domain`、`severity`、`message`、`details`、`params`。`details` 包含原发送 `data` 包；业务扩展可能嵌在 `details` 里，不要假定被内核平铺。

内核把通知正文与级别并入日志审计，同时广播实时副本。没有独立通知历史库、已读状态、确认/消警或自动阈值规则。`notify` 返回内核处理结果，不保证每个观察者已收到或用户已读。

## 共同边界

- 这三组观察流都是**允许丢失**的尽力交付：慢观察者不会阻塞业务投递，但可能看到缺口。
- 队列与积压管理由内核/SDK 封装，不向业务域暴露容量参数。
- 它们**没有**域状态镜像那种"最终状态收敛"承诺（那是 [域状态观察](domain-state-observation.md) 的语义）。业务结果请走 `send` / `request` 的回执，不要把观察流当结果通道。
