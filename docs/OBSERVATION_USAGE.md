# 观察能力使用说明：按检测范围与职能分组

范围：不包含域状态、域装配及处理器发现；普通日志仅作为进度传输入口提及。优先使用官方 SDK，不在业务域手工拼装 kernel.* 协议。以下为0.1.14源码与配套SDK契约。

## 一、调用与执行观察

### 消息生命周期：kernel.observer

| 操作 | 内核 message_type | Node SDK | Python SDK |
|---|---|---|---|
| 实时订阅 | SubscribeObserver | domain.onObservation(callback) | @domain.on_observation |
| 查询近期观察记录 | QueryObserver | await domain.observer.query(options) | await domain.observer.query(...) |
| 取消内核订阅 | UnsubscribeObserver | await domain.observer.unsubscribe() | await domain.observer.unsubscribe() |

注册监听后由 SDK 负责订阅。查询过滤：message_id、target、source_domain、limit（默认100）。Node 同时接受 messageId/sourceDomain。查询返回事件数组，仅查询当前有界内存观察历史，不是持久账本历史查询，没有按 logical_id 查询的原生参数。

事件字段：event_id、timestamp（Unix毫秒）、logical_id、message_id、source_domain、target、seq、message_type、status、details。

状态：Sent → Received → Started → Completed；错误可能为 TargetInvalid 或 ExecutionFailed。logical_id关联一次发送，message_id识别拆分后的独立目标消息。耗时可按同一message_id的阶段时间差计算，但缺失阶段不能自行判为成功或失败。

```js
domain.onObservation(event => {
  // 同一通道也承载域状态事件，消息链路展示应排除它们。
  if (['DomainStateSnapshot', 'DomainStatusChanged'].includes(event.message_type)) return;
  consumeMessageEvent(event);
});
const events = await domain.observer.query({messageId: childMessageId, limit: 100});
```

实时副本允许丢失，近期记录也会被淘汰。不保证完整链路追溯，不是可靠业务结果接收接口。业务调用应继续使用SDK原有请求/结果接口。

### 执行进度：kernel.logger

| 操作 | 内核入口 | Node SDK | Python SDK |
|---|---|---|---|
| 上报进度 | log帧 | domain.logger.progress(message, options) | domain.logger.progress(message, key=..., frac=...) |
| 接收进度更新 | SubscribeLogs | domain.onLog(callback) | @domain.on_log |
| 查询当前活动进度 | QueryProgress | await domain.logger.queryProgress() | await domain.logger.query_progress() |

进度未另建处理器，复用结构化日志通道。接收方筛选带进度key的记录。身份为source_domain与key组合；frac为比例，waiting表示等待，state为running/completed/failed/cancelled。终态更新必须复用原key。done仅是终止兼容字段，不能当成功标志。

QueryProgress返回{progress: [...], count: N}，仅当前活动条目，不是完成历史。终态会移出活动集合；查不到不代表成功。进程崩溃后的活动进度自动清理不在此接口保证范围内。

实时进度副本允许丢失；进度key与消息ID之间没有内核自动映射，需上报方提供业务关联信息。

## 二、运行指标观察

### 遥测：kernel.telemetry

| 操作 | 内核 message_type | Node SDK | Python SDK |
|---|---|---|---|
| 上报 | 指标名 | domain.telemetry.emit(name, value, tags) | domain.telemetry.emit(metric_name=..., value=..., tags=...) |
| 订阅 | SubscribeTelemetry | domain.onTelemetry(callback) | @domain.on_telemetry |
| 取消订阅 | UnsubscribeTelemetry | 底层系统协议支持 | 底层系统协议支持 |

```js
domain.telemetry.emit('inference_latency_ms', 125.4, {engine: 'WhisperEngine'});
domain.onTelemetry(metric => consumeMetric(metric));
```

事件包括metric_name、source_domain及发送方数据（官方SDK上报含name、value、tags）。由域主动采集和上报；内核不自动提供GPU/CPU/吞吐量采集或阈值判定。当前没有专用历史指标查询、聚合、趋势存储接口。观察副本允许丢失，消费方如需趋势需自行保留有限展示窗口。不要为取消订阅另写通用协议适配层；若产品需要可补SDK原生封装。

## 三、异常与事件观察

### 通知与告警：kernel.notify

| 操作 | 内核 message_type | Node SDK | Python SDK |
|---|---|---|---|
| 发通知 | 标题或Notification | domain.notify(level, message, title, details) | domain.notify(level=..., message=..., title=..., details=...) |
| 等待内核确认 | 同上 | await domain.notifyAsync(...) | await domain.notify_async(...) |
| 订阅 | SubscribeNotify | domain.onNotify(callback) | @domain.on_notify |
| 取消订阅 | UnsubscribeNotify | 底层系统协议支持 | 底层系统协议支持 |

```js
domain.notify('warning', '磁盘使用率较高', 'DiskSpaceAlert', {used_percent: 91.5});
domain.onNotify(alert => consumeAlert(alert));
```

事件字段：notification_type、source_domain、severity、message、details、params。details包含原发送data包，业务扩展details可能嵌套于其中；不要假定被内核平铺。

内核将通知正文与级别归入日志审计，并广播实时副本。没有独立通知历史库、已读状态、确认/消警或自动阈值规则接口。notifyAsync确认内核处理，不保证每个观察者已收到或用户已读。实时副本允许丢失；日志审计不等同完整通知载荷历史。

## 共同行为

Python与Node使用相同内核消息契约，API命名遵循各语言惯例。慢观察者隔离、队列与资源回收由内核/SDK封装，不向业务域暴露容量或积压管理。订阅生命周期绑定进程实例，断开后清理；新进程需正常重新注册监听。以上三组普通观察流没有域状态镜像那种最终状态收敛承诺，不应照搬其无损恢复语义。
