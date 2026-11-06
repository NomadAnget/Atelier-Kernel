# 无轮询的域状态观察

用一个观察订阅就能拿到域状态，无需队列参数、恢复循环或定时查询。

**顺序很关键：先注册 `on_observation` / `onObservation` 回调，再 `await subscribe_observer()`。** 内核一收到 `SubscribeObserver` 就**立刻推一条 `DomainStateSnapshot`**，然后才回订阅确认；而 SDK 在没有回调时直接丢弃事件（不缓存）。反过来写就会丢掉那条初始镜像。

## 实际状态转换

内核在 Router 状态实际写入时生成 `message_type: "DomainStatusChanged"` 的观察记录，details 为：

```json
{"domain_id":"engines","instance_id":12,"generation":12,"status":"active","revision":42,"timestamp":1792938000000,"reason":null}
```

状态包括 spawning、active、faulted、stopped。覆盖处理器注册、进程/IPC故障、显式停止、自动重启恢复和滚动更新提交。失败候选不改变旧实例的实际状态。重复相同代际/状态不制造虚假转换；旧代际迟到故障不覆盖新代际。首次进程创建失败也记录faulted，尚未分配实例时generation为0。主动停止不再把真实Router状态留为Faulted。

active表示内核处理器注册状态，不等同业务ping.ready或模型加载结果；此流不推断业务健康，也不替代业务能力声明。状态镜像仅含状态信息，不包含完整进程规格、路由描述或业务数据。

## 初始快照与恢复

同一个订阅还自动收到 `message_type: "DomainStateSnapshot"`：

```json
{"stream_id":"kernel启动UUID","revision":42,"domains":[{"domain_id":"engines","instance_id":12,"generation":12,"status":"active","revision":42,"timestamp":1792938000000,"reason":null}]}
```

上述对象位于观察记录的details，不是直接的业务data。revision为本次Kernel内全局递增的状态修订号，每条域记录保留其最后变化的revision。stream_id在内核重启后更换。订阅时立即发送完整镜像，此后每次真实状态变化都触发镜像更新。订阅与变化交错时，出口和SDK避免旧修订覆盖新修订。

转换事件仍是可丢失的诊断记录，不承诺逐条重放所有中间状态。状态镜像是当前状态的可替换内存值：内核保留一份最新待发镜像，SDK慢回调保留一份最新待回调镜像；普通可丢失事件为镜像让出预算，不因它们填满队列而漏掉最后状态。不写持久消息账本/磁盘，不引入持久事件系统。状态表及镜像固有空间随当前已知域数量变化，不随历史转换次数增长。停止读取的客户端恢复读取后取得最新状态，而不是重放全部历史。

**SDK 不缓存状态镜像**：v0.2 的 `AtelierDomain` 没有 `observer` 子对象，也不暴露 `domain_states` / `domainStates`。需要"最新镜像"的调用方（例如给浏览器 SSE 重连补基线）必须在自己的域里保存收到的最新 `details`，见下方示例。镜像只存在于内核出口与你自己保存的那一份；内核不保证重放历史转换。

同stream_id下忽略旧revision，允许等revision的幂等初始重发；stream_id变化时完整替换基线。ObservationEvent顶层event_id仍属于普通观察历史，镜像中的event_id只是观察水位；状态排序必须使用details中的stream_id/revision，不能按message_id统计状态事件。

## Web接入示例

```js
let latestSnapshot = null;                 // 自己保存最新镜像（SDK 不提供缓存）
// 1) 先登记回调——内核在订阅被处理时会立刻推一条初始快照，那时回调必须已经在。
domain.onObservation(event => {
  if (event.message_type === 'DomainStateSnapshot') {
    const prev = latestSnapshot;
    // 同 stream_id 下忽略旧 revision；stream_id 变化时整体替换基线。
    if (!prev || prev.stream_id !== event.details.stream_id || event.details.revision >= prev.revision) {
      latestSnapshot = event.details;
    }
    publishStateToSse(latestSnapshot);
  }
});
// 2) 再订阅，才会真正收到事件（含那条初始快照）。
await domain.subscribeObserver();
// 每个新浏览器连接：
if (latestSnapshot) sendInitialState(latestSnapshot);
```

不增加状态轮询兜底。普通消息生命周期与逐条转换历史仍允许丢失，当前状态镜像负责最终收敛；用户回调永不返回或进程永不恢复时，不承诺该客户端能继续接收。
