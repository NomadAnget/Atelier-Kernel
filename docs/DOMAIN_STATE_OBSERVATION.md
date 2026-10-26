# 无轮询的域状态观察

使用既有 Python `domain.on_observation` / Node `domain.onObservation`，无需队列参数、恢复循环或定时查询。

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

Python `domain.observer.domain_states`、Node `domain.observer.domainStates`为只读拷贝，尚未订阅时为None/null。它们缓存最新完整details。Web在浏览器SSE重连时直接发送该镜像，不请求轮询；新的Web进程正常订阅即重新取得初始镜像。底层IPC断开后SDK仍遵循原退出契约，由监督器恢复域，恢复后的新进程重新订阅；不宣称能在死进程中重连IPC。

同stream_id下忽略旧revision，允许等revision的幂等初始重发；stream_id变化时完整替换基线。ObservationEvent顶层event_id仍属于普通观察历史，镜像中的event_id只是观察水位；状态排序必须使用details中的stream_id/revision，不能按message_id统计状态事件。

## Web接入示例

```js
domain.onObservation(event => {
  if (event.message_type === 'DomainStateSnapshot') {
    publishStateToSse(event.details);
  }
});
// 每个新浏览器连接：
const latest = domain.observer.domainStates;
if (latest) sendInitialState(latest);
```

不增加状态轮询兜底。普通消息生命周期与逐条转换历史仍允许丢失，当前状态镜像负责最终收敛；用户回调永不返回或进程永不恢复时，不承诺该客户端能继续接收。
