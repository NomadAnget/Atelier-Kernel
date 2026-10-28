# `atelier-sdk` (Python Domain SDK for Atelier Kernel)

Official zero-dependency Python SDK for building **Atelier Kernel** domains.

## Installation

```bash
pip install atelier_sdk-0.1.14-py3-none-any.whl
```

---

## 8 Native Infrastructure Facilities (Send & Receive Full Reference)

The SDK exposes 8 native infrastructure facilities directly on `domain.*`, allowing domains to interact with kernel capabilities through language-native methods.

For each facility below, both the **Sending / Invocations (How to Send)** and the **Receiving / Listening / Responses (How to Receive)** are documented with payload formats:

---

### 1. Registry & Discovery (`domain.registry`)

- **发送 / 调用 (How to Send / Invoke)**:
  - **自动报备**：在 `domain.run()` / `domain.run_async()` 启动时，SDK 自动将 `@domain.handler` 注册的处理器与自动提取的 docstring / 注释以 `RegisterHandlers` 发送给内核；退出时发送 `UnregisterHandlers`。
  - **服务发现查询**：
    ```python
    # 查询可用业务路由（支持 domain/target/handler 过滤，严格排除 kernel.*）
    handlers = await domain.registry.query_handlers(domain="billing", include_inactive=False)

    # 查询指定路由的方法文档注释
    doc = await domain.registry.get_description("billing.charge")
    ```
- **接收 / 响应结构 (How to Receive / Response)**:
  ```python
  # handlers 结构:
  {
      "count": 2,
      "routes": ["billing.charge", "billing.refund"],
      "handlers": [
          {"domain": "billing", "handler": "charge", "target": "billing.charge", "description": "处理信用卡扣款"}
      ]
  }
  ```

---

### 2. Scoped Persistence (`domain.storage`)

- **发送 / 写入与查询 (How to Send / Mutate & Query)**:
  - **写入类操作**（物理管道严格限定在本域专属表 `store_<domain>`）：
    ```python
    # 插入或更新
    await domain.storage.insert(
        record_id="ORD-1001",
        category="sales_order",
        data={"amount": 99.5, "status": "pending"},
        upsert=True
    )

    # 局部更新
    await domain.storage.update(record_id="ORD-1001", data={"status": "paid"}, patch=True)

    # 删除
    await domain.storage.delete(record_id="ORD-1001")
    ```
  - **查询操作**（支持查本域或通过 `domain` 跨域只读查询）：
    ```python
    # 查询本域
    my_orders = await domain.storage.query(category="sales_order", limit=50)

    # 跨域安全只读查询
    billing_data = await domain.storage.query(domain="billing", category="invoice", limit=20)
    ```
- **接收 / 响应结构 (How to Receive / Response)**:
  - 写入操作返回：`{"status": "ok", "record_id": "ORD-1001", "version": 1}`
  - 查询操作返回：记录对象数组 `[{"id": "ORD-1001", "category": "sales_order", "data": {...}, "updated_at": 1729220000}]`

---

### 3. Structured Logging (`domain.logger` & `@domain.on_log`)

- **发送 / 日志与进度上报 (How to Send / Emit)**:
  ```python
  # 1. 常规分级日志（走专用日志管道，与 stdout/stderr 诊断输出物理隔离）
  domain.logger.info("Order created", order_id="ORD-1001")
  domain.logger.warning("High payment latency", latency_ms=450)
  domain.logger.error("Database connection dropped")

  # 2. 阶段生命周期配对（用于长耗时链路 start/finish 对偶分析）
  domain.logger.phase("INFO", "order_validation", "Validating order items")

  # 3. 异步长任务进度上报
  domain.logger.progress("Indexing catalog", key="catalog_index", frac=0.65, waiting=False, done=False)

  # 4. 主动查询历史日志与活动进度
  logs = await domain.logger.query_logs(level="WARNING", limit=100)
  active_tasks = await domain.logger.query_progress()
  ```
- **接收 / 实时日志流监听 (How to Receive / Listen)**:
  ```python
  # 原生订阅日志流（瞬态直推，零 Receipt 负担，内核断连自动解绑）
  @domain.on_log(level="WARNING", source_domain="billing")
  def on_log_record(record):
      # record 包含: level, message, source_domain, layer, component, phase, ts
      print(f"[{record['level']}] [{record.get('source_domain')}]: {record['message']}")
  ```

---

### 4. Telemetry Metrics (`domain.telemetry` & `@domain.on_telemetry`)

- **发送 / 指标发射 (How to Send / Emit)**:
  ```python
  # 发射时序指标、传感器读数或业务计数
  domain.telemetry.emit(
      metric_name="order_latency_ms",
      value=125.4,
      tags={"region": "us-east", "env": "prod"}
  )
  ```
- **接收 / 实时遥测流监听 (How to Receive / Listen)**:
  ```python
  # 订阅全系统指标流（适用于仪表盘、监控域与传感器聚合）
  @domain.on_telemetry
  def on_metric(metric):
      # metric 包含: metric_name, value, tags, source_domain
      print(f"[{metric.get('source_domain')}] {metric.get('metric_name')} = {metric.get('value')}")
  ```

---

### 5. System Notifications (`domain.notify` & `@domain.on_notify`)

- **发送 / 告警发射 (How to Send / Dispatch)**:
  ```python
  # 方式 A：同步即发即弃（非阻塞，极高吞吐）
  domain.notify(
      level="error",                      # 级别：info / warning / error / critical
      message="磁盘使用率已超过 90%",        # 告警正文
      title="DiskSpaceAlert",             # 告警标题（可选）
      details={"mount": "/data", "used_percent": 91.5} # 扩展数据
  )

  # 方式 B：异步等待内核确认回执（awaitable）
  receipt = await domain.notify_async(
      level="warning",
      message="上游网关响应缓慢",
      title="GatewayLatencyAlert",
      details={"latency_ms": 1200}
  )
  # receipt: {"status": "delivered", "source": "billing", "notification_type": "GatewayLatencyAlert"}
  ```
- **接收 / 实时告警监听 (How to Receive / Listen)**:
  ```python
  # 原生监听全网告警与事件广播
  @domain.on_notify
  def on_alert(alert):
      # alert 包含: severity, message, notification_type, source_domain, details, params
      print(f"[{alert['severity'].upper()}] 来自 [{alert['source_domain']}]: {alert['message']}")
  ```

---

### 6. Runtime Assembly Topology (`domain.runtime`)

- **发送 / 装配查询 (How to Send / Query)**:
  ```python
  # 列出系统中所有受内核托管的域
  topology = await domain.runtime.list_domains()

  # 查询指定域的详细状态（含 OS 进程 PID、启动命令、重启计数等）
  domain_info = await domain.runtime.get_domain("worker_service")
  ```
- **接收 / 拓扑数据结构 (How to Receive / Response)**:
  ```python
  # domain_info 结构:
  {
      "domain_id": "worker_service",
      "status": "Active",
      "instance_id": 1,
      "routes": ["worker_service.process", "worker_service.health_check"],
      "process": {
          "pid": 48210,
          "command": "python",
          "args": ["worker.py"],
          "auto_restart": True,
          "restart_count": 0,
          "is_stopped": False
      }
  }
  ```

---

### 7. Message Lifecycle Observation (`domain.observer` & `@domain.on_observation`)

观察事件为允许丢失的瞬态流，不提供完整投递、持久化或跨重启重放保证。内核内部隔离慢订阅者并回收资源；SDK 对每个尚未完成的异步观察回调跳过后续观察事件，避免积压任务，完成后自动继续接收。其他回调与业务消息无需等待该回调。调用方式不变，无队列/背压管理参数；历史查询只能补查仍保留的窗口，不保证补齐缺口。Python/Node.js 行为一致。同步回调应及时返回，SDK 不会强行中断用户函数。

Durable 持久化故障通过现有 ExecutionFailed/SDK 标准异常反馈，业务域无需检查 SQLite 或管理补写；生命周期控制与账本恢复的内部故障由内核处理。

- **发送 / 快照检索与订阅控制 (How to Send / Query & Control)**:
  ```python
  # 检索最近消息四阶段流转快照（支持按 message_id, target, source_domain 过滤）
  snapshots = await domain.observer.query(limit=50, target="billing.charge")

  # 动态控制订阅
  await domain.observer.subscribe()
  await domain.observer.unsubscribe()
  ```
- **接收 / 实时观察流监听 (How to Receive / Listen)**:
  ```python
  # 原生监听全系统消息四阶段（Sent -> Received -> Started -> Completed）生命周期流
  @domain.on_observation
  def on_lifecycle_event(event):
      # event 包含: event_id, status, message_type, message_id, logical_id, source_domain, target, timestamp, details
      if event.get("status") == "ExecutionFailed":
          print(f"消息执行失败: {event.get('message_id')} -> {event.get('target')}")
  ```

---

### 8. Process Supervision & Rolling Update (`domain.supervisor`)

- **发送 / 进程控制与滚动更新指令 (How to Send / Command)**:
  ```python
  # 显式拉起、受控停止（不误触发崩溃自启）、重启
  await domain.supervisor.start("worker_service")
  await domain.supervisor.stop("worker_service")
  await domain.supervisor.restart("worker_service")

  # 平滑滚动更新：隔离候选 -> 注册及托管端口应用启动握手 -> 切换 / 失败保留旧实例
  res = await domain.supervisor.update(
      domain="worker_service",
      timeout_ms=5000,                  # 候选启动窗口
  )
  ```
- **接收 / 更新执行结果 (How to Receive / Response)**:
  - **成功（切换至新实例）**：
    ```python
    {"domain_id": "worker_service", "old_pid": 100, "new_pid": 101, "instance_id": 2, "status": "updated"}
    ```
  - **失败（自动回滚）**：
    ```python
    # SDK raises DomainRequestError: status=ExecutionFailed; data contains domain_id, status="rolled_back", error="update_failed", reason
    ```

---

## Cross-Domain Business Messaging

业务域之间的通信统一使用点对点 RPC 或单向多投递，经历严格的四阶段状态回执（`Sent -> Received -> Started -> Completed`）：

```python
import asyncio
from atelier_sdk import AtelierDomain, MSG_DELIVERY_DURABLE, MSG_DELIVERY_VOLATILE, MSG_DELIVERY_EPHEMERAL

domain = AtelierDomain(domain="order_service")

@domain.handler("create_order")
async def handle_create_order(msg):
    # 单目标请求并等待返回结果（综合 send + wait 并在同一个超时窗口释放资源）
    receipt = await domain.request("billing.charge", "Durable", data={"amount": 100}, timeout=10)

    # 多目标并发投递（返回内核分配的 message_id 列表）
    mids = await domain.send(
        targets=["analytics.record", "notification.push"],
        message_type=MSG_DELIVERY_DURABLE,
        params={},
        data={"order_id": msg.data["order_id"]}
    )
    results = await domain.wait_results(mids)

    return {"status": "success", "charge_data": receipt["data"]}

if __name__ == "__main__":
    domain.run()
```

---

## Event Stream Delivery Contract

系统事件流监听器（`@domain.on_log`、`@domain.on_telemetry`、`@domain.on_notify`、`@domain.on_observation`）遵循 `DeliveryPolicy::Ephemeral` 准则：
- **零 Receipt**：域绝不向上报 receipt 状态确认帧；
- **自动订阅协商**：SDK 在 `run` / `run_async` 阶段自动完成与内核的订阅握手；
- **故障自清理**：域异常退出或故障时，内核自动清理其订阅通道，杜绝内存泄漏。

实时日志订阅是允许缺口的尽力推送。慢异步日志回调只保留一个在途调用，忙时跳过后续推送，完成后自动继续；不积压任务，不影响其他订阅者。历史补查仅能读取已成功入库且仍保留的记录，本次不增加自动补查或可靠日志协议。

## Historical log pages

Use the existing log query method with optional `before_id` (Node also accepts `beforeId`). Read `logs`, `total` (page count), `has_more`, and `next_before_id`. Pass the last returned cursor to read older records with unchanged filters; stop when `has_more` is false. The default limit is 200, maximum 2000. Pages are bounded to 2 MiB; a single oversized record explicitly fails with its ID. History contains imported persistent records only; normal journal import can lag behind live log delivery. No query forces a journal flush. Real-time progress is not persisted history.

### Progress outcomes

`logger.progress` accepts optional `state`: `running`, `completed`, `failed`, or
`cancelled`. Explicit state takes precedence over legacy `done`. Missing/null state
maps done=true to completed, otherwise running. Terminal updates normalize
`done=true`, `waiting=false`, preserve `frac`, and remove the source-domain/key
from active progress before broadcasting. Reuse the original key to finish it.
Invalid states are rejected. Python state is keyword-only; Node uses options.

Events remain ephemeral and may be lost. A missing active key means no longer
active, not successful. QueryProgress refreshes active entries, not past outcomes.
Older kernels discard state; paired kernel/SDK upgrades are required for failure
semantics. Old UIs must not interpret done as success. Process-crash cleanup is
outside this change. No subscriber queue or retry management API is added.

域状态推送复用`on_observation`；`DomainStateSnapshot`的details为完整当前状态，`domain.observer.domain_states`提供只读缓存。无需状态轮询。契约见 ../../docs/DOMAIN_STATE_OBSERVATION.md。

## 0.1.14 托管端口与启动回调

启动配置的`ports`声明公开地址；域通过`domain.bindings`取得各绑定的`{host, port}`内部端点。Python在`run_async(on_start=...)`、Node在`runAsync({onStart})`回调内等待监听成功。SDK随后报告应用启动完成；无ports域不增加业务健康门槛。`probe_handler`/`probeHandler`仍可接受但不执行健康探针。文件启动的update不传spec时由内核重读配置，读取或候选启动失败保留旧实例。见[端口绑定](../../docs/PORT_BINDINGS.md)。


### Terminal waits without an execution deadline

Use `await domain.request(target, kind, data=data, timeout=None)` or `await domain.wait_result(message_id, timeout=None)` for long-running handlers. Message acceptance remains bounded (30 seconds for request); terminal waiting continues until a final receipt, task cancellation, or disconnection. Existing finite timeouts remain supported. Cancelling the local wait does not itself cancel remote work: use the owner domain's cancellation contract. Cancellation racing message acceptance is preserved.
