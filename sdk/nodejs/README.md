# `atelier-sdk` (Node.js / TypeScript Domain SDK for Atelier Kernel)

Official zero-dependency Node.js / TypeScript SDK for building **Atelier Kernel** domains.

## Installation

```bash
npm install atelier-sdk-0.1.8.tgz
```

---

## 8 Native Infrastructure Facilities (Send & Receive Full Reference)

The SDK exposes 8 native infrastructure facilities directly on `domain.*`, allowing domains to interact with kernel capabilities through language-native methods.

For each facility below, both the **Sending / Invocations (How to Send)** and the **Receiving / Listening / Responses (How to Receive)** are documented with payload formats:

---

### 1. Registry & Discovery (`domain.registry`)

- **发送 / 调用 (How to Send / Invoke)**:
  - **自动报备**：在 `domain.run()` / `domain.runAsync()` 启动时，SDK 自动将 `domain.handler` 注册的处理器与自动提取的代码注释以 `RegisterHandlers` 发送给内核；退出时发送 `UnregisterHandlers`。
  - **服务发现查询**：
    ```javascript
    // 查询可用业务路由（支持 domain/target/handler 过滤，严格排除 kernel.*）
    const handlers = await domain.registry.queryHandlers({ domain: "billing", includeInactive: false });

    // 查询指定路由的方法文档注释
    const doc = await domain.registry.getDescription("billing.charge");
    ```
- **接收 / 响应结构 (How to Receive / Response)**:
  ```javascript
  // handlers 结构:
  {
    count: 2,
    routes: ["billing.charge", "billing.refund"],
    handlers: [
      { domain: "billing", handler: "charge", target: "billing.charge", description: "处理信用卡扣款" }
    ]
  }
  ```

---

### 2. Scoped Persistence (`domain.storage`)

- **发送 / 写入与查询 (How to Send / Mutate & Query)**:
  - **写入类操作**（物理管道严格限定在本域专属表 `store_<domain>`）：
    ```javascript
    // 插入或更新
    await domain.storage.insert(
      "ORD-1001",
      "sales_order",
      { amount: 99.5, status: "pending" },
      true // upsert
    );

    // 局部更新
    await domain.storage.update("ORD-1001", { status: "paid" }, true);

    // 删除
    await domain.storage.delete({ recordId: "ORD-1001" });
    ```
  - **查询操作**（支持查本域或通过 `domain` 跨域只读查询）：
    ```javascript
    // 查询本域
    const myOrders = await domain.storage.query({ category: "sales_order", limit: 50 });

    // 跨域安全只读查询
    const billingData = await domain.storage.query({ domain: "billing", category: "invoice", limit: 20 });
    ```
- **接收 / 响应结构 (How to Receive / Response)**:
  - 写入操作返回：`{ status: "ok", record_id: "ORD-1001", version: 1 }`
  - 查询操作返回：记录对象数组 `[ { id: "ORD-1001", category: "sales_order", data: {...}, updated_at: 1729220000 } ]`

---

### 3. Structured Logging (`domain.logger` & `domain.onLog`)

- **发送 / 日志与进度上报 (How to Send / Emit)**:
  ```javascript
  // 1. 常规分级日志（走专用日志管道，杜绝 stdout 协议帧污染）
  domain.logger.info("Order created", { orderId: "ORD-1001" });
  domain.logger.warn("High payment latency", { latencyMs: 450 });
  domain.logger.error("Database connection dropped");

  // 2. 阶段生命周期配对（用于长耗时链路 start/finish 对偶分析）
  domain.logger.phase("INFO", "order_validation", "Validating order items");

  // 3. 异步长任务进度上报
  domain.logger.progress("Indexing catalog", { key: "catalog_index", frac: 0.65, waiting: false, done: false });

  // 4. 主动查询历史日志与活动进度
  const logs = await domain.logger.queryLogs({ level: "WARNING", limit: 100 });
  const activeTasks = await domain.logger.queryProgress();
  ```
- **接收 / 实时日志流监听 (How to Receive / Listen)**:
  ```javascript
  // 原生订阅日志流（瞬态直推，零 Receipt 负担，内核断连自动解绑）
  domain.onLog((record) => {
    // record 包含: level, message, source_domain, layer, component, phase, ts
    console.log(`[${record.level}] [${record.source_domain}]: ${record.message}`);
  }, { level: "WARNING", sourceDomain: "billing" });
  ```

---

### 4. Telemetry Metrics (`domain.telemetry` & `domain.onTelemetry`)

- **发送 / 指标发射 (How to Send / Emit)**:
  ```javascript
  // 发射时序指标、传感器读数或业务计数
  domain.telemetry.emit("order_latency_ms", 125.4, { region: "us-east", env: "prod" });
  ```
- **接收 / 实时遥测流监听 (How to Receive / Listen)**:
  ```javascript
  // 订阅全系统指标流（适用于仪表盘、监控域与传感器聚合）
  domain.onTelemetry((metric) => {
    // metric 包含: metric_name, value, tags, source_domain
    console.log(`[${metric.source_domain}] ${metric.metric_name || metric.name} = ${metric.value}`);
  });
  ```

---

### 5. System Notifications (`domain.notify` & `domain.onNotify`)

- **发送 / 告警发射 (How to Send / Dispatch)**:
  ```javascript
  // 方式 A：同步即发即弃（非阻塞，极高吞吐）
  domain.notify(
    "error",                      // 级别：info / warning / error / critical
    "磁盘使用率已超过 90%",         // 告警正文
    "DiskSpaceAlert",             // 告警标题（可选）
    { mount: "/data", used_percent: 91.5 } // 扩展数据
  );

  // 方式 B：异步等待内核确认回执（awaitable）
  const receipt = await domain.notifyAsync(
    "warning",
    "上游网关响应缓慢",
    "GatewayLatencyAlert",
    { latencyMs: 1200 }
  );
  // receipt: { status: "delivered", source: "billing", notification_type: "GatewayLatencyAlert" }
  ```
- **接收 / 实时告警监听 (How to Receive / Listen)**:
  ```javascript
  // 原生监听全网告警与事件广播
  domain.onNotify((alert) => {
    // alert 包含: severity, message, notification_type, source_domain, details, params
    console.log(`[${alert.severity.toUpperCase()}] 来自 [${alert.source_domain}]: ${alert.message}`);
  });
  ```

---

### 6. Runtime Assembly Topology (`domain.runtime`)

- **发送 / 装配查询 (How to Send / Query)**:
  ```javascript
  // 列出系统中所有受内核托管的域
  const topology = await domain.runtime.listDomains();

  // 查询指定域的详细状态（含 OS 进程 PID、启动命令、重启计数等）
  const domainInfo = await domain.runtime.getDomain("worker_service");
  ```
- **接收 / 拓扑数据结构 (How to Receive / Response)**:
  ```javascript
  // domainInfo 结构:
  {
    domain_id: "worker_service",
    status: "Active",
    instance_id: 1,
    routes: ["worker_service.process", "worker_service.health_check"],
    process: {
      pid: 48210,
      command: "node",
      args: ["worker.js"],
      auto_restart: true,
      restart_count: 0,
      is_stopped: false
    }
  }
  ```

---

### 7. Message Lifecycle Observation (`domain.observer` & `domain.onObservation`)

观察事件为允许丢失的瞬态流，不提供完整投递、持久化或跨重启重放保证。内核内部隔离慢订阅者并回收资源；SDK 对每个尚未完成的异步观察回调跳过后续观察事件，避免积压任务，完成后自动继续接收。其他回调与业务消息无需等待该回调。调用方式不变，无队列/背压管理参数；历史查询只能补查仍保留的窗口，不保证补齐缺口。Python/Node.js 行为一致。同步回调应及时返回，SDK 不会强行中断用户函数。

Durable 持久化故障通过现有 ExecutionFailed/SDK 标准异常反馈，业务域无需检查 SQLite 或管理补写；生命周期控制与账本恢复的内部故障由内核处理。

- **发送 / 快照检索与订阅控制 (How to Send / Query & Control)**:
  ```javascript
  // 检索最近消息四阶段流转快照（支持按 messageId, target, sourceDomain 过滤）
  const snapshots = await domain.observer.query({ limit: 50, target: "billing.charge" });

  // 动态控制订阅
  await domain.observer.subscribe();
  await domain.observer.unsubscribe();
  ```
- **接收 / 实时观察流监听 (How to Receive / Listen)**:
  ```javascript
  // 原生监听全系统消息四阶段（Sent -> Received -> Started -> Completed）生命周期流
  domain.onObservation((event) => {
    // event 包含: topic, state, message_id, logical_id, source_domain, target, timestamp
    if (event.state === "ExecutionFailed") {
      console.log(`消息执行失败: ${event.message_id} -> ${event.target}`);
    }
  });
  ```

---

### 8. Process Supervision & Rolling Update (`domain.supervisor`)

- **发送 / 进程控制与滚动更新指令 (How to Send / Command)**:
  ```javascript
  // 显式拉起、受控停止（不误触发崩溃自启）、重启
  await domain.supervisor.start("worker_service");
  await domain.supervisor.stop("worker_service");
  await domain.supervisor.restart("worker_service");

  // 平滑滚动更新：隔离拉起候选进程 -> 探活探针验证 -> 原子路由切换与排干旧实例 / 自动回滚
  const res = await domain.supervisor.update("worker_service", {
    probeHandler: "healthCheck",     // 候选版本主动探活处理器
    timeoutMs: 5000,                 // 探活验证窗口
  });
  ```
- **接收 / 更新执行结果 (How to Receive / Response)**:
  - **成功（零服务间隙）**：
    ```javascript
    { success: true, domain: "worker_service", old_instance_id: 1, new_instance_id: 2, message: "Domain updated smoothly" }
    ```
  - **失败（自动回滚）**：
    ```javascript
    { success: false, domain: "worker_service", instance_id: 1, message: "Candidate probe check failed, automatically rolled back" }
    ```

---

## Cross-Domain Business Messaging

业务域之间的通信统一使用点对点 RPC 或单向多投递，经历严格的四阶段状态回执（`Sent -> Received -> Started -> Completed`）：

```javascript
const { AtelierDomain, MSG_DELIVERY_DURABLE, MSG_DELIVERY_VOLATILE, MSG_DELIVERY_EPHEMERAL } = require("atelier-sdk");

const domain = new AtelierDomain({ domain: "order_service" });

domain.handler("create_order", async (msg) => {
  // 单目标请求并等待返回结果（综合 send + wait 并在同一个超时窗口释放资源）
  const receipt = await domain.request("billing.charge", "Charge", {}, { amount: 100 }, { timeout: 10 });

  // 多目标并发投递（返回内核分配的 message_id 列表）
  const mids = await domain.send(
    ["analytics.record", "notification.push"],
    MSG_DELIVERY_DURABLE,
    {},
    { order_id: msg.data.order_id }
  );
  const results = await domain.waitResults(mids);

  return { status: "success", charge_data: receipt.data };
});

domain.run();
```

---

## Event Stream Delivery Contract

系统事件流监听器（`domain.onLog`、`domain.onTelemetry`、`domain.onNotify`、`domain.onObservation`）遵循 `DeliveryPolicy::Ephemeral` 准则：
- **零 Receipt**：域绝不向上报 receipt 状态确认帧；
- **自动订阅协商**：SDK 在 `run()` / `runAsync()` 阶段自动完成与内核的订阅握手；
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
