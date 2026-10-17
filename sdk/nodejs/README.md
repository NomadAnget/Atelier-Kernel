# `atelier-sdk` (Node.js / TypeScript Domain SDK for Atelier Kernel)

Official zero-dependency Node.js / TypeScript SDK for building **Atelier Kernel** domains.

## Installation

```bash
npm install atelier-sdk-0.1.7.tgz
```

---

## 8 Native Infrastructure Facilities

The SDK exposes 8 native infrastructure facilities directly on `domain.*`, allowing domains to interact with kernel capabilities through language-native methods without treating kernel targets as business message handlers:

### 1. Registry & Handler Discovery (`domain.registry`)

Enables domains to discover registered handlers across the kernel and inspect their auto-extracted documentation comments. Discovery results strictly exclude all internal `kernel.*` targets.

```javascript
// Query active handlers across all domains (or filter by domain/target/handler)
const handlers = await domain.registry.queryHandlers({ domain: "billing", includeInactive: false });
console.log("Available routes:", handlers.routes);

// Retrieve docstring / comment description for a target
const description = await domain.registry.getDescription("billing.charge");
```

### 2. Scoped Persistence (`domain.storage`)

"Storage as Messaging" with strict per-domain SQLite table isolation (`store_<domain>`). Write operations (`insert`, `update`, `delete`) are strictly scoped to the domain's own table; queries can safely perform read-only inspections of other domain tables.

```javascript
// Insert or upsert a record into the domain's own table
await domain.storage.insert(
  "ORD-1001",
  "sales_order",
  { amount: 99.5, status: "pending" },
  true // upsert
);

// Query records (scoped to own table, or cross-domain read via domain: ...)
const orders = await domain.storage.query({ category: "sales_order", limit: 50 });
const billingRecords = await domain.storage.query({ domain: "billing", category: "invoice", limit: 20 });

// Partial or full update
await domain.storage.update("ORD-1001", { status: "paid" }, true);

// Delete records
await domain.storage.delete({ recordId: "ORD-1001" });
```

### 3. Structured Logging & Stream Listening (`domain.logger` & `domain.onLog`)

Physical-pipe isolated structured logger. Prevents standard output pollution while supporting execution phase pairing, async progress tracking, and filtered event stream subscriptions.

```javascript
// Standard structured logging (outputs to kernel dedicated log pipe)
domain.logger.info("Order processed successfully", { orderId: "ORD-1001" });
domain.logger.warn("Payment latency high", { latencyMs: 450 });

// Lifecycle phase pairing (start/finish tracking)
domain.logger.phase("INFO", "order_validation", "Validating order schema");

// Long-running task progress tracking
domain.logger.progress("Indexing catalog", { key: "catalog_index", frac: 0.65, waiting: false });

// Query historical logs and active progress
const recentLogs = await domain.logger.queryLogs({ level: "WARNING", limit: 100 });
const activeTasks = await domain.logger.queryProgress();

// Real-time log stream subscription (zero receipt frames, kernel auto-cleanup)
domain.onLog((record) => {
  console.log(`[${record.level}] from ${record.source_domain}: ${record.message}`);
}, { level: "WARNING", sourceDomain: "billing" });
```

### 4. Telemetry & Metric Streaming (`domain.telemetry` & `domain.onTelemetry`)

High-frequency metric ingestion and real-time subscription for monitoring, dashboards, and sensors.

```javascript
// Emit metric frames to kernel
domain.telemetry.emit("order_latency_ms", 125.4, { region: "us-east" });

// Subscribe to real-time telemetry stream
domain.onTelemetry((metric) => {
  console.log(`Metric: ${metric.metric_name || metric.name} = ${metric.value}`);
});
```

### 5. System Notifications & Alerts (`domain.notify` & `domain.onNotify`)

System-wide alert dispatching and real-time notification broadcast listening.

```javascript
// Dispatch an alert (severity: info, warning, error, critical)
domain.notify("error", "Disk storage exceeded 90%", "StorageAlert", { mount: "/data" });

// Subscribe to notifications
domain.onNotify((alert) => {
  console.log(`[${alert.severity}] ${alert.message}`);
});
```

### 6. Runtime Topology & Assembly (`domain.runtime`)

Inspect live domain assembly, generation instance IDs, route registrations, and OS process information.

```javascript
// List all active domains in the system
const topology = await domain.runtime.listDomains();
for (const item of topology) {
  console.log(`Domain: ${item.domain_id}, Status: ${item.status}, Routes: ${item.routes}`);
}

// Get detailed info for a specific domain (PID, command, restart count)
const info = await domain.runtime.getDomain("worker_service");
console.log("Process PID:", info?.process?.pid);
```

### 7. Message Lifecycle Observation (`domain.observer` & `domain.onObservation`)

Inspect kernel message state snapshots and stream real-time message lifecycle transitions.

```javascript
// Query recent message snapshots
const events = await domain.observer.query({ limit: 50, target: "billing.charge" });

// Real-time observation stream (ephemeral delivery, zero receipts)
domain.onObservation((event) => {
  console.log(`Message ${event.message_id} -> ${event.target}: state=${event.state}`);
});
```

### 8. Process Supervision & Smooth Rolling Update (`domain.supervisor`)

Process control for starting, stopping, restarting domains, and executing smooth rolling updates with isolated candidate probing and automatic rollback.

```javascript
// Explicit lifecycle controls
await domain.supervisor.start("worker_service");
await domain.supervisor.stop("worker_service");       // Explicit stop will not trigger auto-restart
await domain.supervisor.restart("worker_service");

// Smooth rolling update with candidate probe and automatic zero-gap rollback
const updateResult = await domain.supervisor.update("worker_service", {
  probeHandler: "healthCheck",     // Active probe validation on candidate instance
  timeoutMs: 5000,                 // Candidate validation timeout window
});
console.log("Update status:", updateResult);
```

---

## Cross-Domain Business Messaging

For communicating with other business domains, the SDK provides standard awaitable RPC and multi-target delivery with strict 4-stage receipts (`Sent -> Received -> Started -> Completed`):

```javascript
const { AtelierDomain, MSG_DELIVERY_DURABLE, MSG_DELIVERY_VOLATILE, MSG_DELIVERY_EPHEMERAL } = require("atelier-sdk");

const domain = new AtelierDomain({ domain: "order_service" });

domain.handler("create_order", async (msg) => {
  // Await response from single target (combined send + wait under one deadline)
  const receipt = await domain.request("billing.charge", "Charge", {}, { amount: 100 }, { timeout: 10 });

  // Multi-target fanout (returns kernel-assigned message IDs)
  const messageIds = await domain.send(
    ["analytics.record", "notification.push"],
    MSG_DELIVERY_DURABLE,
    {},
    { order_id: msg.data.order_id }
  );
  const results = await domain.waitResults(messageIds);

  return { status: "success", chargeReceipt: receipt.data };
});

domain.run();
```

---

## Event Stream Delivery Contract

System event listeners (`domain.onLog`, `domain.onTelemetry`, `domain.onNotify`, `domain.onObservation`) use `DeliveryPolicy::Ephemeral`:
- **Zero receipts**: Domains never report receipt acknowledgements (`received`, `started`, `completed`) for pushed system events.
- **Auto-subscription**: SDK negotiates subscription handshake with the kernel during `run()` / `runAsync()`.
- **Fault-safe**: When a domain exits or crashes, the kernel automatically unregisters its push subscriptions.
