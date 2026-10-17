# `atelier-sdk` (Python Domain SDK for Atelier Kernel)

Official zero-dependency Python SDK for building **Atelier Kernel** domains.

## Installation

```bash
pip install atelier_sdk-0.1.7-py3-none-any.whl
```

---

## 8 Native Infrastructure Facilities

The SDK exposes 8 native infrastructure facilities directly on `domain.*`, allowing domains to interact with kernel capabilities through language-native methods without treating kernel targets as business message handlers:

### 1. Registry & Handler Discovery (`domain.registry`)

Enables domains to discover registered handlers across the kernel and inspect their auto-extracted documentation comments. Discovery results strictly exclude all internal `kernel.*` targets.

```python
# Query active handlers across all domains (or filter by domain/target/handler)
handlers = await domain.registry.query_handlers(domain="billing", include_inactive=False)
print("Available routes:", handlers["routes"])

# Retrieve docstring / comment description for a target
description = await domain.registry.get_description("billing.charge")
```

### 2. Scoped Persistence (`domain.storage`)

"Storage as Messaging" with strict per-domain SQLite table isolation (`store_<domain>`). Write operations (`insert`, `update`, `delete`) are strictly scoped to the domain's own table; queries can safely perform read-only inspections of other domain tables.

```python
# Insert or upsert a record into the domain's own table
await domain.storage.insert(
    record_id="ORD-1001",
    category="sales_order",
    data={"amount": 99.5, "status": "pending"},
    upsert=True,
)

# Query records (scoped to own table, or cross-domain read via domain=...)
orders = await domain.storage.query(category="sales_order", limit=50)
billing_records = await domain.storage.query(domain="billing", category="invoice", limit=20)

# Partial or full update
await domain.storage.update(record_id="ORD-1001", data={"status": "paid"}, patch=True)

# Delete records
await domain.storage.delete(record_id="ORD-1001")
```

### 3. Structured Logging & Stream Listening (`domain.logger` & `@domain.on_log`)

Physical-pipe isolated structured logger. Prevents standard output pollution while supporting execution phase pairing, async progress tracking, and filtered event stream subscriptions.

```python
# Standard structured logging (outputs to kernel dedicated log pipe)
domain.logger.info("Order processed successfully", order_id="ORD-1001")
domain.logger.warning("Payment latency high", latency_ms=450)

# Lifecycle phase pairing (start/finish tracking)
domain.logger.phase("INFO", "order_validation", "Validating order schema")

# Long-running task progress tracking
domain.logger.progress("Indexing catalog", key="catalog_index", frac=0.65, waiting=False)

# Query historical logs and active progress
recent_logs = await domain.logger.query_logs(level="WARNING", limit=100)
active_tasks = await domain.logger.query_progress()

# Real-time log stream subscription (zero receipt frames, kernel auto-cleanup)
@domain.on_log(level="WARNING", source_domain="billing")
def handle_log_stream(record):
    print(f"[{record['level']}] from {record.get('source_domain')}: {record['message']}")
```

### 4. Telemetry & Metric Streaming (`domain.telemetry` & `@domain.on_telemetry`)

High-frequency metric ingestion and real-time subscription for monitoring, dashboards, and sensors.

```python
# Emit metric frames to kernel
domain.telemetry.emit("order_latency_ms", 125.4, tags={"region": "us-east"})

# Subscribe to real-time telemetry stream
@domain.on_telemetry
def handle_telemetry(metric):
    print(f"Metric: {metric.get('metric_name')} = {metric.get('value')}")
```

### 5. System Notifications & Alerts (`domain.notify` & `@domain.on_notify`)

System-wide alert dispatching and real-time notification broadcast listening.

```python
# Dispatch an alert (severity: info, warning, error, critical)
domain.notify("error", "Disk storage exceeded 90%", title="StorageAlert", details={"mount": "/data"})

# Subscribe to notifications
@domain.on_notify
def handle_notification(alert):
    print(f"[{alert.get('severity')}] {alert.get('message')}")
```

### 6. Runtime Topology & Assembly (`domain.runtime`)

Inspect live domain assembly, generation instance IDs, route registrations, and OS process information.

```python
# List all active domains in the system
topology = await domain.runtime.list_domains()
for item in topology:
    print(f"Domain: {item['domain_id']}, Status: {item['status']}, Routes: {item['routes']}")

# Get detailed info for a specific domain (PID, command, restart count)
info = await domain.runtime.get_domain("worker_service")
print("Process PID:", info.get("process", {}).get("pid"))
```

### 7. Message Lifecycle Observation (`domain.observer` & `@domain.on_observation`)

Inspect kernel message state snapshots and stream real-time message lifecycle transitions.

```python
# Query recent message snapshots
events = await domain.observer.query(limit=50, target="billing.charge")

# Real-time observation stream (ephemeral delivery, zero receipts)
@domain.on_observation
def handle_lifecycle(event):
    print(f"Message {event.get('message_id')} -> {event.get('target')}: state={event.get('state')}")
```

### 8. Process Supervision & Smooth Rolling Update (`domain.supervisor`)

Process control for starting, stopping, restarting domains, and executing smooth rolling updates with isolated candidate probing and automatic rollback.

```python
# Explicit lifecycle controls
await domain.supervisor.start("worker_service")
await domain.supervisor.stop("worker_service")       # Explicit stop will not trigger auto-restart
await domain.supervisor.restart("worker_service")

# Smooth rolling update with candidate probe and automatic zero-gap rollback
update_result = await domain.supervisor.update(
    domain="worker_service",
    probe_handler="health_check",     # Active probe validation on candidate instance
    timeout_ms=5000,                  # Candidate validation timeout window
)
print("Update status:", update_result)
```

---

## Cross-Domain Business Messaging

For communicating with other business domains, the SDK provides standard awaitable RPC and multi-target delivery with strict 4-stage receipts (`Sent -> Received -> Started -> Completed`):

```python
import asyncio
from atelier_sdk import AtelierDomain, MSG_DELIVERY_DURABLE, MSG_DELIVERY_VOLATILE, MSG_DELIVERY_EPHEMERAL

domain = AtelierDomain(domain="order_service")

@domain.handler("create_order")
async def handle_create_order(msg):
    # Await response from single target (combined send + wait under one deadline)
    receipt = await domain.request("billing.charge", "Charge", data={"amount": 100}, timeout=10)

    # Multi-target fanout (returns kernel-assigned message IDs)
    message_ids = await domain.send(
        targets=["analytics.record", "notification.push"],
        message_type=MSG_DELIVERY_DURABLE,
        params={},
        data={"order_id": msg.data["order_id"]},
    )
    results = await domain.wait_results(message_ids)

    return {"status": "success", "charge_receipt": receipt["data"]}

if __name__ == "__main__":
    domain.run()
```

---

## Event Stream Delivery Contract

System event listeners (`@domain.on_log`, `@domain.on_telemetry`, `@domain.on_notify`, `@domain.on_observation`) use `DeliveryPolicy::Ephemeral`:
- **Zero receipts**: Domains never report receipt acknowledgements (`received`, `started`, `completed`) for pushed system events.
- **Auto-subscription**: SDK negotiates subscription handshake with the kernel during `run()` / `run_async()`.
- **Fault-safe**: When a domain exits or crashes, the kernel automatically unregisters its push subscriptions.
