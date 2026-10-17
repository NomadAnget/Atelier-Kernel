# `atelier-sdk` (Python Domain SDK for Atelier Kernel)

Official zero-dependency Python SDK for building **Atelier Kernel** domains.

## Installation

```bash
pip install atelier_sdk-0.1.6-py3-none-any.whl
```

## Quick Start

```python
import asyncio
from atelier_sdk import AtelierDomain

domain = AtelierDomain(domain="orders")

@domain.handler("create")
async def on_create(msg):
    order_id = msg.data.get("order_id", "ORD-001")
    await domain.storage_insert_async(record_id=order_id, category="sales_order", data=msg.data)
    return {"ok": True, "order_id": order_id}

if __name__ == "__main__":
    asyncio.run(domain.run_async())
```

Log records returned by the kernel observer include source_domain (kernel-bound IPC owner, kernel for internal records, null for historical records before v0.1.1). SDK callers do not need to supply it; a claimed source is overwritten by the kernel.
# Async handlers and request/response

```python
import asyncio
from atelier_sdk import AtelierDomain, DomainRequestError

domain = AtelierDomain(domain="task")

@domain.handler("run")
async def run(msg):
    receipt = await domain.request("engines.execute", "Execute", data=msg.data, timeout=30)
    return receipt["data"]

asyncio.run(domain.run_async())
```

Use `run_async(on_start=..., on_close=...)` when handlers await requests. It continues receiving while handlers run. `request` accepts one target and returns the full completed receipt; `DomainRequestError` exposes `status`, `data`, and `message_id`. Timeout is in seconds, default 30. Terminal failures, timeout and EOF clear pending requests. Cancelling the waiting asyncio task clears its entry and propagates `CancelledError`; it does not cancel remote business work. `close()` ends the receive session. A domain object has one receive session. `run()` remains available. Sending and storage submission helpers now require await and return kernel IDs, not local sequence numbers. Offload blocking work with `asyncio.to_thread`; do not block the event loop.


## Send IDs, then wait

```python
message_id = await domain.send("engines.execute", "Execute", data=payload)
receipt = await domain.wait_result(message_id)

message_ids = await domain.send(["engines.prepare", "engines.render"], "Execute", data=payload)
results = await domain.wait_results(message_ids)  # input ID order, partial failures retained
```

A string target returns a string ID; a target list returns an ordered ID list after
kernel receipt acknowledgement, without waiting for business completion. Duplicate
targets are deduplicated in first-seen order. `request_many(ids)` aliases
`wait_results(ids)` and never sends messages. `request(target, ...)` combines send
and wait under one overall timeout, releasing its own resources on exit.

Early terminal receipts are retained until consumed by a wait. Successful waits
and remote terminal failures consume the local result slot; later waits return
`unknown_message`. Timeout/cancellation of wait_result retains the slot so callers
can wait again by the same ID. Use `forget_result(id)` to release an unwanted slot;
it refuses while a waiter is active. max_pending bounds submissions plus unread
result slots; full capacity rejects with busy, never silently evicts early results.
This is bounded client memory, not a second execution ledger or durable replay.

`await storage_insert/update/delete/query(...)` returns a single kernel message ID;
`await storage_*_async(...)` waits for the kernel operation and returns response data.
