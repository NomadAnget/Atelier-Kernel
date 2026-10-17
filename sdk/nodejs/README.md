# `atelier-sdk` (Node.js / TypeScript Domain SDK for Atelier Kernel)

Official zero-dependency Node.js / TypeScript SDK for building **Atelier Kernel** domains.

## Installation

```bash
npm install ./atelier-sdk-0.1.7.tgz
```

## Quick Start

```javascript
const { AtelierDomain, DomainExecutionFailed } = require("atelier-sdk");

const domain = new AtelierDomain({domain:'inventory'});

domain.handler("deduct", async (msg) => {
  const { sku, qty } = msg.data;
  await domain.storageInsertAsync(sku, "stock_item", { sku, deducted: qty });
  return { ok: true, sku, deducted: qty };
});

domain.runAsync().catch(error => { process.stderr.write(String(error)); process.exitCode = 1; });
```

Log records returned by the kernel observer include source_domain (kernel-bound IPC owner, kernel for internal records, null for historical records before v0.1.1). SDK callers do not need to supply it; a claimed source is overwritten by the kernel.
# Async handlers and request/response

```js
const { AtelierDomain, DomainRequestError } = require('atelier-sdk');
const domain = new AtelierDomain({ domain: 'task' });
domain.handler('run', async msg => {
  const receipt = await domain.request('engines.execute', 'Execute', {}, msg.data, { timeout: 30 });
  return receipt.data;
});
domain.runAsync().catch(error => { process.stderr.write(String(error)); process.exitCode = 1; });
```

`runAsync({onStart, onClose})` resolves when the receive session ends. Existing `run()` remains available. `request` accepts one target and returns its full completed receipt. `DomainRequestError` exposes `status`, `data`, and `message_id`; timeout is in seconds (default 30). Intermediate receipts do not resolve requests. Terminal failure, timeout, EOF and optional AbortSignal cancellation clean up pending entries. `close()` ends the session. Cancellation is local and does not cancel remote business work. Sending and storage submission helpers now return Promises of kernel message IDs, not local sequence numbers. Each domain object has one receive session.


## Send IDs, then wait

```js
const id = await domain.send('engines.execute', 'Execute', {}, payload);
const receipt = await domain.waitResult(id);
const ids = await domain.send(['engines.prepare', 'engines.render'], 'Execute', {}, payload);
const results = await domain.waitResults(ids);
```

A string target resolves to one ID; an array resolves to ordered IDs after kernel
receipt acknowledgement, without waiting for execution. Duplicate targets are
removed in first-seen order. requestMany(ids) aliases waitResults(ids), with no send.
request(target, ...) combines send/wait under one timeout and releases its resources.

Waits consume terminal results, including remote failures. Early results remain
available until consumed. Timeout/AbortSignal cancellation of waitResult preserves
the slot for another wait by ID. forgetResult(id) releases an unwanted slot but
refuses while a waiter is active. maxPending bounds submissions and unread results;
capacity rejection is busy, with no silent result eviction. Results are local,
non-durable client memory; the kernel owns message identity and execution state.

Await storageInsert/Update/Delete/Query for one message ID; await the corresponding
Async helper for kernel response data. All timeout values are seconds.
