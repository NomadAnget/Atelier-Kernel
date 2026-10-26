# SDK responsibility boundary (Python / Node)

The kernel owns domain lifecycle, registered routes, authenticated source identity,
global receipt state, persistence, and process supervision. RegisterHandlers makes
the domain Active according to the kernel protocol. For port-bound domains only, the SDK reports application startup completion after
on_start/onStart has successfully opened its assigned endpoint. This is a transport
readiness handshake, not a business health endpoint or engine capability check.

The SDK is a protocol client: JSON-lines transport, handler invocation and protocol
receipts, asynchronous receipt correlation, awaitable kernel storage messages, and
cleanup of its local connection resources. Request timeout or cancellation only
stops local waiting; it does not undo remote work. No automatic business retry.

Business data validation, authorization rules, error payload shape, concurrency,
task queues, and dependency orchestration belong to domains or product assembly.
Handlers accept opaque JSON data. DomainExecutionFailed passes the domain's own
payload through; there is no required application error schema.

## Native Infrastructure Facilities

The SDK exposes 8 native infrastructure clients and listeners directly on the domain instance:
1. `domain.registry`: Handler discovery (`query_handlers` / `get_description`). The kernel strictly excludes all `kernel.*` internal targets from discovery results to prevent domain-to-domain pub-sub anti-patterns.
2. `domain.storage`: Scoped persistence (`insert`, `query`, `update`, `delete`). Write operations are strictly scoped to the domain's own table; queries can read other tables via `domain`.
3. `domain.logger`: Isolated structured logging via dedicated pipe (`debug`, `info`, `warn`, `error`, `phase`, `progress`). Listen via `@domain.on_log` / `domain.onLog`.
4. `domain.telemetry`: High-frequency metric emission (`domain.telemetry.emit`). Listen via `@domain.on_telemetry` / `domain.onTelemetry`.
5. `domain.notify`: Alert dispatching (`domain.notify`). Listen via `@domain.on_notify` / `domain.onNotify`.
6. `domain.runtime`: Runtime topology and process inspection (`list_domains`, `get_domain`, `query`).
7. `domain.observer`: Lifecycle snapshot queries (`query`) and global event stream listening via `@domain.on_observation` / `domain.onObservation`.
8. `domain.supervisor`: Process lifecycle control (`start`, `stop`, `restart`) and smooth rolling update with isolated candidate registration/startup handshake and failure rollback (`update`).

## Event Stream Delivery Contract

System event streams (`LogEvent`, `TelemetryEvent`, `NotifyEvent`, `ObservationEvent`) delivered from kernel pipes to registered listeners use `DeliveryPolicy::Ephemeral`:
- Zero receipt frames: Domains never report receipt acknowledgements (`received`, `started`, `completed`) for system push events.
- Automatic subscription negotiation: Listeners registered before `run` / `run_async` automatically send subscription messages during startup.
- Automatic teardown: When a subscriber domain terminates or faults, the kernel automatically unregisters its subscriptions to prevent stale pipes or memory leaks.

## Local Transport Safeguards

Only local transport safeguards remain: max_pending=1024, max_outbox=256,
max_frame_bytes=16777216, shutdown_grace=5 seconds (Node camelCase). These limit
connection resources, not business data shape or global task scheduling. Local
connection states new/open/draining/closed are private, not kernel domain states.
shutdown drains local handler calls and pending waits within a deadline, then
cancels cooperatively and calls the cleanup hook. It cannot kill arbitrary native
work; process termination and restart remain the kernel supervisor's job.
An IPC bridge that exits early cancels its cleanup deadline timer; a finished
connection must not retain the Node event loop until shutdown_grace expires.
When its cleanup deadline expires, the SDK terminates its own IPC bridge helper;
it never waits for the kernel input to close before completing an explicit shutdown.

Async storage helpers send existing kernel.storage CRUD messages and return kernel
response data. They do not implement storage, transactions, migration or paging.
send returns kernel-assigned ID(s) after initial receipts for all unique targets.
wait_result/wait_results consume results by ID; request_many is an alias for waiting
existing IDs, never a second send. request combines send/wait with one deadline.
Timeout/cancellation preserves a public wait slot; completed/failed result consumption
releases it. Explicit forget_result releases an unwanted slot. Unread results and
submissions share max_pending capacity and are never silently evicted.

Request context is a read-only snapshot of the current kernel envelope identity:
logical_id, message_id, source_domain, target and message_type. The SDK generates
no tracing identity, does not interpret or inject params._sdk, and does not add
correlation fields to logs. New outbound messages receive their identity from the
kernel. Local Future/Promise receipt correlation remains client plumbing.

Paired tests cover wire receipts, async RPC, large frames, resource bounds,
storage messages, opaque business payloads/errors, startup without a second gate,
and connection cleanup. Runtime integration is tested with the installed packages.


## 内部执行和观察资源

Python/Node 的现有调用保持不变。SDK 同时执行的入站业务调用有独立的内部上限（1024），不与出站请求额度关联；达到上限时经标准 ExecutionFailed 回执报告 `sdk_execution_capacity_exhausted`，不创建无限等待任务。该额度不代表业务调度或自动重试。同步阻塞或不响应取消的业务代码由进程停止隔离，SDK 不承诺能强制终止语言运行时中的任意函数。

观察与实时日志订阅仍为尽力副本，内核历史、每个订阅和进程出口均有内部资源上限。慢订阅、超大事件、订阅资源不足或断开可能产生缺口；不提供持久重放保证，不新增队列/背压/重连参数。普通日志的持久接收独立于这些副本，主库写入落后可由独立 WAL 缓冲及游标恢复。

## Port bindings and domain state (0.1.14)

`domain.bindings` exposes the kernel-assigned private endpoint for each configured public port binding. See [PORT_BINDINGS.md](docs/PORT_BINDINGS.md). `probe_handler` is compatibility-only and does not execute a health probe. Updates without explicit spec reread the source boot file.

`on_observation` / `onObservation` also receives DomainStatusChanged and DomainStateSnapshot. The former is best-effort; the latter retains the latest current-state image and coalesces slow delivery. Cache: Python `domain.observer.domain_states`, Node `domain.observer.domainStates`. No polling, persistent replay, or subscriber queue settings. See [DOMAIN_STATE_OBSERVATION.md](docs/DOMAIN_STATE_OBSERVATION.md). The snapshot does not make ordinary lifecycle observations lossless.
