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
storage messages, opaque business payloads/errors, startup without a business health gate (port-bound domains require startup completion),
and connection cleanup. Runtime integration is tested with the installed packages.


## 内部执行和观察资源

Python/Node 的现有调用保持不变。SDK 同时执行的入站业务调用有独立的内部上限（1024），不与出站请求额度关联；达到上限时经标准 ExecutionFailed 回执报告 `sdk_execution_capacity_exhausted`，不创建无限等待任务。该额度不代表业务调度或自动重试。同步阻塞或不响应取消的业务代码由进程停止隔离，SDK 不承诺能强制终止语言运行时中的任意函数。

观察与实时日志订阅仍为尽力副本，内核历史、每个订阅和进程出口均有内部资源上限。慢订阅、超大事件、订阅资源不足或断开可能产生缺口；不提供持久重放保证，不新增队列/背压/重连参数。普通日志的持久接收独立于这些副本，主库写入落后可由独立 WAL 缓冲及游标恢复。

## Port bindings and domain state (0.1.14)

`domain.bindings` exposes the kernel-assigned private endpoint for each configured public port binding. See [PORT_BINDINGS.md](docs/PORT_BINDINGS.md). `probe_handler` is compatibility-only and does not execute a health probe. Updates without explicit spec reread the source boot file.

`on_observation` / `onObservation` also receives DomainStatusChanged and DomainStateSnapshot. The former is best-effort; the latter retains the latest current-state image and coalesces slow delivery. Cache: Python `domain.observer.domain_states`, Node `domain.observer.domainStates`. No polling, persistent replay, or subscriber queue settings. See [DOMAIN_STATE_OBSERVATION.md](docs/DOMAIN_STATE_OBSERVATION.md). The snapshot does not make ordinary lifecycle observations lossless.

## C4 virtual file namespace (`vfs://`)

The kernel storage hub indexes `(domain, virtual_path) → object_id → physical_path`. Physical files are flat unique-id files under `<project_root>/<domain>/<object_id>`; the domain's directory tree is virtual and lives in the kernel index.

SDK file interception is **scoped to an explicit `vfs://` prefix** — nothing else is intercepted, so dependency libraries keep native file semantics:

```python
domain.enable_file_interception()        # Python; root defaults to ATELIER_FILES_ROOT
await domain.refresh_file_index()        # pull this domain's index into the local read cache
with open("vfs://users/a.png", "wb") as f:
    f.write(payload)
object_id = domain.file_object_id("vfs://users/a.png")
```

```js
domain.enableFileInterception();          // Node
await domain.refreshFileIndex();
fs.writeFileSync("vfs://users/a.png", payload);
```

Rules:

- Only `vfs://` paths are resolved; `open()`/`fs.*` on any other path is untouched.
- Writes generate a globally-unique `object_id`, write the flat file, and fire-and-forget register `virtual_path → object_id`; reads resolve from the local index cache.
- The SDK keeps the native handle's lifecycle (no wrapper, no close hook). `size` and file existence are taken by the kernel on `resolve` (metadata + self-heal).
- Cross-domain consumers pass the `object_id` in message `data` and resolve it via `ObjectGet`; the returned `physical_path` is opened natively.
- `atelier-kernel --check` statically fails any domain source file whose file operations use a string-literal path without the `vfs://` prefix. Third-party libraries are not scanned and not prefix-matched.
- No OS sandbox: C extensions / native addons / subprocesses bypass language-level wrapping, so centralization is a convention, not a hard boundary.

## API semantics and naming (both SDKs)

Two primitives, one rule:

| Primitive | Returns | Use for |
|---|---|---|
| `send(targets, type, params, data)` | message id(s) | fire-and-forget, pipelining, callback-driven flows |
| `request(target, type, params, data)` | the terminal result's `data` | request/response, business CRUD, queries |

Business APIs MUST use `request` semantics and return data; `send` is the explicit low-level escape hatch. `send`-returning-id variants of business APIs are removed.

Naming rules:

- Python uses `snake_case`, Node uses `camelCase`; the same capability maps to the same name after case conversion.
- No `_async` / `Async` suffixes: both SDKs are async, the suffix is legacy and must not be added.
- Every capability below exists on both sides with identical semantics.

| Capability | Python | Node |
|---|---|---|
| lifecycle | `run_async(on_start=, on_close=)` / `shutdown` | `runAsync({onStart, onClose})` / `run` / `shutdown` |
| handler | `handler(name)`（docstring 注册描述） | `handler(name, fn, description?)` |
| send / request | `send` / `request` | `send` / `request` |
| wait / forget | `wait_result` / `wait_results` / `forget_result` | `waitResult` / `forgetResult` |
| report receipt | `report_receipt` | `reportReceipt` |
| events | `on_receipt/log/notify/observation/telemetry` | `onReceipt/Log/Notify/Observation/Telemetry` |
| storage CRUD | `storage_insert/update/delete/query` | `storageInsert/Update/Delete/Query` |
| queries | `query_logs/handlers/runtime/observer/progress` | `queryLogs/Handlers/Runtime/Observer/Progress` |
| supervisor | `supervisor_start/stop/restart/update` | `supervisorStart/Stop/Restart/Update` |
| subscribe | `subscribe_*` / `unsubscribe_*` | `subscribe*` / `unsubscribe*` |
| notify | `notify` | `notify` |
| C4 objects | `enable_file_interception` / `put_object` / `get_object` / `delete_object` / `file_object_id` / `refresh_file_index` | `enableFileInterception` / `putObject` / `getObject` / `deleteObject` / `fileObjectId` / `refreshFileIndex` |
| ports | `bindings` | `bindings` |
| logger | `logger` | `logger` |

## v0.2.0：传输层与 SDK 重写

**传输**：内核不再向域传递 fd/HANDLE，改为创建两条**单向命名端点**并由域主动连接：

| 环境变量 | 方向 | 域侧打开方式 |
|---|---|---|
| `ATELIER_IPC_IN` | 内核 → 域 | Python `open(IN, "rb", buffering=0)`；Node `net.createConnection(IN)` |
| `ATELIER_IPC_OUT` | 域 → 内核 | Python `open(OUT, "wb", buffering=0)`；Node `net.createConnection(OUT)` |

- Windows 用命名管道（`\\.\pipe\atelier-<domain>-<nonce>-{in,out}`），Unix 用 domain socket；
- 两条管道各自单向——**不要**合成一条双向管道：Windows 上同一句柄的阻塞读会串行化写；
- 域在 `on_start` 完成后，若带 `ATELIER_MANAGED_READY=1`，必须发 `{"frame_type":"ready"}` 帧，内核据此激活端口转发。

**SDK**：单层 `AtelierDomain`、全异步、无门面客户端类、无同步 API、无 `_async` 别名。旧版 `domain.registry` / `domain.supervisor` / `attach_stdlib_logging` / `discover_handlers` / `get_handler_description` / `close` 均已移除，对应能力改为上表中的主类方法。

