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

8 个内核内置目标（`kernel.*`）全部由**单层 `AtelierDomain` 上的方法**提供，没有 `domain.registry` / `domain.storage` / `domain.supervisor` 之类的门面子对象（见本节末尾的"已移除"清单）。

| 内核目标 | 能力 | Python | Node.js |
| :--- | :--- | :--- | :--- |
| `kernel.registry` | 处理器发现（结果严格排除 `kernel.*`） | `query_handlers()` | `queryHandlers()` |
| `kernel.storage` | 按域持久化（写严格限本域） | `storage_insert` / `storage_update` / `storage_delete` / `storage_query` | `storageInsert` / `storageUpdate` / `storageDelete` / `storageQuery` |
| `kernel.logger` | 结构化日志、进度、查询与订阅 | `logger.debug/info/warn/error`、`query_logs`、`query_progress`、`subscribe_logs` / `unsubscribe_logs` | `logger.debug/info/warn/error`、`queryLogs`、`queryProgress`、`subscribeLogs` / `unsubscribeLogs` |
| `kernel.telemetry` | 高频遥测（仅订阅，内核侧发出） | `subscribe_telemetry` / `unsubscribe_telemetry`、`on_telemetry` | `subscribeTelemetry` / `unsubscribeTelemetry`、`onTelemetry` |
| `kernel.notify` | 告警广播 | `notify(level, message, title, details)`、`subscribe_notify` / `unsubscribe_notify` | `notify(...)`、`subscribeNotify` / `unsubscribeNotify` |
| `kernel.runtime` | 运行态拓扑查询 | `query_runtime()` | `queryRuntime()` |
| `kernel.observer` | 生命周期快照与事件流 | `query_observer()`、`subscribe_observer` / `unsubscribe_observer`、`on_observation` | `queryObserver()`、`subscribeObserver` / `unsubscribeObserver`、`onObservation` |
| `kernel.supervisor` | 域进程管控与滚动更新 | `supervisor_start` / `supervisor_stop` / `supervisor_restart` / `supervisor_update` | `supervisorStart` / `supervisorStop` / `supervisorRestart` / `supervisorUpdate` |

## Event Stream Delivery Contract

System event streams (`LogEvent`, `TelemetryEvent`, `NotifyEvent`, `ObservationEvent`) delivered from kernel pipes to registered listeners use `DeliveryPolicy::Ephemeral`:
- Zero receipt frames: Domains never report receipt acknowledgements (`received`, `started`, `completed`) for system push events.
- Explicit subscription: `on_log` / `onLog` and the other callbacks only register a **local** handler; they do not subscribe. A domain receives events only after it calls `subscribe_logs` / `subscribe_telemetry` / `subscribe_notify` / `subscribe_observer` (camelCase in Node), and should call the matching `unsubscribe_*` when done. There is no automatic subscription at `run` / `run_async` startup.
- Automatic teardown: When a subscriber domain terminates or faults, the kernel automatically unregisters its subscriptions to prevent stale subscriptions or memory leaks.

## Local Transport Safeguards

SDK 侧只有少量本地资源上限，用来约束自己的等待槽，与业务数据形状、全局调度无关：

| 项 | Python | Node.js | 说明 |
| :--- | :--- | :--- | :--- |
| 待处理提交 | `max_pending=1024` | `maxPending: 1024` | 未完成提交 + 未读取结果的槽位总数 |
| 保留结果 | `max_results=4096`、`result_ttl=30.0` 秒 | `resultTtlMs: 30000` | 没人等待的终态回执保留多久后被驱逐 |
| 单次请求超时 | `timeout=30` | `options.timeout`，默认 30 秒 | 超时只放弃本地等待，不取消内核侧执行 |

`shutdown()` 是协作式的：把实例标记为已关闭、结束 `run` / `runAsync` 主循环、销毁端点流并让出事件循环。**它不会中断语言运行时里正在执行的业务函数**；进程停止与重启是内核 supervisor 的职责。SDK 没有独立的入站并发配额，也没有可配置的排空宽限期。

Async storage helpers send existing kernel.storage CRUD messages and return kernel
response data. They do not implement storage, transactions, migration or paging.
send returns kernel-assigned ID(s) after initial receipts for all unique targets: a
string target yields one id, a target list yields an id list.
wait_result/wait_results consume results by ID and never trigger a second send.
request combines send/wait with one deadline and a single target.
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

Python/Node 的现有调用保持不变。SDK **没有独立的入站并发配额**，也不会因为本地容量拒绝内核投递；在途工作量由业务域自己控制（B6：handler 必须快速返回，长任务丢后台任务或队列）。同步阻塞或不响应取消的业务代码由进程停止隔离，SDK 不承诺能强制终止语言运行时中的任意函数。

观察与实时日志订阅仍为尽力副本，内核历史、每个订阅和进程出口均有内部资源上限。慢订阅、超大事件、订阅资源不足或断开可能产生缺口；不提供持久重放保证，不新增队列/背压/重连参数。普通日志的持久接收独立于这些副本，主库写入落后可由独立 WAL 缓冲及游标恢复。

## Port bindings and domain state

`domain.bindings` exposes the kernel-assigned private endpoint for each configured public port binding. See [端口绑定](capabilities/port-bindings.md). `probe_handler` is compatibility-only and does not execute a health probe. Updates without explicit spec reread the source boot file.

`on_observation` / `onObservation` also receives DomainStatusChanged and DomainStateSnapshot (after `subscribe_observer` / `subscribeObserver`). The former is best-effort; the latter carries the latest current-state image and coalesces slow delivery. **The SDK keeps no state cache** — there is no `domain.observer` sub-object and no `domain_states` / `domainStates`; a caller that needs the latest image must retain it from the events it receives. No polling, persistent replay, or subscriber queue settings. See [域状态观察](capabilities/domain-state-observation.md). The snapshot does not make ordinary lifecycle observations lossless.

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
| `send(targets, type, params, data)` | message id (string target) or id list (list target) | fire-and-forget, pipelining, callback-driven flows |
| `request(target, type, params, data)` | the **whole terminal receipt** (`{status, data, message_id, …}`) | request/response, business CRUD, queries |

`request` takes a **single** target string, not a list: it forwards to `send` and then waits on the returned id, so a list would hand `wait_result` a list instead of an id and fail with `unknown_message`. To wait on several results, use `send` + `wait_results(ids)`. Read the business payload from `receipt["data"]`; a non-`completed` terminal raises `DomainRequestError` carrying the real status.

Business APIs MUST use `request` semantics and return the terminal receipt's `data`; `send` is the explicit low-level escape hatch. `send`-returning-id variants of business APIs are removed.

Naming rules:

- Python uses `snake_case`, Node uses `camelCase`; the same capability maps to the same name after case conversion.
- No `_async` / `Async` suffixes: both SDKs are async, the suffix is legacy and must not be added.
- Every capability below exists on both sides with identical semantics.

| Capability | Python | Node |
|---|---|---|
| lifecycle | `run_async(on_start=, on_close=)` / `shutdown` | `runAsync({onStart, onClose})` / `run` / `shutdown` |
| handler | `handler(name)`（docstring 注册描述） | `handler(name, fn, description?)` |
| send / request | `send` / `request` | `send` / `request` |
| wait / forget | `wait_result` / `wait_results` / `forget_result` | `waitResult` / `waitResults` / `forgetResult` |
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

