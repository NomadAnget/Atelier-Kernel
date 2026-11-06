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

System event streams (`LogEvent`, `TelemetryEvent`, `NotifyEvent`, `ObservationEvent`, `KernelStats`) delivered from kernel pipes to registered listeners use `DeliveryPolicy::Ephemeral`:
- Zero receipt frames: Domains never report receipt acknowledgements (`received`, `started`, `completed`) for system push events.
- Explicit subscription: `on_log` / `onLog` and the other callbacks only register a **local** handler; they do not subscribe. A domain receives events only after it calls `subscribe_logs` / `subscribe_telemetry` / `subscribe_notify` / `subscribe_observer` (camelCase in Node), and should call the matching `unsubscribe_*` when done. There is no automatic subscription at `run` / `run_async` startup.
- Automatic teardown: When a subscriber domain terminates or faults, the kernel automatically unregisters its subscriptions to prevent stale subscriptions or memory leaks.
- 每条 `ObservationEvent` 的 `data.stats` 携带累计计数：`kernel_instance`、`sent_total`、`terminal_total`、`emitted_total`、`dropped_total`。**全部是自启动以来累计值，不是每秒增量**；消费方用相邻事件的差值算真速率，因事件有损而漏掉几条时，下一条携带的累计值会自动校正。没有额外的定时统计帧，也不需要轮询 `QueryRuntime`。`kernel_instance` 变化代表内核重启（累计值归零，须重置基线）。

## Local Transport Safeguards

SDK 只承担协议传输：连接两条命名端点、JSON-Lines 编解码、handler 分发与四阶段回执上报、回执关联。**它不实现任何策略**——没有默认超时、没有提交窗口、没有结果过期：

| 项 | Python | Node.js | 说明 |
| :--- | :--- | :--- | :--- |
| 超时 | `timeout=None`（默认不超时） | `options.timeout` 缺省 = 不超时 | 只有调用方显式传入才生效。超时只放弃本地等待，不取消内核侧执行，**也不丢弃已经写入内核那条消息的回执归属** |
| 结果寿命 | 取走或 `forget_result` | 取走或 `forgetResult` | 终态回执**不会自动过期**；未取走的结果占用的内存由域自己负责 |
| 构造参数 | 无 | 无 | `AtelierDomain(domain)`。历史上的 `max_pending` / `result_ttl` / `max_results` 全部删除——**不要再按它们调优** |

### 本地限制与内核终态的边界（硬约束）

内核是全系统唯一具备准入判定权的一方：每条真正提交给内核的消息都由内核给出明确终态。SDK 不得替内核判决，具体五条：

1. **没有默认超时**。不传 `timeout` 就一直等内核的初始回执与终态；只有调用方显式要求才允许本地放弃等待——这正是"SDK 不替内核决定这条消息失败"。
2. **已经写入就不许忘**。帧写进管道后，即使本地等待超时，SDK 仍保留该提交的回执归属（有界，`ORPHAN_LIMIT=4096`，先进先出），迟到的回执照样能关联到 `message_id` 并进入 `wait_result`。丢弃计入 `stats().dropped_orphan_receipts`，正常为 0。
3. **关闭后不许假装提交**。`shutdown()` 之后调用 `send` / `request` 立即以 `local_backpressure`（`data.reason="sdk closed"`、`submitted=false`）拒绝，不再"静默不写然后等超时"；被丢弃的帧计入 `stats().dropped_frames`。
4. **传输死亡不得沉默**。读端点 EOF 或读错误时，内核已经把该代际的在途消息收敛为 `ExecutionFailed`；SDK 必须以本地 `transport_closed` 立即终结所有等待（在途提交与正在等待的结果），不允许让调用方挂到自己的超时。`stats().transport_closed` 随之为真。
5. **一切丢弃都要可观测**。`stats()` 暴露十个计数：`pending` / `results` / `orphans` / `dropped_orphan_receipts` / `local_backpressure` / `unmatched_receipts` / `malformed_frames` / `unknown_statuses` / `dropped_frames` / `transport_closed`。它们只描述 SDK 自己的内存与本地失败，不代表内核状态。

### 两处"用内核词汇说话"的说明

- `_dispatch` 在域内**没有该 handler** 时回 `target_invalid`：路由仍在，是**本域自己**对无法处理的消息给出的回执，不是内核的准入裁决。
- handler 抛出的非 `DomainExecutionFailed` 异常由 SDK 兜底上报 `execution_failed`，载荷固定为 `{error:"handler_exception", reason}`。这是 SDK 定义的兜底 schema；业务需要自己的错误结构时应显式抛 `DomainExecutionFailed`。
- 回执状态若既不在四阶段（`sent`/`received`/`started`）也不在终态集合内，计入 `stats().unknown_statuses` 并丢弃，绝不静默当作"非终态"永久等待；找不到归属的回执计入 `unmatched_receipts`，无法解析的帧计入 `malformed_frames`。

本地状态与内核状态用词分离：`local_backpressure`（端到端已关闭，帧从未写入）、`transport_closed`（读端点 EOF/错误）、`result_in_use`（`forget_result` 时该结果仍有等待者）、`unknown_message`（id 从未在此 SDK 实例出现过）——**这四个都不是内核终态**，不会被内核发出，也不代表内核对这些消息的任何判断。

> `local_backpressure` 现在只有一个来源：端到端已关闭（`shutdown()` 或传输断开）。它表示帧**从未交给内核**，与 `completed` / `execution_failed` / `target_invalid` / `contract_invalid` 这些内核状态严格区分，调用方可以据此安全重试。孤儿保留上限是模块常量，不是构造参数。

`shutdown()` 是协作式的：把实例标记为已关闭、结束 `run` / `runAsync` 主循环、销毁端点流并让出事件循环。**它不会中断语言运行时里正在执行的业务函数**；进程停止与重启是内核 supervisor 的职责。SDK 没有独立的入站并发配额，也没有可配置的排空宽限期。

Async storage helpers send existing kernel.storage CRUD messages and return kernel
response data. They do not implement storage, transactions, migration or paging.
send returns kernel-assigned ID(s) after initial receipts for all unique targets: a
string target yields one id, a target list yields an id list.
wait_result/wait_results consume results by ID and never trigger a second send.
request combines send/wait for a single target.
Timeout is a caller decision, not an SDK policy: without an explicit timeout the SDK
waits for the kernel's receipt, and a local timeout only stops local waiting while the
receipt correlation is kept (bounded orphan retention). Results do not expire; consume
them with wait_result or release them with forget_result. `stats()` exposes transport
counters only and never represents kernel state.

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

## 域侧端点与 SDK 形态

**传输**：内核不向域传递 fd/HANDLE，而是创建两条**单向命名端点**，由域主动连接：

| 环境变量 | 方向 | 域侧打开方式 |
|---|---|---|
| `ATELIER_IPC_IN` | 内核 → 域 | Python：Windows `open(IN, "rb", buffering=0)`；Unix `socket(AF_UNIX).connect(IN)` 后 `makefile("rb", buffering=0)`。Node：`net.createConnection(IN)` |
| `ATELIER_IPC_OUT` | 域 → 内核 | Python：Windows `open(OUT, "wb", buffering=0)`；Unix 同上再 `makefile("wb", buffering=0)`。Node：`net.createConnection(OUT)` |

- Windows 用命名管道（`\\.\pipe\atelier-<domain>-<nonce>-{in,out}`），可以用文件 API 直接开；
- Unix 是 domain socket，**必须 `connect()`**：`open()` 对 socket 文件会失败。自带客户端时别照抄 Windows 的写法；
- 两条端点各自单向——**不要**合成一条双向连接：Windows 上同一句柄的阻塞读会串行化写；
- 域在 `on_start` 完成后，若带 `ATELIER_MANAGED_READY=1`，必须发 `{"frame_type":"ready"}` 帧，内核据此激活端口转发。

**SDK**：单层 `AtelierDomain`、全异步、无门面客户端类、无同步 API、无 `_async` 别名。旧版 `domain.registry` / `domain.supervisor` / `attach_stdlib_logging` / `discover_handlers` / `get_handler_description` / `close` 均已移除，对应能力改为上表中的主类方法。

