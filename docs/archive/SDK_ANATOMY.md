> **归档（历史记录）**：本文档记录当时的设计、验收或发布状态，**不代表当前契约**。
> 当前契约、能力与 SDK 入口见 [../INDEX.md](../INDEX.md)。

# SDK 解剖：能力、结构、耦合

> 基线：`sdk/python`（1960 行 / 7 文件 / 16 类）、`sdk/nodejs`（1599 行 / 8 文件）。
> 目的：在做「收敛成单层」之前，先把现状讲清楚。

## 1. SDK 到底干了什么

| 层 | Python | Node | 职责 |
|---|---|---|---|
| 对外主类 | `AtelierDomain`（`__init__.py`） | `AtelierDomain`（`index.js`） | 生命周期、处理器注册、消息、存储、查询、监督、订阅、C4、日志器 |
| 运行时 mixin | `AsyncRuntime`（`_runtime.py`） | `runtime.js` | 管道读写、帧分发、回执上报、处理器执行、存储/查询/监督/订阅的底层实现 |
| 消息 mixin | `MessageClient`（`_messages.py`） | `messages.js` | `send`/`request`/`wait`/`forget`、`_pending`/`_results`、`busy` 背压 |
| 门面客户端 | `DomainStorageClient` / `DomainSupervisorClient` / `DomainRegistryClient` / `DomainRuntimeClient` / `DomainObserverClient` / `DomainTelemetry` / `DomainLogger` | 同名类 | 给业务代码按领域分组调用的入口 |
| 数据结构 | `DeliveryMessage` | 普通对象 | 消息视图（`data`/`params`/`target`/…） |
| 文件命名空间 | `FileNamespace`（`_files.py`） | `FileNamespace`（`files.js`） | `vfs://` 前缀判定、虚拟路径→平铺 id、登记、反查 |
| 编解码 | `_codec.py` | `codec.js` | 文件对象 ↔ `{__atelier_file__: object_id}` |
| ESM 拦截 | — | `vfs-loader/vfs-hooks/vfs-shim.mjs` | 让 `import {writeFile} from 'node:fs/promises'` 命中 vfs |

## 2. 对外真正需要的能力（最小集）

| # | 能力 | 说明 |
|---|---|---|
| 1 | 生命周期 | `run` / `shutdown` |
| 2 | 注册处理器 | `handler(name)` |
| 3 | 发消息 | `send`（返回 id） |
| 4 | 请求-响应 | `request`（返回数据） |
| 5 | 等待/遗忘 | `wait_result` / `forget_result` |
| 6 | 上报回执 | `report_receipt` |
| 7 | 事件回调 | `on_receipt` / `on_log` / `on_notify` / `on_observation` / `on_telemetry` |
| 8 | 本域存储 | `storage_insert/update/delete/query`（返回数据） |
| 9 | 查询 | `query_logs` / `query_handlers` / `query_runtime` / `query_observer` / `query_progress` |
| 10 | 监督 | `supervisor_start/stop/restart/update` |
| 11 | 订阅 | `subscribe_logs/telemetry/notify/observer`（+unsubscribe） |
| 12 | 通知 | `notify` |
| 13 | C4 对象 | `put_object` / `get_object` / `delete_object` / `file_object_id` / `enable_file_interception` / `refresh_file_index` |
| 14 | 日志 | `logger`（debug/info/warn/error）、`attach_stdlib_logging` |
| 15 | 端口 | `bindings` |

**共 15 项能力**，这就是对外契约的全部。

## 3. 内部耦合现状

### 3.1 三层委托
```
AtelierDomain.storage_insert()      （第 1 层，公开）
  └─ AsyncRuntime.storage_insert()  （第 2 层，mixin 实现）
DomainStorageClient.insert()        （另一条入口，转发回 domain）
```
同一个能力至少两条调用路径。

### 3.2 同名能力重复（不同类里）
| 名字 | 出现位置 | 语义 |
|---|---|---|
| `query_logs` | `DomainLogger`（同步）+ `AtelierDomain`（异步）+ `_runtime.py`（异步） | **不一致** |
| `query_progress` | `DomainLogger`（同步+异步）+ `AtelierDomain`（异步） | **不一致** |
| `storage_insert` | `AtelierDomain` + `_runtime.py` 的 `_async` 版 | 曾经双份 |

### 3.3 同步/异步双版
`DomainLogger` 里每个查询都有同步版和 `_async` 版；`AtelierDomain` 只有异步版。业务代码不知道该用哪个。

### 3.4 全局状态
- Python：`FileNamespace` 在 `install()` 里**全局替换 `builtins.open`**；
- Node：`vfs-shim.mjs` 读 `globalThis.__atelierVfs`；
- 编解码 `_codec` 在 Python 里读模块级 `_NAMESPACE` 全局变量。
- 这三处是**进程级单例**，无法多域共存于同一进程（当前每域独立进程，暂时没问题）。

### 3.5 循环引用
`DomainLogger` 持有 `_domain`，`AtelierDomain.logger` 又持有 `DomainLogger`。`DomainStorageClient` 等也是同样结构。

### 3.6 编解码与回执/背压耦合
`_codec.decode` 在 Python 里由 `_runtime._decode_files` 注入 resolver（要发 `StorageObjectGet`）；`_results`/`_pending` 同时承担去重、等待、背压、TTL、解码注入五种职责。

## 4. 耦合度评估

| 耦合 | 严重度 | 后果 |
|---|---|---|
| 三层委托 + 同名重复 | **高** | 改一处漏一处（`put_object` 坏了很久就是这原因） |
| 同步/异步双版 | **高** | 调用方选错就得到协程而不是数据 |
| 全局文件拦截 | 中 | 单进程只能一个域；测试难隔离 |
| 循环引用 | 中 | 初始化顺序敏感、难单测 |
| `_results` 五职责 | **高** | 一个 bug 可能同时表现为去重错、等待超时、busy 或解码失败 |

## 5. 收敛方向（单层）

```
AtelierDomain（唯一公开面，全异步）
 ├─ 消息：send / request / wait_result / forget_result / report_receipt
 ├─ 存储：storage_*
 ├─ 查询：query_logs / query_handlers / query_runtime / query_observer / query_progress
 ├─ 监督：supervisor_*
 ├─ 订阅：subscribe_* / unsubscribe_*
 ├─ C4：put_object / get_object / delete_object / file_object_id / enable_file_interception
 ├─ 日志：logger
 └─ 生命周期：run / shutdown
```

- 门面客户端**只保留对象、不再另起方法名**（或直接删除，业务统一走主类）；
- 删除同步版与 `_async` 别名；
- 把 `_results` 的职责拆开（去重/等待/背压/解码各归各的）；
- `FileNamespace` 从全局单例改成 domain 实例持有，编解码走实例引用。

## 6. 结论

- SDK 的能力面其实只有 **15 项**，但实现上被拆成 **3 层、16 个类、约 3500 行**；
- 复杂度主要来自**历史叠加**（同步/异步、门面/主类、`_async` 别名），不是需求本身；
- 「不可靠」的根因是**同一个能力有多个落点**，改一处漏一处。

收敛成单层后，预计可把 16 个类降到 **6-8 个**，行数减 30-40%，并且让「改哪里」变得唯一。
