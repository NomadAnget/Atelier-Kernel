# Atelier SDK v2（重写版）

> 基于新内核契约（stdio JSON-Lines、四阶段回执、C4 虚拟文件命名空间）与
> `docs/SDK_ANATOMY.md` 的 15 项能力清单，从零重写，不继承旧 SDK 的结构。

## 设计原则

1. **单层**：只有 `AtelierDomain` 一个公开面。不再有 `DomainStorageClient` / `DomainLogger`
   等门面类另起一套方法名。
2. **全异步**：所有 I/O 方法都是 `async`。没有同步版、没有 `_async` 后缀。
3. **实例持有**：`FileNamespace`、编解码、命名空间都是 `AtelierDomain` 的实例字段，
   不再有模块级全局单例。
4. **职责单一**：每个内部 map 只做一件事——
   - `_pending`：已发出、等 id 的提交；
   - `_results`：已终态、等取走的结果；
   - `_namespace`：虚拟路径 ↔ 平铺 id 映射。
5. **两语言同构**：Python `snake_case`、Node `camelCase`，能力一一对应。

## 对外能力（15 项）

| # | 能力 | Python | Node |
|---|---|---|---|
| 1 | 生命周期 | `run` / `run_async` / `shutdown` | `run` / `runAsync` / `shutdown` |
| 2 | 注册处理器 | `handler(name)` | `handler(name)` |
| 3 | 发消息 | `send` | `send` |
| 4 | 请求-响应 | `request` | `request` |
| 5 | 等待/遗忘 | `wait_result` / `wait_results` / `forget_result` | `waitResult` / `waitResults` / `forgetResult` |
| 6 | 上报回执 | `report_receipt` | `reportReceipt` |
| 7 | 事件回调 | `on_receipt` / `on_log` / `on_notify` / `on_observation` / `on_telemetry` | `onReceipt` / `onLog` / `onNotify` / `onObservation` / `onTelemetry` |
| 8 | 本域存储 | `storage_insert` / `update` / `delete` / `query` | `storageInsert` / `Update` / `Delete` / `Query` |
| 9 | 查询 | `query_logs` / `query_handlers` / `query_runtime` / `query_observer` / `query_progress` | `queryLogs` / `queryHandlers` / `queryRuntime` / `queryObserver` / `queryProgress` |
| 10 | 监督 | `supervisor_start` / `stop` / `restart` / `update` | `supervisorStart` / `Stop` / `Restart` / `Update` |
| 11 | 订阅 | `subscribe_logs/telemetry/notify/observer`（+ `unsubscribe_*`） | `subscribeLogs/Telemetry/Notify/Observer`（+ `unsubscribe*`） |
| 12 | 通知 | `notify` | `notify` |
| 13 | C4 对象 | `put_object` / `get_object` / `delete_object` / `file_object_id` / `enable_file_interception` / `refresh_file_index` | `putObject` / `getObject` / `deleteObject` / `fileObjectId` / `enableFileInterception` / `refreshFileIndex` |
| 14 | 日志 | `logger` / `attach_stdlib_logging` | `logger` / `attachStdlibLogging` |
| 15 | 端口 | `bindings` | `bindings` |

## 内核契约（必须对齐）

| 面 | 内容 |
|---|---|
| 帧 | `{frame_type:"send", seq, targets, message_type, params, data}` |
| 回执 | `sent` → `received` → `started` → `completed` / `execution_failed` / `target_invalid` / `contract_invalid` |
| 内核目标 | `kernel.storage` / `kernel.logger` / `kernel.registry` / `kernel.runtime` / `kernel.observer` / `kernel.telemetry` / `kernel.supervisor` / `kernel.notify` / `kernel.contract` |
| C4 | `StorageObjectPut`（`virtual_path`）/ `Get`（`object_id`）/ `Delete` / `List` |
| 投递策略 | `Durable` / `Volatile` / `Ephemeral` |

## 目录

```
sdk/v2/
  README.md
  python/atelier_sdk/__init__.py     # 单层实现
  python/tests/                      # 单测
  nodejs/index.js                    # 单层实现（CommonJS）
  nodejs/codec.js                    # 文件对象编解码
  nodejs/tests/
```

## 移植计划

1. **传输 + 消息 + 回执**（send/request/wait/forget/report_receipt）
2. **处理器 + 事件**
3. **存储 + 查询 + 监督 + 订阅 + 通知**
4. **C4（命名空间 + 编解码 + loader）**
5. **日志 + 端口**
6. 每层：单测 + 真实内核 e2e
