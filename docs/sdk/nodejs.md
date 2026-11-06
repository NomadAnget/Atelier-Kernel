# Node.js SDK

Atelier Kernel 的官方 Node.js 域 SDK：**单层 `AtelierDomain`**、全异步、15 项能力，无门面类、无遗留别名。

包位置：`sdk/nodejs`（`atelier-sdk`）。Python 端见 [python.md](python.md)，跨语言语义见 [../SDK_CONTRACT.md](../SDK_CONTRACT.md)。

## 安装

```bash
npm install ./atelier-sdk-<version>.tgz
```

## 最小示例

```js
const { AtelierDomain } = require("atelier-sdk");

const domain = new AtelierDomain("order");

domain.handler(
  "create",
  async (msg) => {
    await domain.storageInsert(msg.data.id, "sales_order", msg.data);
    return { ok: true };
  },
  "创建订单并写入本域存储"
);

domain.run();
```

## 15 项能力

| # | 能力 | 方法 |
| :--- | :--- | :--- |
| 1 | 生命周期 | `runAsync({onStart, onClose})` / `run()` / `shutdown()` |
| 2 | 注册处理器 | `handler(name, fn, description?)` |
| 3 | 发消息 | `send(targets, messageType, params, data)` → 消息 id |
| 4 | 请求-响应 | `request(...)` → 终态回执；非 `completed` 抛 `DomainRequestError` |
| 5 | 等待/遗忘 | `waitResult` / `waitResults` / `forgetResult` |
| 6 | 上报回执 | `reportReceipt(messageId, target, status, data)` |
| 7 | 事件回调 | `onReceipt` / `onLog` / `onNotify` / `onObservation` / `onTelemetry` |
| 8 | 本域存储 | `storageInsert` / `storageUpdate` / `storageDelete` / `storageQuery` |
| 9 | 查询 | `queryLogs` / `queryHandlers` / `queryRuntime` / `queryObserver` / `queryProgress` |
| 10 | 监督 | `supervisorStart` / `supervisorStop` / `supervisorRestart` / `supervisorUpdate` |
| 11 | 订阅 | `subscribeLogs` / `subscribeTelemetry` / `subscribeNotify` / `subscribeObserver`（+ `unsubscribe*`） |
| 12 | 通知 | `notify(level, message, title, details)` |
| 13 | C4 文件对象 | `enableFileInterception` / `putObject` / `getObject` / `deleteObject` / `fileObjectId` / `refreshFileIndex` |
| 14 | 日志 | `logger.debug/info/warn/error`；多状态用 options：`{ phase, ephemeral, key, frac, waiting, done, state, console_only }`（字段名即 `LogRecord` 字段，snake_case） |
| 15 | 端口 | `bindings` |

## 传输

| 变量 | 方向 |
| :--- | :--- |
| `ATELIER_IPC_IN` | 内核 → 域（域只读） |
| `ATELIER_IPC_OUT` | 域 → 内核（域只写） |

SDK 用 `net.createConnection(pipePath)` 连接（Windows 命名管道 / Unix domain socket），**不需要任何桥接子进程**。细节见 [命名端点传输](../capabilities/transport.md)。

## C4 虚拟文件

```js
domain.enableFileInterception();
fs.writeFileSync("vfs://out/clip.mp4", payload);
const objectId = domain.fileObjectId("vfs://out/clip.mp4");
```

- 运行时补丁覆盖 CommonJS 的 `fs` / `fs.promises`；
- ESM 命名导入（`import { writeFile } from "node:fs/promises"`）需要在域入口加
  `node --import atelier-sdk/vfs-loader.mjs domain.mjs`，由 `vfs-shim.mjs` 代理到同一个命名空间。

## 版本

SDK 与内核、Python SDK 同版本发布；**跨语言行为一致性**是硬要求，改动需同时更新 [../SDK_CONTRACT.md](../SDK_CONTRACT.md) 与 Python 端。
