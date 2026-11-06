# atelier-sdk (Node.js)

Atelier Kernel 的官方 Node.js 域 SDK：**单层 `AtelierDomain`**、全异步、15 项能力，无门面类、无遗留别名。

> 完整能力表、传输模型与 C4 用法见 [docs/sdk/nodejs.md](https://github.com/NomadAnget/Atelier-Kernel/blob/main/docs/sdk/nodejs.md)。

15 项能力：生命周期 · 注册处理器 · 发消息 · 请求-响应 · 等待/遗忘 · 上报回执 · 事件回调 · 本域存储 · 查询 · 监督 · 订阅 · 通知 · C4 文件对象 · 日志 · 端口。

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

## 传输

内核通过 `ATELIER_IPC_IN`（内核 → 域）与 `ATELIER_IPC_OUT`（域 → 内核）两条单向端点通信，SDK 用 `net.createConnection(pipePath)` 连接（Windows 命名管道 / Unix domain socket），**不需要任何桥接子进程**。
