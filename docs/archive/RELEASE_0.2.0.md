> **归档（历史记录）**：本文档记录当时的设计、验收或发布状态，**不代表当前契约**。
> 当前契约、能力与 SDK 入口见 [../INDEX.md](../INDEX.md)。

# Atelier Kernel v0.2.0 发版说明

Atelier Kernel **v0.2.0** 正式发布。本版本的主线是**传输层重构**：域 IPC 从「匿名管道 + 继承 fd/HANDLE」整体切换为**命名端点**（Windows 命名管道 / Unix domain socket），并以此为基础重写了 Python 与 Node.js 两个域 SDK。

> ⚠️ **破坏性变更**：旧 SDK 的 fd/HANDLE 环境变量（`ATELIER_IPC_READ_FD` / `ATELIER_IPC_WRITE_FD` / `ATELIER_IPC_HANDLE_MODE`）已移除。业务域必须升级到 v0.2.0 SDK 才能在新内核上启动。

---

## 🌟 核心更新

### 1. 传输层：命名端点（两条单向管道）

内核不再向域传递 fd 或 HANDLE 句柄，而是自己创建端点、由域主动连接：

| 环境变量 | 方向 |
|---|---|
| `ATELIER_IPC_IN` | 内核 → 域（域只读） |
| `ATELIER_IPC_OUT` | 域 → 内核（域只写） |

- **Windows**：`CreateNamedPipeW` / `ConnectNamedPipe` / `ReadFile` / `WriteFile`（内核零新依赖）；
- **Unix**：`UnixListener` / `UnixStream`；
- 每个域两条**独立单向管道**——这避免了 Windows 上「同一句柄并发阻塞读 + 写」被串行化的死锁（双向单管道方案的实测结论）。

### 2. SDK 重写：单层 `AtelierDomain`

两个语言的 SDK 都重写为单层实现，只保留 **15 项对外能力**：

| # | 能力 |
|---|---|
| 1 | 生命周期（`run_async` / `shutdown`） |
| 2 | 注册处理器 |
| 3 | 发消息 |
| 4 | 请求-响应 |
| 5 | 等待 / 遗忘 |
| 6 | 上报回执 |
| 7 | 事件回调 |
| 8 | 本域存储 |
| 9 | 查询 |
| 10 | 监督 |
| 11 | 订阅 |
| 12 | 通知 |
| 13 | C4 文件对象 |
| 14 | 日志 |
| 15 | 端口 |

同时移除了：门面客户端类（`DomainStorageClient` 等 7 个）、同步 API、`_async` 别名后缀。Python 侧从 7 文件 / ~1960 行降到 **1 文件 / ~650 行**；Node 侧从多文件 / ~1600 行降到 **1 文件 / ~590 行**。

### 3. Node.js：彻底去掉 Python 桥接

旧实现在 Windows 上要起一个 Python 子进程把 HANDLE 转成管道。v0.2.0 改用 `net.createConnection(pipePath)` 直连命名端点，**每个 Node 域少一个常驻子进程**，也不再要求部署环境里有 Python。

### 4. C4 虚拟文件：实例持有

`FileNamespace` 与消息编解码改为 `AtelierDomain` 实例字段，不再是模块级全局单例。CommonJS 的 `fs` / `fs.promises` 由运行时补丁覆盖；ESM 命名导入由 `vfs-loader.mjs` + `vfs-shim.mjs` 代理。

### 5. 宪法 M5 规则同步

「禁止域代码直接操纵 IPC 端点」的检查从 `ATELIER_IPC_READ_FD` / `WRITE_FD` 更新为 `ATELIER_IPC_IN` / `OUT`（旧名仍保留在禁止列表）。

### 6. 内核修复

- **自然退出归因**：`terminate` 在 kill 前给已自然退出的子进程一个短暂观察窗口，避免把自然退出误记为内核清理（`child_exit` 与 `child_exit_after_cleanup` 的区分）。
- **端点接受语义**：reader 内建 `PeekNamedPipe` 轮询（无数据返回 `WouldBlock`），保持 `pump_lines` 的 `stop` 检查有效；
- **managed-ready 帧**：域在 `on_start` 完成后发送 `{"frame_type":"ready"}`，内核据此激活端口转发。

---

## ✅ 验证

```bash
cargo test                                   # 31 个测试二进制，0 失败
python sdk/python/tests/test_domain.py       # 17 项单元测试
node --test sdk/nodejs/tests/domain.test.mjs # 19 项单元测试
```

- SDK 端到端 e2e（两端）：覆盖全部 **15 项能力** + 连续 20 次请求后 `_pending` / `_results` 归零（无泄漏）；
- 内核 e2e：C4 多进程、存储文件对象、域环境、域状态观测、端口转发、进程诊断、可靠性回归全部通过。

---

## 📦 升级指引

1. 内核升级到 v0.2.0（`cargo build --release` 或替换 `atelier-kernel` 可执行文件）；
2. SDK 升级到 0.2.0：
   - Python：`pip install atelier_sdk-0.2.0-py3-none-any.whl`
   - Node：`npm install ./atelier-sdk-0.2.0.tgz`
3. 域代码改造要点：
   - `AtelierDomain()` → `AtelierDomain("<domain_id>")`；
   - `domain.run()` → `asyncio.run(domain.run_async())`（Node 的 `run()` 保留）；
   - `domain.run_async(on_start=..., on_close=...)` 参数名不变；
   - 不再需要设置/读取 `ATELIER_IPC_READ_FD` / `WRITE_FD` / `HANDLE_MODE`；
   - 若使用 `run()` 之外的门面对象（`domain.storage.xxx` 等），改为直接调用 `domain.storage_xxx`。

> 业务域在本版本中**尚未迁移**，因此升级内核前请确认该域已适配 0.2.0 SDK，否则会因读不到旧 fd 变量而启动失败。
