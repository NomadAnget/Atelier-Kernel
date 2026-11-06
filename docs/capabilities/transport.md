# 传输：命名端点（v2）

内核与每个域代际之间用**一对命名端点**收发 JSON-Lines 帧，不再通过继承的文件描述符 / HANDLE。

## 端点

每次拉起域时内核创建一对端点，nonce 随代际变化：

| 环境变量 | 方向 | 谁 accept | 谁 connect |
| :--- | :--- | :--- | :--- |
| `ATELIER_IPC_IN` | 内核 → 域（域读） | 内核 `accept_writer` | 域 |
| `ATELIER_IPC_OUT` | 域 → 内核（域写） | 内核 `accept_reader` | 域 |

路径由 `endpoint::endpoint_path(domain, nonce)` 生成：

| 平台 | 形式 | 打开方式 |
| :--- | :--- | :--- |
| Windows | `\\.\pipe\atelier-<domain≤12>-<nonce>-in` / `-out` | 文件 API（`open` / `OpenOptions`） |
| Unix | `/tmp/atl-<domain≤12>-<nonce>-in.sock` / `-out.sock` | **必须** `socket(AF_UNIX).connect()`，`open()` 会失败 |

nonce 含代际标识，因此旧代际的端点不会与新代际复用，回执不会串代际。

## 读写语义

- 域 → 内核（`ATELIER_IPC_OUT`）与内核 → 域（`ATELIER_IPC_IN`）都是单向流，各自独立，没有共享锁。
- `EndpointStream::read` 是**阻塞**语义。
- `EndpointReader::read` 是**轮询**语义：没有数据时返回 `WouldBlock`，由 supervisor 的 `pump_lines` 轮询停机信号；业务代码不要直接依赖它。
- Unix 侧的 listener 是非阻塞的，accept 循环带 `stop` 检查，因此停机不必等待一个永远不会到来的连接。
- 帧格式与 v1 完全一致（一行一帧的 JSON），协议层没有变化。

## 安全

- **Unix**：socket 文件权限收紧为 `0600`，仅创建者可连接。
- **Windows**：命名管道未显式设置安全描述符（`CreateNamedPipeW` 的安全参数传 `NULL`），依赖进程默认 DACL 与 nonce 的不可猜性。这是**已知限制**：共享主机上如需严格同用户隔离，需补一个只含当前用户 SID 的 DACL。
- 两侧的 `stdout` / `stderr` 与协议端点**物理隔离**，由内核作为子进程诊断日志单独采集。

## 归入宪法的不变量（机制变了，约束没变）

- **身份来自端点**：一条入站消息的 `source_domain` 由它到达的那条 outbound 端点决定，域无法在帧里声明别人的身份（M2）。
- **端点专管**：域代码不得自行读写 `ATELIER_IPC_*`；静态扫描 `--check` 会拒绝（M5）。旧名 `ATELIER_IPC_READ_FD` / `WRITE_FD` / `HANDLE_MODE` / `PIPE` 仍留在禁止列表里，设置它们会直接拒绝启动。

## 已废弃

v1 的匿名管道 + fd/HANDLE 传递（`ATELIER_IPC_READ_FD`、`ATELIER_IPC_WRITE_FD`、`ATELIER_IPC_HANDLE_MODE`）已删除。迁移过程见归档 [named-endpoint-migration.md](../archive/named-endpoint-migration.md)。