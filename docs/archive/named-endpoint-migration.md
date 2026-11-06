# 内核传输层：命名管道（一刀切）

> 目标：内核不再用「匿名管道 + 继承 fd/HANDLE」做域 IPC，改为**命名端点**：
> Windows 用命名管道，Unix 用 Unix domain socket。SDK 连接端点，不再需要桥接进程。

## 1. 现状（要替换的东西）

| 位置 | 现状 |
|---|---|
| `src/supervisor.rs:709` `spawn_child_pipes` | `std::io::pipe()` 造两条匿名管道，Windows 上 `SetHandleInformation` 置继承位，把 `ATELIER_IPC_READ_FD` / `ATELIER_IPC_WRITE_FD` / `ATELIER_IPC_HANDLE_MODE` 写进子进程 env |
| `src/supervisor.rs` 读线程 | 阻塞读 `std::io::PipeReader`，逐行解析 |
| `src/supervisor.rs` 写帧 | `std::io::PipeWriter` |
| `src/supervisor.rs:1650` `DomainPipeClient` | 测试夹具用的客户端（`router` + `inbox`），上层 API 不变 |
| `tests/*.rs` | 全部走 `DomainPipeClient` 的 `send` / `drain_frames`，**上层不用改** |
| `sdk/python/atelier_sdk/_runtime.py` | `msvcrt.open_osfhandle` 转换 HANDLE |
| `sdk/v2/nodejs/index.js` | Python 桥接子进程（`BRIDGE_PY`） |

内核依赖里**没有 tokio、没有 windows-sys**，WinAPI 是手写 `extern "system"`（见 `supervisor.rs:345`）。

## 2. 新设计

```
Rust Kernel / Supervisor
  创建端点（Windows: \\.\pipe\atelier-<domain>-<uuid>
            Unix:    <tmp>/atelier-<domain>-<uuid>.sock）
  env: ATELIER_IPC_PIPE=<path>
  spawn 子进程 → 等待连接（accept）
  连接建立后：同一连接双向读写 JSON-Lines
```

- **不再传** `ATELIER_IPC_READ_FD` / `WRITE_FD` / `HANDLE_MODE`。
- 端点由内核创建、内核 accept；域只负责 `connect`。
- 协议（帧格式）完全不变。

## 3. 实现选择（零新依赖）

| 平台 | 实现 |
|---|---|
| Unix | `std::os::unix::net::UnixListener` + `UnixStream`（std，阻塞，零成本） |
| Windows | 手写 `CreateNamedPipeW` / `ConnectNamedPipe` / `ReadFile` / `WriteFile` / `CloseHandle`（沿用现有 `extern "system"` 风格） |

替代方案：引入 `interprocess` crate 可少写约 120 行 WinAPI，但会给内核加一个新依赖；当前按「零新依赖」推进。

## 4. 安全

命名管道需要安全描述符，限制为**当前用户**可连（否则本机任意进程都能连进域）。
- Windows：`SECURITY_ATTRIBUTES` + 一个只含当前用户 SID 的 DACL。
- Unix：socket 文件权限 `0600`。

## 5. 生命周期

- 命名管道**一个实例只服务一个客户端**；域断开后内核需重建实例再 accept（循环）。
- 断开 = 域退出；内核的读线程收到 EOF 后走现有的收尾路径。

## 6. 改动清单

| # | 文件 | 改动 |
|---|---|---|
| 1 | `src/supervisor.rs` | `spawn_child_pipes` → `spawn_child_endpoint`：建端点、传 `ATELIER_IPC_PIPE`、spawn、accept |
| 2 | 新增 `src/endpoint.rs` | 跨平台端点：`EndpointListener`（accept）+ `EndpointStream`（Read/Write） |
| 3 | `src/supervisor.rs` 读/写 | 用 `EndpointStream` 替换 `PipeReader`/`PipeWriter` |
| 4 | `sdk/v2/nodejs/index.js` | 删掉 `BRIDGE_PY` 与 `openStreams` 的 win32 分支；改为 `net.createConnection(process.env.ATELIER_IPC_PIPE)` |
| 5 | `sdk/v2/python/atelier_sdk/__init__.py` | 删掉 `msvcrt.open_osfhandle` 分支；改为 `open(pipe_path, "r+b", buffering=0)` |
| 6 | `tests/*.rs` | 上层 `DomainPipeClient` API 应不变；仅传端点 |
| 7 | 旧 SDK（`sdk/python`、`sdk/nodejs`） | 暂不动（业务域先不管；它们会因缺 fd 而失败，属预期） |

## 7. 验证标准

1. `cargo test` 全绿（含所有 e2e）；
2. `sdk_v2_e2e`（Python）通过；
3. `sdk_v2_node_e2e`（Node）通过，且**进程树里没有 Python 桥接**；
4. 7/7 域 active（需要业务域迁移后才满足，本轮不要求）。
