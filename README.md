<p align="center">
  <img src="docs/kernel.svg" width="112" height="112" alt="Atelier Kernel"/>
</p>

<h1 align="center">Atelier Kernel</h1>

<p align="center"><em>Rust 单机多域微内核：每个业务域是独立子进程，跨域协作一律走内核消息。</em></p>

---

## 最高准则

> **一切设计服务于进程级认知隔离：新增或修改一个业务域时，只需理解本域收发的 `<domain>.<handler>` 消息契约与本域目录代码，无需阅读其他域的实现。**

这条准则是所有取舍的判据。凡是让一个域不得不去读另一个域实现的设计，都不接受。

## 最高宪法

[`docs/CONSTITUTION.md`](docs/CONSTITUTION.md)（V4.1）是本项目的最高契约，冲突时以它为准：

- **四大内核契约 C1–C4**：端点身份、两层消息与投递策略、持久与恢复、对象枢纽；
- **机器宪法 M1–M7**：可由 `--check` 静态执法（域标识唯一、端点即身份、写权随域、进程即边界、端点纯净、配置显式、表结构防漂移）；
- **行为规范 B1–B6**：单一逻辑、认知隔离、显式失败、零业务绑定、契约同步、异步幂等。

## 内核提供什么

| 能力 | 一句话 | 文档 |
| :--- | :--- | :--- |
| 传输 | 每代际一对命名端点，域连接后收发 JSON-Lines | [transport](docs/capabilities/transport.md) |
| 消息身份 | 一次提交一个 `logical_id`，每个目标一个 `message_id`，按 seq 去重 | [identity](docs/MESSAGE_IDENTITY.md) |
| 投递账本 | `Durable` / `Volatile` / `Ephemeral` 三级策略 + 四阶段回执 | [constitution](docs/CONSTITUTION.md) |
| 按域存储 | `store_<domain>` 写隔离、读可跨表；C4 `vfs://` 对象枢纽 | [SDK contract](docs/SDK_CONTRACT.md) |
| 结构化日志 | WAL 缓冲 + 批量导入 `hub_logs`，游标分页查询 | [log-query-pagination](docs/capabilities/log-query-pagination.md) |
| 观察 | 生命周期 / 进度 / 遥测 / 通知四组事件流 | [observation](docs/capabilities/observation.md) |
| 域状态镜像 | 真实状态写入驱动，订阅初始镜像 + 最新状态合并，无轮询 | [domain-state-observation](docs/capabilities/domain-state-observation.md) |
| 进程管控 | 启动 / 停止 / 重启 / 滚动更新，候选就绪才切流 | [VARIANT_GUIDE](docs/VARIANT_GUIDE.md) |
| 端口托管 | 内核托管薄 TCP 转发进程，代际切换与连接排空 | [port-bindings](docs/capabilities/port-bindings.md) |
| 宪法检查 | `--check` 静态执法 M1–M7 + 账本行级完整性 | [ARCHITECTURE](docs/ARCHITECTURE.md) |

## SDK

两套官方 SDK 能力完全对齐（15 项），语义由 [SDK_CONTRACT.md](docs/SDK_CONTRACT.md) 统一。

| 语言 | 包 | 能力文档 |
| :--- | :--- | :--- |
| Python | `sdk/python`（`atelier_sdk`） | [docs/sdk/python.md](docs/sdk/python.md) |
| Node.js | `sdk/nodejs`（`atelier-sdk`） | [docs/sdk/nodejs.md](docs/sdk/nodejs.md) |

## 快速开始

```bash
cargo build --release

# 生成一个可运行的变体骨架（配置 + 示例域）
./target/release/atelier-kernel --init ./my-variant

# 宪法静态检查 + 账本完整性
./target/release/atelier-kernel --check ./my-variant/kernel_boot.json

# 启动（--web-ui 可附带观察台）
./target/release/atelier-kernel --config ./my-variant/kernel_boot.json

# 端到端自检：注册 + 四阶段回执 + 日志入库
./target/release/atelier-kernel --config ./my-variant/kernel_boot.json --smoke-test
```

跑测试：`cargo test`。完整文档入口：[`docs/INDEX.md`](docs/INDEX.md)。

## 仓库结构

```
src/
├── core/         协议与消息状态机（不依赖 services）
├── edge/         命名端点传输（Windows 管道 / Unix socket）
├── services/     账本、出口、存储、日志、观察、端口、对象
├── diag/         机器宪法检查器
├── supervisor/   域进程与代际生命周期
└── router.rs     路由、内置基础设施分发、扇出

sdk/python/            Python 域 SDK
sdk/nodejs/            Node.js 域 SDK
docs/                  契约、能力与 SDK 文档（入口 docs/INDEX.md）
tests/                 分层测试（unit / transport / lifecycle / reliability / logging / c4 / sdk_e2e）
```

## 状态

三平台（Linux / Windows / macOS）CI 全绿；传输为命名端点，账本终态历史只存 SQLite 并按需读取。历史设计与验收记录见 [`docs/archive/`](docs/archive/INDEX.md)。
