# 文档索引

内核只有**一份**根文档：[README.md](../README.md)。它给出内核定位、最高准则与最高宪法入口；具体契约、能力与 SDK 用法都在这份索引里往下展开。

## 最高契约

| 文档 | 内容 |
| :--- | :--- |
| [CONSTITUTION.md](CONSTITUTION.md) | **最高宪法（V4.1）**：四大内核契约 C1–C4、机器宪法 M1–M7、行为规范 B1–B6，以及后续追加的专项契约 |
| [MESSAGE_IDENTITY.md](MESSAGE_IDENTITY.md) | 消息身份与扇出：`logical_id` / `message_id`、提交去重与 seq 语义、重启后的收敛 |
| [ARCHITECTURE.md](ARCHITECTURE.md) | 架构与源码映射：11 条基线铁律、模块职责、8 大内置基础设施 |

## SDK

| 文档 | 内容 |
| :--- | :--- |
| [SDK_CONTRACT.md](SDK_CONTRACT.md) | 跨语言 SDK 责任边界：能力集合、事件流投递契约、传输保护、C4 命名空间 |
| [sdk/python.md](sdk/python.md) | Python SDK：安装、最小示例、15 项能力表、传输与 C4 用法 |
| [sdk/nodejs.md](sdk/nodejs.md) | Node.js SDK：同上（方法名为 camelCase） |

## 内核能力

| 文档 | 内容 |
| :--- | :--- |
| [transport.md](capabilities/transport.md) | 命名端点传输：`ATELIER_IPC_IN` / `OUT`、平台路径、读写语义、安全边界 |
| [port-bindings.md](capabilities/port-bindings.md) | 内核托管的域端口绑定：薄转发进程、代际切换、连接排空 |
| [observation.md](capabilities/observation.md) | 观察能力：调用与执行观察、运行指标、异常事件，以及丢失边界 |
| [domain-state-observation.md](capabilities/domain-state-observation.md) | 域状态镜像：真实状态写入驱动、初始快照与最新状态合并、无轮询 |
| [progress.md](capabilities/progress.md) | 进度终态：`running` / `completed` / `failed` / `cancelled` 与活动键回收 |
| [log-query-pagination.md](capabilities/log-query-pagination.md) | `QueryLogs` 游标分页：`before_id`、页预算、继续游标 |
| [outbox-overflow.md](capabilities/outbox-overflow.md) | 出口溢出：超过内存窗口时临时磁盘暂存及其边界 |
| [process-diagnostics.md](capabilities/process-diagnostics.md) | 每代际进程故障诊断事件的字段与用法 |
| [process-reaping.md](capabilities/process-reaping.md) | 进程回收可见性：回收完成前状态仍可观察 |

## 使用与运维

| 文档 | 内容 |
| :--- | :--- |
| [VARIANT_GUIDE.md](VARIANT_GUIDE.md) | 变体开发指南：发布包结构、三步创建变体、命令行参数、发布流程 |

## 发版说明

| 文档 | 内容 |
| :--- | :--- |
| [releases/RELEASE_0.3.3.md](releases/RELEASE_0.3.3.md) | 当前版本：SDK 新增域存在性探测（内核元数据）、死代码清理 |
| [releases/RELEASE_0.3.2.md](releases/RELEASE_0.3.2.md) | 上一版：内核累计计数（真速率）与观察流丢弃计数，观察者限速不再掩盖投递量 |
| [releases/RELEASE_0.3.1.md](releases/RELEASE_0.3.1.md) | 上一版：SDK 收缩为只做协议传输、去掉超时/窗口/TTL 策略、传输失败不再沉默 |
| [releases/RELEASE_0.3.0.md](releases/RELEASE_0.3.0.md) | 上一版：分层落定、SDK 契约收敛、门禁补齐、破坏性变更与升级方式 |

## 归档

[archive/INDEX.md](archive/INDEX.md) —— 旧的设计方案、验收记录与逐版发版说明。**这些不代表当前契约**，仅用于追溯。
