# Atelier Kernel：Rust 单机多域微内核设计方案

**文档版本：** V3.1（定稿实现基线）  
**实现语言：** Rust（内核宿主与基础设施） / 语言无感（各业务域）  
**系统定位：** 单机多域可组合系统内核基础设施（非分布式、零网络通信），负责接管域生命周期、生成全局消息 ID、保障逐目标幂等投递、跨域自动路由、四阶段状态回执、全局消息实时观察与按域数据存储。

---

## 一、内核核心准则（9 条基线铁律）

1. **非分布式、零网络通信**：系统为纯单机微内核架构，不监听任何 TCP/UDP/HTTP 网络端口。内核与各域之间仅通过操作系统内核态的**父子进程匿名管道（`stdin` / `stdout`）**按行传输标准 JSON 帧（JSON-Lines）。
2. **域（Domain）为最小组装单元，生命周期由内核全权接管**：内核是系统的唯一启动入口与宿主，负责拉起（Spawn）、监控、重启和销毁各域子进程。任何语言编写的程序，只要能通过标准输入输出读写内核消息，即可成为一个合法的域。
3. **身份由物理管道决定，不依赖 JSON 声明**：内核在拉起某域子进程时，直接在内存中将该进程的 `stdin/stdout` 管道句柄与该域身份（`domain_id`）物理绑定。从该管道发出的任何消息或回执，其所属域身份由内核根据管道句柄直接判定并注入，彻底杜绝身份伪造与跨表越权。
4. **消息 ID 由内核统一生成**：发送域在提交消息时无需自己生成全局消息 ID；消息进入内核管道的第一时间，由内核统一分配全局唯一、单调递增的逻辑 ID（`message_id`），并通过回执告知发送端。
5. **幂等投递由内核全权保证**：内核维护权威的投递状态与幂等账本，从**发送端提交去重（`(source_domain, seq)`）**、**逐目标 `(message_id, target)` 状态机防重投**、**重复回执吸收**以及**存储主键防重**四个层面全权保障消息幂等。
6. **极简注册与 `域.消息处理器` 自动路由**：域启动后只需向内核发送一条注册消息，上报自己能处理的**消息处理器（Handlers）名称列表**；内核据此自动构建 `<domain>.<handler>` 路由表。
7. **无效目标前置拦截（零盲发）**：内核路由消息时，若发现目标 `域` 或 `消息处理器` 不存在（或目标域未处于就绪态），**绝不发送消息**，立即向发送端返回全局契约状态 `TargetInvalid`（目标无效）。
8. **两层消息结构，隔离业务演进**：
   - **上层为内核契约元数据**：包含内核生成的 `message_id`、管道绑定的 `source_domain`、目标位置列表（`targets`）、消息类型（`message_type`）与参数列表（`params`），用于内核路由、幂等账本、回执关联和按域建表。
   - **内层为实际数据包（`data`）**：标准 JSON 格式。域内部业务数据结构的变化仅改变内层数据包结构，**内核行为与数据库表结构完全无差异**。
9. **四阶段标准回执、实时观察与“存储即消息”**：
   - 每条消息流转严格产出 **已发出（`Sent`）$\rightarrow$ 已接收（`Received`）$\rightarrow$ 开始执行（`Started`）$\rightarrow$ 执行完成（`Completed`）** 四阶段回执，发送端可接收全部状态。
   - 内核内置专门的**消息观察组件（Observer）**，实时观测全系统所有消息状态。
    - 内核以“域”为单位利用上层契约元数据自动建表，内层存标准 JSON 数据包；存储操作以 `message_type` 表示，**增、删、改、查（CRUD）完全等同于向内核发送一条标准消息**。写入操作（`StorageInsert` / `StorageUpdate` / `StorageDelete`）严格绑定物理管道所属域（禁止跨表写入），读取操作（`StorageQuery`）支持查询本域或通过 `params.domain` / `params.target_domain` 读取其他域表数据。

---

## 二、模块划分与源码映射

| 模块 | 源码文件 | 核心职责 |
| :--- | :--- | :--- |
| **核心协议与两层契约** | `src/protocol.rs` | 定义 `DomainToKernelFrame`、`KernelToDomainFrame`、`OutboundMessage`、`KernelMessage`、`MessageReceipt`、`ContractStatus` 及 `ObservationEvent`。 |
| **ID 生成与三级幂等账本** | `src/ledger.rs` | 生成全局单调 `message_id`；维护 `(source_domain, seq)` 提交幂等表与 `(message_id, target)` 四阶段单向状态机。 |
| **实时消息观察组件** | `src/observer.rs` | 独立旁路观察器 `KernelObserver`，支持多订阅者实时流式观测与历史事件检索，不阻塞主投递链路。 |
| **按域存储引擎** | `src/storage.rs` | 基于 SQLite 的按域建表与“写隔离、读可跨表”存储引擎，响应 `StorageInsert`、`StorageQuery`、`StorageUpdate`、`StorageDelete` 消息并产生标准四阶段回执。 |
| **契约校验与自动路由器** | `src/router.rs` | 维护 `<domain>.<handler>` 路由表、执行 `TargetInvalid` 零盲发拦截、多目标独立分发、回执归属校验与状态推进。 |
| **域宿主与管道生命周期** | `src/supervisor.rs` | 通过 OS `Stdio` 管道拉起多语言子进程、物理绑定 `domain_id` 身份、监控崩溃退出并自动触发 `ExecutionFailed` 恢复。 |
