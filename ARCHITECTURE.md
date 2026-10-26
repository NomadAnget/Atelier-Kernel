# Atelier Kernel：Rust 单机多域微内核设计方案

**文档版本：** V3.3（0.1.14：托管端口与状态观察）
**实现语言：** Rust（内核宿主与基础设施） / 语言无感（各业务域）  
**系统定位：** 单机多域可组合系统内核基础设施（非分布式、域间零网络通信），负责接管域生命周期、生成全局消息 ID、保障逐目标幂等投递、跨域自动路由、四阶段状态回执、全局消息实时观察、按域数据存储以及原生基础设施方法提供。

---

## 一、内核核心准则（11 条基线铁律）

1. **非分布式、域间零网络通信**：系统为纯单机微内核架构，域间协议不使用网络；对外TCP端口可通过启动配置ports声明，由内核独立薄转发进程托管。内核与各域之间仅通过操作系统内核态的**父子进程专用匿名管道（`ATELIER_IPC_READ_FD` / `ATELIER_IPC_WRITE_FD`，与 `stdout/stderr` 诊断物理分离）**按行传输标准 JSON 帧（JSON-Lines）。
2. **域（Domain）为最小组装单元，生命周期由内核全权接管**：内核是系统的唯一启动入口与宿主，负责拉起（Spawn）、监控、重启和销毁各域子进程。任何语言编写的程序，只要能通过专用匿名管道读写内核消息，即可成为一个合法的域。
3. **身份由物理管道决定，不依赖 JSON 声明**：内核在拉起某域子进程时，直接在内存中将该进程的 专用匿名管道句柄与该域身份（`domain_id`）物理绑定。从该管道发出的任何消息或回执，其所属域身份由内核根据管道句柄直接判定并注入，彻底杜绝身份伪造与跨表越权。
4. **消息 ID 由内核统一生成**：发送域在提交消息时无需自己生成全局消息 ID；消息进入内核管道的第一时间，由内核统一分配全局唯一的逻辑 ID（`logical_id`），再为每个目标拆分消息分配独立 `message_id`，并通过回执告知发送端。
5. **幂等投递由内核全权保证**：内核维护权威的投递状态与幂等账本，从**发送端提交去重（`(source_domain, 进程代际, seq)`）**、**逐目标 `message_id` 状态机防重投**、**重复回执吸收**以及**存储主键防重**四个层面全权保障消息幂等。
6. **极简注册与 `域.消息处理器` 自动路由**：域启动后只需向内核发送一条注册消息，上报自己能处理的**消息处理器（Handlers）名称列表**；内核据此自动构建 `<domain>.<handler>` 路由表。
7. **无效目标前置拦截（零盲发）**：内核路由消息时，若发现目标 `域` 或 `消息处理器` 不存在（或目标域未处于就绪态），**绝不发送消息**，立即向发送端返回全局契约状态 `TargetInvalid`（目标无效）。
8. **两层消息结构，隔离业务演进**：
   - **上层为内核契约元数据**：包含内核生成的 `message_id`、管道绑定的 `source_domain`、目标位置列表（`targets`）、消息类型（`message_type`）与参数列表（`params`），用于内核路由、幂等账本、回执关联和按域建表。
   - **内层为实际数据包（`data`）**：标准 JSON 格式。域内部业务数据结构的变化仅改变内层数据包结构，**内核行为与数据库表结构完全无差异**。
9. **四阶段标准回执、实时观察与“存储即消息”**：
   - 每条业务消息流转严格产出 **已发出（`Sent`）$\rightarrow$ 已接收（`Received`）$\rightarrow$ 开始执行（`Started`）$\rightarrow$ 执行完成（`Completed`）** 四阶段回执，发送端可接收全部状态。
   - 内核以“域”为单位利用上层契约元数据自动建表，内层存标准 JSON 数据包；存储操作以 `message_type` 表示，**增、删、改、查（CRUD）完全等同于向内核发送一条标准消息**。写入操作严格绑定物理管道所属域（禁止跨表写入），读取操作支持通过 `params.domain` 跨表只读查询。
10. **内核基础设施方法 vs 严格禁止域间发布订阅**：
    - 内核将通用的系统级底层能力收敛为 8 大内置基础设施目标（`kernel.*`），直接作为多语言 SDK 的基础设施对象与方法提供，不作为外部可订阅的消息处理器。
    - 业务路由列表（`available_handlers`）严格**排除**所有 `kernel.*` 处理器。
    - **域间通信严格禁止 Pub-Sub 强绑定**，只允许点对点精确路由；只有内核流（日志、遥测、告警、观察者）可通过 SDK 原生方法监听，且全部采用 `DeliveryPolicy::Ephemeral` 瞬态推流，不占用账本、零收据产生，域断开连接时内核自动清理订阅。
11. **候选隔离就绪、路由切换与失败回滚**：
    - 针对运行中域的更新，内核支持两阶段平滑滚动：拉起携带独立 `instance_id` 的候选版本进程，接入隔离管道并不承接任何业务流量。
    - 当前实现以候选处理器注册就绪作为基本切换条件，有ports绑定时还需SDK应用启动完成握手；保留 `probe_handler` 入参兼容性，但不把它声称为已实现的业务健康检查。
    - 候选就绪：原子切换路由，旧实例进入 `Draining`；有限宽限后明确失败收敛剩余在途消息并回收旧进程。不保证业务请求全部无损完成。
    - 候选未就绪或连接失败：销毁候选实例并回收资源，原版本实例持续不受影响地对外服务，自动回滚。

---

## 二、模块划分与源码映射

| 模块 | 源码文件 | 核心职责 |
| :--- | :--- | :--- |
| **对外端口托管** | `src/port_forwarder.rs` | 独立薄TCP转发进程、代际后端切换、连接排空与故障恢复 |
| **核心协议与两层契约** | `src/protocol.rs` | 定义 `DomainToKernelFrame`、`KernelToDomainFrame`、`KernelMessage`、`MessageReceipt`、`ContractStatus`、`ObservationEvent` 及 8 大内置目标常量。 |
| **ID 生成与三级幂等账本** | `src/ledger.rs` | 生成带内核实例 UUID 的唯一逻辑 ID 和子消息 ID；维护 `(source_domain, 进程代际, seq)` 提交幂等表与 `message_id` 四阶段单向状态机。 |
| **全局消息观察组件** | `src/observer.rs` | 独立旁路观察器 `KernelObserver`，支持多订阅者实时流式观测与历史事件快照检索，不阻塞主投递链路。 |
| **按域存储引擎** | `src/storage.rs` | 基于 SQLite 的按域建表与“写隔离、读可跨表”存储引擎，响应 `StorageInsert`、`StorageQuery`、`StorageUpdate`、`StorageDelete` 消息并产生标准四阶段回执。 |
| **契约校验与自动路由器** | `src/router.rs` | 维护 `<domain>.<handler>` 路由表、执行 `TargetInvalid` 零盲发拦截、内置 8 大基础设施分发、系统流扇出与原子候选路由交接。 |
| **域宿主与管道生命周期** | `src/supervisor.rs` | 通过 OS 匿名管道拉起子进程、物理绑定 `domain_id` 身份、代际隔离、崩溃自动恢复，以及**候选注册就绪、路由切换与失败回滚**。 |
| **机器宪法与契约检查器** | `src/discipline.rs` | 实现机器宪法（M1–M7）静态与启动期执法（`atelier-kernel --check`），校验配置显式性、存储表结构防漂移及多语言域源码纪律。 |

---

## 三、内核 8 大内置基础设施规范

内核内置目标不属于任何业务域，由内核路由器直接接管处理，不向外界暴露为业务路由：

### 1. `kernel.registry`（服务注册与元数据发现）
- **接收消息类型**：`RegisterHandlers`, `UnregisterHandlers`, `QueryHandlers`
- **说明**：域启动时自动通过 SDK 报备 handlers 列表及其文档注释；调用 `QueryHandlers` 可查询系统当前可用路由清单和方法说明。
- **保护策略**：返回列表中严格排除所有 `kernel.*` 目标。

### 2. `kernel.storage`（按域隔离存储即消息）
- **接收消息类型**：`StorageInsert`, `StorageQuery`, `StorageUpdate`, `StorageDelete`
- **说明**：按域自动建立 `store_<domain_id>` SQLite 数据表。写入类操作只允许操作本域数据表；读取类操作通过 `params.domain` 支持安全的跨域只读查询。

### 3. `kernel.logger`（统一结构化日志与任务进度）
- **接收消息类型**：`QueryLogs`, `QueryProgress`, `SubscribeLogs`, `UnsubscribeLogs`
- **推流消息类型**：`LogEvent`（`DeliveryPolicy::Ephemeral`）
- **说明**：各域通过专用管道向内核输出日志，杜绝标准输出污染；支持分级过滤（`level`）与来源过滤（`source_domain`）；支持长任务进度双向追踪与实时监听。

### 4. `kernel.telemetry`（高频遥测指标与传感器流）
- **接收消息类型**：`SubscribeTelemetry`, `UnsubscribeTelemetry`, 任意业务指标上报
- **推流消息类型**：`TelemetryEvent`（`DeliveryPolicy::Ephemeral`）
- **说明**：面向传感器数据、UI 刷新高频状态与系统性能指标，旁路推流，零 receipt 账单开销。

### 5. `kernel.notify`（系统告警通知与事件广播）
- **接收消息类型**：`SubscribeNotify`, `UnsubscribeNotify`, 任意告警消息
- **推流消息类型**：`NotifyEvent`（`DeliveryPolicy::Ephemeral`）
- **说明**：支持各域广播通知或触发严重告警（`severity`：info / warning / error / critical），推送到所有已订阅管道。

### 6. `kernel.runtime`（域运行态拓扑与装配查询）
- **接收消息类型**：`QueryRuntime`, `QueryDomainAssembly`
- **说明**：提供当前内核所有受托管域的活跃状态、当前代际 `instance_id`、已就绪的路由列表、OS 进程信息（PID、启动命令、工作目录、重启策略与已重启计数）。

### 7. `kernel.observer`（全局消息生命周期观察与快照）
- **接收消息类型**：`QueryObserver`, `SubscribeObserver`, `UnsubscribeObserver`
- **推流消息类型**：`ObservationEvent`（`DeliveryPolicy::Ephemeral`）
- **说明**：允许审计与监控域检索内核全局消息最近的处理快照（支持按 `message_id`, `target`, `source_domain` 过滤）；支持实时流式监听全局消息状态变更事件，且在域异常断开时内核自动注销清理。

### 8. `kernel.supervisor`（域生命周期管理与平滑更新）
- **接收消息类型**：`StartDomain`, `StopDomain`, `RestartDomain`, `UpdateDomain`
- **说明**：提供对指定域的显式拉起、受控停止（显式停止不触发崩溃自动重启）和重启；`UpdateDomain` 无显式spec时重读来源启动文件；候选注册及（如有ports）应用启动握手后切换路由/后端，失败保留旧实例。

---

## 四、消息身份、日志与契约规范索引

- **日志来源契约**：详见 [CONSTITUTION.md](CONSTITUTION.md) 中的 `M5`。所有日志记录强制注入内核认证的 `source_domain`，禁止伪造。
- **消息身份契约**：详见 [MESSAGE_IDENTITY.md](MESSAGE_IDENTITY.md)。每次提交共享 `logical_id`，每个目标拥有独立 `message_id`。
- **SDK 责任边界**：详见 [SDK_CONTRACT.md](SDK_CONTRACT.md)。SDK 负责协议适配与基础设施客户端封装，不包含业务验证或私有准入队列。


## 核心可靠性实现基线

持久接收/恢复、每代际传输窗口、进程资源所有权及尽力观察的当前状态转换见 [CORE_RELIABILITY_ACCEPTANCE.md](docs/CORE_RELIABILITY_ACCEPTANCE.md)。接口与职责维持上述边界；Durable 真相在 SQLite，正常历史结果不再驻留 Rust 内存账本。具体可靠性契约以 CONSTITUTION.md 为准。

## 0.1.14 扩展

- [端口绑定](docs/PORT_BINDINGS.md)：外部入口与域代际分离，跨域消息仍走IPC。
- [域状态镜像](docs/DOMAIN_STATE_OBSERVATION.md)：真实状态写入驱动，订阅初始镜像及最新状态合并，无轮询。
- [观察能力分组使用说明](docs/OBSERVATION_USAGE.md)：生命周期、进度、遥测、通知及其丢失边界。
