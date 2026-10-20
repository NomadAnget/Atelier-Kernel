# Atelier Kernel 宪法与核心契约（V4.0）

> 本文档是 **Atelier Kernel（Rust 单机多域微内核）** 的最高架构宪法与契约基线。
> 内核只做一件事的地基：**跨语言子进程物理管道通信中转站 + 三级投递幂等账本 + 按域存储枢纽 + 统一结构化日志黑匣子**。
> 一切设计服务于 **进程级认知隔离**：新增或修改一个业务域时，只需理解本域收发的 `<domain>.<handler>` 消息契约与本域目录代码，无需阅读其他域的实现。

---

## 一、三大内核契约（Kernel Contracts）

### C1 · 物理管道身份与命名空间契约
- **管道即身份**：内核在拉起子进程时，直接创建独立双匿名管道（`ATELIER_IPC_READ_FD` / `ATELIER_IPC_WRITE_FD`）并与 `domain_id` 物理绑定；子进程的 `stdout` / `stderr` 与协议通道物理隔离，由内核统一作为子进程诊断日志采集。
- **命名空间强绑定**：
  - 路由地址固定为 `<domain_id>.<handler>`；
  - 专属存储表固定为 `store_<domain_id>`；
  - 入站日志、`stdout` 与 `stderr` 来源固定由内核覆写为 `source_domain = domain_id`，彻底杜绝身份伪造。

### C2 · 两层消息结构与 10 类标准 `message_type` 契约
- **上层为内核契约元数据**：包含 `logical_id`、`message_id`、`source_domain`、`targets`、`message_type`、`params`、`timestamp`。
- **内层为业务数据包（`data`）**：标准 JSON 格式。业务数据结构演进、字段校验、前端数据脱敏等均由对应业务域自治，**内核行为与数据库表结构完全零感知**。
- **`message_type` 与内置基础设施契约**：
  1. **跨域业务消息投递策略（`target = "<domain>.<handler>"`，严格 4-stage 回执闭环）**：
     - `Durable`：写入 SQLite 持久投递账本（`kernel_ledger`）+ 内存账本，支持跨重启状态恢复与审计（未显式指定时默认按 `Durable` 处理）；
     - `Volatile`：仅进入内存账本与实时观察器（`KernelObserver`），**零磁盘写入**；
     - `Ephemeral`：瞬态流式投递，不落盘，仅在途（In-Flight）驻留内存账本以完成四阶段回执，到达终态后立即自动从内存账本驱逐（$O(1)$ 内存开销）。
  2. **内核 8 大内置基础设施（`target = "kernel.*"`，SDK 原生方法调用，不对外作为业务路由暴露）**：
     - `kernel.registry`：`RegisterHandlers`、`UnregisterHandlers`、`QueryHandlers`（元数据发现，严格排除 `kernel.*`）；
     - `kernel.storage`：`StorageInsert`、`StorageQuery`、`StorageUpdate`、`StorageDelete`（按域隔离存储）；
     - `kernel.logger`：`QueryLogs`、`QueryProgress`、`SubscribeLogs`、`UnsubscribeLogs`（日志与进度查询/监听）；
     - `kernel.telemetry`：`SubscribeTelemetry`、`UnsubscribeTelemetry`（高频遥测指标）；
     - `kernel.notify`：`SubscribeNotify`、`UnsubscribeNotify`（系统告警通知广播）；
     - `kernel.runtime`：`QueryRuntime`、`QueryDomainAssembly`（域运行态拓扑与装配查询）；
     - `kernel.observer`：`QueryObserver`、`SubscribeObserver`、`UnsubscribeObserver`（生命周期观察与快照检索）；
     - `kernel.supervisor`：`StartDomain`、`StopDomain`、`RestartDomain`、`UpdateDomain`（域进程管控与带探活的平滑滚动更新）。
  3. **系统事件推流与零发布订阅约束**：
     - 域与域之间**严禁发布订阅强绑定**，只允许精准单目标 RPC 通信；
     - 仅有内核流（`LogEvent`、`TelemetryEvent`、`NotifyEvent`、`ObservationEvent`）由 SDK 原生方法监听，并强制采用 `DeliveryPolicy::Ephemeral` 瞬态直推（**零 Receipt 确认帧产生**，域断开或崩溃时内核自动解绑注销）。

### C3 · 四阶段回执与零盲发契约

- **观察查询不得自包含放大**：`QueryObserver` 的完整结果只交付请求方；其自身生命周期事件的 details 只记录返回条数等有界元信息，不再次嵌入 events 快照。`kernel.observer/QueryObserver` 与 `kernel.runtime` 的只读查询按 Ephemeral 处理，保留在途四阶段回执，终态后不在投递账本保存查询结果；业务 Durable 消息的持久化与回执不变。此约束在内核实现，SDK 方法与完整返回结构保持一致。
- **历史账本恢复按需取载荷**：启动时恢复 Durable 在途记录和历史身份索引，历史终态结果继续保留于 SQLite，按原 message_id/logical_id 查询时完整读取；不得为恢复服务而删除历史，也不得把全部历史结果 JSON 一次性展开到内存。跨重启回执查询、终态不可覆盖和消息身份保持不变。
- **只读基础设施结果不做重复常驻缓存**：registry/QueryHandlers、storage/StorageQuery、logger/QueryLogs/QueryProgress 与上述 runtime/observer 查询统一使用 Ephemeral 四阶段回执；完整查询结果交付请求方，其 Completed 观察事件仅携带有界条数元信息。存储写入、注册变更、业务消息的投递策略和数据本身不变。客户端需要重新读取时调用同一 SDK 查询方法，不依赖查询消息终态常驻。
- **标准四阶段回执**：每条目标消息严格经历 `Sent`（已发出）$\rightarrow$ `Received`（已接收）$\rightarrow$ `Started`（开始执行）$\rightarrow$ `Completed`（执行完成），或进入异常终态 `TargetInvalid` / `ContractInvalid` / `ExecutionFailed`。
- **发布顺序与账本一致**：独立双匿名管道的收发线程并行工作；初次投递、回执推进与故障终结在内核内串行完成状态推进和回执发布，保证发送端和观察器均不先看到后续阶段再看到 `Sent`。串行区不执行域业务，不等待子进程实际读写管道；SDK 不承担回执重排。
- **零盲发前置拦截**：当目标 `<domain>.<handler>` 不存在或目标域未处于 `Active` 就绪态时，内核**绝不向管道盲发消息**，立即前置拦截并向发送端返回 `TargetInvalid`。

---

## 二、宪法分两级

| 级别 | 定义 | 执法者 |
| :--- | :--- | :--- |
| **第一级 · 机器宪法（M1–M7）** | 机器可精确判定对错的硬性规则 | 内核运行时闸门 + 启动期自检 + `atelier-kernel --check` 静态检查器 |
| **第二级 · 行为规范（B1–B6）** | 机器宪法全集 + 架构与工程自律准则 | 开发者与 AI Agent 共同遵守、代码审查把关 |

**项目级宪法检查内置于内核可执行文件中**，一条命令即可检查变体配置、数据库表结构与各语言业务域源码：

```bash
atelier-kernel --check ./kernel_boot.json
# 或直接指定变体/域目录：
atelier-kernel --check .
```

### 第一级 · 机器宪法（M1–M7）

| 编号 | 规则名称 | 核心约束内容 | 静态检查 (`--check`) | 启动期 / 运行时执法 |
| :--- | :--- | :--- | :--- | :--- |
| **M1** | **域标识与路由注册律** | `domain_id` 必须为非空 ASCII 字母、数字或 `_`，且在变体内全局唯一；处理器名非空且不含 `.`，注册后生成 `<domain_id>.<handler>` | ✓ 校验 `domain_id`合法性与唯一性 | 运行时 [`KernelRouter`](src/router.rs) 强校验 |
| **M2** | **管道即身份律（防伪造）** | 一切入站消息、回执、日志的发送域身份由独立双匿名协议管道决定；回执上报的目标 `<domain>.<handler>` 首段必须等于上报管道所属 `domain_id` | — | 运行时 [`KernelLedger`](src/ledger.rs) 越权拒收（`UnauthorizedTarget`） |
| **M3** | **写权随域走（写隔离、读共享）** | `StorageInsert` / `StorageUpdate` / `StorageDelete` 只能修改本域 `store_<source_domain>`；`StorageQuery` 支持只读跨域查询 | — | 运行时 [`DomainStorageEngine`](src/storage.rs) 拒越域写（`CrossDomainWriteForbidden`） |
| **M4** | **进程即边界律（禁跨域私调与直连库）** | 各域为独立子进程，严禁在域源码中跨目录相对 `import` / `require` 其他域的私有文件，严禁业务域绕过 `kernel.storage` 直接读写内核 SQLite 数据库文件 | ✓ AST/源码扫描跨域相对导入与直连 SQLite | OS 独立进程地址空间物理隔离 |
| **M5** | **协议管道纯净律（独立双匿名管道专管）** | 内核与域通过独立双匿名管道（`ATELIER_IPC_READ_FD` / `ATELIER_IPC_WRITE_FD`）收发 JSON-Lines 协议帧，与 `stdout/stderr` 物理隔离；严禁业务域代码绕过 SDK 私自读取或篡改专用 IPC 描述符 | ✓ 静态扫描业务域源码私自访问 `ATELIER_IPC_*_FD` | SDK 启动即锁定描述符不可继承并擦除环境变量，运行时非法帧立即拦截返回 `ContractInvalid` |
| **M6** | **配置显式律** | `kernel_boot.json` 严禁包含未知字段（`deny_unknown_fields`），`domain_id` 不得重复，工作目录与入口脚本路径必须真实存在 | ✓ 严格反序列化与入口文件存在性校验 | 启动期 [`KernelBootConfig::load_from_file`](src/supervisor.rs) 即刻报错 |
| **M7** | **两层存储防漂移律** | 内核核心表（`kernel_ledger`、`hub_logs`、`store_<domain>`）固定为上层契约列结构，启动时校验实表列结构完整性；实表结构漂移立即报错，绝不静默改表 | ✓ 若存在数据库文件则校验表结构 | 启动期与建表期 `PRAGMA table_info` 结构防漂移校验 |

### 第二级 · 行为规范（B1–B6）

- **B1 单一逻辑**：同一逻辑不散落多处。新增能力前先检索现有模块。
- **B2 认知隔离**：修改一个域只需关注本域收发的 `<domain>.<handler>` 消息契约与本域代码，跨域协作一律走内核消息。
- **B3 显式失败，不静默兜底**：前置条件缺失、配置错误、契约不符、目标不可达（`TargetInvalid`）立即显式返回错误状态，绝不静默吞掉。
- **B4 内核零业务绑定**：内核与 SDK 严守传输与契约边界（零业务逻辑、零业务 Schema 耦合）；业务数据脱敏、AI 推理引擎执行队列管理等一律由对应业务域/引擎域自治。
- **B5 契约同步律**：协议、宪法或 SDK 行为发生变更时，必须同步更新本文档与 Python / Node.js 双端 SDK。
- **B6 Handler 异步与幂等律**：Handler 保持非阻塞响应（耗时计算或排队交由域内异步任务或自有执行队列处理），并基于内核分配的 `message_id` 保障幂等。
