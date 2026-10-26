> 历史记录：以下结论、版本、计数和部署状态对应记录当时的基线，不代表当前发布状态。当前接口以[文档索引](docs/INDEX.md)及宪法为准；旧证据保留，不作当前保证。

# Atelier Kernel v0.1.8 发版说明

Atelier Kernel **v0.1.8** 正式发布！本版本确立了微内核架构下的 **8 大内核原生基础设施能力（8 Native Infrastructure Facilities）**，将内核内置处理器全面跃升为具备完备「发送 (Send) 与接收 (Receive)」语义的原生服务门面；同时新增候选实例平滑更新（Candidate Rolling Update）与回滚机制，全面升级 Python 与 Node.js 领域 SDK，并对 CI/CD 构建流水线进行了 Node 24 现代化与权限加固。

---

## 🌟 核心更新亮点

### 1. 全套 8 大内核原生基础设施能力（8 Native Infrastructure Facilities）
内核原生挂载 8 项核心系统服务，提供微秒级本地进程间统一调用标准，彻底告别外部中间件依赖：

1. **服务注册与发现 (`kernel.registry`)**：
   - 支持跨域服务报备（`RegisterHandlers`）、动态注销（`UnregisterHandlers`）与服务能力发现（`QueryAvailableHandlers`）。
   - 内置代码注释与 Docstring 自动提取能力，实时生成各域能力的契约与描述。
2. **域隔离持久化存储 (`kernel.storage`)**：
   - 基于 SQLite WAL 的 Storage-as-Messaging 架构，提供严格的域键空间物理隔离。
   - 提供 `StoragePut`、`StorageGet`、`StorageDelete`、`StorageBatch`、`StorageScan` 强一致 ACID 原语。
3. **多级别日志与进度查询 (`kernel.logger`)**：
   - 具备内存环形日志缓冲检索与 SQLite 磁盘持久化双层查询（`QueryLogs`），支持游标分页与等级过滤。
   - 原生聚合任务生命周期，支持根据 `task_id` 查询跨域执行进度（`QueryProgress`）。
4. **高频指标遥测 (`kernel.telemetry`)**：
   - 专为监控度量设计的 `DeliveryPolicy::Ephemeral` QoS 机制。
   - 瞬态单帧保留、高频丢弃不阻塞，实时上报 CPU、内存及吞吐指标（`MetricTick`）。
5. **通知告警中心 (`kernel.notify`)**：
   - 面向通知与系统告警的 `DeliveryPolicy::Volatile` QoS 机制。
   - 提供 `AlertPush` 与 `NoticePush`，所有告警与通知均自动归集入 `hub_logs` 审计表，实现系统级事件回溯。
6. **运行时实例监控 (`kernel.runtime`)**：
   - 实时采集各域 OS 进程的 PID、活跃状态、存活时长、消息收发计数与健康状态（`QueryInstanceInfo`）。
7. **实时事件观察台 (`kernel.observer`)**：
   - 提供事件广播订阅体系（`SubscribeEvents`），支持按事件类别筛选生命周期变化与状态回执。
   - 具备长连接断开检测与观察者上下文自动清理机制。
8. **域进程生命周期监管 (`kernel.supervisor`)**：
   - 原生 OS 进程托管，支持显式启动、停止与故障隔离。
   - 异常退出指数退避重试（Exponential Backoff）与最大重启次数保护；显式停止不触发误重启。
   - **平滑热更新与回滚**：支持候选实例灰度部署（`StartCandidate`）、流量无缝晋升（`PromoteCandidate`）与异常一键回滚（`RollbackCandidate`）。

---

### 2. 多语言 SDK 原生门面全面升级（Python / Node.js）
Python 与 Node.js 领域 SDK 同步升级至 `0.1.8`，彻底消除手动拼接内部协议的繁琐操作：

- **强类型直觉式调用门面**：
  - **Python SDK**：挂载 `domain.registry`、`domain.storage`、`domain.logger`、`domain.telemetry`、`domain.notify`、`domain.runtime`、`domain.observer`、`domain.supervisor` 命名空间，完整提供同步与异步（`*_async`）双套 API。
  - **Node.js SDK**：同样挂载 8 大原生门面，全流程支持 Promise / async-await，提供完备的 TypeScript 类型声明（`index.d.ts`）。
- **完善的契约文档**：
  - SDK 文档详尽补齐 8 大设施的**发送方式 (How to Send)** 与 **接收/响应格式 (How to Receive)**，包含入参与出参的 JSON Schema 样例。

---

### 3. CI/CD 流水线现代化与安全性加固
- **Node 24 Runtime 升级**：
  - 全面更新 GitHub Actions 至最新主版本（`checkout@v7`, `setup-python@v7`, `setup-node@v7`, `cache@v5`, `upload-artifact@v7`, `download-artifact@v8`, `softprops/action-gh-release@v3`）。
  - 彻底消除了旧版 Actions 的 Node 20 运行时弃用警告。
- **发布权限防护**：
  - 移除外部 `pull_request` 自动触发机制，确保仅仓库所有者的直接推送与 Tag 发布能够执行 CI/CD 构建与发版任务。
- **零依赖 Wheel 与 NPM 打包增强**：
  - `scripts/build_dist.sh` 升级为动态版本自适应构建，杜绝手动修改脚本导致的打包版本脱节。

---

### 4. 严苛的自动化测试与合规性审查
- **25 项 Rust 单元与集成测试全部通过**：
  - 覆盖单域阻塞线头阻塞隔离、4 阶段收据顺序性、SQLite 并发共享缓存、故障重启限制与多语言并发压力。
- **机器宪法审查（Machine Constitution）通过**：
  - 执行 `cargo run -- --check`，M1~M7 机器宪法规范 100% 合规，0 违规条目。
- **端到端变体冒烟验证**：
  - 自动化验证 `--init` 生成变体脚手架、自动报备路由、4 阶段回执确认与跨域日志持久化，全平台全绿通过。

---

## 📦 发版产物与安装方式

### 1. 预编译二进制运行时
可在 [GitHub Releases v0.1.8](https://github.com/NomadAnget/Atelier-Kernel/releases/tag/v0.1.8) 直接下载对应平台的独立运行时运行包：
- Linux x86_64: `atelier-kernel-runtime-linux-x86_64.tar.gz`
- macOS arm64 (Apple Silicon): `atelier-kernel-runtime-macos-arm64.tar.gz`
- Windows x86_64: `atelier-kernel-runtime-windows-x86_64.tar.gz`

### 2. 领域 SDK 安装
```bash
# Python 领域 SDK
pip install atelier_sdk-0.1.8-py3-none-any.whl

# Node.js 领域 SDK
npm install atelier-sdk-0.1.8.tgz
```

---

## 🔄 升级指南
- 从 `v0.1.7` 升级：完全向后兼容。推荐将应用域逻辑逐步切换至 `domain.logger` / `domain.telemetry` / `domain.notify` / `domain.storage` 等原生设施门面，以获取最佳的吞吐效率与类型安全保障。
