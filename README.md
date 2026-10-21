# Atelier Kernel

**Atelier Kernel** 是一个采用 **Rust** 编写的单机多域微内核（非分布式、零网络内部通信），为多语言业务域提供统一的生命周期托管、跨域消息路由、四阶段状态回执、全链路实时观测与按域数据存储基础设施。

- ⚖️ **内核契约与宪法规范**：[CONSTITUTION.md](./CONSTITUTION.md)
- 📐 **架构与契约设计规范**：[ARCHITECTURE.md](./ARCHITECTURE.md)
- 📦 **发布包与新变体（Variant）开发指南**：[VARIANT_GUIDE.md](./VARIANT_GUIDE.md)
- 🐍 **Python 域 SDK 文档**：[sdk/python/README.md](./sdk/python/README.md)
- 🟢 **Node.js / TS 域 SDK 文档**：[sdk/nodejs/README.md](./sdk/nodejs/README.md)

---

## 核心特性

1. **非分布式、零网络内部通信**：内核与各业务域之间仅通过操作系统**父子进程匿名管道（`stdin` / `stdout` JSON-Lines）**通信，不占用任何内部 TCP/UDP 端口。
2. **内核接管域生命周期 & 语言无感**：内核读取 `kernel_boot.json` 拉起各域子进程；任何语言（Python、Node.js、Rust、Go、C++ 等）只要能读写标准输入输出 JSON 行即可接入，新增域无需修改或重新编译内核源码。
3. **物理管道级身份绑定 & 代际隔离（`instance_id`）**：域身份由内核持有的子进程管道句柄直接决定，不依赖 JSON 自声明；每次进程重启分配单调代际 `instance_id` 并通过按域互斥门序列化，彻底杜绝身份伪造、跨表越权与旧线程竞态干扰。
4. **消息 ID 由内核统一生成 & 三级幂等投递保证**：
   - 发送端无需自建全局 ID，消息进入内核瞬间分配共同 `logical_id`，并为每个目标分配独立唯一 `message_id`；
   - 第一级：按 `(source_domain, seq)` 对发送端重复提交防抖；
   - 第二级：按 `message_id` 维护逐目标独立状态账本；
   - 第三级：按单调偏序吸收重复或乱序回执。
5. **`<domain>.<handler>` 自动路由 & `TargetInvalid` 零盲发拦截**：域启动后只需发送 `RegisterHandlers` 报备处理器列表；目标不存在或未就绪时内核绝不发送消息，立即返回 `TargetInvalid`。
6. **两层消息结构，隔离业务演进**：上层为固定内核契约元数据（用于路由、回执追踪与自动建表），内层 `data` 为标准 JSON 数据包；业务数据结构演进对内核与表结构零影响。
7. **四阶段标准回执 & 三档持久化策略**：严格遵循 `Sent -> Received -> Started -> Completed` 四阶段回执闭环，内置 `Durable`（SQLite 落盘）、`Volatile`（纯内存账本）、`Ephemeral`（用后即焚）三种投递消息类型，并由 `KernelObserver` 实时旁路广播全量消息状态。
8. **存储即消息（写隔离、读可跨表）**：按域自动建表（`store_<domain>`），向 `kernel.storage` 发送 `StorageInsert`、`StorageQuery`、`StorageUpdate`、`StorageDelete` 消息即完成增删改查；写入类操作严格限制在本域表，`StorageQuery` 支持通过 `params.domain` 跨表只读查询。
9. **8 大内置基础设施方法（非业务处理器，零域间发布订阅）**：
   - 内核将注册、存储、日志、遥测、告警、运行时拓扑、观察者快照与生命周期更新收敛为统一的**内核基础设施方法**，直接挂载在多语言 SDK 的 `domain.*` 实例上；
   - 业务路由列表（`available_handlers`）严格**排除**所有 `kernel.*` 处理器，彻底杜绝域间强耦合与发布订阅反模式；
   - 观察者流、日志流、遥测流、告警流严格采用 `DeliveryPolicy::Ephemeral` 瞬态直推，零回执报账，域崩溃断开时内核自动解绑注销；
   - 支持平滑滚动更新（`update`）：隔离拉起候选实例进行主动探活（`probe_handler`），探活成功原子切换路由并平滑排干旧实例在途请求；探活失败零感自动回滚。

---

## ⚖️ 面向开发者的契约与宪法速查（Developer Guide）

编写业务域或引擎域代码前，只需掌握 **3 项核心契约**、**8 大基础设施方法** 与 **开发红线（Do's & Don'ts）**。完整规范见 **[CONSTITUTION.md](./CONSTITUTION.md)**。

### 1. 三大核心契约（Contracts `C1–C3`）

| 契约 | 开发者只需记住的规则 |
| :--- | :--- |
| **`C1` 身份与命名空间** | 域 ID（`domain_id`）在 `kernel_boot.json` 中声明，仅限 `[A-Za-z0-9_]+`。它同时决定了你的路由前缀 `<domain_id>.<handler>`、专属表 `store_<domain_id>` 与日志来源 `source_domain`。你无需在消息中填写 `source_domain`，内核按物理管道强制覆写。 |
| **`C2` 两层消息与 3 种跨域投递策略** | 外层信封归内核，内层 `data` 归业务域（内核绝不解析 `data` 内部业务字段，**业务校验、脱敏加密、引擎内部执行队列均由各业务域自治**）。调用 `domain.send(targets, message_type, params, data)` 时，跨域消息统一使用以下 3 种投递类型：<br>• `Durable`（默认）：写入 SQLite `kernel_ledger` + 内存账本，跨重启可审计恢复<br>• `Volatile`：仅驻留内存账本，适合高频传感器/UI 状态同步<br>• `Ephemeral`：在途内存追踪，到达终态（`Completed` 等）后立即从内存抹除，适合流式 Token/心跳 |
| **`C3` 四阶段回执与零盲发** | 每条跨域业务消息经历 `Sent -> Received -> Started -> Completed`（SDK 自动代发 `Received`/`Started`/`Completed`）。若目标 `<domain>.<handler>` 未注册或进程离线，内核**拒绝盲发**并立即向发送端返回 `TargetInvalid`；若处理器抛出异常，SDK 自动捕获并上报 `ExecutionFailed`。 |

> **内核内置服务目标清单（共 8 个，仅供 SDK 基础设施方法调用，不在业务路由中暴露）**：
> - `kernel.registry`：处理器注册握手与元数据文档发现（`RegisterHandlers`, `UnregisterHandlers`, `QueryHandlers`）
> - `kernel.storage`：按域建表隔离存储（`StorageInsert`, `StorageQuery`, `StorageUpdate`, `StorageDelete`）
> - `kernel.logger`：统一结构化日志、进度查询与日志流监听（`QueryLogs`, `QueryProgress`, `SubscribeLogs`, `UnsubscribeLogs`）
> - `kernel.telemetry`：高频指标发射与遥测流监听（`SubscribeTelemetry`, `UnsubscribeTelemetry`）
> - `kernel.notify`：系统级告警通知与事件广播（`SubscribeNotify`, `UnsubscribeNotify`）
> - `kernel.runtime`：域运行拓扑与装配状态查询（`QueryRuntime`, `QueryDomainAssembly`）
> - `kernel.observer`：全局消息生命周期观察与快照检索（`QueryObserver`, `SubscribeObserver`, `UnsubscribeObserver`）
> - `kernel.supervisor`：域进程生命周期控制与平滑滚动更新（`StartDomain`, `StopDomain`, `RestartDomain`, `UpdateDomain`）

---

### 2. 内核 8 大基础设施方法速查表（Kernel Infrastructure Facilities）

所有内核能力均已沉淀为 SDK 语言原生的基础设施对象与方法，区分明确的**发送端（发起/调用/上报）**与**接收端（监听/响应/流直推）**契约：

| 基础设施模块 | 内核内置目标 | 发送 / 调用方式（How to Send） | 接收 / 监听方式（How to Receive） | 适用场景与底层机制 |
| :--- | :--- | :--- | :--- | :--- |
| **1. 注册与发现** | `kernel.registry` | • 启动时自动发送 `RegisterHandlers`<br>• `domain.registry.query_handlers(...)`<br>• `domain.registry.get_description(...)` | • 返回当前可用业务路由列表及方法文档注释<br>• 格式：`{ count, routes: [...], handlers: [...] }` | 启动握手；发现目标服务路由；**严格排除所有 `kernel.*` 处理器** |
| **2. 隔离存储** | `kernel.storage` | • `domain.storage.insert(id, cat, data)`<br>• `domain.storage.update(id, data)`<br>• `domain.storage.delete(id)`<br>• `domain.storage.query(domain, ...)` | • 写入操作返回：`{ status: "ok", record_id, version }`<br>• 查询操作返回：记录对象数组 `[ { id, category, data, updated_at }, ... ]` | 存储即消息，按所属域独立建表；写操作仅限本域；读操作支持跨域安全只读 |
| **3. 统一日志** | `kernel.logger` | • `domain.logger.info / warn / error(...)`<br>• `domain.logger.phase(...)`（阶段对偶）<br>• `domain.logger.progress(...)`（进度上报）<br>• `domain.logger.query_logs(...)` | • **实时日志流监听**：<br>  - Py: `@domain.on_log(level="...")`<br>  - JS: `domain.onLog(fn, { level })`<br>• 接收：`{ level, message, source_domain, ts }` | 专属日志管道隔离输出（防 stdout 污染）；日志流直推**零 Receipt** 负担 |
| **4. 高频遥测** | `kernel.telemetry` | • `domain.telemetry.emit(metric, val, tags)` | • **实时遥测流监听**：<br>  - Py: `@domain.on_telemetry`<br>  - JS: `domain.onTelemetry(fn)`<br>• 接收：`{ metric_name, value, tags, source_domain }` | 高频传感器、性能指标上报；瞬态直推，零回执报账 |
| **5. 系统告警** | `kernel.notify` | • 同步即发即弃：`domain.notify(...)`<br>• 异步等待确认：`await domain.notify_async(...)` | • **实时告警监听**：<br>  - Py: `@domain.on_notify`<br>  - JS: `domain.onNotify(fn)`<br>• 接收：`{ severity, message, notification_type, details }` | 系统异常广播与跨域告警通知；瞬态直推，零 Receipt |
| **6. 装配拓扑** | `kernel.runtime` | • `domain.runtime.list_domains()`<br>• `domain.runtime.get_domain(domain_id)`<br>• `domain.runtime.query(...)` | • 返回全系统域装配拓扑数据：<br>  `{ count, domains: [ { domain_id, status, instance_id, routes, process: { pid, command, ... } } ] }` | 拓扑监控、探活与排障，随时获取活跃域与其 OS 进程信息 |
| **7. 观察者流** | `kernel.observer` | • 快照检索：`domain.observer.query(...)`<br>• 订阅流控制：`subscribe()` / `unsubscribe()` | • **全局生命周期流监听**：<br>  - Py: `@domain.on_observation`<br>  - JS: `domain.onObservation(fn)`<br>• 接收：`{ topic, state, message_id, logical_id, target, source_domain }` | 审计与全链路监控；消息状态瞬态推送；订阅域断开时内核自动清理注销 |
| **8. 更新与回滚** | `kernel.supervisor` | • `domain.supervisor.start(domain)`<br>• `domain.supervisor.stop(domain)`<br>• `domain.supervisor.restart(domain)`<br>• `domain.supervisor.update(domain, ...)` | • 返回更新与探活结果：<br>  `{ success: true, domain, old_instance_id, new_instance_id }`<br>  或探活失败自动回滚通知：`{ success: false, domain, message }` | 进程受控启停；平滑滚动更新（候选版本隔离探活成功后原子切换，失败自动回滚） |

---

### 3. 域开发者红线清单（Machine Constitution `M1–M7` & Guidelines `B1–B6`）

#### ✅ 正确做法（Do's） vs ❌ 严禁做法（Don'ts）

| 维度 | ❌ 严禁做法（会被内核拦截或 `--check` 报错） | ✅ 正确做法（符合宪法） |
| :--- | :--- | :--- |
| **进程与代码隔离 (`M4`)** | 跨目录直接 import 其他域的代码（如 `from ..billing import xxx` 或 `require('../billing')`） | 域间 100% 物理隔离，仅通过 `domain.send(["billing.charge"], "Durable", params, data)` 通信 |
| **数据库访问 (`M3`, `M4`, `M7`)** | 域代码中自行 `import sqlite3` 直连内核数据库，或尝试修改他域表、执行 `ALTER TABLE` | 统一调用 `domain.storage.insert / query / ...` 操作本域表；跨表只读查询传 `domain="other_domain"`；业务字段放进 `data` JSON |
| **日志与标准输出 (`M5`)** | 在域代码中使用裸 `print(...)` 或 `console.log(...)`（会直接污染 `stdout` JSON-Lines 协议管道！） | 统一使用 SDK 提供的日志器：`domain.logger.info("msg")`（或向 `stderr` 输出调试信息） |
| **处理器命名与说明 (`M1`)** | 处理器名包含点号 `.`（如 `@domain.handler("a.b")`） | 处理器名不含 `.`（如 `@domain.handler("calc_order")`），直接在方法上写注释或 Docstring，SDK 自动提取上报给内核 |
| **异常处理 (`B3`, `B6`)** | 裸 `except: pass` 静默吞掉异常或返回假成功；在处理器里写死循环阻塞管道 | 校验失败或依赖异常时直接抛出异常（让 SDK 自动触发 `ExecutionFailed` 回执）；耗时任务放入引擎域自管队列异步执行 |

#### 代码示例对比

**Python 域示例（展示 8 大基础设施方法与优雅处理）：**
```python
from atelier_sdk import AtelierDomain, MSG_DELIVERY_DURABLE, MSG_DELIVERY_EPHEMERAL

domain = AtelierDomain("order_service")

# ✅ 原生监听全局观察流（零 Receipt 负担，内核管道推流）
@domain.on_observation
def on_lifecycle(event):
    if event.get("state") == "ExecutionFailed":
        domain.logger.warning("Detected failure on %s", event.get("target"))

# ✅ 注册业务处理器（零额外参数，自动提取 Docstring）
@domain.handler("create_order")
async def handle_create_order(msg):
    """创建销售订单并持久化（SDK 自动提取此注释为处理器说明）"""
    domain.logger.info("Creating order %s", msg.data.get("order_id"))

    if "amount" not in msg.data:
        raise ValueError("Missing required field: amount")

    # ✅ 1. 存储基础设施（本域写隔离）
    await domain.storage.insert(
        record_id=msg.data["order_id"],
        category="order.v1",
        data=msg.data,
    )

    # ✅ 2. 遥测基础设施（原生发射高频指标）
    domain.telemetry.emit("order_created_total", 1, {"currency": "USD"})

    # ✅ 3. 注册表元数据基础设施（查询可用处理器与说明）
    all_handlers = await domain.registry.query_handlers()
    pay_doc = await domain.registry.get_description("payment_service.pay")

    # ✅ 4. 运行时拓扑基础设施（查询各域当前状态）
    topology = await domain.runtime.list_domains()

    # ✅ 5. 跨域标准 RPC 消息投递（严格 4-stage 回执）
    await domain.send(["payment_service.pay"], MSG_DELIVERY_DURABLE, {}, msg.data)
    return {"ok": True, "pay_handler_doc": pay_doc, "active_domains": len(topology)}

domain.run()
```

**Node.js 域示例（展示 8 大基础设施方法与优雅处理）：**
```javascript
const { AtelierDomain, MSG_DELIVERY_VOLATILE } = require("atelier-sdk");

const domain = new AtelierDomain({ domain: "telemetry_hub" });

// ✅ 原生监听日志流（过滤 WARNING 及以上）
domain.onLog((record) => {
  if (record.level === "ERROR") {
    domain.notify("error", `Alert: ${record.message}`, "LogError");
  }
}, { level: "WARNING" });

// 接收主机遥测指标并转发至实时仪表盘（SDK 自动提取上方注释作为处理器说明）
domain.handler("report_metrics", async (msg) => {
  domain.logger.info(`Received metrics from ${msg.data.host}`);

  // ✅ 1. 查询指定域的可用处理器列表与文档
  const orderHandlers = await domain.registry.queryHandlers({ domain: "order_service" });

  // ✅ 2. 跨表只读查询其他域的数据
  const orders = await domain.storage.query({ domain: "order_service", limit: 10 });

  // ✅ 3. 发射业务遥测指标
  domain.telemetry.emit("host_load", msg.data.load, { host: msg.data.host });

  // ✅ 4. 标准跨域消息投递
  await domain.send(["dashboard.refresh"], MSG_DELIVERY_VOLATILE, {}, msg.data);
  return { status: "forwarded", order_count: orders.length, handlers: orderHandlers.count };
});

domain.run();
```

---

### 4. 提交前一键宪法自检（`--check`）

在提交代码或启动变体前，随时运行内置宪法检查器，自动扫描 `M1–M7` 机器宪法合规性（配置严格校验、域源码隔离扫描、`stdout` 管道污染检测、SQLite 表结构漂移检测）：

```bash
# 检查当前目录（默认读取 ./kernel_boot.json 与 ./data）
./target/release/atelier-kernel --check

# 或指定变体目录 / 配置文件路径
./target/release/atelier-kernel --check ./my-variant
```

输出 `Result: PASSED`（退出码 `0`）即表示全部通过；若有违宪项，终端会精确打印违规的条款编号（如 `M5`）、文件路径与行号。

---

## 发布包与产品变体（Variant）极简开发

完整指南请参阅 **[VARIANT_GUIDE.md](./VARIANT_GUIDE.md)**。

### 1. 一键构建 SDK 安装包与内核运行包

```bash
./scripts/build_dist.sh
```

输出产物位于 `dist/`：
- **SDK 安装包**：
  - Python Wheel：`dist/sdk/atelier_sdk-0.1.8-py3-none-any.whl`
  - Node.js NPM 包：`dist/sdk/atelier-sdk-0.1.8.tgz`
- **内核运行包**：
  - 预编译二进制：`dist/kernel-runtime/atelier-kernel`
  - 启动配置模板：`dist/kernel-runtime/kernel_boot.template.json`
  - 变体使用指南：`dist/kernel-runtime/VARIANT_GUIDE.md`

### 2. 使用运行包 3 步启动新变体（无需 Rust 环境）

```bash
# 1) 初始化新变体脚手架（自动生成 kernel_boot.json 与示例域）
./dist/kernel-runtime/atelier-kernel --init ./my-variant

# 2) 安装 Python / Node.js 域 SDK
pip install ./dist/sdk/atelier_sdk-0.1.8-py3-none-any.whl
npm install ./dist/sdk/atelier-sdk-0.1.8.tgz

# 3) 运行宪法检查 & 启动变体（默认开启 http://127.0.0.1:8787 实时观察台；生产环境可加 --headless）
./dist/kernel-runtime/atelier-kernel --check ./my-variant
./dist/kernel-runtime/atelier-kernel --config ./my-variant/kernel_boot.json
```

---

## 源码开发与验证命令

```bash
# 1. 启动内核 + 多语言测试域 + 实时观察网页前端域 (默认监听 http://127.0.0.1:8787)
cargo run

# 2. 纯后台无头模式运行（不启动 Web 调试前端）
cargo run -- --headless

# 3. 运行机器宪法与契约合规检查（M1-M7）
cargo run -- --check

# 4. 启动终端交互式 REPL 测试菜单
cargo run -- --cli

# 5. 一键运行全功能自动化演练
cargo run -- --auto

# 6. 运行集成测试套件
cargo test
```

### 持久日志缓冲（待发布）

文件数据库启动时会创建 `<db_path>.log-spool.sqlite`（及 SQLite WAL/SHM 文件），用于普通日志的可靠接收与批量入库。不要将它当临时缓存删除；迁移、备份和恢复需将主库与日志缓冲作为一组资产一致处理。主库写锁不会使已接收日志被容量策略丢弃；导入失败保留记录并重试，原子游标防止重复。`LogStore.flush(timeout)` 返回确认结果；运行健康的 `logging.persistence_error` 暴露接收/导入故障。接收等待本地 FULL 同步提交，非零等待；瞬态进度、console_only 及实时订阅维持原有语义。