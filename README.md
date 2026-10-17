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

---

## ⚖️ 面向开发者的契约与宪法速查（Developer Guide）

编写业务域或引擎域代码前，只需掌握 **3 项核心契约** 与 **开发红线（Do's & Don'ts）**。完整规范见 **[CONSTITUTION.md](./CONSTITUTION.md)**。

### 1. 三大核心契约（Contracts `C1–C3`）

| 契约 | 开发者只需记住的规则 |
| :--- | :--- |
| **`C1` 身份与命名空间** | 域 ID（`domain_id`）在 `kernel_boot.json` 中声明，仅限 `[A-Za-z0-9_]+`。它同时决定了你的路由前缀 `<domain_id>.<handler>`、专属表 `store_<domain_id>` 与日志来源 `source_domain`。你无需在消息中填写 `source_domain`，内核按物理管道强制覆写。 |
| **`C2` 两层消息与 9 种标准 `message_type`** | 外层信封归内核，内层 `data` 归业务域（内核绝不解析 `data` 内部业务字段，**业务校验、脱敏加密、引擎内部执行队列均由各业务域自治**）。调用 `domain.send(targets, message_type, data)` 时，跨域消息统一使用以下 3 种投递类型：<br>• `Durable`（默认）：写入 SQLite `kernel_ledger` + 内存账本，跨重启可审计恢复<br>• `Volatile`：仅驻留内存账本，适合高频传感器/UI 状态同步<br>• `Ephemeral`：在途内存追踪，到达终态（`Completed` 等）后立即从内存抹除，适合流式 Token/心跳 |
| **`C3` 四阶段回执与零盲发** | 每条消息经历 `Sent -> Received -> Started -> Completed`（SDK 自动代发 `Received`/`Started`/`Completed`）。若目标 `<domain>.<handler>` 未注册或进程离线，内核**拒绝盲发**并立即向发送端返回 `TargetInvalid`；若处理器抛出异常，SDK 自动捕获并上报 `ExecutionFailed`。 |

> **内置内核服务消息类型（共 6 种，由 SDK 封装方法自动调用）**：
> - `kernel.registry`：`RegisterHandlers`、`UnregisterHandlers`
> - `kernel.storage`：`StorageInsert`、`StorageQuery`、`StorageUpdate`、`StorageDelete`

---

### 2. 域开发者红线清单（Machine Constitution `M1–M7` & Guidelines `B1–B6`）

#### ✅ 正确做法（Do's） vs ❌ 严禁做法（Don'ts）

| 维度 | ❌ 严禁做法（会被内核拦截或 `--check` 报错） | ✅ 正确做法（符合宪法） |
| :--- | :--- | :--- |
| **进程与代码隔离 (`M4`)** | 跨目录直接 import 其他域的代码（如 `from ..billing import xxx` 或 `require('../billing')`） | 域间 100% 物理隔离，仅通过 `domain.send(["billing.charge"], "Durable", data)` 通信 |
| **数据库访问 (`M3`, `M4`, `M7`)** | 域代码中自行 `import sqlite3` 直连内核数据库，或尝试修改他域表、执行 `ALTER TABLE` | 仅通过 `domain.storage.insert / query / update / delete` 操作本域表；跨表只读查询传 `domain="other_domain"`；业务字段扩展直接放进 `data` JSON |
| **日志与标准输出 (`M5`)** | 在域代码中使用裸 `print(...)` 或 `console.log(...)`（会直接污染 `stdout` JSON-Lines 协议管道！） | 统一使用 SDK 提供的日志器：`domain.logger.info("msg", {"k": "v"})`（或向 `stderr` 输出调试信息） |
| **处理器命名 (`M1`)** | 处理器名包含点号 `.`（如 `@domain.on("a.b")`） | 处理器名不含 `.`（如 `@domain.on("calc_order")`），完整路由由内核拼接为 `<domain>.calc_order` |
| **异常处理 (`B3`, `B6`)** | 裸 `except: pass` 静默吞掉异常或返回假成功；在处理器里写死循环阻塞管道 | 校验失败或依赖异常时直接抛出异常（让 SDK 自动触发 `ExecutionFailed` 回执）；耗时任务放入引擎域自管队列异步执行 |

#### 代码示例对比

**Python 域示例：**
```python
from atelier_sdk import AtelierDomain, MSG_DELIVERY_DURABLE, MSG_DELIVERY_EPHEMERAL

domain = AtelierDomain("order_service")

@domain.on("create_order")  # ✅ M1: 处理器名不含 '.'
def handle_create_order(ctx):
    # ❌ M5 违宪: print("creating order...")  -> 会破坏 stdout 协议管道！
    # ✅ M5 合规: 使用 domain.logger
    domain.logger.info("Creating order", {"order_id": ctx.data.get("order_id")})

    if "amount" not in ctx.data:
        # ✅ B3 合规: 显式抛出异常，SDK 自动回传 ExecutionFailed 回执
        raise ValueError("Missing required field: amount")

    # ✅ M3/M4 合规: 通过存储消息写入本域表 store_order_service
    domain.storage.insert(
        record_key=ctx.data["order_id"],
        schema_version="order.v1",
        data=ctx.data,  # ✅ C2/B4: 如需敏感字段脱敏，由业务域在写入前自行处理
    )

    # ✅ C2 合规: 按业务性质选择 Durable / Volatile / Ephemeral 投递策略
    domain.send(["payment_service.pay"], MSG_DELIVERY_DURABLE, ctx.data)
    domain.send(["ui_stream.push_progress"], MSG_DELIVERY_EPHEMERAL, {"step": "created"})
    return {"ok": True}

domain.run()
```

**Node.js 域示例：**
```javascript
const { AtelierDomain, MSG_DELIVERY_VOLATILE } = require("atelier-sdk");

const domain = new AtelierDomain("telemetry_hub");

domain.on("report_metrics", (ctx) => {
  // ❌ M5 违宪: console.log("metrics:", ctx.data);
  // ✅ M5 合规:
  domain.logger.info("Received metrics", { host: ctx.data.host });

  // ✅ M3 合规: 跨表只读查询其他域的数据（写操作仅限本域）
  domain.storage.query({ domain: "order_service", limit: 10 });

  // ✅ C2 合规: 高频状态同步使用 Volatile（纯内存账本，零磁盘 I/O）
  domain.send(["dashboard.refresh"], MSG_DELIVERY_VOLATILE, ctx.data);
  return { status: "forwarded" };
});

domain.run();
```

---

### 3. 提交前一键宪法自检（`--check`）

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
  - Python Wheel：`dist/sdk/atelier_sdk-0.1.4-py3-none-any.whl`
  - Node.js NPM 包：`dist/sdk/atelier-sdk-0.1.4.tgz`
- **内核运行包**：
  - 预编译二进制：`dist/kernel-runtime/atelier-kernel`
  - 启动配置模板：`dist/kernel-runtime/kernel_boot.template.json`
  - 变体使用指南：`dist/kernel-runtime/VARIANT_GUIDE.md`

### 2. 使用运行包 3 步启动新变体（无需 Rust 环境）

```bash
# 1) 初始化新变体脚手架（自动生成 kernel_boot.json 与示例域）
./dist/kernel-runtime/atelier-kernel --init ./my-variant

# 2) 安装 Python / Node.js 域 SDK
pip install ./dist/sdk/atelier_sdk-0.1.4-py3-none-any.whl
npm install ./dist/sdk/atelier-sdk-0.1.4.tgz

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
