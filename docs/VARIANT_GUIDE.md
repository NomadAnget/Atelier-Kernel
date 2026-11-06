# Atelier Kernel 发布包与产品变体（Variant）开发指南

本文档面向**变体组装者**与**业务域开发者**，说明如何直接使用 **Atelier Kernel 预编译运行包** 与 **多语言 Domain SDK 安装包** 构建新的产品变体，**全程无需安装 Rust 编译环境，也无需修改内核源码**。

---

## 一、发布包组成结构

从 GitHub Actions Artifacts / Releases（或本地运行 `./scripts/build_dist.sh` 后的 `dist/` 目录）获取的发布包包含两大核心部分：

```text
atelier-kernel-runtime-<os>-<arch>/
├── atelier-kernel                        # 1. 预编译 Rust 微内核可执行程序（Windows 下为 atelier-kernel.exe）
├── kernel_boot.template.json             # 2. 默认多域启动配置模板
├── README.md                             # 3. 内核介绍与最高准则
├── docs/                                 # 4. 宪法、架构、能力与 SDK 文档（入口 docs/INDEX.md），含 kernel.svg 图标
└── sdk/
    ├── atelier_sdk-<version>-py3-none-any.whl # 5. Python 域官方 SDK 安装包 (Wheel)
    └── atelier-sdk-<version>.tgz              # 6. Node.js / TypeScript 域官方 SDK 安装包 (NPM Tarball)
```

---

## 二、3 步快速创建一个新产品变体（Variant）

### 第 1 步：一键初始化变体目录脚手架

使用内核自带的 `--init` 命令，可在任意位置生成一套干净的变体工程目录（包含 `kernel_boot.json` 与示例域脚本）：

```bash
./atelier-kernel --init ./my-product-variant
```

生成的目录结构如下：
```text
my-product-variant/
├── kernel_boot.json          # 该变体的域编排与启动配置
└── domains/
    └── sample_domain.py      # 示例业务域脚本
```

---

### 第 2 步：安装 Domain SDK 并编写业务域

#### 方式 A：开发 Python 业务域

1. **安装 Python Wheel 包**：
   ```bash
   pip install ./sdk/atelier_sdk-<version>-py3-none-any.whl
   ```

2. **编写业务域代码（例如 `domains/billing_domain.py`）**：
   只需导入 `AtelierDomain`，用 `@domain.handler(...)` 注册处理器，无需关心底层专用端点与四阶段回执细节：
   ```python
   #!/usr/bin/env python3
   import asyncio

   from atelier_sdk import AtelierDomain, DomainExecutionFailed

   domain = AtelierDomain("billing")          # 必须等于 kernel_boot.json 里的 domain_id

   @domain.handler("charge")
   async def on_charge(msg):
       order_id = msg.data.get("order_id", "ORD-001")
       amount = msg.data.get("amount", 0)
       if amount <= 0:
           # 抛出 DomainExecutionFailed，SDK 自动向内核汇报 ExecutionFailed 回执
           raise DomainExecutionFailed({"error": "invalid_amount", "amount": amount})

       # 1. 存储即消息：写入本域专属表 store_billing（写操作严格物理隔离）
       await domain.storage_insert(
           record_id=order_id,
           category="invoice",
           data={"order_id": order_id, "amount": amount, "from": msg.source_domain},
       )

       # 2. 跨域读取：读取其他域（如 order 域）的表数据（只读共享）
       await domain.storage_query(domain="order", record_id=order_id)

       # 3. 跨域消息发送：调用其他域的处理器
       await domain.send(
           targets=["web_ui.push_alert"],
           message_type="BillingSuccess",
           params={"severity": "info"},
           data={"text": f"订单 {order_id} 扣款成功: ¥{amount}"},
       )

       # 4. 直接 return 字典，SDK 自动汇报 Completed 回执
       return {"ok": True, "order_id": order_id, "charged": amount}

   if __name__ == "__main__":
       asyncio.run(domain.run_async())
   ```

---

#### 方式 B：开发 Node.js / TypeScript 业务域

1. **安装 Node.js NPM 包（自带 TypeScript `.d.ts` 类型声明）**：
   ```bash
   npm install ./sdk/atelier-sdk-<version>.tgz
   ```

2. **编写业务域代码（例如 `domains/notify_domain.js`）**：
   ```javascript
   #!/usr/bin/env node
   const { AtelierDomain, DomainExecutionFailed } = require("atelier-sdk");

   const domain = new AtelierDomain();

   domain.handler("send_sms", async (msg) => {
     const { phone, content } = msg.data;
     if (!phone) {
       throw new DomainExecutionFailed({ error: "missing_phone" });
     }

     // 写入本域专属存储表 store_notify
     domain.storageInsert(`sms_${msg.message_id}`, "sms_log", {
       phone,
       content,
       requested_by: msg.source_domain,
     });

     return { delivered: true, phone };
   });

   domain.run();
   ```

---

### 第 3 步：在 `kernel_boot.json` 中装配域并启动内核

编辑变体目录下的 `kernel_boot.json`，将你的业务域加入 `domains` 列表：

```json
{
  "db_path": "./data/atelier_kernel.db",
  "web_ui_bind": "0.0.0.0:8787",
  "web_ui_enabled": false,
  "domains": [
    {
      "domain_id": "billing",
      "command": "python3",
      "args": ["-u", "domains/billing_domain.py"],
      "working_dir": null
    },
    {
      "domain_id": "notify",
      "command": "node",
      "args": ["domains/notify_domain.js"],
      "working_dir": null
    }
  ]
}
```

然后启动内核：

```bash
# 默认：无头微内核（不监听任何 HTTP 端口，仅托管域、路由、存储与日志）
./atelier-kernel --config ./my-product-variant/kernel_boot.json

# 需要实时观察台时再显式开启（监听 web_ui_bind，默认 0.0.0.0:8787）
./atelier-kernel --config ./my-product-variant/kernel_boot.json --web-ui
# 也可以把 "web_ui_enabled": true 写进配置，省掉命令行开关
```

> **Web UI 默认关闭**。加 `--headless` 可强制关闭，即使配置里写了 `web_ui_enabled: true` 也不生效。

---

## 三、内核命令行参数一览 (`atelier-kernel --help`)

| 参数 | 说明 |
| :--- | :--- |
| `--config <path>` | 指定变体启动配置文件路径（默认：`./kernel_boot.json`） |
| `--init <dir>` | 在目标目录一键生成新变体配置模板与示例域脚手架 |
| `--check <path>` | 机器宪法静态检查（M1–M7）+ 账本行级完整性；退出码非 0 表示有违规 |
| `--smoke-test` | 端到端自检：等所有域注册、向每个域的首个路由发探针、校验四阶段与 `hub_logs` |
| `--web-ui` / `--dashboard` | 启动实时观察台（默认关闭） |
| `--headless` | 强制无头运行，优先级高于配置里的 `web_ui_enabled` |
| `--cli` | 启动终端交互式 REPL 测试前端域 |
| `--auto` / `-a` | 运行内置 7 项内核契约与幂等自动化自检套件并退出 |
| `--version` / `-v` | 查看内核版本号 |
| `--help` / `-h` | 查看命令行帮助 |

`--port-relay` 是内核托管端口转发时自用的内部子进程模式，不要手工调用。

---

## 四、存储枢纽（`kernel.storage`）权限与 API 速查

内核为每个域自动创建独立的 SQLite 表 `store_<domain_id>`，遵循**“写严格隔离、读可跨表”**原则：

| 操作 | `message_type` | SDK 方法（Python / Node.js） | 权限规则 |
| :--- | :--- | :--- | :--- |
| **新增/覆盖** | `StorageInsert` | `storage_insert` / `storageInsert` | **仅限本域表**（传入其他 `domain` 参数将被拒绝并返回 `ExecutionFailed`） |
| **局部/全量更新** | `StorageUpdate` | `storage_update` / `storageUpdate` | **仅限本域表**（默认 `patch: true` 递归合并内层 `data` JSON，支持业务结构无感演进） |
| **删除** | `StorageDelete` | `storage_delete` / `storageDelete` | **仅限本域表**（按 `record_id` 或 `category` 删除） |
| **查询** | `StorageQuery` | `storage_query` / `storageQuery` | **可查本域或跨域只读**：不传 `domain` 时查本域表；传 `domain="order"` 时只读查询 `store_order` 表 |

---

## 五、内核统一接管的多语言日志体系（Unified Multi-State Logging）

Atelier Kernel 内建了与 `atelier-core` 一脉相承的黑匣子日志基础设施。不仅能跨语言统一接管子进程日志，还原生支持**多状态结构化日志**：

### 域的独立环境变量

域启动项支持可选的 `env` 字符串映射，无需 `/usr/bin/env` 或 shell 包装：

```json
{
  "domain_id": "engines",
  "command": "./.venv/bin/python",
  "args": ["-m", "domains.engines"],
  "env": {"GCP_MODELS_DIR": "/home/Architecture/checkpoints/"},
  "working_dir": "."
}
```

未设置 `env` 或设置 `{}` 时保持继承环境的原行为；配置值覆盖继承值，空字符串是有效值。
这些值按字面传入进程，不做 `$VAR`、`~`、shell 或相对路径展开；相对值由业务进程自行解释。
手动启动、保存配置的重启、自动重启和候选域更新使用同一套进程启动逻辑；
传入新 spec 的更新以新 spec 的完整 `env` 为准，省略 `env` 代表空覆盖映射而非合并旧 spec。
不传 spec 的启动/更新复用已保存 spec，因此保留其环境设置。失败候选不替换旧 spec。

`ATELIER_IPC_*`（不区分 ASCII 大小写）保留给内核，配置拒绝覆盖它。
环境键不能是空字符串、不能含 `=` 或 NUL；值不能含 NUL；仅大小写不同的键同时出现会被拒绝。
JSON 的 `env` 必须是字符串到字符串映射，不能用 null、数组或数值代替。
运行态摘要显示真实 command/args，不返回 env 值；不要从该摘要重建完整 spec。
对于已装配域，Python 使用 SDK `supervisor.start(domain)` / `restart(domain)`；Node 使用
`supervisorStartAsync({domain})`（省略 spec）或 `supervisor.restart(domain)` 复用保存配置。

### 1. 三大出口（Three Sinks）

- **控制台（Console）**：紧凑单行格式 `HH:MM:SS LEVEL [layer/component] message <phase>`，TTY 环境色彩高亮。
- **实时订阅流（Subscriber Stream / SSE）**：基于事件流扇出至 Web 前端与观察者（`/api/logs/stream`），慢订阅丢帧绝不阻塞业务。
- **内核直管持久化表（SQLite `hub_logs`）**：由内核独立后台写线程按 WAL 批处理异步落库，队列饱和丢帧，黑匣子绝不反噬被观察者。

### 2. 多状态日志形态（Multi-State Log Patterns）

日志只有四个方法：`logger.debug` / `info` / `warn` / `error`。多状态是通过**关键字参数**表达的——它们都是 `LogRecord` 字段，直接透传给内核。

| 日志形态 | 用法 | 去向 | 特性与用途 |
| :--- | :--- | :--- | :--- |
| **分级日志** | `logger.info(msg, layer=..., component=...)` | 控制台 + SSE + `hub_logs` | 记录事实与业务状态。 |
| **生命周期起止配对** | `logger.info(msg, phase="start")` / `phase="finish"` | 控制台 + SSE + `hub_logs` | 结构化字段 `phase` 原生配对同一操作起止，无需靠正则猜文本前缀。 |
| **临时实时进度** | `logger.info(msg, ephemeral=True, key=..., frac=..., waiting=..., done=...)` | **仅 SSE 实时流** | 前端按 `key` 原地更新进度条，`frac`∈[0,1]，`waiting=True` 渲等待脉冲，`done=True` 清除。**不落 `hub_logs`**，防止高频刷屏膨胀数据库。 |
| **显式进度终态** | 同上再加 `state="completed" \| "failed" \| "cancelled"` | **仅 SSE 实时流** | 显式状态优先于 `done`；终态会把该 `key` 从活动进度表移除。见 [进度状态](capabilities/progress.md)。 |
| **控制台刷屏过滤** | `logger.debug(msg, console_only=True)` | **仅终端控制台** | 引擎心跳与高频调试，既不入库也不进 SSE。 |
| **子进程裸输出兜底** | 域内第三方库裸 `print` 或未捕获 tracebacks | 控制台 + SSE + `hub_logs` | 内核 supervisor 异步文本泵自动截获 `stderr`，标记为 `layer: "SUBPROC"` 并统一接管。 |

### 3. 多语言使用示例

#### Python 域示例：
```python
# 1. 结构化起止配对
domain.logger.info(f"开始生成订单 {order_id}", phase="start")

# 2. 事实日志
domain.logger.info("订单数据已落库", order_id=order_id)

# 3. 临时进度条（高频更新，只走 Web SSE 原地刷新，不污染 hub_logs）
domain.logger.info("正在下载模型分片 1/2", ephemeral=True, key=f"dl_{order_id}", frac=0.50)
domain.logger.info("分片下载完成", ephemeral=True, key=f"dl_{order_id}", frac=1.00, done=True)

# 4. 显式失败终态（让 UI 区分"没跑完"和"失败了"）
domain.logger.info("模型加载失败", ephemeral=True, key=f"dl_{order_id}", state="failed")

# 5. 起止完成配对
domain.logger.info(f"订单 {order_id} 处理完成", phase="finish")
```

#### Node.js 域示例：
```javascript
// 1. 结构化起止配对
domain.logger.info(`开始扣减库存 ${sku}`, { phase: "start" });

// 2. 事实日志
domain.logger.info(`扣减成功，剩余库存: ${remaining}`);

// 3. 临时进度条
domain.logger.info("任务计算中", { ephemeral: true, key: `calc_${sku}`, frac: 0.75 });
domain.logger.info("计算完毕", { ephemeral: true, key: `calc_${sku}`, frac: 1.0, done: true });

// 4. 起止完成配对
domain.logger.info(`库存 ${sku} 扣减完毕`, { phase: "finish" });
```

---

## 六、如何触发自动化构建与发布

1. **本地一键构建发布包**：
   在仓库根目录执行：
   ```bash
   ./scripts/build_dist.sh
   ```
   构建产物自动输出至 `dist/sdk/` 与 `dist/kernel-runtime/`。

2. **GitHub Actions 的构建与发布**（[`.github/workflows/build-and-release.yml`](../.github/workflows/build-and-release.yml)）：

   | 触发 | 实际执行的 job | 产物 |
   | :--- | :--- | :--- |
   | 推送到 `main` | 只在 Linux / Windows / macOS 跑 `ci-test` | **无产物**，仅测试 |
   | 推版本标签 `v*` | `ci-test` + 构建 SDK + 构建三平台运行包 + 发布 Release | Wheel、npm 包、三平台 tar.gz，挂到 Release 页面 |
   | 手动 `workflow_dispatch` | 同标签，但不创建 Release | 上传到 Actions Artifacts |

   发布正式版本：

   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```

   注意：**普通 push 不会产出任何安装包**，需要 Artifacts 时手动触发 workflow。

# v0.1.2: unexpected process exit recovery

Each external domain accepts `auto_restart` (default true), `max_restarts` (default 10, null means unlimited), and `restart_delay_ms` (default 500). Unexpected process exit fails in-flight deliveries, revokes old routes, and schedules a fresh process generation; the restarted domain must register its handlers again. Explicit supervisor stop disables restart until explicitly started. More than five seconds of process uptime resets the consecutive restart counter. Delay is fixed, not exponential. Automatic restart restores process availability; it does not replay failed business requests or repair application defects. If spawning a replacement fails, the current implementation logs the failure without scheduling another attempt.

# Async SDK contract

- 生命周期：Python `await domain.run_async(on_start=..., on_close=...)`，Node `await domain.runAsync({onStart, onClose})`。异步 handler 在 `await` 其他域时仍能继续收帧。
- `send` / `request` / 存储方法全部是 async，需要 `await`。
- `request(target, message_type, params, data, timeout=30)`：**target 只接受单个字符串**；成功返回**整条终态回执**，中间回执不会让它 resolve。
- 终态不是 `completed` 时抛 `DomainRequestError`，带 `status` / `data` / `message_id`。除真实终态外还会出现 `unknown_message`、`busy`；Node 本地超时抛 `status="timeout"`，Python 本地超时抛 `asyncio.TimeoutError`。**SDK 没有 `connection_closed` 这个状态，也不支持 AbortSignal**。
- 本地超时或取消只是停止本地等待，**不取消内核侧的业务执行**；之后仍可用 id 再 `wait_result`。
- 帧大小上限由**内核**执行（`MAX_IPC_FRAME` = 16 MiB，域→内核方向超限会被断连并记录诊断）。**SDK 自己不设帧上限**，也不代表任意大的消息都能过。
- 阻塞式 Python 业务代码请用 `asyncio.to_thread` 跑，否则会占住该域事件循环。

# Waiting by ID

`send` 返回内核分配的 id：字符串 target 返回单个 id，列表 target 返回有序 id 列表。`wait_result` / `waitResult` 与 `wait_results` / `waitResults` 按 id 消费结果（后者只等待已存在的 id，绝不重新发送）。超时或取消的等待可以用 id 重试；未读取的结果槽有上限，也可以显式 `forget_result` 释放。

### 滚动更新重读启动配置

从文件加载的 KernelBootConfig 会携带仅供内核使用、不会写入 JSON 的来源路径。监督器启动装配时记住该路径。UpdateDomain 未提供 spec 时重新读取和校验该文件，采用目标域最新 command、args、working_dir、env 及监督配置；不会修改其他域或重开数据库。文件不可读、解析/校验失败、目标域缺失时返回错误，保留旧域运行，不静默回退旧配置。显式 spec 优先；没有文件来源的嵌入式宿主继续使用内存规格。自动重启及普通重启行为不因本项修改而改变。

域端口托管配置与SDK启动示例见 [端口绑定](capabilities/port-bindings.md)。公开端口只在启动配置的ports绑定中声明，域使用SDK分配的内部端点。
