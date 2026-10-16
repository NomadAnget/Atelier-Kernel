# Atelier Kernel 发布包与产品变体（Variant）开发指南

本文档面向**变体组装者**与**业务域开发者**，说明如何直接使用 **Atelier Kernel 预编译运行包** 与 **多语言 Domain SDK 安装包** 构建新的产品变体，**全程无需安装 Rust 编译环境，也无需修改内核源码**。

---

## 一、发布包组成结构

从 GitHub Actions Artifacts / Releases（或本地运行 `./scripts/build_dist.sh` 后的 `dist/` 目录）获取的发布包包含两大核心部分：

```text
atelier-kernel-runtime-<os>-<arch>/
├── atelier-kernel                        # 1. 预编译 Rust 微内核可执行程序（Windows 下为 atelier-kernel.exe）
├── kernel_boot.template.json             # 2. 默认多域启动配置模板
├── VARIANT_GUIDE.md                      # 3. 本使用说明文档
└── sdk/
    ├── atelier_sdk-0.1.0-py3-none-any.whl # 4. Python 域官方 SDK 安装包 (Wheel)
    └── atelier-sdk-0.1.0.tgz              # 5. Node.js / TypeScript 域官方 SDK 安装包 (NPM Tarball)
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
   pip install ./sdk/atelier_sdk-0.1.0-py3-none-any.whl
   ```

2. **编写业务域代码（例如 `domains/billing_domain.py`）**：
   只需导入 `AtelierDomain`，用 `@domain.handler(...)` 注册处理器，无需关心底层 `stdin/stdout` 管道与四阶段回执细节：
   ```python
   #!/usr/bin/env python3
   from atelier_sdk import AtelierDomain, DomainExecutionFailed

   domain = AtelierDomain()

   @domain.handler("charge")
   def on_charge(msg):
       order_id = msg.data.get("order_id", "ORD-001")
       amount = msg.data.get("amount", 0)
       if amount <= 0:
           # 抛出 DomainExecutionFailed，SDK 自动向内核汇报 ExecutionFailed 回执
           raise DomainExecutionFailed({"error": "invalid_amount", "amount": amount})

       # 1. 存储即消息：写入本域专属表 store_billing（写操作严格物理隔离）
       domain.storage_insert(
           record_id=order_id,
           category="invoice",
           data={"order_id": order_id, "amount": amount, "from": msg.source_domain},
       )

       # 2. 跨域读取：读取其他域（如 order 域）的表数据（只读共享）
       domain.storage_query(domain="order", record_id=order_id)

       # 3. 跨域消息发送：调用其他域的处理器
       domain.send(
           targets=["web_ui.push_alert"],
           message_type="BillingSuccess",
           params={"severity": "info"},
           data={"text": f"订单 {order_id} 扣款成功: ¥{amount}"},
       )

       # 4. 直接 return 字典，SDK 自动汇报 Completed 回执
       return {"ok": True, "order_id": order_id, "charged": amount}

   if __name__ == "__main__":
       domain.run()
   ```

---

#### 方式 B：开发 Node.js / TypeScript 业务域

1. **安装 Node.js NPM 包（自带 TypeScript `.d.ts` 类型声明）**：
   ```bash
   npm install ./sdk/atelier-sdk-0.1.0.tgz
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
  "web_ui_bind": "127.0.0.1:8787",
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
# 模式 1：带实时观察台与测试网页前端域（默认监听 http://127.0.0.1:8787）
./atelier-kernel --config ./my-product-variant/kernel_boot.json

# 模式 2：纯后台微内核生产模式（不启动测试网页，仅托管与路由各业务域子进程）
./atelier-kernel --config ./my-product-variant/kernel_boot.json --headless
```

---

## 三、内核命令行参数一览 (`atelier-kernel --help`)

| 参数 | 说明 |
| :--- | :--- |
| `--config <path>` | 指定变体启动配置文件路径（默认：`./kernel_boot.json`） |
| `--headless` | 纯微内核无头运行模式（仅托管子进程域与路由存储，不启动 Web 调试前端） |
| `--init <dir>` | 在目标目录一键生成新变体配置模板与示例域脚手架 |
| `--cli` | 启动终端交互式 REPL 测试前端域 |
| `--auto` / `-a` | 运行内置 7 项内核契约与幂等自动化自检套件并退出 |
| `--version` / `-v` | 查看内核版本号 |
| `--help` / `-h` | 查看命令行帮助 |

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

### 1. 三大出口（Three Sinks）

- **控制台（Console）**：紧凑单行格式 `HH:MM:SS LEVEL [layer/component] message <phase>`，TTY 环境色彩高亮。
- **实时订阅流（Subscriber Stream / SSE）**：基于事件流扇出至 Web 前端与观察者（`/api/logs/stream`），慢订阅丢帧绝不阻塞业务。
- **内核直管持久化表（SQLite `hub_logs`）**：由内核独立后台写线程按 WAL 批处理异步落库，队列饱和丢帧，黑匣子绝不反噬被观察者。

### 2. 多状态日志形态（Multi-State Log Patterns）

| 日志形态 | API | 去向 | 特性与用途 |
| :--- | :--- | :--- | :--- |
| **分级日志** | `domain.logger.info(msg, layer=..., component=...)` | 控制台 + SSE + `hub_logs` | 记录事实与业务状态，支持 `DEBUG`、`INFO`、`WARNING`、`ERROR`、`CRITICAL`。 |
| **生命周期起止配对** | `domain.logger.phase(level, "start" \| "finish", msg)` | 控制台 + SSE + `hub_logs` | 结构化字段 `phase` 原生配对同一操作起止，无需靠正则表达式猜测文本前缀。 |
| **临时实时进度** | `domain.logger.progress(msg, key=..., frac=..., waiting=..., done=...)` | **仅 SSE 实时流** | 前端按 `key` 原地更新进度条，`frac`∈[0,1] 渲确定进度条，`waiting=True` 渲等待脉冲，`done=True` 清除。**不落 `hub_logs` 磁盘库**，防止高频刷屏膨胀数据库！ |
| **控制台刷屏过滤** | `domain.logger.debug(msg, console_only=True)` | **仅终端控制台** | 引擎心跳与高频调试，既不入库也不进 SSE。 |
| **子进程裸输出兜底** | 域内第三方库裸 `print` 或未捕获 tracebacks | 控制台 + SSE + `hub_logs` | 内核 supervisor 异步文本泵自动截获 `stderr`，自动标记为 `layer: "SUBPROC"` 并统一接管。 |

### 3. 多语言使用示例

#### Python 域示例：
```python
# 1. 结构化起止配对
domain.logger.phase("INFO", "start", f"正在生成订单 {order_id}")

# 2. 事实日志
domain.logger.info("订单 %s 数据已落库", order_id)

# 3. 临时进度条（高频更新，只走 Web SSE 原地刷新，不污染 hub_logs）
domain.logger.progress("正在下载模型分片 1/2", key=f"dl_{order_id}", frac=0.50)
domain.logger.progress("分片下载完成", key=f"dl_{order_id}", frac=1.00, done=True)

# 4. 起止完成配对
domain.logger.phase("INFO", "finish", f"订单 {order_id} 处理完成")
```

#### Node.js 域示例：
```javascript
// 1. 结构化起止配对
domain.logger.phase("INFO", "start", `正在扣减库存 ${sku}`);

// 2. 事实日志
domain.logger.info(`扣减成功，剩余库存: ${remaining}`);

// 3. 临时进度条
domain.logger.progress("任务计算中", { key: `calc_${sku}`, frac: 0.75 });
domain.logger.progress("计算完毕", { key: `calc_${sku}`, frac: 1.0, done: true });

// 4. 起止完成配对
domain.logger.phase("INFO", "finish", `库存 ${sku} 扣减完毕`);
```

---

## 六、如何触发自动化构建与发布

1. **本地一键构建发布包**：
   在仓库根目录执行：
   ```bash
   ./scripts/build_dist.sh
   ```
   构建产物自动输出至 `dist/sdk/` 与 `dist/kernel-runtime/`。

2. **GitHub Actions 自动构建与 Release 发布**：
   - 每次推送代码到 `main` 分支：GitHub Actions（[`.github/workflows/build-and-release.yml`](./.github/workflows/build-and-release.yml)）自动构建 Python `.whl`、Node.js `.tgz` 以及 Linux / Windows / macOS 三端预编译运行包，并上传至 **Actions -> Artifacts**。
   - 推送版本标签时自动发布正式 Release：
     ```bash
     git tag v0.1.0
     git push origin v0.1.0
     ```
     GitHub Actions 将自动创建 Release 页面并挂载全部平台运行包与 SDK 安装包。

# v0.1.2: unexpected process exit recovery

Each external domain accepts `auto_restart` (default true), `max_restarts` (default 10, null means unlimited), and `restart_delay_ms` (default 500). Unexpected process exit fails in-flight deliveries, revokes old routes, and schedules a fresh process generation; the restarted domain must register its handlers again. Explicit supervisor stop disables restart until explicitly started. More than five seconds of process uptime resets the consecutive restart counter. Delay is fixed, not exponential. Automatic restart restores process availability; it does not replay failed business requests or repair application defects. If spawning a replacement fails, the current implementation logs the failure without scheduling another attempt.

