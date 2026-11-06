# AGENTS.md — 给 AI 编程代理的工作约定

Atelier Kernel 的主要使用者就是 AI 编程代理。这份文件只回答两件事：**怎么用它对、怎么改它不破坏契约**。完整契约不在这里复制，见 [docs/INDEX.md](docs/INDEX.md)。

---

## 0. 先读什么

按这个顺序，**读到够用就停**：

1. [README.md](README.md) —— 内核做什么 + 一句话最高准则
2. [docs/INDEX.md](docs/INDEX.md) —— 找到你要的那一页
3. 只在涉及契约/边界时读 [docs/CONSTITUTION.md](docs/CONSTITUTION.md)

> **不要通读别人的域代码。** 内核的设计目标就是让你不必这么做（见 §2 第 1 条）。你要的信息在消息契约里，不在别的域的实现里。

---

## 1. 先分清任务类型

| 类型 | 典型请求 | 去哪一节 |
| :--- | :--- | :--- |
| **A. 用内核搭业务** | "加一个订单域"、"让 A 域调用 B 域"、"存个文件" | §3 |
| **B. 改内核本身** | "加一项 SDK 能力"、"改路由/账本/监督逻辑" | §4 |

分不清就先按 A 处理；A 不需要动 `src/`。

---

## 2. 硬规矩（违反会静默坏掉，不是代码风格问题）

| # | 规则 | 为什么 | 违反了会怎样 |
| :--- | :--- | :--- | :--- |
| 1 | **域之间只走内核消息**。禁止跨域相对 `import`/`require`、共享文件、`sys.path` 拼接 | 宪法 M4 + B2 认知隔离 | `--check` 报 M4 违规；CI 红 |
| 2 | **不读不写 `ATELIER_IPC_*`**。不要在 `env` 里设，不要在代码里读 | C1 + M5，端点是身份来源 | 启动直接拒绝；或身份伪造 |
| 3 | **不直接打开内核 SQLite**（`data/*.db`） | M4，账本是内核私有状态 | `--check` 报 M4 违规；可能损坏账本 |
| 4 | **域间禁止发布订阅**。只有 `kernel.*` 的流可以订阅 | C2.3 | 架构违规，评审会退回 |
| 5 | `domain_id` 只能是 ASCII 字母/数字/`_` 且全局唯一；handler 名**不含 `.`** | M1 | 启动校验失败 |
| 6 | **handler 必须快速返回**，长任务丢后台任务/队列 | B6：一个域只有一个事件循环 | 该域事件循环被占住，所有消息排队 |
| 7 | **显式失败**，不要 try/except 后返回 `{"ok": true}` | B3 | 故障被掩盖，回执说谎 |
| 8 | **用 SDK 的 15 项能力**，不手搓 JSON 帧 | 协议细节会变，SDK 是唯一入口 | 升级即坏 |

---

## 3. 用内核搭业务（任务 A）

### 3.1 三步

```bash
# 1) 生成骨架（已存在则不覆盖）
atelier-kernel --init ./my-variant

# 2) 写域脚本 + 在 kernel_boot.json 里登记

# 3) 验证（必须两步都过）
atelier-kernel --check ./my-variant/kernel_boot.json
atelier-kernel --config ./my-variant/kernel_boot.json --smoke-test
```

### 3.2 `kernel_boot.json`

```jsonc
{
  "db_path": "./data/atelier_kernel.db",   // 默认值
  "project_root": "./data",                // C4 文件根：<project_root>/<domain>/
  "web_ui_bind": "0.0.0.0:8787",
  "web_ui_enabled": false,
  "domains": [
    {
      "domain_id": "order",                 // M1：字母数字下划线，全局唯一
      "command": "python3",                 // Windows 会自动映射到 python
      "args": ["-u", "domains/order.py"],
      "working_dir": null,
      "env": { "MY_FLAG": "1" },            // 不得含 ATELIER_IPC_*（大小写都算）
      "ports": [{ "name": "api", "listen": "127.0.0.1:8080" }],
      "auto_restart": true,                 // 默认 true
      "restart_delay_ms": 500               // 默认 500；max_restarts 默认 10
    }
  ]
}
```

配置是 `deny_unknown_fields`：**多写一个字段就启动失败**（M6）。

### 3.3 域脚本

Python（`sdk/python`）：

```python
import asyncio
from atelier_sdk import AtelierDomain

domain = AtelierDomain("order")          # 必须等于 domain_id

@domain.handler("create")                # 处理器名不要带域前缀
async def on_create(msg):
    await domain.storage_insert(msg.data["id"], "sales_order", msg.data)
    return {"ok": True}

if __name__ == "__main__":
    asyncio.run(domain.run_async())
```

Node.js（`sdk/nodejs`）：

```js
const { AtelierDomain } = require("atelier-sdk");
const domain = new AtelierDomain("order");

domain.handler("create", async (msg) => {
  await domain.storageInsert(msg.data.id, "sales_order", msg.data);
  return { ok: true };
});

domain.run();
```

能力清单见 [docs/sdk/python.md](docs/sdk/python.md) / [docs/sdk/nodejs.md](docs/sdk/nodejs.md)。

### 3.4 域之间怎么协作

```python
# 只发不收：拿到子消息 id 列表（send 也是 async）
ids = await domain.send(["billing.charge"], "Charge", {"amount": 10}, {"order_id": "o-1"})

# 要结果：等终态回执，返回的是那条回执的 data；非 completed 抛 DomainRequestError
data = await domain.request(["billing.charge"], "Charge", {"amount": 10}, {"order_id": "o-1"})
```

`send` / `request` 的目标是 `"<domain>.<handler>"`。**不要**为了让两个域"更快地"协作而共享文件或直接调用——那会同时破坏 M4 和可观测性。

**订阅必须显式调用。** `on_log` / `onLog` 之类只是登记一个本地回调，**不会**向内核订阅；不调 `subscribe_logs()` 就永远收不到事件：

```python
domain.on_notify(lambda payload: print(payload))   # 只是登记回调
await domain.subscribe_notify()                    # 真正向内核订阅
# ... 用完要 await domain.unsubscribe_notify()
```

### 3.5 C4 文件对象

```python
domain.enable_file_interception()
with open("vfs://out/clip.mp4", "wb") as f:
    f.write(payload)
```

`vfs://` 是虚拟路径，物理落盘是 `<project_root>/<domain>/<object_id>` 的平铺文件。跨域传文件时把文件对象放进消息 `data`，SDK 自动编码为 `{"__atelier_file__": "<object_id>"}`。

---

## 4. 改内核本身（任务 B）

先读 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) 的模块映射，再看这几条不可越过的边界：

1. **`src/core/` 不得引用 `src/services/`**。`tests/cases/unit/discipline.rs` 会红。
2. **终态历史只在 SQLite**（`audit` 表），核内不建全量内存索引，启动不载入历史（C3:54）。要加"记住某些终态"的逻辑，先想清楚它是不是在把历史搬回内存。
3. **先盘后存**：DB 提交成功后才改内存状态；失败必须显式（`mark_unpersisted_failure`）。
4. **改契约要三方同步**：`docs/CONSTITUTION.md` + `sdk/python` + `sdk/nodejs`（B5）。只改一端会导致跨语言行为不一致，这是硬性要求。
5. **改传输/端点**时注意：`EndpointStream::read` 是阻塞语义，`EndpointReader::read` 是无数据返回 `WouldBlock` 的轮询语义，别混用。

### 测试结构（不要新增 `tests/*.rs`）

```
tests/unit.rs        底层：不变式、消息身份、保留、顺序、分层纪律
tests/transport.rs   端点、跨语言打开方式、出口背压、大响应
tests/lifecycle.rs   注册、四阶段、停止/重启/更新、环境、端口、诊断
tests/reliability.rs 崩溃窗口、原子提交、恢复、对抗
tests/logging.rs     日志游标分页、持久缓冲
tests/c4.rs          C4 对象与跨域文件
tests/sdk_e2e.rs     两语言 15 能力全链路
```

加用例就加到对应的 `tests/cases/<层>/<主题>.rs` 里。新增顶层 `tests/*.rs` 会让 CI 多一个测试二进制，属于破坏既有结构。

---

## 5. 验证（Definition of Done）

任何改动在交付前跑完：

```bash
cargo test                                                   # 全量
cargo build --release
./target/release/atelier-kernel --check ./kernel_boot.json    # 机器宪法 M1–M7
./target/release/atelier-kernel --config ./my-variant/kernel_boot.json --smoke-test
```

| 场景 | 必须过 |
| :--- | :--- |
| 只加业务域 | `--check` + `--smoke-test` |
| 改 `src/` | 上面四条全跑；`cargo test` 必须 0 failed、0 warning |
| 改契约 / SDK | 再加：两语言 SDK 单测（`sdk/python/tests`、`sdk/nodejs/tests`）与 `tests/sdk_e2e.rs` |

**`--smoke-test` 的适用条件**（不满足会超时失败，不是内核 bug）：

- 必须配 `--config`，且 `domains` 非空；
- **每个已配置域都必须在 5 秒内启动并注册至少一个 handler**（否则报 `Timed out waiting for domains to register handlers`）；
- 它会额外挂一个 `smoke_verifier` 探针域，向每个域的第一个路由发 `VariantSmokeProbe`，要求拿到 Completed 终态，然后核对 `hub_logs`；
- 需要域侧能连上端点（`PYTHONPATH` / `node_modules` 里要有对应 SDK）。

Linux/macOS 的行为由 CI 覆盖（`cargo check --tests --target x86_64-unknown-linux-gnu` 在本机需要交叉 `cc`，通常只在 CI 跑）。

---

## 6. 常见错误

| 错误 | 症状 | 正确做法 |
| :--- | :--- | :--- |
| Unix 上用 `open(path)` 打开端点 | `ENXIO`/`ENODEV` | 用 SDK；自写客户端用 `socket(AF_UNIX).connect()` |
| 在 `env` 里写 `ATELIER_IPC_IN` | 启动被拒 | 端点由内核注入 |
| handler 名写成 `"order.create"` | 注册失败或被当成非法 | 只写 `"create"` |
| 直接 `sqlite3.connect("data/atelier_kernel.db")` | M4 违规 | `storage_query` / `storage_insert` |
| handler 里 `await` 一个几十秒的操作 | 该域事件循环被占住，后续消息全排队 | 起后台任务，先回 `{"accepted": true}` |
| 为了"复用"从另一个域 `import` 工具函数 | M4 违规 | 复制到本域，或让那个域暴露 handler |
| 用 `sleep` 等回执 | 结果不稳 | `await domain.request(...)` |
| 新增 `tests/foo.rs` | 测试目标数量变了 | 加到 `tests/cases/` 现有文件 |

---

## 7. 提交与文档

- 提交信息用 `type(scope): 说明`（`feat` / `fix` / `perf` / `refactor` / `test` / `docs`）。
- **文档只有一份事实来源**：根目录只有 `README.md`；契约在 `docs/CONSTITUTION.md`；能力在 `docs/capabilities/`；SDK 在 `docs/sdk/`。不要新建平行的说明文件，改动后同步更新对应文档。
- 历史设计与验收记录放 `docs/archive/`，不要放在 `docs/` 顶层。
