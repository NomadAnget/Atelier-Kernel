> **归档（历史记录）**：本文档记录当时的设计、验收或发布状态，**不代表当前契约**。
> 当前契约、能力与 SDK 入口见 [../INDEX.md](../INDEX.md)。

# 内核收敛重构基线（Kernel Convergence）

> 状态：**核心收敛 + C4 已落地**（M1–M8 完成：三表一入口；C4 虚拟文件命名空间——`<project_root>/<domain>/<object_id>` 平铺唯一 id、内核索引虚拟路径、Python/Node SDK 只拦 `vfs://`、`--check` 静态门禁、多进程 e2e 通过；outbox 每通道单调 seq），全量回归绿。M9 的版本 bump / tag / 发版按项目边界待明确授权后再做。
> 目标：**内部全面收敛重构，对外 SDK / handler 契约不变**。
> 验证标准：宪法（V4.0 + C4）派生出的**不变量**，而非旧测试脚本所固化的实现事故。
> 关联宪法：CONSTITUTION.md（C1 管道身份 / C2 消息结构与投递策略 / C3 四阶段回执 / **C4 存储枢纽对象索引**）、M1–M7、B1–B6。

---

## 1. 结论先行

Atelier-Kernel 的腐化不是"转发器被改复杂了"，而是**宪法的三条契约被实现成三套并行机制，且没有单一权威状态**：

- 一条消息的"当前状态"存在 **4 张表**里（`kernel_ledger` / `identities` / `kernel_sessions` / `highwater`），其中 `identities.record` 还缓存了第二份完整 JSON；
- 一条消息的"终结清理"存在 **6 条出口**里，每条只覆盖 4 张表的一部分；
- 持久性策略 `DeliveryPolicy::resolve()` 是**运行时字符串推断**，被 6 处重复调用，还内嵌了 `"web_ui.push_alert"` 这类业务字符串。

修一处冒一处，是矩阵结构决定的，不是运气。**小修小补已经无法收敛。**

---

## 2. 从宪法重新提取的公理（不是历史条款）

历史条款是事故补丁的沉积（"临时传输溢出""内核托管端口""无轮询恢复"…），不能作为重构起点。以下从 C1/C2/C3/C4 的核心语义重提：

1. **管道即身份**：域只见 `<domain>.<handler>` 契约，物理管道决定身份。
2. **一条消息 = 一行状态**：状态单调推进，终态即删（Durable 另存审计副本，不是状态副本）。
3. **持久性是消息属性，不是运行时推断**：由发送/注册时声明，不由 `target`/`message_type` 字符串表猜测。
4. **传输就是传输**：一个有序队列 + 一个每通道单调序号，与持久性解耦。
5. **大载荷不经过管道**：C4 之下，大文件是存储枢纽的一个索引对象，消息里只走引用。
6. **控制面是旁观者**：observer / logger / telemetry / 端口转发 / 滚动更新握手，只读派生，永不成为第二真相源。

---

## 3. 目标结构：三表一入口 + 对象索引

### 3.1 消息生命周期（三张表）

```
messages(id, logical_id, source, target, message_type, policy, seq,
         status, data, created_at, updated_at)
    —— 唯一权威。policy 是声明值。终态时：
        Durable → 转存 audit 后删除；
        Volatile/Ephemeral → 直接删除。

submissions(source, seq, logical_id, children)
    —— 去重索引，按 source 保留最近 N 个 seq（有界）。
    —— 替换 kernel_sessions + highwater 两张表。

audit(id, message_id, logical_id, source, target, status, data, ts)
    —— Durable 终态的只增审计副本（"留日志不留状态"）。
```

删除：`identities`、`highwater`、`kernel_sessions` 的磁盘副本、`identities.record` 全量缓存。

### 3.2 传输（一张表）

```
outbox(channel, seq, frame, state)   —— 单表，每通道单调 seq
```

Durable / Volatile / Ephemeral 的区别只在"重启后这行要不要"和"满不满丢不丢"，**不在出队顺序**。A2 跨策略乱序从结构上消失。

### 3.3 存储枢纽对象索引（C4，正交于生命周期）

```
objects(object_id, owner_domain, path, size, checksum, ref_count, created_at)
```

- 消息里的文件 = `{"object_id","size","checksum"}` 引用，字节不进管道；
- 位置由内核索引，域不寻址、不得绕过索引直连磁盘；
- 写权随域走（M3），跨域只读；
- 生命周期独立于消息：引用计数 + 显式删除 + 孤儿 GC；
- 大文件不再挤占 64MB 瞬态账本，也不再触发 16MiB 帧上限（A1 随之缓解）。

---

## 4. 三条不变量（新的测试预言）

旧测试脚本固化了实现事故（"重启后查旧结果""无绑定回执回落到当前代际"），**不再作为重构的判据**。新判据是：

| 不变量 | 断言 |
|---|---|
| **I1 唯一性** | 任一 `message_id` 在 `messages` 至多一行，在 `audit` 至多一行，且二者不同时存在 |
| **I2 单调性** | `status` 只沿 `Sent→Received→Started→终态` 推进，终态不可改写、不可回退 |
| **I3 有界性** | `submissions` 每 source ≤ N；`messages` 只含非终态；`audit` 是唯一允许无限增长的纯追加 |

对外接口红线（重构全程不得违反）：
- `<domain>.<handler>` 路由、`message_type` 集合、四阶段回执、`kernel.*` 8 大内置目标不变；
- SDK（Python / Node）公开方法、消息结构、错误码不变；
- `kernel_boot.json`、滚动更新、端口绑定语义不变。

---

## 5. 迁移步骤（每一步都是等价替换，可灰度、可回滚）

| 步骤 | 内容 | 验证 |
|---|---|---|
| M0 | 提交当前审计修复为基线（不发布） | 全量测试绿 |
| M1 | 建立宪法不变量测试预言 `tests/constitution_invariants.rs` | I1/I2/I3 通过 |
| M2 | `resolve()` 收敛为单一调用点，删除 `"web_ui.push_alert"` 等业务字符串；策略改为发送时声明 | 策略判定只在一处 |
| M3 | 删除 `identities.record` 全量缓存，状态只从 `messages` 读 | 无第二真相源 |
| M4 | 合并 `highwater` + `kernel_sessions` 磁盘副本 → `submissions` 单表 | 有界 + 去重语义不变 |
| M5 | 六个终态出口收敛为一个 `commit_terminal()` | 清理只在一处 |
| M6 | 传输层加每通道单调 seq，收掉三层出队顺序问题 | 跨策略 FIFO |
| M7 | Durable 终态转存 `audit`，与状态解耦 | I1 唯一性成立 |
| M8 | C4 存储枢纽对象索引：协议 + SDK + 内核 | 大文件不进管道 |
| M9 | 全量回归 + 版本发版 | 对外契约不变 |

> **落地状态**：M1–M8 已完成并提交——`0d5dfde`（四表六出口 → 三表一入口，nucleus 派生）、`f66995d`（C4 内核：`kernel.storage` 对象接口 + `objects` 表持久化，传输机制=内核复制域本地文件）、`061db2c`（C4 SDK：Python/Node 对象接口）、`outbox` 每通道单调 seq（跨策略 FIFO 由 `cross_policy_business_fifo_is_preserved` 证明）。全量 `cargo test` 绿；M9 的版本 bump / tag / 发版按项目边界「不主动发版」待明确授权。

---

## 6. 风险与边界

1. **`identities` 承载的"通道绑定"不可丢**：M3 删除记录缓存时，`accepts_channel` 的物理代际校验必须保留到别处（否则回执串代际，见 B1 审计项）。
2. **Durable 重放依赖磁盘去重索引**：M4 合并表时，磁盘 `kernel_sessions` 的去重/重放能力要等价迁移到 `submissions`，不能只删不搬。
3. **C4 是协议新增**：新增 `object_id` 到 `data` 引用 + `kernel.storage` 对象接口，属于对外接口增量——需同步 Python / Node 双端 SDK（B5）。
4. **旧测试的处理**：不删除，但标记为"实现回归网"；重构期只以 I1/I2/I3 为通过标准，旧测试作为辅助信号，冲突时以宪法为准。

---

## 7. C4 决策（已定案）

1. `LARGE_OBJECT_THRESHOLD` 默认 1 MiB，与"域显式声明对象传输"两种触发方式并存。
2. 对象生命周期：**无 TTL**；引用计数 + 显式删除，删除时机由业务域决定；域退出后的孤儿对象由内核 GC 回收。
3. 跨域对象读取：**只读共享，不可写**；写入/删除仅限本域对象，与 `store_<domain>` 同一写权随域走原则（M3）。
4. `checksum`：由内核计算并随索引返回，域仅校验。
5. 对象目录：**不设硬性容量上限**，仅诊断计量；不因配额拒收。
