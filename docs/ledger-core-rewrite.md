# 账本核心换骨对照表（ledger-core rewrite）

> 目的：把 `src/ledger.rs` 从"四表六出口"一次性换成"三表一入口"，公共方法签名不变。
> 验收：`tests/constitution_invariants.rs` 的 I1/I2/I3，以及全量 `cargo test`。
> 这是换骨地图，不是补丁清单。执行时按 §5 顺序一次完成核心重写，不做逐列迁移。

---

## 1. 对外契约（不可变，29 个方法）

路由器 / outbox / 测试只通过这些方法与账本交互，重写后签名与语义不变：

| 方法 | 语义 | 归属新结构 |
|---|---|---|
| `new()` / `open_with_db(path)` | 建库/恢复 | 建 messages/submissions/audit 三表 |
| `database_path()` | 磁盘路径 | 不变 |
| `message_gate(id)` | 每消息串行锁 | 不变 |
| `generate_message_id()` | 生成 id | 不变 |
| `allocate_submission` / `try_allocate_submission` | 分配去重/子 id | 读写 `submissions` |
| `logical_id_for_message(id)` | id→logical | 读 `messages.logical_id`（活）或 `audit` |
| `bind_origin(ids, instance)` / `source_instance(id)` | 代际绑定 | `messages.origin`（活行） |
| `accepts_channel(id, channel)` | 回执管道校验 | `messages.channel`（活行） |
| `has_unpersisted_failure(id)` | 未持久化失败标记 | `messages.persistence_failed`（活行） |
| `finish_publication(receipt)` | 回执发布后回收 | 调用 `commit_terminal` |
| `clear_session_dedup_for_domain(source)` | 代际退役清理 | 批量调 `commit_terminal` + 删 submissions |
| `retention_counts()` | 诊断快照 | 统计三表 |
| `record_initial_target_state[_locked]` | 入口写状态 | 写 `messages`；终态→`commit_terminal` |
| `record_delivery(msg, seq, channel)` | 出站绑定 | 写 `messages` + `channel` |
| `advance_status[_locked]` | 回执推进 | 读改 `messages`；终态→`commit_terminal` |
| `get_record(id, target)` / `get_persisted_record(id)` | 读结果 | 读 `messages`(活) 或 `audit`(历史) |
| `persisted_count()` | 磁盘记录数 | `audit` 行数 |
| `receipts_for_message(id)` / `receipts_for_logical(logical)` | 回执重放 | 读 `messages`/`audit` |
| `fail_inflight_for_domain` / `fail_inflight_for_channel` | 死域收敛 | 批量：改状态→`commit_terminal` |

---

## 2. 三张表（唯一 schema，内存/磁盘同一份）

```sql
CREATE TABLE messages(
  message_id TEXT PRIMARY KEY,
  logical_id TEXT NOT NULL,
  source_domain TEXT NOT NULL,      -- 纯域 id（"sender"），不是去重键
  seq INTEGER NOT NULL,
  target TEXT NOT NULL,
  message_type TEXT NOT NULL,
  policy TEXT NOT NULL,             -- 'durable'|'volatile'|'ephemeral'
  status TEXT NOT NULL,
  data TEXT,
  origin INTEGER NOT NULL DEFAULT 0,
  channel TEXT,
  persistence_failed INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE submissions(
  dedup_key TEXT NOT NULL,          -- "{domain}#{generation}"，纯去重键
  seq INTEGER NOT NULL,
  logical_id TEXT NOT NULL,
  children TEXT NOT NULL,           -- JSON 数组
  PRIMARY KEY(dedup_key, seq)
);

CREATE TABLE audit(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT NOT NULL,
  logical_id TEXT NOT NULL,
  source_domain TEXT NOT NULL,
  target TEXT NOT NULL,
  status TEXT NOT NULL,
  data TEXT,
  created_at INTEGER NOT NULL
);
```

- **`messages` 是唯一权威**：一条消息恰好一行，活消息常驻（Durable 落在磁盘 messages，Volatile/Ephemeral 落在内存 messages —— 同一 schema，只是连接不同）。
- **`submissions` 有界**：每个 `dedup_key` 只保留最近 `SESSION_REPLAY_WINDOW` 个 seq，替换 `kernel_sessions`(两份) + `identities`。
- **`submission_high` 保留高水位语义（实测确认）**：`highwater` 是单调高水位标记，会话行被终态清理后仍须记住曾到过的最高 seq，否则重放已完成的老 seq 会被误判成新提交而重复执行。它**不能**从 `max(seq)` 推导——`submissions` 只保留有界窗口，窗口外的会话行已删，`max(seq)` 会回落。目标结构里它是独立单列表（或每-key 单行），不是可有可无的缓存。
- **`audit` 只增**：Durable 终态结果追加，跨重启可查，永不删除（"留日志不留状态"）。

删除的旧表：`identities`、`highwater`、`kernel_sessions`（含磁盘副本）、`identities.record` 缓存。

---

## 3. 单一入口 `commit_terminal`

```rust
fn commit_terminal(&self, r: &DeliveryRecord) -> ReceiptTransition::Advanced 等
```

这是**唯一**负责"终态该发生什么"的地方，行为固定为四条，顺序不可变：

1. 若 `policy == Durable`：`INSERT INTO audit(...)` 追加终态结果；
2. `DELETE FROM messages WHERE message_id = ?`；
3. 若该 logical 已无活消息：`DELETE FROM submissions WHERE logical_id = ?`（回收去重索引）；
4. 返回 `r.to_receipt()`（回执携带完整身份与结果，不依赖已删除的行）。

**不变量**（I1/I2/I3）由此直接推出：
- 唯一性：终态行只在 `audit`，活行只在 `messages`，二者不同时存在；
- 单调性：`messages.status` 只沿四阶段推进，终态经 `commit_terminal` 一次性离开 messages；
- 有界性：`messages` 只含非终态，`submissions` 每 key ≤ N，`audit` 是唯一无限增长的纯追加。

---

## 4. 六个出口 → 全部坍缩为 `commit_terminal`

| # | 旧出口（当前位置） | 现状 | 换骨后 |
|---|---|---|---|
| 1 | `commit_transition`（回执推进到终态） | 手写 DELETE + 删 outbox 帧 | 调 `commit_terminal` |
| 2 | `record_initial`（入口即终态） | 非持久终态不落库 | 调 `commit_terminal` |
| 3 | 配额拒收（`need>0`） | `remember_failure` 自锁 | 不发回执、不落库，直接返回失败 |
| 4 | 配额淘汰（eviction 循环） | 手删账本行+身份+会话 | 只删 `messages` 活行（淘汰即终态，走 `commit_terminal` 的"非持久不审计"分支） |
| 5 | `open_with_db` 启动恢复 | 手写 UPDATE/DELETE 四表 | 单事务：旧非终态→`commit_terminal`(失败)，旧终态→转 `audit` |
| 6 | `clear_session_dedup_for_domain` + `finish_publication` | 手删身份/会话/高水位 | 批量 `commit_terminal` + 删 `submissions` |

清理逻辑从"六份各自为政的 SQL"变成"一个函数 + 一条不变量"。

---

## 5. 换骨顺序（已更新：不是"重写 ledger.rs"，是"路由器级重接 + 删旧四表"）

> **已完成**（`0d5dfde`）：`src/ledger.rs` 已改写为 nucleus 门面——`Nucleus` 持有活跃/审计/提交去重唯一权威，SQLite 只镜像 Durable 子集为 `messages`（活）+ `audit`（终态，只增）；`identities` / `highwater` / `kernel_sessions` / 旧 `kernel_ledger` 四表删除。下列 §1–§4 的三表设计即该实现的出处。

> 上面 §1–§4 的三表设计，已由 `src/nucleus.rs` + `src/transport.rs` + `src/object_hub.rs` + `src/observer_proj.rs` + `src/routing.rs` 独立实现并验证。换骨 = 把这些模块接进 router/outbox/supervisor，删掉旧四表。

1. **路由器入口** `handle_outbound_message_inner`：`allocate_submission + bind_origin + record_initial(逐 target)` 改为一次 `nucleus.admit(...)` + `routing.bind` + `transport.enqueue_delivers`；
2. **路由器回执** `handle_inbound_receipt_inner`：改为 `nucleus.advance` + `transport.enqueue_receipt` + `routing.accepts` 校验；
3. **代际退役**：`mark_domain_faulted` / `clear_session_dedup` 改为 `nucleus.retire_source` + `routing.retire`；
4. **outbox**：删旧 SQLite spool + 内存 spool，用 `transport`；
5. **删除** `ledger.rs` 四表 SQL、`identities`/`highwater`/`kernel_sessions`；
6. 全量 `cargo test` + `tests/constitution_invariants.rs` + `tests/kernel_assembly.rs` 验收。

### 关键断点：核的粒度（换骨前必须想清）

旧 `KernelLedger` 的公共接口有一个与核不兼容的分裂：

```
allocate_submission(source, seq, count) → (logical_id, [ids], replayed)   // 分配身份，无 target
record_initial_target_state(id, seq, source, target, kind, status, data)   // 每 target 记录状态
```

核的 `admit` 把"分配身份 + 建活跃行"合并成一次。**这个"先分配、再逐 target 记录"的分裂本身是四表结构的产物**（id 与状态曾分属 `identities` 与 `kernel_ledger`），无法无损映射到 `admit`。

正确解法（二选一）：
- **A**：核保持提交级 `admit`，路由器入口重构成"先校验全部 target，再一次 admit，再对入口即终态的 target 逐条 advance"；
- **B**：核下沉到**消息级** `put` + `advance`（一条消息一行），提交身份/去重/多 target 变成核外的 `SubmissionCache` 推导——这样旧 API 的 `allocate`（→ SubmissionCache）与 `record`（→ put）自然对应。

**B 更符合"极简核"**：核只懂"一条消息的状态机"，提交（多 target、去重、重放）是派生。换骨时优先按 B 重构路由器入口。

---

## 6. 已知必须保留的语义（不可因收敛而丢）

- **Durable 重放**：`receipts_for_*` 在终态后仍能从 `audit` 读回结果；
- **磁盘无关的故障收敛（已定案 §7=A）**：放弃磁盘无关兜底——磁盘坏时死域 Durable 在途消息留在 `execution_outcome: unknown`，不强行收敛，不维护常驻第二真相源；
- **跨代际隔离**：`origin`/`channel` 绑定随 `messages` 活行，代际退役即清；
- **`source` 去重键 ≠ `source_domain` 域 id**：这是上次 M3 踩的坑，新结构里 `submissions.dedup_key` 与 `messages.source_domain` 显式分列，不再混用。

## 7. 磁盘无关兜底（已定案 A）

`fail_inflight_for_channel` 的旧注释要求"即使磁盘不可用也能关死代际"，旧结构靠 `identities.record` 内存缓存兜底。**已定案 A**：放弃磁盘无关兜底——磁盘坏时死域 Durable 在途消息留在 `execution_outcome: unknown`，不强行收敛。依据宪法 C3:48"无法持久化的失败反馈仅为当前进程中的明确失败，不伪称其本身已有跨重启持久保证"；不为极端故障维护常驻第二真相源。

我建议 **A**：宪法 C3:48 已经写死"无法持久化的失败反馈仅为当前进程中的明确失败，不伪称其本身已有跨重启持久保证"，磁盘坏属于极端场景，不该为它维护常驻第二真相源。
