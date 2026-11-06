> **归档（历史记录）**：本文档记录当时的设计、验收或发布状态，**不代表当前契约**。
> 当前契约、能力与 SDK 入口见 [../INDEX.md](../INDEX.md)。

# Atelier Kernel v0.1.18

## 非持久账本：Volatile 终态也清理

- `Volatile`（内核基础设施：`kernel.registry` / `kernel.storage` / `kernel.logger` / `kernel.notify` / `web_ui.push_alert`）此前只写“tracked in-memory”，**没有终态清理路径**，会永久堆在内存账本里。
- 现在 `commit_transition` 的非 Durable 分支统一为：**终态即从内存账本删除**（Ephemeral 与 Volatile 一致）。
- 依据：这两类消息都不落盘、不承担审计、也不提供事后结果查询；唯一的读取方是同一代际内的幂等回执，与是否保留终态行无关。

## 容量淘汰（v0.1.17 已含）

- 非持久账本达到 64MB 软上限时按 `created_at` 升序淘汰最旧的 Ephemeral 记录（含在途），Durable/Volatile 不参与淘汰。
- 只有淘汰后仍不够时才返回 `nonpersistent ledger capacity exhausted`。
- 诊断计数 `retention_counts().nonpersistent_evicted` 可观测。

## 启动恢复（v0.1.17 已含）

- 内核重启时先清理上一代终态记录（内核重启必然伴随所有域重启，重启后没有读取者），在途消息标记为 `execution_failed` + `kernel_restarted_with_inflight_delivery`。
- `kernel_outbox`、`kernel_sessions` 清空。

## 测试调整

- `mixed_and_non_ephemeral_submissions_keep_original_identity_and_replay`：不再断言 Volatile 终态可查。
- `test_delivery_persistence_message_types_durable_volatile_ephemeral`、`test_concurrent_multidomain_stress`：改为断言 Volatile 终态已从内存清理。
- 删除 `test_single_domain_blocking_head_of_line_isolation`（其断言依赖 Volatile 终态常驻，语义已不存在）。
- 全量 `cargo test` 通过。

## 兼容性

- 对外消息契约、错误码、回执结构未变；`sdk/python` 与 `sdk/nodejs` 版本对齐 0.1.18。
