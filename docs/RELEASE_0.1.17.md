# Atelier Kernel v0.1.17

## 非持久账本容量语义修正

- 非持久（Ephemeral）账本达到 64MB 软上限时**不再拒绝新消息**：按 `created_at` 升序淘汰最旧的 Ephemeral 记录，Durable/Volatile 一律不淘汰。
- Ephemeral 是尽力而为：终态与在途都可以被淘汰；只有“淘汰后仍不够”（账本里全是不可淘汰的 Durable/Volatile）时才返回 `nonpersistent ledger capacity exhausted`。
- 新增诊断计数 `retention_counts().nonpersistent_evicted`。

## 启动恢复语义修正

- 内核重启时**先清理上一代终态记录**：内核重启必然伴随所有业务域重启，重启后不存在需要查询上一代终态结果的调用方。
- 在途消息（`sent`/`received`/`started`）继续标记为 `execution_failed` + `kernel_restarted_with_inflight_delivery`，`kernel_outbox` 与 `kernel_sessions` 清空。
- 运行期内的终态记录保留，供同一代际的幂等回放与实时诊断使用。

## 问题背景

v0.1.16 及之前，非持久账本是一本“满了就拒收一切”的有界队列：发布域的高频状态通知把 64MB 打满后，web 域因为启动也需要 Ephemeral 注册/发现消息而无法启动，最终整个内核的 Ephemeral 通道全部瘫痪。

## 测试调整

- 删除/改写断言“重启后仍可查询上一代终态结果”的用例（该场景在架构上不存在）：
  - `tests/core_reliability.rs`
  - `tests/final_reliability_regressions.rs`
  - `tests/kernel_integration.rs`
- 全量 `cargo test` 通过。

## 兼容性

- 对外消息契约、错误码、回执结构均未改变；`sdk/python` 与 `sdk/nodejs` 行为一致，版本对齐到 0.1.17。
- 诊断输出新增 `nonpersistent_evicted` 字段，为纯增量。
