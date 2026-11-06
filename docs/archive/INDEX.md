# 归档索引

这里保留**当时的设计、验收与发版记录**。它们不作为当前契约或能力保证，仅用于追溯"为什么变成现在这样"。

当前契约与能力入口见 [docs/INDEX.md](../INDEX.md)。

## 发版说明

| 文档 | 内容 |
| :--- | :--- |
| [RELEASE_NOTES_v0.1.8.md](RELEASE_NOTES_v0.1.8.md) | v0.1.8 发版说明 |
| [RELEASE_0.1.13.md](RELEASE_0.1.13.md) – [RELEASE_0.1.18.md](RELEASE_0.1.18.md) | 0.1.13 – 0.1.18 逐版记录 |
| [RELEASE_NOTES_v0.2.0.md](RELEASE_NOTES_v0.2.0.md) / [RELEASE_0.2.0.md](RELEASE_0.2.0.md) | v0.2.0：传输改为命名端点、SDK 收敛为单层 15 能力 |

## 可靠性审查与验收

| 文档 | 内容 |
| :--- | :--- |
| [ADVERSARIAL_AUDIT.md](ADVERSARIAL_AUDIT.md) | 对抗审计发现与修复进展 |
| [FINAL_RELIABILITY_REVIEW.md](FINAL_RELIABILITY_REVIEW.md) | 核心可靠性最终复审（R1–R6） |
| [CORE_RELIABILITY_REIMPLEMENTATION.md](CORE_RELIABILITY_REIMPLEMENTATION.md) | 重实现目标与执行清单 |
| [CORE_RELIABILITY_FIX_PLAN.md](CORE_RELIABILITY_FIX_PLAN.md) | 复审问题修复计划 |
| [CORE_RELIABILITY_ACCEPTANCE.md](CORE_RELIABILITY_ACCEPTANCE.md) | 重实现验收记录 |
| [CORE_RELIABILITY_FIX_ACCEPTANCE.md](CORE_RELIABILITY_FIX_ACCEPTANCE.md) | 六项修复的验收记录 |
| [RELIABILITY_STATUS.md](RELIABILITY_STATUS.md) | 当时的可靠性状态与已知边界 |
| [PROCESS_REAPING_INVESTIGATION.md](PROCESS_REAPING_INVESTIGATION.md) | 进程回收状态提前清空的专项调查 |
| [process-reaping-fix.md](process-reaping-fix.md) | 进程回收可见性的专项修复记录（缺陷证据、实现、回归）；当前契约见 [进程回收](../capabilities/process-reaping.md) |
| [process-diagnostics-patch.md](process-diagnostics-patch.md) | 每代际进程诊断的本地补丁记录（基线、部署前置、当时的验证状态）；当前接口见 [进程诊断事件](../capabilities/process-diagnostics.md) |

## 重构与迁移方案

| 文档 | 内容 | 结论 |
| :--- | :--- | :--- |
| [KERNEL_CONVERGENCE.md](KERNEL_CONVERGENCE.md) | 内核收敛重构基线（三表一入口） | 已落地 |
| [ledger-core-rewrite.md](ledger-core-rewrite.md) | 账本换骨对照表 | 已终止；§8 记录 C3:54 的落地过程 |
| [named-endpoint-migration.md](named-endpoint-migration.md) | 从 fd/HANDLE 迁移到命名端点 | 已落地，现行机制见 [capabilities/transport.md](../capabilities/transport.md) |
| [STORAGE_QUERY_PROJECTION_REVIEW.md](STORAGE_QUERY_PROJECTION_REVIEW.md) | StorageQuery 顶层字段投影契约审查 | 方案记录，未作为已实现能力 |

## SDK 分析

| 文档 | 内容 |
| :--- | :--- |
| [SDK_ANATOMY.md](SDK_ANATOMY.md) | 旧 SDK 的能力、结构与耦合分析；"收敛为单层"的方向已落地为现行 15 能力单文件 SDK |
