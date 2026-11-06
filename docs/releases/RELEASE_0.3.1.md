# Atelier Kernel v0.3.1 发布说明

v0.3.0 之后的三个提交，全部集中在 SDK：**把 SDK 收缩到只承担协议传输**，并修掉四处"替内核决定"的行为。内核行为未变——本版本号随单一版本源递增。

## SDK：只做协议传输

架构裁定：**等多久、等不等、什么时候取结果，全部由业务域自己决定；内核只负责"你发出来了，我给你送出去"。**

SDK 保留协议职责（连接、JSON-Lines 编解码、handler 分发、四阶段回执上报、回执关联、`ready` 握手、`bindings` 解析），删除全部策略：

| 删除项 | 0.3.0 行为 | 0.3.1 行为 |
| :--- | :--- | :--- |
| 默认超时 | `send` / `request` / `waitResult` 默认 30 秒 | **默认 `None` / `undefined`**：不传就一直等内核回执；显式传入才允许本地放弃等待 |
| 提交窗口 | `max_pending=1024` / `maxPending: 1024`，满窗阻塞直至抛 `busy` | **整段删除**。SDK 不再限流，准入由内核 `MAX_LIVE` 与每条消息的终态负责 |
| 结果过期 | `result_ttl=30.0` / `resultTtlMs: 30000`，到期自动驱逐 | **删除**。终态结果不自动过期，取走或 `forgetResult` / `forget_result` 释放，内存归域自己管 |

构造参数因此归零：`AtelierDomain(domain)`（Python 与 Node 一致）。

## SDK：不再替内核决定

- **超时不再丢回执**：帧写入管道后，即使本地等待超时，SDK 仍保留该提交的回执归属（有界 `ORPHAN_LIMIT=4096`，FIFO），迟到回执照样关联到 `message_id` 并可 `waitResult` 取到。
- **关闭后不再假装提交**：`shutdown()` 之后调用 `send` / `request` 立即抛 `local_backpressure`（`data.submitted=false`、`reason="sdk closed"`），不再"静默不写然后等超时"。
- **传输死亡不再沉默**：读端点 EOF 或读错误时，以本地 `transport_closed` 立即终结所有等待（在途提交与正在等待的结果），而不是让调用方挂到自己的超时。
- **一切丢弃可观测**：`stats()` 暴露十个本地计数——`pending` / `results` / `orphans` / `dropped_orphan_receipts` / `local_backpressure` / `unmatched_receipts` / `malformed_frames` / `unknown_statuses` / `dropped_frames` / `transport_closed`。

## 状态用词分离

以下四个状态由 SDK 发出，**都不是内核终态**：

| 状态 | 含义 |
| :--- | :--- |
| `local_backpressure` | 端到端已关闭，帧从未写入内核（可安全重试） |
| `transport_closed` | 读端点结束/出错，内核已把该代际在途消息收敛为 `ExecutionFailed` |
| `result_in_use` | `forgetResult` 时该结果仍有等待者（原 `busy`，它是本地状态而非内核状态） |
| `unknown_message` | 该 id 从未在此 SDK 实例出现过 |

`_dispatch` 在域内没有该 handler 时回的 `target_invalid`，是本域自己对无法处理的消息给出的回执，不是内核的准入裁决；handler 抛出的非 `DomainExecutionFailed` 异常由 SDK 兜底上报 `execution_failed`（`{error:"handler_exception", reason}`），业务需要自己的错误结构时请显式抛 `DomainExecutionFailed`。

## 破坏性变更

- **构造参数**：`max_pending` / `result_ttl` / `maxPending` / `resultTtlMs` 全部删除。传入会立即得到 `TypeError`，而不是继续以为自己在限流。
- **默认超时**：不再有 30 秒。依赖旧默认值的调用方必须显式传 `timeout`，否则会一直等到内核回执（符合"业务域自己决定等多久"）。
- **`forgetResult` 的错误状态**：`busy` → `result_in_use`。
- **`waitResult` 的 `unknown_message`**：不再带 `data.evicted`（结果已不自动过期）。

## 验证

- Node SDK 单元测试 29 项、Python SDK 单元测试 25 项全过。
- `scripts/check_docs.py`（文档漂移）与 `scripts/check_release.py`（版本一致性）通过。

## 升级方式

```bash
pip install --upgrade atelier-sdk==0.3.1
npm install atelier-sdk@0.3.1
```

内核运行时包与 SDK 版本必须一致。
