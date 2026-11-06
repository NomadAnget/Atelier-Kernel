# 进度状态

进度是**瞬态日志的一种形态**，不是独立 API：用 `logger` 的多状态字段表达。

```python
# 进行中
domain.logger.info("正在下载分片 1/2", ephemeral=True, key=f"dl_{order_id}", frac=0.50)

# 完成（等价于 done=True）
domain.logger.info("分片下载完成", ephemeral=True, key=f"dl_{order_id}", frac=1.0, done=True)

# 显式失败 —— UI 才能区分"还没跑完"和"失败了"
domain.logger.info("下载失败", ephemeral=True, key=f"dl_{order_id}", state="failed")
```

Node 端字段名相同，作为 options 传入：`domain.logger.info(msg, { ephemeral: true, key, frac, state })`。

## 语义

- `state` 取 `running` / `completed` / `failed` / `cancelled`，**优先于** `done`。
- 缺省或 `null` 时按 `done` 推导：`done=true` → `completed`，否则 `running`。
- 终态会归一化 `done=true`、`waiting=false`，保留 `frac`，并在广播前把该 `(source_domain, key)` 从活动进度表移除。要结束一个进度，必须复用原来的 `key`。
- 非法 `state` 被拒绝，不会静默接受。
- 活动身份是 `(内核绑定的 source_domain, key)`，因此不同域的同名 `key` 互不干扰。
- 普通日志不携带 `state`；不影响数据库 schema。

## 边界（不要误解）

- 进度事件是**瞬态且允许丢失**的，不落 `hub_logs`。
- `query_progress` 只返回**当前活动**条目，不是历史结果；某个 `key` 消失了表示"不再活动"，**不代表成功**。
- 不提供订阅队列或重试管理 API；进程崩溃时的清理不在能力范围内。

## 对应测试

`tests/cases/lifecycle/integration.rs::test_unified_multi_state_logging_and_hub_logs_persistence` 覆盖：终态清理、显式状态优先、旧 `done` 兼容、`frac` 保留、来源隔离、非法值拒绝，以及两语言线上格式一致。
