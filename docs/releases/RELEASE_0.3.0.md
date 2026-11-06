# Atelier Kernel v0.3.0 发版说明

v0.2.0 之后累积 52 个提交，主线是**内核分层落定 + SDK 契约收敛 + 自动化门禁补齐**。

## 内核

- **分层完成**：`src/core/`（protocol / routing / nucleus / transport）、`src/services/`（ledger / logs / observer / storage / ports）、`src/edge/`、`src/supervisor/`。`tests/cases/unit/discipline.rs` 强制 `core` 不得引用 `services`。
- **传输 v2 收口**：域侧只认两条单向命名端点（Windows 命名管道 / Unix domain socket）。Unix 端点绑定后收紧为 `0600`，listener 析构时清理 socket 文件。
- **终态历史只存 SQLite**：内存只保留活跃行与提交缓存，历史按需从 `audit` 读；新增 `idx_audit_message` / `idx_audit_logical`，投递热路径不再扫审计表。
- **提交身份收严**：窗口外的过期重放不再重新执行，Durable/Volatile 返回 `ContractInvalid` + `stale_submission`；纯 Ephemeral 在终态后回收身份。入口即终态的路径也会回收，不再残留过期回执。
- **进程生命周期**：域的 PID 在清理真正完成前持续可查；端点 EOF 与进程退出竞争时保留原始退出码与信号，不再把自然退出记成"被清理"。

## SDK

- **单层 `AtelierDomain`，15 项能力**，全异步，无门面类、无同步 API、无 `_async` 别名。
- **日志修好**：此前两个 SDK 用 `send` + `message_type: Log` 发送，内核没有该分支，日志被静默丢弃。现在改为专用 `log` 帧，`hub_logs` 真正入库。
- **批量等待**：`wait_results` / `waitResults` 按 id 批量消费，不触发二次发送。
- **错误类型**：`DomainRequestError` 带 `status` / `data` / `message_id`；`send` 返回内核分配的 id（单目标返回单个 id，目标列表返回有序列表）。
- **类型声明**：Node `index.d.ts` 补齐，`send` 增加字符串/数组重载，并由 `tsc --strict` 消费方在 CI 中把关。

## 测试与门禁

- 27 个测试二进制收敛为 7 个分层目标（`unit` / `transport` / `lifecycle` / `reliability` / `logging` / `c4` / `sdk_e2e`）。
- 修掉一批跨平台脆弱点：端点前缀不再硬编码、轮询读遵守 `WouldBlock` 契约、大响应读取从逐字节改为缓冲。
- CI 新增三道门禁：**文档漂移检查**、**Node 类型检查**、**每次 push 都跑 `--init` + `--smoke-test`**（此前只在发版构建里跑）。

## 文档

- 根目录只留 `README.md` 与 `AGENTS.md`；`docs/` 按契约 / 能力 / SDK / 归档分区，`docs/INDEX.md` 为唯一入口。
- 新增 `AGENTS.md`：面向 AI 编码代理的使用契约（硬规矩、常见错误、验收清单）。
- 归档 25 份历史记录，当前能力文档只保留接口与运行行为。

## 破坏性变更

- Python SDK 构造参数 `max_results` **已删除**。它此前只被赋值、不参与任何容量判断，删除后若仍有代码传入会立即得到 `TypeError`，而不是继续以为自己在限流。
- 域侧实现必须使用命名端点（`ATELIER_IPC_IN` / `ATELIER_IPC_OUT`）。旧的 fd/HANDLE 环境变量在 v0.2.0 已移除，v0.3.0 不再提供任何兼容层。

## 升级方式

```bash
pip install --upgrade atelier-sdk==0.3.0
npm install atelier-sdk@0.3.0
```

内核运行时包与 SDK 必须版本一致；域代码无需改动，除非你传入过 `max_results`。
