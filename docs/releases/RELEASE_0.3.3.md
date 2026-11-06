# Atelier Kernel v0.3.3 发布说明

v0.3.2 之后的补丁：清理一处死代码，并给 SDK 补上**域存在性探测**，让"域在不在"不再靠业务消息去试。

## 新增

- **`domainInfo(domainId)` / `domain_info(domain_id)`**：读内核 `QueryRuntime` 元数据，返回该域运行态条目（`domain_id`、`status`、`instance_id`、`routes`、`process`），不存在时返回 `null`。它是纯查询，**不发送业务消息、不产生回执、不写观察流**。
- 契约与变体指南同步：**探测域是否存在只能用元数据**。用 `pipelines.ping` 这类业务消息探存在性是错的——域没运行时只会收到 `target_invalid`，把"基础设施不存在"伪装成"业务调用失败"，还污染回执账本与观察流；业务 ping 只在对方已存在时用于查询就绪状态。

## 修复

- 删除 `src/router.rs` 里 `let mut last_stats` 的残留（上一版把每秒统计帧改成随事件携带后遗留）。`cargo check --all-targets` 现在零 warning、零 error。

## 验证

- `cargo test` 全绿；`cargo check --all-targets` 零警告。
- Node SDK 30 项、Python SDK 26 项测试全过（新增 domainInfo / domain_info 用例，覆盖"命中"与"不存在返回 null"）。

## 升级方式

```bash
pip install --upgrade atelier-sdk==0.3.3
npm install atelier-sdk@0.3.3
```

内核运行时包与 SDK 版本必须一致。
