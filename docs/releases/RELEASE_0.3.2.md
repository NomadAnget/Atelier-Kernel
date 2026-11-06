# Atelier Kernel v0.3.2 发布说明

v0.3.1 之后的一次小版本：**让真实投递速率可读，并让观察流的丢失可见**。不改变消息投递语义，属于诊断能力补齐。

## 背景

维护页的「每秒消息数量」取自观察流，而观察流是**允许丢失**的尽力交付（订阅通道 64 深，慢消费者即丢）。高负载下它只能反映订阅者消费速度（约 1,000 条/秒），与真实投递（8,000 条/秒）相差数倍，且没有任何提示。观察者被限速，本身没有错；**错在把它当成了投递速率，且丢弃无人知道**。

## 内核

- **新增累计计数**（`QueryRuntime` 的 `ledger`）：
  - `sent_total`：自启动以来成功分配身份的目标数（过期重放不计）；
  - `terminal_total`：自启动以来进入终态的消息数。
  二者均为单调累计值，差值即真实投递速率，与观察流是否丢事件无关。
- **新增观察丢弃计数**（`QueryRuntime` 的 `observations.dropped_total`）：订阅通道满时被丢弃的观察事件累计数。观察流仍然有损，但不再静默。
- **新增每秒累计计数帧** `KernelStats`（随观察订阅推送）：`kernel_instance` / `sent_total` / `terminal_total` / `emitted_total` / `dropped_total` / `uptime_ms`，全部为**累计值**（非增量），消费方用相邻两帧差值算速率；`kernel_instance` 变化即内核重启，须重置基线。它每秒一帧、携带累计值、**本身不参与事件计数**——错过一帧只延迟一个区间，不会永久丢一个区间；消费方因此不必轮询 `QueryRuntime`。SDK 侧对应 `on_stats` / `onStats`。
- 宪法同步：`docs/CONSTITUTION.md` 的观察流条目补充"丢弃必须可观测"，`ledger` 条目补充"真速率只认累计计数"。

## 契约与文档

- `QueryRuntime` 响应新增 `observations` 段与 `ledger.sent_total` / `ledger.terminal_total` 两个字段；既有字段不变，属纯增量。
- 观察流仍是尽力交付，不增加重连协议、不暴露订阅队列参数、不改变 SDK 调用。

## 配套（变体侧，非本仓代码）

`web` 域维护面板改为：速率取事件内嵌的累计计数（相邻事件差值），并展示观察流样本量与**样本覆盖率**（`1 - dropped_total / emitted_total`）。面板数字因此与发送端回执账本一致，同时看得出来观察流漏了多少。

## 验证

- `cargo test` 全绿（含 observer / ledger / retention 用例）。
- 实测：10 域互发期间内核 `sent_total` 增量 250,755 与发送端发出的 250,577 一致（差额为查询自身流量），同期观察流丢弃 782,838 条事件——正是旧面板读数偏小的原因。

## 升级方式

```bash
pip install --upgrade atelier-sdk==0.3.2
npm install atelier-sdk@0.3.2
```

内核运行时包与 SDK 版本必须一致。
