# 进程诊断事件

内核把每个域代际的进程与传输故障，以 **JSON-Lines 写到自己的 stderr**。这是尽力而为的诊断输出，不是持久消息，**没有 SQLite 诊断表、没有订阅 API**。

```json
{"diagnostic": {"stage": "rx_read", "domain_id": "worker", "pid": 5237, "generation": 4, "ts": 1793963240.387, "details": {"kind": "InvalidData", "message": "IPC/diagnostic frame exceeds limit"}}, "first_observed_fault": {"stage": "tx_write", "details": {"message": "domain output disconnected"}}}
```

## 字段

| 字段 | 说明 |
| :--- | :--- |
| `domain_id` / `pid` / `generation` | 该代际的身份；同一 `domain_id` 的不同 `generation` 是不同进程 |
| `ts` | Unix 秒（浮点） |
| `stage` | 见下表 |
| `details` | I/O 错误保留 `kind`（`io::ErrorKind`）、原始 OS 错误码，以及**最多 1024 字符**的 `message`；超限出站帧另带序列化字节数（含换行）与 IPC 上限 |
| `first_observed_fault` | 该代际最先捕获的传输故障，之后的事件都带着它；无故障时为 `null` |

## stage 取值

`tx_write`、`tx_acknowledge`、`outbox`、`rx_read`、`rx_drain`、`child_poll`、`child_exit`、`cleanup_requested`、`child_kill`、`child_wait`、`child_exit_after_cleanup`。

- 每个 stage **每代际只记一次**；记录随代际回收，不留无界内存历史。
- `rx_drain` 只表示"缓冲帧排空后合成的 EOF"，**永不替代主故障**；真实读错误仍然是 `rx_read`，即使排空已并发开始。
- `child_exit` 是**清理请求之前**观测到的退出；`code` 与 Unix `signal` 分开记录（Windows 上 `signal` 为 `null`）。若清理前没拿到状态，则记 `cleanup_requested` 与随后的 `child_exit_after_cleanup`（`after_cleanup_request=true`）。

## 怎么读（重要）

**"first observed" 只表示该代际互斥锁下最先被捕获，不证明物理因果。** 线程调度与 OS 轮询会改变观测顺序：`BrokenPipe` 可能是子进程退出的后果，`outbox` 错误可能早于也可能晚于其它传输错误，自然退出也可能与 kill 请求竞争。

判断根因必须把 child exit 与传输证据合起来看，**不要只凭那条通用的 `domain disconnected` 行下结论**。

## 边界

- 尽力而为的 stderr 输出：报告不了内核自身被 SIGKILL / OOM，或 WSL 关机。
- 不改变 SDK 行为、不重排回执、不改变"先排空缓冲帧再判故障"的策略。
- 域自身的 `stdout` / `stderr` 与协议端点物理隔离采集，不混进这些诊断事件。
- 采集方需要长期保留 stderr 到已知文件；诊断不落库，丢了就没有。

## 对应测试

`tests/cases/lifecycle/diagnostics.rs`：首次错误保留、同代际去重、真实 BrokenPipe、退出码与信号元数据、清理失败时保留原始 outbox 失败，以及超限入站帧（`rx_read` + `InvalidData`）。