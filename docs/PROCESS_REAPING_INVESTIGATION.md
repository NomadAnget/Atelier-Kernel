> 历史记录：本文件保留当时诊断和方案，不代表0.1.14发布或部署状态。当前实现与能力边界见[文档索引](INDEX.md)。投影评审属于方案记录，不代表已实现查询投影。

# 进程回收状态提前清空：专项调查

2026-10-23。状态：已修复、验证并推送 main（0012774406c0f799d3b0b20420745e7c5129520f），已更新本地 8790。下文保留初始调查证据，修复见 PROCESS_REAPING.md。调查只启动隔离内存内核，不操作 8790 或生产服务。

## 既有失败

原断言 `tests/core_reliability.rs` 的 oversized IPC 用例等待 `get_pid` 返回 None，随后要求 `/proc/<pid>` 已消失。全量回归在后一断言失败。原测试未被修改。

原失败记录：`/home/Architecture/atelier-kernel-domain-env/tmp/domain-env-validation/all-targets.log`。随后一次同样的全量回归通过不能证明竞态消失。

## 独立复现

探针源码 `tmp/process-reaping-investigation/probe.rs`，完整输出同目录 `probe.log`。运行时临时作为 cargo integration test，完成后移回 tmp，不加入标准 CI。

40 次分别启动 Python 子进程：关闭自己的 IPC 写端，随后睡眠 30 秒，禁用自动重启。探针等待 get_pid=None，立即读取对应 /proc/status，然后等待最终回收。

结果：40/40 次在 get_pid=None 后仍读到活进程（S sleeping 或 R running），40/40 次最终均在 3 秒观察上限内消失。探针通过仅代表成功复现，不代表缺陷修复。此样本排除了“仅僵尸残留”的解释；没有证明所有路径均不存在永久泄漏。

## 根因与边界

monitor 在 DomainState 锁内执行 process.take()，释放锁后才 stop_owned → terminate → kill → wait → join。get_pid 仅读取 process，因此在 take 和 wait 之间就返回 None。IPC 断开与回收不是同一个完成点。

手动 stop 和常规 restart 也先 take 后回收，具有同类查询窗口；同步 stop 返回发生在回收调用之后，与并发查询提前返回空需要区分。更新成功后的 get_pid 指向新一代，而旧一代仍被滚动更新局部变量持有，是另一种语义，不能据新 PID 判断旧进程已经回收。

现有实现会尝试 kill/wait，当前证据指向状态发布过早，而非已经证实的永久进程泄漏。wait 失败时目前仅诊断，Child 已被 take；失败所有权与后续重试需要纳入修复审查，不能凭本次 40 次成功作保证。

## 修复约束与验收建议

- 内核内部保留待回收进程身份直到完成 wait；路由不可用与 OS 进程已回收分开。无需业务域理解回收流程，不新增业务调用门槛。
- 不通过延长测试睡眠、删掉 /proc 断言或重跑取绿解决。
- 不让 get_pid 等待生命周期 operation 锁，也不持 DomainState 或路由锁执行 kill/wait/join；需要检查 runtime 查询与故障收敛的锁顺序。
- 优先复用 DomainControl/ManagedProcess 的现有代际所有权；覆盖故障退出、手动停止/重启、自动重启、候选失败及更新旧代际回收。wait 错误须显式保留诊断及可回收所有权。
- 增加受控同步的回归：暂停回收阶段，验证查询不提前声称无进程；放行后验证进程消失。并发 runtime 查询必须及时返回，其他域保持可用。
- 修复后运行未改的既有失败用例、多次全量回归与平台检查，再决定提交/部署。
