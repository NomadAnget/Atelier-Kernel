> 历史记录：以下结论、版本、计数和部署状态对应记录当时的基线，不代表当前发布状态。当前接口以[文档索引](INDEX.md)及宪法为准；旧证据保留，不作当前保证。

# 进程回收可见性修复

## 缺陷与证据

监护线程与手动停止/重启原先在状态锁内取走 `ManagedProcess`，释放状态锁后才执行 kill、wait 与工作线程 join。`get_pid` 只读取执行槽位，导致提前返回 None。原 oversized IPC 回归的 `/proc/<pid>` 消失断言因此偶发失败。

修复前隔离故障探针 40/40 次在 get_pid=None 后仍观察到活进程。新增四个受控回归在旧实现上全部失败；不放宽既有测试断言或延长其等待时间。

## 实现与契约

`DomainState::take_for_cleanup` 在移出所有权时原子记录待回收 PID，`get_pid` 继续返回该 PID。`stop_owned` 在 wait 和工作线程 join 完成后才清除待回收 PID。执行槽位、待回收身份属于同一进程代际，由原生命周期操作锁串行管理。

kill/wait/join 全程不持有状态锁或路由锁。查询不等待 operation 锁，不增加 SDK API、订阅参数或业务域补偿逻辑。PID 存在不表示服务就绪；服务是否可调用继续由原路由/Active 状态决定。

主槽位回收遇 Interrupted 重试 wait；其他 wait 错误保留 Child 与 ManagedProcess 所有权，监护线程后续再处理，PID 不清空，不启动替代进程。同步 stop 在该失败路径返回 false，restart 返回既有 SpawnFailed 错误，不假报新进程已启动。

滚动更新 PID 仍表示当前代际；切换到新 PID 不代表旧代际回收结束，不扩展既有查询为全代际进程清单。本修复不声称解决操作系统永久拒绝 wait、内核被强杀、或整机掉电后的回收。

## 回归

新增四项单元测试分别覆盖故障回收、手动停止、手动重启、自动重启。测试给真实进程增加受控工作线程，在 cleanup join 阶段暂停，通过通道查询 PID 并执行真实 QueryRuntime 请求；要求查询及时返回旧 PID、回收不持状态锁。放行后确认旧 PID 消失，以及停止保持停止、重启得到新 PID。

旧实现验证 0/4，通过修复后 4/4。原 oversized IPC 用例与停止/候选失败更新用例保持原样。完整平台与 SDK 验证通过后部署，部署记录保存于本地变体 tmp。

本次本地验证：标准 `cargo test --all-targets` 连续三轮通过；工作区既有 ignored 故障夹具未作修改。工作流的十项 Python/Node SDK 脚本全部通过（含双语言真实自重启）。Windows GNU all-targets 交叉检查通过，保留原有 unused-import 警告。本地已部署的进度/诊断/环境组合源码叠加修复后，另一次完整 Rust 回归通过。
