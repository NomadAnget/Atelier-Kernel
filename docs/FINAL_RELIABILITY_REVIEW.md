# 核心可靠性最终复审（2026-10-22）

后续状态：本记录保留修复前证据；六项均已修复并通过云端原生三平台验收，见 [修复验收记录](CORE_RELIABILITY_FIX_ACCEPTANCE.md)。

基线：`be1b0ee4405a6a721dcc1538cf1df97840998e39`。本轮未修改内核实现、SDK、消息协议或运行服务；只新增审查记录与 `tmp/final-review/` 隔离探针。结论：**尚未达到最终可靠性验收条件；确认 3 项 P1、3 项 P2。** 前轮云端绿灯与本轮原有测试通过均不覆盖下述条件。

## 覆盖范围与验证方法

- 阅读 ledger/outbox 的提交事务、恢复事务、源会话去重、消息发布锁、完整信封租约与实际写后确认。
- 阅读 Router 的源身份、故障收敛、注册、异步生命周期控制及回执路径，Supervisor 的 active/candidate/draining 资源所有权与退出排空。
- 阅读 Observer 的条数/字节限制、非阻塞扇出，Logging/LogSpool 的独立 WAL、原子导入游标、失败清理与 flush，以及域存储的 JSON 解码/更新。
- 原有 Rust 测试本轮重跑：83 passed、0 failed，另 3 项显式 ignored 故障/测量夹具未作为普通通过计数。没有因为旧测试通过而排除新增问题。
- 六项探针均使用现有公开 Rust/消息入口、独立数据库和自建子进程，连续三次复现。R1 使用第二个真实 OS 进程打开同一个测试库；R5 的旧进程与候选均是自建短期 Python 子进程，最终确认回收。R3 通过仍持有的旧 DomainPipeClient 复现入口缺口，未对生产进程注入帧。
- 跨平台限制：本轮新探针实跑于 Linux/WSL；没有将其宣称为 Windows/macOS 上新故障场景的原生验证。前轮三平台 CI 通过对应现有套件。

## R1 · P1：第二个宿主能将仍在运行的宿主当成重启遗留清理

位置：`src/ledger.rs:160-166`、`src/lib.rs:71-75`。

`open_with_db` 没有同一数据库的独占宿主所有权检查，直接将所有 Sent/Received/Started 改为 ExecutionFailed，删除全部 outbox/sessions。SQLite 事务互斥不能证明原宿主已经退出。

复现：宿主 A 已将 Durable 消息实际交付给目标；启动第二个 OS 宿主 B，打开同一测试数据库后立即正常退出。B 被接受，A 的记录被改成 ExecutionFailed；目标随后报告真实 Completed 被当作旧终态之后的重复消息吸收。A 的源收到终态回执数为 **0**，实际成功结果未保存。

建议边界：在任何恢复性写入前取得并持有数据库对应的唯一宿主所有权，第二个实例必须明确拒绝。守卫随宿主退出释放，不增加协调服务，不用长期占有 SQLite 写事务替代所有权。

## R2 · P1：损坏的域存储 JSON 被伪装为成功，patch 会覆盖原文

位置：`src/storage.rs:340-341`、`src/storage.rs:408-424`。

查询的 params/data 解码失败被 `unwrap_or(Value::Null)` 吞掉；更新也把损坏原文变成 null，再合并 patch 并写回。

复现：在隔离 `store_storage_probe` 中将 data 注入非法 JSON。StorageQuery 返回 **Completed / data:null**；随后 `patch=true` 的 StorageUpdate 返回 **Completed**，数据库原始内容被替换成 `{"new_field":"patch"}`。损坏检测与保留可恢复原文均未实现。此测试不声称 SQLite 正常写入会自行生成非法 JSON。

建议边界：JSON 解码错误作为存储基础设施错误明确传播；读取不伪造 null，patch 在解码失败时拒绝写入，保留原文。不要求业务域识别或修复内核内部损坏。

## R3 · P1：Faulted 旧代际仍可发送消息、恢复注册

位置：`src/router.rs:585-600`、`src/router.rs:1201-1206`。

`is_valid_instance_for_domain` 只比较当前 instance_id，未要求该代际仍绑定有效管道。故障处理移走 pipe 后保留 instance_id，所以旧来源仍被认可；注册分支还能把 Faulted 改回 Active。

复现：显式将 sender 标记 Faulted 后，旧 sender 客户端继续提交，另一个 worker **实际收到业务信封**；将 worker 标记 Faulted 后，旧客户端注册 `resurrected`，结果变成 **Active / worker.resurrected**，但并未接入新管道。

建议边界：输入准入与代际撤销必须有一致的原子边界；已撤销绑定不能再提交/注册。正常 Spawning 首次注册、candidate 与 draining 的有效管道行为仍需保留，不能简单要求所有入站都先 Active。此处不能机械补一把可能与自操作重入的锁。

## R4 · P2：合法业务字段 persisted=false 让终态身份长期驻留

位置：`src/ledger.rs:425-443`。

`finish_publication` 仅依据结果 data.persisted=false 就跳过回收，没有验证这是内核生成的真实持久化失败。业务 JSON 因而影响内核资源管理。

复现：连续完成 10 条 Durable 消息，结果为合法 `{"persisted":false,"value":"valid business result"}`。10 条账本均为持久 Completed，但 child_links 从 **0 增至 10**、submissions 相应增加，正常完成不能释放。长期来源继续发送会消耗全局 MAX_LIVE 配额；本轮未用 65,536 条洪峰或 OOM 作为证明手段。

建议边界：内部持久化失败来源/状态与业务 data 分离，由内核自己的可信状态决定是否保留身份。不保留任意业务字段作为隐藏控制指令；现有失败对外数据结构可保持不变。

## R5 · P2：未完成的异步更新反向延长已释放宿主寿命

位置：`src/router.rs:988-996`、`src/router.rs:1836-1838`、`src/supervisor.rs:1099-1110`、`src/supervisor.rs:1149-1157`。

生命周期线程持有 Router，并在 dispatch 中取得 Supervisor 的强引用；shutdown 只在 Supervisor 最后一个强引用释放的 Drop 中设置。未完成的 UpdateDomain 因而把发出取消信号的所有者也保活。

复现：启动真实旧域与不注册的候选，设更新超时 2,500 ms，释放源客户端与顶层 AtelierKernel。200 ms 后 Router 弱引用仍可升级，旧 PID 仍存在；直到更新超时后才回收。超时值来自调用参数，当前没有宿主先行取消这些工作的退出边界。本轮实证是延迟回收，不声称短超时下永久泄漏。

建议边界：顶层宿主释放能先表达关闭意图，再等待其内部工作回收；后台控制任务不得反向控制宿主关闭信号的生命周期。保持一个清晰的资源所有者，不增加协调层或业务取消样板代码。

## R6 · P2：动态启动绕过启动配置已有的域名校验

位置：`src/supervisor.rs:518-526`、`src/supervisor.rs:1011-1025`；对照 `KernelBootConfig::validate`。

配置文件入口拒绝非法域名，但运行时 StartDomain/UpdateDomain 和直接 spawn 进入 ensure/launch 时没有同一项验证。

复现：使用 `domain_id="bad.domain"` 动态启动，返回 **Ok(pid)**，真实进程已创建；之后才由注册/存储等下游因不合法命名失败。相同标识无法作为合法 boot 配置使用，违反 M1 的统一入口约束。

建议边界：复用一处 DomainProcessSpec 校验，在创建进程、撤销旧代际和占用注册表项前拒绝非法身份与基本配置。不引入另一套消息协议或把校验责任交给业务域。

## 未复现为新缺陷的边界

- 前轮“子进程退出回执先排空”确定性回归、原生进程四阶段回执和旧代际结果隔离仍通过。
- 截图发现 B 的原问题——已持久终态因发送失败后的 finish_publication 被删除——本轮仍无证据；原成功/失败终态重启后按 ID 查询的回归仍通过。R4 是该清理函数的另一个已复现缺陷，不能混为一谈。
- 观察/实时日志副本允许缺口；本轮不把其升级为持久事件流，不把业务级健康验证/无损滚动更新的暂缓需求混入修复。

## 证据与复现

- `tmp/final-review/src/main.rs`：探针。
- `tmp/final-review/repeat-1.log` 至 `repeat-3.log`：三轮完整输出。
- `tmp/final-review/evidence.json` 与 `verify_evidence.py`：六项观测的机器核对。
- `tmp/final-review/existing-suite.log`：原有 Rust 测试结果。

```bash
cd /home/Architecture/atelier-kernel-observer-fix
rm -f tmp/final-review/candidate-pid
CARGO_TARGET_DIR=/home/Architecture/atelier-kernel-migration-lab/target \
  /root/.cargo/bin/cargo run --offline --manifest-path tmp/final-review/Cargo.toml
```

探针自身退出 0 只说明证据采集完成，**不是六项契约通过**。后续修复应把这些观测转为要求正确行为的回归断言。建议先处理 R1/R2/R3，再处理 R4/R5/R6，最后集中验收；本轮没有擅自开始修复、提交、推送或发版。
