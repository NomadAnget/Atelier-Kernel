# Atelier Kernel

**Atelier Kernel** 是一个采用 **Rust** 编写的单机多域微内核（非分布式、零网络内部通信），为多语言业务域提供统一的生命周期托管、跨域消息路由、四阶段状态回执、全链路实时观测与按域数据存储基础设施。

完整架构规范请参阅：[ARCHITECTURE.md](./ARCHITECTURE.md)。

---

## 核心特性

1. **非分布式、零网络内部通信**：内核与各业务域之间仅通过操作系统**父子进程匿名管道（`stdin` / `stdout` JSON-Lines）**通信，不占用任何内部 TCP/UDP 端口。
2. **内核接管域生命周期 & 语言无感**：内核读取 `kernel_boot.json` 拉起各域子进程；任何语言（Python、Node.js、Rust、Go、C++ 等）只要能读写标准输入输出 JSON 行即可接入，新增域无需修改或重新编译内核源码。
3. **物理管道级身份绑定**：域身份由内核持有的子进程管道句柄直接决定，不依赖 JSON 自声明，彻底杜绝身份伪造与跨表越权。
4. **消息 ID 由内核统一生成 & 三级幂等投递保证**：
   - 发送端无需自建全局 ID，消息进入内核瞬间分配全局单调唯一 `message_id`；
   - 第一级：按 `(source_domain, seq)` 对发送端重复提交防抖；
   - 第二级：按 `(message_id, target)` 维护逐目标独立状态账本；
   - 第三级：按单调偏序吸收重复或乱序回执。
5. **`<domain>.<handler>` 自动路由 & `TargetInvalid` 零盲发拦截**：域启动后只需发送 `RegisterHandlers` 报备处理器列表；目标不存在或未就绪时内核绝不发送消息，立即返回 `TargetInvalid`。
6. **两层消息结构，隔离业务演进**：上层为固定内核契约元数据（用于路由、回执追踪与自动建表），内层 `data` 为标准 JSON 数据包；业务数据结构演进对内核与表结构零影响。
7. **四阶段标准回执 & 独立实时观察组件（`KernelObserver`）**：严格遵循 `Sent -> Received -> Started -> Completed` 四阶段回执闭环，并由 `KernelObserver` 实时旁路广播全量消息状态。
8. **存储即消息（Storage as Messaging）**：按域自动建表（`store_<domain>`），向 `kernel.storage` 发送 `StorageInsert`、`StorageQuery`、`StorageUpdate`、`StorageDelete` 消息即完成增删改查。

---

## 快速运行

```bash
# 1. 启动内核 + 多语言测试域 + 实时观察网页前端域 (默认监听 http://127.0.0.1:8787)
cargo run

# 2. 启动终端交互式 REPL 测试菜单
cargo run -- --cli

# 3. 一键运行全功能自动化演练
cargo run -- --auto

# 4. 运行集成测试套件
cargo test
```
