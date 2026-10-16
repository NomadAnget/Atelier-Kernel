# Atelier Kernel

**Atelier Kernel** 是一个采用 **Rust** 编写的单机多域微内核（非分布式、零网络内部通信），为多语言业务域提供统一的生命周期托管、跨域消息路由、四阶段状态回执、全链路实时观测与按域数据存储基础设施。

- 📐 **架构与契约设计规范**：[ARCHITECTURE.md](./ARCHITECTURE.md)
- 📦 **发布包与新变体（Variant）开发指南**：[VARIANT_GUIDE.md](./VARIANT_GUIDE.md)
- 🐍 **Python 域 SDK 文档**：[sdk/python/README.md](./sdk/python/README.md)
- 🟢 **Node.js / TS 域 SDK 文档**：[sdk/nodejs/README.md](./sdk/nodejs/README.md)

---

## 核心特性

1. **非分布式、零网络内部通信**：内核与各业务域之间仅通过操作系统**父子进程匿名管道（`stdin` / `stdout` JSON-Lines）**通信，不占用任何内部 TCP/UDP 端口。
2. **内核接管域生命周期 & 语言无感**：内核读取 `kernel_boot.json` 拉起各域子进程；任何语言（Python、Node.js、Rust、Go、C++ 等）只要能读写标准输入输出 JSON 行即可接入，新增域无需修改或重新编译内核源码。
3. **物理管道级身份绑定 & 代际隔离（`instance_id`）**：域身份由内核持有的子进程管道句柄直接决定，不依赖 JSON 自声明；每次进程重启分配单调代际 `instance_id` 并通过按域互斥门序列化，彻底杜绝身份伪造、跨表越权与旧线程竞态干扰。
4. **消息 ID 由内核统一生成 & 三级幂等投递保证**：
   - 发送端无需自建全局 ID，消息进入内核瞬间分配共同 `logical_id`，并为每个目标分配独立唯一 `message_id`；
   - 第一级：按 `(source_domain, seq)` 对发送端重复提交防抖；
   - 第二级：按 `message_id` 维护逐目标独立状态账本；
   - 第三级：按单调偏序吸收重复或乱序回执。
5. **`<domain>.<handler>` 自动路由 & `TargetInvalid` 零盲发拦截**：域启动后只需发送 `RegisterHandlers` 报备处理器列表；目标不存在或未就绪时内核绝不发送消息，立即返回 `TargetInvalid`。
6. **两层消息结构，隔离业务演进**：上层为固定内核契约元数据（用于路由、回执追踪与自动建表），内层 `data` 为标准 JSON 数据包；业务数据结构演进对内核与表结构零影响。
7. **四阶段标准回执 & 独立实时观察组件（`KernelObserver`）**：严格遵循 `Sent -> Received -> Started -> Completed` 四阶段回执闭环，并由 `KernelObserver` 实时旁路广播全量消息状态。
8. **存储即消息（写隔离、读可跨表）**：按域自动建表（`store_<domain>`），向 `kernel.storage` 发送 `StorageInsert`、`StorageQuery`、`StorageUpdate`、`StorageDelete` 消息即完成增删改查；写入类操作严格限制在本域表，`StorageQuery` 支持通过 `params.domain` 跨表只读查询。

---

## 发布包与产品变体（Variant）极简开发

完整指南请参阅 **[VARIANT_GUIDE.md](./VARIANT_GUIDE.md)**。

### 1. 一键构建 SDK 安装包与内核运行包

```bash
./scripts/build_dist.sh
```

输出产物位于 `dist/`：
- **SDK 安装包**：
  - Python Wheel：`dist/sdk/atelier_sdk-0.1.0-py3-none-any.whl`
  - Node.js NPM 包：`dist/sdk/atelier-sdk-0.1.0.tgz`
- **内核运行包**：
  - 预编译二进制：`dist/kernel-runtime/atelier-kernel`
  - 启动配置模板：`dist/kernel-runtime/kernel_boot.template.json`
  - 变体使用指南：`dist/kernel-runtime/VARIANT_GUIDE.md`

### 2. 使用运行包 3 步启动新变体（无需 Rust 环境）

```bash
# 1) 初始化新变体脚手架（自动生成 kernel_boot.json 与示例域）
./dist/kernel-runtime/atelier-kernel --init ./my-variant

# 2) 安装 Python / Node.js 域 SDK
pip install ./dist/sdk/atelier_sdk-0.1.0-py3-none-any.whl
npm install ./dist/sdk/atelier-sdk-0.1.0.tgz

# 3) 启动变体（默认开启 http://127.0.0.1:8787 实时观察台；生产环境可加 --headless）
./dist/kernel-runtime/atelier-kernel --config ./my-variant/kernel_boot.json
```

---

## 源码开发与验证命令

```bash
# 1. 启动内核 + 多语言测试域 + 实时观察网页前端域 (默认监听 http://127.0.0.1:8787)
cargo run

# 2. 纯后台无头模式运行（不启动 Web 调试前端）
cargo run -- --headless

# 3. 启动终端交互式 REPL 测试菜单
cargo run -- --cli

# 4. 一键运行全功能自动化演练
cargo run -- --auto

# 5. 运行集成测试套件
cargo test
```
