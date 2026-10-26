> 历史记录：本文件保留当时诊断和方案，不代表0.1.14发布或部署状态。当前实现与能力边界见[文档索引](INDEX.md)。投影评审属于方案记录，不代表已实现查询投影。

# StorageQuery 顶层字段投影契约审查

2026-10-22；基线 2de8f56 / v0.1.12。来源：Web 维护会话 01a1441e-f846-7bf0-aa2e-7b62b2372bd9。

状态：原则接受通用能力，需按以下边界定稿；本次仅审查，未实现、未发布或部署。

## 已核对的现状

- StorageQuery 目前逐行读取并完整解码 params/data，再在 Rust 中执行 record_id/category/filter 和 limit。当前没有字段投影。除返回量外，单条查询也可能先解码大量不匹配记录，不能只在任务域拿到结果后裁剪。
- Kernel IPC 和两个 SDK 的缺省单帧上限均为 16 MiB。来源报告的约 11.75 MB 不直接证明帧超限；实际 JSON 编码、信封开销、日志诊断和退出原因仍须核实，不能把该数值当作已确认根因。本次没有主动触发 task.inspect。

## 接受的通用契约及修订

1. 保留 kernel.storage / StorageQuery，增加可选 params.exclude_fields: string[]；只表示 data 对象的顶层字面键，不解释为 JSONPath、点分路径或 SQL。数据不是对象时保持原值。内核不识别 outputs、checkpoint 或任何业务类型。
2. 不传 exclude_fields 时，现有 SDK 参数位置、返回字段、排序、filter、limit、读写权限保持兼容，不无条件新增 omitted_fields。显式传入时，每个 item 增加同级 omitted_fields 数组：仅列实际存在且被移除的键；原值为 JSON null 也算存在，重复请求键去重，缺失键不列入。显式空列表返回完整 data 和空 omitted_fields。
3. filter 必须针对原始 params/data 判定，再投影；排除的字段仍可用于原有 filter。record_id/category/limit 的含义和排序不改变；不得把在 SQL 中执行 LIMIT 放在完整过滤之前。
4. 投影应在内核读取层、进入 Rust 大型 Value 构建与跨域传输之前完成；优先使用绑定参数的 SQLite JSON 操作及匹配谓词下推，不在任务域或 SDK 接收完整对象后再删除字段。实现必须验证 SQLite JSON 路径编码和类型一致性，不把字段名直接拼成 SQL/路径；支持点号、引号、反斜线、Unicode、空键等合法字面键。
5. 不修改原存储数据、表结构或写入契约；损坏 JSON 仍显式失败，不以投影隐藏损坏或伪造 null。保持默认查询原行为，不以新增业务 Schema 校验为条件。
6. Python 的可选参数追加在原参数之后或作为 keyword-only，不能占用现有 timeout 位置；Node 保留 options，支持 exclude_fields，按既有风格可同时支持 excludeFields；低层 send/wait 与 storage facade 同步透传，双端文档和类型声明一致。调用者不增加内部重试/补偿或队列管理。
7. Kernel/SDK/消费者应按支持该契约的版本配套接入。旧内核当前会忽略未知参数，不能静默假定投影已执行或自动退回全量读取；显式投影返回的元数据应成为确认其被执行的依据。尚未发布时不得让业务代码假称依赖已满足。

## 资源成本与边界

读取层投影可降低 Rust 对象构建、序列化和两个域之间传输的开销，但 SQLite 处理 JSON 本身仍可能使用与单行大小相关的内存/CPU；不是任意大 JSON 的恒定内存保证。即使只排除少数字段，其他字段/params 很大时，响应仍可能过大；不擅自截断其余业务数据、抬高帧上限或新增持久消息系统。

## 业务域所有权

- Task 默认请求 exclude_fields=['outputs']，pipelines 在写 checkpoint 时保存轻量 output_summary：均由对应域维护，内核仅提供通用查询投影，不内置摘要规则。
- 旧 checkpoint 没有 output_summary 时显式表示摘要缺失，不把被排除的 outputs 误当作 null 或完整输出。
- job_id + step_name 读取单步并生成最多 256 KiB UTF-8 预览、truncated、original_bytes：属于 task 的展示契约。明确原字节数按哪一种 JSON 编码计算、字节预算不切坏 UTF-8，允许截断的文本不是完整 JSON 文档。
- 单步完整读取后生成预览仍需要传输和解析该单步全部输出；不证明大于 IPC 上限的单条 checkpoint 可访问。若存在该规模，需另行审查通用的有界读取/流式预览机制，不能让业务域理解传输故障或假称当前方案已经解决。

## 实现前与验收要求

- 来源提供当前 task 退出/重启的精确日志、实际编码后的帧大小、15 条记录总量和最大单条大小；以只读既有证据优先，避免反复触发运行域崩溃。
- 回归覆盖缺省兼容、无存储修改、顶层字面键、null/缺失/重复、非对象 data、原始过滤语义、排序/limit、损坏 JSON 与域权限。
- 隔离真实 Kernel + Python/Node SDK 验证高低层 API。使用大排除字段和大量不匹配记录证明投影在跨域读取之前生效；分别记录返回字节和读取资源，不把结果变小等同于全局恒定内存。
- 不将业务摘要/预览测试算作内核投影验收，也不把内核投影通过算作生产任务页修复完成。
