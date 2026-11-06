# Atelier Kernel v0.3.3 发布说明

v0.3.2 之后的补丁：**把日志级别在写入侧统一成大写**，并让按级别筛选真正可用；顺带清掉一处死代码。

## 背景

`hub_logs.level` 之前存在两种写法：内核自身写 `INFO`（大写），业务域通过 SDK 写 `info`（小写），写入路径从不规范化。而 `QueryLogs` 的级别筛选却把查询值 `to_uppercase()` 再比较——于是**只筛得到内核自己写的少数行，全部业务域日志被漏掉**；直接查库、导出或第三方工具同样会踩到大小写不一致。

## 修复

- **写入规范化**：`KernelLogHub::reinject()` 入口把 `level` 统一为 `trim + 大写`（空值取 `INFO`）。内核自身、业务域、控制台着色、SSE 推送从此一致。
- **历史迁移**：启动时探测并执行 `UPDATE hub_logs SET level = upper(level)`，已入库的小写行一次性归一（无脏行时跳过，不做无谓写）。
- **查询兼容**：级别筛选改为大小写无关，并把 `WARN` 与 `WARNING` 视为同一级（与内核的级别权重表一致），历史数据或外部写入的变体写法仍可命中。

## 顺带

- 删除 `src/router.rs` 中 `let mut last_stats` 的死变量（上一版删除定时统计帧后遗留），`cargo check --all-targets` 现为零警告。

## 验证

- 迁移后 `hub_logs` 的级别分布为 `INFO / WARN / ERROR`（非大写行 0 条）。
- 新写入日志均为大写；`level=INFO`、`level=info`、`level=WARNING`、`level=WARN`、`level=ERROR` 全部命中预期记录。
- `cargo test` 全绿。

## 升级方式

```bash
pip install --upgrade atelier-sdk==0.3.3
npm install atelier-sdk@0.3.3
```

内核运行时包与 SDK 版本必须一致。