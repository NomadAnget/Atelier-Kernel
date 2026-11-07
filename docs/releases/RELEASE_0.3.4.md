# Atelier Kernel v0.3.4 发布说明

v0.3.3 之后的增量：**SDK 文件句柄保留 C4 逻辑路径**，日志查询新增**关键字过滤**。

## 背景

- C4 的物理落盘是平铺的 `<object_id>`（无扩展名），而业务域与引擎判定格式依赖逻辑路径（目录 + 后缀）。
  此前 SDK 解码 `{__atelier_file__}` 只返回物理路径，逻辑路径在这一步被丢弃。
- `QueryLogs` 只有级别/来源域过滤，排查问题要自己翻全量日志。

## 新增

- **SDK（Python / Node.js）**：解码文件引用时，句柄同时携带 `object_id` 与 `virtual_path`；
  物理路径仍是平铺 id，逻辑路径只用于格式判定与展示，不改变消息传输契约。
- **日志查询**：`QueryLogs` 支持 `keyword` 过滤（`message LIKE`，通配符已转义），
  可与级别、来源域、时间窗组合使用。

## 验证

- SDK 单元测试 27 条通过，其中新增 2 条：句柄必须携带 `object_id + virtual_path`；对象无逻辑路径时句柄仍可读。
- `cargo test` 全绿；`atelier-kernel --check` 通过。
- 实机（atelier-solo 变体）：管线交付物经 `vfs://` 登记为对象后，web 按 `object_id` 解析并流式返回成片
  （7,397,499 字节，`video/mp4`，Range 请求 206）。

## 升级方式

```bash
pip install --upgrade atelier-sdk==0.3.4
npm install atelier-sdk@0.3.4
```

内核运行时包与 SDK 版本号必须一致。
