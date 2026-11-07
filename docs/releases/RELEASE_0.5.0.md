# Atelier Kernel v0.5.0 发布说明

**破坏性重构**：C4 文件对象接口收敛为 **4 个操作**（`put` / `get` / `delete` / `list`），
业务域只见逻辑路径与文件对象，`object_id` 完全留在内核；大小与类型由内核 `stat` 维护，不存哈希。

## 背景

0.4.x 的对象接口仍有 5 种消息、7 种调用形态，并且 `object_id` 会出现在业务消息里：

- `put` 有“内核分配”和“SDK 登记”两个分支；
- `commit` 单独一条消息；
- `get`/`delete` 同时支持 id 与路径两种形式；
- 覆盖写会更换 id 和物理文件，消费者手里的旧路径会失效。

## 新接口（4 个操作）

| 操作 | 参数 | 行为 |
| :--- | :--- | :--- |
| `put` | `virtual_path`, `staging_path` | 内核分配 id；**先建索引、再 rename 暂存文件**到 `<域根>/<id>`；覆盖复用同一 id/路径；`stat` 记录大小、后缀映射类型 |
| `get` | `virtual_path[, owner]` | 读表并 `stat` 刷新 `size/file_mtime`；文件缺失返回 `object_file_missing`，不隐式删索引 |
| `delete` | `virtual_path[, owner]` | 删除物理文件 + 索引行；幂等 |
| `list` | `[owner]` | 只读表 |

- 创建只写本域；读取、查询、删除允许跨域；
- 无 `commit`、无引用计数、无 id 形式入口、无哈希、无自愈；
- 文件对象：`{owner, virtual_path, physical_path, size, file_type, created_at, updated_at}`。

## SDK

- 写模式 `vfs://` 返回**原生文件对象的子类**（Python `io.BufferedWriter`/`io.TextIOWrapper`，Node `FileHandle`/stream），
  `close()`/完成时自动归档；业务不调用任何非原生操作；
- 暂存区在 `<project_root>/.staging/<域>/`（域根之外），内核 rename 进对象路径，不复制大文件；
- 读模式解析为原生句柄；编解码改为 `{"__atelier_file__": {"owner","virtual_path"}}`；
- 方法：`put_object/get_object/delete_object/list_objects`（Node 同名驼峰），删除 `commit_object`、`file_object_id`、`refresh_file_index`。

## 验证

- 内核 lib 单测 50、C4 集成测试 7、SDK e2e（Python + Node）2 全绿；
- 覆盖场景：跨域删除并删除物理文件、覆盖复用路径、先索引后替换（无未索引文件）、staging 越界拒绝、跨重启。

## 升级方式

```bash
pip install --upgrade atelier-sdk==0.5.0
npm install atelier-sdk@0.5.0
```

内核运行时包与 SDK 版本号必须一致。
