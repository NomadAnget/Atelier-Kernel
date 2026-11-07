# Atelier Kernel v0.4.0 发布说明

**语义变更**：C4 存储枢纽的权限与删除语义重写——创建限本域，读取/写入/查询/删除可跨域，
删除对象 = 删除物理文件；同时移除了引用计数与读取时的隐式自愈。

## 背景

v0.3.x 的对象接口存在三处不一致：

- 读取可跨域、写入只限本域、删除只限本域，`StorageObjectCommit` 又不校验归属；
- `delete` 只删索引行，物理文件永久残留；`ref_count` 没有调用方，`object_referenced` 永远不触发；
- 同一虚拟路径再次登记时复用旧 id/旧物理路径，域重启后重写会读到旧内容、新文件成孤儿。

## 新语义

| 操作 | 跨域 | 说明 |
| :--- | :--- | :--- |
| 创建 `StorageObjectPut`（无 `object_id`） | 否 | 只在本域命名空间创建；幂等复用同一虚拟路径 |
| 登记已有文件（带 `object_id`） | 否 | 同一虚拟路径再次登记 = **替换**：删旧文件、换新 id |
| 更新 `StorageObjectCommit` | 是 | 更新已有对象的 size |
| 读取 `StorageObjectGet` | 是 | 按 `object_id`；按 `virtual_path` 时可用 `owner` 指定归属域 |
| 查询 `StorageObjectList` | 是 | 缺省本域；传 `owner` 可跨域 |
| 删除 `StorageObjectDelete` | 是 | **删索引 + 删物理文件**；幂等（不存在返回 `deleted:false`） |

- 移除 `ref_count` / `acquire` / `release`：没有真实调用方，删除直接生效；
- 移除读取时的隐式自愈：物理文件缺失不自动删索引，需业务域显式 `delete`；
- SDK 新增 `commit_object` / `list_objects`（Node: `commitObject` / `listObjects`）；
- 解码文件句柄继续携带 `object_id + virtual_path`（0.3.4 起）。

## 验证

- 对象单测 6 条、C4 集成测试 5 条、SDK Python e2e 均覆盖跨域删除、物理文件删除、幂等与覆盖写替换；
- 内核 lib 单测、C4 集成测试、SDK Python e2e 全绿；
- 变体实测：按 `object_id` 读取成片 7,397,499 字节，删除后文件与索引同时消失。

## 升级方式

```bash
pip install --upgrade atelier-sdk==0.4.0
npm install atelier-sdk@0.4.0
```

内核运行时包与 SDK 版本号必须一致。
