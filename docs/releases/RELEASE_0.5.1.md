# Atelier Kernel v0.5.1 发布说明

**对象物理文件名改为 UUID**：C4 对象 id 不再使用 `obj_N` 顺序号，改为内核分配的 32 位十六进制 UUID
（与 SDK 生成的历史文件名形态一致，且不再泄漏分配顺序）。

## 变更

- `ObjectHub::alloc_id` 生成 `uuid::Uuid::new_v4().simple()`；
- 删除 `next_id` 递增与重启时的 `obj_N` 解析；
- 逻辑路径、文件对象结构、4 个操作、权限与删除语义均不变；
- 已存在的旧 `obj_N` 物化文件继续可用（索引不变），新对象一律 UUID 命名。

## 验证

- 对象单测与 C4 集成测试断言 32 位十六进制 id；
- 内核 lib、C4、SDK e2e 全绿（本地）。

## 升级方式

```bash
pip install --upgrade atelier-sdk==0.5.1
npm install atelier-sdk@0.5.1
```
