# Python SDK

Atelier Kernel 的官方 Python 域 SDK：**单层 `AtelierDomain`**、全异步、15 项能力，无门面类、无 `_async` 别名、无同步版。

包位置：`sdk/python`（`atelier_sdk`）。Node 端见 [nodejs.md](nodejs.md)，跨语言语义见 [../SDK_CONTRACT.md](../SDK_CONTRACT.md)。

## 安装

```bash
cd sdk/python && python build_wheel.py .     # 或 pip install .
pip install atelier_sdk-<version>-py3-none-any.whl
```

## 最小示例

```python
import asyncio
from atelier_sdk import AtelierDomain

domain = AtelierDomain("order")

async def on_create(msg):
    """创建订单并写入本域存储"""
    await domain.storage_insert(msg.data["id"], "sales_order", msg.data)
    return {"ok": True}

domain.handler("create")(on_create)

asyncio.run(domain.run_async())
```

## 15 项能力

| # | 能力 | 方法 |
| :--- | :--- | :--- |
| 1 | 生命周期 | `run_async(on_start=, on_close=)` / `shutdown()` |
| 2 | 注册处理器 | `handler(name)`（docstring 首行自动注册为描述） |
| 3 | 发消息 | `send(targets, message_type, params, data)` → 消息 id |
| 4 | 请求-响应 | `request(target, ...)` → **整条终态回执**（业务载荷在 `receipt["data"]`）；`target` 只接受单个字符串；非 `completed` 抛 `DomainRequestError` |
| 5 | 等待/遗忘 | `wait_result` / `wait_results` / `forget_result` |
| 6 | 上报回执 | `report_receipt(message_id, target, status, data)` |
| 7 | 事件回调 | `on_receipt` / `on_log` / `on_notify` / `on_observation` / `on_telemetry` |
| 8 | 本域存储 | `storage_insert` / `storage_update` / `storage_delete` / `storage_query` |
| 9 | 查询 | `query_logs` / `query_handlers` / `query_runtime` / `query_observer` / `query_progress` |
| 10 | 监督 | `supervisor_start` / `supervisor_stop` / `supervisor_restart` / `supervisor_update` |
| 11 | 订阅 | `subscribe_logs` / `subscribe_telemetry` / `subscribe_notify` / `subscribe_observer`（+ `unsubscribe_*`） |
| 12 | 通知 | `notify(level, message, title, details)` |
| 13 | C4 文件对象 | `enable_file_interception` / `put_object` / `get_object` / `delete_object` / `list_objects` |
| 14 | 日志 | `logger.debug/info/warn/error`；多状态用关键字参数：`phase=` / `ephemeral=` / `key=` / `frac=` / `waiting=` / `done=` / `state=` / `console_only=` |
| 15 | 端口 | `bindings`（`ATELIER_PORT_BINDINGS` 的解析结果） |

## 传输

SDK 构造时连接内核为该代际准备的两条单向端点，域代码不需要关心传输细节：

| 变量 | 方向 |
| :--- | :--- |
| `ATELIER_IPC_IN` | 内核 → 域（域只读） |
| `ATELIER_IPC_OUT` | 域 → 内核（域只写） |

Windows 用命名管道（文件 API），Unix 必须 `socket.connect()`。细节见 [命名端点传输](../capabilities/transport.md)。

`on_start` 完成后，若进程带 `ATELIER_MANAGED_READY=1`，SDK 会发 `{"frame_type":"ready"}` 帧，内核据此激活该域的端口转发。

## C4 虚拟文件

```python
domain.enable_file_interception()          # 把 vfs:// 映射到平铺 object_id 文件
with open("vfs://out/clip.mp4", "wb") as f:
    f.write(payload)
meta = await domain.get_object("out/clip.mp4")   # 文件对象：size / file_type / physical_path
```

消息里放文件对象时，SDK 自动编码为 `{"__atelier_file__": {"owner", "virtual_path"}}`，接收方按逻辑路径解析回可读句柄。

## 版本

SDK 与内核、Node SDK 同版本发布；**跨语言行为一致性**是硬要求，改动需同时更新 [../SDK_CONTRACT.md](../SDK_CONTRACT.md) 与 Node 端。
