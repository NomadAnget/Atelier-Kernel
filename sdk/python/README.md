# atelier-sdk (Python)

Atelier Kernel 的官方 Python 域 SDK。**单层 `AtelierDomain`**、全异步、15 项能力，无门面类、无 `_async` 别名、无同步版。

## 安装

```bash
python build_wheel.py .          # 或 pip install .
pip install atelier_sdk-0.2.0-py3-none-any.whl
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
|---|---|---|
| 1 | 生命周期 | `run_async(on_start=, on_close=)` / `shutdown()` |
| 2 | 注册处理器 | `handler(name)`（docstring 首行自动注册为描述） |
| 3 | 发消息 | `send(targets, message_type, params, data)` → 消息 id |
| 4 | 请求-响应 | `request(...)` → 终态回执的 data |
| 5 | 等待/遗忘 | `wait_result` / `wait_results` / `forget_result` |
| 6 | 上报回执 | `report_receipt(message_id, target, status, data)` |
| 7 | 事件回调 | `on_receipt` / `on_log` / `on_notify` / `on_observation` / `on_telemetry` |
| 8 | 本域存储 | `storage_insert` / `storage_update` / `storage_delete` / `storage_query` |
| 9 | 查询 | `query_logs` / `query_handlers` / `query_runtime` / `query_observer` / `query_progress` |
| 10 | 监督 | `supervisor_start` / `supervisor_stop` / `supervisor_restart` / `supervisor_update` |
| 11 | 订阅 | `subscribe_logs` / `subscribe_telemetry` / `subscribe_notify` / `subscribe_observer`（+ `unsubscribe_*`） |
| 12 | 通知 | `notify(level, message, title, details)` |
| 13 | C4 文件对象 | `enable_file_interception` / `put_object` / `get_object` / `delete_object` / `file_object_id` / `refresh_file_index` |
| 14 | 日志 | `logger.debug/info/warn/error` |
| 15 | 端口 | `bindings`（`ATELIER_PORT_BINDINGS` 的解析结果） |

## 传输

内核通过两条**单向命名管道**把端点交给域（Windows 命名管道 / Unix domain socket）：

| 变量 | 方向 |
|---|---|
| `ATELIER_IPC_IN` | 内核 → 域（域只读） |
| `ATELIER_IPC_OUT` | 域 → 内核（域只写） |

SDK 在构造时打开这两条端点，域代码不需要关心传输细节。

`on_start` 完成后，若进程带 `ATELIER_MANAGED_READY=1`，SDK 会发 `{"frame_type":"ready"}` 帧，内核据此激活该域的端口转发。

## C4 虚拟文件

```python
domain.enable_file_interception()          # 把 vfs:// 映射到平铺 object_id 文件
with open("vfs://out/clip.mp4", "wb") as f:
    f.write(payload)
object_id = domain.file_object_id("vfs://out/clip.mp4")
```

消息里放文件对象时，SDK 自动编码为 `{"__atelier_file__": "<object_id>"}`，接收方解码回可读句柄。

## 版本

0.2.0 —— 传输层改为命名管道；单层 `AtelierDomain`；移除门面类、同步 API 与 `_async` 别名。
