# atelier-sdk (Python)

Atelier Kernel 的官方 Python 域 SDK：**单层 `AtelierDomain`**、全异步、15 项能力，无门面类、无 `_async` 别名、无同步版。

> 完整能力表、传输模型与 C4 用法见仓库 [`docs/sdk/python.md`](../../docs/sdk/python.md)。

## 安装

```bash
python build_wheel.py .          # 或 pip install .
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

## 传输

内核通过 `ATELIER_IPC_IN`（内核 → 域）与 `ATELIER_IPC_OUT`（域 → 内核）两条单向端点通信，SDK 构造时自动连接，域代码不需要关心传输细节。
