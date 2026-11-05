import asyncio
from atelier_sdk import AtelierDomain

domain = AtelierDomain("order")


@domain.handler("create")
async def on_create(msg):
    """创建销售订单并持久化到本域存储表 store_order"""
    return {"ok": True}


@domain.handler("echo")
def on_echo(msg):
    """原样回显请求参数与载荷数据（连通性测试）"""
    return {"echo_from": "order.echo"}


asyncio.run(domain.run_async())
