import asyncio
"""
Python Domain (`order`) for Atelier Kernel — powered by `atelier_sdk.py`.
Business logic only: zero manual stdio framing or receipt boilerplate!
"""
import random
import time
from atelier_sdk import AtelierDomain
domain = AtelierDomain(stage_delay_sec=0.03)
auto_traffic_enabled = True
auto_interval_sec = 2.2

@domain.handler('create')
async def on_create(msg):
    data = msg.data
    order_id = data.get('order_id', f'ORD-{msg.message_id[-6:]}')
    sku = data.get('sku', 'ATELIER-GEAR-01')
    qty = data.get('qty', 1)
    domain.logger.phase('INFO', 'start', f'Processing new order {order_id} ({qty}x {sku})', layer='DOMAIN')
    order_record = {'order_id': order_id, 'sku': sku, 'qty': qty, 'created_by_domain': 'order (Python3)', 'requested_by': msg.source_domain}
    await domain.storage_insert_async(record_id=order_id, category='sales_order', data=order_record)
    domain.logger.info('Order %s persisted to store_order', order_id, layer='DOMAIN')
    domain.logger.phase('INFO', 'finish', f'Order {order_id} created successfully', layer='DOMAIN')
    return {'ok': True, 'runtime': 'Python3 (atelier_sdk)', 'saved_to_table': 'store_order', 'order': order_record}

@domain.handler('echo')
def on_echo(msg):
    domain.logger.info('Handling echo request for %s', msg.source_domain, layer='DOMAIN')
    return {'echo_from': 'order.echo (Python3)', 'received_params': msg.params, 'received_data': msg.data}

@domain.handler('slow_op')
async def on_slow_op(msg):
    delay_ms = int(msg.params.get('delay_ms', 600))
    key = f'order_task_{msg.message_id[-6:]}'
    domain.logger.progress(f'Waiting to acquire resource for {key}', key=key, waiting=True, layer='DOMAIN')
    await asyncio.sleep(delay_ms * 0.3 / 1000.0)
    domain.logger.progress(f'Executing batch step 1/2 for {key}', key=key, frac=0.5, waiting=False, layer='DOMAIN')
    await asyncio.sleep(delay_ms * 0.4 / 1000.0)
    domain.logger.progress(f'Executing batch step 2/2 for {key}', key=key, frac=0.9, waiting=False, layer='DOMAIN')
    await asyncio.sleep(delay_ms * 0.3 / 1000.0)
    domain.logger.progress(f'Task {key} completed', key=key, frac=1.0, done=True, layer='DOMAIN')
    return {'ok': True, 'slept_ms': delay_ms}

@domain.handler('callback_ui')
async def on_callback_ui(msg):
    await domain.wait_results(await domain.send(targets=['web_ui.push_alert', 'frontend.notify'], message_type='OrderDomainNotification', params={'severity': 'info'}, data={'greeting': f'来自 Python order 域的反向通知（响应 {msg.source_domain} 的消息 {msg.message_id}）'}))
    return {'pushed_notification_to': ['web_ui.push_alert', 'frontend.notify']}

@domain.handler('set_auto_traffic')
def on_set_auto_traffic(msg):
    global auto_traffic_enabled, auto_interval_sec
    if 'enabled' in msg.data:
        auto_traffic_enabled = bool(msg.data['enabled'])
    if 'interval_ms' in msg.data:
        auto_interval_sec = max(0.3, float(msg.data['interval_ms']) / 1000.0)
    return {'domain': 'order', 'auto_traffic_enabled': auto_traffic_enabled, 'interval_sec': auto_interval_sec}

async def random_traffic_worker():
    skus = ['ATELIER-GEAR-01', 'ATELIER-CORE-X', 'NEURAL-LINK-V2', 'CHRONO-MOD-7']
    order_no = 100
    while True:
        await asyncio.sleep(auto_interval_sec + random.uniform(0.2, 1.2))
        if not auto_traffic_enabled:
            continue
        order_no += 1
        sku = random.choice(skus)
        qty = random.randint(1, 4)
        oid = f'AUTO-ORD-{order_no}'
        roll = random.random()
        if roll < 0.45:
            await domain.wait_results(await domain.send(targets=['inventory.deduct', 'risk.evaluate'], message_type='OrderPipelineCheck', params={'order_id': oid, 'origin': 'order_auto_bot'}, data={'order_id': oid, 'sku': sku, 'qty': qty, 'amount': qty * 199}))
        elif roll < 0.8:
            await domain.storage_insert_async(record_id=oid, category='auto_order', data={'order_id': oid, 'sku': sku, 'qty': qty, 'status': 'created'})
        else:
            await domain.wait_results(await domain.send(targets=['web_ui.push_alert'], message_type='OrderFlashEvent', params={'level': 'info'}, data={'text': f'订单域自动生成新订单 {oid} ({sku} x{qty})'}))

async def main():
    task = None
    async def start():
        nonlocal task
        task = asyncio.create_task(random_traffic_worker())
    async def close():
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    await domain.run_async(on_start=start, on_close=close)

if __name__ == '__main__':
    asyncio.run(main())
