import asyncio
"""
Python Domain (`sensor_hub`) for Atelier Kernel — powered by `atelier_sdk.py`.
"""
import random
import time
from atelier_sdk import AtelierDomain
domain = AtelierDomain()
auto_traffic_enabled = True

@domain.handler('read')
def on_read(_msg):
    """读取当前传感器实时温度与运行状态"""
    return {'sensor': 'sensor_hub (Python3)', 'temperature_c': round(random.uniform(40.0, 65.0), 1), 'status': 'nominal'}

# 校准传感器零点偏移量
@domain.handler('calibrate')
def on_calibrate(msg):
    return {'calibrated': True, 'offset_applied': msg.data.get('offset', 0.0)}

@domain.handler('set_auto_traffic')
def on_set_auto_traffic(msg):
    """动态开启或关闭传感器域自动遥测上报"""
    global auto_traffic_enabled
    if 'enabled' in msg.data:
        auto_traffic_enabled = bool(msg.data['enabled'])
    return {'domain': 'sensor_hub', 'auto_traffic_enabled': auto_traffic_enabled}

async def random_sensor_worker():
    tick = 0
    while True:
        await asyncio.sleep(random.uniform(1.8, 3.2))
        if not auto_traffic_enabled:
            continue
        tick += 1
        temp = round(random.uniform(36.5, 82.0), 1)
        load = round(random.uniform(0.15, 0.95), 2)
        r = random.random()
        if r < 0.55:
            await domain.wait_results(await domain.send(targets=['risk.evaluate', 'kernel.telemetry'], message_type='SensorTelemetry', params={'zone': f'ZONE-{random.randint(1, 3)}', 'tick': tick}, data={'temperature_c': temp, 'cpu_load': load}))
        elif r < 0.85:
            await domain.storage_insert_async(record_id=f'sensor_tick_{tick % 10}', category='telemetry', data={'tick': tick, 'temperature_c': temp, 'cpu_load': load})
        else:
            await domain.wait_results(await domain.send(targets=['legacy_alarm.trigger', 'kernel.notify'], message_type='HighTempProbe', params={'probe': 'thermal_watch'}, data={'text': f'传感器探测脉冲 #{tick} (温度 {temp}°C)，同时测试未注册目标拦截'}))

async def main():
    task = None
    async def start():
        nonlocal task
        task = asyncio.create_task(random_sensor_worker())
    async def close():
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    await domain.run_async(on_start=start, on_close=close)

if __name__ == '__main__':
    asyncio.run(main())
