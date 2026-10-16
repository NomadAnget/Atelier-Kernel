#!/usr/bin/env python3
"""
Language-Agnostic Python Domain (`sensor_hub`) for Atelier Kernel.
Randomly emits telemetry messages across domains, writes to `store_sensor_hub`,
and occasionally targets a non-existent handler to demonstrate live `TargetInvalid` interception.
"""
import json
import random
import sys
import threading
import time

seq_counter = 1
write_lock = threading.Lock()
auto_traffic_enabled = True


def send_frame(frame: dict):
    line = json.dumps(frame, ensure_ascii=False) + "\n"
    with write_lock:
        sys.stdout.write(line)
        sys.stdout.flush()


def send_message(targets, message_type, params, data):
    global seq_counter
    with write_lock:
        seq = seq_counter
        seq_counter += 1
    send_frame({
        "frame_type": "send",
        "seq": seq,
        "targets": targets,
        "message_type": message_type,
        "params": params,
        "data": data,
    })
    return seq


def report_receipt(message_id: str, target: str, status: str, data=None):
    send_frame({
        "frame_type": "receipt",
        "message_id": message_id,
        "target": target,
        "status": status,
        "data": data,
    })


def random_sensor_worker():
    tick = 0
    while True:
        time.sleep(random.uniform(1.8, 3.2))
        if not auto_traffic_enabled:
            continue
        tick += 1
        temp = round(random.uniform(36.5, 82.0), 1)
        load = round(random.uniform(0.15, 0.95), 2)

        r = random.random()
        if r < 0.55:
            # Multi-target fan-out: send to risk.evaluate + web_ui.metric_tick
            send_message(
                targets=["risk.evaluate", "web_ui.metric_tick"],
                message_type="SensorTelemetry",
                params={"zone": f"ZONE-{random.randint(1, 3)}", "tick": tick},
                data={"temperature_c": temp, "cpu_load": load},
            )
        elif r < 0.85:
            # Persist sensor snapshot to `store_sensor_hub` via kernel.storage
            send_message(
                targets=["kernel.storage"],
                message_type="StorageInsert",
                params={"record_id": f"sensor_tick_{tick % 10}", "category": "telemetry", "upsert": True},
                data={"tick": tick, "temperature_c": temp, "cpu_load": load},
            )
        else:
            # Intentionally include an unregistered target (`legacy_alarm.trigger`) to show TargetInvalid interception
            send_message(
                targets=["legacy_alarm.trigger", "web_ui.push_alert"],
                message_type="HighTempProbe",
                params={"probe": "thermal_watch"},
                data={"text": f"传感器探测脉冲 #{tick} (温度 {temp}°C)，同时测试未注册目标拦截"},
            )


def main():
    global auto_traffic_enabled

    send_message(
        targets=["kernel.registry"],
        message_type="RegisterHandlers",
        params={"handlers": ["read", "calibrate", "set_auto_traffic"]},
        data={},
    )

    t = threading.Thread(target=random_sensor_worker, daemon=True)
    t.start()

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            frame = json.loads(line)
        except Exception:
            continue

        if frame.get("frame_type") != "deliver":
            continue

        msg_id = frame["message_id"]
        target = frame["target"]
        data = frame.get("data") or {}
        handler = target.split(".", 1)[1] if "." in target else target

        report_receipt(msg_id, target, "received", None)
        report_receipt(msg_id, target, "started", None)

        if handler == "read":
            report_receipt(msg_id, target, "completed", {
                "sensor": "sensor_hub (Python3)",
                "temperature_c": round(random.uniform(40.0, 65.0), 1),
                "status": "nominal",
            })
        elif handler == "calibrate":
            report_receipt(msg_id, target, "completed", {
                "calibrated": True,
                "offset_applied": data.get("offset", 0.0),
            })
        elif handler == "set_auto_traffic":
            if "enabled" in data:
                auto_traffic_enabled = bool(data["enabled"])
            report_receipt(msg_id, target, "completed", {
                "domain": "sensor_hub",
                "auto_traffic_enabled": auto_traffic_enabled,
            })
        else:
            report_receipt(msg_id, target, "execution_failed", {"error": f"Unknown handler {handler}"})


if __name__ == "__main__":
    main()
