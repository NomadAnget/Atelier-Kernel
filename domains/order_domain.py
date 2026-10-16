#!/usr/bin/env python3
"""
Language-Agnostic Python Domain (`order`) for Atelier Kernel.
Registers `create`, `echo`, `callback_ui`, `set_auto_traffic` and also
periodically generates random cross-domain messages when auto-traffic is enabled.
"""
import json
import random
import sys
import threading
import time

seq_counter = 1
write_lock = threading.Lock()
auto_traffic_enabled = True
auto_interval_sec = 2.2


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


def random_traffic_worker():
    skus = ["ATELIER-GEAR-01", "ATELIER-CORE-X", "NEURAL-LINK-V2", "CHRONO-MOD-7"]
    order_no = 100
    while True:
        time.sleep(auto_interval_sec + random.uniform(0.2, 1.2))
        if not auto_traffic_enabled:
            continue
        order_no += 1
        sku = random.choice(skus)
        qty = random.randint(1, 4)
        oid = f"AUTO-ORD-{order_no}"

        roll = random.random()
        if roll < 0.45:
            # 1) Multi-target call: deduct inventory + evaluate risk
            send_message(
                targets=["inventory.deduct", "risk.evaluate"],
                message_type="OrderPipelineCheck",
                params={"order_id": oid, "origin": "order_auto_bot"},
                data={"order_id": oid, "sku": sku, "qty": qty, "amount": qty * 199},
            )
        elif roll < 0.80:
            # 2) Store auto-generated order into `store_order` via kernel.storage
            send_message(
                targets=["kernel.storage"],
                message_type="StorageInsert",
                params={"record_id": oid, "category": "auto_order", "upsert": True},
                data={"order_id": oid, "sku": sku, "qty": qty, "status": "created"},
            )
        else:
            # 3) Push live business event to `web_ui.push_alert`
            send_message(
                targets=["web_ui.push_alert"],
                message_type="OrderFlashEvent",
                params={"level": "info"},
                data={"text": f"订单域自动生成新订单 {oid} ({sku} x{qty})"},
            )


def main():
    global auto_traffic_enabled, auto_interval_sec

    # 1. Register handlers with the Rust Kernel on startup
    send_message(
        targets=["kernel.registry"],
        message_type="RegisterHandlers",
        params={"handlers": ["create", "echo", "slow_op", "callback_ui", "set_auto_traffic"]},
        data={},
    )

    # 2. Start background random traffic thread
    t = threading.Thread(target=random_traffic_worker, daemon=True)
    t.start()

    # 3. Event loop reading KernelToDomainFrame from stdin
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
        source_domain = frame["source_domain"]
        params = frame.get("params") or {}
        data = frame.get("data") or {}

        # Stage 2: Received
        report_receipt(msg_id, target, "received", None)
        # Stage 3: Started
        report_receipt(msg_id, target, "started", None)
        time.sleep(0.03)  # slight realistic execution delay so 4 stages are visually distinct

        handler_name = target.split(".", 1)[1] if "." in target else target

        if handler_name == "create":
            order_id = data.get("order_id", f"ORD-{msg_id[-6:]}")
            sku = data.get("sku", "ATELIER-GEAR-01")
            qty = data.get("qty", 1)
            order_record = {
                "order_id": order_id,
                "sku": sku,
                "qty": qty,
                "created_by_domain": "order (Python3)",
                "requested_by": source_domain,
            }
            send_message(
                targets=["kernel.storage"],
                message_type="StorageInsert",
                params={"record_id": order_id, "category": "sales_order", "upsert": True},
                data=order_record,
            )
            report_receipt(msg_id, target, "completed", {
                "ok": True,
                "runtime": "Python3 (Stdio Pipe)",
                "saved_to_table": "store_order",
                "order": order_record,
            })

        elif handler_name == "echo":
            report_receipt(msg_id, target, "completed", {
                "echo_from": "order.echo (Python3)",
                "received_params": params,
                "received_data": data,
            })

        elif handler_name == "slow_op":
            delay_ms = int(params.get("delay_ms", 400))
            time.sleep(delay_ms / 1000.0)
            report_receipt(msg_id, target, "completed", {
                "ok": True,
                "slept_ms": delay_ms,
            })

        elif handler_name == "callback_ui":
            send_message(
                targets=["web_ui.push_alert", "frontend.notify"],
                message_type="OrderDomainNotification",
                params={"severity": "info"},
                data={
                    "greeting": f"来自 Python order 域的反向通知（响应 {source_domain} 的消息 {msg_id}）",
                },
            )
            report_receipt(msg_id, target, "completed", {
                "pushed_notification_to": ["web_ui.push_alert", "frontend.notify"],
            })

        elif handler_name == "set_auto_traffic":
            if "enabled" in data:
                auto_traffic_enabled = bool(data["enabled"])
            if "interval_ms" in data:
                auto_interval_sec = max(0.3, float(data["interval_ms"]) / 1000.0)
            report_receipt(msg_id, target, "completed", {
                "domain": "order",
                "auto_traffic_enabled": auto_traffic_enabled,
                "interval_sec": auto_interval_sec,
            })

        else:
            report_receipt(msg_id, target, "execution_failed", {
                "error": f"Unknown handler {handler_name}",
            })


if __name__ == "__main__":
    main()
