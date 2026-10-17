"""
Cross-language verification for Kernel Infrastructure methods:
- on_log / onLog: auto-subscription, level filtering, ephemeral push without receipts
- on_telemetry / onTelemetry: auto-subscription, metric ingestion, zero receipts
- on_notify / onNotify: auto-subscription, alert fanout, zero receipts
- on_observation / onObservation: auto-subscription, lifecycle events, zero receipts
- domain.telemetry.emit / domain.notify: native emission helpers
- domain.runtime: native assembly query
- domain.observer: native snapshot query and subscriptions
- domain.supervisor: native process lifecycle control and rolling update
"""
import json, os, queue, subprocess, sys, threading, time, uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / "tmp/sdk-infra-tests" / str(uuid.uuid4())
WORK.mkdir(parents=True, exist_ok=True)

PY_CODE = '''import asyncio, json, sys
from atelier_sdk import AtelierDomain

d = AtelierDomain(domain="infra_test_py", shutdown_grace=0.1)

received_logs = []
received_telemetry = []
received_alerts = []
received_observations = []

@d.on_log(level="WARNING")
def handle_log(record):
    received_logs.append(record.get("message"))

@d.on_telemetry
def handle_telemetry(metric):
    received_telemetry.append(metric.get("metric_name") or metric.get("name"))

@d.on_notify
def handle_alert(alert):
    received_alerts.append(alert.get("message"))

@d.on_observation
def handle_observation(event):
    received_observations.append(event.get("topic") or event.get("event_type"))

@d.handler("status")
def status(msg):
    return {
        "logs": received_logs,
        "telemetry": received_telemetry,
        "alerts": received_alerts,
        "observations": received_observations,
    }

@d.handler("emit_test")
def emit_test(msg):
    d.telemetry.emit("cpu_load", 85.5, {"host": "node1"})
    d.notify("error", "Disk full", "StorageAlert")
    return {"emitted": True}

@d.handler("infra_test")
async def infra_test(msg):
    res_runtime = await d.runtime.query(domain="domain_alpha")
    res_observer = await d.observer.query(limit=50)
    res_supervisor = await d.supervisor.update(domain="domain_alpha", probe_handler="ping", timeout_ms=3000)
    return {
        "runtime": res_runtime,
        "observer": res_observer,
        "supervisor": res_supervisor,
    }

d.run()
'''

JS_CODE = '''const { AtelierDomain } = require(process.argv[2]);

const d = new AtelierDomain({ domain: "infra_test_js", shutdownGrace: 0.1 });

const receivedLogs = [];
const receivedTelemetry = [];
const receivedAlerts = [];
const receivedObservations = [];

d.onLog((record) => {
  receivedLogs.push(record.message);
}, { level: "WARNING" });

d.onTelemetry((metric) => {
  receivedTelemetry.push(metric.metric_name || metric.name);
});

d.onNotify((alert) => {
  receivedAlerts.push(alert.message);
});

d.onObservation((event) => {
  receivedObservations.push(event.topic || event.event_type);
});

d.handler("status", () => {
  return {
    logs: receivedLogs,
    telemetry: receivedTelemetry,
    alerts: receivedAlerts,
    observations: receivedObservations,
  };
});

d.handler("emit_test", () => {
  d.telemetry.emit("cpu_load", 85.5, { host: "node1" });
  d.notify("error", "Disk full", "StorageAlert");
  return { emitted: true };
});

d.handler("infra_test", async () => {
  const resRuntime = await d.runtime.query({ domain: "domain_alpha" });
  const resObserver = await d.observer.query({ limit: 50 });
  const resSupervisor = await d.supervisor.update("domain_alpha", { probeHandler: "ping", timeoutMs: 3000 });
  return {
    runtime: resRuntime,
    observer: resObserver,
    supervisor: resSupervisor,
  };
});

d.run();
'''

def run_scenario(lang, code):
    r_pipe_read, r_pipe_write = os.pipe()
    w_pipe_read, w_pipe_write = os.pipe()

    if lang == "py":
        script_path = WORK / f"test_{lang}.py"
        script_path.write_text(code, encoding="utf-8")
        cmd = [sys.executable, "-u", str(script_path)]
    else:
        script_path = WORK / f"test_{lang}.js"
        script_path.write_text(code, encoding="utf-8")
        sdk_js_path = str(ROOT / "sdk/nodejs")
        cmd = ["node", str(script_path), sdk_js_path]

    env = os.environ.copy()
    if lang == "py":
        env["PYTHONPATH"] = str(ROOT / "sdk/python") + os.pathsep + env.get("PYTHONPATH", "")
    env["ATELIER_IPC_READ_FD"] = str(r_pipe_read)
    env["ATELIER_IPC_WRITE_FD"] = str(w_pipe_write)

    proc = subprocess.Popen(
        cmd,
        env=env,
        pass_fds=(r_pipe_read, w_pipe_write),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    os.close(r_pipe_read)
    os.close(w_pipe_write)

    out_file = os.fdopen(w_pipe_read, "r", encoding="utf-8", buffering=1)
    in_file = os.fdopen(r_pipe_write, "w", encoding="utf-8", buffering=1)

    frames_received = []
    frames_q = queue.Queue()

    def reader():
        while True:
            line = out_file.readline()
            if not line:
                break
            try:
                frame = json.loads(line.strip())
                frames_received.append(frame)
                frames_q.put(frame)
            except Exception:
                pass

    t = threading.Thread(target=reader, daemon=True)
    t.start()

    # 1. Collect startup frames: should contain RegisterHandlers and 4 auto-subscriptions
    startup_sub_targets = set()
    start_time = time.time()
    while time.time() - start_time < 3.0:
        try:
            f = frames_q.get(timeout=0.2)
            if f.get("frame_type") == "send":
                target = f.get("targets", [""])[0]
                m_type = f.get("message_type")
                if target == "kernel.registry" and m_type == "RegisterHandlers":
                    startup_sub_targets.add("registry")
                elif target == "kernel.logger" and m_type == "SubscribeLogs":
                    startup_sub_targets.add("logger")
                    assert f.get("params", {}).get("level") == "WARNING"
                elif target == "kernel.telemetry" and m_type == "SubscribeTelemetry":
                    startup_sub_targets.add("telemetry")
                elif target == "kernel.notify" and m_type == "SubscribeNotify":
                    startup_sub_targets.add("notify")
                elif target == "kernel.observer" and m_type == "SubscribeObserver":
                    startup_sub_targets.add("observer")
            if len(startup_sub_targets) == 5:
                break
        except queue.Empty:
            continue

    if len(startup_sub_targets) != 5:
        stdout_text = proc.stdout.read().decode("utf-8", errors="replace") if proc.stdout else ""
        stderr_text = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
        raise AssertionError(f"[{lang}] Missing auto-subscriptions: {startup_sub_targets}\nSTDOUT: {stdout_text}\nSTDERR: {stderr_text}\nFRAMES: {frames_received}")
    print(f"  [{lang}] Auto-subscriptions sent correctly: {startup_sub_targets}")

    # 2. Push system events to domain pipe:
    # 2a. Push INFO log (should be ignored due to level=WARNING)
    in_file.write(json.dumps({
        "frame_type": "deliver",
        "message_id": "log_1",
        "target": "kernel.logger",
        "message_type": "LogEvent",
        "source_domain": "kernel",
        "data": {"level": "INFO", "message": "Ignored info log"},
    }) + "\n")
    in_file.flush()

    # 2b. Push WARNING log (should be received)
    in_file.write(json.dumps({
        "frame_type": "deliver",
        "message_id": "log_2",
        "target": "kernel.logger",
        "message_type": "LogEvent",
        "source_domain": "kernel",
        "data": {"level": "WARNING", "message": "Captured warning log"},
    }) + "\n")
    in_file.flush()

    # 2c. Push Telemetry event
    in_file.write(json.dumps({
        "frame_type": "deliver",
        "message_id": "telem_1",
        "target": "kernel.telemetry",
        "message_type": "TelemetryEvent",
        "source_domain": "kernel",
        "data": {"metric_name": "temp_gauge", "value": 45.2},
    }) + "\n")
    in_file.flush()

    # 2d. Push Notify event
    in_file.write(json.dumps({
        "frame_type": "deliver",
        "message_id": "alert_1",
        "target": "kernel.notify",
        "message_type": "NotifyEvent",
        "source_domain": "kernel",
        "data": {"severity": "error", "message": "Cluster alert text"},
    }) + "\n")
    in_file.flush()

    # 2e. Push Observer event
    in_file.write(json.dumps({
        "frame_type": "deliver",
        "message_id": "obs_1",
        "target": "kernel.observer",
        "message_type": "ObservationEvent",
        "source_domain": "kernel",
        "data": {"topic": "worker_lifecycle", "state": "active"},
    }) + "\n")
    in_file.flush()

    # Wait 250ms and verify NO receipts were generated for the above 5 system deliveries
    time.sleep(0.25)
    sent_receipt_ids = [
        f["message_id"] for f in frames_received if f.get("frame_type") == "receipt"
    ]
    for sys_id in ["log_1", "log_2", "telem_1", "alert_1", "obs_1"]:
        assert sys_id not in sent_receipt_ids, f"[{lang}] System event {sys_id} must NEVER trigger receipt frames!"
    print(f"  [{lang}] Verified zero receipt frames sent for system event deliveries")

    # 3. Call 'status' handler to verify callbacks recorded the events
    target_name = f"infra_test_{lang}.status"
    in_file.write(json.dumps({
        "frame_type": "deliver",
        "message_id": "rpc_status_1",
        "target": target_name,
        "message_type": "QueryStatus",
        "source_domain": "test_runner",
        "data": {},
    }) + "\n")
    in_file.flush()

    # Expect standard 4-stage receipts for the RPC delivery
    status_result = None
    start_time = time.time()
    while time.time() - start_time < 3.0:
        try:
            f = frames_q.get(timeout=0.2)
            if f.get("frame_type") == "receipt" and f.get("message_id") == "rpc_status_1":
                if f.get("status") == "completed":
                    status_result = f.get("data")
                    break
        except queue.Empty:
            continue

    assert status_result is not None, f"[{lang}] RPC status failed to complete"
    assert status_result["logs"] == ["Captured warning log"], f"[{lang}] Unexpected logs: {status_result['logs']}"
    assert status_result["telemetry"] == ["temp_gauge"], f"[{lang}] Unexpected telemetry: {status_result['telemetry']}"
    assert status_result["alerts"] == ["Cluster alert text"], f"[{lang}] Unexpected alerts: {status_result['alerts']}"
    assert status_result["observations"] == ["worker_lifecycle"], f"[{lang}] Unexpected observations: {status_result.get('observations')}"
    print(f"  [{lang}] Verified callbacks executed with filtered event data")

    # 4. Call 'emit_test' handler to verify domain.telemetry.emit and domain.notify outbound emission
    in_file.write(json.dumps({
        "frame_type": "deliver",
        "message_id": "rpc_emit_1",
        "target": f"infra_test_{lang}.emit_test",
        "message_type": "EmitTest",
        "source_domain": "test_runner",
        "data": {},
    }) + "\n")
    in_file.flush()

    emitted_telem = False
    emitted_notify = False
    start_time = time.time()
    while time.time() - start_time < 3.0:
        try:
            f = frames_q.get(timeout=0.2)
            if f.get("frame_type") == "send":
                target = f.get("targets", [""])[0]
                if target == "kernel.telemetry":
                    assert f["data"]["name"] == "cpu_load"
                    assert f["data"]["value"] == 85.5
                    emitted_telem = True
                elif target == "kernel.notify":
                    assert f["params"]["severity"] == "error"
                    assert f["data"]["message"] == "Disk full"
                    emitted_notify = True
            elif f.get("frame_type") == "receipt" and f.get("message_id") == "rpc_emit_1" and f.get("status") == "completed":
                pass
            if emitted_telem and emitted_notify:
                break
        except queue.Empty:
            continue

    assert emitted_telem, f"[{lang}] Missing telemetry emission"
    assert emitted_notify, f"[{lang}] Missing notify emission"
    print(f"  [{lang}] Verified domain.telemetry.emit and domain.notify outbound frame generation")

    # 5. Call 'infra_test' handler to verify runtime, observer, supervisor native client methods
    in_file.write(json.dumps({
        "frame_type": "deliver",
        "message_id": "rpc_infra_1",
        "target": f"infra_test_{lang}.infra_test",
        "message_type": "InfraTest",
        "source_domain": "test_runner",
        "data": {},
    }) + "\n")
    in_file.flush()

    infra_completed_data = None
    start_time = time.time()
    while time.time() - start_time < 4.0:
        try:
            f = frames_q.get(timeout=0.2)
            if f.get("frame_type") == "send":
                target = f.get("targets", [""])[0]
                seq = f.get("seq")
                mid = f.get("message_id") or f"mock_msg_{seq}"
                if target == "kernel.runtime":
                    assert f.get("params", {}).get("domain") == "domain_alpha"
                    # Mock Kernel response completed receipt
                    in_file.write(json.dumps({
                        "frame_type": "receipt",
                        "seq": seq,
                        "message_id": mid,
                        "target": "kernel.runtime",
                        "status": "completed",
                        "data": {"domain": {"domain_id": "domain_alpha", "status": "Running"}},
                    }) + "\n")
                    in_file.flush()
                elif target == "kernel.observer":
                    assert f.get("params", {}).get("limit") == 50
                    # Mock Kernel response completed receipt
                    in_file.write(json.dumps({
                        "frame_type": "receipt",
                        "seq": seq,
                        "message_id": mid,
                        "target": "kernel.observer",
                        "status": "completed",
                        "data": {"events": [{"topic": "msg_delivered", "target": "domain_alpha.ping"}]},
                    }) + "\n")
                    in_file.flush()
                elif target == "kernel.supervisor":
                    assert f.get("params", {}).get("domain") == "domain_alpha"
                    assert f.get("params", {}).get("probe_handler") == "ping"
                    assert f.get("params", {}).get("timeout_ms") == 3000
                    # Mock Kernel response completed receipt
                    in_file.write(json.dumps({
                        "frame_type": "receipt",
                        "seq": seq,
                        "message_id": mid,
                        "target": "kernel.supervisor",
                        "status": "completed",
                        "data": {"success": True, "domain": "domain_alpha", "action": "update"},
                    }) + "\n")
                    in_file.flush()
            elif f.get("frame_type") == "receipt" and f.get("message_id") == "rpc_infra_1" and f.get("status") == "completed":
                infra_completed_data = f.get("data")
                break
        except queue.Empty:
            continue

    if infra_completed_data is None:
        proc.terminate()
        try:
            stdout_bytes, stderr_bytes = proc.communicate(timeout=1.0)
        except Exception:
            proc.kill()
            stdout_bytes, stderr_bytes = proc.communicate()
        stdout_text = stdout_bytes.decode("utf-8", errors="replace")
        stderr_text = stderr_bytes.decode("utf-8", errors="replace")
        raise AssertionError(f"[{lang}] rpc_infra_1 failed to complete\nSTDOUT: {stdout_text}\nSTDERR: {stderr_text}\nFRAMES: {frames_received}")
    assert infra_completed_data["runtime"]["domain"]["domain_id"] == "domain_alpha"
    assert infra_completed_data["observer"] == [{"topic": "msg_delivered", "target": "domain_alpha.ping"}]
    assert infra_completed_data["supervisor"]["success"] is True
    print(f"  [{lang}] Verified runtime, observer, and supervisor native client RPC roundtrips")

    proc.terminate()
    try:
        proc.wait(timeout=1.0)
    except Exception:
        proc.kill()
    out_file.close()
    in_file.close()

def main():
    print("Testing Python SDK Kernel Infrastructure methods...")
    run_scenario("py", PY_CODE)
    print("\nTesting Node.js SDK Kernel Infrastructure methods...")
    run_scenario("js", JS_CODE)
    print("\nALL MULTI-LANGUAGE SDK INFRASTRUCTURE FACILITIES 100% VERIFIED!")

if __name__ == "__main__":
    main()
