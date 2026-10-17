#!/usr/bin/env python3
"""
Cross-Language SDK Behavioral Consistency Verification Suite.

Tests that both Python (`atelier_sdk.py`) and Node.js (`atelier_sdk.js`)
exhibit identical wire protocol framing, lifecycle stages, error handling,
and Storage-as-Messaging CRUD behavior.
"""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHON_SDK_PATH = ROOT / "domains" / "atelier_sdk.py"
NODE_SDK_PATH = ROOT / "domains" / "atelier_sdk.js"

assert PYTHON_SDK_PATH.exists(), f"Missing {PYTHON_SDK_PATH}"
assert NODE_SDK_PATH.exists(), f"Missing {NODE_SDK_PATH}"


import os


def run_domain_script(cmd, input_lines):
    k2d_r, k2d_w = os.pipe()
    d2k_r, d2k_w = os.pipe()
    env = {**os.environ, "PYTHON": sys.executable, "PYTHON_BIN": sys.executable}
    kwargs = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
        "cwd": ROOT,
    }
    if os.name == "nt":
        import msvcrt
        os.set_inheritable(k2d_r, True)
        os.set_inheritable(d2k_w, True)
        env["ATELIER_IPC_READ_FD"] = str(msvcrt.get_osfhandle(k2d_r))
        env["ATELIER_IPC_WRITE_FD"] = str(msvcrt.get_osfhandle(d2k_w))
        env["ATELIER_IPC_HANDLE_MODE"] = "win32"
        kwargs["close_fds"] = False
    else:
        env["ATELIER_IPC_READ_FD"] = str(k2d_r)
        env["ATELIER_IPC_WRITE_FD"] = str(d2k_w)
        kwargs["pass_fds"] = (k2d_r, d2k_w)
    proc = subprocess.Popen(cmd, env=env, **kwargs)
    os.close(k2d_r)
    os.close(d2k_w)
    with os.fdopen(k2d_w, "w", encoding="utf-8") as w:
        w.write("\n".join(input_lines) + "\n")
    with os.fdopen(d2k_r, "r", encoding="utf-8") as r:
        ipc_out = r.read()
    stdout, stderr = proc.communicate()
    frames = [json.loads(line) for line in ipc_out.splitlines() if line.strip()]
    return frames, stdout, stderr


def test_python_sdk():
    py_code = """
import os, sys
from domains.atelier_sdk import AtelierDomain, DomainExecutionFailed

domain = AtelierDomain()

@domain.handler("echo")
def on_echo(msg, ctx):
    print("THIRD_PARTY_STDOUT_PRINT")
    os.write(1, b"RAW_FD1_STDOUT_WRITE\\n")
    domain.logger.info("processing echo %s", msg.data.get("val"), layer="TEST", component="demo")
    domain.logger.phase("INFO", "start", "starting calc", layer="ENGINES")
    domain.logger.progress("calculating", key="calc_1", frac=0.6, waiting=False, done=False)
    domain.logger.progress("calc done", key="calc_1", frac=1.0, done=True)
    return {
        "reply": msg.data.get("val"),
        "env_scrubbed": (
            "ATELIER_IPC_READ_FD" not in os.environ
            and "ATELIER_IPC_WRITE_FD" not in os.environ
        ),
    }

@domain.handler("fail_op")
def on_fail(msg):
    raise DomainExecutionFailed({"err_code": "ERR_CUSTOM", "detail": "custom error"})

@domain.handler("crash_op")
def on_crash(msg):
    raise RuntimeError("unhandled crash")

domain.run()
"""
    inputs = [
        # Normal delivery
        json.dumps({
            "frame_type": "deliver",
            "message_id": "msg_001",
            "source_domain": "tester",
            "target": "demo.echo",
            "targets": ["demo.echo"],
            "message_type": "EchoTest",
            "params": {},
            "data": {"val": 42},
        }),
        # Custom failure
        json.dumps({
            "frame_type": "deliver",
            "message_id": "msg_002",
            "source_domain": "tester",
            "target": "demo.fail_op",
            "targets": ["demo.fail_op"],
            "message_type": "FailTest",
            "params": {},
            "data": {},
        }),
        # Unhandled crash
        json.dumps({
            "frame_type": "deliver",
            "message_id": "msg_003",
            "source_domain": "tester",
            "target": "demo.crash_op",
            "targets": ["demo.crash_op"],
            "message_type": "CrashTest",
            "params": {},
            "data": {},
        }),
        # Unknown handler
        json.dumps({
            "frame_type": "deliver",
            "message_id": "msg_004",
            "source_domain": "tester",
            "target": "demo.unknown",
            "targets": ["demo.unknown"],
            "message_type": "UnknownTest",
            "params": {},
            "data": {},
        }),
    ]
    frames, stdout, _stderr = run_domain_script([sys.executable, "-c", py_code], inputs)
    return frames, stdout


def test_node_sdk():
    node_code = """
const fs = require("fs");
const { AtelierDomain, DomainExecutionFailed } = require("./domains/atelier_sdk");

const domain = new AtelierDomain();

domain.handler("echo", (msg, ctx) => {
  console.log("THIRD_PARTY_STDOUT_PRINT");
  fs.writeSync(1, "RAW_FD1_STDOUT_WRITE\\n");
  domain.logger.info("processing echo " + msg.data.val, { layer: "TEST", component: "demo" });
  domain.logger.phase("INFO", "start", "starting calc", { layer: "ENGINES" });
  domain.logger.progress("calculating", { key: "calc_1", frac: 0.6, waiting: false, done: false });
  domain.logger.progress("calc done", { key: "calc_1", frac: 1.0, done: true });
  return {
    reply: msg.data.val,
    env_scrubbed: (
      !("ATELIER_IPC_READ_FD" in process.env)
      && !("ATELIER_IPC_WRITE_FD" in process.env)
    ),
  };
});

domain.handler("fail_op", (msg) => {
  throw new DomainExecutionFailed({ err_code: "ERR_CUSTOM", detail: "custom error" });
});

domain.handler("crash_op", (msg) => {
  throw new Error("unhandled crash");
});

domain.run();
"""
    inputs = [
        # Normal delivery
        json.dumps({
            "frame_type": "deliver",
            "message_id": "msg_001",
            "source_domain": "tester",
            "target": "demo.echo",
            "targets": ["demo.echo"],
            "message_type": "EchoTest",
            "params": {},
            "data": {"val": 42},
        }),
        # Custom failure
        json.dumps({
            "frame_type": "deliver",
            "message_id": "msg_002",
            "source_domain": "tester",
            "target": "demo.fail_op",
            "targets": ["demo.fail_op"],
            "message_type": "FailTest",
            "params": {},
            "data": {},
        }),
        # Unhandled crash
        json.dumps({
            "frame_type": "deliver",
            "message_id": "msg_003",
            "source_domain": "tester",
            "target": "demo.crash_op",
            "targets": ["demo.crash_op"],
            "message_type": "CrashTest",
            "params": {},
            "data": {},
        }),
        # Unknown handler
        json.dumps({
            "frame_type": "deliver",
            "message_id": "msg_004",
            "source_domain": "tester",
            "target": "demo.unknown",
            "targets": ["demo.unknown"],
            "message_type": "UnknownTest",
            "params": {},
            "data": {},
        }),
    ]
    frames, stdout, _stderr = run_domain_script(["node", "-e", node_code], inputs)
    return frames, stdout


def test_no_fallback_without_ipc_fds():
    clean_env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("ATELIER_IPC_READ_FD", "ATELIER_IPC_WRITE_FD", "ATELIER_IPC_HANDLE_MODE")
    }
    py_proc = subprocess.run(
        [sys.executable, "-c", "from domains.atelier_sdk import AtelierDomain; AtelierDomain().run()"],
        cwd=ROOT,
        env=clean_env,
        capture_output=True,
        text=True,
    )
    node_proc = subprocess.run(
        ["node", "-e", "const {AtelierDomain}=require('./domains/atelier_sdk'); new AtelierDomain().runAsync().catch(e=>{console.error(e.message);process.exit(1);});"],
        cwd=ROOT,
        env=clean_env,
        capture_output=True,
        text=True,
    )
    assert py_proc.returncode != 0
    assert node_proc.returncode != 0
    expected_msg = "missing ATELIER_IPC_READ_FD / ATELIER_IPC_WRITE_FD: dedicated dual anonymous pipes are required"
    assert expected_msg in py_proc.stderr, py_proc.stderr
    assert expected_msg in node_proc.stderr, node_proc.stderr


def main():
    print("Testing Python Domain SDK...")
    py_frames, py_stdout = test_python_sdk()
    print("Testing Node.js Domain SDK...")
    node_frames, node_stdout = test_node_sdk()

    # 1. Startup registration frame
    py_reg = py_frames[0]
    node_reg = node_frames[0]
    assert py_reg["frame_type"] == "send"
    assert node_reg["frame_type"] == "send"
    assert py_reg["targets"] == ["kernel.registry"]
    assert node_reg["targets"] == ["kernel.registry"]
    assert py_reg["message_type"] == "RegisterHandlers"
    assert node_reg["message_type"] == "RegisterHandlers"
    assert set(py_reg["params"]["handlers"]) == {"echo", "fail_op", "crash_op"}
    assert set(node_reg["params"]["handlers"]) == {"echo", "fail_op", "crash_op"}
    print("  [OK] Registration handshake frame format identical")

    # 2. Success message (msg_001): received -> started -> completed + env scrubbing + stdout isolation
    for lang, frames, stdout in [("Python", py_frames, py_stdout), ("Node.js", node_frames, node_stdout)]:
        echo_stages = [f for f in frames if f.get("message_id") == "msg_001"]
        assert len(echo_stages) == 3, f"{lang} expected 3 stages for msg_001, got {len(echo_stages)}"
        assert echo_stages[0]["status"] == "received"
        assert echo_stages[1]["status"] == "started"
        assert echo_stages[2]["status"] == "completed"
        assert echo_stages[2]["data"] == {"reply": 42, "env_scrubbed": True}
        assert "THIRD_PARTY_STDOUT_PRINT" in stdout and "RAW_FD1_STDOUT_WRITE" in stdout, f"{lang} stdout missing raw output: {stdout!r}"
    print("  [OK] 4-stage success receipt lifecycle, stdout physical isolation & env FD scrubbing identical")

    # 3. DomainExecutionFailed (msg_002): received -> started -> execution_failed
    for lang, frames in [("Python", py_frames), ("Node.js", node_frames)]:
        fail_stages = [f for f in frames if f.get("message_id") == "msg_002"]
        assert len(fail_stages) == 3
        assert fail_stages[2]["status"] == "execution_failed"
        assert fail_stages[2]["data"] == {"err_code": "ERR_CUSTOM", "detail": "custom error"}
    print("  [OK] DomainExecutionFailed structured error payload identical")

    # 4. Unhandled crash (msg_003): received -> started -> execution_failed
    for lang, frames in [("Python", py_frames), ("Node.js", node_frames)]:
        crash_stages = [f for f in frames if f.get("message_id") == "msg_003"]
        assert len(crash_stages) == 3
        assert crash_stages[2]["status"] == "execution_failed"
        assert crash_stages[2]["data"]["error"] == "handler_exception"
        assert "unhandled crash" in crash_stages[2]["data"]["reason"]
    print("  [OK] Unhandled crash error framing identical")

    # 5. Unknown handler (msg_004): received -> started -> execution_failed
    for lang, frames in [("Python", py_frames), ("Node.js", node_frames)]:
        unknown_stages = [f for f in frames if f.get("message_id") == "msg_004"]
        assert len(unknown_stages) == 3
        assert unknown_stages[2]["status"] == "execution_failed"
        assert "Unknown handler" in unknown_stages[2]["data"]["error"]
    print("  [OK] Unknown handler rejection identical")

    # 6. Structured Multi-State Logging consistency (info, phase start, progress, progress done)
    for lang, frames in [("Python", py_frames), ("Node.js", node_frames)]:
        log_frames = [f for f in frames if f.get("frame_type") == "log"]
        assert len(log_frames) == 4, f"{lang} expected 4 log frames, got {len(log_frames)}"

        info_log = log_frames[0]["record"]
        assert info_log["level"] == "INFO"
        assert info_log["layer"] == "TEST"
        assert info_log["component"] == "demo"
        assert "processing echo 42" in info_log["message"]
        assert not info_log["ephemeral"]

        phase_log = log_frames[1]["record"]
        assert phase_log["level"] == "INFO"
        assert phase_log["phase"] == "start"
        assert phase_log["layer"] == "ENGINES"
        assert phase_log["message"] == "starting calc"

        prog_log = log_frames[2]["record"]
        assert prog_log["ephemeral"] is True
        assert prog_log["key"] == "calc_1"
        assert abs(prog_log["frac"] - 0.6) < 1e-5
        assert not prog_log["done"]

        done_log = log_frames[3]["record"]
        assert done_log["ephemeral"] is True
        assert done_log["key"] == "calc_1"
        assert abs(done_log["frac"] - 1.0) < 1e-5
        assert done_log["done"] is True
    print("  [OK] Structured multi-state logging framing (info/phase/progress) identical")

    # 7. Strict no-fallback check when ATELIER_IPC_READ_FD / ATELIER_IPC_WRITE_FD are absent
    test_no_fallback_without_ipc_fds()
    print("  [OK] Strict fail-fast (no stdin/stdout fallback) identical across Python and Node.js")

    print("\nALL MULTI-LANGUAGE SDK BEHAVIORS 100% CONSISTENT!")


if __name__ == "__main__":
    main()
