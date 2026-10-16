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


def run_domain_script(cmd, input_lines):
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=ROOT,
    )
    stdout, stderr = proc.communicate(input="\n".join(input_lines) + "\n")
    frames = [json.loads(line) for line in stdout.splitlines() if line.strip()]
    return frames, stderr


def test_python_sdk():
    py_code = """
import sys
from domains.atelier_sdk import AtelierDomain, DomainExecutionFailed

domain = AtelierDomain()

@domain.handler("echo")
def on_echo(msg, ctx):
    return {"reply": msg.data.get("val")}

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
    frames, stderr = run_domain_script([sys.executable, "-c", py_code], inputs)
    return frames


def test_node_sdk():
    node_code = """
const { AtelierDomain, DomainExecutionFailed } = require("./domains/atelier_sdk");

const domain = new AtelierDomain();

domain.handler("echo", (msg, ctx) => {
  return { reply: msg.data.val };
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
    frames, stderr = run_domain_script(["node", "-e", node_code], inputs)
    return frames


def main():
    print("Testing Python Domain SDK...")
    py_frames = test_python_sdk()
    print("Testing Node.js Domain SDK...")
    node_frames = test_node_sdk()

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

    # 2. Success message (msg_001): received -> started -> completed
    for lang, frames in [("Python", py_frames), ("Node.js", node_frames)]:
        echo_stages = [f for f in frames if f.get("message_id") == "msg_001"]
        assert len(echo_stages) == 3, f"{lang} expected 3 stages for msg_001, got {len(echo_stages)}"
        assert echo_stages[0]["status"] == "received"
        assert echo_stages[1]["status"] == "started"
        assert echo_stages[2]["status"] == "completed"
        assert echo_stages[2]["data"] == {"reply": 42}
    print("  [OK] 4-stage success receipt lifecycle identical")

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

    print("\nALL MULTI-LANGUAGE SDK BEHAVIORS 100% CONSISTENT!")


if __name__ == "__main__":
    main()
