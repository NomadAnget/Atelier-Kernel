"""Atelier SDK v2（Python）：单层、全异步、无遗留别名。

只暴露 `AtelierDomain`。15 项能力见 `sdk/v2/README.md`。
本文件先实现核心：传输、消息、回执、处理器、生命周期。
存储/查询/监督/订阅/C4/日志在后续层补齐。
"""

import asyncio
import json
import os
import sys
import threading
import uuid

TERMINAL = {"completed", "execution_failed", "target_invalid", "contract_invalid"}


class DomainRequestError(Exception):
    def __init__(self, status, data=None, message_id=None):
        super().__init__(status)
        self.status = status
        self.data = data
        self.message_id = message_id


class DomainExecutionFailed(Exception):
    def __init__(self, payload):
        super().__init__(str(payload))
        self.payload = payload


class AtelierDomain:
    """单层公开面。全异步，snake_case，无 `_async` 后缀。"""

    def __init__(self, domain, *, max_pending=1024, max_results=4096, result_ttl=30.0):
        self.domain = domain
        self._max_pending = max_pending
        self._max_results = max_results
        self._result_ttl = result_ttl

        self._handlers = {}
        self._pending = {}  # seq -> {"targets", "ids", "future"}
        self._results = {}  # message_id -> {"target", "future", "waiters", "evict"}
        self._seq = 1
        self._lock = threading.Lock()

        self._reader = None
        self._writer = None
        self._loop = None
        self._closed = False
        self._read_thread = None

        read_fd = os.environ.get("ATELIER_IPC_READ_FD")
        write_fd = os.environ.get("ATELIER_IPC_WRITE_FD")
        if read_fd and write_fd:
            self._reader = os.fdopen(int(read_fd), "r", encoding="utf-8", newline="\n")
            self._writer = os.fdopen(int(write_fd), "w", encoding="utf-8", newline="\n")

    # ── 能力 2：注册处理器 ──────────────────────────────────────────────
    def handler(self, name):
        def register(fn):
            if not isinstance(name, str) or not name or "." in name:
                raise ValueError("invalid handler name")
            self._handlers[name] = fn
            return fn

        return register

    # ── 能力 3/4：发消息 / 请求 ────────────────────────────────────────
    async def send(self, targets, message_type, params=None, data=None, *, timeout=30):
        names = [targets] if isinstance(targets, str) else list(targets)
        names = list(dict.fromkeys(names))
        if not names:
            raise ValueError("targets required")
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout

        while len(self._pending) + len(names) > self._max_pending:
            if deadline is not None and loop.time() >= deadline:
                raise DomainRequestError("busy")
            await asyncio.sleep(0.01)

        with self._lock:
            seq = self._seq
            self._seq += 1
        future = loop.create_future()
        self._pending[seq] = {"targets": names, "ids": {}, "future": future}
        try:
            self._write(
                {
                    "frame_type": "send",
                    "seq": seq,
                    "targets": names,
                    "message_type": message_type,
                    "params": params or {},
                    "data": data or {},
                }
            )
            ids = await asyncio.wait_for(asyncio.shield(future), None if timeout is None else timeout)
            return ids[0] if isinstance(targets, str) else ids
        finally:
            self._pending.pop(seq, None)

    async def request(self, target, message_type, params=None, data=None, *, timeout=30):
        message_id = await self.send(target, message_type, params, data, timeout=timeout)
        try:
            return await self.wait_result(message_id, timeout=timeout)
        finally:
            self.forget_result(message_id)

    # ── 能力 5：等待 / 遗忘 ────────────────────────────────────────────
    async def wait_result(self, message_id, *, timeout=30):
        entry = self._results.get(message_id)
        if entry is None:
            raise DomainRequestError("unknown_message", message_id=message_id)
        handle = entry.pop("evict", None)
        if handle:
            handle.cancel()
        entry["waiters"] += 1
        observed = False
        try:
            receipt = await asyncio.wait_for(
                asyncio.shield(entry["future"]), None if timeout is None else timeout
            )
            observed = True
            if receipt["status"] != "completed":
                raise DomainRequestError(receipt["status"], receipt.get("data"), message_id)
            return receipt
        finally:
            entry["waiters"] -= 1
            if observed and not entry["waiters"]:
                self._results.pop(message_id, None)

    async def wait_results(self, message_ids, *, timeout=30):
        return await asyncio.gather(*(self.wait_result(mid, timeout=timeout) for mid in message_ids))

    def forget_result(self, message_id):
        entry = self._results.get(message_id)
        if entry and entry["waiters"]:
            raise DomainRequestError("busy", message_id=message_id)
        if entry:
            handle = entry.pop("evict", None)
            if handle:
                handle.cancel()
        return self._results.pop(message_id, None) is not None

    # ── 能力 6：上报回执 ──────────────────────────────────────────────
    def report_receipt(self, message_id, target, status, data=None):
        self._write(
            {
                "frame_type": "receipt",
                "message_id": message_id,
                "target": target,
                "status": status,
                "data": data if data is not None else {},
            }
        )

    # ── 能力 8：本域存储（唯一实现 `_storage`，其余只转发）──────────────
    async def _storage(self, message_type, params, data=None):
        return (await self.request("kernel.storage", message_type, params, data or {}))["data"]

    async def storage_insert(self, record_id, category, data, upsert=True):
        return await self._storage(
            "StorageInsert",
            {"record_id": record_id, "category": category, "upsert": upsert},
            data,
        )

    async def storage_update(self, record_id, data, patch=True):
        return await self._storage("StorageUpdate", {"record_id": record_id, "patch": patch}, data)

    async def storage_delete(self, record_id=None, category=None):
        params = {}
        if record_id is not None:
            params["record_id"] = record_id
        if category is not None:
            params["category"] = category
        return await self._storage("StorageDelete", params)

    async def storage_query(self, domain=None, record_id=None, category=None, filter=None, limit=500):
        params = {"limit": limit}
        if domain is not None:
            params["domain"] = domain
        if record_id is not None:
            params["record_id"] = record_id
        if category is not None:
            params["category"] = category
        if filter is not None:
            params["filter"] = filter
        return await self._storage("StorageQuery", params)

    # ── 能力 9：查询（唯一实现 `_query`，其余只转发）────────────────────
    async def _query(self, target, message_type, params):
        return (await self.request(target, message_type, params or {}))["data"]

    async def query_logs(self, **params):
        return await self._query("kernel.logger", "QueryLogs", params)

    async def query_handlers(self, **params):
        return await self._query("kernel.registry", "QueryHandlers", params)

    async def query_runtime(self, **params):
        return await self._query("kernel.runtime", "QueryRuntime", params)

    async def query_observer(self, **params):
        return await self._query("kernel.observer", "QueryObserver", params)

    async def query_progress(self):
        return await self._query("kernel.logger", "QueryProgress", {})

    # ── 能力 10：监督（唯一实现 `_supervisor`，其余只转发）──────────────
    async def _supervisor(self, action, params):
        return (await self.request("kernel.supervisor", action, params))["data"]

    async def supervisor_start(self, domain, spec=None):
        params = {"domain": domain}
        if spec is not None:
            params["spec"] = spec
        return await self._supervisor("StartDomain", params)

    async def supervisor_stop(self, domain):
        return await self._supervisor("StopDomain", {"domain": domain})

    async def supervisor_restart(self, domain):
        return await self._supervisor("RestartDomain", {"domain": domain})

    async def supervisor_update(self, domain, spec=None, probe_handler=None):
        params = {"domain": domain}
        if spec is not None:
            params["spec"] = spec
        if probe_handler is not None:
            params["probe_handler"] = probe_handler
        return await self._supervisor("UpdateDomain", params)

    # ── 能力 11：订阅（唯一实现 `_subscribe`，其余只转发）───────────────
    async def _subscribe(self, kind, topic, enabled):
        message_type = ("Subscribe" if enabled else "Unsubscribe") + topic
        return (await self.request(f"kernel.{kind}", message_type, {}))["data"]

    async def subscribe_logs(self):
        return await self._subscribe("logger", "Logs", True)

    async def unsubscribe_logs(self):
        return await self._subscribe("logger", "Logs", False)

    async def subscribe_telemetry(self):
        return await self._subscribe("telemetry", "Telemetry", True)

    async def unsubscribe_telemetry(self):
        return await self._subscribe("telemetry", "Telemetry", False)

    async def subscribe_notify(self):
        return await self._subscribe("notify", "Notify", True)

    async def unsubscribe_notify(self):
        return await self._subscribe("notify", "Notify", False)

    async def subscribe_observer(self):
        return await self._subscribe("observer", "Observer", True)

    async def unsubscribe_observer(self):
        return await self._subscribe("observer", "Observer", False)

    # ── 能力 12：通知（唯一实现 `notify`）───────────────────────────────
    async def notify(self, level, message, title="", details=None):
        return (
            await self.request(
                "kernel.notify",
                title or "Notification",
                {"severity": level, "level": level},
                {"title": title, "message": message, "details": details or {}},
            )
        )["data"]

    # ── 能力 1：生命周期 ──────────────────────────────────────────────
    async def run_async(self, *, on_start=None, on_close=None):
        if self._reader is None or self._writer is None:
            raise RuntimeError("IPC pipes are not configured (ATELIER_IPC_READ_FD / WRITE_FD)")
        self._loop = asyncio.get_running_loop()
        self._read_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._read_thread.start()
        self._write(
            {
                "frame_type": "send",
                "seq": self._next_seq(),
                "targets": ["kernel.registry"],
                "message_type": "RegisterHandlers",
                "params": {"domain": self.domain, "handlers": list(self._handlers)},
                "data": {},
            }
        )
        if on_start is not None:
            result = on_start()
            if asyncio.iscoroutine(result):
                await result

    def run(self, **kwargs):
        asyncio.run(self.run_async(**kwargs))

    def shutdown(self):
        self._closed = True

    def _next_seq(self):
        with self._lock:
            seq = self._seq
            self._seq += 1
        return seq

    # ── 传输 ──────────────────────────────────────────────────────────
    def _write(self, frame):
        if self._closed:
            return
        line = json.dumps(frame, ensure_ascii=False)
        with self._lock:
            if self._writer is not None:
                self._writer.write(line + "\n")
                self._writer.flush()

    def _read_loop(self):
        for line in self._reader:
            line = line.strip()
            if not line:
                continue
            try:
                frame = json.loads(line)
            except Exception:
                continue
            if self._loop is not None:
                asyncio.run_coroutine_threadsafe(self._handle(frame), self._loop).result()

    async def _handle(self, frame):
        if frame.get("frame_type") == "receipt":
            self._receive_result(frame)
        else:
            await self._dispatch(frame)

    def _receive_result(self, frame):
        message_id = frame.get("message_id")
        target = frame.get("target")
        status = frame.get("status")
        if not isinstance(message_id, str) or status not in {"sent", "received", "started", *TERMINAL}:
            return
        entry = self._pending.get(frame.get("seq"))
        if entry and target not in entry["ids"] and target in entry["targets"]:
            entry["ids"][target] = message_id
            self._results[message_id] = {
                "target": target,
                "future": self._loop.create_future(),
                "waiters": 0,
                "evict": None,
            }
            if len(entry["ids"]) == len(entry["targets"]) and not entry["future"].done():
                entry["future"].set_result([entry["ids"][t] for t in entry["targets"]])
        result = self._results.get(message_id)
        if result and result["target"] == target and not result["future"].done() and status in TERMINAL:
            result["future"].set_result(frame)
            if not result["waiters"] and self._result_ttl > 0:
                result["evict"] = self._loop.call_later(
                    self._result_ttl, self._results.pop, message_id, None
                )

    async def _dispatch(self, frame):
        target = frame.get("target", "")
        name = target.split(".", 1)[-1]
        fn = self._handlers.get(name)
        if fn is None:
            self.report_receipt(
                frame.get("message_id"), target, "target_invalid", {"error": "unknown handler"}
            )
            return
        self.report_receipt(frame.get("message_id"), target, "started")
        try:
            result = fn(frame)
            if asyncio.iscoroutine(result):
                result = await result
            self.report_receipt(frame.get("message_id"), target, "completed", result)
        except DomainExecutionFailed as error:
            self.report_receipt(frame.get("message_id"), target, "execution_failed", error.payload)
        except Exception as error:  # noqa: BLE001
            self.report_receipt(
                frame.get("message_id"),
                target,
                "execution_failed",
                {"error": "handler_exception", "reason": str(error)},
            )
