"""Atelier SDK (Python) — 单层 `AtelierDomain`，全异步，15 项能力。

传输：内核通过两条单向命名端点（`ATELIER_IPC_IN` / `ATELIER_IPC_OUT`）交付；
本模块只负责协议与能力，不关心 Windows 命名管道 / Unix domain socket 的差异。
"""

__version__ = "0.4.0"

import asyncio
import json
import os
import socket
import sys
import threading
import uuid


def _open_endpoint(path, mode):
    """打开一条单向内核端点。

    Windows 命名管道可直接当文件打开；Unix domain socket 不是文件，
    只能 `socket.connect()` 后用 `makefile()` 拿到同样的文件接口。
    """
    if sys.platform == "win32":
        return open(path, mode, buffering=0)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(path)
    return sock.makefile("wb" if "w" in mode else "rb", buffering=0)

TERMINAL = {"completed", "execution_failed", "target_invalid", "contract_invalid"}
# 已停止等待、但帧已写入内核的提交，其回执归属最多保留这么多条；超出按先进先出丢弃并计数。
_ORPHAN_LIMIT = 4096


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

    def __init__(self, domain):
        self.domain = domain
        # 显式超时后帧已经写进管道：内核仍可能回执，归属必须保留，不能丢弃内核发出的回执。
        self._orphan_limit = _ORPHAN_LIMIT

        self._handlers = {}
        self._descriptions = {}
        self._pending = {}  # seq -> {"targets", "ids", "future"}
        self._results = {}  # message_id -> {"target", "future", "waiters"}
        self._orphans = {}  # seq -> 与 _pending 同构，供已停止等待的提交继续认领回执
        self._transport_closed = False
        self._stats = {
            "dropped_orphan_receipts": 0,
            "local_backpressure": 0,
            "unmatched_receipts": 0,
            "malformed_frames": 0,
            "unknown_statuses": 0,
            "dropped_frames": 0,
        }
        self._events = {kind: [] for kind in ("receipt", "log", "notify", "observation", "telemetry")}
        self._logger = _Logger(self)
        self._namespace = None
        self._seq = 1
        self._lock = threading.Lock()

        self._read_pipe = None
        self._write_pipe = None
        self._loop = None
        self._done = None
        self._closed = False
        self._read_thread = None

        in_path = os.environ.pop("ATELIER_IPC_IN", None)
        out_path = os.environ.pop("ATELIER_IPC_OUT", None)
        if in_path and out_path:
            # 两条单向端点：in（内核→域，只读）、out（域→内核，只写）。
            # Windows 命名管道支持文件 API；Unix domain socket 不是文件，必须 connect()。
            self._read_pipe = _open_endpoint(in_path, "rb")
            self._write_pipe = _open_endpoint(out_path, "wb")

    # ── 能力 2：注册处理器 ──────────────────────────────────────────────
    def handler(self, name):
        def register(fn):
            if not isinstance(name, str) or not name or "." in name:
                raise ValueError("invalid handler name")
            self._handlers[name] = fn
            doc = (fn.__doc__ or "").strip()
            if doc:
                self._descriptions[name] = doc.splitlines()[0].strip()
            return fn

        return register

    # ── 能力 3/4：发消息 / 请求 ────────────────────────────────────────
    async def send(self, targets, message_type, params=None, data=None, *, timeout=None):
        if self._closed or self._transport_closed:
            # 端到端已关闭：消息不可能交给内核，必须显式拒绝而不是静默丢弃后等超时。
            self._stats["local_backpressure"] += 1
            raise DomainRequestError(
                "local_backpressure",
                {"submitted": False, "reason": "sdk closed" if self._closed else "transport closed"},
            )
        names = [targets] if isinstance(targets, str) else list(targets)
        names = list(dict.fromkeys(names))
        if not names:
            raise ValueError("targets required")
        params = self._encode_files(params or {})
        data = self._encode_files(data or {})
        loop = asyncio.get_running_loop()

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
        except (asyncio.TimeoutError, asyncio.CancelledError):
            # 帧已在管道里：本地停止等待，但内核的回执仍会到来，保留它的归属。
            entry = self._pending.get(seq)
            if entry is not None:
                self._remember_orphan(seq, entry)
            raise
        finally:
            self._pending.pop(seq, None)

    def _remember_orphan(self, seq, entry):
        if self._orphan_limit <= 0:
            self._stats["dropped_orphan_receipts"] += 1
            return
        self._orphans[seq] = entry
        while len(self._orphans) > self._orphan_limit:
            oldest = next(iter(self._orphans))
            self._orphans.pop(oldest, None)
            self._stats["dropped_orphan_receipts"] += 1

    async def request(self, target, message_type, params=None, data=None, *, timeout=None):
        message_id = await self.send(target, message_type, params, data, timeout=timeout)
        try:
            return await self.wait_result(message_id, timeout=timeout)
        finally:
            self.forget_result(message_id)

    # ── 能力 5：等待 / 遗忘 ────────────────────────────────────────────
    async def wait_result(self, message_id, *, timeout=None):
        entry = self._results.get(message_id)
        if entry is None:
            # 区分"从没收到过"和"收到过但本地条目已过期"，调用方才能判断是否自己错过了回执。
            raise DomainRequestError("unknown_message", {"message_id": message_id}, message_id)
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

    async def wait_results(self, message_ids, *, timeout=None):
        return await asyncio.gather(*(self.wait_result(mid, timeout=timeout) for mid in message_ids))

    def forget_result(self, message_id):
        entry = self._results.get(message_id)
        if entry and entry["waiters"]:
            raise DomainRequestError("result_in_use", message_id=message_id)
        return self._results.pop(message_id, None) is not None

    def stats(self):
        """本地队列与丢回执的计数；只描述 SDK 自己的内存，不代表内核状态。"""
        return {
            "pending": len(self._pending),
            "results": len(self._results),
            "orphans": len(self._orphans),
            "dropped_orphan_receipts": self._stats["dropped_orphan_receipts"],
            "local_backpressure": self._stats["local_backpressure"],
            "unmatched_receipts": self._stats["unmatched_receipts"],
            "malformed_frames": self._stats["malformed_frames"],
            "unknown_statuses": self._stats["unknown_statuses"],
            "dropped_frames": self._stats["dropped_frames"],
            "transport_closed": self._transport_closed,
        }

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
        result = (await self.request("kernel.storage", message_type, params, data or {}))["data"]
        return await self._decode_files(result)

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

    # ── 能力 7：事件回调（唯一实现 `_emit`）────────────────────────────
    def on_receipt(self, fn):
        self._events["receipt"].append(fn)
        return fn

    def on_log(self, fn):
        self._events["log"].append(fn)
        return fn

    def on_notify(self, fn):
        self._events["notify"].append(fn)
        return fn

    def on_observation(self, fn):
        self._events["observation"].append(fn)
        return fn

    def on_telemetry(self, fn):
        self._events["telemetry"].append(fn)
        return fn

    def _emit(self, kind, payload):
        for fn in list(self._events[kind]):
            try:
                result = fn(payload)
                if asyncio.iscoroutine(result):
                    asyncio.ensure_future(result)
            except Exception:  # noqa: BLE001
                pass

    # ── 能力 14：日志（唯一实例 `_logger`）─────────────────────────────
    @property
    def logger(self):
        return self._logger

    # ── 能力 15：端口 ─────────────────────────────────────────────────
    @property
    def bindings(self):
        raw = os.environ.get("ATELIER_PORT_BINDINGS")
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except Exception:  # noqa: BLE001
            return {}

    # ── 能力 13：C4 对象（唯一命名空间实例 `_namespace`）────────────────
    def enable_file_interception(self, root=None):
        import builtins

        base = root or os.environ.get("ATELIER_FILES_ROOT") or "."
        self._namespace = _FileNamespace(base, self._register_object)
        original = builtins.open
        namespace = self._namespace

        def patched(path, mode="r", *args, **kwargs):
            if isinstance(path, int) or not namespace.matches(path):
                return original(path, mode, *args, **kwargs)
            physical = namespace.resolve(path, any(ch in mode for ch in "wax+"))
            return original(physical, mode, *args, **kwargs)

        builtins.open = patched
        return self._namespace

    def _register_object(self, virtual, object_id):
        self._write(
            {
                "frame_type": "send",
                "seq": self._next_seq(),
                "targets": ["kernel.storage"],
                "message_type": "StorageObjectPut",
                "params": {"virtual_path": virtual, "object_id": object_id},
                "data": {},
            }
        )

    async def put_object(self, virtual_path):
        return (await self.request("kernel.storage", "StorageObjectPut", {"virtual_path": virtual_path}))["data"]

    async def get_object(self, object_id):
        return (await self.request("kernel.storage", "StorageObjectGet", {"object_id": object_id}))["data"]

    async def commit_object(self, object_id, size):
        """更新已有对象的大小（跨域写入允许；对象必须存在）。"""
        return (await self.request("kernel.storage", "StorageObjectCommit",
                                   {"object_id": object_id, "size": int(size)}))["data"]

    async def delete_object(self, object_id):
        """删除对象：删物理文件 + 索引行；跨域允许、幂等。"""
        return (await self.request("kernel.storage", "StorageObjectDelete", {"object_id": object_id}))["data"]

    async def list_objects(self, owner=None):
        """列出对象；owner 缺省为本域，给定时可跨域查询。"""
        params = {"owner": owner} if owner else {}
        return (await self.request("kernel.storage", "StorageObjectList", params))["data"]

    def file_object_id(self, path):
        return self._namespace.object_id(path) if self._namespace is not None else None

    async def refresh_file_index(self):
        return await self.list_objects()

    # ── 能力 13 配套：消息里的文件对象编解码（实例级，无全局状态）──────────
    def _encode_files(self, value):
        namespace = self._namespace
        if (
            namespace is not None
            and hasattr(value, "read")
            and isinstance(getattr(value, "name", None), str)
        ):
            object_id = namespace.object_id_for_physical(value.name)
            if object_id:
                return {"__atelier_file__": object_id}
        if isinstance(value, dict):
            return {key: self._encode_files(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._encode_files(item) for item in value]
        return value

    async def _decode_files(self, value):
        if isinstance(value, dict):
            if len(value) == 1 and isinstance(value.get("__atelier_file__"), str):
                object_id = value["__atelier_file__"]
                ref = await self.get_object(object_id)
                handle = open(ref["physical_path"], "rb+")
                # C4：物理文件是平铺 id（无扩展名），逻辑路径（含目录与后缀）在索引里。
                # 句柄同时带上两者，业务侧/引擎据此选格式，不必猜物理名。
                handle.object_id = object_id
                handle.virtual_path = ref.get("virtual_path")
                return handle
            return {key: await self._decode_files(item) for key, item in value.items()}
        if isinstance(value, list):
            return [await self._decode_files(item) for item in value]
        return value

    # ── 能力 1：生命周期 ──────────────────────────────────────────────
    async def run_async(self, *, on_start=None, on_close=None):
        if self._read_pipe is None or self._write_pipe is None:
            raise RuntimeError("IPC endpoints are not configured (ATELIER_IPC_IN / ATELIER_IPC_OUT)")
        self._loop = asyncio.get_running_loop()
        self._done = self._loop.create_future()
        self._read_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._read_thread.start()
        self._write(
            {
                "frame_type": "send",
                "seq": self._next_seq(),
                "targets": ["kernel.registry"],
                "message_type": "RegisterHandlers",
                "params": {"handlers": list(self._handlers), "descriptions": self._descriptions},
                "data": {},
            }
        )
        if on_start is not None:
            try:
                result = on_start()
                if asyncio.iscoroutine(result):
                    await result
            except Exception as error:  # noqa: BLE001
                self._write({"frame_type": "ready", "error": str(error)})
                raise
        if os.environ.get("ATELIER_MANAGED_READY") == "1":
            self._write({"frame_type": "ready"})
        try:
            await self._done
        finally:
            if on_close is not None:
                result = on_close()
                if asyncio.iscoroutine(result):
                    await result

    def shutdown(self):
        self._closed = True
        if self._done is not None and not self._done.done() and self._loop is not None:
            self._loop.call_soon_threadsafe(self._done.set_result, True)

    def _next_seq(self):
        with self._lock:
            seq = self._seq
            self._seq += 1
        return seq

    # ── 传输 ──────────────────────────────────────────────────────────
    def _write(self, frame):
        if self._closed or self._write_pipe is None:
            self._stats["dropped_frames"] += 1
            return
        line = json.dumps(frame, ensure_ascii=False) + "\n"
        with self._lock:
            self._write_pipe.write(line.encode("utf-8"))

    def _read_loop(self):
        buffer = b""
        reason = "read loop ended"
        while not self._closed:
            try:
                chunk = os.read(self._read_pipe.fileno(), 8192)
            except Exception as error:  # noqa: BLE001
                reason = f"read failed: {error}"
                break
            if not chunk:
                reason = "read stream closed"
                break
            buffer += chunk
            while b"\n" in buffer:
                raw, buffer = buffer.split(b"\n", 1)
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    frame = json.loads(raw.decode("utf-8"))
                except Exception:  # noqa: BLE001
                    self._stats["malformed_frames"] += 1
                    continue
                if self._loop is not None:
                    # 不阻塞读线程：处理器里可能再发 request，需要读线程继续处理回执。
                    asyncio.run_coroutine_threadsafe(self._handle(frame), self._loop)
        self._on_transport_closed(reason)

    # 传输死亡是内核已经给出结论的场景（在途消息会被内核收敛为 ExecutionFailed）。
    # SDK 不能沉默：用本地 transport_closed 终结所有等待，绝不让调用方挂到自己的超时。
    def _on_transport_closed(self, reason):
        if self._transport_closed:
            return
        self._transport_closed = True
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        loop.call_soon_threadsafe(self._fail_pending_on_transport_close, reason)

    def _fail_pending_on_transport_close(self, reason):
        error = DomainRequestError("transport_closed", {"submitted": True, "reason": reason})
        for entry in list(self._pending.values()):
            if not entry["future"].done():
                entry["future"].set_exception(error)
        self._pending.clear()
        self._orphans.clear()
        for message_id, result in list(self._results.items()):
            if result["waiters"] and not result["future"].done():
                result["future"].set_exception(error)
            self._results.pop(message_id, None)

    async def _handle(self, frame):
        if frame.get("frame_type") == "receipt":
            self._receive_result(frame)
            self._emit("receipt", frame)
            return
        kind = SYSTEM_EVENTS.get(frame.get("message_type"))
        if kind is not None:
            self._emit(kind, frame.get("data") or {})
            return
        await self._dispatch(frame)

    def _receive_result(self, frame):
        message_id = frame.get("message_id")
        target = frame.get("target")
        status = frame.get("status")
        if not isinstance(message_id, str):
            return
        if status not in {"sent", "received", "started", *TERMINAL}:
            # 内核给出了 SDK 不认识的状态：计入可观测计数，绝不静默当作非终态。
            self._stats["unknown_statuses"] += 1
            return
        seq = frame.get("seq")
        # 本地已停止等待的提交同样要认出归属：内核已经发出的回执不能被丢弃。
        entry = self._pending.get(seq) or self._orphans.get(seq)
        claimed = False
        if entry and target not in entry["ids"] and target in entry["targets"]:
            entry["ids"][target] = message_id
            self._results[message_id] = {
                "target": target,
                "future": self._loop.create_future(),
                "waiters": 0,
            }
            claimed = True
            if len(entry["ids"]) == len(entry["targets"]):
                if not entry["future"].done():
                    entry["future"].set_result([entry["ids"][t] for t in entry["targets"]])
                self._orphans.pop(seq, None)
        result = self._results.get(message_id)
        if result and result["target"] == target and not result["future"].done() and status in TERMINAL:
            # 结果不自动过期：取走或用 forget_result 释放，寿命由域自己决定。
            result["future"].set_result(frame)
        if not claimed and not result:
            # 回执找不到任何归属（超出孤儿窗口、或来自其他代际）：计数，不静默。
            self._stats["unmatched_receipts"] += 1

    async def _dispatch(self, frame):
        target = frame.get("target", "")
        name = target.split(".", 1)[-1]
        fn = self._handlers.get(name)
        if fn is None:
            self.report_receipt(
                frame.get("message_id"), target, "target_invalid", {"error": "unknown handler"}
            )
            return
        self.report_receipt(frame.get("message_id"), target, "received")
        self.report_receipt(frame.get("message_id"), target, "started")
        try:
            frame["data"] = await self._decode_files(frame.get("data"))
            result = fn(_Message(frame))
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


SYSTEM_EVENTS = {
    "LogEvent": "log",
    "NotifyEvent": "notify",
    "ObservationEvent": "observation",
    "TelemetryEvent": "telemetry",
}


class _Message:
    """处理器收到的消息视图（只读）。"""

    def __init__(self, frame):
        self._frame = frame

    @property
    def message_id(self):
        return self._frame.get("message_id")

    @property
    def target(self):
        return self._frame.get("target", "")

    @property
    def message_type(self):
        return self._frame.get("message_type", "")

    @property
    def source_domain(self):
        return self._frame.get("source_domain", "")

    @property
    def params(self):
        return self._frame.get("params") or {}

    @property
    def data(self):
        return self._frame.get("data") or {}


class _Logger:
    """唯一的日志实现：把记录发给 `kernel.logger`。"""

    def __init__(self, domain):
        self._domain = domain

    def _log(self, level, message, **fields):
        # 日志走**专用 log 帧**，不是 `send`：它不分配消息身份、不产生四阶段回执，
        # 由内核直接注入 hub_logs / 活动进度表 / 订阅流。
        # `phase`、`ephemeral`、`key`、`frac`、`waiting`、`done`、`state`、`console_only`
        # 都是 LogRecord 字段，直接作为关键字参数传进来即可。
        record = {
            "level": level,
            "layer": fields.pop("layer", "DOMAIN"),
            "component": fields.pop("component", self._domain.domain),
            "message": message,
            **fields,
        }
        self._domain._write({"frame_type": "log", "record": record})

    def debug(self, message, **fields):
        self._log("debug", message, **fields)

    def info(self, message, **fields):
        self._log("info", message, **fields)

    def warn(self, message, **fields):
        self._log("warn", message, **fields)

    def error(self, message, **fields):
        self._log("error", message, **fields)


class _FileNamespace:
    """唯一的 C4 命名空间实现：虚拟路径 → `<域根>/<uuid>` 平铺文件。"""

    def __init__(self, root, submit, prefix="vfs://"):
        self.root = os.path.abspath(root)
        self._submit = submit
        self.prefix = prefix
        self._cache = {}
        self._by_physical = {}

    def matches(self, path):
        return isinstance(path, str) and path.startswith(self.prefix)

    def normalize(self, path):
        raw = str(path)[len(self.prefix) :].replace("\\", "/").lstrip("/")
        parts = [part for part in raw.split("/") if part not in ("", ".", "..")]
        return "/".join(parts) or "unnamed"

    def resolve(self, path, for_write):
        virtual = self.normalize(path)
        physical = self._cache.get(virtual)
        if physical is None:
            if not for_write:
                raise FileNotFoundError("virtual file not found: " + str(path))
            object_id = uuid.uuid4().hex
            physical = os.path.join(self.root, object_id)
            os.makedirs(self.root, exist_ok=True)
            self._cache[virtual] = physical
            self._by_physical[os.path.abspath(physical)] = object_id
            self._submit(virtual, object_id)
        return physical

    def object_id(self, path):
        physical = self._cache.get(self.normalize(path))
        return self._by_physical.get(os.path.abspath(physical)) if physical else None

    def object_id_for_physical(self, physical):
        try:
            return self._by_physical.get(os.path.abspath(physical))
        except Exception:  # noqa: BLE001
            return None
