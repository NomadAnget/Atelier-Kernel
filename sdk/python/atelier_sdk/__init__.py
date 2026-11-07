"""Atelier SDK (Python) — 单层 `AtelierDomain`，全异步，15 项能力。

传输：内核通过两条单向命名端点（`ATELIER_IPC_IN` / `ATELIER_IPC_OUT`）交付；
本模块只负责协议与能力，不关心 Windows 命名管道 / Unix domain socket 的差异。
"""

__version__ = "0.5.1"

import asyncio
import io
import json
import os
import socket
import sys
import threading
import time
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


class _VfsBinaryWriter(io.BufferedWriter):
    """vfs:// 写模式的原生二进制句柄：close 时自动归档到存储枢纽。"""

    def __init__(self, raw, sdk, virtual_path, staging):
        super().__init__(raw)
        self._sdk = sdk
        self.virtual_path = virtual_path
        self.owner = sdk.domain
        self._staging = staging
        self._adopted = False

    def _adopt_once(self):
        if self._adopted:
            return
        self._adopted = True
        self._sdk._adopt(self.virtual_path, self._staging)

    def close(self):
        if self.closed:
            return
        try:
            super().close()
        finally:
            self._adopt_once()


class _VfsTextWriter(io.TextIOWrapper):
    """vfs:// 写模式的原生文本句柄：close 时自动归档到存储枢纽。"""

    def __init__(self, buffer, sdk, virtual_path, staging, **kwargs):
        super().__init__(buffer, **kwargs)
        self._sdk = sdk
        self.virtual_path = virtual_path
        self.owner = sdk.domain
        self._staging = staging
        self._adopted = False

    def _adopt_once(self):
        if self._adopted:
            return
        self._adopted = True
        self._sdk._adopt(self.virtual_path, self._staging)

    def close(self):
        if self.closed:
            return
        try:
            super().close()
        finally:
            self._adopt_once()


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
        self._vfs_files = {}       # virtual_path -> file object metadata
        self._staging_files = {}   # virtual_path -> staging path (not yet adopted)
        self._sync_pending = {}    # seq -> {"event", "message_id", "frame"}
        self._sync_waiters = {}    # message_id -> {"event", "message_id", "frame"}
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
    # ── C4 同步桥（close/open 这类同步路径需要等内核回执） ─────────────────────
    def _sync_request(self, target, message_type, params=None, data=None, timeout=15):
        if self._closed or self._transport_closed:
            raise DomainRequestError("local_backpressure", {"reason": "sdk closed"})
        params = self._encode_files(params or {})
        data = self._encode_files(data or {})
        with self._lock:
            seq = self._seq
            self._seq += 1
        waiter = {"event": threading.Event(), "message_id": None, "frame": None}
        self._sync_pending[seq] = waiter
        self._write(
            {
                "frame_type": "send",
                "seq": seq,
                "targets": [target],
                "message_type": message_type,
                "params": params or {},
                "data": data or {},
            }
        )
        if not waiter["event"].wait(timeout):
            self._sync_pending.pop(seq, None)
            raise DomainRequestError("sync_timeout", {"target": target})
        frame = waiter["frame"] or {}
        if frame.get("status") != "completed":
            raise DomainRequestError(frame.get("status"), frame.get("data"))
        return frame.get("data") or {}

    def _staging_dir(self):
        files_root = os.environ.get("ATELIER_FILES_ROOT")
        if not files_root:
            raise RuntimeError("C4: ATELIER_FILES_ROOT 未设置，无法定位暂存目录")
        files_root = os.path.abspath(files_root)
        project_root = os.path.dirname(files_root)
        owner = os.path.basename(files_root)  # 内核域 id 以文件根目录名为准
        staging = os.path.join(project_root, ".staging", owner)
        os.makedirs(staging, exist_ok=True)
        return staging

    def _new_staging(self):
        return os.path.join(self._staging_dir(), f"{uuid.uuid4().hex}.part")

    def _clean_stale_staging(self):
        """清理 24 小时前的暂存残留（崩溃/未关闭）。"""
        cutoff = time.time() - 24 * 3600
        for name in os.listdir(self._staging_dir()):
            path = os.path.join(self._staging_dir(), name)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
            except OSError:
                continue

    def _adopt(self, virtual_path, staging, *, raise_on_error=True):
        try:
            meta = self._sync_request(
                "kernel.storage",
                "StorageObjectPut",
                {"virtual_path": virtual_path, "staging_path": staging},
            )
        except Exception:
            self._staging_files[virtual_path] = staging
            if raise_on_error:
                raise
            return None
        self._staging_files.pop(virtual_path, None)
        self._vfs_files[virtual_path] = meta
        return meta

    def _resolve_read(self, virtual_path):
        meta = self._vfs_files.get(virtual_path)
        if meta is None:
            meta = self._sync_request(
                "kernel.storage", "StorageObjectGet", {"virtual_path": virtual_path}
            )
            self._vfs_files[virtual_path] = meta
        return meta

    def enable_file_interception(self, root=None):
        """vfs:// 写模式落到暂存区并在 close 时自动归档；读模式解析后返回原生句柄。

        只拦截 `vfs://` 前缀：非 vfs 路径原样放行，第三方库完全不受影响。
        写模式返回的是 io 原生类的子类（isinstance 兼容），业务无需调用任何非原生操作。
        """
        import builtins

        self._staging_dir()
        self._clean_stale_staging()
        original = builtins.open
        sdk = self
        prefix = "vfs://"

        def patched(path, mode="r", *args, **kwargs):
            if isinstance(path, int) or not isinstance(path, str) or not path.startswith(prefix):
                return original(path, mode, *args, **kwargs)
            virtual_path = path[len(prefix):].lstrip("/")
            if any(ch in mode for ch in "wax+"):
                if "+" in mode:
                    raise ValueError(f"vfs 写模式不支持读写混合: {mode}")
                return sdk._open_vfs_write(virtual_path, mode, kwargs)
            meta = sdk._resolve_read(virtual_path)
            handle = original(meta["physical_path"], mode, *args, **kwargs)
            return sdk._attach(handle, meta)

        builtins.open = patched
        try:
            io.open = patched
        except Exception:  # noqa: BLE001
            pass
        return self

    def _open_vfs_write(self, virtual_path, mode, kwargs):
        staging = self._new_staging()
        binary = "b" in mode
        file_mode = "wb" if ("w" in mode or "x" in mode) else "ab"
        raw = io.FileIO(staging, file_mode)
        if binary:
            return _VfsBinaryWriter(raw, self, virtual_path, staging)
        text_kwargs = {k: kwargs[k] for k in ("encoding", "errors", "newline") if k in kwargs}
        return _VfsTextWriter(io.BufferedWriter(raw), self, virtual_path, staging, **text_kwargs)

    @staticmethod
    def _attach(handle, meta):
        try:
            handle.virtual_path = meta.get("virtual_path")
            handle.owner = meta.get("owner")
            handle.file_meta = meta
        except Exception:  # noqa: BLE001 - 某些句柄不允许挂属性
            pass
        return handle

    async def put_object(self, virtual_path):
        """显式归档：把该路径的暂存文件立即交给内核（close 钩子已自动调用）。"""
        staging = self._staging_files.pop(virtual_path, None)
        if staging is None:
            return await self.get_object(virtual_path)
        meta = (
            await self.request(
                "kernel.storage",
                "StorageObjectPut",
                {"virtual_path": virtual_path, "staging_path": staging},
            )
        )["data"]
        self._vfs_files[virtual_path] = meta
        return meta

    async def get_object(self, virtual_path, owner=None):
        params = {"virtual_path": virtual_path}
        if owner:
            params["owner"] = owner
        meta = (await self.request("kernel.storage", "StorageObjectGet", params))["data"]
        if not owner:
            self._vfs_files[virtual_path] = meta
        return meta

    async def delete_object(self, virtual_path, owner=None):
        params = {"virtual_path": virtual_path}
        if owner:
            params["owner"] = owner
        result = (await self.request("kernel.storage", "StorageObjectDelete", params))["data"]
        self._vfs_files.pop(virtual_path, None)
        staging = self._staging_files.pop(virtual_path, None)
        if staging:
            try:
                os.remove(staging)
            except OSError:
                pass
        return result

    async def list_objects(self, owner=None):
        params = {"owner": owner} if owner else {}
        data = (await self.request("kernel.storage", "StorageObjectList", params))["data"]
        if not owner:
            for item in data.get("objects") or []:
                path = item.get("virtual_path")
                if path:
                    self._vfs_files[path] = item
        return data

    # ── 能力 13 配套：消息里的文件句柄编解码（实例级，无全局状态） ─────────────
    def _encode_files(self, value):
        virtual_path = getattr(value, "virtual_path", None)
        owner = getattr(value, "owner", None)
        if isinstance(virtual_path, str) and isinstance(owner, str):
            return {"__atelier_file__": {"owner": owner, "virtual_path": virtual_path}}
        if isinstance(value, dict):
            return {key: self._encode_files(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._encode_files(item) for item in value]
        return value

    async def _decode_files(self, value):
        if isinstance(value, dict):
            ref = value.get("__atelier_file__")
            if len(value) == 1 and isinstance(ref, dict):
                meta = await self.get_object(ref["virtual_path"], ref.get("owner"))
                handle = open(meta["physical_path"], "rb+")
                return self._attach(handle, meta)
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

    def _deliver_sync_receipt(self, frame):
        """同步桥回执在 reader 线程直接交付，避免 close/open 阻塞事件循环。"""
        message_id = frame.get("message_id")
        seq = frame.get("seq")
        status = frame.get("status")
        if not isinstance(message_id, str) or status not in {"sent", "received", "started", *TERMINAL}:
            return False
        if seq in self._sync_pending:
            waiter = self._sync_pending.pop(seq)
            waiter["message_id"] = message_id
            if status in TERMINAL:
                waiter["frame"] = frame
                waiter["event"].set()
            else:
                self._sync_waiters[message_id] = waiter
            return True
        waiter = self._sync_waiters.get(message_id)
        if waiter is not None:
            if status in TERMINAL:
                self._sync_waiters.pop(message_id, None)
                waiter["frame"] = frame
                waiter["event"].set()
            return True
        return False

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
                if frame.get("frame_type") == "receipt" and self._deliver_sync_receipt(frame):
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
        # 同步桥（close/open 路径）：不经过 asyncio future，直接唤醒等待线程。
        if seq in self._sync_pending:
            waiter = self._sync_pending.pop(seq)
            waiter["message_id"] = message_id
            if status in TERMINAL:
                waiter["frame"] = frame
                waiter["event"].set()
            else:
                self._sync_waiters[message_id] = waiter
            return
        waiter = self._sync_waiters.get(message_id)
        if waiter is not None:
            if status in TERMINAL:
                self._sync_waiters.pop(message_id, None)
                waiter["frame"] = frame
                waiter["event"].set()
            return
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
