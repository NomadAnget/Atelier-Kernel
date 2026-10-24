"""
Atelier Kernel Official Python Domain SDK (`atelier_sdk`).

Encapsulates OS Stdio JSON-Lines pipe framing, sequence numbering, handler
registration (`kernel.registry`), 4-stage receipt reporting (`Received` ->
`Started` -> `Completed` / `ExecutionFailed`), and `kernel.storage` CRUD helpers
so business domains only need to import `AtelierDomain` and declare handlers.
"""
from ._runtime import AsyncRuntime
from ._errors import DomainRequestError, DomainExecutionFailed
import asyncio
import inspect
import json
import logging
import math
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional

__version__ = "0.1.13"

KERNEL_REGISTRY_TARGET = "kernel.registry"
KERNEL_STORAGE_TARGET = "kernel.storage"
KERNEL_LOGGER_TARGET = "kernel.logger"
KERNEL_TELEMETRY_TARGET = "kernel.telemetry"
KERNEL_NOTIFY_TARGET = "kernel.notify"
KERNEL_RUNTIME_TARGET = "kernel.runtime"
KERNEL_OBSERVER_TARGET = "kernel.observer"
KERNEL_SUPERVISOR_TARGET = "kernel.supervisor"

MSG_REGISTER_HANDLERS = "RegisterHandlers"
MSG_UNREGISTER_HANDLERS = "UnregisterHandlers"
MSG_QUERY_HANDLERS = "QueryHandlers"
MSG_QUERY_LOGS = "QueryLogs"
MSG_QUERY_PROGRESS = "QueryProgress"
MSG_SUBSCRIBE_LOGS = "SubscribeLogs"
MSG_UNSUBSCRIBE_LOGS = "UnsubscribeLogs"
MSG_SUBSCRIBE_TELEMETRY = "SubscribeTelemetry"
MSG_UNSUBSCRIBE_TELEMETRY = "UnsubscribeTelemetry"
MSG_SUBSCRIBE_NOTIFY = "SubscribeNotify"
MSG_UNSUBSCRIBE_NOTIFY = "UnsubscribeNotify"
MSG_QUERY_RUNTIME = "QueryRuntime"
MSG_QUERY_DOMAIN_ASSEMBLY = "QueryDomainAssembly"
MSG_QUERY_OBSERVER = "QueryObserver"
MSG_SUBSCRIBE_OBSERVER = "SubscribeObserver"
MSG_UNSUBSCRIBE_OBSERVER = "UnsubscribeObserver"
MSG_START_DOMAIN = "StartDomain"
MSG_STOP_DOMAIN = "StopDomain"
MSG_RESTART_DOMAIN = "RestartDomain"
MSG_UPDATE_DOMAIN = "UpdateDomain"
MSG_SYSTEM_LOG_EVENT = "LogEvent"
MSG_SYSTEM_TELEMETRY_EVENT = "TelemetryEvent"
MSG_SYSTEM_NOTIFY_EVENT = "NotifyEvent"
MSG_SYSTEM_OBSERVER_EVENT = "ObservationEvent"
MSG_STORAGE_INSERT = "StorageInsert"
MSG_STORAGE_QUERY = "StorageQuery"
MSG_STORAGE_UPDATE = "StorageUpdate"
MSG_STORAGE_DELETE = "StorageDelete"
MSG_DELIVERY_DURABLE = "Durable"
MSG_DELIVERY_VOLATILE = "Volatile"
MSG_DELIVERY_EPHEMERAL = "Ephemeral"

__all__ = [
    "AtelierDomain",
    "DeliveryMessage",
    "DomainExecutionFailed",
    "DomainLogger",
    "DomainObserverClient",
    "DomainRegistryClient",
    "DomainRequestError",
    "DomainRuntimeClient",
    "DomainStorageClient",
    "DomainSupervisorClient",
    "DomainTelemetry",
    "KERNEL_LOGGER_TARGET",
    "KERNEL_NOTIFY_TARGET",
    "KERNEL_OBSERVER_TARGET",
    "KERNEL_REGISTRY_TARGET",
    "KERNEL_RUNTIME_TARGET",
    "KERNEL_STORAGE_TARGET",
    "KERNEL_SUPERVISOR_TARGET",
    "KERNEL_TELEMETRY_TARGET",
    "MSG_DELIVERY_DURABLE",
    "MSG_DELIVERY_EPHEMERAL",
    "MSG_DELIVERY_VOLATILE",
    "MSG_QUERY_DOMAIN_ASSEMBLY",
    "MSG_QUERY_HANDLERS",
    "MSG_QUERY_LOGS",
    "MSG_QUERY_OBSERVER",
    "MSG_QUERY_PROGRESS",
    "MSG_QUERY_RUNTIME",
    "MSG_REGISTER_HANDLERS",
    "MSG_RESTART_DOMAIN",
    "MSG_START_DOMAIN",
    "MSG_STOP_DOMAIN",
    "MSG_STORAGE_DELETE",
    "MSG_STORAGE_INSERT",
    "MSG_STORAGE_QUERY",
    "MSG_STORAGE_UPDATE",
    "MSG_SUBSCRIBE_LOGS",
    "MSG_SUBSCRIBE_NOTIFY",
    "MSG_SUBSCRIBE_OBSERVER",
    "MSG_SUBSCRIBE_TELEMETRY",
    "MSG_SYSTEM_LOG_EVENT",
    "MSG_SYSTEM_NOTIFY_EVENT",
    "MSG_SYSTEM_OBSERVER_EVENT",
    "MSG_SYSTEM_TELEMETRY_EVENT",
    "MSG_UNREGISTER_HANDLERS",
    "MSG_UNSUBSCRIBE_LOGS",
    "MSG_UNSUBSCRIBE_NOTIFY",
    "MSG_UNSUBSCRIBE_OBSERVER",
    "MSG_UNSUBSCRIBE_TELEMETRY",
    "MSG_UPDATE_DOMAIN",
    "__version__",
]


class DeliveryMessage(dict):
    """Two-layer message wrapper with attribute and dictionary access."""
    @property
    def logical_id(self) -> str:
        return self.get("logical_id", "")

    @property
    def message_id(self) -> str:
        return self["message_id"]

    @property
    def source_domain(self) -> str:
        return self["source_domain"]

    @property
    def target(self) -> str:
        return self["target"]

    @property
    def targets(self) -> List[str]:
        return self.get("targets") or [self.target]

    @property
    def message_type(self) -> str:
        return self.get("message_type", "")

    @property
    def params(self) -> Dict[str, Any]:
        return self.get("params") or {}

    @property
    def data(self) -> Dict[str, Any]:
        return self.get("data", {})


class DomainLogger:
    """
    Unified structured logger for Atelier Domain.
    Emits structured log frames over the domain's stdout pipe to the Kernel.

    Supports 3 core operational patterns:
    1. Hierarchical logs: debug, info, warning, error, critical with `phase` execution pairing.
    2. Ephemeral progress: progress(msg, key=..., frac=..., waiting=..., done=...).
    3. Console only: console_only=True (suppressed from hub_logs and SSE).
    """

    def __init__(
        self,
        domain: "AtelierDomain",
        default_layer: str = "DOMAIN",
        default_component: str = "",
    ):
        self._domain = domain
        self.default_layer = default_layer
        self.default_component = default_component

    def _log(
        self,
        level: str,
        msg: str,
        *args: Any,
        layer: str = "",
        component: str = "",
        phase: str = "",
        console_only: bool = False,
        exc_info: Any = None,
    ) -> None:
        try:
            text = (msg % args) if args else str(msg)
        except Exception:
            text = str(msg)

        if exc_info:
            import traceback

            if exc_info is True:
                text += "\n" + traceback.format_exc()
            elif isinstance(exc_info, tuple):
                text += "\n" + "".join(traceback.format_exception(*exc_info))
            elif isinstance(exc_info, BaseException):
                text += f"\n{type(exc_info).__name__}: {exc_info}"

        record: Dict[str, Any] = {
            "ts": time.time(),
            "level": level.upper(),
            "layer": layer or self.default_layer,
            "component": component or self.default_component,
            "message": text,
            "phase": phase or "",
            "console_only": bool(console_only),
            "ephemeral": False,
        }
        self._domain._emit_log(record)

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("DEBUG", msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("INFO", msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("WARNING", msg, *args, **kwargs)

    def warn(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("WARNING", msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("ERROR", msg, *args, **kwargs)

    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("CRITICAL", msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("exc_info", True)
        self._log("ERROR", msg, *args, **kwargs)

    def phase(self, level: str, phase_name: str, msg: str, *args: Any, **kwargs: Any) -> None:
        """Logs a lifecycle start/finish execution phase pair."""
        kwargs["phase"] = phase_name
        self._log(level, msg, *args, **kwargs)

    def progress(
        self,
        msg: str,
        *,
        key: str = "",
        frac: Optional[float] = None,
        waiting: bool = False,
        done: bool = False,
        state: Optional[str] = None,
        layer: str = "",
        component: str = "",
    ) -> None:
        """
        Ephemeral real-time progress update.
        Dispatched in-memory to Web UI / SSE observers; never enters SQLite hub_logs.
        """
        if state is None:
            state = "completed" if done else "running"
        if state not in ("running", "completed", "failed", "cancelled"):
            raise ValueError("invalid progress state")
        done = state != "running"
        record: Dict[str, Any] = {
            "ts": time.time(),
            "level": "INFO",
            "layer": layer or self.default_layer,
            "component": component or self.default_component,
            "message": str(msg),
            "phase": "",
            "console_only": False,
            "ephemeral": True,
            "key": key or None,
            "frac": frac,
            "waiting": bool(waiting) and not done,
            "state": state,
            "done": bool(done),
        }
        self._domain._emit_log(record)

    def query_logs(self, *args: Any, **kwargs: Any) -> Any:
        """Queries persisted logs from SQLite hub_logs via kernel.logger."""
        return self._domain.query_logs(*args, **kwargs)

    async def query_logs_async(self, *args: Any, **kwargs: Any) -> Any:
        """Asynchronously queries persisted logs from SQLite hub_logs via kernel.logger."""
        return await self._domain.query_logs_async(*args, **kwargs)

    def query_progress(self, *args: Any, **kwargs: Any) -> Any:
        """Queries active in-flight task progress indicators via kernel.logger."""
        return self._domain.query_progress(*args, **kwargs)

    async def query_progress_async(self, *args: Any, **kwargs: Any) -> Any:
        """Asynchronously queries active task progress indicators via kernel.logger."""
        return await self._domain.query_progress_async(*args, **kwargs)


class _StdlibLoggingHandler(logging.Handler):
    """Bridges standard Python `logging` into AtelierDomain's structured logger."""

    def __init__(self, domain_logger: DomainLogger):
        super().__init__()
        self._logger = domain_logger

    def emit(self, record: logging.LogRecord) -> None:
        layer = getattr(record, "layer", "") or ""
        comp = getattr(record, "component", "") or record.name
        phase = getattr(record, "phase", "") or ""
        console_only = getattr(record, "console_only", False)
        msg = record.getMessage()
        self._logger._log(
            record.levelname,
            msg,
            layer=layer,
            component=comp,
            phase=phase,
            console_only=console_only,
            exc_info=record.exc_info,
        )


class DomainTelemetry:
    """Native telemetry facility for reporting metrics to the Kernel."""

    def __init__(self, domain: "AtelierDomain"):
        self._domain = domain

    def emit(self, metric_name: str, value: Any, tags: Optional[Dict[str, Any]] = None) -> int:
        return self._domain._submit(
            [KERNEL_TELEMETRY_TARGET],
            metric_name,
            params={"tags": tags or {}},
            data={"name": metric_name, "value": value, "tags": tags or {}},
        )

    async def emit_async(
        self,
        metric_name: str,
        value: Any,
        tags: Optional[Dict[str, Any]] = None,
        timeout: float = 30,
    ) -> Dict[str, Any]:
        return (await self._domain.request(
            KERNEL_TELEMETRY_TARGET,
            metric_name,
            {"tags": tags or {}},
            {"name": metric_name, "value": value, "tags": tags or {}},
            timeout,
        ))["data"]


class DomainStorageClient:
    """Native storage facility providing safe per-domain CRUD operations."""

    def __init__(self, domain: "AtelierDomain"):
        self._domain = domain

    async def insert(self, record_id: str, category: str, data: Dict[str, Any], upsert: bool = True, timeout: float = 30):
        return await self._domain.storage_insert_async(record_id, category, data, upsert=upsert, timeout=timeout)

    async def update(self, record_id: str, data: Dict[str, Any], patch: bool = True, timeout: float = 30):
        return await self._domain.storage_update_async(record_id, data, patch=patch, timeout=timeout)

    async def delete(self, record_id: Optional[str] = None, category: Optional[str] = None, timeout: float = 30):
        return await self._domain.storage_delete_async(record_id=record_id, category=category, timeout=timeout)

    async def query(self, domain: Optional[str] = None, record_id: Optional[str] = None, category: Optional[str] = None, filter: Optional[Dict[str, Any]] = None, filter_map: Optional[Dict[str, Any]] = None, limit: int = 500, timeout: float = 30):
        return await self._domain.storage_query_async(domain=domain, record_id=record_id, category=category, filter=filter, filter_map=filter_map, limit=limit, timeout=timeout)


class DomainRegistryClient:
    """Native registry facility for business service discovery."""

    def __init__(self, domain: "AtelierDomain"):
        self._domain = domain

    async def query_handlers(self, domain: Optional[str] = None, target: Optional[str] = None, handler: Optional[str] = None, include_inactive: bool = False, timeout: float = 30):
        return await self._domain.query_handlers_async(domain=domain, target=target, handler=handler, include_inactive=include_inactive, timeout=timeout)

    async def get_description(self, target: str, timeout: float = 30):
        return await self._domain.get_handler_description_async(target, timeout=timeout)


class DomainRuntimeClient:
    """Native runtime infrastructure facility for querying domain assembly."""

    def __init__(self, domain: "AtelierDomain"):
        self._domain = domain

    async def query(self, domain: Optional[str] = None, timeout: float = 30) -> Dict[str, Any]:
        return await self._domain.query_runtime_async(domain=domain, timeout=timeout)

    async def list_domains(self, timeout: float = 30) -> List[Dict[str, Any]]:
        res = await self.query(timeout=timeout)
        return res.get("domains") or []

    async def get_domain(self, domain_id: str, timeout: float = 30) -> Optional[Dict[str, Any]]:
        res = await self.query(domain=domain_id, timeout=timeout)
        return res.get("domain")


class DomainObserverClient:
    """Native observer infrastructure facility for querying snapshots and managing subscriptions."""

    def __init__(self, domain: "AtelierDomain"):
        self._domain = domain

    async def query(
        self,
        limit: int = 100,
        message_id: Optional[str] = None,
        target: Optional[str] = None,
        source_domain: Optional[str] = None,
        timeout: float = 30,
    ) -> List[Dict[str, Any]]:
        res = await self._domain.query_observer_async(
            limit=limit,
            message_id=message_id,
            target=target,
            source_domain=source_domain,
            timeout=timeout,
        )
        return res.get("events") or []

    async def subscribe(self, timeout: float = 30) -> Dict[str, Any]:
        return await self._domain.subscribe_observer_async(timeout=timeout)

    async def unsubscribe(self, timeout: float = 30) -> Dict[str, Any]:
        return await self._domain.unsubscribe_observer_async(timeout=timeout)


class DomainSupervisorClient:
    """Native supervisor infrastructure facility for process lifecycle and smooth rolling updates."""

    def __init__(self, domain: "AtelierDomain"):
        self._domain = domain

    async def start(self, domain: str, spec: Optional[Dict[str, Any]] = None, timeout: float = 30) -> Dict[str, Any]:
        return await self._domain.supervisor_start_async(domain=domain, spec=spec, timeout=timeout)

    async def stop(self, domain: str, timeout: float = 30) -> Dict[str, Any]:
        return await self._domain.supervisor_stop_async(domain=domain, timeout=timeout)

    async def restart(self, domain: str, timeout: float = 30) -> Dict[str, Any]:
        return await self._domain.supervisor_restart_async(domain=domain, timeout=timeout)

    async def update(
        self,
        domain: str,
        spec: Optional[Dict[str, Any]] = None,
        probe_handler: Optional[str] = None,
        timeout_ms: int = 5000,
        timeout: float = 30,
    ) -> Dict[str, Any]:
        return await self._domain.supervisor_update_async(
            domain=domain,
            spec=spec,
            probe_handler=probe_handler,
            timeout_ms=timeout_ms,
            timeout=timeout,
        )


class AtelierDomain(AsyncRuntime):
    """High-level Python Domain SDK for Atelier Kernel."""

    def __init__(
        self,
        domain: str = "",
        stage_delay_sec: float = 0.0,
        **options,
    ):
        self._domain_name = domain
        self._seq_counter = 1
        self._write_lock = threading.Lock()
        self._handlers: Dict[str, Callable[..., Any]] = {}
        self._handler_docs: Dict[str, str] = {}
        self._receipt_callbacks: List[Callable[[Dict[str, Any]], None]] = []
        self._log_listeners: List[Any] = []
        self._telemetry_listeners: List[Any] = []
        self._notify_listeners: List[Any] = []
        self._observation_listeners: List[Any] = []
        self._pending = {}
        self._closed = False
        self._async_loop = None
        self._inbox = None
        self._configure(**options)
        self._stage_delay_sec = stage_delay_sec
        self.logger = DomainLogger(
            self,
            default_layer="DOMAIN",
            default_component=domain,
        )
        self.telemetry = DomainTelemetry(self)
        self.storage = DomainStorageClient(self)
        self.registry = DomainRegistryClient(self)
        self.runtime = DomainRuntimeClient(self)
        self.observer = DomainObserverClient(self)
        self.supervisor = DomainSupervisorClient(self)

    def handler(self, name: str):
        """Registers a handler for `<domain>.<name>`. Automatically extracts docstring or leading `#` comments as handler description."""
        def decorator(func):
            return self._register_handler(name, func)
        return decorator

    def on(self, name: str):
        """Alias for `@domain.handler(name)`."""
        return self.handler(name)

    def on_receipt(self, func: Callable[[Dict[str, Any]], None]):
        """Optional decorator to observe receipts returned by the Kernel for outbound messages."""
        self._receipt_callbacks.append(func)
        return func

    def on_log(self, func: Optional[Callable] = None, *, level: Optional[str] = None, source_domain: Optional[str] = None):
        """
        Registers a callback or decorator to listen to the real-time system log stream.
        Can filter by minimum `level` (e.g. 'WARNING') and/or `source_domain`.
        """
        def decorator(fn):
            self._log_listeners.append((fn, level, source_domain))
            if getattr(self, "_state", None) == "open":
                params = {}
                if level:
                    params["level"] = level
                if source_domain:
                    params["source_domain"] = source_domain
                self._submit([KERNEL_LOGGER_TARGET], MSG_SUBSCRIBE_LOGS, params=params, data={})
            return fn
        if func is not None:
            return decorator(func)
        return decorator

    def on_telemetry(self, func: Optional[Callable] = None):
        """Registers a callback or decorator to listen to the real-time telemetry stream."""
        def decorator(fn):
            self._telemetry_listeners.append(fn)
            if getattr(self, "_state", None) == "open":
                self._submit([KERNEL_TELEMETRY_TARGET], MSG_SUBSCRIBE_TELEMETRY, params={}, data={})
            return fn
        if func is not None:
            return decorator(func)
        return decorator

    def on_notify(self, func: Optional[Callable] = None):
        """Registers a callback or decorator to listen to system alerts and notifications."""
        def decorator(fn):
            self._notify_listeners.append(fn)
            if getattr(self, "_state", None) == "open":
                self._submit([KERNEL_NOTIFY_TARGET], MSG_SUBSCRIBE_NOTIFY, params={}, data={})
            return fn
        if func is not None:
            return decorator(func)
        return decorator

    def on_observation(self, func: Optional[Callable] = None):
        """Registers a callback or decorator to listen to the real-time global message observation stream."""
        def decorator(fn):
            self._observation_listeners.append(fn)
            if getattr(self, "_state", None) == "open":
                self._submit([KERNEL_OBSERVER_TARGET], MSG_SUBSCRIBE_OBSERVER, params={}, data={})
            return fn
        if func is not None:
            return decorator(func)
        return decorator

    def notify(self, level: str, message: str, title: str = "", details: Optional[Dict[str, Any]] = None) -> int:
        """Emits a system notification or alert to `kernel.notify`."""
        return self._submit(
            [KERNEL_NOTIFY_TARGET],
            title or "Notification",
            params={"severity": level.lower(), "level": level.lower()},
            data={"title": title, "message": message, "details": details or {}},
        )

    async def notify_async(self, level: str, message: str, title: str = "", details: Optional[Dict[str, Any]] = None, timeout: float = 30) -> Dict[str, Any]:
        """Emits a system notification or alert to `kernel.notify` asynchronously."""
        return (await self.request(
            KERNEL_NOTIFY_TARGET,
            title or "Notification",
            {"severity": level.lower(), "level": level.lower()},
            {"title": title, "message": message, "details": details or {}},
            timeout,
        ))["data"]

    async def discover_handlers(self, domain: Optional[str] = None, target: Optional[str] = None, handler: Optional[str] = None, include_inactive: bool = False, timeout: float = 30):
        """Discovers active business domain handlers (excludes kernel internal processors)."""
        return await self.query_handlers_async(domain=domain, target=target, handler=handler, include_inactive=include_inactive, timeout=timeout)

    async def query_handlers(
        self,
        domain: Optional[str] = None,
        target: Optional[str] = None,
        handler: Optional[str] = None,
    ):
        """Queries available handlers and their descriptions from `kernel.registry` (`QueryHandlers`)."""
        params: Dict[str, Any] = {}
        if domain is not None:
            params["domain"] = domain
        if target is not None:
            params["target"] = target
        if handler is not None:
            params["handler"] = handler
        return await self.send(
            targets="kernel.registry",
            message_type=MSG_QUERY_HANDLERS,
            params=params,
            data={},
        )
        if handler is not None:
            params["handler"] = handler
        return await self.send(
            targets="kernel.registry",
            message_type=MSG_QUERY_HANDLERS,
            params=params,
            data={},
        )

    def _submit(
        self,
        targets: List[str],
        message_type: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        seq: Optional[int] = None,
    ) -> int:
        """Sends a two-layer message to one or more `<domain>.<handler>` targets."""
        with self._write_lock:
            if seq is None or seq <= 0:
                assigned_seq = self._seq_counter
                self._seq_counter += 1
            else:
                assigned_seq = seq
                self._seq_counter = max(self._seq_counter, assigned_seq + 1)
            self._write_frame_unlocked({
                "frame_type": "send",
                "seq": assigned_seq,
                "targets": targets,
                "message_type": message_type,
                "params": params if params is not None else {},
                "data": data if data is not None else {},
            })
            return assigned_seq

    async def storage_insert(
        self,
        record_id: str,
        category: str,
        data: Dict[str, Any],
        upsert: bool = True,
    ):
        """Writes a record to this domain's dedicated storage table via `kernel.storage`."""
        return await self.send(
            targets="kernel.storage",
            message_type="StorageInsert",
            params={"record_id": record_id, "category": category, "upsert": upsert},
            data=data,
        )

    async def storage_update(
        self,
        record_id: str,
        data: Dict[str, Any],
        patch: bool = True,
    ):
        """Updates a record in this domain's dedicated storage table via `kernel.storage`."""
        return await self.send(
            targets="kernel.storage",
            message_type="StorageUpdate",
            params={"record_id": record_id, "patch": patch},
            data=data,
        )

    async def storage_delete(
        self,
        record_id: Optional[str] = None,
        category: Optional[str] = None,
    ):
        """Deletes record(s) from this domain's dedicated storage table via `kernel.storage`."""
        params: Dict[str, Any] = {}
        if record_id is not None:
            params["record_id"] = record_id
        if category is not None:
            params["category"] = category
        return await self.send(
            targets="kernel.storage",
            message_type="StorageDelete",
            params=params,
            data={},
        )

    async def storage_query(
        self,
        domain: Optional[str] = None,
        record_id: Optional[str] = None,
        category: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
        filter_map: Optional[Dict[str, Any]] = None,
        limit: int = 500,
    ):
        """Queries this domain's storage table or another domain's table (read-only cross-domain access)."""
        params: Dict[str, Any] = {"limit": limit}
        if domain is not None:
            params["domain"] = domain
        if record_id is not None:
            params["record_id"] = record_id
        if category is not None:
            params["category"] = category
        active_filter = filter if filter is not None else filter_map
        if active_filter is not None:
            params["filter"] = active_filter
        return await self.send(
            targets="kernel.storage",
            message_type="StorageQuery",
            params=params,
            data={},
        )

    async def query_logs(
        self,
        domain: Optional[str] = None,
        level: Optional[str] = None,
        layer: Optional[str] = None,
        component: Optional[str] = None,
        phase: Optional[str] = None,
        since_ts: Optional[float] = None,
        limit: int = 200,
        before_id: Optional[int] = None,
    ):
        """Queries persisted logs via `kernel.logger` (`QueryLogs`)."""
        params: Dict[str, Any] = {"limit": limit}
        if domain is not None:
            params["domain_id"] = domain
        if level is not None:
            params["level"] = level
        if layer is not None:
            params["layer"] = layer
        if component is not None:
            params["component"] = component
        if phase is not None:
            params["phase"] = phase
        if since_ts is not None:
            params["since_ts"] = since_ts
        if before_id is not None:
            params["before_id"] = before_id
        return await self.send(
            targets=KERNEL_LOGGER_TARGET,
            message_type=MSG_QUERY_LOGS,
            params=params,
            data={},
        )

    async def query_progress(self):
        """Queries active in-flight task progress via `kernel.logger` (`QueryProgress`)."""
        return await self.send(
            targets=KERNEL_LOGGER_TARGET,
            message_type=MSG_QUERY_PROGRESS,
            params={},
            data={},
        )

    def report_receipt(
        self,
        message_id: str,
        target: str,
        status: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Reports a lifecycle stage receipt (`received`, `started`, `completed`, `execution_failed`)."""
        with self._write_lock:
            self._write_frame_unlocked({
                "frame_type": "receipt",
                "message_id": message_id,
                "target": target,
                "status": status,
                "data": data,
            })

    def _emit_log(self, record: Dict[str, Any]) -> None:
        """Emits a structured log frame over the domain's stdout pipe to the Kernel."""
        with self._write_lock:
            self._write_frame_unlocked({
                "frame_type": "log",
                "record": record,
            })

    def attach_stdlib_logging(self, level: str = "INFO", root: bool = True) -> _StdlibLoggingHandler:
        """
        Bridges Python's standard `logging` library to this domain's structured logger.
        Any `logging.info(...)` or third-party logger will be relayed to the Kernel.
        """
        target_logger = logging.getLogger() if root else logging.getLogger(self._domain_name)
        target_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        handler = _StdlibLoggingHandler(self.logger)
        target_logger.addHandler(handler)
        return handler

    def _write_frame_unlocked(self, frame: Dict[str, Any]) -> None:
        try:line = json.dumps(frame, ensure_ascii=False, allow_nan=False) + "\n"
        except (ValueError,TypeError) as error:raise DomainRequestError('invalid_argument',{'reason':'message is not JSON serializable'}) from error
        self._write_line(line)
