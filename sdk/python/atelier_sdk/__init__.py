"""
Atelier Kernel Official Python Domain SDK (`atelier_sdk`).

Encapsulates OS Stdio JSON-Lines pipe framing, sequence numbering, handler
registration (`kernel.registry`), 4-stage receipt reporting (`Received` ->
`Started` -> `Completed` / `ExecutionFailed`), and `kernel.storage` CRUD helpers
so business domains only need to import `AtelierDomain` and declare handlers.
"""
import asyncio
import inspect
import json
import logging
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional

__version__ = "0.1.2"
__all__ = [
    "AtelierDomain",
    "DeliveryMessage",
    "DomainExecutionFailed",
    "DomainLogger",
    "__version__",
]


class DomainExecutionFailed(Exception):
    """Raise inside a `@domain.handler(...)` to report an `execution_failed` receipt with structured payload."""

    def __init__(self, payload: Dict[str, Any]):
        super().__init__(str(payload))
        self.payload = payload


class DeliveryMessage(dict):
    """Two-layer message wrapper supporting both attribute (`msg.data`) and dict (`msg['data']`) access."""

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
        return self.get("data") or {}


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
        layer: str = "",
        component: str = "",
    ) -> None:
        """
        Ephemeral real-time progress update.
        Dispatched in-memory to Web UI / SSE observers; never enters SQLite hub_logs.
        """
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
            "waiting": bool(waiting),
            "done": bool(done),
        }
        self._domain._emit_log(record)


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


class AtelierDomain:
    """High-level Python Domain SDK for Atelier Kernel."""

    def __init__(
        self,
        domain: str = "",
        stage_delay_sec: float = 0.0,
    ):
        self._domain_name = domain
        self._seq_counter = 1
        self._write_lock = threading.Lock()
        self._handlers: Dict[str, Callable[..., Any]] = {}
        self._receipt_callbacks: List[Callable[[Dict[str, Any]], None]] = []
        self._stage_delay_sec = stage_delay_sec
        self.logger = DomainLogger(
            self,
            default_layer="DOMAIN",
            default_component=domain,
        )

    def handler(self, name: str):
        """Decorator that registers a message handler for `<domain>.<name>`."""
        if not name or "." in name:
            raise ValueError(f"Invalid handler name '{name}': must be non-empty and not contain '.'")

        def decorator(func: Callable[..., Any]):
            self._handlers[name] = func
            return func

        return decorator

    def on_receipt(self, func: Callable[[Dict[str, Any]], None]):
        """Optional decorator to observe receipts returned by the Kernel for outbound messages."""
        self._receipt_callbacks.append(func)
        return func

    def send(
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
            self._write_frame_unlocked({
                "frame_type": "send",
                "seq": assigned_seq,
                "targets": targets,
                "message_type": message_type,
                "params": params if params is not None else {},
                "data": data if data is not None else {},
            })
            return assigned_seq

    def storage_insert(
        self,
        record_id: str,
        category: str,
        data: Dict[str, Any],
        upsert: bool = True,
    ) -> int:
        """Writes a record to this domain's dedicated storage table via `kernel.storage`."""
        return self.send(
            targets=["kernel.storage"],
            message_type="StorageInsert",
            params={"record_id": record_id, "category": category, "upsert": upsert},
            data=data,
        )

    def storage_update(
        self,
        record_id: str,
        data: Dict[str, Any],
        patch: bool = True,
    ) -> int:
        """Updates a record in this domain's dedicated storage table via `kernel.storage`."""
        return self.send(
            targets=["kernel.storage"],
            message_type="StorageUpdate",
            params={"record_id": record_id, "patch": patch},
            data=data,
        )

    def storage_delete(
        self,
        record_id: Optional[str] = None,
        category: Optional[str] = None,
    ) -> int:
        """Deletes record(s) from this domain's dedicated storage table via `kernel.storage`."""
        params: Dict[str, Any] = {}
        if record_id is not None:
            params["record_id"] = record_id
        if category is not None:
            params["category"] = category
        return self.send(
            targets=["kernel.storage"],
            message_type="StorageDelete",
            params=params,
            data={},
        )

    def storage_query(
        self,
        domain: Optional[str] = None,
        record_id: Optional[str] = None,
        category: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
        filter_map: Optional[Dict[str, Any]] = None,
        limit: int = 500,
    ) -> int:
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
        return self.send(
            targets=["kernel.storage"],
            message_type="StorageQuery",
            params=params,
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
        line = json.dumps(frame, ensure_ascii=False) + "\n"
        sys.stdout.write(line)
        sys.stdout.flush()

    def run(self) -> None:
        """Registers all declared handlers with the Kernel and starts the blocking stdin event loop."""
        if self._handlers:
            self.send(
                targets=["kernel.registry"],
                message_type="RegisterHandlers",
                params={"handlers": list(self._handlers.keys())},
                data={},
            )

        for raw_line in sys.stdin:
            line = raw_line.strip()
            if not line:
                continue
            try:
                frame = json.loads(line)
            except Exception:
                continue

            frame_type = frame.get("frame_type")
            if frame_type == "receipt":
                for cb in self._receipt_callbacks:
                    try:
                        cb(frame)
                    except Exception:
                        pass
                continue

            if frame_type != "deliver":
                continue

            msg = DeliveryMessage(frame)
            msg_id = msg.message_id
            target = msg.target
            handler_name = target.split(".", 1)[1] if "." in target else target

            # Automatically report Stage 2 (`Received`) and Stage 3 (`Started`)
            self.report_receipt(msg_id, target, "received", None)
            self.report_receipt(msg_id, target, "started", None)
            if self._stage_delay_sec > 0:
                time.sleep(self._stage_delay_sec)

            handler_fn = self._handlers.get(handler_name)
            if handler_fn is None:
                self.report_receipt(
                    msg_id,
                    target,
                    "execution_failed",
                    {"error": f"Unknown handler '{handler_name}'"},
                )
                continue

            try:
                try:
                    sig = inspect.signature(handler_fn)
                    param_count = len(sig.parameters)
                except Exception:
                    param_count = 1

                if param_count >= 2:
                    call_res = handler_fn(msg, self)
                else:
                    call_res = handler_fn(msg)

                if inspect.iscoroutine(call_res):
                    result = asyncio.run(call_res)
                else:
                    result = call_res

                self.report_receipt(msg_id, target, "completed", result)
            except DomainExecutionFailed as exc:
                self.report_receipt(msg_id, target, "execution_failed", exc.payload)
            except Exception as exc:
                self.report_receipt(
                    msg_id,
                    target,
                    "execution_failed",
                    {"error": "handler_exception", "reason": str(exc)},
                )
