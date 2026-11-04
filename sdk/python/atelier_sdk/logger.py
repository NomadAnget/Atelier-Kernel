"""模块级结构化日志器：调用方只写 ``logger.info(...)``，由本模块判断记录该发往哪里。

三种进程角色，调用契约完全相同：

1. **域主进程**：``AtelierDomain`` 构造时自动 ``register_domain(self)``，记录直接走
   ``domain._emit_log`` → 内核管道 → ``hub_logs`` / SSE；
2. **worker 子进程**：worker 引导时 ``attach_worker_sink(send, identity=...)``，记录
   交给 ``send`` 回传父进程（父进程 ``reinject`` 后走同一条管道）；
3. **都未注册**（脚本、单元测试、引导早期）：退化为控制台输出，不抛错。

``identity`` 是 worker 的默认组件名：引擎 worker 用引擎类名（如 ``FaraEngine``），
于是父进程收到的 ``source_domain=engines`` + ``component=FaraEngine`` 即
``engines.引擎名``。内核的 ``source_domain`` 仍由管道身份决定，本模块不伪造它。

本模块不 import ``atelier_sdk`` 的其余部分（避免循环依赖），也不依赖任何第三方库。
"""

from __future__ import annotations

import contextlib
import contextvars
import sys
import threading
import time
import traceback
from typing import Any, Callable, Dict, Optional

# 日志命名空间：SDK 自己的 stdlib 桥（domain.attach_stdlib_logging）默认挂 root；
# 本模块的记录不经过 stdlib，因此不会与第三方库的日志重复。
ROOT = "atelier_sdk"

_SCOPE: contextvars.ContextVar[str] = contextvars.ContextVar("atelier_sdk_log_scope", default="")

_LOCK = threading.Lock()
_SINK: Optional[Callable[[Dict[str, Any]], None]] = None
_IDENTITY: str = ""          # worker 默认组件名（引擎类名）
_LAYER: str = "DOMAIN"       # 默认层
_DOMAIN: Any = None          # 域主进程的 AtelierDomain
_STDLIB_HANDLER: Any = None  # worker 模式下转发第三方库日志的 handler
_CONSOLE: bool = True        # 无角色时是否打印到控制台
_SUBSCRIBERS: Dict[Any, Any] = {}   # asyncio.Queue -> loop（worker 内部日志流）


# ── 角色注册 ────────────────────────────────────────────────────────────────

def attach_worker_sink(send: Callable[[Dict[str, Any]], None], *, identity: str = "",
                       layer: str = "ENGINES", forward_stdlib: bool = True) -> None:
    """worker 子进程：把记录交给 ``send``；可选把第三方库日志也接过来。

    必须在 import 业务引擎代码之前调用，之后的记录才带默认身份。
    """
    global _SINK, _IDENTITY, _LAYER, _STDLIB_HANDLER
    with _LOCK:
        _SINK = send
        _IDENTITY = str(identity or "")
        _LAYER = str(layer or "ENGINES")
    if forward_stdlib and _STDLIB_HANDLER is None:
        import logging

        logging.getLogger().setLevel(logging.INFO)
        handler = _StdlibSinkHandler(send)
        logging.getLogger().addHandler(handler)
        _STDLIB_HANDLER = handler


def set_identity(identity: str, *, layer: str = "") -> None:
    """worker 解析出引擎类之后调用：之后的记录默认归属该引擎。"""
    global _IDENTITY, _LAYER
    with _LOCK:
        _IDENTITY = str(identity or "")
        if layer:
            _LAYER = str(layer)


def register_domain(domain: Any) -> None:
    """域主进程：``AtelierDomain`` 构造时自动调用。"""
    global _DOMAIN
    with _LOCK:
        _DOMAIN = domain


def detach() -> None:
    """测试/关闭：清空角色注册与 stdlib 转发。"""
    global _SINK, _IDENTITY, _LAYER, _DOMAIN, _STDLIB_HANDLER, _CONSOLE
    with _LOCK:
        _SINK = None
        _IDENTITY = ""
        _LAYER = "DOMAIN"
        _DOMAIN = None
        _CONSOLE = True
    if _STDLIB_HANDLER is not None:
        import logging

        with contextlib.suppress(Exception):
            logging.getLogger().removeHandler(_STDLIB_HANDLER)
        _STDLIB_HANDLER = None


def current_identity() -> str:
    return _IDENTITY


def detach_console() -> None:
    """域/worker 进程用：不再向控制台打印兜底日志（日志走内核管道）。"""
    global _CONSOLE
    _CONSOLE = False


# ── 作用域（scope:load/<call_id> 这类隐式 phase）──────────────────────────────

@contextlib.contextmanager
def scope(kind: str, ident: str):
    token = _SCOPE.set(f"scope:{kind}:{ident}" if kind and ident else "")
    try:
        yield
    finally:
        _SCOPE.reset(token)


def push_scope(kind: str, ident: str):
    """非 with 形式的 scope（供已有 try/finally 使用），返回 token。"""
    return _SCOPE.set(f"scope:{kind}:{ident}" if kind and ident else "")


def pop_scope(token) -> None:
    with contextlib.suppress(Exception):
        _SCOPE.reset(token)


# ── 记录构造与分派 ───────────────────────────────────────────────────────────

def _default_layer() -> str:
    if _SINK is not None:
        return _LAYER
    domain = _DOMAIN
    return str(getattr(getattr(domain, "logger", None), "default_layer", "") or "DOMAIN")


def _default_component() -> str:
    if _SINK is not None:
        return _IDENTITY
    domain = _DOMAIN
    return str(getattr(getattr(domain, "logger", None), "default_component", "") or "")


def _dispatch(record: Dict[str, Any]) -> None:
    if record.get("console_only"):
        if _CONSOLE:
            try:
                print(f"{time.strftime('%H:%M:%S')} {record.get('message', '')}", file=sys.stderr)
            except Exception:  # noqa: BLE001
                pass
        return
    sink = _SINK
    if sink is not None:
        sink(record)
        return
    domain = _DOMAIN
    if domain is not None:
        domain._emit_log(record)   # noqa: SLF001 —— SDK 内部约定
        return
    # 兜底：控制台（引导早期 / 脚本 / 单元测试）
    if not _CONSOLE:
        return
    try:
        text = f"{time.strftime('%H:%M:%S')} {record.get('level', 'INFO'):<8} " \
               f"[{record.get('layer', '')}/{record.get('component', '')}] {record.get('message', '')}"
        print(text, file=sys.stderr)
    except Exception:  # noqa: BLE001
        pass


def _emit(level: str, msg: Any, args: tuple, *, layer: str = "", component: str = "",
          phase: str = "", console_only: bool = False, exc_info: Any = None) -> None:
    try:
        text = (msg % args) if args else str(msg)
    except Exception:  # noqa: BLE001
        text = str(msg)
    if exc_info:
        if exc_info is True:
            text += "\n" + traceback.format_exc()
        elif isinstance(exc_info, tuple):
            text += "\n" + "".join(traceback.format_exception(*exc_info))
        elif isinstance(exc_info, BaseException):
            text += f"\n{type(exc_info).__name__}: {exc_info}"
    record: Dict[str, Any] = {
        "ts": time.time(),
        "level": level.upper(),
        "layer": layer or _default_layer(),
        "component": component or _default_component(),
        "message": text,
        "phase": phase or _SCOPE.get(),
        "console_only": bool(console_only),
        "ephemeral": False,
    }
    _dispatch(record)


def debug(msg: Any, *args: Any, **kwargs: Any) -> None:
    _emit("DEBUG", msg, args, **kwargs)


def info(msg: Any, *args: Any, **kwargs: Any) -> None:
    _emit("INFO", msg, args, **kwargs)


def warning(msg: Any, *args: Any, **kwargs: Any) -> None:
    _emit("WARNING", msg, args, **kwargs)


def warn(msg: Any, *args: Any, **kwargs: Any) -> None:
    _emit("WARNING", msg, args, **kwargs)


def error(msg: Any, *args: Any, **kwargs: Any) -> None:
    _emit("ERROR", msg, args, **kwargs)


def exception(msg: Any, *args: Any, **kwargs: Any) -> None:
    kwargs.setdefault("exc_info", True)
    _emit("ERROR", msg, args, **kwargs)


def phase(level: str, phase_name: str, msg: Any, *args: Any, **kwargs: Any) -> None:
    """生命周期配对行（``load:start:<id>`` 等）。"""
    kwargs["phase"] = phase_name
    _emit(level, msg, args, **kwargs)


def progress(msg: Any, *, key: str = "", frac: Optional[float] = None,
             waiting: bool = False, done: bool = False, state: Optional[str] = None,
             layer: str = "", component: str = "") -> None:
    """瞬时进度行：不落 ``hub_logs``，由内核按 key 合并。"""
    if state is None:
        state = "completed" if done else "running"
    if state not in ("running", "completed", "failed", "cancelled"):
        raise ValueError("invalid progress state")
    done = state != "running"
    record: Dict[str, Any] = {
        "ts": time.time(),
        "level": "INFO",
        "layer": layer or _default_layer(),
        "component": component or _default_component(),
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
    if record["frac"] is None:
        record.pop("frac")
    if not record["waiting"]:
        record.pop("waiting")
    _dispatch(record)


def reinject(record: Dict[str, Any]) -> None:
    """把子进程回传的记录按原样重放（父进程用）。"""
    if not isinstance(record, dict):
        return
    _dispatch(dict(record))


# ── 第三方库日志 → worker sink ───────────────────────────────────────────────

class _StdlibSinkHandler:
    """worker 模式：把 ``logging`` 记录转成结构化 record 后交给 sink。

    引擎自身的日志已走 ``logger.info``；这个 handler 只兜第三方库
    （transformers / urllib3 / 引擎内部 import 的重依赖）。
    """

    def __init__(self, send: Callable[[Dict[str, Any]], None]):
        import logging

        self._send = send
        self._handler = logging.Handler()
        self._handler.setLevel(logging.INFO)

        def emit(record):  # noqa: ANN001
            if getattr(record, "console_only", False):
                return
            try:
                self._send({
                    "ts": record.created,
                    "level": record.levelname,
                    "layer": getattr(record, "layer", "") or _LAYER,
                    "component": getattr(record, "component", "") or _IDENTITY,
                    "message": record.getMessage(),
                    "phase": getattr(record, "phase", "") or "",
                    "console_only": False,
                    "ephemeral": False,
                })
            except Exception:  # noqa: BLE001
                pass

        self._handler.emit = emit  # type: ignore[method-assign]

    def __getattr__(self, name: str):
        return getattr(self._handler, name)
