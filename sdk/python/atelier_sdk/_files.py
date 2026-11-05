"""C4 虚拟文件命名空间（方案 A）。

在 SDK 的文件 API 层拦截原生操作，让开发者仍用 `open()` 等原生句柄：
- **写**：SDK 生成全局唯一 object_id（uuid），直接写 `<root>/<object_id>`（平铺），
        close 时把 `(virtual_path -> object_id, size)` 异步登记给内核。
- **读**：查本地缓存（启动时从内核拉一次 `StorageObjectList`），命中后打开平铺物理文件。

内核仍是「虚拟路径 → object_id」的唯一真相源；本模块只是无阻塞的本地映射 + 登记。
注意：这是语言层约定（C 扩展 / 子进程会绕过），不做 OS 沙箱。
"""

import builtins
import io
import os
import uuid


class _TrackedFile:
    """包装原生文件对象：close/exit 时登记虚拟路径映射，其余属性透传。"""

    def __init__(self, handle, register, virtual_path):
        object.__setattr__(self, "_handle", handle)
        object.__setattr__(self, "_register", register)
        object.__setattr__(self, "_virtual_path", virtual_path)
        object.__setattr__(self, "_finalized", False)

    def __getattr__(self, name):
        return getattr(self._handle, name)

    def __enter__(self):
        self._handle.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            return self._handle.__exit__(exc_type, exc, tb)
        finally:
            self._finalize()

    def __iter__(self):
        return iter(self._handle)

    def close(self):
        try:
            self._handle.close()
        finally:
            self._finalize()

    def _finalize(self):
        if self._finalized:
            return
        self._finalized = True
        try:
            size = os.fstat(self._handle.fileno()).st_size
        except Exception:
            size = 0
        try:
            self._register(self._virtual_path, size)
        except Exception:
            pass


class FileNamespace:
    """虚拟路径 → `<root>/<object_id>` 平铺文件。"""

    WRITE_FLAGS = set("wax+")

    def __init__(self, root, submit):
        self.root = os.path.abspath(root)
        self._submit = submit  # 同步 _submit(targets, message_type, params, data)
        self._cache = {}  # virtual_path -> (object_id, physical_path)
        self._original_open = builtins.open
        self._installed = False

    @staticmethod
    def normalize(path):
        raw = str(path).replace("\\", "/").lstrip("/")
        parts = [p for p in raw.split("/") if p not in ("", ".", "..")]
        return "/".join(parts) or "unnamed"

    def load_index(self, objects):
        """用内核返回的本域对象索引填充本地读缓存。"""
        for item in objects or []:
            vp = item.get("virtual_path")
            oid = item.get("object_id")
            ph = item.get("physical_path")
            if vp and oid and ph:
                self._cache[vp] = (oid, ph)

    def _physical(self, object_id):
        return os.path.join(self.root, object_id)

    def _register(self, virtual_path, object_id, size):
        if self._submit is None:
            return
        self._submit(
            ["kernel.storage"],
            "StorageObjectPut",
            {"virtual_path": virtual_path, "object_id": object_id, "size": size},
            {},
        )

    def open(self, path, mode="r", *args, **kwargs):
        if isinstance(path, int):
            return self._original_open(path, mode, *args, **kwargs)
        virtual = self.normalize(path)
        if any(ch in mode for ch in self.WRITE_FLAGS):
            object_id = uuid.uuid4().hex
            physical = self._physical(object_id)
            os.makedirs(self.root, exist_ok=True)
            handle = self._original_open(physical, mode, *args, **kwargs)
            self._cache[virtual] = (object_id, physical)

            def register(vp, size, oid=object_id):
                self._register(vp, oid, size)

            return _TrackedFile(handle, register, virtual)
        entry = self._cache.get(virtual)
        if entry is None:
            raise FileNotFoundError("virtual file not found: %s" % virtual)
        return self._original_open(entry[1], mode, *args, **kwargs)

    def install(self):
        if self._installed:
            return
        self._installed = True
        ns = self
        real_open = self._original_open

        def patched_open(file, mode="r", *args, **kwargs):
            if isinstance(file, int):
                return real_open(file, mode, *args, **kwargs)
            return ns.open(file, mode, *args, **kwargs)

        builtins.open = patched_open
        io.open = patched_open


def install(root, submit):
    """安装拦截层并返回 FileNamespace。"""
    ns = FileNamespace(root, submit)
    ns.install()
    return ns
