"""C4 虚拟文件命名空间（薄拦截：只解析显式前缀路径）。

**边界**：只拦截带 `prefix`（默认 `vfs://`）的路径；其余一律原生放行。
这样依赖库自己的文件操作（config、tempfile、缓存、socket 等）完全不受影响。
不靠调用栈/路径特征猜测——那不可靠。

拦截层只做一件事：把 `vfs://` 虚拟路径解析成 `<root>/<object_id>` 平铺物理路径，
然后返回原生文件对象；不包装、不重写 close/enter/exit，不在这里算 size。
size / 文件是否存在由内核在 resolve 时按 metadata 取并自愈。
"""

import builtins
import io
import os
import uuid


class FileNamespace:
    """虚拟路径（带前缀）→ `<root>/<object_id>` 平铺文件。只做路径解析。"""

    WRITE_FLAGS = set("wax+")

    def __init__(self, root, submit, prefix="vfs://"):
        self.root = os.path.abspath(root)
        self._submit = submit  # 同步 _submit(targets, message_type, params, data)
        self.prefix = prefix
        self._cache = {}  # virtual_path -> physical_path
        self._original_open = builtins.open
        self._installed = False

    def matches(self, path):
        return isinstance(path, str) and path.startswith(self.prefix)

    def normalize(self, path):
        raw = str(path)[len(self.prefix) :].replace("\\", "/").lstrip("/")
        parts = [p for p in raw.split("/") if p not in ("", ".", "..")]
        return "/".join(parts) or "unnamed"

    def load_index(self, objects):
        """用内核返回的本域对象索引填充本地读缓存。"""
        for item in objects or []:
            vp = item.get("virtual_path")
            ph = item.get("physical_path")
            if vp and ph:
                self._cache[vp] = ph

    def _physical(self, object_id):
        return os.path.join(self.root, object_id)

    def _register(self, virtual_path, object_id):
        if self._submit is None:
            return
        self._submit(
            ["kernel.storage"],
            "StorageObjectPut",
            {"virtual_path": virtual_path, "object_id": object_id},
            {},
        )

    def open(self, path, mode="r", *args, **kwargs):
        # 非虚拟命名空间（库自身的文件操作）→ 原样交给原生 open。
        if isinstance(path, int) or not self.matches(path):
            return self._original_open(path, mode, *args, **kwargs)
        virtual = self.normalize(path)
        physical = self._cache.get(virtual)
        if physical is None:
            if not any(ch in mode for ch in self.WRITE_FLAGS):
                raise FileNotFoundError("virtual file not found: vfs://%s" % virtual)
            object_id = uuid.uuid4().hex
            physical = self._physical(object_id)
            os.makedirs(self.root, exist_ok=True)
            self._cache[virtual] = physical
            self._register(virtual, object_id)
        return self._original_open(physical, mode, *args, **kwargs)

    def install(self):
        if self._installed:
            return
        self._installed = True
        ns = self
        real_open = self._original_open

        def patched_open(file, mode="r", *args, **kwargs):
            if isinstance(file, int) or not ns.matches(file):
                return real_open(file, mode, *args, **kwargs)
            return ns.open(file, mode, *args, **kwargs)

        builtins.open = patched_open
        io.open = patched_open


def install(root, submit, prefix="vfs://"):
    """安装薄拦截层并返回 FileNamespace。只有 `prefix` 路径会被解析。"""
    ns = FileNamespace(root, submit, prefix)
    ns.install()
    return ns
