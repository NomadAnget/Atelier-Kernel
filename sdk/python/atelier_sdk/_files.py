"""C4 虚拟文件命名空间（薄拦截：只改路径解析）。

拦截层**只做路径映射**，然后返回原生文件对象——不包装、不重写 close/enter/exit，
不在这里算 size、不挂钩文件生命周期。原生方法照旧负责它们自己的职责。

- **写**：`open(虚拟路径, "w")` → 生成全局唯一 object_id → 解析成 `<root>/<object_id>`
        平铺物理路径 → 原生 open；同时 fire-and-forget 登记 `virtual_path → object_id`。
- **读**：`open(虚拟路径, "r")` → 查本地缓存 → 原生打开平铺物理文件。

size / 物理文件是否存在由**内核**在 resolve 时用 metadata 取得并自愈（内核拥有物理文件）。
虚拟路径 → object_id 的唯一真相源仍是内核索引。
"""

import builtins
import io
import os
import uuid


class FileNamespace:
    """虚拟路径 → `<root>/<object_id>` 平铺文件。只做路径解析。"""

    WRITE_FLAGS = set("wax+")

    def __init__(self, root, submit):
        self.root = os.path.abspath(root)
        self._submit = submit  # 同步 _submit(targets, message_type, params, data)
        self._cache = {}  # virtual_path -> physical_path
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
            ph = item.get("physical_path")
            if vp and ph:
                self._cache[vp] = ph

    def _physical(self, object_id):
        return os.path.join(self.root, object_id)

    def _register(self, virtual_path, object_id):
        # fire-and-forget：只登记「虚拟路径 → 唯一 id」映射；size 由内核按 metadata 取。
        if self._submit is None:
            return
        self._submit(
            ["kernel.storage"],
            "StorageObjectPut",
            {"virtual_path": virtual_path, "object_id": object_id},
            {},
        )

    def open(self, path, mode="r", *args, **kwargs):
        if isinstance(path, int):
            return self._original_open(path, mode, *args, **kwargs)
        virtual = self.normalize(path)
        physical = self._cache.get(virtual)
        if physical is None:
            if not any(ch in mode for ch in self.WRITE_FLAGS):
                raise FileNotFoundError("virtual file not found: %s" % virtual)
            object_id = uuid.uuid4().hex
            physical = self._physical(object_id)
            os.makedirs(self.root, exist_ok=True)
            self._cache[virtual] = physical
            self._register(virtual, object_id)
        # 只改路径；文件对象及其生命周期完全交给原生 open。
        return self._original_open(physical, mode, *args, **kwargs)

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
    """安装薄拦截层并返回 FileNamespace。"""
    ns = FileNamespace(root, submit)
    ns.install()
    return ns
