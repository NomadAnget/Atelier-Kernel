"""C4 文件命名空间单元测试：虚拟路径 → 平铺唯一 id 文件、非 vfs 路径放行、读缓存。"""

import builtins
import os
import shutil
import tempfile
import unittest

from atelier_sdk._files import FileNamespace, install


class _FakeSubmit:
    def __init__(self):
        self.calls = []

    def __call__(self, targets, message_type, params, data):
        self.calls.append((targets, message_type, params, data))


class FileNamespaceTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="c4-ns-")
        self.submit = _FakeSubmit()
        self.ns = FileNamespace(self.root, self.submit)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_write_creates_flat_unique_id_file_and_registers(self):
        with self.ns.open("vfs://users/a.txt", "wb") as f:
            f.write(b"hello")
        physical = self.ns._cache["users/a.txt"]
        self.assertTrue(physical.startswith(self.root))
        # 物理文件名就是唯一 id，平铺在域根下
        self.assertEqual(os.path.dirname(physical), self.root)
        self.assertTrue(os.path.exists(physical))
        with open(physical, "rb") as real:
            self.assertEqual(real.read(), b"hello")
        # 登记了 virtual_path -> object_id
        self.assertEqual(len(self.submit.calls), 1)
        _, mtype, params, _ = self.submit.calls[0]
        self.assertEqual(mtype, "StorageObjectPut")
        self.assertEqual(params["virtual_path"], "users/a.txt")
        self.assertTrue(params["object_id"])

    def test_same_virtual_path_resolves_to_same_physical(self):
        with self.ns.open("vfs://a/b.bin", "wb") as f:
            f.write(b"one")
        first = self.ns._cache["a/b.bin"]
        with self.ns.open("vfs://a/b.bin", "wb") as f:
            f.write(b"two")
        self.assertEqual(self.ns._cache["a/b.bin"], first)

    def test_read_uses_cached_index(self):
        physical = os.path.join(self.root, "obj_cached")
        with open(physical, "wb") as f:
            f.write(b"cached")
        self.ns.load_index([{"virtual_path": "x/y", "physical_path": physical}])
        with self.ns.open("vfs://x/y", "rb") as f:
            self.assertEqual(f.read(), b"cached")

    def test_missing_read_raises(self):
        with self.assertRaises(FileNotFoundError):
            self.ns.open("vfs://nope/missing.bin", "rb")

    def test_install_keeps_non_vfs_paths_native(self):
        plain = os.path.join(self.root, "plain.txt")
        original = builtins.open
        try:
            ns = install(self.root, self.submit)
            with builtins.open(plain, "wb") as f:  # non-vfs -> native
                f.write(b"native")
            with builtins.open("vfs://u/v.txt", "wb") as f:  # vfs -> intercepted
                f.write(b"virtual")
        finally:
            builtins.open = original
        self.assertTrue(os.path.exists(plain))
        self.assertIn("u/v.txt", ns._cache)
        self.assertTrue(os.path.exists(ns._cache["u/v.txt"]))


if __name__ == "__main__":
    unittest.main()
