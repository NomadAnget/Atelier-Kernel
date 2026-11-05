"""C4 消息编解码单元测试：文件对象 ↔ `{__atelier_file__: object_id}`。"""

import asyncio
import io
import unittest

from atelier_sdk._codec import FILE_KEY, decode, encode


class FakeNamespace:
    def object_id_for_path(self, path):
        return "obj-123" if path == "/tmp/x.bin" else None


def file_handle(name):
    handle = io.BytesIO(b"payload")
    handle.name = name
    return handle


class CodecTest(unittest.TestCase):
    def test_encode_file_object(self):
        encoded = encode({"f": file_handle("/tmp/x.bin")}, FakeNamespace())
        self.assertEqual(encoded, {"f": {FILE_KEY: "obj-123"}})

    def test_encode_nested(self):
        encoded = encode({"a": [{"b": file_handle("/tmp/x.bin")}]}, FakeNamespace())
        self.assertEqual(encoded, {"a": [{"b": {FILE_KEY: "obj-123"}}]})

    def test_encode_unknown_path_passthrough(self):
        encoded = encode({"f": file_handle("/tmp/other.bin")}, FakeNamespace())
        self.assertTrue(hasattr(encoded["f"], "read"))

    def test_encode_non_file(self):
        self.assertEqual(encode({"n": 1, "s": "x"}, FakeNamespace()), {"n": 1, "s": "x"})

    def test_decode_top_level(self):
        async def resolver(object_id):
            return "OPEN:" + object_id

        self.assertEqual(asyncio.run(decode({"f": {FILE_KEY: "obj-123"}}, resolver)), {"f": "OPEN:obj-123"})

    def test_decode_nested(self):
        async def resolver(object_id):
            return "H:" + object_id

        self.assertEqual(asyncio.run(decode({"a": [{FILE_KEY: "id1"}]}, resolver)), {"a": ["H:id1"]})

    def test_decode_non_reference(self):
        async def resolver(object_id):
            return object_id

        self.assertEqual(asyncio.run(decode({"n": 1}, resolver)), {"n": 1})


if __name__ == "__main__":
    unittest.main()
