"""Atelier SDK v2 单元测试：用假传输验证消息路径与能力转发。

不依赖真实内核：把 `_write` 换成记录器，再手工注入「内核回执」。
"""

import asyncio
import unittest

from atelier_sdk import AtelierDomain


class FakeDomain(AtelierDomain):
    def __init__(self):
        super().__init__("test")
        self.frames = []

    def _write(self, frame):
        self.frames.append(frame)

    def receipt(self, message_id, target, status, data=None, seq=None):
        self._receive_result(
            {
                "frame_type": "receipt",
                "message_id": message_id,
                "target": target,
                "status": status,
                "data": data,
                "seq": seq,
            }
        )


class ConstructorSurfaceTest(unittest.TestCase):
    """构造参数必须与 docs/SDK_CONTRACT.md 里写的一致，不允许再长出无效旋钮。"""

    def test_constructor_accepts_only_documented_options(self):
        import inspect

        params = [
            name
            for name in inspect.signature(AtelierDomain.__init__).parameters
            if name not in ("self", "domain")
        ]
        self.assertEqual(params, ["max_pending", "result_ttl"])


class MessagePathTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.domain = FakeDomain()
        self.domain._loop = asyncio.get_running_loop()

    async def test_send_returns_message_id(self):
        task = asyncio.create_task(self.domain.send("t.h", "T", {"p": 1}, {"d": 2}))
        await asyncio.sleep(0)
        frame = self.domain.frames[0]
        self.assertEqual(frame["frame_type"], "send")
        self.assertEqual(frame["targets"], ["t.h"])
        self.assertEqual(frame["params"], {"p": 1})
        self.assertEqual(frame["data"], {"d": 2})
        self.domain.receipt("m1", "t.h", "sent", seq=frame["seq"])
        self.assertEqual(await task, "m1")

    async def test_request_waits_for_terminal_and_forgets(self):
        task = asyncio.create_task(self.domain.request("t.h", "T"))
        await asyncio.sleep(0)
        frame = self.domain.frames[0]
        self.domain.receipt("m2", "t.h", "sent", seq=frame["seq"])
        await asyncio.sleep(0)
        self.domain.receipt("m2", "t.h", "completed", {"ok": True})
        receipt = await task
        self.assertEqual(receipt["data"], {"ok": True})
        self.assertNotIn("m2", self.domain._results)

    async def test_non_completed_terminal_raises(self):
        task = asyncio.create_task(self.domain.request("t.h", "T"))
        await asyncio.sleep(0)
        frame = self.domain.frames[0]
        self.domain.receipt("m3", "t.h", "sent", seq=frame["seq"])
        await asyncio.sleep(0)
        self.domain.receipt("m3", "t.h", "execution_failed", {"error": "boom"})
        with self.assertRaises(Exception):
            await task

    async def test_wait_result_unknown_message(self):
        with self.assertRaises(Exception):
            await self.domain.wait_result("nope")


class CapabilityRoutingTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.domain = FakeDomain()
        self.domain._loop = asyncio.get_running_loop()

    async def _capture(self, coro):
        task = asyncio.create_task(coro)
        await asyncio.sleep(0)
        frame = self.domain.frames[-1]
        self.domain.receipt("x", frame["targets"][0], "sent", seq=frame["seq"])
        await asyncio.sleep(0)
        self.domain.receipt("x", frame["targets"][0], "completed", {"done": True})
        await task
        return frame

    async def test_storage_insert_routes_to_kernel_storage(self):
        frame = await self._capture(
            self.domain.storage_insert("rec", "cat", {"value": 1}, upsert=True)
        )
        self.assertEqual(frame["targets"], ["kernel.storage"])
        self.assertEqual(frame["message_type"], "StorageInsert")
        self.assertEqual(frame["params"]["record_id"], "rec")
        self.assertEqual(frame["params"]["category"], "cat")
        self.assertTrue(frame["params"]["upsert"])

    async def test_query_logs_routes_to_kernel_logger(self):
        frame = await self._capture(self.domain.query_logs(limit=5))
        self.assertEqual(frame["targets"], ["kernel.logger"])
        self.assertEqual(frame["message_type"], "QueryLogs")
        self.assertEqual(frame["params"], {"limit": 5})

    async def test_supervisor_restart_routes(self):
        frame = await self._capture(self.domain.supervisor_restart("web"))
        self.assertEqual(frame["targets"], ["kernel.supervisor"])
        self.assertEqual(frame["message_type"], "RestartDomain")
        self.assertEqual(frame["params"], {"domain": "web"})

    async def test_subscribe_logs_routes(self):
        frame = await self._capture(self.domain.subscribe_logs())
        self.assertEqual(frame["targets"], ["kernel.logger"])
        self.assertEqual(frame["message_type"], "SubscribeLogs")

    async def test_notify_routes(self):
        frame = await self._capture(self.domain.notify("info", "hello", title="Hi"))
        self.assertEqual(frame["targets"], ["kernel.notify"])
        self.assertEqual(frame["data"]["message"], "hello")


class C4AndObservabilityTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.domain = FakeDomain()
        self.domain._loop = asyncio.get_running_loop()

    async def _capture(self, coro):
        task = asyncio.create_task(coro)
        await asyncio.sleep(0)
        frame = self.domain.frames[-1]
        self.domain.receipt("x", frame["targets"][0], "sent", seq=frame["seq"])
        await asyncio.sleep(0)
        self.domain.receipt("x", frame["targets"][0], "completed", {"done": True})
        await task
        return frame

    async def test_put_object_routes(self):
        frame = await self._capture(self.domain.put_object("evidence/x"))
        self.assertEqual(frame["targets"], ["kernel.storage"])
        self.assertEqual(frame["message_type"], "StorageObjectPut")
        self.assertEqual(frame["params"], {"virtual_path": "evidence/x"})

    async def test_get_object_routes(self):
        frame = await self._capture(self.domain.get_object("abc"))
        self.assertEqual(frame["message_type"], "StorageObjectGet")
        self.assertEqual(frame["params"], {"object_id": "abc"})

    async def test_delete_object_routes(self):
        frame = await self._capture(self.domain.delete_object("abc"))
        self.assertEqual(frame["message_type"], "StorageObjectDelete")
        self.assertEqual(frame["params"], {"object_id": "abc"})

    async def test_refresh_file_index_routes(self):
        frame = await self._capture(self.domain.refresh_file_index())
        self.assertEqual(frame["message_type"], "StorageObjectList")

    def test_events_are_dispatched_to_callbacks(self):
        seen = []
        self.domain.on_log(seen.append)
        self.domain._emit("log", {"level": "info"})
        self.domain._emit("log", {"level": "warn"})
        self.assertEqual([item["level"] for item in seen], ["info", "warn"])

    def test_event_callback_failure_does_not_break_emit(self):
        def boom(_payload):
            raise RuntimeError("boom")

        self.domain.on_telemetry(boom)
        self.domain._emit("telemetry", {"ok": True})

    def test_bindings_default_empty(self):
        self.assertEqual(self.domain.bindings, {})

    def test_logger_writes_to_kernel_logger(self):
        self.domain.logger.info("hello", phase="start", tag="x")
        frame = self.domain.frames[-1]
        # 日志走专用 log 帧：不分配消息身份，也不产生四阶段回执。
        self.assertEqual(frame["frame_type"], "log")
        self.assertNotIn("targets", frame)
        record = frame["record"]
        self.assertEqual(record["level"], "info")
        self.assertEqual(record["message"], "hello")
        self.assertEqual(record["component"], "test")
        self.assertEqual(record["layer"], "DOMAIN")
        self.assertEqual(record["phase"], "start")
        self.assertEqual(record["tag"], "x")


if __name__ == "__main__":
    unittest.main()
