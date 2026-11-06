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
        self.assertEqual(params, [])


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


# ── 本地限制与内核终态的边界 ──────────────────────────────────────────

from atelier_sdk import DomainRequestError  # noqa: E402  (与上面的用例同文件)


def make_fake(**kwargs):
    domain = FakeDomain.__new__(FakeDomain)
    AtelierDomain.__init__(domain, "test", **kwargs)
    domain.frames = []
    domain._loop = asyncio.get_running_loop()
    return domain


class LocalLimitTest(unittest.IsolatedAsyncioTestCase):
    async def test_no_submission_window_accepts_many_in_flight_sends(self):
        domain = make_fake()
        tasks = [asyncio.create_task(domain.send("t.h", "T")) for _ in range(20)]
        await asyncio.sleep(0.01)
        self.assertEqual(domain.stats()["pending"], 20)
        for index, frame in enumerate(domain.frames):
            domain.receipt(f"m-{index}", "t.h", "sent", seq=frame["seq"])
        ids = await asyncio.gather(*tasks)
        self.assertEqual(len(ids), 20)
        self.assertEqual(domain.stats()["pending"], 0)

    async def test_timed_out_send_still_accepts_the_kernel_receipt(self):
        domain = make_fake()
        with self.assertRaises(asyncio.TimeoutError):
            await domain.send("t.h", "T", timeout=0.01)
        self.assertEqual(domain.stats()["orphans"], 1)

        # 帧已经写进管道；回执迟到时仍必须认出归属。
        seq = domain.frames[0]["seq"]
        domain.receipt("m-late", "t.h", "sent", seq=seq)
        self.assertIn("m-late", domain._results)
        self.assertEqual(domain.stats()["orphans"], 0)

        domain.receipt("m-late", "t.h", "completed", {"ok": 1})
        receipt = await domain.wait_result("m-late", timeout=1)
        self.assertEqual(receipt["data"], {"ok": 1})

    async def test_unknown_message_names_the_message_id(self):
        domain = make_fake()
        with self.assertRaises(DomainRequestError) as ctx:
            await domain.wait_result("never-seen")
        self.assertEqual(ctx.exception.status, "unknown_message")
        self.assertEqual(ctx.exception.data["message_id"], "never-seen")


class TransportFailureTest(unittest.IsolatedAsyncioTestCase):
    async def test_send_after_shutdown_is_rejected_locally(self):
        domain = make_fake()
        domain._closed = True
        with self.assertRaises(DomainRequestError) as ctx:
            await domain.send("t.h", "T")
        self.assertEqual(ctx.exception.status, "local_backpressure")
        self.assertFalse(ctx.exception.data["submitted"])

    async def test_transport_loss_settles_a_pending_send(self):
        domain = make_fake()
        task = asyncio.create_task(domain.send("t.h", "T"))
        await asyncio.sleep(0.01)
        domain._on_transport_closed("socket closed")
        await asyncio.sleep(0.01)
        with self.assertRaises(DomainRequestError) as ctx:
            await task
        self.assertEqual(ctx.exception.status, "transport_closed")
        self.assertTrue(domain.stats()["transport_closed"])

    async def test_unknown_status_is_counted(self):
        domain = make_fake()
        domain._receive_result({"message_id": "m", "target": "t.h", "status": "weird_status", "seq": 1})
        self.assertEqual(domain.stats()["unknown_statuses"], 1)

    async def test_receipt_without_submission_is_counted(self):
        domain = make_fake()
        domain._receive_result({"message_id": "m-x", "target": "t.h", "status": "completed", "seq": 999})
        self.assertEqual(domain.stats()["unmatched_receipts"], 1)

    async def test_domain_info_reads_kernel_metadata(self):
        domain = make_fake()
        calls = []

        async def fake(**params):
            calls.append(params)
            return {"domain": {"domain_id": "pipelines", "status": "Active"}, "domains": []}

        domain.query_runtime = fake
        info = await domain.domain_info("pipelines")
        self.assertEqual(info["domain_id"], "pipelines")
        self.assertEqual(calls, [{"domain": "pipelines"}])

        async def empty(**params):
            return {"count": 0, "domains": [], "domain": None}

        domain.query_runtime = empty
        self.assertIsNone(await domain.domain_info("missing"))
