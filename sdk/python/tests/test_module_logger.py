"""模块级日志器：角色判定、默认身份、scope、progress 的最小验证。

直接运行：``python sdk/python/tests/test_module_logger.py``
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from atelier_sdk import logger  # noqa: E402


class FakeDomain:
    def __init__(self, name="engines"):
        self._domain_name = name
        self.logger = type("L", (), {"default_layer": "DOMAIN", "default_component": name})()
        self.records = []

    def _emit_log(self, record):
        self.records.append(record)


class ModuleLoggerTests(unittest.TestCase):
    def tearDown(self):
        logger.detach()

    def test_worker_mode_uses_engine_identity(self):
        sent = []
        logger.attach_worker_sink(sent.append, identity="FaraEngine")
        logger.info("载入开始 %s", "cuda:0")
        self.assertEqual(len(sent), 1)
        record = sent[0]
        self.assertEqual(record["component"], "FaraEngine")
        self.assertEqual(record["layer"], "ENGINES")
        self.assertEqual(record["message"], "载入开始 cuda:0")
        self.assertFalse(record["ephemeral"])

    def test_identity_updates_after_resolve(self):
        sent = []
        logger.attach_worker_sink(sent.append)
        logger.info("bootstrap")
        self.assertEqual(sent[0]["component"], "")
        logger.set_identity("IndexTTSEngine")
        logger.info("ready")
        self.assertEqual(sent[1]["component"], "IndexTTSEngine")

    def test_scope_fills_phase(self):
        sent = []
        logger.attach_worker_sink(sent.append, identity="FaraEngine")
        token = logger.push_scope("load", "call-1")
        try:
            logger.info("内部日志")
        finally:
            logger.pop_scope(token)
        self.assertEqual(sent[0]["phase"], "scope:load:call-1")

    def test_explicit_phase_wins_over_scope(self):
        sent = []
        logger.attach_worker_sink(sent.append, identity="FaraEngine")
        token = logger.push_scope("infer", "call-1")
        try:
            logger.info("边界", phase="load:start:call-1")
        finally:
            logger.pop_scope(token)
        self.assertEqual(sent[0]["phase"], "load:start:call-1")

    def test_progress_is_ephemeral(self):
        sent = []
        logger.attach_worker_sink(sent.append, identity="IndexTTSEngine")
        logger.progress("批量 1/2", key="call:abc", frac=0.5)
        record = sent[0]
        self.assertTrue(record["ephemeral"])
        self.assertEqual(record["key"], "call:abc")
        self.assertEqual(record["frac"], 0.5)
        self.assertEqual(record["state"], "running")

    def test_domain_mode_dispatches_to_domain(self):
        domain = FakeDomain("engines")
        logger.register_domain(domain)
        logger.info("域日志")
        self.assertEqual(len(domain.records), 1)
        self.assertEqual(domain.records[0]["component"], "engines")
        self.assertEqual(domain.records[0]["layer"], "DOMAIN")

    def test_worker_sink_wins_over_domain(self):
        sent = []
        domain = FakeDomain()
        logger.register_domain(domain)
        logger.attach_worker_sink(sent.append, identity="FaraEngine")
        logger.info("x")
        self.assertEqual(len(sent), 1)
        self.assertEqual(domain.records, [])

    def test_fallback_without_role_does_not_raise(self):
        logger.info("没有角色时的兜底")
        logger.progress("兜底进度", key="k")

    def test_reinject_goes_to_domain(self):
        domain = FakeDomain()
        logger.register_domain(domain)
        logger.reinject({"ts": 1.0, "level": "INFO", "layer": "ENGINES",
                         "component": "FaraEngine", "message": "子进程行", "phase": "load:start:c1"})
        self.assertEqual(domain.records[0]["component"], "FaraEngine")
        self.assertEqual(domain.records[0]["phase"], "load:start:c1")


if __name__ == "__main__":
    unittest.main(verbosity=2)
