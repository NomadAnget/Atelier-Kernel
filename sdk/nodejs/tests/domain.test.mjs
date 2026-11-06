import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { AtelierDomain } = require("../index.js");

function makeDomain() {
  const domain = new AtelierDomain("test");
  domain.frames = [];
  domain._write = (frame) => domain.frames.push(frame);
  return domain;
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

function receipt(domain, messageId, target, status, data, seq) {
  domain._receiveResult({ frame_type: "receipt", message_id: messageId, target, status, data, seq });
}

// ── 消息路径 ──────────────────────────────────────────────────────────
test("send resolves the message id", async () => {
  const domain = makeDomain();
  const task = domain.send("t.h", "T", { p: 1 }, { d: 2 });
  await tick();
  const frame = domain.frames[0];
  assert.equal(frame.frame_type, "send");
  assert.deepEqual(frame.targets, ["t.h"]);
  assert.deepEqual(frame.params, { p: 1 });
  assert.deepEqual(frame.data, { d: 2 });
  receipt(domain, "m1", "t.h", "sent", null, frame.seq);
  assert.equal(await task, "m1");
});

test("request returns the terminal data and forgets the result", async () => {
  const domain = makeDomain();
  const task = domain.request("t.h", "T");
  await tick();
  const frame = domain.frames[0];
  receipt(domain, "m2", "t.h", "sent", null, frame.seq);
  await tick();
  receipt(domain, "m2", "t.h", "completed", { ok: true });
  const result = await task;
  assert.deepEqual(result.data, { ok: true });
  assert.equal(domain._results.has("m2"), false);
});

test("non-completed terminal rejects", async () => {
  const domain = makeDomain();
  const task = domain.request("t.h", "T");
  await tick();
  const frame = domain.frames[0];
  receipt(domain, "m3", "t.h", "sent", null, frame.seq);
  await tick();
  receipt(domain, "m3", "t.h", "execution_failed", { error: "boom" });
  await assert.rejects(task);
});

test("waitResult on an unknown id rejects", async () => {
  const domain = makeDomain();
  await assert.rejects(domain.waitResult("nope"));
});

test("waitResult rejects a non-completed terminal", async () => {
  const domain = makeDomain();
  const pending = domain.send("t.h", "T");
  await tick();
  const frame = domain.frames[0];
  receipt(domain, "m4", "t.h", "sent", null, frame.seq);
  assert.equal(await pending, "m4");
  const task = domain.waitResult("m4");
  receipt(domain, "m4", "t.h", "execution_failed", { error: "boom" });
  await assert.rejects(task);
});

test("waitResults resolves every terminal receipt", async () => {
  const domain = makeDomain();
  const first = domain.send("t.h", "T");
  const second = domain.send("t.h", "T");
  await tick();
  const [f1, f2] = domain.frames;
  receipt(domain, "m5", "t.h", "sent", null, f1.seq);
  receipt(domain, "m6", "t.h", "sent", null, f2.seq);
  assert.deepEqual(await Promise.all([first, second]), ["m5", "m6"]);
  const task = domain.waitResults(["m5", "m6"]);
  receipt(domain, "m5", "t.h", "completed", { n: 1 });
  receipt(domain, "m6", "t.h", "completed", { n: 2 });
  const results = await task;
  assert.deepEqual(results.map((r) => r.data.n), [1, 2]);
});

// ── 能力转发 ──────────────────────────────────────────────────────────
async function capture(domain, coro) {
  const task = coro;
  await tick();
  const frame = domain.frames[domain.frames.length - 1];
  receipt(domain, "x", frame.targets[0], "sent", null, frame.seq);
  await tick();
  receipt(domain, "x", frame.targets[0], "completed", { done: true });
  await task;
  return frame;
}

test("storageInsert routes to kernel.storage", async () => {
  const domain = makeDomain();
  const frame = await capture(domain, domain.storageInsert("rec", "cat", { value: 1 }, true));
  assert.equal(frame.targets[0], "kernel.storage");
  assert.equal(frame.message_type, "StorageInsert");
  assert.equal(frame.params.record_id, "rec");
  assert.equal(frame.params.category, "cat");
});

test("queryLogs routes to kernel.logger", async () => {
  const domain = makeDomain();
  const frame = await capture(domain, domain.queryLogs({ limit: 5 }));
  assert.equal(frame.targets[0], "kernel.logger");
  assert.equal(frame.message_type, "QueryLogs");
});

test("supervisorRestart routes to kernel.supervisor", async () => {
  const domain = makeDomain();
  const frame = await capture(domain, domain.supervisorRestart("web"));
  assert.equal(frame.targets[0], "kernel.supervisor");
  assert.equal(frame.message_type, "RestartDomain");
  assert.deepEqual(frame.params, { domain: "web" });
});

test("subscribeLogs routes to kernel.logger", async () => {
  const domain = makeDomain();
  const frame = await capture(domain, domain.subscribeLogs());
  assert.equal(frame.targets[0], "kernel.logger");
  assert.equal(frame.message_type, "SubscribeLogs");
});

test("notify routes to kernel.notify", async () => {
  const domain = makeDomain();
  const frame = await capture(domain, domain.notify("info", "hello", "Hi"));
  assert.equal(frame.targets[0], "kernel.notify");
  assert.equal(frame.data.message, "hello");
});

// ── 编解码 ────────────────────────────────────────────────────────────
test("_encodeFiles turns a tracked handle into an object reference", () => {
  const domain = makeDomain();
  const handle = { read() {}, path: "" };
  domain._handles.set(handle, "obj-1");
  assert.deepEqual(domain._encodeFiles({ f: handle }), { f: { __atelier_file__: "obj-1" } });
});

test("_encodeFiles recurses into arrays and objects", () => {
  const domain = makeDomain();
  const handle = { read() {}, path: "" };
  domain._handles.set(handle, "obj-2");
  assert.deepEqual(domain._encodeFiles({ a: [handle] }), { a: [{ __atelier_file__: "obj-2" }] });
});

test("_decodeFiles leaves non-references untouched", async () => {
  const domain = makeDomain();
  assert.deepEqual(await domain._decodeFiles({ n: 1, s: "x" }), { n: 1, s: "x" });
});

// ── C4 CRUD / 事件 / 日志 / 端口 ──────────────────────────────────────
test("putObject routes to kernel.storage", async () => {
  const domain = makeDomain();
  const frame = await capture(domain, domain.putObject("evidence/x"));
  assert.equal(frame.targets[0], "kernel.storage");
  assert.equal(frame.message_type, "StorageObjectPut");
  assert.deepEqual(frame.params, { virtual_path: "evidence/x" });
});

test("getObject routes to kernel.storage", async () => {
  const domain = makeDomain();
  const frame = await capture(domain, domain.getObject("abc"));
  assert.equal(frame.message_type, "StorageObjectGet");
  assert.deepEqual(frame.params, { object_id: "abc" });
});

test("deleteObject routes to kernel.storage", async () => {
  const domain = makeDomain();
  const frame = await capture(domain, domain.deleteObject("abc"));
  assert.equal(frame.message_type, "StorageObjectDelete");
  assert.deepEqual(frame.params, { object_id: "abc" });
});

test("events dispatch to callbacks", () => {
  const domain = makeDomain();
  const seen = [];
  domain.onLog((payload) => seen.push(payload));
  domain._emit("log", { level: "info" });
  domain._emit("log", { level: "warn" });
  assert.deepEqual(seen.map((item) => item.level), ["info", "warn"]);
});

test("event callback failure does not break emit", () => {
  const domain = makeDomain();
  domain.onTelemetry(() => {
    throw new Error("boom");
  });
  domain._emit("telemetry", { ok: true });
});

test("bindings defaults to an empty object", () => {
  assert.deepEqual(makeDomain().bindings, {});
});

test("logger writes a dedicated log frame", () => {
  const domain = makeDomain();
  domain.logger.info("hello", { phase: "start", tag: "x" });
  const frame = domain.frames[domain.frames.length - 1];
  // 日志走专用 log 帧：不分配消息身份，也不产生四阶段回执。
  assert.equal(frame.frame_type, "log");
  assert.equal(frame.targets, undefined);
  assert.equal(frame.record.level, "info");
  assert.equal(frame.record.message, "hello");
  assert.equal(frame.record.component, "test");
  assert.equal(frame.record.layer, "DOMAIN");
  assert.equal(frame.record.phase, "start");
  assert.equal(frame.record.tag, "x");
});

// ── 本地限制与内核终态的边界 ──────────────────────────────────────────
test("no submission window: many in-flight sends are all accepted", async () => {
  const domain = makeDomain();
  const tasks = [];
  for (let i = 0; i < 20; i += 1) tasks.push(domain.send("t.h", "T", {}, {}, { timeout: 5 }));
  await tick();
  assert.equal(domain.stats().pending, 20);
  domain.frames.forEach((frame, index) => receipt(domain, `m-${index}`, "t.h", "sent", null, frame.seq));
  const ids = await Promise.all(tasks);
  assert.equal(ids.length, 20);
  assert.equal(domain.stats().pending, 0);
});

test("a submission that timed out locally still accepts the kernel receipt", async () => {
  const domain = makeDomain();
  await assert.rejects(domain.send("t.h", "T", {}, {}, { timeout: 0.01 }));
  assert.equal(domain.stats().orphans, 1);

  // 帧已经写进管道；内核的回执迟到时，SDK 必须仍然认出它属于谁。
  const seq = domain.frames[0].seq;
  receipt(domain, "m-late", "t.h", "sent", null, seq);
  assert.equal(domain._results.has("m-late"), true);
  assert.equal(domain.stats().orphans, 0);

  receipt(domain, "m-late", "t.h", "completed", { ok: 1 });
  const value = await domain.waitResult("m-late", { timeout: 1 });
  assert.deepEqual(value.data, { ok: 1 });
});

test("unknown_message names the message id", async () => {
  const domain = makeDomain();
  await assert.rejects(domain.waitResult("never-seen"), (error) => {
    assert.equal(error.status, "unknown_message");
    assert.equal(error.data.message_id, "never-seen");
    return true;
  });
});

// ── 传输失败与可观测性 ────────────────────────────────────────────────
test("send after shutdown is rejected locally instead of waiting for a timeout", async () => {
  const domain = makeDomain();
  domain._closed = true;
  await assert.rejects(domain.send("t.h", "T"), (error) => {
    assert.equal(error.status, "local_backpressure");
    assert.equal(error.data.submitted, false);
    assert.equal(error.data.reason, "sdk closed");
    return true;
  });
});

test("transport loss settles a pending send instead of hanging", async () => {
  const domain = makeDomain();
  const task = domain.send("t.h", "T");
  await tick();
  domain._onTransportClosed("socket closed");
  await assert.rejects(task, (error) => error.status === "transport_closed");
  assert.equal(domain.stats().transport_closed, true);
});

test("transport loss rejects a waiting result", async () => {
  const domain = makeDomain();
  const send = domain.send("t.h", "T");
  await tick();
  receipt(domain, "m-wait", "t.h", "sent", null, domain.frames[0].seq);
  await send;
  const waiting = domain.waitResult("m-wait", { timeout: 30 });
  await tick();
  domain._onTransportClosed("eof");
  await assert.rejects(waiting, (error) => error.status === "transport_closed");
});

test("an unknown receipt status is counted instead of being treated as non-terminal", () => {
  const domain = makeDomain();
  domain._receiveResult({ frame_type: "receipt", message_id: "m", target: "t.h", status: "weird_status", seq: 1 });
  assert.equal(domain.stats().unknown_statuses, 1);
});

test("a receipt with no submission is counted as unmatched", () => {
  const domain = makeDomain();
  domain._receiveResult({ frame_type: "receipt", message_id: "m-x", target: "t.h", status: "completed", seq: 999 });
  assert.equal(domain.stats().unmatched_receipts, 1);
});
