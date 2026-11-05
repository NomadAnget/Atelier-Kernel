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
