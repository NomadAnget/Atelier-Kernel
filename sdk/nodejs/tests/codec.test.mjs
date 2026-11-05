import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { encode, decode, FILE_KEY } = require("../codec.js");

// 真实 FileHandle 是类实例（constructor !== Object），这里用类实例模拟。
class FakeHandle {
  constructor(p) {
    this.path = p;
  }
  read() {
    return "";
  }
}

test("encode turns a file handle into an object reference", () => {
  globalThis.__atelierVfs = { objectIdForPath: (p) => (String(p).endsWith("x.bin") ? "obj-1" : null) };
  const handle = new FakeHandle("G:/x.bin");
  assert.deepEqual(encode({ f: handle }), { f: { [FILE_KEY]: "obj-1" } });
  delete globalThis.__atelierVfs;
});

test("encode keeps handles without an object id untouched", () => {
  globalThis.__atelierVfs = { objectIdForPath: () => null };
  const handle = new FakeHandle("G:/other.bin");
  assert.equal(encode({ f: handle }).f, handle);
  delete globalThis.__atelierVfs;
});

test("decode resolves an object reference", async () => {
  const out = await decode({ f: { [FILE_KEY]: "obj-1" } }, async (id) => "H:" + id);
  assert.deepEqual(out, { f: "H:obj-1" });
});

test("decode recurses into arrays", async () => {
  const out = await decode({ a: [{ [FILE_KEY]: "id1" }] }, async (id) => "H:" + id);
  assert.deepEqual(out, { a: ["H:id1"] });
});

test("decode passes non-reference values through", async () => {
  assert.deepEqual(await decode({ n: 1, s: "x" }, async (id) => id), { n: 1, s: "x" });
});
