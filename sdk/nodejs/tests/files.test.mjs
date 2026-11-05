import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const require = createRequire(import.meta.url);
const { FileNamespace } = require("../files.js");

test("vfs writes resolve under the domain root and register an object id", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "c4-ns-"));
  const registered = [];
  const ns = new FileNamespace(root, (targets, type, params) => registered.push({ targets, type, params }));

  const physical = ns.resolve("vfs://a/b.bin", true);
  assert.equal(path.dirname(physical), path.resolve(root));
  assert.equal(path.basename(physical), ns.objectId("vfs://a/b.bin"));
  assert.equal(ns.objectIdForPath(physical), path.basename(physical));
  assert.equal(registered.length, 1);
  assert.equal(registered[0].type, "StorageObjectPut");
  assert.equal(registered[0].params.virtual_path, "a/b.bin");

  fs.rmSync(root, { recursive: true, force: true });
});

test("loadIndex fills both virtual and physical lookups", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "c4-ns-"));
  const ns = new FileNamespace(root, () => {});
  const physical = path.join(root, "some-id");
  ns.loadIndex([{ virtual_path: "x/y", physical_path: physical, object_id: "some-id" }]);

  assert.equal(ns.cache.get("x/y"), physical);
  assert.equal(ns.objectId("vfs://x/y"), "some-id");
  assert.equal(ns.objectIdForPath(physical), "some-id");

  fs.rmSync(root, { recursive: true, force: true });
});

test("non-vfs paths are never rewritten", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "c4-ns-"));
  const ns = new FileNamespace(root, () => {});
  assert.equal(ns.matches("G:/plain/file.txt"), false);
  assert.equal(ns.matches("vfs://plain/file.txt"), true);
  fs.rmSync(root, { recursive: true, force: true });
});
