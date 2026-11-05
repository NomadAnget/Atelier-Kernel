// C4 虚拟文件命名空间（薄拦截：只解析显式 vfs:// 前缀路径）。
//
// 只拦截带 prefix（默认 vfs://）的路径；其余一律原生放行，第三方库的文件操作不受影响。
// 拦截层只把虚拟路径解析成 <root>/<object_id> 平铺物理路径，然后交给原生 fs.*。
// 不包装文件对象、不挂钩生命周期、不算 size（size/存在性由内核 resolve 时用 metadata 取）。

"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

class FileNamespace {
  constructor(root, submit, prefix = "vfs://") {
    this.root = path.resolve(root);
    this._submit = submit; // (targets, messageType, params, data) => seq
    this.prefix = prefix;
    this.cache = new Map(); // virtual -> physical
  }

  matches(p) {
    return typeof p === "string" && p.startsWith(this.prefix);
  }

  normalize(p) {
    const raw = String(p).slice(this.prefix.length).replace(/\\/g, "/").replace(/^\/+/, "");
    const parts = raw.split("/").filter((s) => s && s !== "." && s !== "..");
    return parts.join("/") || "unnamed";
  }

  loadIndex(objects) {
    for (const item of objects || []) {
      if (item && item.virtual_path && item.physical_path) {
        this.cache.set(item.virtual_path, item.physical_path);
      }
    }
  }

  _physical(objectId) {
    return path.join(this.root, objectId);
  }

  _register(virtualPath, objectId) {
    if (this._submit) {
      this._submit(
        ["kernel.storage"],
        "StorageObjectPut",
        { virtual_path: virtualPath, object_id: objectId },
        {}
      );
    }
  }

  /** 解析虚拟路径；forWrite=true 时分配新 object_id。 */
  resolve(p, forWrite) {
    const virtual = this.normalize(p);
    let physical = this.cache.get(virtual);
    if (!physical) {
      if (!forWrite) throw new Error("virtual file not found: " + p);
      const objectId = crypto.randomUUID().replace(/-/g, "");
      physical = this._physical(objectId);
      fs.mkdirSync(this.root, { recursive: true });
      this.cache.set(virtual, physical);
      this._register(virtual, objectId);
    }
    return physical;
  }
}

/** 包装 fs 的常见文件操作：只改 vfs:// 路径，其余原样。 */
function install(namespace) {
  globalThis.__atelierVfs = namespace;
  const wrap = (name, forWrite, argIndexes = [0]) => {
    const original = fs[name];
    if (typeof original !== "function") return;
    fs[name] = function (...args) {
      for (const idx of argIndexes) {
        if (namespace.matches(args[idx])) {
          args[idx] = namespace.resolve(args[idx], forWrite);
        }
      }
      return original.apply(fs, args);
    };
  };

  // 写
  wrap("writeFile", true);
  wrap("writeFileSync", true);
  wrap("appendFile", true);
  wrap("appendFileSync", true);
  wrap("createWriteStream", true);
  // 读
  wrap("readFile", false);
  wrap("readFileSync", false);
  wrap("createReadStream", false);
  wrap("stat", false);
  wrap("statSync", false);
  wrap("access", false);
  wrap("accessSync", false);
  wrap("existsSync", false);
  // 删除
  wrap("rm", true);
  wrap("rmSync", true);
  wrap("unlink", true);
  wrap("unlinkSync", true);
  // 重命名（两个路径都可能命中）
  wrap("rename", true, [0, 1]);
  wrap("renameSync", true, [0, 1]);

  // open/openSync：按 flags 判断读或写
  const wrapOpen = (name) => {
    const original = fs[name];
    if (typeof original !== "function") return;
    fs[name] = function (p, flags, ...rest) {
      if (namespace.matches(p)) {
        const forWrite = typeof flags === "string" ? /[wa+]/i.test(flags) : true;
        p = namespace.resolve(p, forWrite);
      }
      return original.call(fs, p, flags, ...rest);
    };
  };
  wrapOpen("open");
  wrapOpen("openSync");

  // fs.promises
  const promises = fs.promises;
  if (promises) {
    const wrapP = (name, forWrite, argIndexes = [0]) => {
      const original = promises[name];
      if (typeof original !== "function") return;
      promises[name] = function (...args) {
        for (const idx of argIndexes) {
          if (namespace.matches(args[idx])) {
            args[idx] = namespace.resolve(args[idx], forWrite);
          }
        }
        return original.apply(promises, args);
      };
    };
    wrapP("readFile", false);
    wrapP("writeFile", true);
    wrapP("appendFile", true);
    wrapP("stat", false);
    wrapP("access", false);
    wrapP("rm", true);
    wrapP("unlink", true);
    wrapP("rename", true, [0, 1]);
  }
}

module.exports = { FileNamespace, install };
