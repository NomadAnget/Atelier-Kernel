// C4 消息文件对象编解码（Node）。
//
// 发送：文件句柄 → { "__atelier_file__": object_id }
// 接收：{ "__atelier_file__": object_id } → 原生文件句柄（由 runtime 注入解析器）
//
// 业务域只在消息里看到文件句柄，不接触 object_id / physical_path / StorageObjectGet。

"use strict";

const { FILE_KEY } = { FILE_KEY: "__atelier_file__" };

function isFileHandle(value) {
  return Boolean(value) && typeof value.read === "function" && typeof value.path === "string";
}

function encode(value) {
  const ns = globalThis.__atelierVfs;
  if (ns && isFileHandle(value)) {
    const objectId = ns.objectIdForPath(value.path);
    if (objectId) return { [FILE_KEY]: objectId };
  }
  if (Array.isArray(value)) return value.map(encode);
  if (value && typeof value === "object" && value.constructor === Object) {
    const out = {};
    for (const [key, item] of Object.entries(value)) out[key] = encode(item);
    return out;
  }
  return value;
}

async function decode(value, resolver) {
  if (Array.isArray(value)) {
    const out = [];
    for (const item of value) out.push(await decode(item, resolver));
    return out;
  }
  if (value && typeof value === "object" && value.constructor === Object) {
    const keys = Object.keys(value);
    if (keys.length === 1 && keys[0] === FILE_KEY && typeof value[FILE_KEY] === "string") {
      return resolver(value[FILE_KEY]);
    }
    const out = {};
    for (const [key, item] of Object.entries(value)) out[key] = await decode(item, resolver);
    return out;
  }
  return value;
}

module.exports = { FILE_KEY, encode, decode };
