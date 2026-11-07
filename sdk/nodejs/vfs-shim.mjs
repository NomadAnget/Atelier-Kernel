// C4 ESM shim: vfs-aware re-export of `node:fs/promises`.
//
// Loaded through vfs-loader.mjs so domain code can keep the idiomatic
// `import {writeFile} from 'node:fs/promises'` and still hit `vfs://` paths.
// Only paths matching the active FileNamespace prefix are resolved; everything
// else falls through to the real implementation with identical semantics.

import * as real from "node:fs/promises";

function ns() {
  return globalThis.__atelierVfs || null;
}

function rp(p, forWrite) {
  const n = ns();
  if (n && typeof p === "string" && n.matches(p)) return n.resolve(p, forWrite);
  return p;
}

const wrap = (fn, forWrite) =>
  function (p, ...rest) {
    const n = ns();
    const vp = n && typeof p === "string" ? n.vpath(p) : null;
    if (!vp) return fn.call(this, p, ...rest);
    const result = fn.call(this, rp(p, forWrite), ...rest);
    if (forWrite && result && typeof result.then === "function") {
      return result.then((value) => {
        n.commit(p);
        return value;
      });
    }
    return result;
  };

// path-taking operations, wrapped
export const readFile = wrap(real.readFile, false);
export const writeFile = wrap(real.writeFile, true);
export const appendFile = wrap(real.appendFile, true);
export const open = async (p, flags, ...rest) => {
  const n = ns();
  const vp = n && typeof p === "string" ? n.vpath(p) : null;
  if (!vp) return real.open.call(real, p, flags, ...rest);
  const forWrite = /[wax]/.test(flags) && !flags.includes("+");
  const handle = await real.open.call(real, rp(p, forWrite), flags, ...rest);
  if (!forWrite) return handle;
  const originalClose = handle.close.bind(handle);
  handle.close = async () => {
    await originalClose();
    n.commit(p);
  };
  return handle;
};
export const stat = wrap(real.stat, false);
export const lstat = wrap(real.lstat, false);
export const access = wrap(real.access, false);
export const rm = async (p, ...rest) => {
  const n = ns();
  if (n && typeof p === "string" && n.vpath(p)) return n.remove(p);
  return real.rm.call(real, p, ...rest);
};
export const unlink = async (p) => {
  const n = ns();
  if (n && typeof p === "string" && n.vpath(p)) return n.remove(p);
  return real.unlink.call(real, p);
};
export const rename = (a, b, ...rest) => real.rename(rp(a, true), rp(b, true), ...rest);
export const copyFile = (a, b, ...rest) => real.copyFile(rp(a, false), rp(b, true), ...rest);

// untouched pass-throughs
export const mkdir = real.mkdir;
export const mkdtemp = real.mkdtemp;
export const readdir = real.readdir;
export const realpath = real.realpath;
export const rmdir = real.rmdir;
export const truncate = real.truncate;
export const utimes = real.utimes;
export const chmod = real.chmod;
export const chown = real.chown;
export const link = real.link;
export const symlink = real.symlink;
export const readlink = real.readlink;
export const opendir = real.opendir;
export const cp = real.cp;
export const constants = real.constants;

export default {
  ...real,
  readFile,
  writeFile,
  appendFile,
  open,
  stat,
  lstat,
  access,
  rm,
  unlink,
  rename,
  copyFile,
};
