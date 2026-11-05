// C4 module resolve hooks: send `node:fs/promises` to the vfs shim.
//
// The shim itself imports `node:fs/promises`, so requests whose parent is the
// shim (or the hooks) must fall through to the real builtin to avoid recursion.

const SHIM_SUFFIXES = ["vfs-shim.mjs", "vfs-hooks.mjs", "vfs-loader.mjs"];
const TARGETS = new Set(["node:fs/promises", "fs/promises"]);

export async function resolve(specifier, context, nextResolve) {
  const parent = context.parentURL || "";
  if (TARGETS.has(specifier) && !SHIM_SUFFIXES.some((s) => parent.endsWith(s))) {
    return {
      url: new URL("./vfs-shim.mjs", import.meta.url).href,
      shortCircuit: true,
    };
  }
  return nextResolve(specifier, context);
}
