// C4 loader entry: `node --import atelier-sdk/vfs-loader.mjs domain.mjs`
// or the domain entry starts with `import 'atelier-sdk/vfs-loader';`.
//
// Registering is idempotent for our purposes: if the hooks are already active
// the extra registration is harmless.

import { register } from "node:module";

if (!globalThis.__atelierVfsLoaded) {
  globalThis.__atelierVfsLoaded = true;
  register("./vfs-hooks.mjs", import.meta.url);
}
