// Atelier SDK v2（Node）：单层、全异步、无遗留别名。15 项能力见 docs/sdk/nodejs.md。
"use strict";

const crypto = require("crypto");
const fs = require("fs");
const net = require("net");
const os = require("os");
const path = require("path");

const TERMINAL = new Set(["completed", "execution_failed", "target_invalid", "contract_invalid"]);
// 已停止等待、但帧已写入内核的提交，其回执归属最多保留这么多条；超出按先进先出丢弃并计数。
const ORPHAN_LIMIT = 4096;
const SYSTEM_EVENTS = { LogEvent: "log", NotifyEvent: "notify", ObservationEvent: "observation", TelemetryEvent: "telemetry" };

class DomainRequestError extends Error {
  constructor(status, data = null, messageId = null) {
    super(status);
    this.status = status;
    this.data = data;
    this.message_id = messageId;
  }
}

class DomainExecutionFailed extends Error {
  constructor(payload) {
    super(JSON.stringify(payload));
    this.payload = payload;
  }
}

class FileNamespace {
  constructor(root, submit, prefix = "vfs://") {
    this.root = path.resolve(root);
    this._submit = submit;
    this.prefix = prefix;
    this.cache = new Map();
    this.byPhysical = new Map();
  }
  matches(p) {
    return typeof p === "string" && p.startsWith(this.prefix);
  }
  normalize(p) {
    const raw = String(p).slice(this.prefix.length).replace(/\\/g, "/").replace(/^\/+/, "");
    return raw.split("/").filter((x) => x && x !== "." && x !== "..").join("/") || "unnamed";
  }
  resolve(p, forWrite) {
    const virtual = this.normalize(p);
    let physical = this.cache.get(virtual);
    if (!physical) {
      if (!forWrite) throw Object.assign(new Error("virtual file not found: " + p), { code: "ENOENT" });
      const objectId = crypto.randomUUID().replace(/-/g, "");
      physical = path.join(this.root, objectId);
      fs.mkdirSync(this.root, { recursive: true });
      this.cache.set(virtual, physical);
      this.byPhysical.set(path.resolve(physical), objectId);
      this._submit(virtual, objectId);
    }
    return physical;
  }
  objectId(p) {
    const physical = this.cache.get(this.normalize(p));
    return physical ? this.byPhysical.get(path.resolve(physical)) || null : null;
  }
  objectIdForPath(physical) {
    try {
      return this.byPhysical.get(path.resolve(physical)) || null;
    } catch (_) {
      return null;
    }
  }
}

class AtelierDomain {
  constructor(domain) {
    this.domain = domain;
    this._handlers = new Map();
    this._descriptions = new Map();
    this._pending = new Map();
    this._results = new Map();
    // 显式超时后帧已经写进管道：内核仍可能回执，归属必须保留，不能丢掉内核发出的回执。
    this._orphans = new Map();
    this._orphanLimit = ORPHAN_LIMIT;
    this._transportClosed = false;
    this._stats = {
      dropped_orphan_receipts: 0,
      local_backpressure: 0,
      unmatched_receipts: 0,
      malformed_frames: 0,
      unknown_statuses: 0,
      dropped_frames: 0,
    };
    this._events = { receipt: [], log: [], notify: [], observation: [], telemetry: [] };
    this._seq = 1;
    this._namespace = null;
    this._handles = new WeakMap();
    this._vfsFiles = new Map();
    this._stagingFiles = new Map();
    this._filesRoot = null;
    this._logger = new Logger(this);
    this._closed = false;
    this._input = null;
    this._output = null;
    this._paths = IPC_PATHS;
  }

  // 能力 2：注册处理器
  handler(name, fn, description) {
    this._handlers.set(name, fn);
    if (typeof description === "string" && description.trim()) {
      this._descriptions.set(name, description.trim());
    }
    return this;
  }

  // 能力 3：发消息（返回消息 id）
  async send(targets, messageType, params = {}, data = {}, options = {}) {
    if (this._closed || this._transportClosed) {
      // 端到端已关闭：这条消息不可能交给内核，必须显式拒绝而不是静默丢弃后等超时。
      this._stats.local_backpressure += 1;
      throw new DomainRequestError(
        "local_backpressure",
        { submitted: false, reason: this._closed ? "sdk closed" : "transport closed" },
        null
      );
    }
    const names = typeof targets === "string" ? [targets] : [...new Set(targets)];
    // 超时是调用方的选择，不是 SDK 的策略：不传就一直等内核的初始回执。
    const timeout = options.timeout ?? null;
    params = this._encodeFiles(params);
    data = this._encodeFiles(data);
    const seq = this._seq++;
    const promise = new Promise((resolve, reject) => {
      this._pending.set(seq, { targets: names, ids: new Map(), resolve, reject, seq });
    });
    this._write({ frame_type: "send", seq, targets: names, message_type: messageType, params, data });
    try {
      const ids = await withTimeout(promise, timeout);
      return typeof targets === "string" ? ids[0] : ids;
    } catch (error) {
      // 帧已在管道里：本地停止等待，但内核的回执仍会到来，保留它的归属。
      const entry = this._pending.get(seq);
      if (entry) this._rememberOrphan(seq, entry);
      throw error;
    } finally {
      this._pending.delete(seq);
    }
  }

  _rememberOrphan(seq, entry) {
    if (this._orphanLimit <= 0) {
      this._stats.dropped_orphan_receipts += 1;
      return;
    }
    this._orphans.set(seq, entry);
    while (this._orphans.size > this._orphanLimit) {
      this._orphans.delete(this._orphans.keys().next().value);
      this._stats.dropped_orphan_receipts += 1;
    }
  }

  // 能力 4：请求（send + 等终态 + forget）
  async request(target, messageType, params = {}, data = {}, options = {}) {
    const timeout = options.timeout ?? null;
    const messageId = await this.send(target, messageType, params, data, { timeout });
    try {
      return await this.waitResult(messageId, { timeout });
    } finally {
      this.forgetResult(messageId);
    }
  }

  // 能力 5：等待 / 遗忘
  async waitResult(messageId, options = {}) {
    const entry = this._results.get(messageId);
    if (!entry) {
      throw new DomainRequestError("unknown_message", { message_id: messageId }, messageId);
    }
    entry.waiters++;
    try {
      const receipt = await withTimeout(entry.promise, options.timeout ?? null);
      entry.observed = true;
      if (receipt.status !== "completed") {
        throw new DomainRequestError(receipt.status, receipt.data, messageId);
      }
      return receipt;
    } finally {
      entry.waiters--;
      if (entry.observed && !entry.waiters) this._results.delete(messageId);
    }
  }

  async waitResults(messageIds, options = {}) {
    return Promise.all(messageIds.map((id) => this.waitResult(id, options)));
  }

  forgetResult(messageId) {
    const entry = this._results.get(messageId);
    if (entry?.waiters) throw new DomainRequestError("result_in_use", null, messageId);
    return this._results.delete(messageId);
  }

  // 本地队列与丢回执的可观测性：这些计数只描述 SDK 自己的内存，不代表内核状态。
  stats() {
    return {
      pending: this._pending.size,
      results: this._results.size,
      orphans: this._orphans.size,
      dropped_orphan_receipts: this._stats.dropped_orphan_receipts,
      local_backpressure: this._stats.local_backpressure,
      unmatched_receipts: this._stats.unmatched_receipts,
      malformed_frames: this._stats.malformed_frames,
      unknown_statuses: this._stats.unknown_statuses,
      dropped_frames: this._stats.dropped_frames,
      transport_closed: this._transportClosed,
    };
  }

  // 能力 6：上报回执
  reportReceipt(messageId, target, status, data = {}) {
    this._write({ frame_type: "receipt", message_id: messageId, target, status, data });
  }

  // 能力 8：本域存储
  async _storage(messageType, params, data = {}) {
    return (await this.request("kernel.storage", messageType, params, data)).data;
  }
  async storageInsert(recordId, category, data, upsert = true) {
    return this._storage("StorageInsert", { record_id: recordId, category, upsert }, data);
  }
  async storageUpdate(recordId, data, patch = true) {
    return this._storage("StorageUpdate", { record_id: recordId, patch }, data);
  }
  async storageDelete(recordId, category) {
    const params = {};
    if (recordId !== undefined) params.record_id = recordId;
    if (category !== undefined) params.category = category;
    return this._storage("StorageDelete", params);
  }
  async storageQuery(options = {}) {
    const params = { limit: options.limit ?? 500 };
    for (const key of ["domain", "record_id", "category", "filter"]) {
      if (options[key] !== undefined) params[key] = options[key];
    }
    return this._storage("StorageQuery", params);
  }

  // 能力 9：查询
  async _query(target, messageType, params) {
    return (await this.request(target, messageType, params)).data;
  }
  async queryLogs(params = {}) {
    return this._query("kernel.logger", "QueryLogs", params);
  }
  async queryHandlers(params = {}) {
    return this._query("kernel.registry", "QueryHandlers", params);
  }
  async queryRuntime(params = {}) {
    return this._query("kernel.runtime", "QueryRuntime", params);
  }
  async queryObserver(params = {}) {
    return this._query("kernel.observer", "QueryObserver", params);
  }
  async queryProgress() {
    return this._query("kernel.logger", "QueryProgress", {});
  }

  // 能力 10：监督
  async _supervisor(action, params) {
    return (await this.request("kernel.supervisor", action, params)).data;
  }
  async supervisorStart(domain, spec) {
    const params = { domain };
    if (spec !== undefined) params.spec = spec;
    return this._supervisor("StartDomain", params);
  }
  async supervisorStop(domain) {
    return this._supervisor("StopDomain", { domain });
  }
  async supervisorRestart(domain) {
    return this._supervisor("RestartDomain", { domain });
  }
  async supervisorUpdate(domain, spec, probeHandler) {
    const params = { domain };
    if (spec !== undefined) params.spec = spec;
    if (probeHandler !== undefined) params.probe_handler = probeHandler;
    return this._supervisor("UpdateDomain", params);
  }

  // 能力 11：订阅
  async _subscribe(kind, topic, enabled) {
    return (await this.request(`kernel.${kind}`, (enabled ? "Subscribe" : "Unsubscribe") + topic, {})).data;
  }
  async subscribeLogs() { return this._subscribe("logger", "Logs", true); }
  async unsubscribeLogs() { return this._subscribe("logger", "Logs", false); }
  async subscribeTelemetry() { return this._subscribe("telemetry", "Telemetry", true); }
  async unsubscribeTelemetry() { return this._subscribe("telemetry", "Telemetry", false); }
  async subscribeNotify() { return this._subscribe("notify", "Notify", true); }
  async unsubscribeNotify() { return this._subscribe("notify", "Notify", false); }
  async subscribeObserver() { return this._subscribe("observer", "Observer", true); }
  async unsubscribeObserver() { return this._subscribe("observer", "Observer", false); }

  // 能力 12：通知
  async notify(level, message, title = "", details = {}) {
    return (await this.request("kernel.notify", title || "Notification", { severity: level, level }, { title, message, details })).data;
  }

  // ── 能力 13：C4 文件对象（逻辑路径寻址；object_id 只在核心里） ──────────
  _vpath(p) {
    return typeof p === "string" && p.startsWith("vfs://")
      ? p.slice("vfs://".length).replace(/^\/+/, "")
      : null;
  }

  _stagingDir() {
    const filesRoot = this._filesRoot || process.env.ATELIER_FILES_ROOT;
    if (!filesRoot) throw new Error("C4: ATELIER_FILES_ROOT 未设置，无法定位暂存目录");
    const absolute = path.resolve(filesRoot);
    const owner = path.basename(absolute);
    const dir = path.join(path.dirname(absolute), ".staging", owner);
    fs.mkdirSync(dir, { recursive: true });
    return dir;
  }

  _newStaging() {
    return path.join(this._stagingDir(), `${crypto.randomUUID()}.part`);
  }

  async _adopt(virtualPath, staging) {
    try {
      const meta = (await this.request("kernel.storage", "StorageObjectPut", {
        virtual_path: virtualPath,
        staging_path: staging,
      })).data;
      this._stagingFiles.delete(virtualPath);
      this._vfsFiles.set(virtualPath, meta);
      return meta;
    } catch (error) {
      this._stagingFiles.set(virtualPath, staging);
      throw error;
    }
  }

  _sendAdopt(virtualPath, staging) {
    this._stagingFiles.set(virtualPath, staging);
    this._write({
      frame_type: "send",
      seq: this._seq++,
      targets: ["kernel.storage"],
      message_type: "StorageObjectPut",
      params: { virtual_path: virtualPath, staging_path: staging },
      data: {},
    });
  }

  async _resolveRead(virtualPath) {
    let meta = this._vfsFiles.get(virtualPath);
    if (!meta) {
      meta = (await this.request("kernel.storage", "StorageObjectGet", { virtual_path: virtualPath })).data;
      this._vfsFiles.set(virtualPath, meta);
    }
    return meta;
  }

  _resolveReadSync(virtualPath) {
    const meta = this._vfsFiles.get(virtualPath);
    if (!meta) throw new Error(`vfs 同步读取需要先 getObject()/listObjects(): ${virtualPath}`);
    return meta;
  }

  enableFileInterception(root) {
    this._filesRoot = root || process.env.ATELIER_FILES_ROOT || ".";
    this._stagingDir();
    this._patchFs();
    globalThis.__atelierVfs = {
      matches: (p) => this._vpath(p) !== null,
      vpath: (p) => this._vpath(p),
    };
    return this;
  }

  _patchFs() {
    const sdk = this;
    const vpath = (p) => sdk._vpath(p);
    const isWriteMode = (flags) => typeof flags === "string" && /[wax]/.test(flags) && !flags.includes("+");

    for (const name of ["writeFile", "appendFile"]) {
      const original = fs.promises[name];
      if (typeof original === "function") {
        fs.promises[name] = async (p, ...rest) => {
          const vp = vpath(p);
          if (!vp) return original.call(fs.promises, p, ...rest);
          const staging = sdk._newStaging();
          await original.call(fs.promises, staging, ...rest);
          return sdk._adopt(vp, staging);
        };
      }
      const syncOriginal = fs[name + "Sync"];
      if (typeof syncOriginal === "function") {
        fs[name + "Sync"] = (p, ...rest) => {
          const vp = vpath(p);
          if (!vp) return syncOriginal.call(fs, p, ...rest);
          const staging = sdk._newStaging();
          const result = syncOriginal.call(fs, staging, ...rest);
          sdk._sendAdopt(vp, staging);
          return result;
        };
      }
    }

    for (const name of ["readFile", "stat", "access"]) {
      const original = fs.promises[name];
      if (typeof original === "function") {
        fs.promises[name] = async (p, ...rest) => {
          const vp = vpath(p);
          if (!vp) return original.call(fs.promises, p, ...rest);
          const meta = await sdk._resolveRead(vp);
          return original.call(fs.promises, meta.physical_path, ...rest);
        };
      }
      const syncOriginal = fs[name + "Sync"];
      if (typeof syncOriginal === "function") {
        fs[name + "Sync"] = (p, ...rest) => {
          const vp = vpath(p);
          if (!vp) return syncOriginal.call(fs, p, ...rest);
          return syncOriginal.call(fs, sdk._resolveReadSync(vp).physical_path, ...rest);
        };
      }
    }

    for (const name of ["unlink", "rm"]) {
      const original = fs.promises[name];
      if (typeof original === "function") {
        fs.promises[name] = async (p, ...rest) => {
          const vp = vpath(p);
          if (!vp) return original.call(fs.promises, p, ...rest);
          return sdk.deleteObject(vp);
        };
      }
      const syncOriginal = fs[name + "Sync"];
      if (typeof syncOriginal === "function") {
        fs[name + "Sync"] = (p, ...rest) => {
          const vp = vpath(p);
          if (!vp) return syncOriginal.call(fs, p, ...rest);
          sdk._write({
            frame_type: "send",
            seq: sdk._seq++,
            targets: ["kernel.storage"],
            message_type: "StorageObjectDelete",
            params: { virtual_path: vp },
            data: {},
          });
          sdk._vfsFiles.delete(vp);
          return undefined;
        };
      }
    }

    const originalPromiseOpen = fs.promises.open;
    fs.promises.open = async (p, flags, ...rest) => {
      const vp = vpath(p);
      if (!vp) return originalPromiseOpen.call(fs.promises, p, flags, ...rest);
      if (isWriteMode(flags)) {
        const staging = sdk._newStaging();
        const handle = await originalPromiseOpen.call(fs.promises, staging, flags, ...rest);
        const originalClose = handle.close.bind(handle);
        handle.close = async () => {
          await originalClose();
          await sdk._adopt(vp, staging);
        };
        handle.virtual_path = vp;
        handle.owner = sdk.domain;
        return handle;
      }
      const meta = await sdk._resolveRead(vp);
      const handle = await originalPromiseOpen.call(fs.promises, meta.physical_path, flags, ...rest);
      handle.virtual_path = vp;
      handle.owner = meta.owner;
      handle.file_meta = meta;
      return handle;
    };

    const originalCreateWriteStream = fs.createWriteStream;
    fs.createWriteStream = (p, options) => {
      const vp = vpath(p);
      if (!vp) return originalCreateWriteStream.call(fs, p, options);
      const staging = sdk._newStaging();
      const stream = originalCreateWriteStream.call(fs, staging, options);
      stream.on("close", () => {
        sdk._adopt(vp, staging).catch((error) => sdk._logger.error(`vfs 归档失败 ${vp}: ${error.message}`));
      });
      return stream;
    };
  }

  async putObject(virtualPath) {
    const staging = this._stagingFiles.get(virtualPath);
    if (!staging) return this.getObject(virtualPath);
    return this._adopt(virtualPath, staging);
  }

  async getObject(virtualPath, owner) {
    const params = { virtual_path: virtualPath };
    if (owner) params.owner = owner;
    const meta = (await this.request("kernel.storage", "StorageObjectGet", params)).data;
    if (!owner) this._vfsFiles.set(virtualPath, meta);
    return meta;
  }

  async deleteObject(virtualPath, owner) {
    const params = { virtual_path: virtualPath };
    if (owner) params.owner = owner;
    const result = (await this.request("kernel.storage", "StorageObjectDelete", params)).data;
    this._vfsFiles.delete(virtualPath);
    const staging = this._stagingFiles.get(virtualPath);
    this._stagingFiles.delete(virtualPath);
    if (staging) fs.rm(staging, { force: true }, () => {});
    return result;
  }

  async listObjects(owner) {
    const data = (await this.request("kernel.storage", "StorageObjectList", owner ? { owner } : {})).data;
    if (!owner) for (const item of data.objects || []) this._vfsFiles.set(item.virtual_path, item);
    return data;
  }
  _encodeFiles(value) {
    if (value && typeof value === "object" && (typeof value.read === "function" || typeof value.close === "function") && typeof value.virtual_path === "string" && typeof value.owner === "string") {
      return { __atelier_file__: { owner: value.owner, virtual_path: value.virtual_path } };
    }
    if (Array.isArray(value)) return value.map((item) => this._encodeFiles(item));
    if (value && typeof value === "object" && value.constructor === Object) {
      const out = {};
      for (const [key, item] of Object.entries(value)) out[key] = this._encodeFiles(item);
      return out;
    }
    return value;
  }

  async _decodeFiles(value) {
    if (Array.isArray(value)) {
      const out = [];
      for (const item of value) out.push(await this._decodeFiles(item));
      return out;
    }
    if (value && typeof value === "object" && value.constructor === Object) {
      const keys = Object.keys(value);
      const ref = value.__atelier_file__;
      if (keys.length === 1 && ref && typeof ref === "object") {
        const meta = await this.getObject(ref.virtual_path, ref.owner);
        const handle = await fs.promises.open(meta.physical_path, "r+");
        handle.virtual_path = ref.virtual_path;
        handle.owner = ref.owner || meta.owner;
        handle.file_meta = meta;
        return handle;
      }
      const out = {};
      for (const [key, item] of Object.entries(value)) out[key] = await this._decodeFiles(item);
      return out;
    }
    return value;
  }

  // 能力 14：日志
  get logger() {
    return this._logger;
  }

  // 能力 15：端口
  get bindings() {
    try {
      return JSON.parse(process.env.ATELIER_PORT_BINDINGS || "{}");
    } catch (_) {
      return {};
    }
  }

  // 能力 7：事件
  on(kind, fn) {
    this._events[kind].push(fn);
    return this;
  }
  onReceipt(fn) { return this.on("receipt", fn); }
  onLog(fn) { return this.on("log", fn); }
  onNotify(fn) { return this.on("notify", fn); }
  onObservation(fn) { return this.on("observation", fn); }
  onTelemetry(fn) { return this.on("telemetry", fn); }
  _emit(kind, payload) {
    for (const fn of this._events[kind]) {
      try { fn(payload); } catch (_) {}
    }
  }

  // 能力 1：生命周期
  async runAsync({ onStart, onClose } = {}) {
    if (!this._paths) throw new Error("IPC endpoints are not configured (ATELIER_IPC_IN / ATELIER_IPC_OUT)");
    this._streams = openStreams(this._paths);
    this._input = this._streams.input;
    this._output = this._streams.output;
    this._readLoop();
    this._write({ frame_type: "send", seq: this._seq++, targets: ["kernel.registry"], message_type: "RegisterHandlers", params: { handlers: [...this._handlers.keys()], descriptions: Object.fromEntries(this._descriptions) }, data: {} });
    if (onStart) {
      try {
        await onStart();
      } catch (error) {
        this._write({ frame_type: "ready", error: String(error && error.message || error) });
        throw error;
      }
    }
    if (process.env.ATELIER_MANAGED_READY === "1") {
      this._write({ frame_type: "ready" });
    }
    this._done = new Promise((resolve) => { this._finish = resolve; });
    await this._done;
    if (onClose) await onClose();
  }
  run(options) {
    return this.runAsync(options);
  }
  shutdown() {
    this._closed = true;
    if (this._finish) this._finish();
    if (this._streams?.input) this._streams.input.destroy();
    if (this._streams?.output) this._streams.output.destroy();
  }

  _write(frame) {
    if (this._closed || !this._output) {
      this._stats.dropped_frames += 1;
      return;
    }
    this._output.write(JSON.stringify(frame) + "\n");
  }

  _readLoop() {
    const readline = require("readline");
    this._rl = readline.createInterface({ input: this._input, terminal: false });
    this._rl.on("line", (line) => {
      const trimmed = line.trim();
      if (!trimmed) return;
      let frame;
      try {
        frame = JSON.parse(trimmed);
      } catch (_) {
        this._stats.malformed_frames += 1;
        return;
      }
      this._handle(frame);
    });
    this._rl.on("close", () => this._onTransportClosed("read stream closed"));
    if (this._input?.on) {
      this._input.on("error", (error) => this._onTransportClosed(String(error?.message || error)));
    }
  }

  // 传输死亡是内核已经给出结论的场景（在途消息会被内核收敛为 ExecutionFailed）。
  // SDK 不能沉默：用本地 transport_closed 终结所有等待，绝不让调用方挂到自己的超时。
  _onTransportClosed(reason) {
    if (this._transportClosed) return;
    this._transportClosed = true;
    const error = new DomainRequestError("transport_closed", { submitted: true, reason }, null);
    for (const entry of this._pending.values()) entry.reject(error);
    this._pending.clear();
    for (const entry of this._orphans.values()) entry.reject?.(error);
    this._orphans.clear();
    for (const [messageId, result] of this._results) {
      if (result.waiters > 0) result.reject?.(error);
      this._results.delete(messageId);
    }
  }

  _handle(frame) {
    if (frame.frame_type === "receipt") {
      this._receiveResult(frame);
      this._emit("receipt", frame);
      return;
    }
    const kind = SYSTEM_EVENTS[frame.message_type];
    if (kind) {
      this._emit(kind, frame.data || {});
      return;
    }
    this._dispatch(frame).catch(() => {});
  }

  _receiveResult(frame) {
    const messageId = frame.message_id;
    const target = frame.target;
    const status = frame.status;
    if (typeof messageId !== "string") return;
    const nonTerminal = status === "sent" || status === "received" || status === "started";
    if (!nonTerminal && !TERMINAL.has(status)) {
      // 内核给出了 SDK 不认识的状态：计入可观测计数，绝不静默当作非终态。
      this._stats.unknown_statuses += 1;
      return;
    }
    // 本地已停止等待的提交同样要认出归属：内核已经发出的回执不能被丢弃。
    const entry = this._pending.get(frame.seq) ?? this._orphans.get(frame.seq);
    let claimed = false;
    if (entry && !entry.ids.has(target) && entry.targets.includes(target)) {
      entry.ids.set(target, messageId);
      let resolve;
      let reject;
      const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
      const result = { target, promise, resolve, reject, waiters: 0, observed: false, evict: null };
      this._results.set(messageId, result);
      claimed = true;
      if (entry.ids.size === entry.targets.length) {
        entry.resolve(entry.targets.map((t) => entry.ids.get(t)));
        this._orphans.delete(frame.seq);
      }
    }
    const result = this._results.get(messageId);
    if (result && result.target === target && !result.observed && TERMINAL.has(status)) {
      // 结果不自动过期：取走或用 forgetResult 释放，寿命由域自己决定。
      result.resolve(frame);
    }
    if (!claimed && !result) {
      // 回执找不到任何归属（超出孤儿窗口、或来自其他代际）：计数，不静默。
      this._stats.unmatched_receipts += 1;
    }
  }

  async _dispatch(frame) {
    const name = String(frame.target || "").split(".").slice(1).join(".");
    const fn = this._handlers.get(name);
    if (!fn) {
      this.reportReceipt(frame.message_id, frame.target, "target_invalid", { error: "unknown handler" });
      return;
    }
    this.reportReceipt(frame.message_id, frame.target, "received");
    this.reportReceipt(frame.message_id, frame.target, "started");
    try {
      frame.data = await this._decodeFiles(frame.data || {});
      const result = await fn(frame);
      this.reportReceipt(frame.message_id, frame.target, "completed", result ?? {});
    } catch (error) {
      this.reportReceipt(frame.message_id, frame.target, "execution_failed", error instanceof DomainExecutionFailed ? error.payload : { error: "handler_exception", reason: String(error?.message || error) });
    }
  }
}

class Logger {
  constructor(domain) {
    this._domain = domain;
  }
  _log(level, message, fields = {}) {
    // 日志走**专用 log 帧**，不是 `send`：它不分配消息身份、不产生四阶段回执，
    // 由内核直接注入 hub_logs / 活动进度表 / 订阅流。
    const { layer = "DOMAIN", component = this._domain.domain, ...rest } = fields;
    this._domain._write({
      frame_type: "log",
      record: { level, layer, component, message, ...rest },
    });
  }
  debug(message, fields) { this._log("debug", message, fields); }
  info(message, fields) { this._log("info", message, fields); }
  warn(message, fields) { this._log("warn", message, fields); }
  error(message, fields) { this._log("error", message, fields); }
}

function claimIpcPaths() {
  const inPath = process.env.ATELIER_IPC_IN;
  const outPath = process.env.ATELIER_IPC_OUT;
  delete process.env.ATELIER_IPC_IN;
  delete process.env.ATELIER_IPC_OUT;
  delete process.env.ATELIER_IPC_PIPE;
  if (inPath === undefined || outPath === undefined) return null;
  return { inPath, outPath };
}

function openStreams(paths) {
  // 两条单向连接：input 读内核数据（内核→域），output 写内核数据（域→内核）。
  const input = net.createConnection(paths.inPath);
  input.pause();
  const output = net.createConnection(paths.outPath);
  return { input, output, bridge: null };
}

function withTimeout(promise, seconds) {
  if (seconds === null || seconds === undefined) return promise;
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new DomainRequestError("timeout")), seconds * 1000)),
  ]);
}

// 与旧 SDK 一致：模块加载时就认领 IPC 端点。
const IPC_PATHS = claimIpcPaths();

module.exports = { AtelierDomain, DomainRequestError, DomainExecutionFailed, FileNamespace };
