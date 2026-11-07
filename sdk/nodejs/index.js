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

  // 能力 13：C4
  enableFileInterception(root) {
    this._namespace = new FileNamespace(root || process.env.ATELIER_FILES_ROOT || ".", (virtual, objectId) =>
      this._write({ frame_type: "send", seq: this._seq++, targets: ["kernel.storage"], message_type: "StorageObjectPut", params: { virtual_path: virtual, object_id: objectId }, data: {} })
    );
    this._patchFs(this._namespace);
    // ESM 命名导入（`import {writeFile} from 'node:fs/promises'`）在模块加载时绑定，
    // 运行时改 fs 对象覆盖不到；vfs-shim.mjs 通过这个全局读取当前实例的命名空间。
    globalThis.__atelierVfs = this._namespace;
    return this._namespace;
  }

  _patchFs(ns) {
    const handles = this._handles;
    const originalPromiseOpen = fs.promises.open;
    const wrap = (holder, name, forWrite, index = 0) => {
      const original = holder[name];
      if (typeof original !== "function") return;
      holder[name] = function (...args) {
        if (ns.matches(args[index])) args[index] = ns.resolve(args[index], forWrite);
        return original.apply(holder, args);
      };
    };
    const table = [
      ["readFile", false],
      ["stat", false],
      ["access", false],
      ["writeFile", true],
      ["appendFile", true],
      ["unlink", true],
      ["rm", true],
    ];
    for (const [name, forWrite] of table) {
      wrap(fs, name, forWrite);
      wrap(fs, name + "Sync", forWrite);
      wrap(fs.promises, name, forWrite);
    }
    // fs.open 的句柄不暴露路径，用 WeakMap 记住「句柄 → object_id」供编解码反查。
    const openHandler = (holder, original, sync) => {
      if (typeof original !== "function") return;
      holder.open = function (p, ...rest) {
        if (ns.matches(p)) {
          const physical = ns.resolve(p, true);
          const objectId = ns.objectIdForPath(physical);
          const result = original.call(holder, physical, ...rest);
          if (!sync && result && typeof result.then === "function") {
            return result.then((handle) => {
              handles.set(handle, objectId);
              return handle;
            });
          }
          return result;
        }
        return original.call(holder, p, ...rest);
      };
    };
    openHandler(fs, fs.open, false);
    openHandler(fs, fs.openSync, true);
    fs.promises.open = function (p, ...rest) {
      if (ns.matches(p)) {
        const physical = ns.resolve(p, true);
        const objectId = ns.objectIdForPath(physical);
        return originalPromiseOpen.call(fs.promises, physical, ...rest).then((handle) => {
          handles.set(handle, objectId);
          return handle;
        });
      }
      return originalPromiseOpen.call(fs.promises, p, ...rest);
    };
  }
  async putObject(virtualPath) {
    return (await this.request("kernel.storage", "StorageObjectPut", { virtual_path: virtualPath })).data;
  }
  async getObject(objectId) {
    return (await this.request("kernel.storage", "StorageObjectGet", { object_id: objectId })).data;
  }
  async commitObject(objectId, size) {
    return (await this.request("kernel.storage", "StorageObjectCommit", { object_id: objectId, size: Number(size) })).data;
  }

  async deleteObject(objectId) {
    // 删除对象 = 删物理文件 + 索引行；跨域允许、幂等。
    return (await this.request("kernel.storage", "StorageObjectDelete", { object_id: objectId })).data;
  }

  async listObjects(owner) {
    return (await this.request("kernel.storage", "StorageObjectList", owner ? { owner } : {})).data;
  }
  fileObjectId(virtualPath) {
    return this._namespace ? this._namespace.objectId(virtualPath) : null;
  }
  async refreshFileIndex() {
    return this.listObjects();
  }

  // 能力 13 配套：消息里的文件句柄编解码（实例级，无全局状态）
  _encodeFiles(value) {
    if (this._handles.has(value)) return { __atelier_file__: this._handles.get(value) };
    if (this._namespace && value && typeof value.read === "function" && typeof value.path === "string" && value.path) {
      const objectId = this._namespace.objectIdForPath(value.path);
      if (objectId) return { __atelier_file__: objectId };
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
      if (keys.length === 1 && keys[0] === "__atelier_file__" && typeof value.__atelier_file__ === "string") {
        const ref = await this.getObject(value.__atelier_file__);
        const handle = await fs.promises.open(ref.physical_path, "r+");
        // C4：物理文件是平铺 id（无扩展名）；逻辑路径随句柄携带，供格式判定。
        handle.object_id = value.__atelier_file__;
        handle.virtual_path = ref.virtual_path;
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
