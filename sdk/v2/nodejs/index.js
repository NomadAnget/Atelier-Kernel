// Atelier SDK v2（Node）：单层、全异步、无遗留别名。15 项能力见 sdk/v2/README.md。
"use strict";

const crypto = require("crypto");
const fs = require("fs");
const net = require("net");
const os = require("os");
const path = require("path");
const { spawn } = require("child_process");

const TERMINAL = new Set(["completed", "execution_failed", "target_invalid", "contract_invalid"]);
const SYSTEM_EVENTS = { LogEvent: "log", NotifyEvent: "notify", ObserverEvent: "observation", TelemetryEvent: "telemetry" };

// Windows 上内核传的是 HANDLE，Node 不能直接用；桥接进程把 HANDLE 转成管道。
const BRIDGE_PY = [
  "import ctypes,msvcrt,os,sys,threading",
  "k32=ctypes.windll.kernel32",
  "r_h,w_h,ppid=int(sys.argv[1]),int(sys.argv[2]),int(sys.argv[3])",
  "k32.SetHandleInformation(ctypes.c_void_p(r_h),1,0)",
  "k32.SetHandleInformation(ctypes.c_void_p(w_h),1,0)",
  "h_p=k32.OpenProcess(0x0040,False,ppid)",
  "if h_p:",
  " d=ctypes.c_void_p()",
  " k32.DuplicateHandle(h_p,ctypes.c_void_p(r_h),0,ctypes.byref(d),0,False,1)",
  " k32.DuplicateHandle(h_p,ctypes.c_void_p(w_h),0,ctypes.byref(d),0,False,1)",
  " k32.CloseHandle(h_p)",
  "msvcrt.setmode(0,os.O_BINARY);msvcrt.setmode(1,os.O_BINARY)",
  "r_fd=msvcrt.open_osfhandle(r_h,os.O_RDONLY|os.O_BINARY)",
  "w_fd=msvcrt.open_osfhandle(w_h,os.O_WRONLY|os.O_BINARY)",
  "def p_in():",
  " try:",
  "  while True:",
  "   b=os.read(r_fd,65536)",
  "   if not b:break",
  "   mv=memoryview(b)",
  "   while mv:mv=mv[os.write(1,mv):]",
  " except Exception:pass",
  " finally:",
  "  try:os.close(1)",
  "  except Exception:pass",
  "def p_out():",
  " try:",
  "  while True:",
  "   b=os.read(0,65536)",
  "   if not b:break",
  "   mv=memoryview(b)",
  "   while mv:mv=mv[os.write(w_fd,mv):]",
  " except Exception:pass",
  " finally:",
  "  try:os.close(w_fd)",
  "  except Exception:pass",
  "  os._exit(0)",
  "t1=threading.Thread(target=p_in,daemon=True);t2=threading.Thread(target=p_out,daemon=True)",
  "t1.start();t2.start();t2.join()",
].join("\n");

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
}

class AtelierDomain {
  constructor(domain, options = {}) {
    this.domain = domain;
    this._maxPending = options.maxPending ?? 1024;
    this._resultTtlMs = options.resultTtlMs ?? 30000;
    this._handlers = new Map();
    this._pending = new Map();
    this._results = new Map();
    this._events = { receipt: [], log: [], notify: [], observation: [], telemetry: [] };
    this._seq = 1;
    this._namespace = null;
    this._logger = new Logger(this);
    this._closed = false;
    this._input = null;
    this._output = null;
    this._fds = IPC_FDS;
  }

  // 能力 2：注册处理器
  handler(name, fn) {
    this._handlers.set(name, fn);
    return this;
  }

  // 能力 3：发消息（返回消息 id）
  async send(targets, messageType, params = {}, data = {}, options = {}) {
    const names = typeof targets === "string" ? [targets] : [...new Set(targets)];
    const timeout = options.timeout ?? 30;
    const deadline = Date.now() + timeout * 1000;
    while (this._pending.size + names.length > this._maxPending) {
      if (Date.now() >= deadline) throw new DomainRequestError("busy");
      await new Promise((r) => setTimeout(r, 10));
    }
    const seq = this._seq++;
    const promise = new Promise((resolve, reject) => {
      this._pending.set(seq, { targets: names, ids: new Map(), resolve, reject });
    });
    this._write({ frame_type: "send", seq, targets: names, message_type: messageType, params, data });
    try {
      const ids = await withTimeout(promise, timeout);
      return typeof targets === "string" ? ids[0] : ids;
    } finally {
      this._pending.delete(seq);
    }
  }

  // 能力 4：请求（send + 等终态 + forget）
  async request(target, messageType, params = {}, data = {}, options = {}) {
    const timeout = options.timeout ?? 30;
    const messageId = await this.send(target, messageType, params, data, { timeout });
    try {
      const receipt = await this.waitResult(messageId, { timeout });
      if (receipt.status !== "completed") throw new DomainRequestError(receipt.status, receipt.data, messageId);
      return receipt;
    } finally {
      this.forgetResult(messageId);
    }
  }

  // 能力 5：等待 / 遗忘
  async waitResult(messageId, options = {}) {
    const entry = this._results.get(messageId);
    if (!entry) throw new DomainRequestError("unknown_message", null, messageId);
    if (entry.evict) {
      clearTimeout(entry.evict);
      entry.evict = null;
    }
    entry.waiters++;
    try {
      const receipt = await withTimeout(entry.promise, options.timeout ?? 30);
      entry.observed = true;
      return receipt;
    } finally {
      entry.waiters--;
      if (entry.observed && !entry.waiters) this._results.delete(messageId);
    }
  }

  forgetResult(messageId) {
    const entry = this._results.get(messageId);
    if (entry?.waiters) throw new DomainRequestError("busy", null, messageId);
    if (entry?.evict) clearTimeout(entry.evict);
    return this._results.delete(messageId);
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
    return this._namespace;
  }
  async putObject(virtualPath) {
    return (await this.request("kernel.storage", "StorageObjectPut", { virtual_path: virtualPath })).data;
  }
  async getObject(objectId) {
    return (await this.request("kernel.storage", "StorageObjectGet", { object_id: objectId })).data;
  }
  async deleteObject(objectId) {
    return (await this.request("kernel.storage", "StorageObjectDelete", { object_id: objectId })).data;
  }
  fileObjectId(virtualPath) {
    return this._namespace ? this._namespace.objectId(virtualPath) : null;
  }
  async refreshFileIndex() {
    return (await this.request("kernel.storage", "StorageObjectList", {})).data;
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
    if (!this._fds) throw new Error("IPC pipes are not configured");
    this._streams = openStreams(this._fds);
    this._input = this._streams.input;
    this._output = this._streams.output;
    process.stderr.write("v2 transport fds=" + JSON.stringify(this._fds) + " bridge=" + !!this._streams.bridge + "\n");
    this._write({ frame_type: "send", seq: this._seq++, targets: ["kernel.registry"], message_type: "RegisterHandlers", params: { handlers: [...this._handlers.keys()], descriptions: {} }, data: {} });
    this._readLoop();
    if (onStart) await onStart();
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
    if (this._streams?.bridge) this._streams.bridge.kill();
  }

  _write(frame) {
    if (this._closed || !this._output) return;
    this._output.write(JSON.stringify(frame) + "\n");
  }

  _readLoop() {
    let buffer = "";
    this._input.setEncoding("utf8");
    this._input.on("data", (chunk) => {
      buffer += chunk;
      let index;
      while ((index = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, index).trim();
        buffer = buffer.slice(index + 1);
        if (!line) continue;
        let frame;
        try { frame = JSON.parse(line); } catch (_) { continue; }
        this._handle(frame);
      }
    });
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
    if (typeof messageId !== "string" || !(status === "sent" || status === "received" || status === "started" || TERMINAL.has(status))) return;
    const entry = this._pending.get(frame.seq);
    if (entry && !entry.ids.has(target) && entry.targets.includes(target)) {
      entry.ids.set(target, messageId);
      let resolve;
      const promise = new Promise((yes) => { resolve = yes; });
      const result = { target, promise, resolve, waiters: 0, observed: false, evict: null };
      this._results.set(messageId, result);
      if (entry.ids.size === entry.targets.length) entry.resolve(entry.targets.map((t) => entry.ids.get(t)));
    }
    const result = this._results.get(messageId);
    if (result && result.target === target && !result.observed && TERMINAL.has(status)) {
      result.resolve(frame);
      if (!result.waiters && this._resultTtlMs > 0) {
        result.evict = setTimeout(() => { if (!result.waiters) this._results.delete(messageId); }, this._resultTtlMs);
      }
    }
  }

  async _dispatch(frame) {
    const name = String(frame.target || "").split(".").slice(1).join(".");
    const fn = this._handlers.get(name);
    if (!fn) {
      this.reportReceipt(frame.message_id, frame.target, "target_invalid", { error: "unknown handler" });
      return;
    }
    this.reportReceipt(frame.message_id, frame.target, "started");
    try {
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
    this._domain._write({ frame_type: "send", seq: this._domain._seq++, targets: ["kernel.logger"], message_type: "Log", params: { level, component: this._domain.domain }, data: { message, ...fields } });
  }
  debug(message, fields) { this._log("debug", message, fields); }
  info(message, fields) { this._log("info", message, fields); }
  warn(message, fields) { this._log("warn", message, fields); }
  error(message, fields) { this._log("error", message, fields); }
}

function claimIpcFds() {
  const readRaw = process.env.ATELIER_IPC_READ_FD;
  const writeRaw = process.env.ATELIER_IPC_WRITE_FD;
  const handleMode = process.env.ATELIER_IPC_HANDLE_MODE || "";
  delete process.env.ATELIER_IPC_READ_FD;
  delete process.env.ATELIER_IPC_WRITE_FD;
  delete process.env.ATELIER_IPC_HANDLE_MODE;
  if (readRaw === undefined || writeRaw === undefined) return null;
  return { readFd: Number.parseInt(readRaw, 10), writeFd: Number.parseInt(writeRaw, 10), handleMode };
}

function openStreams(fds) {
  if (process.platform === "win32" && (fds.handleMode === "win32" || fds.handleMode === "win32_handle")) {
    const python = process.env.PYTHON || process.env.PYTHON_BIN || "python";
    const bridge = spawn(python, ["-u", "-c", BRIDGE_PY, String(fds.readFd), String(fds.writeFd), String(process.pid)], {
      stdio: ["pipe", "pipe", "inherit"],
      windowsHide: true,
    });
    bridge.stdout.pause();
    bridge.on("spawn", () => process.stderr.write("v2 bridge spawned pid=" + bridge.pid + "\n"));
    bridge.on("exit", (code) => process.stderr.write("v2 bridge exit " + code + "\n"));
    bridge.on("error", (error) => process.stderr.write("v2 bridge error " + String(error && error.message) + "\n"));
    return { input: bridge.stdout, output: bridge.stdin, bridge };
  }
  const input = new net.Socket({ fd: fds.readFd, readable: true, writable: false });
  input.pause();
  const output = new net.Socket({ fd: fds.writeFd, readable: false, writable: true });
  return { input, output, bridge: null };
}

function withTimeout(promise, seconds) {
  if (seconds === null) return promise;
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new DomainRequestError("timeout")), seconds * 1000)),
  ]);
}

// 与旧 SDK 一致：模块加载时就认领 IPC 句柄。
const IPC_FDS = claimIpcFds();

module.exports = { AtelierDomain, DomainRequestError, DomainExecutionFailed, FileNamespace };
