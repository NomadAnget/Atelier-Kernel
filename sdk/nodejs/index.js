/**
 * Atelier Kernel Official Node.js Domain SDK (`atelier-sdk`).
 *
 * Encapsulates OS Stdio JSON-Lines pipe framing, sequence numbering, handler
 * registration (`kernel.registry`), 4-stage receipt reporting (`Received` ->
 * `Started` -> `Completed` / `ExecutionFailed`), and `kernel.storage` CRUD helpers
 * so business domains only need to `require("atelier-sdk")` and declare handlers.
 */
const fs = require("fs");
const readline = require("readline");

const _sourceFileCache = new Map();

function _cleanBlockComment(raw) {
  return raw
    .replace(/^\/\*+/, "")
    .replace(/\*+\/$/, "")
    .split(/\r?\n/)
    .map((line) => line.replace(/^\s*\*?\s?/, "").trim())
    .filter(Boolean)
    .join("\n")
    .trim();
}

function _extractBodyComment(fn) {
  if (typeof fn !== "function") return "";
  if (typeof fn.description === "string" && fn.description.trim()) {
    return fn.description.trim();
  }
  const src = Function.prototype.toString.call(fn);
  const isFuncKeyword = /^(\s*async\s+)?function\b/.test(src);
  const arrowIdx = src.indexOf("=>");
  let bodyStart = -1;
  if (!isFuncKeyword && arrowIdx !== -1) {
    const afterArrow = src.slice(arrowIdx + 2).trimStart();
    if (afterArrow.startsWith("{")) {
      bodyStart = src.indexOf("{", arrowIdx + 2) + 1;
    } else {
      bodyStart = arrowIdx + 2;
    }
  } else {
    const braceIdx = src.indexOf("{");
    if (braceIdx !== -1) {
      bodyStart = braceIdx + 1;
    }
  }
  if (bodyStart === -1) return "";
  const rest = src.slice(bodyStart).trimStart();
  if (rest.startsWith("/*")) {
    const endIdx = rest.indexOf("*/");
    if (endIdx !== -1) {
      return _cleanBlockComment(rest.slice(0, endIdx + 2));
    }
  }
  if (rest.startsWith("//")) {
    const lines = [];
    for (const line of rest.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (trimmed.startsWith("//")) {
        lines.push(trimmed.replace(/^\/\/+\s?/, "").trim());
      } else {
        break;
      }
    }
    return lines.filter(Boolean).join("\n").trim();
  }
  return "";
}

function _extractCallSiteComment() {
  const stack = new Error().stack || "";
  const stackLines = stack.split(/\r?\n/).slice(1);
  for (const frame of stackLines) {
    if (frame.includes("sdk/nodejs/") || frame.includes("sdk\\nodejs\\") || frame.includes("node:internal")) {
      continue;
    }
    const match = frame.match(/\((.+):(\d+):(\d+)\)$/) || frame.match(/at\s+(.+):(\d+):(\d+)$/);
    if (!match) continue;
    let filePath = match[1].trim();
    if (filePath.startsWith("file://")) {
      try { filePath = new URL(filePath).pathname; } catch (_) {}
    }
    const lineNo = parseInt(match[2], 10);
    if (!filePath || !Number.isFinite(lineNo) || lineNo <= 1) continue;
    let fileLines = _sourceFileCache.get(filePath);
    if (!fileLines) {
      try {
        fileLines = fs.readFileSync(filePath, "utf8").split(/\r?\n/);
        _sourceFileCache.set(filePath, fileLines);
      } catch (_) {
        continue;
      }
    }
    let idx = lineNo - 2;
    while (idx >= 0 && fileLines[idx].trim() === "") {
      return "";
    }
    if (idx < 0) return "";
    const prevTrimmed = fileLines[idx].trim();
    if (prevTrimmed.startsWith("//")) {
      const collected = [];
      while (idx >= 0 && fileLines[idx].trim().startsWith("//")) {
        collected.unshift(fileLines[idx].trim().replace(/^\/\/+\s?/, "").trim());
        idx--;
      }
      return collected.filter(Boolean).join("\n").trim();
    }
    if (prevTrimmed.endsWith("*/")) {
      const blockLines = [];
      while (idx >= 0) {
        blockLines.unshift(fileLines[idx]);
        if (fileLines[idx].includes("/*")) {
          return _cleanBlockComment(blockLines.join("\n"));
        }
        idx--;
      }
    }
    break;
  }
  return "";
}

function extractHandlerDescription(fn) {
  return _extractBodyComment(fn) || _extractCallSiteComment() || "";
}

class DomainRequestError extends Error {
  constructor(status, data = null, messageId = null) {
    super(data?.reason || data?.error || status);
    this.name = 'DomainRequestError';
    this.status = status; this.data = data; this.message_id = messageId;
  }
}

class DomainExecutionFailed extends Error {
  constructor(payload) {
    super(JSON.stringify(payload));
    this.payload = payload;
  }
}

class DomainLogger {
  constructor(domain, defaultLayer = "DOMAIN", defaultComponent = "") {
    this._domain = domain;
    this.defaultLayer = defaultLayer;
    this.defaultComponent = defaultComponent;
  }

  _log(level, msg, opts = {}) {
    const record = {
      ts: Date.now() / 1000,
      level: String(level).toUpperCase(),
      layer: opts.layer || this.defaultLayer,
      component: opts.component || this.defaultComponent,
      message: String(msg),
      phase: opts.phase || "",
      console_only: Boolean(opts.consoleOnly || opts.console_only),
      ephemeral: false,
    };
    this._domain._emitLog(record);
  }

  debug(msg, opts = {}) { this._log("DEBUG", msg, opts); }
  info(msg, opts = {}) { this._log("INFO", msg, opts); }
  warn(msg, opts = {}) { this._log("WARNING", msg, opts); }
  warning(msg, opts = {}) { this._log("WARNING", msg, opts); }
  error(msg, opts = {}) { this._log("ERROR", msg, opts); }
  critical(msg, opts = {}) { this._log("CRITICAL", msg, opts); }

  phase(level, phaseName, msg, opts = {}) {
    this._log(level, msg, { ...opts, phase: phaseName });
  }

  progress(msg, { key = null, frac = null, waiting = false, done = false, state = null, layer = "", component = "" } = {}) {
    if (state === null) state = done ? "completed" : "running";
    if (!["running", "completed", "failed", "cancelled"].includes(state)) {
      throw new TypeError("invalid progress state");
    }
    done = state !== "running";
    const record = {
      ts: Date.now() / 1000,
      level: "INFO",
      layer: layer || this.defaultLayer,
      component: component || this.defaultComponent,
      message: String(msg),
      phase: "",
      console_only: false,
      ephemeral: true,
      key: key || null,
      frac: typeof frac === "number" ? frac : null,
      waiting: Boolean(waiting) && !done,
      state,
      done: Boolean(done),
    };
    this._domain._emitLog(record);
  }

  queryLogs(options = {}, ...args) {
    if (typeof this._domain.queryLogsAsync === "function") {
      return this._domain.queryLogsAsync(options, ...args);
    }
    return this._domain.queryLogs(options, ...args);
  }

  queryLogsAsync(options = {}, ...args) {
    if (typeof this._domain.queryLogsAsync === "function") {
      return this._domain.queryLogsAsync(options, ...args);
    }
    return this._domain.queryLogs(options, ...args);
  }

  queryProgress(options = {}, ...args) {
    if (typeof this._domain.queryProgressAsync === "function") {
      return this._domain.queryProgressAsync(options, ...args);
    }
    return this._domain.queryProgress(options, ...args);
  }

  queryProgressAsync(options = {}, ...args) {
    if (typeof this._domain.queryProgressAsync === "function") {
      return this._domain.queryProgressAsync(options, ...args);
    }
    return this._domain.queryProgress(options, ...args);
  }
}

class DomainTelemetry {
  /**
   * Native telemetry facility for reporting metrics to the Kernel.
   */
  constructor(domain) {
    this._domain = domain;
  }

  emit(metricName, value, tags = {}) {
    return this._domain._submit(
      [KERNEL_TELEMETRY_TARGET],
      metricName,
      { tags: tags || {} },
      { name: metricName, value, tags: tags || {} }
    );
  }

  async emitAsync(metricName, value, tags = {}, options = {}) {
    return (await this._domain.request(
      KERNEL_TELEMETRY_TARGET,
      metricName,
      { tags: tags || {} },
      { name: metricName, value, tags: tags || {} },
      options
    )).data;
  }
}

class DomainStorageClient {
  /**
   * Native storage facility providing safe per-domain CRUD operations.
   */
  constructor(domain) {
    this._domain = domain;
  }

  insert(recordId, category, data, upsert = true) {
    return this._domain.storageInsert(recordId, category, data, upsert);
  }

  update(recordId, data, patch = true) {
    return this._domain.storageUpdate(recordId, data, patch);
  }

  delete(arg1, arg2) {
    return this._domain.storageDelete(arg1, arg2);
  }

  query(arg1, recordId, category, filter, limit = 500) {
    return this._domain.storageQuery(arg1, recordId, category, filter, limit);
  }
}

class DomainRegistryClient {
  /**
   * Native registry facility for business service discovery.
   */
  constructor(domain) {
    this._domain = domain;
  }

  queryHandlers(options = {}) {
    return this._domain.queryHandlersAsync(options);
  }

  getDescription(target, options = {}) {
    return this._domain.getHandlerDescriptionAsync(target, options);
  }
}

class DomainRuntimeClient {
  /**
   * Native runtime infrastructure facility for querying domain assembly.
   */
  constructor(domain) {
    this._domain = domain;
  }

  query(options = {}) {
    return this._domain.queryRuntimeAsync(typeof options === 'string' ? { domain: options } : options);
  }

  async listDomains(options = {}) {
    const res = await this.query(options);
    return res?.domains || [];
  }

  async getDomain(domainId, options = {}) {
    const res = await this._domain.queryRuntimeAsync({ domain: domainId }, options);
    return res?.domain || null;
  }
}

class DomainObserverClient {
  /**
   * Native observer infrastructure facility for querying snapshots and managing subscriptions.
   */
  constructor(domain) {
    this._domain = domain;
  }

  async query(options = {}) {
    const res = await this._domain.queryObserverAsync(options);
    return res?.events || [];
  }

  subscribe(options = {}) {
    return this._domain.subscribeObserverAsync(options);
  }

  unsubscribe(options = {}) {
    return this._domain.unsubscribeObserverAsync(options);
  }
}

class DomainSupervisorClient {
  /**
   * Native supervisor infrastructure facility for process lifecycle and smooth rolling updates.
   */
  constructor(domain) {
    this._domain = domain;
  }

  start(domain, spec = null, options = {}) {
    return this._domain.supervisorStartAsync({ domain, spec }, options);
  }

  stop(domain, options = {}) {
    return this._domain.supervisorStopAsync({ domain }, options);
  }

  restart(domain, options = {}) {
    return this._domain.supervisorRestartAsync({ domain }, options);
  }

  update(domain, spec = null, options = {}) {
    if (spec && typeof spec === "object" && !spec.command && (spec.probeHandler || spec.probe_handler || spec.timeoutMs || spec.timeout_ms)) {
      return this._domain.supervisorUpdateAsync({ domain, ...spec });
    }
    const payload = { domain, ...options };
    if (spec !== undefined && spec !== null) payload.spec = spec;
    return this._domain.supervisorUpdateAsync(payload);
  }
}

class AtelierDomain {
  constructor({ domain = "", stageDelayMs = 0 } = {}) {
    this._domainName = domain;
    this._seqCounter = 1;
    this._handlers = new Map();
    this._handlerDocs = new Map();
    this._receiptCallbacks = [];
    this._logListeners = [];
    this._telemetryListeners = [];
    this._notifyListeners = [];
    this._observationListeners = [];
    this._stageDelayMs = stageDelayMs;
    this._pending = new Map(); this._closed = false; this._running = false;
    this.logger = new DomainLogger(this, "DOMAIN", domain);
    this.telemetry = new DomainTelemetry(this);
    this.storage = new DomainStorageClient(this);
    this.registry = new DomainRegistryClient(this);
    this.runtime = new DomainRuntimeClient(this);
    this.observer = new DomainObserverClient(this);
    this.supervisor = new DomainSupervisorClient(this);
  }

  /**
   * Registers a handler function for `<domain>.<name>`.
   * Automatically extracts leading comments or function body comments as the handler description.
   */
  handler(name, fn) {
    if (!name || name.includes(".")) {
      throw new Error(`Invalid handler name '${name}'`);
    }
    this._handlers.set(name, fn);
    this._handlerDocs.set(name, extractHandlerDescription(fn));
    return this;
  }

  /**
   * Alias for `domain.handler(name, fn)`.
   */
  on(name, fn) {
    return this.handler(name, fn);
  }

  /**
   * Optional callback to observe receipts returned by the Kernel.
   */
  onReceipt(fn) {
    this._receiptCallbacks.push(fn);
    return this;
  }

  /**
   * Registers a callback to listen to the real-time system log stream.
   */
  onLog(callback, options = {}) {
    const level = options.level;
    const sourceDomain = options.sourceDomain || options.source_domain;
    this._logListeners.push({ callback, level, sourceDomain });
    if (this._state === "open") {
      const params = {};
      if (level) params.level = level;
      if (sourceDomain) params.source_domain = sourceDomain;
      this._submit([KERNEL_LOGGER_TARGET], "SubscribeLogs", params, {});
    }
    return this;
  }

  /**
   * Registers a callback to listen to the real-time telemetry stream.
   */
  onTelemetry(callback) {
    this._telemetryListeners.push(callback);
    if (this._state === "open") {
      this._submit([KERNEL_TELEMETRY_TARGET], "SubscribeTelemetry", {}, {});
    }
    return this;
  }

  /**
   * Registers a callback to listen to system alerts and notifications.
   */
  onNotify(callback) {
    this._notifyListeners.push(callback);
    if (this._state === "open") {
      this._submit([KERNEL_NOTIFY_TARGET], "SubscribeNotify", {}, {});
    }
    return this;
  }

  /**
   * Registers a callback to listen to the real-time global message observation stream.
   */
  onObservation(callback) {
    this._observationListeners.push(callback);
    if (this._state === "open") {
      this._submit([KERNEL_OBSERVER_TARGET], "SubscribeObserver", {}, {});
    }
    return this;
  }

  /**
   * Emits a system notification or alert to `kernel.notify`.
   */
  notify(level, message, title = "", details = {}) {
    return this._submit(
      [KERNEL_NOTIFY_TARGET],
      title || "Notification",
      { severity: String(level).toLowerCase(), level: String(level).toLowerCase() },
      { title, message, details: details || {} }
    );
  }

  /**
   * Emits a system notification or alert to `kernel.notify` asynchronously.
   */
  async notifyAsync(level, message, title = "", details = {}, options = {}) {
    return (await this.request(
      KERNEL_NOTIFY_TARGET,
      title || "Notification",
      { severity: String(level).toLowerCase(), level: String(level).toLowerCase() },
      { title, message, details: details || {} },
      options
    )).data;
  }

  /**
   * Discovers active business domain handlers (excludes kernel internal processors).
   */
  discoverHandlers(options = {}) {
    return this.queryHandlersAsync(options);
  }

  /**
   * Queries available handlers and their descriptions from `kernel.registry` (`QueryHandlers`).
   */
  queryHandlers(arg1 = {}, target, handler) {
    const params = {};
    if (typeof arg1 === "object" && arg1 !== null) {
      if (arg1.domain !== undefined) params.domain = arg1.domain;
      if (arg1.target !== undefined) params.target = arg1.target;
      if (arg1.handler !== undefined) params.handler = arg1.handler;
      if (arg1.includeInactive !== undefined) params.include_inactive = Boolean(arg1.includeInactive);
      if (arg1.include_inactive !== undefined) params.include_inactive = Boolean(arg1.include_inactive);
    } else {
      if (arg1 !== undefined) params.domain = arg1;
      if (target !== undefined) params.target = target;
      if (handler !== undefined) params.handler = handler;
    }
    return this.send("kernel.registry", "QueryHandlers", params, {});
  }

  /**
   * Sends a two-layer message to one or more `<domain>.<handler>` targets.
   */
  _submit(targets, messageType, params = {}, data = {}, seq = null) {
    const assignedSeq = seq && seq > 0 ? seq : this._seqCounter++;
    this._seqCounter = Math.max(this._seqCounter, assignedSeq + 1);
    this._writeFrame({
      frame_type: "send",
      seq: assignedSeq,
      targets,
      message_type: messageType,
      params: params ?? {},
      data: data ?? {},
    });
    return assignedSeq;
  }

  /**
   * Writes a record to this domain's dedicated storage table via `kernel.storage`.
   */
  storageInsert(recordId, category, data, upsert = true) {
    return this.send(
      "kernel.storage",
      "StorageInsert",
      { record_id: recordId, category, upsert },
      data
    );
  }

  /**
   * Updates a record in this domain's dedicated storage table via `kernel.storage`.
   */
  storageUpdate(recordId, data, patch = true) {
    return this.send(
      "kernel.storage",
      "StorageUpdate",
      { record_id: recordId, patch },
      data
    );
  }

  /**
   * Deletes record(s) from this domain's dedicated storage table via `kernel.storage`.
   * Supports both object options `{ recordId, category }` and positional arguments `(recordId, category)`.
   */
  storageDelete(arg1, arg2) {
    const params = {};
    if (typeof arg1 === "object" && arg1 !== null) {
      if (arg1.recordId !== undefined) params.record_id = arg1.recordId;
      if (arg1.record_id !== undefined) params.record_id = arg1.record_id;
      if (arg1.category !== undefined) params.category = arg1.category;
    } else {
      if (arg1 !== undefined) params.record_id = arg1;
      if (arg2 !== undefined) params.category = arg2;
    }
    return this.send("kernel.storage", "StorageDelete", params, {});
  }

  /**
   * Queries this domain's storage table or another domain's storage table (read-only cross-domain access).
   * Supports both object options `{ domain, recordId, category, filter, limit }` and positional arguments.
   */
  storageQuery(arg1, recordId, category, filter, limit = 500) {
    const params = { limit };
    if (typeof arg1 === "object" && arg1 !== null) {
      if (arg1.domain !== undefined) params.domain = arg1.domain;
      if (arg1.recordId !== undefined) params.record_id = arg1.recordId;
      if (arg1.record_id !== undefined) params.record_id = arg1.record_id;
      if (arg1.category !== undefined) params.category = arg1.category;
      if (arg1.filter !== undefined) params.filter = arg1.filter;
      if (arg1.filter_map !== undefined) params.filter = arg1.filter_map;
      if (arg1.limit !== undefined) params.limit = arg1.limit;
    } else {
      if (arg1 !== undefined) params.domain = arg1;
      if (recordId !== undefined) params.record_id = recordId;
      if (category !== undefined) params.category = category;
      if (filter !== undefined) params.filter = filter;
    }
    return this.send("kernel.storage", "StorageQuery", params, {});
  }

  /**
   * Queries persisted logs from SQLite `hub_logs` via `kernel.logger` (`QueryLogs`).
   */
  queryLogs(arg1 = {}, level, layer, component, phase, sinceTs, limit = 200) {
    const params = { limit };
    if (typeof arg1 === "object" && arg1 !== null) {
      if (arg1.domain !== undefined) params.domain_id = arg1.domain;
      if (arg1.domainId !== undefined) params.domain_id = arg1.domainId;
      if (arg1.domain_id !== undefined) params.domain_id = arg1.domain_id;
      if (arg1.source_domain !== undefined) params.domain_id = arg1.source_domain;
      if (arg1.level !== undefined) params.level = arg1.level;
      if (arg1.layer !== undefined) params.layer = arg1.layer;
      if (arg1.component !== undefined) params.component = arg1.component;
      if (arg1.phase !== undefined) params.phase = arg1.phase;
      if (arg1.sinceTs !== undefined) params.since_ts = arg1.sinceTs;
      if (arg1.since_ts !== undefined) params.since_ts = arg1.since_ts;
      if (arg1.limit !== undefined) params.limit = arg1.limit;
      if (arg1.beforeId !== undefined) params.before_id = arg1.beforeId;
      if (arg1.before_id !== undefined) params.before_id = arg1.before_id;
    } else {
      if (arg1 !== undefined) params.domain_id = arg1;
      if (level !== undefined) params.level = level;
      if (layer !== undefined) params.layer = layer;
      if (component !== undefined) params.component = component;
      if (phase !== undefined) params.phase = phase;
      if (sinceTs !== undefined) params.since_ts = sinceTs;
    }
    return this.send("kernel.logger", "QueryLogs", params, {});
  }

  /**
   * Queries active in-flight task progress indicators via `kernel.logger` (`QueryProgress`).
   */
  queryProgress() {
    return this.send("kernel.logger", "QueryProgress", {}, {});
  }

  /**
   * Reports a lifecycle stage receipt (`received`, `started`, `completed`, `execution_failed`).
   */
  reportReceipt(messageId, target, status, data = null) {
    this._writeFrame({
      frame_type: "receipt",
      message_id: messageId,
      target,
      status,
      data,
    });
  }

  _emitLog(record) {
    this._writeFrame({
      frame_type: "log",
      record,
    });
  }

  _writeFrame(_frame) {
    throw new Error("missing ATELIER_IPC_READ_FD / ATELIER_IPC_WRITE_FD: dedicated dual anonymous pipes are required");
  }
}

const KERNEL_REGISTRY_TARGET = "kernel.registry";
const KERNEL_STORAGE_TARGET = "kernel.storage";
const KERNEL_LOGGER_TARGET = "kernel.logger";
const KERNEL_TELEMETRY_TARGET = "kernel.telemetry";
const KERNEL_NOTIFY_TARGET = "kernel.notify";
const KERNEL_RUNTIME_TARGET = "kernel.runtime";
const KERNEL_OBSERVER_TARGET = "kernel.observer";
const KERNEL_SUPERVISOR_TARGET = "kernel.supervisor";

const MSG_REGISTER_HANDLERS = "RegisterHandlers";
const MSG_UNREGISTER_HANDLERS = "UnregisterHandlers";
const MSG_QUERY_HANDLERS = "QueryHandlers";
const MSG_QUERY_LOGS = "QueryLogs";
const MSG_QUERY_PROGRESS = "QueryProgress";
const MSG_SUBSCRIBE_LOGS = "SubscribeLogs";
const MSG_UNSUBSCRIBE_LOGS = "UnsubscribeLogs";
const MSG_SUBSCRIBE_TELEMETRY = "SubscribeTelemetry";
const MSG_UNSUBSCRIBE_TELEMETRY = "UnsubscribeTelemetry";
const MSG_SUBSCRIBE_NOTIFY = "SubscribeNotify";
const MSG_UNSUBSCRIBE_NOTIFY = "UnsubscribeNotify";
const MSG_QUERY_RUNTIME = "QueryRuntime";
const MSG_QUERY_DOMAIN_ASSEMBLY = "QueryDomainAssembly";
const MSG_QUERY_OBSERVER = "QueryObserver";
const MSG_SUBSCRIBE_OBSERVER = "SubscribeObserver";
const MSG_UNSUBSCRIBE_OBSERVER = "UnsubscribeObserver";
const MSG_START_DOMAIN = "StartDomain";
const MSG_STOP_DOMAIN = "StopDomain";
const MSG_RESTART_DOMAIN = "RestartDomain";
const MSG_UPDATE_DOMAIN = "UpdateDomain";
const MSG_SYSTEM_LOG_EVENT = "LogEvent";
const MSG_SYSTEM_TELEMETRY_EVENT = "TelemetryEvent";
const MSG_SYSTEM_NOTIFY_EVENT = "NotifyEvent";
const MSG_SYSTEM_OBSERVER_EVENT = "ObservationEvent";
const MSG_STORAGE_INSERT = "StorageInsert";
const MSG_STORAGE_QUERY = "StorageQuery";
const MSG_STORAGE_UPDATE = "StorageUpdate";
const MSG_STORAGE_DELETE = "StorageDelete";
const MSG_DELIVERY_DURABLE = "Durable";
const MSG_DELIVERY_VOLATILE = "Volatile";
const MSG_DELIVERY_EPHEMERAL = "Ephemeral";

module.exports = {
  ...require("./runtime").enhance(AtelierDomain, DomainRequestError, DomainExecutionFailed),
  DomainExecutionFailed,
  DomainLogger,
  DomainObserverClient,
  DomainRegistryClient,
  DomainRequestError,
  DomainRuntimeClient,
  DomainStorageClient,
  DomainSupervisorClient,
  DomainTelemetry,
  KERNEL_LOGGER_TARGET,
  KERNEL_NOTIFY_TARGET,
  KERNEL_OBSERVER_TARGET,
  KERNEL_REGISTRY_TARGET,
  KERNEL_RUNTIME_TARGET,
  KERNEL_STORAGE_TARGET,
  KERNEL_SUPERVISOR_TARGET,
  KERNEL_TELEMETRY_TARGET,
  MSG_DELIVERY_DURABLE,
  MSG_DELIVERY_EPHEMERAL,
  MSG_DELIVERY_VOLATILE,
  MSG_QUERY_DOMAIN_ASSEMBLY,
  MSG_QUERY_HANDLERS,
  MSG_QUERY_LOGS,
  MSG_QUERY_OBSERVER,
  MSG_QUERY_PROGRESS,
  MSG_QUERY_RUNTIME,
  MSG_REGISTER_HANDLERS,
  MSG_RESTART_DOMAIN,
  MSG_START_DOMAIN,
  MSG_STOP_DOMAIN,
  MSG_STORAGE_DELETE,
  MSG_STORAGE_INSERT,
  MSG_STORAGE_QUERY,
  MSG_STORAGE_UPDATE,
  MSG_SUBSCRIBE_LOGS,
  MSG_SUBSCRIBE_NOTIFY,
  MSG_SUBSCRIBE_OBSERVER,
  MSG_SUBSCRIBE_TELEMETRY,
  MSG_SYSTEM_LOG_EVENT,
  MSG_SYSTEM_NOTIFY_EVENT,
  MSG_SYSTEM_OBSERVER_EVENT,
  MSG_SYSTEM_TELEMETRY_EVENT,
  MSG_UNREGISTER_HANDLERS,
  MSG_UNSUBSCRIBE_LOGS,
  MSG_UNSUBSCRIBE_NOTIFY,
  MSG_UNSUBSCRIBE_OBSERVER,
  MSG_UNSUBSCRIBE_TELEMETRY,
  MSG_UPDATE_DOMAIN,
};
