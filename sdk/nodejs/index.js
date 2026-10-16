/**
 * Atelier Kernel Official Node.js Domain SDK (`atelier-sdk`).
 *
 * Encapsulates OS Stdio JSON-Lines pipe framing, sequence numbering, handler
 * registration (`kernel.registry`), 4-stage receipt reporting (`Received` ->
 * `Started` -> `Completed` / `ExecutionFailed`), and `kernel.storage` CRUD helpers
 * so business domains only need to `require("atelier-sdk")` and declare handlers.
 */
const readline = require("readline");

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

  progress(msg, { key = null, frac = null, waiting = false, done = false, layer = "", component = "" } = {}) {
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
      waiting: Boolean(waiting),
      done: Boolean(done),
    };
    this._domain._emitLog(record);
  }
}

class AtelierDomain {
  constructor({ domain = "", stageDelayMs = 0 } = {}) {
    this._domainName = domain;
    this._seqCounter = 1;
    this._handlers = new Map();
    this._receiptCallbacks = [];
    this._stageDelayMs = stageDelayMs;
    this._pending = new Map(); this._closed = false; this._running = false;
    this.logger = new DomainLogger(this, "DOMAIN", domain);
  }

  /**
   * Registers a handler function for `<domain>.<name>`.
   */
  handler(name, fn) {
    if (!name || name.includes(".")) {
      throw new Error(`Invalid handler name '${name}'`);
    }
    this._handlers.set(name, fn);
    return this;
  }

  /**
   * Optional callback to observe receipts returned by the Kernel.
   */
  onReceipt(fn) {
    this._receiptCallbacks.push(fn);
    return this;
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

  _writeFrame(frame) {
    process.stdout.write(JSON.stringify(frame) + "\n");
  }

  /**
   * Registers all declared handlers with `kernel.registry` and starts the stdin event loop.
   */
}

module.exports = {
  ...require("./runtime").enhance(AtelierDomain, DomainRequestError, DomainExecutionFailed),
  DomainExecutionFailed,
  DomainLogger,
  DomainRequestError,
};
