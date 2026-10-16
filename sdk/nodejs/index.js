/**
 * Atelier Kernel Official Node.js Domain SDK (`atelier-sdk`).
 *
 * Encapsulates OS Stdio JSON-Lines pipe framing, sequence numbering, handler
 * registration (`kernel.registry`), 4-stage receipt reporting (`Received` ->
 * `Started` -> `Completed` / `ExecutionFailed`), and `kernel.storage` CRUD helpers
 * so business domains only need to `require("atelier-sdk")` and declare handlers.
 */
const readline = require("readline");

class DomainExecutionFailed extends Error {
  constructor(payload) {
    super(JSON.stringify(payload));
    this.payload = payload;
  }
}

class AtelierDomain {
  constructor({ stageDelayMs = 0 } = {}) {
    this._seqCounter = 1;
    this._handlers = new Map();
    this._receiptCallbacks = [];
    this._stageDelayMs = stageDelayMs;
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
  send(targets, messageType, params = {}, data = {}, seq = null) {
    const assignedSeq = seq && seq > 0 ? seq : this._seqCounter++;
    this._writeFrame({
      frame_type: "send",
      seq: assignedSeq,
      targets,
      message_type: messageType,
      params: params || {},
      data: data || {},
    });
    return assignedSeq;
  }

  /**
   * Writes a record to this domain's dedicated storage table via `kernel.storage`.
   */
  storageInsert(recordId, category, data, upsert = true) {
    return this.send(
      ["kernel.storage"],
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
      ["kernel.storage"],
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
    return this.send(["kernel.storage"], "StorageDelete", params, {});
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
    return this.send(["kernel.storage"], "StorageQuery", params, {});
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

  _writeFrame(frame) {
    process.stdout.write(JSON.stringify(frame) + "\n");
  }

  /**
   * Registers all declared handlers with `kernel.registry` and starts the stdin event loop.
   */
  run() {
    if (this._handlers.size > 0) {
      this.send(
        ["kernel.registry"],
        "RegisterHandlers",
        { handlers: Array.from(this._handlers.keys()) },
        {}
      );
    }

    const rl = readline.createInterface({
      input: process.stdin,
      terminal: false,
    });

    rl.on("line", async (rawLine) => {
      const line = rawLine.trim();
      if (!line) return;
      let frame;
      try {
        frame = JSON.parse(line);
      } catch (_) {
        return;
      }

      if (frame.frame_type === "receipt") {
        for (const cb of this._receiptCallbacks) {
          try {
            cb(frame);
          } catch (_) {}
        }
        return;
      }

      if (frame.frame_type !== "deliver") return;

      const msgId = frame.message_id;
      const target = frame.target;
      const handlerName = target.includes(".") ? target.split(".")[1] : target;

      // Stage 2 (`Received`) and Stage 3 (`Started`)
      this.reportReceipt(msgId, target, "received", null);
      this.reportReceipt(msgId, target, "started", null);
      if (this._stageDelayMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, this._stageDelayMs));
      }

      const handlerFn = this._handlers.get(handlerName);
      if (!handlerFn) {
        this.reportReceipt(msgId, target, "execution_failed", {
          error: `Unknown handler: ${handlerName}`,
        });
        return;
      }

      try {
        const msg = {
          message_id: msgId,
          source_domain: frame.source_domain,
          target,
          targets: frame.targets || [target],
          message_type: frame.message_type || "",
          params: frame.params || {},
          data: frame.data || {},
        };
        const result = await handlerFn(msg, this);
        this.reportReceipt(msgId, target, "completed", result ?? null);
      } catch (err) {
        if (err instanceof DomainExecutionFailed) {
          this.reportReceipt(msgId, target, "execution_failed", err.payload);
        } else {
          this.reportReceipt(msgId, target, "execution_failed", {
            error: "handler_exception",
            reason: err && err.message ? err.message : String(err),
          });
        }
      }
    });
  }
}

module.exports = {
  AtelierDomain,
  DomainExecutionFailed,
};
