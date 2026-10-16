#!/usr/bin/env node
/**
 * Language-Agnostic Node.js Domain (`inventory`) for Atelier Kernel.
 * Registers `deduct`, `restock`, `stock_info`, `set_auto_traffic`, and
 * periodically sends random messages across domains when auto-traffic is enabled.
 */
const readline = require("readline");

let seqCounter = 1;
let autoTrafficEnabled = true;

const stockLedger = {
  "ATELIER-GEAR-01": 150,
  "ATELIER-CORE-X": 80,
  "NEURAL-LINK-V2": 60,
  "CHRONO-MOD-7": 45,
};

function sendFrame(frame) {
  process.stdout.write(JSON.stringify(frame) + "\n");
}

function sendMessage(targets, messageType, params, data) {
  const seq = seqCounter++;
  sendFrame({
    frame_type: "send",
    seq,
    targets,
    message_type: messageType,
    params,
    data,
  });
  return seq;
}

function reportReceipt(messageId, target, status, data = null) {
  sendFrame({
    frame_type: "receipt",
    message_id: messageId,
    target,
    status,
    data,
  });
}

// 1. Register handlers with the Rust Kernel on startup
sendMessage(
  ["kernel.registry"],
  "RegisterHandlers",
  { handlers: ["deduct", "restock", "stock_info", "set_auto_traffic"] },
  {}
);

// 2. Periodic random message generator
setInterval(() => {
  if (!autoTrafficEnabled) return;
  const skus = Object.keys(stockLedger);
  const sku = skus[Math.floor(Math.random() * skus.length)];
  const r = Math.random();

  if (r < 0.5) {
    // Auto restock if stock gets below 120 and record in `store_inventory`
    if (stockLedger[sku] < 120) {
      stockLedger[sku] += 5;
    }
    sendMessage(
      ["kernel.storage"],
      "StorageInsert",
      { record_id: sku, category: "stock_snapshot", upsert: true },
      { sku, remaining: stockLedger[sku], checked_at: Date.now() }
    );
  } else {
    // Send stock telemetry to `web_ui.metric_tick`
    sendMessage(
      ["web_ui.metric_tick"],
      "StockTelemetry",
      { warehouse: "WH-NODE-01" },
      { sku, stock: stockLedger[sku], total_skus: skus.length }
    );
  }
}, 2800);

// 3. Listen for KernelToDomainFrame lines on stdin
const rl = readline.createInterface({
  input: process.stdin,
  terminal: false,
});

rl.on("line", (rawLine) => {
  const line = rawLine.trim();
  if (!line) return;
  let frame;
  try {
    frame = JSON.parse(line);
  } catch (_) {
    return;
  }

  if (frame.frame_type !== "deliver") return;

  const msgId = frame.message_id;
  const target = frame.target;
  const data = frame.data || {};
  const handler = target.includes(".") ? target.split(".")[1] : target;

  // Stage 2: Received
  reportReceipt(msgId, target, "received", null);
  // Stage 3: Started
  reportReceipt(msgId, target, "started", null);

  if (handler === "deduct") {
    const sku = data.sku || "ATELIER-GEAR-01";
    const qty = Number(data.qty || 1);
    const current = stockLedger[sku] ?? 100;
    if (current < qty) {
      reportReceipt(msgId, target, "execution_failed", {
        error: "insufficient_stock",
        sku,
        available: current,
        requested: qty,
      });
      return;
    }
    stockLedger[sku] = current - qty;
    sendMessage(
      ["kernel.storage"],
      "StorageInsert",
      { record_id: sku, category: "stock_item", upsert: true },
      { sku, remaining: stockLedger[sku], updated_by: "inventory (Node.js)" }
    );
    reportReceipt(msgId, target, "completed", {
      ok: true,
      runtime: "Node.js (Stdio Pipe)",
      sku,
      deducted: qty,
      remaining_stock: stockLedger[sku],
    });
  } else if (handler === "restock") {
    const sku = data.sku || "ATELIER-GEAR-01";
    const qty = Number(data.qty || 10);
    stockLedger[sku] = (stockLedger[sku] ?? 0) + qty;
    reportReceipt(msgId, target, "completed", {
      ok: true,
      runtime: "Node.js (Stdio Pipe)",
      sku,
      remaining_stock: stockLedger[sku],
    });
  } else if (handler === "stock_info") {
    reportReceipt(msgId, target, "completed", {
      runtime: "Node.js (Stdio Pipe)",
      stock: stockLedger,
    });
  } else if (handler === "set_auto_traffic") {
    if (typeof data.enabled === "boolean") {
      autoTrafficEnabled = data.enabled;
    }
    reportReceipt(msgId, target, "completed", {
      domain: "inventory",
      auto_traffic_enabled: autoTrafficEnabled,
    });
  } else {
    reportReceipt(msgId, target, "execution_failed", {
      error: `Unknown handler: ${handler}`,
    });
  }
});
