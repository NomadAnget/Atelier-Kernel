#!/usr/bin/env node
/**
 * Language-Agnostic Node.js Domain (`risk`) for Atelier Kernel.
 * Evaluates risk for incoming order/sensor messages, writes audit logs to `store_risk`,
 * and randomly dispatches audit checks.
 */
const readline = require("readline");

let seqCounter = 1;
let autoTrafficEnabled = true;
let evaluatedCount = 0;

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

// 1. Register handlers with the Rust Kernel
sendMessage(
  ["kernel.registry"],
  "RegisterHandlers",
  { handlers: ["evaluate", "audit", "set_auto_traffic"] },
  {}
);

// 2. Periodic random audit ping
setInterval(() => {
  if (!autoTrafficEnabled) return;
  evaluatedCount++;
  const r = Math.random();
  if (r < 0.6) {
    sendMessage(
      ["order.echo"],
      "RiskHeartbeatPing",
      { audit_round: evaluatedCount },
      { auditor: "risk (Node.js)", check: "ok" }
    );
  } else {
    sendMessage(
      ["kernel.storage"],
      "StorageInsert",
      { record_id: `audit_${evaluatedCount % 8}`, category: "risk_audit", upsert: true },
      { audit_round: evaluatedCount, status: "clean", ts: Date.now() }
    );
  }
}, 3400);

// 3. Process incoming Deliver frames on stdin
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

  reportReceipt(msgId, target, "received", null);
  reportReceipt(msgId, target, "started", null);

  if (handler === "evaluate") {
    const score = Math.floor(Math.random() * 35) + 65;
    reportReceipt(msgId, target, "completed", {
      ok: true,
      runtime: "Node.js (Stdio Pipe)",
      risk_level: score > 85 ? "LOW" : "NORMAL",
      safety_score: score,
    });
  } else if (handler === "audit") {
    reportReceipt(msgId, target, "completed", {
      audited: true,
      total_evaluated: evaluatedCount,
    });
  } else if (handler === "set_auto_traffic") {
    if (typeof data.enabled === "boolean") {
      autoTrafficEnabled = data.enabled;
    }
    reportReceipt(msgId, target, "completed", {
      domain: "risk",
      auto_traffic_enabled: autoTrafficEnabled,
    });
  } else {
    reportReceipt(msgId, target, "execution_failed", {
      error: `Unknown handler ${handler}`,
    });
  }
});
