#!/usr/bin/env node
/**
 * Node.js Domain (`risk`) for Atelier Kernel — powered by `atelier_sdk.js`.
 */
const { AtelierDomain } = require("./atelier_sdk");

const domain = new AtelierDomain();
let autoTrafficEnabled = true;
let evaluatedCount = 0;

domain.handler("evaluate", () => {
  const score = Math.floor(Math.random() * 35) + 65;
  return {
    ok: true,
    runtime: "Node.js (atelier_sdk)",
    risk_level: score > 85 ? "LOW" : "NORMAL",
    safety_score: score,
  };
});

domain.handler("audit", () => ({
  audited: true,
  total_evaluated: evaluatedCount,
}));

domain.handler("set_auto_traffic", (msg) => {
  if (typeof msg.data.enabled === "boolean") {
    autoTrafficEnabled = msg.data.enabled;
  }
  return {
    domain: "risk",
    auto_traffic_enabled: autoTrafficEnabled,
  };
});

setInterval(() => {
  if (!autoTrafficEnabled) return;
  evaluatedCount++;
  if (Math.random() < 0.6) {
    domain.send(
      ["order.echo"],
      "RiskHeartbeatPing",
      { audit_round: evaluatedCount },
      { auditor: "risk (Node.js)", check: "ok" }
    );
  } else {
    domain.storageInsert(`audit_${evaluatedCount % 8}`, "risk_audit", {
      audit_round: evaluatedCount,
      status: "clean",
      ts: Date.now(),
    });
  }
}, 3400);

domain.run();
