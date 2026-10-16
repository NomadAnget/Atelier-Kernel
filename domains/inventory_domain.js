#!/usr/bin/env node
/**
 * Node.js Domain (`inventory`) for Atelier Kernel — powered by `atelier_sdk.js`.
 */
const { AtelierDomain, DomainExecutionFailed } = require("./atelier_sdk");

const domain = new AtelierDomain();
let autoTrafficEnabled = true;

const stockLedger = {
  "ATELIER-GEAR-01": 150,
  "ATELIER-CORE-X": 80,
  "NEURAL-LINK-V2": 60,
  "CHRONO-MOD-7": 45,
};

domain.handler("deduct", (msg) => {
  const sku = msg.data.sku || "ATELIER-GEAR-01";
  const qty = Number(msg.data.qty || 1);
  domain.logger.phase("INFO", "start", `Deducting ${qty}x ${sku} from inventory`, { layer: "DOMAIN" });
  const current = stockLedger[sku] ?? 100;
  if (current < qty) {
    domain.logger.error(`Failed to deduct ${sku}: requested ${qty} but available ${current}`, { layer: "DOMAIN" });
    throw new DomainExecutionFailed({
      error: "insufficient_stock",
      sku,
      available: current,
      requested: qty,
    });
  }
  stockLedger[sku] = current - qty;
  domain.storageInsert(sku, "stock_item", {
    sku,
    remaining: stockLedger[sku],
    updated_by: "inventory (Node.js)",
  });
  domain.logger.info(`Updated stock for ${sku}, remaining: ${stockLedger[sku]}`, { layer: "DOMAIN" });
  domain.logger.phase("INFO", "finish", `Deduction completed for ${sku}`, { layer: "DOMAIN" });
  return {
    ok: true,
    runtime: "Node.js (atelier_sdk)",
    sku,
    deducted: qty,
    remaining_stock: stockLedger[sku],
  };
});

domain.handler("restock", (msg) => {
  const sku = msg.data.sku || "ATELIER-GEAR-01";
  const qty = Number(msg.data.qty || 10);
  stockLedger[sku] = (stockLedger[sku] ?? 0) + qty;
  return {
    ok: true,
    runtime: "Node.js (atelier_sdk)",
    sku,
    remaining_stock: stockLedger[sku],
  };
});

domain.handler("stock_info", () => ({
  runtime: "Node.js (atelier_sdk)",
  stock: stockLedger,
}));

domain.handler("set_auto_traffic", (msg) => {
  if (typeof msg.data.enabled === "boolean") {
    autoTrafficEnabled = msg.data.enabled;
  }
  return {
    domain: "inventory",
    auto_traffic_enabled: autoTrafficEnabled,
  };
});

setInterval(() => {
  if (!autoTrafficEnabled) return;
  const skus = Object.keys(stockLedger);
  const sku = skus[Math.floor(Math.random() * skus.length)];
  if (Math.random() < 0.5) {
    if (stockLedger[sku] < 120) {
      stockLedger[sku] += 5;
    }
    domain.storageInsert(sku, "stock_snapshot", {
      sku,
      remaining: stockLedger[sku],
      checked_at: Date.now(),
    });
  } else {
    domain.send(
      ["web_ui.metric_tick"],
      "StockTelemetry",
      { warehouse: "WH-NODE-01" },
      { sku, stock: stockLedger[sku], total_skus: skus.length }
    );
  }
}, 2800);

domain.run();
