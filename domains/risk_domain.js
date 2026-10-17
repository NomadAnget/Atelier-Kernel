#!/usr/bin/env node
/**
 * Node.js Domain (`risk`) for Atelier Kernel — powered by `atelier_sdk.js`.
 */
const { AtelierDomain } = require("./atelier_sdk");

const domain = new AtelierDomain();
let autoTrafficEnabled = true;
let evaluatedCount = 0;

// 评估订单或传感器遥测数据的安全风险等级与得分
domain.handler("evaluate", () => {
  const score = Math.floor(Math.random() * 35) + 65;
  return {
    ok: true,
    runtime: "Node.js (atelier_sdk)",
    risk_level: score > 85 ? "LOW" : "NORMAL",
    safety_score: score,
  };
});

/**
 * 查询风控域累计审计次数与合规状态
 */
domain.handler("audit", () => ({
  audited: true,
  total_evaluated: evaluatedCount,
}));

domain.handler("set_auto_traffic", async (msg) => {
  // 动态开启或关闭风控域自动心跳与审计任务
  if (typeof msg.data.enabled === "boolean") {
    autoTrafficEnabled = msg.data.enabled;
  }
  return {
    domain: "risk",
    auto_traffic_enabled: autoTrafficEnabled,
  };
});

const timer = setInterval(() => {
  void tick().catch(error => domain.logger.error(String(error)));
}, 3400);
async function tick() {
  if (!autoTrafficEnabled) return;
  evaluatedCount++;
  if (Math.random() < 0.6) {
    await submit(
      ["order.echo"],
      "RiskHeartbeatPing",
      { audit_round: evaluatedCount },
      { auditor: "risk (Node.js)", check: "ok" }
    );
  } else {
    await domain.storageInsertAsync(`audit_${evaluatedCount % 8}`, "risk_audit", {
      audit_round: evaluatedCount,
      status: "clean",
      ts: Date.now(),
    });
  }
}

async function submit(...args) { return domain.waitResults(await domain.send(...args)); }
domain.runAsync({onClose:()=>clearInterval(timer)}).catch(error=>{process.stderr.write(String(error));process.exitCode=1;});
