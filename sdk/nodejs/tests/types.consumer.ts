// TypeScript 消费方：把 index.d.ts 当真实包来用，验证声明面可用且类型正确。
// 只做类型检查（--noEmit），不执行。
import { AtelierDomain, DomainRequestError } from "../index";

async function main(): Promise<void> {
  // SDK 没有策略开关：构造只接收域名。
  const domain = new AtelierDomain("order");
  const transport = domain.stats();
  const pending: number = transport.pending;
  const closed: boolean = transport.transport_closed;
  void pending;
  void closed;

  // 2. 注册处理器
  domain.handler("create", async (msg) => ({ ok: true, got: msg.data }), "创建订单");

  // 3. 发消息：字符串 → 单 id；数组 → id 列表
  const one: string = await domain.send("billing.charge", "Charge", { a: 1 }, { b: 2 });
  const many: string[] = await domain.send(["billing.charge", "billing.refund"], "Charge");

  // 4. 请求-响应：返回整条回执
  const receipt = await domain.request("billing.charge", "Charge", {}, {});
  const payload = receipt.data;

  // 5. 等待 / 遗忘
  const waited = await domain.waitResult(one);
  const batch = await domain.waitResults(many);
  domain.forgetResult(one);
  const total: number = batch.length;

  // 6. 上报回执
  domain.reportReceipt(one, "order.create", "completed", { ok: true });

  // 7. 事件：通用 on + 各特化
  domain
    .on("log", (p) => void p)
    .on("notify", (p) => void p)
    .on("observation", (p) => void p)
    .on("telemetry", (p) => void p)
    .on("receipt", (p) => void p);
  domain.onReceipt((frame) => void frame.message_id);
  domain.onLog((p) => void p);
  domain.onNotify((p) => void p);
  domain.onObservation((p) => void p);
  domain.onTelemetry((p) => void p);

  // 8. 本域存储
  await domain.storageInsert("r1", "sales_order", { v: 1 });
  await domain.storageUpdate("r1", { v: 2 });
  await domain.storageQuery({ record_id: "r1", limit: 10 });
  await domain.storageDelete("r1");

  // 9. 查询（参数用内核字段名 snake_case）
  await domain.queryHandlers();
  await domain.queryRuntime();
  await domain.queryLogs({ before_id: 12345, limit: 200 });
  await domain.queryObserver({ message_id: one, source_domain: "order" });
  await domain.queryProgress();

  // 10. 监督
  await domain.supervisorStart("web", { command: "node", args: ["web.js"] });
  await domain.supervisorStop("web");
  await domain.supervisorRestart("web");
  await domain.supervisorUpdate("web");

  // 11. 订阅
  await domain.subscribeLogs();
  await domain.unsubscribeLogs();
  await domain.subscribeTelemetry();
  await domain.unsubscribeTelemetry();
  await domain.subscribeNotify();
  await domain.unsubscribeNotify();
  await domain.subscribeObserver();
  await domain.unsubscribeObserver();

  // 12. 通知
  await domain.notify("info", "hello", "title", { k: 1 });

  // 13. C4
  domain.enableFileInterception();
  await domain.putObject("vfs://out/a.bin");
  await domain.getObject("obj_1");
  await domain.deleteObject("obj_1");
  const objId: string | null = domain.fileObjectId("vfs://out/a.bin");
  await domain.refreshFileIndex();

  // 14. 日志（多状态走 options）
  domain.logger.info("plain");
  domain.logger.warn("careful", { phase: "start" });
  domain.logger.error("bad", { ephemeral: true, key: "k", frac: 0.5, state: "failed" });
  domain.logger.debug("noise", { console_only: true });

  // 15. 端口
  const bindings = domain.bindings;

  // 生命周期
  await domain.runAsync({ onStart: async () => undefined, onClose: async () => undefined });
  domain.run();
  domain.shutdown();

  // 错误类型
  try {
    await domain.request("x.y", "T");
  } catch (error) {
    if (error instanceof DomainRequestError) {
      const status: string = error.status;
      void status;
      void error.data;
      void error.message_id;
    }
  }

  void payload;
  void waited;
  void total;
  void objId;
  void bindings;
}

void main();
