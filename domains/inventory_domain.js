#!/usr/bin/env node
const { AtelierDomain } = require("./atelier_sdk");

const domain = new AtelierDomain("inventory");

domain.handler("deduct", async () => ({ ok: true }), "扣减指定 SKU 的库存并持久化最新库存快照");
domain.handler("restock", async () => ({ ok: true }), "补充指定 SKU 的库存数量");
domain.handler("stock_info", async () => ({ ok: true }), "查询当前所有 SKU 的实时库存账本");

domain.run();
