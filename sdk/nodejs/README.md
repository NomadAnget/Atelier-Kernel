# `atelier-sdk` (Node.js / TypeScript Domain SDK for Atelier Kernel)

Official zero-dependency Node.js / TypeScript SDK for building **Atelier Kernel** domains.

## Installation

```bash
npm install ./atelier-sdk-0.1.0.tgz
```

## Quick Start

```javascript
const { AtelierDomain, DomainExecutionFailed } = require("atelier-sdk");

const domain = new AtelierDomain();

domain.handler("deduct", (msg) => {
  const { sku, qty } = msg.data;
  domain.storageInsert(sku, "stock_item", { sku, deducted: qty });
  return { ok: true, sku, deducted: qty };
});

domain.run();
```
