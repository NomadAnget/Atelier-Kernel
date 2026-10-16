# `atelier-sdk` (Node.js / TypeScript Domain SDK for Atelier Kernel)

Official zero-dependency Node.js / TypeScript SDK for building **Atelier Kernel** domains.

## Installation

```bash
npm install ./atelier-sdk-0.1.1.tgz
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

Log records returned by the kernel observer include source_domain (kernel-bound IPC owner, kernel for internal records, null for historical records before v0.1.1). SDK callers do not need to supply it; a claimed source is overwritten by the kernel.
