# `atelier-sdk` (Python Domain SDK for Atelier Kernel)

Official zero-dependency Python SDK for building **Atelier Kernel** domains.

## Installation

```bash
pip install atelier_sdk-0.1.1-py3-none-any.whl
```

## Quick Start

```python
from atelier_sdk import AtelierDomain, DomainExecutionFailed

domain = AtelierDomain()

@domain.handler("create")
def on_create(msg):
    order_id = msg.data.get("order_id", "ORD-001")
    domain.storage_insert(record_id=order_id, category="sales_order", data=msg.data)
    return {"ok": True, "order_id": order_id}

if __name__ == "__main__":
    domain.run()
```

Log records returned by the kernel observer include source_domain (kernel-bound IPC owner, kernel for internal records, null for historical records before v0.1.1). SDK callers do not need to supply it; a claimed source is overwritten by the kernel.
