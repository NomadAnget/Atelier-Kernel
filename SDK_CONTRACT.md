# SDK responsibility boundary (Python / Node)

The kernel owns domain lifecycle, registered routes, authenticated source identity,
global receipt state, persistence, and process supervision. RegisterHandlers makes
the domain Active according to the kernel protocol. The SDK does not introduce a
second readiness gate or health endpoint.

The SDK is a protocol client: JSON-lines transport, handler invocation and protocol
receipts, asynchronous receipt correlation, awaitable kernel storage messages, and
cleanup of its local connection resources. Request timeout or cancellation only
stops local waiting; it does not undo remote work. No automatic business retry.

Business data validation, authorization rules, error payload shape, concurrency,
task queues, and dependency orchestration belong to domains or product assembly.
Handlers accept opaque JSON data. DomainExecutionFailed passes the domain's own
payload through; there is no required application error schema.

Removed from both SDKs: schema/validator handler options, allowed-source policy,
DomainError, health/readiness handlers and polling, business admission queue and
concurrency quotas. Domain dependency ping logic stays in product assembly.

Only local transport safeguards remain: max_pending=1024, max_outbox=256,
max_frame_bytes=16777216, shutdown_grace=5 seconds (Node camelCase). These limit
connection resources, not business data shape or global task scheduling. Local
connection states new/open/draining/closed are private, not kernel domain states.
shutdown drains local handler calls and pending waits within a deadline, then
cancels cooperatively and calls the cleanup hook. It cannot kill arbitrary native
work; process termination and restart remain the kernel supervisor's job.

Async storage helpers send existing kernel.storage CRUD messages and return kernel
response data. They do not implement storage, transactions, migration or paging.
send returns kernel-assigned ID(s) after initial receipts for all unique targets.
wait_result/wait_results consume results by ID; request_many is an alias for waiting
existing IDs, never a second send. request combines send/wait with one deadline.
Timeout/cancellation preserves a public wait slot; completed/failed result consumption
releases it. Explicit forget_result releases an unwanted slot. Unread results and
submissions share max_pending capacity and are never silently evicted.

Request context is a read-only snapshot of the current kernel envelope identity:
logical_id, message_id, source_domain, target and message_type. The SDK generates
no tracing identity, does not interpret or inject params._sdk, and does not add
correlation fields to logs. New outbound messages receive their identity from the
kernel. Local Future/Promise receipt correlation remains client plumbing.

Paired tests cover wire receipts, async RPC, large frames, resource bounds,
storage messages, opaque business payloads/errors, startup without a second gate,
and connection cleanup. Runtime integration is tested with the installed packages.
