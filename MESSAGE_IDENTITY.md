# Kernel message identity and fan-out

One accepted submission has one kernel-generated logical_id. The kernel deduplicates
targets in first-seen order and allocates one distinct message_id per remaining
target, including invalid targets. All children retain logical_id, authenticated
source_domain, original targets, parameters and data. Single-target submissions also
have both IDs. IDs include a kernel-instance UUID to avoid collisions across restarts.

Submission identity is (authenticated source domain, seq) within the source process
session. Repeating a positive seq replays the original children's authoritative
receipts and ID list, without allocating new IDs or executing again. seq=0 opts out.
The kernel allocates the complete split before dispatching the first child.

The ledger keys each independent delivery by message_id. Target ownership is still
checked for every receipt. Repeated or regressing stages are absorbed; terminal
results cannot be overwritten. Duplicate target addresses execute once.

Delivery, caller receipt, observation and ledger records expose logical_id and
message_id. Target receipts only need to report message_id; the kernel obtains
logical_id from its authoritative ledger. Domain implementations cannot choose it.
Native submission methods and HTTP send return the ordered child ID list.

This is session-scoped transport idempotency, not durable exactly-once execution:
the current ledger is in memory, is lost on kernel restart, and submission sequence
deduplication resets when the sender process session changes. Business side effects
still need domain-owned idempotency. No automatic replay of interrupted execution.

Validation: 14 Rust integration tests passed, including four dedicated identity
scenarios in tests/message_identity.rs. A real Python/Node two-domain stdio smoke
run passed with one logical_id, two distinct child IDs and all four receipt stages;
evidence: tmp/message-identity-smoke/runtime.log. Four paired SDK suites also pass.
Changes are local source only; no Release or running variant runtime was replaced.
Python/Node SDKs now expose async send returning kernel child IDs and result waits
by those IDs. The SDK learns identities from initial kernel receipts, retains early
terminal receipts in bounded local memory, and never allocates message IDs itself.
