# Kernel message identity and fan-out

One accepted submission has one kernel-generated logical_id. The kernel deduplicates
targets in first-seen order and allocates one distinct message_id per remaining
target, including invalid targets. All children retain logical_id, authenticated
source_domain, original targets, parameters and data. Single-target submissions also
have both IDs. IDs include a kernel-instance UUID to avoid collisions across restarts.

Submission identity is (authenticated source domain, physical process generation, seq)
within the source process session; generation remains internal to the kernel. The kernel
allocates the complete split before dispatching the first child.

Repeating a positive `seq` has three distinct outcomes, not one:

1. **Inside the dedup window** (the most recent 2048 submissions for that source
   generation): replay the original children's authoritative receipts and ID list without
   allocating new IDs or executing again.
2. **Below the high-water mark but evicted from the window**: the kernel must not guess.
   For any submission that requires deduplication (a `Durable` or `Volatile` target, or a
   mixed split), it **rejects explicitly** with `ContractInvalid` and error code
   `stale_submission`, naming the high-water mark. It never allocates a fresh identity and
   never silently re-executes.
3. **Pure `Ephemeral` submissions** opt out of the stale check (C3:56): while in flight a
   repeated seq still reuses the identity; after the submission reaches terminal and its
   identity is reclaimed, the same seq is a **new** transient delivery.

`seq=0` opts out of deduplication entirely.

The ledger keys each independent delivery by message_id. Target ownership is still
checked for every receipt. Repeated or regressing stages are absorbed; terminal
results cannot be overwritten. Duplicate target addresses execute once.

Delivery, caller receipt, observation and ledger records expose logical_id and
message_id. Target receipts only need to report message_id; the kernel obtains
logical_id from its authoritative ledger. Domain implementations cannot choose it.
Native submission methods and HTTP send return the ordered child ID list.

This is session-scoped transport idempotency, not durable exactly-once execution:
Durable facts and completed results are retained in SQLite and read on demand;
Volatile/Ephemeral keep their nonpersistent policies. On kernel restart, every old
nonterminal Durable delivery becomes an explicit failure with possibly unknown
execution outcome, without automatic replay. Sequence deduplication resets with
the sender process generation. Business side effects still need domain-owned idempotency.

Current reliability and compatibility evidence is recorded in
[核心可靠性验收记录](archive/CORE_RELIABILITY_ACCEPTANCE.md).
Python/Node SDKs expose async send returning kernel child IDs and result waits by
those IDs. They learn identities from initial kernel receipts, retain early terminal
receipts in bounded local memory, and never allocate message IDs themselves.
