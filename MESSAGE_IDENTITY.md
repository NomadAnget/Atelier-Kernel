# Kernel message identity and fan-out

One accepted submission has one kernel-generated logical_id. The kernel deduplicates
targets in first-seen order and allocates one distinct message_id per remaining
target, including invalid targets. All children retain logical_id, authenticated
source_domain, original targets, parameters and data. Single-target submissions also
have both IDs. IDs include a kernel-instance UUID to avoid collisions across restarts.

Submission identity is (authenticated source domain, physical process generation, seq)
within the source process session; generation remains internal to the kernel. Repeating a positive seq replays the original children's authoritative
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
Durable facts and completed results are retained in SQLite and read on demand;
Volatile/Ephemeral keep their nonpersistent policies. On kernel restart, every old
nonterminal Durable delivery becomes an explicit failure with possibly unknown
execution outcome, without automatic replay. Sequence deduplication resets with
the sender process generation. Business side effects still need domain-owned idempotency.

Current reliability and compatibility evidence is recorded in
[CORE_RELIABILITY_ACCEPTANCE.md](docs/CORE_RELIABILITY_ACCEPTANCE.md).
Python/Node SDKs expose async send returning kernel child IDs and result waits by
those IDs. They learn identities from initial kernel receipts, retain early terminal
receipts in bounded local memory, and never allocate message IDs themselves.
