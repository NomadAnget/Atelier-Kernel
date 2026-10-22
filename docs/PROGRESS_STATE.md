# Explicit progress outcomes

### Progress outcomes

`logger.progress` accepts optional `state`: `running`, `completed`, `failed`, or
`cancelled`. Explicit state takes precedence over legacy `done`. Missing/null state
maps done=true to completed, otherwise running. Terminal updates normalize
`done=true`, `waiting=false`, preserve `frac`, and remove the source-domain/key
from active progress before broadcasting. Reuse the original key to finish it.
Invalid states are rejected. Python state is keyword-only; Node uses options.

Events remain ephemeral and may be lost. A missing active key means no longer
active, not successful. QueryProgress refreshes active entries, not past outcomes.
Older kernels discard state; paired kernel/SDK upgrades are required for failure
semantics. Old UIs must not interpret done as success. Process-crash cleanup is
outside this change. No subscriber queue or retry management API is added.

Active identity is (kernel-bound source_domain, key), isolating identical keys across domains. Ordinary logs omit absent state; no database schema change. Rust exhaustive LogRecord literals must add state: None.

Regression tests cover terminal cleanup, explicit precedence, legacy done, fractions, source isolation, invalid values, and Python/Node wire parity. Source change only; not released or deployed.
