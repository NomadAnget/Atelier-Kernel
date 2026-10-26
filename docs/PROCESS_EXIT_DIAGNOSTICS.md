> 历史记录：以下结论、版本、计数和部署状态对应记录当时的基线，不代表当前发布状态。当前接口以[文档索引](INDEX.md)及宪法为准；旧证据保留，不作当前保证。

# Per-generation process failure diagnostics (local patch)

Baseline: 1efd851. This patch does not recover historical process exits and does
not fix the underlying task/pipelines disconnect. No SDK or business call changes.

Kernel stderr emits bounded JSON lines with `diagnostic` and
`first_observed_fault`. Each event carries `domain_id`, `pid`, `generation`, `ts`,
`stage`, and `details`. Existing stderr capture must be retained across restarts.
No business payloads are copied, no SQLite diagnostic table or subscription API
is added. Diagnostic errors contain at most 1024 Unicode characters. Each fixed
stage is recorded once per generation, with the first observed transport fault
retained alongside later events. Records are reclaimed with that generation;
there is no unbounded in-memory history.

Stages distinguish `tx_write`, `tx_acknowledge`, `outbox`, `rx_read`, `rx_drain`,
`child_poll`, `child_exit`, `cleanup_requested`, `child_kill`, `child_wait`, and
`child_exit_after_cleanup`. I/O errors retain kind, raw OS error, and bounded
message. Oversized outbound frames also report serialized bytes including newline
and the IPC limit. Outbox cleanup cannot overwrite an existing queue failure.

`child_exit` is an exit status observed before a cleanup request. Its `code` and
Unix `signal` are distinct (signal is null on Windows). If no status was observed
before cleanup, record a cleanup request and the subsequently observed exit as
`child_exit_after_cleanup`, with `after_cleanup_request=true`. This does not prove
that cleanup killed it: natural exit can race with a kill request. A previously
observed natural status is retained, and a redundant kill is avoided.

The synthesized `transport ended after draining buffered frames` is `rx_drain`,
never a replacement primary fault. Genuine RX failures remain `rx_read`, even if
a drain has concurrently begun. First observed means first captured under the
per-generation mutex, not proof of physical causality; thread scheduling and OS
polling can reorder observations. Child exit and transport evidence must be
interpreted together. A BrokenPipe may be a consequence of child exit, and an
outbox error may precede or follow another transport error.

Diagnostic output is best-effort stderr using the existing kernel output sink.
It is not durable messaging and cannot report the kernel's own SIGKILL/OOM or WSL
shutdown reliably. SDK silent close paths remain unchanged. No automatic retry,
receipt reordering, or change to drain-before-fault policy is introduced.

## Validation and deployment prerequisites

Unit tests cover first-error retention, bounded deduplication, actual BrokenPipe,
exit codes and signal metadata, and preserving an original outbox failure when
cleanup also fails. Isolated subprocess integration tests exercise a real kernel,
Python exit 23, Unix SIGTERM, and over-limit incoming pipe data. The existing
buffered-receipt drain regression must continue passing. Probes use only repo tmp
DBs, never a variant database.

Deployment requires separate authorization, an idle runtime or a coordinated
maintenance window, backup of the binary and current launch configuration, and
continuous stderr capture to a known retained file. Record the exact build commit
and hash. Validate in an isolated runtime first. Do not clear database-owner locks
or infer that this local patch is already installed. For the next incident collect
all events for the same domain/pid/generation plus SDK stderr, OS journal and WSL
boot ID; do not infer root cause from the generic disconnect line alone.

## Current verification status (2026-10-23)

The earlier paused run is historical. After explicit user authorization this
patch was validated, built and deployed locally. The current combined build
(progress + diagnostics + domain env + process reaping) passed all-target Rust
regression; the 3 isolated diagnostic integration tests passed. Deployment:
`atelier-reel-solo-kernel/tmp/process-reaping-deploy-20261023-155019/manifest.json`.
Kernel PID454750; binary SHA256
fd26469a51626880b388aa76ce2df50aa1292ec44a0fe9d4dc531eb57b618ecf.
This diagnostic patch remains local/uncommitted and does not prove the historical
task/pipelines disconnect root cause. See RELIABILITY_STATUS.md for open risks.
