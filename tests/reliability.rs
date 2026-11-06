//! 可靠性：崩溃窗口、原子提交、恢复与所有权、对抗性审计。

#[path = "cases/reliability/ledger.rs"]
mod ledger;
#[path = "cases/reliability/regressions.rs"]
mod regressions;
#[path = "cases/reliability/adversarial.rs"]
mod adversarial;
