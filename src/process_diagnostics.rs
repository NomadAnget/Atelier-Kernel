//! Bounded, per-generation failure evidence. Never retains a business payload.
use parking_lot::Mutex;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::Write;
use std::process::ExitStatus;

#[derive(Default)]
struct Evidence {
    stages: HashMap<&'static str, Value>,
    first_fault: Option<Value>,
}
pub(crate) struct ProcessDiagnostics {
    domain: String,
    pid: u32,
    generation: u64,
    evidence: Mutex<Evidence>,
}
impl ProcessDiagnostics {
    pub(crate) fn new(domain: &str, pid: u32, generation: u64) -> Self {
        Self {
            domain: domain.into(),
            pid,
            generation,
            evidence: Mutex::new(Evidence::default()),
        }
    }
    // Call sites use a fixed set of stages: bounded independently of frame count.
    pub(crate) fn record(&self, stage: &'static str, details: Value, fault: bool) {
        let mut evidence = self.evidence.lock();
        if evidence.stages.contains_key(stage) {
            return;
        }
        let event = json!({"event":"kernel_process_diagnostic", "domain_id":self.domain,
            "pid":self.pid,"generation":self.generation,"stage":stage,
            "ts":crate::logging::now_secs_f64(),"details":details});
        evidence.stages.insert(stage, event.clone());
        if fault && evidence.first_fault.is_none() {
            evidence.first_fault = Some(event.clone());
        }
        // Diagnostics must not panic when the host stderr sink has closed.
        let stderr = std::io::stderr();
        let _ = writeln!(
            stderr.lock(),
            "{}",
            json!({"diagnostic":event,"first_observed_fault":evidence.first_fault})
        );
    }
    pub(crate) fn exit(&self, status: ExitStatus, after_cleanup: bool) {
        #[cfg(unix)]
        let signal = {
            use std::os::unix::process::ExitStatusExt;
            status.signal()
        };
        #[cfg(not(unix))]
        let signal: Option<i32> = None;
        self.record(
            if after_cleanup {
                "child_exit_after_cleanup"
            } else {
                "child_exit"
            },
            json!({"code":status.code(),"signal":signal,"success":status.success(),
                "after_cleanup_request":after_cleanup}),
            false,
        );
    }
}
pub(crate) fn bounded(text: impl std::fmt::Display) -> String {
    text.to_string().chars().take(1024).collect()
}
pub(crate) fn io_error(error: &std::io::Error) -> Value {
    json!({"kind":format!("{:?}",error.kind()),"raw_os_error":error.raw_os_error(),"message":bounded(error)})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn first_fault_is_retained_and_repeated_stage_is_bounded() {
        let d = ProcessDiagnostics::new("probe", 42, 7);
        d.record("tx_write", json!({"kind":"BrokenPipe"}), true);
        for _ in 0..1000 {
            d.record("tx_write", json!({"kind":"replacement"}), true);
        }
        d.record("rx_drain", json!({"message":"transport ended"}), false);
        let e = d.evidence.lock();
        assert_eq!(e.stages.len(), 2);
        assert_eq!(
            e.first_fault.as_ref().unwrap()["details"]["kind"],
            "BrokenPipe"
        );
        assert_eq!(e.first_fault.as_ref().unwrap()["generation"], 7);
        assert_eq!(bounded("界".repeat(5000)).chars().count(), 1024);
    }
    #[test]
    fn child_exit_code_is_preserved() {
        let mut cmd = std::process::Command::new(if cfg!(windows) { "python" } else { "python3" });
        let status = cmd
            .args(["-c", "import sys;sys.exit(23)"])
            .status()
            .unwrap();
        let d = ProcessDiagnostics::new("probe", 42, 7);
        d.exit(status, false);
        assert!(d.evidence.lock().stages.contains_key("child_exit"));
        assert_eq!(
            d.evidence.lock().stages["child_exit"]["details"]["code"],
            23
        );
        assert_eq!(status.code(), Some(23));
    }
    #[cfg(unix)]
    #[test]
    fn child_signal_is_distinct_from_cleanup() {
        use std::os::unix::process::ExitStatusExt;
        let status = ExitStatus::from_raw(15);
        assert_eq!(status.signal(), Some(15));
        let d = ProcessDiagnostics::new("probe", 42, 7);
        d.exit(status, false);
        d.exit(status, true);
        assert_eq!(d.evidence.lock().stages.len(), 2);
        let e = d.evidence.lock();
        assert_eq!(e.stages["child_exit"]["details"]["signal"], 15);
        assert_eq!(
            e.stages["child_exit_after_cleanup"]["details"]["after_cleanup_request"],
            true
        );
        assert!(e.first_fault.is_none());
    }
}
