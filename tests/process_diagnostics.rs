//! Isolated subprocess probes; never opens a variant/production database.
use serde_json::{Value, json};
use std::{
    fs,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
struct Probe(Child);
impl Drop for Probe {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn probe(code: &str, stage: &str) -> Value {
    let work = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp/process-diagnostics")
        .join(uuid::Uuid::new_v4().to_string());
    fs::create_dir_all(&work).unwrap();
    let config = work.join("boot.json");
    let log = work.join("stderr.log");
    fs::write(
        &config,
        json!({"db_path":work.join("probe.db"),"web_ui_enabled":false,"domains":[{
            "domain_id":"diagnostic_probe","command":if cfg!(windows){"python"}else{"python3"},
            "args":["-c",code],"auto_restart":false
        }]})
        .to_string(),
    )
    .unwrap();
    let mut child = Probe(
        Command::new(env!("CARGO_BIN_EXE_atelier-kernel"))
            .args(["--headless", "--config", config.to_str().unwrap()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(fs::File::create(&log).unwrap())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let text = fs::read_to_string(&log).unwrap();
        for line in text.lines() {
            if let Ok(row) = serde_json::from_str::<Value>(line) {
                let event = &row["diagnostic"];
                if event["stage"] == stage {
                    assert_eq!(event["domain_id"], "diagnostic_probe");
                    assert!(event["pid"].as_u64().unwrap() > 0);
                    assert!(event["generation"].as_u64().unwrap() > 0);
                    return row;
                }
            }
        }
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "kernel exited: {text}"
        );
        assert!(Instant::now() < deadline, "missing {stage}: {text}");
        thread::sleep(Duration::from_millis(10));
    }
}
#[test]
fn natural_child_exit_has_original_code() {
    let row = probe("import sys;sys.exit(23)", "child_exit");
    assert_eq!(row["diagnostic"]["details"]["code"], 23);
    assert_eq!(row["diagnostic"]["details"]["after_cleanup_request"], false);
}
#[cfg(unix)]
#[test]
fn natural_child_signal_is_observed_before_cleanup() {
    let row = probe(
        "import os,signal;os.kill(os.getpid(),signal.SIGTERM)",
        "child_exit",
    );
    assert_eq!(row["diagnostic"]["details"]["signal"], 15);
    assert!(row["diagnostic"]["details"]["code"].is_null());
}
#[cfg(unix)]
#[test]
fn oversized_rx_records_original_error_before_cleanup() {
    let row = probe(
        "import os,time;f=os.fdopen(int(os.environ['ATELIER_IPC_WRITE_FD']),'wb',buffering=0);f.write(b'x'*(16*1024*1024+1));time.sleep(30)",
        "rx_read",
    );
    assert_eq!(row["diagnostic"]["details"]["kind"], "InvalidData");
    assert_eq!(
        row["diagnostic"]["details"]["message"],
        "IPC/diagnostic frame exceeds limit"
    );
    assert_eq!(row["first_observed_fault"]["stage"], "rx_read");
}
