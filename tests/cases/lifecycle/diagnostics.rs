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
/// Windows 命名管道可用文件 API 打开；Unix domain socket 不是文件，必须 connect()。
const IPC_OPEN: &str = r#"import os, socket, sys
def _ipc(name, mode):
    path = os.environ[name]
    if sys.platform == "win32":
        return open(path, mode, buffering=0)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(path)
    return sock.makefile(mode, buffering=0)
"#;

fn probe(code: &str, stages: &[&str]) -> Value {
    probe_within(code, stages, Duration::from_secs(10))
}

fn probe_within(code: &str, stages: &[&str], timeout: Duration) -> Value {
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
    let deadline = Instant::now() + timeout;
    loop {
        let text = fs::read_to_string(&log).unwrap();
        for line in text.lines() {
            if let Ok(row) = serde_json::from_str::<Value>(line) {
                let event = &row["diagnostic"];
                if stages.iter().any(|s| event["stage"] == *s) {
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
        assert!(Instant::now() < deadline, "missing {stages:?}: {text}");
        thread::sleep(Duration::from_millis(10));
    }
}
#[test]
fn natural_child_exit_has_original_code() {
    // 传输改成命名端点后，域的管道 EOF 与进程退出不再同步：内核可能先观测到
    // 断连而进入清理。真正重要的是**原始退出码被保留**，而不是把这次退出
    // 归因为「自然退出」——那条断言编码的是旧传输的时序巧合。
    let row = probe(&format!("{IPC_OPEN}\nr=_ipc('ATELIER_IPC_IN','rb'); w=_ipc('ATELIER_IPC_OUT','wb'); sys.exit(23)"), &["child_exit", "child_exit_after_cleanup"]);
    assert_eq!(row["diagnostic"]["details"]["code"], 23);
}
#[cfg(unix)]
#[test]
fn natural_child_signal_is_observed_before_cleanup() {
    let row = probe(
        &format!("{IPC_OPEN}\nimport signal\nr=_ipc('ATELIER_IPC_IN','rb'); w=_ipc('ATELIER_IPC_OUT','wb'); os.kill(os.getpid(),signal.SIGTERM)"),
        &["child_exit", "child_exit_after_cleanup"],
    );
    assert_eq!(row["diagnostic"]["details"]["signal"], 15);
    assert!(row["diagnostic"]["details"]["code"].is_null());
}
#[cfg(unix)]
#[test]
fn oversized_rx_records_original_error_before_cleanup() {
    // 必须像真实域一样先连 IN 再连 OUT：只开 OUT 时内核的 tx 线程会永远卡在 accept，
    // 回收路径一旦置 `stop`，`pump_lines` 就不记录 rx 诊断，这条断言会退化成超时。
    // 16 MiB+1 的越界帧要走完整条 socket/管道链路；macOS CI 在并发负载下 10 s 会抖，
    // 给这条用例单独的更宽限期，避免把"慢"误判成"没有诊断"。
    let row = probe_within(
        &format!("{IPC_OPEN}\nimport time\nr=_ipc('ATELIER_IPC_IN','rb'); w=_ipc('ATELIER_IPC_OUT','wb'); w.write(b'x'*(16*1024*1024+1)); time.sleep(30)"),
        &["rx_read"],
        Duration::from_secs(60),
    );
    assert_eq!(row["diagnostic"]["details"]["kind"], "InvalidData");
    assert_eq!(
        row["diagnostic"]["details"]["message"],
        "IPC/diagnostic frame exceeds limit"
    );
    assert_eq!(row["first_observed_fault"]["stage"], "rx_read");
}
