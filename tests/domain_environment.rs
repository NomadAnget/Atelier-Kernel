use atelier_kernel::{AtelierKernel, DomainProcessSpec, DomainStatus, KernelBootConfig};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};
fn work() -> PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp/domain-environment")
        .join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&p).unwrap();
    p
}
fn python() -> String {
    std::env::var("PYTHON")
        .unwrap_or_else(|_| if cfg!(windows) { "python" } else { "python3" }.into())
}
const SCRIPT: &str = r#"import os,json,time,sys
from pathlib import Path
root=Path(sys.argv[1])
value={'pid':os.getpid(),'custom':os.environ.get('ENV_PROBE_VALUE'),'empty':os.environ.get('ENV_PROBE_EMPTY'),'path':os.environ.get('PATH'),'read_fd':os.environ.get('ATELIER_IPC_READ_FD'),'write_fd':os.environ.get('ATELIER_IPC_WRITE_FD'),'mode':os.environ.get('ATELIER_IPC_HANDLE_MODE')}
if os.environ.get('ENV_PROBE_REGISTER','yes')=='yes':
 fd=int(os.environ['ATELIER_IPC_WRITE_FD'])
 if os.name=='nt':
  import msvcrt
  fd=msvcrt.open_osfhandle(fd,os.O_WRONLY)
 os.write(fd,(json.dumps({'frame_type':'send','seq':1,'targets':['kernel.registry'],'message_type':'RegisterHandlers','params':{'handlers':['ping']},'data':{}})+'\n').encode())
tmp=root/(str(os.getpid())+'.tmp');tmp.write_text(json.dumps(value));tmp.replace(root/(str(os.getpid())+'.json'))
if os.environ.get('ENV_PROBE_EXIT_ONCE')=='yes' and not (root/'exited-once').exists():
 (root/'exited-once').write_text('yes');sys.exit(7)
fd=int(os.environ['ATELIER_IPC_READ_FD'])
if os.name=='nt':
 import msvcrt
 fd=msvcrt.open_osfhandle(fd,os.O_RDONLY)
while os.read(fd,8192):pass
"#;
fn spec(root: &Path) -> DomainProcessSpec {
    let mut s = DomainProcessSpec::new(
        "env_probe",
        python(),
        vec!["-c".into(), SCRIPT.into(), root.to_string_lossy().into()],
    );
    s.auto_restart = false;
    s.env
        .insert("ENV_PROBE_VALUE".into(), "literal $HOME ~ 权重=ok".into());
    s.env.insert("ENV_PROBE_EMPTY".into(), String::new());
    s
}
fn report(root: &Path, pid: u32) -> Value {
    let path = root.join(format!("{pid}.json"));
    let deadline = Instant::now() + Duration::from_secs(8);
    while !path.exists() {
        assert!(Instant::now() < deadline, "probe report missing");
        thread::sleep(Duration::from_millis(10));
    }
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}
fn ready(k: &AtelierKernel) {
    let d = Instant::now() + Duration::from_secs(5);
    while k.router().domain_status("env_probe") != Some(DomainStatus::Active) {
        assert!(Instant::now() < d);
        thread::sleep(Duration::from_millis(5));
    }
}
#[test]
fn defaults_and_invalid_environment_are_checked_before_spawn() {
    let base = json!({"domain_id":"env_probe","command":"never-executed"});
    let s: DomainProcessSpec = serde_json::from_value(base.clone()).unwrap();
    assert!(s.env.is_empty());
    assert!(serde_json::to_value(s).unwrap().get("env").is_none());
    for invalid in [Value::Null, json!([]), json!({"A":1}), json!({"A":null})] {
        let mut v = base.clone();
        v["env"] = invalid;
        assert!(serde_json::from_value::<DomainProcessSpec>(v).is_err());
    }
    for (key, value) in [
        ("", "x"),
        ("A=B", "x"),
        ("A\0B", "x"),
        ("SAFE", "x\0y"),
        ("ATELIER_IPC_READ_FD", "7"),
        ("atelier_ipc_handle_mode", "win32"),
        ("ATELIER_IPC_FUTURE", "x"),
    ] {
        let mut v = base.clone();
        v["env"] = json!({key:value});
        let spec: DomainProcessSpec = serde_json::from_value(v.clone()).unwrap();
        let k = AtelierKernel::new_in_memory().unwrap();
        assert!(k.spawn_process_domain(spec).is_err());
        let config: KernelBootConfig = serde_json::from_value(json!({"domains":[v]})).unwrap();
        assert!(config.validate("probe.json").is_err());
    }
    let mut v = base;
    v["env"] = json!({"Path":"a","PATH":"b"});
    let s: DomainProcessSpec = serde_json::from_value(v).unwrap();
    assert!(
        AtelierKernel::new_in_memory()
            .unwrap()
            .spawn_process_domain(s)
            .is_err()
    );
}
#[test]
fn boot_manual_restart_and_saved_spec_preserve_environment_without_parent_mutation() {
    let root = work();
    let k = AtelierKernel::new_in_memory().unwrap();
    let mut s = spec(&root);
    let parent_path = std::env::var("PATH").unwrap_or_default();
    let key = std::env::var("ENV_PROBE_VALUE").ok();
    let path = format!(
        "{parent_path}{}unused-env-probe",
        if cfg!(windows) { ';' } else { ':' }
    );
    s.env.insert("PATH".into(), path.clone());
    let config: KernelBootConfig = serde_json::from_value(json!({"domains":[s.clone()]})).unwrap();
    let results = k.supervisor().boot_from_config(&config);
    let first = results[0].1.as_ref().unwrap();
    let a = report(&root, *first);
    ready(&k);
    assert_eq!(a["custom"], "literal $HOME ~ 权重=ok");
    assert_eq!(a["empty"], "");
    assert_eq!(a["path"], path);
    assert!(a["read_fd"].as_str().unwrap().parse::<u64>().unwrap() > 0);
    assert!(a["write_fd"].as_str().unwrap().parse::<u64>().unwrap() > 0);
    #[cfg(unix)]
    assert!(a["mode"].is_null());
    let next = k.supervisor().respawn_domain("env_probe").unwrap();
    assert_ne!(*first, next);
    assert_eq!(report(&root, next)["custom"], a["custom"]);
    ready(&k);
    k.supervisor().stop_process_domain("env_probe");
    let last = k.supervisor().respawn_domain("env_probe").unwrap();
    assert_eq!(report(&root, last)["path"], path);
    assert_eq!(std::env::var("PATH").unwrap_or_default(), parent_path);
    assert_eq!(std::env::var("ENV_PROBE_VALUE").ok(), key);
    assert_eq!(k.supervisor().get_spec("env_probe").unwrap().env, s.env);
}
#[test]
fn auto_restart_uses_same_environment() {
    let root = work();
    let k = AtelierKernel::new_in_memory().unwrap();
    let mut s = spec(&root);
    s.auto_restart = true;
    s.restart_delay_ms = 30;
    s.max_restarts = Some(3);
    s.env.insert("ENV_PROBE_EXIT_ONCE".into(), "yes".into());
    let first = k.spawn_process_domain(s).unwrap();
    assert_eq!(report(&root, first)["custom"], "literal $HOME ~ 权重=ok");
    let deadline = Instant::now() + Duration::from_secs(8);
    let second = loop {
        if let Some(pid) = k.supervisor().get_pid("env_probe") {
            if pid != first {
                break pid;
            }
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(report(&root, second)["custom"], "literal $HOME ~ 权重=ok");
    ready(&k);
}
#[test]
fn successful_update_and_failed_candidate_keep_correct_environment() {
    let root = work();
    let k = AtelierKernel::new_in_memory().unwrap();
    let s = spec(&root);
    let first = k.spawn_process_domain(s.clone()).unwrap();
    report(&root, first);
    ready(&k);
    let mut next = s.clone();
    next.env.insert("ENV_PROBE_VALUE".into(), "updated".into());
    let updated = k
        .supervisor()
        .update_process_domain(
            "env_probe",
            Some(next.clone()),
            None,
            Duration::from_secs(3),
        )
        .unwrap();
    assert_eq!(report(&root, updated.new_pid)["custom"], "updated");
    let mut invalid = next.clone();
    invalid
        .env
        .insert("ENV_PROBE_VALUE".into(), "candidate-not-committed".into());
    invalid.env.insert("ENV_PROBE_REGISTER".into(), "no".into());
    assert!(
        k.supervisor()
            .update_process_domain("env_probe", Some(invalid), None, Duration::from_millis(200))
            .is_err()
    );
    assert_eq!(k.supervisor().get_pid("env_probe"), Some(updated.new_pid));
    assert_eq!(k.supervisor().get_spec("env_probe").unwrap().env, next.env);
    let reused = k
        .supervisor()
        .update_process_domain("env_probe", None, None, Duration::from_secs(3))
        .unwrap();
    assert_eq!(report(&root, reused.new_pid)["custom"], "updated");
}

#[cfg(unix)]
#[test]
fn stale_parent_ipc_mode_is_not_inherited_by_unix_domain() {
    use std::process::{Command, Stdio};
    let root = work();
    let config = root.join("boot.json");
    std::fs::write(
        &config,
        json!({"db_path":root.join("probe.db"),"web_ui_enabled":false,"domains":[spec(&root)]})
            .to_string(),
    )
    .unwrap();
    struct Host(std::process::Child);
    impl Drop for Host {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let _host = Host(
        Command::new(env!("CARGO_BIN_EXE_atelier-kernel"))
            .args(["--headless", "--config", config.to_str().unwrap()])
            .env("ATELIER_IPC_HANDLE_MODE", "win32")
            .env("ATELIER_IPC_READ_FD", "12345")
            .env("ATELIER_IPC_WRITE_FD", "12346")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(8);
    let file = loop {
        if let Some(file) = std::fs::read_dir(&root)
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| {
                p.extension().is_some_and(|ext| ext == "json")
                    && p.file_name().unwrap() != "boot.json"
            })
        {
            break file;
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(10));
    };
    let row: Value = serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
    assert!(row["mode"].is_null());
    assert_ne!(row["read_fd"], "12345");
    assert_ne!(row["write_fd"], "12346");
}
