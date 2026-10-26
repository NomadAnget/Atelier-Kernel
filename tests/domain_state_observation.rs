//! Domain states converge via the observation subscription, without status polling.
use atelier_kernel::{AtelierKernel, DomainProcessSpec, DomainStatus};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};
fn python() -> &'static str {
    if cfg!(windows) { "python" } else { "python3" }
}
fn wait_snapshot(path: &Path, check: impl Fn(&Value) -> bool) -> Value {
    let until = Instant::now() + Duration::from_secs(12);
    loop {
        if let Ok(s) = std::fs::read(path) {
            if let Ok(v) = serde_json::from_slice::<Value>(&s) {
                if check(&v) {
                    return v;
                }
            }
        }
        assert!(
            Instant::now() < until,
            "subscription state did not converge: {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(10));
    }
}
fn state<'a>(v: &'a Value, name: &str) -> Option<&'a Value> {
    v["domains"]
        .as_array()?
        .iter()
        .find(|d| d["domain_id"] == name)
}
fn exercise(node: bool) {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let folder =
        std::env::temp_dir().join(format!("atelier-domain-states-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&folder).unwrap();
    let report = folder.join("state.json");
    let py = r#"import asyncio,json,sys,os
from pathlib import Path
from atelier_sdk import AtelierDomain
d=AtelierDomain(domain='watcher')
@d.handler('ping')
def ping(m):return {'ok':True}
@d.on_observation
async def observe(event):
 if event.get('message_type')=='DomainStateSnapshot':
  await asyncio.sleep(.03)
  assert d.observer.domain_states is not None
  p=Path(sys.argv[1]);tmp=p.with_suffix('.tmp');tmp.write_text(json.dumps(event['details']));tmp.replace(p)
d.run()
"#;
    let js = r#"const {AtelierDomain}=require(process.argv[1]);const fs=require('node:fs');const d=new AtelierDomain({domain:'watcher'});d.handler('ping',()=>({ok:true}));d.onObservation(async event=>{if(event.message_type==='DomainStateSnapshot'){await new Promise(r=>setTimeout(r,30));if(!d.observer.domainStates)throw Error('cache missing');fs.writeFileSync(process.argv[2]+'.tmp',JSON.stringify(event.details));fs.renameSync(process.argv[2]+'.tmp',process.argv[2]);}});d.run();"#;
    let args: Vec<String> = if node {
        vec![
            "-e".into(),
            js.into(),
            root.join("sdk/nodejs").to_string_lossy().into(),
            report.to_string_lossy().into(),
        ]
    } else {
        vec!["-c".into(), py.into(), report.to_string_lossy().into()]
    };
    let mut observer =
        DomainProcessSpec::new("watcher", if node { "node" } else { python() }, args);
    observer.auto_restart = false;
    observer.env.insert(
        "PYTHONPATH".into(),
        root.join("sdk/python").to_string_lossy().into(),
    );
    let worker_py = r#"from atelier_sdk import AtelierDomain
d=AtelierDomain(domain='worker')
@d.handler('ping')
def ping(m):return {'ok':True}
d.run()
"#;
    let mut worker =
        DomainProcessSpec::new("worker", python(), vec!["-c".into(), worker_py.into()]);
    worker.env.insert(
        "PYTHONPATH".into(),
        root.join("sdk/python").to_string_lossy().into(),
    );
    worker.restart_delay_ms = 10;
    let k = AtelierKernel::new_in_memory().unwrap();
    k.supervisor().spawn_process_domain(worker.clone()).unwrap();
    k.supervisor().spawn_process_domain(observer).unwrap();
    let active = wait_snapshot(&report, |v| {
        state(v, "worker").is_some_and(|s| s["status"] == "active")
    });
    let stream = active["stream_id"].clone();
    let _first = k.supervisor().get_pid("worker").unwrap();
    k.supervisor().stop_process_domain("worker");
    wait_snapshot(&report, |v| {
        state(v, "worker").is_some_and(|s| s["status"] == "stopped")
    });
    assert_eq!(
        k.router().domain_status("worker"),
        Some(DomainStatus::Stopped)
    );
    k.supervisor().spawn_process_domain(worker).unwrap();
    wait_snapshot(&report, |v| {
        state(v, "worker").is_some_and(|s| s["status"] == "active")
            && v["revision"] != active["revision"]
    });
    {
        let pid = k.supervisor().get_pid("worker").unwrap();
        assert_ne!(pid, _first);
        #[cfg(unix)]
        let exit = std::process::Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status()
            .unwrap();
        #[cfg(windows)]
        let exit = std::process::Command::new("taskkill")
            .args(["/F", "/PID", &pid.to_string()])
            .status()
            .unwrap();
        assert!(exit.success(), "failed to terminate test worker");
        let end = Instant::now() + Duration::from_secs(10);
        while k.supervisor().get_pid("worker").is_none_or(|p| p == pid) {
            assert!(Instant::now() < end);
            thread::sleep(Duration::from_millis(10));
        }
    }
    // Deliberately overrun ordinary observer buffers and keep the last state image.
    for i in 0..300 {
        let d = k.attach_domain(&format!("burst_{i}"));
        d.register_handlers(&["ping"]);
    }
    let revision = k.observer().domain_state_snapshot().details.unwrap();
    let expected = revision["revision"].as_u64().unwrap();
    let final_state = wait_snapshot(&report, |v| {
        v["revision"].as_u64().is_some_and(|r| r >= expected)
            && state(v, "worker").is_some_and(|s| s["status"] == "active")
    });
    assert_eq!(final_state["stream_id"], stream);
    assert!(state(&final_state, "burst_299").is_some());
    // A fresh subscription must get current state even when nothing changes afterwards.
    k.supervisor().respawn_domain("watcher").unwrap();
    let generation = k.router().domain_instance_id("watcher").unwrap();
    wait_snapshot(&report, |v| {
        state(v, "watcher")
            .is_some_and(|s| s["instance_id"] == generation && s["status"] == "active")
    });
    assert!(
        !k.observer()
            .snapshot()
            .iter()
            .any(|e| e.source_domain == "watcher"
                && matches!(
                    e.message_type.as_str(),
                    "QueryRuntime" | "QueryDomainAssembly"
                )),
        "SDK used polling to recover states"
    );
    let transitions = k.observer().snapshot();
    assert!(
        transitions
            .iter()
            .any(|e| e.message_type == "DomainStatusChanged")
    );
    for expected in ["spawning", "active", "faulted", "stopped"] {
        assert!(
            transitions
                .iter()
                .any(|e| e.message_type == "DomainStatusChanged"
                    && e.details
                        .as_ref()
                        .is_some_and(|d| d["domain_id"] == "worker" && d["status"] == expected)),
            "missing actual worker state transition: {expected}"
        );
    }
    k.supervisor().stop_process_domain("worker");
    k.supervisor().stop_process_domain("watcher");
    drop(k);
    let _ = std::fs::remove_dir_all(folder);
}
#[test]
fn python_domain_states_survive_loss_and_resubscription() {
    exercise(false)
}
#[test]
fn node_domain_states_survive_loss_and_resubscription() {
    exercise(true)
}
