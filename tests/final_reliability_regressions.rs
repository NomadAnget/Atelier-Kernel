//! Final-review contract regressions. No production service or database is used.
use atelier_kernel::{
    AtelierKernel, ContractStatus as S, DomainProcessSpec, KernelToDomainFrame as F,
};
use serde_json::json;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
fn dir() -> PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp/final-fix-tests")
        .join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&p).unwrap();
    p
}
fn python() -> &'static str {
    if cfg!(windows) { "python" } else { "python3" }
}
fn spec(id: &str, script: &str) -> DomainProcessSpec {
    let mut s = DomainProcessSpec::new(id, python(), vec!["-c".into(), script.into()]);
    s.auto_restart = false;
    s
}
#[test]
#[ignore = "isolated owner process, invoked by exclusive_database_owner"]
fn owner_process_fixture() {
    let path = std::env::var("ATELIER_OWNER_DB").unwrap();
    let k = AtelierKernel::open_with_db(path);
    if std::env::var_os("ATELIER_OWNER_CRASH").is_some() {
        let _k = k.unwrap();
        std::process::exit(0);
    }
    assert!(k.is_err(), "second OS host must refuse the live database");
}
#[test]
fn exclusive_database_owner_preserves_live_result_and_releases_after_exit() {
    let path = dir().join("kernel.db");
    let k = AtelierKernel::open_with_db(&path).unwrap();
    let s = k.attach_domain("sender");
    let w = k.attach_domain("worker");
    w.register_handlers(&["run"]);
    w.drain_frames();
    let id = s.send_one(vec!["worker.run".into()], "Durable", json!({}), json!({}));
    w.drain_frames();
    s.drain_frames();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "owner_process_fixture", "--ignored"])
        .env("ATELIER_OWNER_DB", &path)
        .status()
        .unwrap();
    assert!(result.success());
    assert!(AtelierKernel::open_with_db(&path).is_err());
    assert_eq!(
        k.ledger().get_persisted_record(&id).unwrap().status,
        S::Sent
    );
    w.report_receipt(&id, "worker.run", S::Completed, Some(json!({"kept":true})));
    assert!(
        s.drain_frames()
            .iter()
            .any(|f| matches!(f,F::Receipt(r) if r.status==S::Completed))
    );
    drop(w);
    drop(s);
    drop(k);
    let reopened = AtelierKernel::open_with_db(&path).unwrap();
    assert_eq!(
        reopened.ledger().get_persisted_record(&id).unwrap().data,
        Some(json!({"kept":true}))
    );
    drop(reopened);
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "owner_process_fixture", "--ignored"])
        .env("ATELIER_OWNER_DB", &path)
        .env("ATELIER_OWNER_CRASH", "1")
        .status()
        .unwrap();
    assert!(result.success());
    assert!(
        AtelierKernel::open_with_db(&path).is_ok(),
        "OS must release guard after abrupt exit"
    );
}
#[cfg(unix)]
#[test]
fn database_symlink_uses_same_owner_and_log_spool_path() {
    let base = dir();
    let path = base.join("kernel.db");
    let alias = base.join("alias.db");
    let k = AtelierKernel::open_with_db(&path).unwrap();
    std::os::unix::fs::symlink(&path, &alias).unwrap();
    assert!(AtelierKernel::open_with_db(&alias).is_err());
    drop(k);
    let k = AtelierKernel::open_with_db(&alias).unwrap();
    assert!(AtelierKernel::open_with_db(&path).is_err());
    drop(k);
    assert!(!base.join("alias.db.log-spool.sqlite").exists());
}
fn terminal(frames: Vec<F>) -> atelier_kernel::MessageReceipt {
    frames
        .into_iter()
        .find_map(|f| match f {
            F::Receipt(r) if r.status.is_terminal() => Some(r),
            _ => None,
        })
        .unwrap()
}
#[test]
fn corrupt_storage_fails_without_patch_overwrite_but_explicit_replace_remains_valid() {
    let path = dir().join("kernel.db");
    let k = AtelierKernel::open_with_db(&path).unwrap();
    let s = k.attach_domain("source");
    s.storage_insert(json!({"record_id":"r"}), json!({"kept":1}));
    s.drain_frames();
    let db = rusqlite::Connection::open(&path).unwrap();
    for column in ["params", "data"] {
        db.execute(
            &format!("UPDATE store_source SET {column}='{{broken' WHERE record_id='r'"),
            [],
        )
        .unwrap();
        s.storage_query(json!({"record_id":"r"}));
        assert_eq!(terminal(s.drain_frames()).status, S::ExecutionFailed);
        if column == "data" {
            s.storage_update(json!({"record_id":"r","patch":true}), json!({"new":1}));
            assert_eq!(terminal(s.drain_frames()).status, S::ExecutionFailed);
            let raw: String = db
                .query_row(
                    "SELECT data FROM store_source WHERE record_id='r'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(raw, "{broken");
        }
        db.execute(
            &format!("UPDATE store_source SET {column}='null' WHERE record_id='r'"),
            [],
        )
        .unwrap();
    }
    s.storage_query(json!({"record_id":"r"}));
    let r = terminal(s.drain_frames());
    assert_eq!(r.status, S::Completed);
    assert_eq!(r.data.unwrap()["items"][0]["data"], serde_json::Value::Null);
    db.execute(
        "UPDATE store_source SET data='{broken' WHERE record_id='r'",
        [],
    )
    .unwrap();
    s.storage_update(
        json!({"record_id":"r","patch":false}),
        json!({"replacement":true}),
    );
    assert_eq!(terminal(s.drain_frames()).status, S::Completed);
}
#[test]
fn revoked_source_cannot_submit_register_or_mutate_storage() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let old = k.attach_domain("source");
    let w = k.attach_domain("worker");
    w.register_handlers(&["run"]);
    w.drain_frames();
    k.router().mark_domain_faulted("source", "revoked");
    assert!(
        !k.router()
            .is_valid_instance_for_domain("source", old.instance_id())
    );
    assert!(
        old.send(vec!["worker.run".into()], "Durable", json!({}), json!({}))
            .is_empty()
    );
    old.register_handlers(&["ghost"]);
    old.storage_insert(json!({"record_id":"bad"}), json!({}));
    assert!(w.drain_frames().is_empty());
    assert!(!k.router().active_routes().contains(&"source.ghost".into()));
    let replacement = k.attach_domain("source");
    replacement.register_handlers(&["valid"]);
    assert!(k.router().active_routes().contains(&"source.valid".into()));
}
#[test]
fn user_failure_shaped_payloads_do_not_control_terminal_retention() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let s = k.attach_domain("source");
    let w = k.attach_domain("worker");
    w.register_handlers(&["run"]);
    w.drain_frames();
    let baseline = k.ledger().retention_counts();
    for status in [S::Completed, S::ExecutionFailed] {
        for _ in 0..50 {
            let id = s.send_one(vec!["worker.run".into()], "Durable", json!({}), json!({}));
            w.drain_frames();
            s.drain_frames();
            let data = json!({"persisted":false,"error":"kernel_persistence_failed","business":"unchanged"});
            w.report_receipt(&id, "worker.run", status, Some(data.clone()));
            assert_eq!(terminal(s.drain_frames()).data, Some(data.clone()));
            assert_eq!(
                k.ledger().get_persisted_record(&id).unwrap().data,
                Some(data)
            );
        }
    }
    // A failure-shaped business result exceeding the RAM receipt window must still spool.
    let id = s.send_one(vec!["worker.run".into()], "Durable", json!({}), json!({}));
    w.drain_frames();
    s.drain_frames();
    let data = json!({"persisted":false,"error":"kernel_persistence_failed","body":"x".repeat(2*1024*1024+1024)});
    w.report_receipt(&id, "worker.run", S::ExecutionFailed, Some(data.clone()));
    assert_eq!(terminal(s.drain_frames()).data, Some(data));
    let after = k.ledger().retention_counts();
    assert_eq!(after["child_links"], baseline["child_links"]);
    assert_eq!(after["submissions"], baseline["submissions"]);
}
#[test]
fn dropped_host_cancels_long_update_and_closes_retained_supervisor() {
    let base = dir();
    let marker = base.join("candidate");
    let k = AtelierKernel::new_in_memory().unwrap();
    k.spawn_process_domain(spec("worker", "import time; time.sleep(30)"))
        .unwrap();
    let source = k.attach_domain("controller");
    let candidate = spec(
        "worker",
        &format!(
            "import pathlib,time; pathlib.Path({:?}).write_text('started'); time.sleep(30)",
            marker.to_string_lossy()
        ),
    );
    source.send_one(
        vec!["kernel.supervisor".into()],
        "UpdateDomain",
        json!({"domain":"worker","spec":candidate,"timeout_ms":60000}),
        json!({}),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while !marker.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(marker.exists());
    let weak = Arc::downgrade(k.router());
    let supervisor = k.supervisor().clone();
    drop(source);
    let start = Instant::now();
    drop(k);
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "shutdown waited for the update timeout"
    );
    assert!(supervisor.get_pid("worker").is_none());
    assert!(
        supervisor
            .spawn_process_domain(spec("late", "pass"))
            .is_err()
    );
    drop(supervisor);
    assert!(
        weak.upgrade().is_none(),
        "background control retained the dropped host"
    );
}
#[test]
fn invalid_dynamic_candidate_is_rejected_before_touching_old_process() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let pid = k
        .spawn_process_domain(spec("worker", "import time; time.sleep(30)"))
        .unwrap();
    for bad in ["bad.domain", "", " padded "] {
        assert!(k.spawn_process_domain(spec(bad, "pass")).is_err());
        assert!(k.supervisor().get_spec(bad).is_none());
    }
    let mut bad = spec("worker", "pass");
    bad.command.clear();
    assert!(k.spawn_process_domain(bad.clone()).is_err());
    assert!(
        k.supervisor()
            .update_process_domain("worker", Some(bad), None, Duration::from_secs(1))
            .is_err()
    );
    assert_eq!(k.supervisor().get_pid("worker"), Some(pid));
    let mut bad = spec("worker", "pass");
    bad.working_dir = Some(dir().join("missing").to_string_lossy().to_string());
    assert!(k.spawn_process_domain(bad).is_err());
    assert_eq!(k.supervisor().get_pid("worker"), Some(pid));
}

#[test]
fn revoke_serializes_with_existing_source_frame_without_resurrecting_binding() {
    use std::sync::mpsc;
    let path = dir().join("kernel.db");
    let k = AtelierKernel::open_with_db(&path).unwrap();
    let source = Arc::new(k.attach_domain("source"));
    source.register_handlers(&["run"]);
    source.drain_frames();
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("BEGIN IMMEDIATE;").unwrap();
    let writer = source.clone();
    let sending = std::thread::spawn(move || {
        writer.storage_insert(json!({"record_id":"inflight"}), json!({"kept":true}))
    });
    loop {
        match source.recv_frame_timeout(Duration::from_secs(3)).unwrap() {
            F::Receipt(r) if r.status == S::Started => break,
            _ => {}
        }
    }
    let router = k.router().clone();
    let (entered, started) = mpsc::channel();
    let (done, finished) = mpsc::channel();
    let revoking = std::thread::spawn(move || {
        entered.send(()).unwrap();
        let result = router.mark_domain_faulted("source", "concurrent revoke");
        done.send(result).unwrap();
    });
    started.recv().unwrap();
    let early = finished.recv_timeout(Duration::from_millis(50));
    // Release before asserting, so the old implementation also exits cleanly on failure.
    db.execute_batch("ROLLBACK;").unwrap();
    sending.join().unwrap();
    revoking.join().unwrap();
    assert!(
        early.is_err(),
        "binding was revoked while its admitted frame was still executing"
    );
    assert!(finished.recv().unwrap());
    source.register_handlers(&["ghost"]);
    assert!(!k.router().active_routes().contains(&"source.ghost".into()));
}

#[test]
fn hard_linked_database_is_rejected_before_recovery_even_without_live_owner() {
    let base = dir();
    let path = base.join("kernel.db");
    let alias = base.join("alias.db");
    let k = AtelierKernel::open_with_db(&path).unwrap();
    std::fs::hard_link(&path, &alias).unwrap();
    assert!(AtelierKernel::open_with_db(&alias).is_err());
    drop(k);
    assert!(AtelierKernel::open_with_db(&path).is_err());
    std::fs::remove_file(alias).unwrap();
    assert!(AtelierKernel::open_with_db(&path).is_ok());
}
