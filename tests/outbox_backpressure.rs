use atelier_kernel::{
    AtelierKernel, ContractStatus, DomainProcessSpec, DomainStatus, KernelToDomainFrame as Frame,
};
use serde_json::json;
use std::time::{Duration, Instant};

#[test]
fn invalid_target_is_terminal_and_never_becomes_pending_work() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let sender = k.attach_domain("sender");
    let ids = sender.send(
        vec!["absent.run".into()],
        "Durable",
        json!({}),
        json!({"must_not_wait":true}),
    );
    assert!(sender.drain_frames().iter().any(|f|matches!(f,Frame::Receipt(r) if r.message_id==ids[0] && r.status==ContractStatus::TargetInvalid)));
    let new = k.attach_domain("absent");
    new.register_handlers(&["run"]);
    assert!(
        !new.drain_frames()
            .iter()
            .any(|f| matches!(f, Frame::Deliver(_)))
    );
}

#[cfg(target_os = "linux")]
fn slow_domain(k: &AtelierKernel, id: &str) {
    let register = serde_json::to_string(
        &json!({"frame_type":"send","seq":1,"targets":["kernel.registry"],
        "message_type":"RegisterHandlers","params":{"handlers":["run"]},"data":{}}),
    )
    .unwrap();
    let script = format!(
        "printf '%s\\n' '{}' > \"/proc/self/fd/$ATELIER_IPC_WRITE_FD\"; exec /bin/sleep 60",
        register
    );
    let spec: DomainProcessSpec = serde_json::from_value(
        json!({"domain_id":id,"command":"/bin/sh","args":["-c",script],"auto_restart":false}),
    )
    .unwrap();
    k.supervisor().spawn_process_domain(spec).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while k.router().domain_status(id) != Some(DomainStatus::Active) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(k.router().domain_status(id), Some(DomainStatus::Active));
}

#[cfg(target_os = "linux")]
#[test]
fn unread_real_process_spools_durable_work_without_blocking_other_domains() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tmp/outbox-acceptance");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!("{}.db", uuid::Uuid::new_v4()));
    let k = AtelierKernel::open_with_db(&path).unwrap();
    slow_domain(&k, "slow");
    struct Stop<'a>(&'a AtelierKernel);
    impl Drop for Stop<'_> {
        fn drop(&mut self) {
            self.0.supervisor().stop_process_domain("slow");
        }
    }
    let _stop = Stop(&k);
    let sender = k.attach_domain("sender");
    let fast = k.attach_domain("fast");
    fast.register_handlers(&["run"]);
    fast.drain_frames();
    let before = std::fs::read_to_string("/proc/self/status").unwrap();
    let start = Instant::now();
    let mut ids = Vec::new();
    for i in 0..512 {
        ids.extend(sender.send(
            vec!["slow.run".into()],
            "Durable",
            json!({"i":i}),
            json!({"payload":"x".repeat(65536)}),
        ));
        sender.drain_frames();
    }
    let db = rusqlite::Connection::open(&path).unwrap();
    let bytes:i64=db.query_row("SELECT coalesce(sum(length(frame)),0) FROM kernel_outbox WHERE delivery_id IS NOT NULL",[],|r|r.get(0)).unwrap();
    assert!(bytes > 30 * 1024 * 1024, "payload not spooled: {bytes}");
    sender.send(
        vec!["fast.run".into()],
        "Ephemeral",
        json!({}),
        json!({"responsive":true}),
    );
    assert!(
        fast.drain_frames()
            .iter()
            .any(|f| matches!(f,Frame::Deliver(m) if m.data["responsive"]==true))
    );
    let after = std::fs::read_to_string("/proc/self/status").unwrap();
    eprintln!(
        "spooled_bytes={bytes} elapsed={:?} RSS before={:?} after={:?}",
        start.elapsed(),
        before.lines().find(|l| l.starts_with("VmRSS")),
        after.lines().find(|l| l.starts_with("VmRSS"))
    );
    assert!(k.supervisor().stop_process_domain("slow"));
    for id in ids {
        assert_eq!(
            k.ledger().get_persisted_record(&id).unwrap().status,
            ContractStatus::ExecutionFailed
        );
    }
    let pending: i64 = db
        .query_row(
            "SELECT count(*) FROM kernel_outbox WHERE delivery_id IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(pending, 0);
    sender.send(vec!["slow.run".into()], "Durable", json!({}), json!({}));
    assert!(
        sender
            .drain_frames()
            .iter()
            .any(|f| matches!(f,Frame::Receipt(r) if r.status==ContractStatus::TargetInvalid))
    );
}

#[test]
fn nonpersistent_overload_has_explicit_failure_and_no_automatic_disk_upgrade() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let sender = k.attach_domain("sender");
    let slow = k.attach_domain("slow");
    slow.register_handlers(&["run"]);
    slow.drain_frames();
    let mut ids = Vec::new();
    for _ in 0..100 {
        ids.extend(sender.send(
            vec!["slow.run".into()],
            "Volatile",
            json!({}),
            json!({"payload":"x".repeat(65536)}),
        ));
        sender.drain_frames();
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while k.router().domain_status("slow") == Some(DomainStatus::Active)
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        k.router().domain_status("slow"),
        Some(DomainStatus::Faulted)
    );
    for id in ids {
        let r = k.ledger().get_record(&id, "slow.run").unwrap();
        assert!(r.status.is_terminal());
        assert!(k.ledger().get_persisted_record(&id).is_none());
    }
}

#[cfg(target_os = "linux")]
#[test]
fn real_nonpersistent_overload_reclaims_exact_process_generation() {
    let k = AtelierKernel::new_in_memory().unwrap();
    slow_domain(&k, "slow_budget");
    struct Stop<'a>(&'a AtelierKernel);
    impl Drop for Stop<'_> {
        fn drop(&mut self) {
            self.0.supervisor().stop_process_domain("slow_budget");
        }
    }
    let _stop = Stop(&k);
    let pid = k.supervisor().get_pid("slow_budget").unwrap();
    let sender = k.attach_domain("sender");
    for _ in 0..128 {
        sender.send(
            vec!["slow_budget.run".into()],
            "Volatile",
            json!({}),
            json!({"payload":"x".repeat(65536)}),
        );
        sender.drain_frames();
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while k.supervisor().get_pid("slow_budget").is_some() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(k.supervisor().get_pid("slow_budget").is_none());
    while std::path::Path::new(&format!("/proc/{pid}")).exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    assert_eq!(
        k.router().domain_status("slow_budget"),
        Some(DomainStatus::Faulted)
    );
}

#[test]
#[ignore = "subprocess fixture invoked explicitly by restart test"]
fn abrupt_exit_fixture() {
    let path = std::env::var("ATELIER_OUTBOX_CRASH_FIXTURE").unwrap();
    let k = AtelierKernel::open_with_db(&path).unwrap();
    let worker = k.attach_domain("worker");
    worker.register_handlers(&["run"]);
    worker.drain_frames();
    let sender = k.attach_domain("sender");
    let ids = sender.send(
        vec!["worker.run".into()],
        "Durable",
        json!({}),
        json!({"payload":"must not silently replay"}),
    );
    std::fs::write(format!("{path}.id"), &ids[0]).unwrap();
    // Intentionally skip destructors, as when a host disappears after committing the send.
    std::process::exit(0);
}

#[test]
fn actual_host_restart_preserves_failure_record_without_replaying_old_generation() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tmp/outbox-acceptance");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!("restart-{}.db", uuid::Uuid::new_v4()));
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "abrupt_exit_fixture", "--ignored", "--nocapture"])
        .env("ATELIER_OUTBOX_CRASH_FIXTURE", &path)
        .status()
        .unwrap();
    assert!(status.success());
    let id = std::fs::read_to_string(format!("{}.id", path.display())).unwrap();
    let k = AtelierKernel::open_with_db(&path).unwrap();
    let record = k.ledger().get_persisted_record(&id).unwrap();
    assert_eq!(record.status, ContractStatus::ExecutionFailed);
    assert_eq!(
        record.data.unwrap()["error"],
        "kernel_restarted_with_inflight_delivery"
    );
    let worker = k.attach_domain("worker");
    worker.register_handlers(&["run"]);
    assert!(
        !worker
            .drain_frames()
            .iter()
            .any(|f| matches!(f, Frame::Deliver(_)))
    );
}
