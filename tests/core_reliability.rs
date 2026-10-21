//! Whole-core reliability acceptance. These regressions reproduce the old implementation's
//! crash windows and resource ownership defects, using the real ledger, queues and processes.
use atelier_kernel::{
    AtelierKernel, ContractStatus, DomainProcessSpec, DomainStatus, KernelToDomainFrame as Frame,
};
use serde_json::json;
use std::{
    path::PathBuf,
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};
fn database(label: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tmp/core-reliability");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{label}-{}.db", uuid::Uuid::new_v4()))
}
#[test]
#[ignore = "isolated child crash fixture invoked by recovery test"]
fn crash_after_physical_write_fixture() {
    let path = std::env::var("ATELIER_CORE_CRASH_DB").unwrap();
    let k = AtelierKernel::open_with_db(&path).unwrap();
    k.logger().set_console_enabled(false);
    let sender = k.attach_domain("sender");
    let worker = k.attach_domain("worker");
    worker.register_handlers(&["run"]);
    worker.drain_frames();
    let mut ids = Vec::new();
    for status in [
        ContractStatus::Sent,
        ContractStatus::Started,
        ContractStatus::Completed,
    ] {
        let id = sender.send_one(
            vec!["worker.run".into()],
            "Durable",
            json!({"required":"input"}),
            json!({"private_payload":"accepted"}),
        );
        assert!(
            worker
                .drain_frames()
                .iter()
                .any(|f| matches!(f, Frame::Deliver(_)))
        );
        if status != ContractStatus::Sent {
            worker.report_receipt(
                &id,
                "worker.run",
                status,
                Some(json!({"result":"must survive"})),
            );
        }
        ids.push(id);
    }
    let db = rusqlite::Connection::open(&path).unwrap();
    let retained: i64 = db
        .query_row(
            "SELECT count(*) FROM kernel_outbox WHERE delivery_id IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        retained, 2,
        "physical acknowledgement must not delete unconfirmed business envelopes"
    );
    std::fs::write(format!("{path}.ids"), serde_json::to_vec(&ids).unwrap()).unwrap();
    std::process::exit(0);
}
#[test]
fn process_recovery_closes_every_old_nonterminal_without_replay_or_losing_completed_result() {
    let path = database("written");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "crash_after_physical_write_fixture",
            "--ignored",
            "--nocapture",
        ])
        .env("ATELIER_CORE_CRASH_DB", &path)
        .status()
        .unwrap();
    assert!(status.success());
    let ids: Vec<String> =
        serde_json::from_slice(&std::fs::read(format!("{}.ids", path.display())).unwrap()).unwrap();
    let k = AtelierKernel::open_with_db(&path).unwrap();
    for id in &ids[..2] {
        let r = k.ledger().get_persisted_record(id).unwrap();
        assert_eq!(r.status, ContractStatus::ExecutionFailed);
        assert_eq!(r.data.unwrap()["execution_outcome"], "unknown");
    }
    let completed = k.ledger().get_persisted_record(&ids[2]).unwrap();
    assert_eq!(completed.status, ContractStatus::Completed);
    assert_eq!(completed.data.unwrap(), json!({"result":"must survive"}));
    let worker = k.attach_domain("worker");
    worker.register_handlers(&["run"]);
    assert!(
        !worker
            .drain_frames()
            .iter()
            .any(|f| matches!(f, Frame::Deliver(_)))
    );
    assert_eq!(k.ledger().retention_counts()["durable_records"], 0);
}
#[test]
fn outbox_and_ledger_admission_are_one_transaction_and_failure_replay_does_not_execute() {
    let path = database("atomic");
    let k = AtelierKernel::open_with_db(&path).unwrap();
    let sender = k.attach_domain("sender");
    let worker = k.attach_domain("worker");
    worker.register_handlers(&["run"]);
    worker.drain_frames();
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TRIGGER reject_frame BEFORE INSERT ON kernel_outbox BEGIN SELECT RAISE(FAIL,'injected envelope write failure'); END;").unwrap();
    let ids = sender.send_with_seq(
        50,
        vec!["worker.run".into()],
        "Durable",
        json!({}),
        json!({"work":1}),
    );
    assert!(worker.drain_frames().is_empty());
    assert!(sender.drain_frames().iter().any(|f|matches!(f,Frame::Receipt(r) if r.message_id==ids[0] && r.status==ContractStatus::ExecutionFailed && r.data.as_ref().unwrap()["persisted"]==false)));
    let persisted: i64 = db
        .query_row(
            "SELECT count(*) FROM kernel_ledger WHERE message_id=?1",
            [&ids[0]],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(persisted, 0);
    db.execute_batch("DROP TRIGGER reject_frame;").unwrap();
    assert_eq!(
        sender.send_with_seq(
            50,
            vec!["worker.run".into()],
            "Durable",
            json!({}),
            json!({"work":1})
        ),
        ids
    );
    assert!(worker.drain_frames().is_empty());
    assert!(
        sender
            .drain_frames()
            .iter()
            .all(|f| matches!(f,Frame::Receipt(r) if r.status==ContractStatus::ExecutionFailed))
    );
}
#[test]
fn locked_durable_writer_does_not_hold_unrelated_runtime_query_or_routes() {
    let path = database("lock");
    let k = Arc::new(AtelierKernel::open_with_db(&path).unwrap());
    let sender = k.attach_domain("sender");
    let worker = k.attach_domain("worker");
    worker.register_handlers(&["run"]);
    worker.drain_frames();
    let query = k.attach_domain("query");
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("BEGIN IMMEDIATE;").unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let job = std::thread::spawn(move || {
        ready_tx.send(()).unwrap();
        sender.send(vec!["worker.run".into()], "Durable", json!({}), json!({}));
    });
    ready_rx.recv().unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let at = Instant::now();
    let ids = query.send(
        vec!["kernel.runtime".into()],
        "QueryRuntime",
        json!({}),
        json!({}),
    );
    let elapsed = at.elapsed();
    let completed=query.drain_frames().iter().any(|f|matches!(f,Frame::Receipt(r) if r.message_id==ids[0] && r.status==ContractStatus::Completed));
    db.execute_batch("ROLLBACK;").unwrap();
    job.join().unwrap();
    eprintln!("unrelated QueryRuntime: {elapsed:?}");
    assert!(completed);
    assert!(
        elapsed < Duration::from_millis(500),
        "global state blocked behind unrelated SQLite writer: {elapsed:?}"
    );
}
#[test]
fn completed_durable_history_keeps_results_on_disk_without_live_identity_growth() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let sender = k.attach_domain("sender");
    let worker = k.attach_domain("worker");
    worker.register_handlers(&["run"]);
    worker.drain_frames();
    let before = k.ledger().retention_counts();
    let payload = json!({"result":"x".repeat(65536)});
    let mut last = String::new();
    for _ in 0..256 {
        last = sender.send_one(vec!["worker.run".into()], "Durable", json!({}), json!({}));
        worker.drain_frames();
        worker.report_receipt(
            &last,
            "worker.run",
            ContractStatus::Completed,
            Some(payload.clone()),
        );
        sender.drain_frames();
    }
    let after = k.ledger().retention_counts();
    assert_eq!(after["durable_records"], 0);
    assert_eq!(after["child_links"], before["child_links"]);
    assert_eq!(after["submissions"], before["submissions"]);
    assert_eq!(after["resident_records"], before["resident_records"]);
    assert_eq!(
        k.ledger().get_persisted_record(&last).unwrap().data,
        Some(payload)
    );
}
#[test]
fn late_target_result_is_not_delivered_to_replacement_source_process() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let old = k.attach_domain("sender");
    let worker = k.attach_domain("worker");
    worker.register_handlers(&["run"]);
    worker.drain_frames();
    let id = old.send_one(vec!["worker.run".into()], "Durable", json!({}), json!({}));
    worker.drain_frames();
    old.drain_frames();
    let replacement = k.attach_domain("sender");
    worker.report_receipt(
        &id,
        "worker.run",
        ContractStatus::Completed,
        Some(json!({"secret":"old process only"})),
    );
    assert!(replacement.drain_frames().is_empty());
    assert_eq!(
        k.ledger().get_persisted_record(&id).unwrap().status,
        ContractStatus::Completed
    );
}
#[cfg(unix)]
#[test]
fn eof_with_live_process_is_reaped_without_waiting_for_natural_exit() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let spec:DomainProcessSpec=serde_json::from_value(json!({"domain_id":"eof_probe","command":"python3","args":["-c","import os,time; os.close(int(os.environ['ATELIER_IPC_WRITE_FD'])); time.sleep(5)"],"auto_restart":false})).unwrap();
    let pid = k.spawn_process_domain(spec).unwrap();
    let until = Instant::now() + Duration::from_secs(2);
    while k.router().domain_status("eof_probe") != Some(DomainStatus::Faulted)
        && Instant::now() < until
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        k.router().domain_status("eof_probe"),
        Some(DomainStatus::Faulted)
    );
    let at = Instant::now();
    k.supervisor().stop_process_domain("eof_probe");
    assert!(at.elapsed() < Duration::from_millis(500));
    assert_eq!(k.supervisor().get_pid("eof_probe"), None);
    assert!(
        !PathBuf::from(format!("/proc/{pid}")).exists(),
        "child still exists after owner shutdown"
    );
}
#[test]
fn dropping_host_releases_monitor_and_router_instead_of_self_retaining() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let weak = Arc::downgrade(k.router());
    drop(k);
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        weak.upgrade().is_none(),
        "supervisor/router ownership cycle survived host release"
    );
}
#[test]
fn corrupt_persisted_transport_is_rejected_before_startup_cleanup() {
    let path = database("badframe");
    {
        let k = AtelierKernel::open_with_db(&path).unwrap();
        drop(k);
    }
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute("INSERT INTO kernel_outbox(channel,delivery_id,priority,frame) VALUES('old','delivery',1,'{broken-json')",[]).unwrap();
    drop(db);
    assert!(AtelierKernel::open_with_db(&path).is_err());
    let db = rusqlite::Connection::open(&path).unwrap();
    let count: i64 = db
        .query_row("SELECT count(*) FROM kernel_outbox", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        count, 1,
        "corrupt persistent payload was silently discarded"
    );
}
#[cfg(unix)]
#[test]
fn overlong_unterminated_ipc_frame_is_bounded_and_terminates_only_its_owner() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let query = k.attach_domain("query");
    let spec:DomainProcessSpec=serde_json::from_value(json!({"domain_id":"oversized","command":"python3","args":["-c","import os,time; fd=int(os.environ['ATELIER_IPC_WRITE_FD']); b=b'x'*8192\ntry:\n for _ in range(2050): os.write(fd,b)\nexcept BrokenPipeError: pass\ntime.sleep(5)"],"auto_restart":false})).unwrap();
    let pid = k.spawn_process_domain(spec).unwrap();
    let deadline = Instant::now() + Duration::from_secs(4);
    while k.supervisor().get_pid("oversized").is_some() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(k.supervisor().get_pid("oversized"), None);
    assert!(!PathBuf::from(format!("/proc/{pid}")).exists());
    let ids = query.send(
        vec!["kernel.runtime".into()],
        "QueryRuntime",
        json!({}),
        json!({}),
    );
    assert!(query.drain_frames().iter().any(|f|matches!(f,Frame::Receipt(r) if r.message_id==ids[0] && r.status==ContractStatus::Completed)));
}
#[cfg(unix)]
#[test]
fn concurrent_stop_and_failed_update_reclaim_both_children_without_global_query_lock() {
    let k = Arc::new(AtelierKernel::new_in_memory().unwrap());
    let old_spec:DomainProcessSpec=serde_json::from_value(json!({"domain_id":"managed","command":"python3","args":["-c","import os,json,time; os.write(int(os.environ['ATELIER_IPC_WRITE_FD']),(json.dumps({'frame_type':'send','seq':1,'targets':['kernel.registry'],'message_type':'RegisterHandlers','params':{'handlers':['run']},'data':{}})+'\\n').encode()); time.sleep(5)"],"auto_restart":false})).unwrap();
    let old_pid = k.spawn_process_domain(old_spec).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while k.router().domain_status("managed") != Some(DomainStatus::Active)
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        k.router().domain_status("managed"),
        Some(DomainStatus::Active)
    );
    let marker = database("candidate-pid");
    let candidate_spec:DomainProcessSpec=serde_json::from_value(json!({"domain_id":"managed","command":"python3","args":["-c","import os,time,sys; open(sys.argv[1],'w').write(str(os.getpid())); time.sleep(5)",marker],"auto_restart":false})).unwrap();
    let updater = Arc::clone(&k);
    let update = std::thread::spawn(move || {
        updater.supervisor().update_process_domain(
            "managed",
            Some(candidate_spec),
            None,
            Duration::from_millis(500),
        )
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    while !marker.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let candidate_pid = std::fs::read_to_string(&marker)
        .unwrap()
        .parse::<u32>()
        .unwrap();
    let stopper = Arc::clone(&k);
    let stop = std::thread::spawn(move || stopper.supervisor().stop_process_domain("managed"));
    let client = k.attach_domain("query");
    let at = Instant::now();
    let ids = client.send(
        vec!["kernel.runtime".into()],
        "QueryRuntime",
        json!({}),
        json!({}),
    );
    assert!(at.elapsed() < Duration::from_millis(300));
    assert!(client.drain_frames().iter().any(|f|matches!(f,Frame::Receipt(r) if r.message_id==ids[0] && r.status==ContractStatus::Completed)));
    assert!(update.join().unwrap().is_err());
    assert!(stop.join().unwrap());
    assert!(k.supervisor().is_stopped("managed"));
    assert_eq!(k.supervisor().get_pid("managed"), None);
    for pid in [old_pid, candidate_pid] {
        assert!(
            !PathBuf::from(format!("/proc/{pid}")).exists(),
            "child {pid} leaked after concurrent lifecycle operations"
        );
    }
}
