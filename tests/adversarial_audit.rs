//! Adversarial regression probes. Failing assertions represent required contracts.
use atelier_kernel::{ContractStatus, KernelLedger, KernelObserver};
use serde_json::json;
use std::time::Instant;

#[test]
fn durable_ingress_must_not_claim_success_after_sqlite_write_failure() {
    let root=std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tmp/adversarial-audit");
    std::fs::create_dir_all(&root).unwrap();
    let path=root.join(format!("locked-{}.db",uuid::Uuid::new_v4()));
    let ledger=KernelLedger::open_with_db(Some(&path)).unwrap();
    let blocker=rusqlite::Connection::open(&path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let started=Instant::now();
    let (record,created)=ledger.record_initial_target_state("audit-durable",1,"sender","worker.run","Durable",ContractStatus::Sent,Some(json!({"work":1})));
    blocker.execute_batch("ROLLBACK").unwrap();
    eprintln!("created={created} status={:?} elapsed={:?} persisted={}",record.status,started.elapsed(),ledger.persisted_count());
    assert!(!created || ledger.get_persisted_record("audit-durable").is_some(),
        "Durable ingress claims successful creation although SQLite rejected the write");
}

#[test]
fn idle_observer_consumer_must_not_retain_every_event_outside_history_limit() {
    let observer=KernelObserver::new(64);
    let rx=observer.subscribe();
    for i in 0..20_000 {
        observer.emit("sender","worker.run",&i.to_string(),"logical",i,"Ephemeral",ContractStatus::Completed,Some(json!({"payload":"x".repeat(1024)})));
    }
    assert_eq!(observer.snapshot().len(),64);
    let queued=rx.try_iter().count();
    eprintln!("history_limit=64 slow_subscriber_queued={queued} payload_bytes_at_least={}",queued*1024);
    assert!(queued<=64,"Slow observer subscriber has an unbounded backlog despite bounded history");
}

#[cfg(unix)]
#[test]
fn starting_own_domain_must_return_or_reject_without_recursive_gate_deadlock() {
    let kernel=std::sync::Arc::new(atelier_kernel::AtelierKernel::new_in_memory().unwrap());
    let client=kernel.attach_domain("selfprobe");
    let original_instance=client.instance_id();
    let witness=std::sync::Arc::clone(&kernel);
    let (done_tx,done_rx)=std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _keep_alive=kernel;
        client.send(vec!["kernel.supervisor".into()],"StartDomain",json!({
            "domain":"selfprobe","spec":{"domain_id":"selfprobe","command":"/bin/sh",
            "args":["-c","exit 0"],"auto_restart":false}}),json!({}));
        let _=done_tx.send(());
    });
    assert!(done_rx.recv_timeout(std::time::Duration::from_secs(3)).is_ok(),
        "Own-domain lifecycle request deadlocked while re-entering its source-domain gate");
    let deadline=Instant::now()+std::time::Duration::from_secs(3);
    while witness.router().domain_instance_id("selfprobe")==Some(original_instance) && Instant::now()<deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_ne!(witness.router().domain_instance_id("selfprobe"),Some(original_instance),"request returned but replacement remained deadlocked");
}

#[test]
fn corrupt_durable_rows_must_not_be_silently_reinterpreted_on_restore() {
    let root=std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tmp/adversarial-audit");
    std::fs::create_dir_all(&root).unwrap();
    let mut accepted=Vec::new();
    for (field,value,status) in [("status","not-a-real-state",ContractStatus::Sent),("data","{broken-json",ContractStatus::Sent),
        ("status","not-a-real-state",ContractStatus::Completed),("data","{broken-json",ContractStatus::Completed)] {
        let path=root.join(format!("corrupt-{}.db",uuid::Uuid::new_v4()));
        let ledger=KernelLedger::open_with_db(Some(&path)).unwrap();
        ledger.record_initial_target_state("audit-corrupt",1,"sender","worker.run","Durable",status,Some(json!({"work":1})));
        drop(ledger);
        let conn=rusqlite::Connection::open(&path).unwrap();
        conn.execute(&format!("UPDATE kernel_ledger SET {field}=?1"),[value]).unwrap();
        drop(conn);
        if let Ok(restored)=KernelLedger::open_with_db(Some(&path)) {
            eprintln!("corrupt {field} accepted: {:?}",restored.get_record("audit-corrupt","worker.run"));
            accepted.push(field);
        }
    }
    assert!(accepted.is_empty(),"Corrupt persisted fields silently accepted: {accepted:?}");
}

#[test]
fn durable_storage_faults_are_standard_failures_without_delivering_or_replaying_success() {
    use atelier_kernel::{AtelierKernel,KernelToDomainFrame as Frame};
    let root=std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tmp/adversarial-audit");
    std::fs::create_dir_all(&root).unwrap();
    let path=root.join(format!("routed-lock-{}.db",uuid::Uuid::new_v4()));
    let kernel=AtelierKernel::open_with_db(&path).unwrap();
    let sender=kernel.attach_domain("sender");
    let worker=kernel.attach_domain("worker");
    worker.register_handlers(&["run"]);worker.drain_frames();
    let blocker=rusqlite::Connection::open(&path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let ids=sender.send_with_seq(100,vec!["worker.run".into()],"Durable",json!({}),json!({"work":1}));
    blocker.execute_batch("ROLLBACK").unwrap();
    assert!(worker.drain_frames().is_empty(),"unpersisted work delivered");
    let frames=sender.drain_frames();
    assert!(frames.iter().any(|f|matches!(f,Frame::Receipt(r) if r.message_id==ids[0] && r.status==ContractStatus::ExecutionFailed && r.data.as_ref().unwrap()["error"]=="kernel_persistence_failed")));
    assert!(kernel.ledger().get_persisted_record(&ids[0]).is_none());
    let again=sender.send_with_seq(100,vec!["worker.run".into()],"Durable",json!({}),json!({"work":1}));
    assert_eq!(again,ids);assert!(worker.drain_frames().is_empty());
    assert!(sender.drain_frames().iter().all(|f|matches!(f,Frame::Receipt(r) if r.status==ContractStatus::ExecutionFailed)));
    let ids=sender.send_with_seq(101,vec!["worker.run".into()],"Durable",json!({}),json!({"work":2}));
    assert!(worker.drain_frames().iter().any(|f|matches!(f,Frame::Deliver(_))));
    sender.drain_frames();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    worker.report_receipt(&ids[0],"worker.run",ContractStatus::Completed,Some(json!({"ok":true})));
    blocker.execute_batch("ROLLBACK").unwrap();
    assert!(sender.drain_frames().iter().any(|f|matches!(f,Frame::Receipt(r) if r.status==ContractStatus::ExecutionFailed)));
    assert_eq!(kernel.ledger().get_persisted_record(&ids[0]).unwrap().status,ContractStatus::Sent);
    assert_eq!(kernel.ledger().get_record(&ids[0],"worker.run").unwrap().status,ContractStatus::ExecutionFailed);
    worker.report_receipt(&ids[0],"worker.run",ContractStatus::Completed,Some(json!({"ok":true})));
    assert!(sender.drain_frames().is_empty(),"late completion overwrote infrastructure failure");
}

#[test]
fn slow_observer_pipe_is_bounded_while_business_deliveries_continue() {
    use atelier_kernel::{AtelierKernel,KernelToDomainFrame as Frame};
    let kernel=AtelierKernel::new_in_memory().unwrap();
    let slow=kernel.attach_domain("slow");
    slow.send(vec!["kernel.observer".into()],"SubscribeObserver",json!({}),json!({}));
    slow.drain_frames();
    let sender=kernel.attach_domain("sender");
    let worker=kernel.attach_domain("worker");
    worker.register_handlers(&["run"]);worker.drain_frames();
    let start=Instant::now();
    for i in 0..20_000 {
        kernel.observer().emit("sender","worker.run",&i.to_string(),"logical",i,"Ephemeral",ContractStatus::Completed,Some(json!({"payload":"x".repeat(1024)})));
    }
    sender.send(vec!["worker.run".into()],"Ephemeral",json!({}),json!({"still":"running"}));
    assert!(worker.drain_frames().iter().any(|f|matches!(f,Frame::Deliver(msg) if msg.data["still"]=="running")));
    assert!(start.elapsed()<std::time::Duration::from_secs(10));
    std::thread::sleep(std::time::Duration::from_millis(100));
    let frames=slow.drain_frames();
    let bytes:usize=frames.iter().filter(|frame|matches!(frame,Frame::Deliver(msg) if msg.message_type=="ObservationEvent"))
        .map(|frame|serde_json::to_vec(frame).unwrap().len()).sum();
    assert!(bytes>0 && bytes<=262_144,"unbounded downstream observer queue: {bytes}");
    kernel.observer().emit("sender","worker.run","after-drain","logical",30_000,"Ephemeral",ContractStatus::Completed,None);
    let deadline=Instant::now()+std::time::Duration::from_secs(2);
    let mut recovered=false;
    while Instant::now()<deadline && !recovered {
        recovered=slow.drain_frames().iter().any(|f|matches!(f,Frame::Deliver(msg) if msg.data["message_id"]=="after-drain"));
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(recovered,"observer did not recover after draining");
}

#[test]
fn oversized_observation_can_be_lost_without_truncating_business_result() {
    use atelier_kernel::{AtelierKernel,KernelToDomainFrame as Frame};
    let kernel=AtelierKernel::new_in_memory().unwrap();
    let sender=kernel.attach_domain("sender");
    let worker=kernel.attach_domain("worker");
    worker.register_handlers(&["run"]);worker.drain_frames();
    let ids=sender.send(vec!["worker.run".into()],"Ephemeral",json!({}),json!({}));
    worker.drain_frames();sender.drain_frames();
    let payload=json!({"text":"x".repeat(300_000)});
    worker.report_receipt(&ids[0],"worker.run",ContractStatus::Completed,Some(payload.clone()));
    assert!(sender.drain_frames().iter().any(|f|matches!(f,Frame::Receipt(r) if r.status==ContractStatus::Completed && r.data.as_ref()==Some(&payload))));
    assert!(!kernel.observer().events_for_message(&ids[0]).iter().any(|e|e.status==ContractStatus::Completed));
}

#[cfg(target_os = "linux")]
#[test]
fn real_process_not_reading_observer_pipe_does_not_block_other_domains() {
    use atelier_kernel::{AtelierKernel,DomainProcessSpec,KernelToDomainFrame as Frame};
    let kernel=AtelierKernel::new_in_memory().unwrap();
    let frame=serde_json::to_string(&json!({"frame_type":"send","seq":1,
        "targets":["kernel.observer"],"message_type":"SubscribeObserver","params":{},"data":{}})).unwrap();
    let script=format!("printf '%s\\n' '{}' > \"/proc/self/fd/$ATELIER_IPC_WRITE_FD\"; exec /bin/sleep 15",frame);
    let spec:DomainProcessSpec=serde_json::from_value(json!({"domain_id":"slow_os",
        "command":"/bin/sh","args":["-c",script],"auto_restart":false})).unwrap();
    kernel.supervisor().spawn_process_domain(spec).unwrap();
    let deadline=Instant::now()+std::time::Duration::from_secs(3);
    while kernel.router().observer_subscribers_count()==0 && Instant::now()<deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let subscribed=kernel.router().observer_subscribers_count()==1;
    let sender=kernel.attach_domain("sender");
    let worker=kernel.attach_domain("worker");
    worker.register_handlers(&["run"]);worker.drain_frames();
    let start=Instant::now();
    for i in 0..20_000 {
        kernel.observer().emit("sender","worker.run",&i.to_string(),"logical",i,"Ephemeral",ContractStatus::Completed,Some(json!({"payload":"x".repeat(1024)})));
    }
    sender.send(vec!["worker.run".into()],"Ephemeral",json!({}),json!({"proof":true}));
    let delivered=worker.drain_frames().iter().any(|f|matches!(f,Frame::Deliver(msg) if msg.data["proof"]==true));
    let elapsed=start.elapsed();
    let stopped=kernel.supervisor().stop_process_domain("slow_os");
    assert!(subscribed,"real process did not establish its subscription");
    assert!(delivered && elapsed<std::time::Duration::from_secs(10),"unrelated delivery stalled");
    assert!(stopped);
    assert_eq!(kernel.router().observer_subscribers_count(),0,"subscription not reclaimed on exit");
}
