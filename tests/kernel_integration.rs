use std::time::Duration;

use atelier_kernel::{
    AtelierKernel, ContractStatus, DomainProcessSpec, DomainStatus, KernelToDomainFrame,
    MSG_DELIVERY_DURABLE, MSG_DELIVERY_EPHEMERAL, MSG_DELIVERY_VOLATILE, MSG_STORAGE_DELETE,
    MSG_STORAGE_QUERY, check_constitution,
};
use serde_json::json;

const PY_IPC_PREAMBLE: &str = r#"
import json, os, sys, time
r_val = int(os.environ["ATELIER_IPC_READ_FD"])
w_val = int(os.environ["ATELIER_IPC_WRITE_FD"])
if os.name == "nt" and os.environ.get("ATELIER_IPC_HANDLE_MODE") in ("win32", "win32_handle"):
    import msvcrt
    r_fd = msvcrt.open_osfhandle(r_val, os.O_RDONLY | getattr(os, "O_BINARY", 0))
    w_fd = msvcrt.open_osfhandle(w_val, getattr(os, "O_BINARY", 0))
else:
    r_fd, w_fd = r_val, w_val
ipc_in = os.fdopen(r_fd, "r", encoding="utf-8", buffering=1, closefd=False)
ipc_out = os.fdopen(w_fd, "w", encoding="utf-8", buffering=1, closefd=False)
"#;

#[test]
fn test_log_origin_additive_migration_and_restart() {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join(format!("origin-migration-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("logs.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE hub_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, level TEXT NOT NULL, layer TEXT DEFAULT '', component TEXT DEFAULT '', message TEXT NOT NULL, phase TEXT DEFAULT ''); INSERT INTO hub_logs(ts,level,layer,component,message) VALUES(1,'INFO','ENGINES','worker','legacy');").unwrap();
    }
    for _ in 0..2 {
        let kernel = AtelierKernel::open_with_db(&path).unwrap();
        kernel.logger().set_console_enabled(false);
        let rows = kernel
            .logger()
            .query_logs(100, None, None, None, None, None)
            .unwrap();
        assert_eq!(
            rows.iter()
                .find(|r| r.message == "legacy")
                .unwrap()
                .source_domain,
            None
        );
    }
}

/// 1. Tests domain handler registration, `<domain>.<handler>` routing, and `TargetInvalid` pre-send interception.
#[test]
fn test_registration_routing_and_target_invalid_interception() {
    let kernel = AtelierKernel::new_in_memory().unwrap();
    let sender = kernel.attach_domain("gateway");
    let worker = kernel.attach_domain("billing");

    // Before registration, `billing` is in `Spawning` state -> sending to `billing.charge` must return `TargetInvalid` without sending.
    let pre_msg_id = sender.send_one(
        vec!["billing.charge".to_string()],
        "ChargeOrder",
        json!({}),
        json!({"amount": 100}),
    );
    assert!(pre_msg_id.starts_with("msg_"));
    assert!(
        worker.drain_frames().is_empty(),
        "Kernel must NOT deliver messages to unregistered/spawning targets"
    );

    let sender_frames = sender.drain_frames();
    assert_eq!(sender_frames.len(), 1);
    match &sender_frames[0] {
        KernelToDomainFrame::Receipt(r) => {
            assert_eq!(r.message_id, pre_msg_id);
            assert_eq!(r.target, "billing.charge");
            assert_eq!(r.status, ContractStatus::TargetInvalid);
        }
        other => panic!("Expected TargetInvalid receipt, got {other:?}"),
    }

    // Now `billing` registers handlers `["charge", "refund"]`
    worker.register_handlers(&["charge", "refund"]);
    let _ = worker.drain_frames();
    assert_eq!(
        kernel.router().domain_status("billing"),
        Some(DomainStatus::Active)
    );

    // Send a multi-target message: one valid (`billing.charge`) and one non-existent (`billing.non_existent`)
    let multi_msg_ids = sender.send(
        vec![
            "billing.charge".to_string(),
            "billing.non_existent".to_string(),
        ],
        "ProcessPayment",
        json!({"currency": "CNY"}),
        json!({"order_id": "O-100"}),
    );

    // Worker should receive exactly 1 delivery (`billing.charge`)
    let worker_frames = worker.drain_frames();
    assert_eq!(worker_frames.len(), 1);
    match &worker_frames[0] {
        KernelToDomainFrame::Deliver(msg) => {
            assert_eq!(msg.message_id, multi_msg_ids[0]);
            assert_ne!(multi_msg_ids[0], multi_msg_ids[1]);
            assert_ne!(msg.logical_id, msg.message_id);
            assert_eq!(msg.source_domain, "gateway");
            assert_eq!(msg.target, "billing.charge");
        }
        other => panic!("Expected Deliver frame, got {other:?}"),
    }

    // Sender should receive `Sent` for `billing.charge` and `TargetInvalid` for `billing.non_existent`
    let receipts: Vec<_> = sender
        .drain_frames()
        .into_iter()
        .map(|f| match f {
            KernelToDomainFrame::Receipt(r) => (r.target, r.status),
            other => panic!("Expected Receipt, got {other:?}"),
        })
        .collect();

    assert!(receipts.contains(&("billing.charge".to_string(), ContractStatus::Sent)));
    assert!(receipts.contains(&(
        "billing.non_existent".to_string(),
        ContractStatus::TargetInvalid
    )));
}

/// 2. Tests the 4-stage lifecycle receipts (`Sent -> Received -> Started -> Completed`),
///    kernel-generated `message_id`, three-level idempotency, and physical pipe identity guards.
#[test]
fn test_four_stage_receipts_and_kernel_idempotency() {
    let kernel = AtelierKernel::new_in_memory().unwrap();
    let order = kernel.attach_domain("order");
    let inventory = kernel.attach_domain("inventory");
    let rogue = kernel.attach_domain("rogue");

    inventory.register_handlers(&["reserve"]);
    rogue.register_handlers(&["noop"]);
    let _ = inventory.drain_frames();
    let _ = rogue.drain_frames();

    // Order sends a message with explicit `seq = 42`
    let msg_id_1 = order.send_one_with_seq(
        42,
        vec!["inventory.reserve".to_string()],
        "ReserveStock",
        json!({"warehouse": "W1"}),
        json!({"sku": "SKU-9", "qty": 2}),
    );

    // Level 1 Idempotency: Resending with the same `(domain="order", seq=42)` must NOT re-deliver to `inventory`
    // and must return the exact same `message_id`!
    let msg_id_2 = order.send_one_with_seq(
        42,
        vec!["inventory.reserve".to_string()],
        "ReserveStock",
        json!({"warehouse": "W1"}),
        json!({"sku": "SKU-9", "qty": 2}),
    );
    assert_eq!(msg_id_1, msg_id_2);

    // Inventory only received 1 delivery frame despite duplicate submission
    let inv_deliveries = inventory.drain_frames();
    assert_eq!(inv_deliveries.len(), 1);
    let _ = order.drain_frames(); // clear initial Sent receipts

    // Security check: `rogue` domain tries to forge a `Completed` receipt for `inventory.reserve`
    rogue.report_receipt(
        &msg_id_1,
        "inventory.reserve",
        ContractStatus::Completed,
        Some(json!({"forged": true})),
    );
    assert!(
        order.drain_frames().is_empty(),
        "Kernel must reject forged receipts from a pipe that does not own the target domain"
    );

    // Inventory reports `Received` twice -> Kernel Level 3 Idempotency must only forward once
    inventory.report_receipt(
        &msg_id_1,
        "inventory.reserve",
        ContractStatus::Received,
        None,
    );
    inventory.report_receipt(
        &msg_id_1,
        "inventory.reserve",
        ContractStatus::Received,
        None,
    );

    // Inventory reports `Started`
    inventory.report_receipt(
        &msg_id_1,
        "inventory.reserve",
        ContractStatus::Started,
        None,
    );

    // Inventory reports `Completed` with output data, plus a late `Started` and duplicate `Completed`
    inventory.report_receipt(
        &msg_id_1,
        "inventory.reserve",
        ContractStatus::Completed,
        Some(json!({"reserved_id": "RES-777"})),
    );
    inventory.report_receipt(
        &msg_id_1,
        "inventory.reserve",
        ContractStatus::Started,
        None,
    );
    inventory.report_receipt(
        &msg_id_1,
        "inventory.reserve",
        ContractStatus::Completed,
        Some(json!({"reserved_id": "OVERWRITE-ATTEMPT"})),
    );

    let stages: Vec<(ContractStatus, Option<serde_json::Value>)> = order
        .drain_frames()
        .into_iter()
        .map(|f| match f {
            KernelToDomainFrame::Receipt(r) => (r.status, r.data),
            other => panic!("Expected Receipt, got {other:?}"),
        })
        .collect();

    assert_eq!(
        stages,
        vec![
            (ContractStatus::Received, None),
            (ContractStatus::Started, None),
            (
                ContractStatus::Completed,
                Some(json!({"reserved_id": "RES-777"}))
            ),
        ]
    );
}

/// 3. Tests "Storage as Messaging" (`StorageInsert`, `StorageQuery`, `StorageUpdate`, `StorageDelete`),
///    two-layer schema evolution invariance, and cross-domain storage isolation.
#[test]
fn test_storage_as_messaging_and_domain_isolation() {
    let kernel = AtelierKernel::new_in_memory().unwrap();
    let user_domain = kernel.attach_domain("user");
    let other_domain = kernel.attach_domain("analytics");

    // 1. Insert v1 data structure into `user` domain storage via message
    user_domain.storage_insert(
        json!({"record_id": "u_1001", "category": "profile"}),
        json!({"name": "Lin", "role": "architect"}),
    );

    let insert_receipts: Vec<_> = user_domain
        .drain_frames()
        .into_iter()
        .filter_map(|f| match f {
            KernelToDomainFrame::Receipt(r) => Some(r.status),
            _ => None,
        })
        .collect();
    // Every storage message goes through all 4 stages: Sent -> Received -> Started -> Completed
    assert_eq!(
        insert_receipts,
        vec![
            ContractStatus::Sent,
            ContractStatus::Received,
            ContractStatus::Started,
            ContractStatus::Completed,
        ]
    );

    // 2. Evolve inner `data` packet structure (add nested preferences and array) without changing kernel schema
    user_domain.storage_update(
        json!({"record_id": "u_1001", "patch": true}),
        json!({
            "role": "chief_architect",
            "preferences": {"theme": "dark", "shortcuts": ["ctrl+k"]},
            "version": 2
        }),
    );
    let _ = user_domain.drain_frames();

    // 3. Query from `user` domain using JSON filter
    user_domain.storage_query(json!({
        "category": "profile",
        "filter": {"role": "chief_architect"}
    }));

    let query_frames = user_domain.drain_frames();
    let completed = query_frames
        .into_iter()
        .find_map(|f| match f {
            KernelToDomainFrame::Receipt(r)
                if r.message_type == MSG_STORAGE_QUERY && r.status == ContractStatus::Completed =>
            {
                r.data
            }
            _ => None,
        })
        .expect("Expected Completed receipt for StorageQuery");

    assert_eq!(completed["count"], 1);
    assert_eq!(completed["items"][0]["data"]["name"], "Lin");
    assert_eq!(
        completed["items"][0]["data"]["preferences"]["theme"],
        "dark"
    );

    // 4. Default query without specifying `domain` queries `analytics`'s own isolated table (`store_analytics`)
    other_domain.storage_query(json!({
        "record_id": "u_1001"
    }));
    let own_query_completed = other_domain
        .drain_frames()
        .into_iter()
        .find_map(|f| match f {
            KernelToDomainFrame::Receipt(r) if r.status == ContractStatus::Completed => r.data,
            _ => None,
        })
        .unwrap();
    assert_eq!(own_query_completed["domain"], "analytics");
    assert_eq!(own_query_completed["count"], 0);

    // 5. Cross-domain READ is allowed: `analytics` domain queries `user` domain's storage (`"domain": "user"`)
    other_domain.storage_query(json!({
        "domain": "user",
        "record_id": "u_1001"
    }));
    let cross_read_completed = other_domain
        .drain_frames()
        .into_iter()
        .find_map(|f| match f {
            KernelToDomainFrame::Receipt(r) if r.status == ContractStatus::Completed => r.data,
            _ => None,
        })
        .expect("Cross-domain StorageQuery should succeed");
    assert_eq!(cross_read_completed["domain"], "user");
    assert_eq!(cross_read_completed["count"], 1);
    assert_eq!(cross_read_completed["items"][0]["data"]["name"], "Lin");

    // 6. Cross-domain WRITE is strictly forbidden: `analytics` cannot Insert, Update, or Delete in `user` storage
    other_domain.storage_update(
        json!({"domain": "user", "record_id": "u_1001", "patch": true}),
        json!({"role": "hacked"}),
    );
    let cross_update_failed = other_domain
        .drain_frames()
        .into_iter()
        .find_map(|f| match f {
            KernelToDomainFrame::Receipt(r) if r.status == ContractStatus::ExecutionFailed => {
                r.data
            }
            _ => None,
        })
        .expect("Cross-domain StorageUpdate must be rejected with ExecutionFailed");
    assert!(
        cross_update_failed
            .to_string()
            .contains("cross-domain write forbidden")
    );

    other_domain.storage_delete(json!({"target_domain": "user", "record_id": "u_1001"}));
    let cross_delete_failed = other_domain
        .drain_frames()
        .into_iter()
        .find_map(|f| match f {
            KernelToDomainFrame::Receipt(r) if r.status == ContractStatus::ExecutionFailed => {
                r.data
            }
            _ => None,
        })
        .expect("Cross-domain StorageDelete must be rejected with ExecutionFailed");
    assert!(
        cross_delete_failed
            .to_string()
            .contains("cross-domain write forbidden")
    );

    // 7. Owner domain (`user`) can delete its own record via `StorageDelete`
    user_domain.storage_delete(json!({"record_id": "u_1001"}));
    let delete_completed = user_domain
        .drain_frames()
        .into_iter()
        .find_map(|f| match f {
            KernelToDomainFrame::Receipt(r)
                if r.message_type == MSG_STORAGE_DELETE
                    && r.status == ContractStatus::Completed =>
            {
                r.data
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(delete_completed["deleted_count"], 1);
}

/// 4. Tests real-time `KernelObserver` and OS child process `Stdio` pipe lifecycle supervision
///    (language-agnostic shell child process registering, handling a message, and crash recovery).
#[test]
fn test_os_process_stdio_supervision_and_observer() {
    let kernel = AtelierKernel::new_in_memory().unwrap();
    let observer_rx = kernel.observer().subscribe();
    let caller = kernel.attach_domain("caller");

    // Spawn a real external OS process that speaks the Atelier Kernel JSON-Lines protocol over dedicated pipes.
    // It registers handler `echo`, reads one delivered message from stdin, emits Received -> Started -> Completed, and then exits.
    let script = format!(
        r#"
{PY_IPC_PREAMBLE}
print("normal stdout noise from child process")
ipc_out.write(json.dumps({{"frame_type":"send","seq":1,"targets":["kernel.registry"],"message_type":"RegisterHandlers","params":{{"handlers":["echo"]}},"data":{{}}}}) + "\n")
ipc_out.flush()
for line in ipc_in:
    frame = json.loads(line)
    if frame.get("frame_type") == "deliver":
        msg_id = frame["message_id"]
        ipc_out.write(json.dumps({{"frame_type":"receipt","message_id":msg_id,"target":"ext_sh.echo","status":"received","data":None}}) + "\n")
        ipc_out.write(json.dumps({{"frame_type":"receipt","message_id":msg_id,"target":"ext_sh.echo","status":"started","data":None}}) + "\n")
        ipc_out.write(json.dumps({{"frame_type":"receipt","message_id":msg_id,"target":"ext_sh.echo","status":"completed","data":{{"echoed_by":"os_pipe_sh"}}}}) + "\n")
        ipc_out.flush()
        break
"#
    );

    let pid = kernel
        .spawn_process_domain(DomainProcessSpec {
            domain_id: "ext_sh".to_string(),
            command: "python3".to_string(),
            args: vec!["-c".to_string(), script],
            working_dir: None,
            auto_restart: false,
            ..Default::default()
        })
        .expect("Failed to spawn external domain process");
    assert!(pid > 0);

    // Wait briefly for `ext_sh` child process to register its `echo` handler over stdout pipe
    let mut active = false;
    for _ in 0..50 {
        if kernel.router().domain_status("ext_sh") == Some(DomainStatus::Active) {
            active = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(active, "External child process domain should become Active");

    // Caller sends message to `ext_sh.echo`
    let msg_id = caller.send_one(
        vec!["ext_sh.echo".to_string()],
        "EchoTest",
        json!({"mode": "stdio"}),
        json!({"hello": "world"}),
    );

    // Collect all 4 stage receipts (`Sent`, `Received`, `Started`, `Completed`) from the external OS process
    let mut received_statuses = Vec::new();
    let mut completed_payload = None;
    for _ in 0..4 {
        let frame = caller
            .recv_frame_timeout(Duration::from_secs(2))
            .expect("Timed out waiting for 4-stage receipt from OS child process");
        if let KernelToDomainFrame::Receipt(r) = frame {
            assert_eq!(r.message_id, msg_id);
            received_statuses.push(r.status);
            if r.status == ContractStatus::Completed {
                completed_payload = r.data;
            }
        }
    }

    assert_eq!(
        received_statuses,
        vec![
            ContractStatus::Sent,
            ContractStatus::Received,
            ContractStatus::Started,
            ContractStatus::Completed,
        ]
    );
    assert_eq!(completed_payload, Some(json!({"echoed_by": "os_pipe_sh"})));

    // Verify `KernelObserver` captured the entire 4-stage lifecycle of `msg_id`
    let observed_for_msg = kernel.observer().events_for_message(&msg_id);
    let observed_statuses: Vec<ContractStatus> =
        observed_for_msg.iter().map(|e| e.status).collect();
    assert_eq!(
        observed_statuses,
        vec![
            ContractStatus::Sent,
            ContractStatus::Received,
            ContractStatus::Started,
            ContractStatus::Completed,
        ]
    );
    assert!(observer_rx.try_recv().is_ok());
}

/// 5. Tests that a delayed fault or pipe read from a stale process generation (`instance_id`)
///    cannot revoke routes or inject frames into a newly respawned instance of the same domain.
#[test]
fn test_stale_instance_fault_does_not_revoke_new_domain_instance() {
    let kernel = AtelierKernel::new_in_memory().unwrap();

    // Instance 1 attaches and registers `worker.job_v1`
    let worker_v1 = kernel.attach_domain("worker");
    let inst_1 = kernel
        .router()
        .domain_instance_id("worker")
        .expect("Expected instance_id for worker v1");
    worker_v1.register_handlers(&["job_v1"]);
    let _ = worker_v1.drain_frames();
    assert_eq!(
        kernel.router().domain_status("worker"),
        Some(DomainStatus::Active)
    );

    // Instance 2 attaches (simulating process restart) and registers `worker.job_v2`
    let worker_v2 = kernel.attach_domain("worker");
    let inst_2 = kernel
        .router()
        .domain_instance_id("worker")
        .expect("Expected instance_id for worker v2");
    assert!(inst_2 > inst_1);
    worker_v2.register_handlers(&["job_v2"]);
    let _ = worker_v2.drain_frames();

    // Now simulate delayed reader thread exit from Instance 1 calling `mark_domain_faulted_for_instance`
    let revoked = kernel.router().mark_domain_faulted_for_instance(
        "worker",
        Some(inst_1),
        "delayed EOF from old reader thread",
    );
    assert!(
        !revoked,
        "Stale instance_id fault report must be ignored by KernelRouter"
    );
    assert_eq!(
        kernel.router().domain_status("worker"),
        Some(DomainStatus::Active),
        "New instance must remain Active"
    );
    assert_eq!(
        kernel.router().active_routes(),
        vec!["worker.job_v2".to_string()],
        "New instance routes must remain intact"
    );

    // Also verify stale raw pipe line and stale DomainPipeClient from Instance 1 are ignored
    kernel.router().handle_raw_pipe_line_for_instance(
        "worker",
        inst_1,
        r#"{"frame_type":"send","seq":99,"targets":["kernel.registry"],"message_type":"UnregisterHandlers","params":{},"data":{}}"#,
    );
    let stale_msg_id = worker_v1.register_handlers(&["zombie_handler"]);
    assert!(
        stale_msg_id.is_empty(),
        "Stale DomainPipeClient frame submission must be rejected"
    );
    assert_eq!(
        kernel.router().active_routes(),
        vec!["worker.job_v2".to_string()],
        "Stale instance pipe line must not affect current instance"
    );

    // Faulting with the current `inst_2` succeeds as expected
    let revoked_current = kernel.router().mark_domain_faulted_for_instance(
        "worker",
        Some(inst_2),
        "current instance exited",
    );
    assert!(revoked_current);
    assert_eq!(
        kernel.router().domain_status("worker"),
        Some(DomainStatus::Faulted)
    );
    assert!(kernel.router().active_routes().is_empty());
}

/// 6. Tests unified multi-state logging:
///    - Structured lifecycle logs with `phase` ("start" / "finish") persisted to `hub_logs`.
///    - Ephemeral progress logs (`ephemeral: true`) tracked in-memory & fanned out via SSE, NEVER polluting `hub_logs`.
///    - `console_only: true` logs suppressed from both DB and subscribers.
///    - Raw pipe JSON frame ingestion for `frame_type: "log"`.
#[test]
fn test_unified_multi_state_logging_and_hub_logs_persistence() {
    let tmp_dir = std::env::temp_dir().join(format!("atelier_log_test_{}", std::process::id()));
    let db_path = tmp_dir.join("kernel.db");
    let kernel = AtelierKernel::open_with_db(&db_path).unwrap();

    let rx = kernel.logger().subscribe();

    // 1. Log structured lifecycle events with `phase`
    kernel.logger().phase(
        "INFO",
        "ENGINES",
        "model_runner",
        "start",
        "Initializing neural weights",
    );
    kernel.logger().phase(
        "INFO",
        "ENGINES",
        "model_runner",
        "finish",
        "Weights loaded successfully",
    );

    // 2. Log high-frequency heartbeat with `console_only = true`
    kernel.logger().reinject(atelier_kernel::LogRecord {
        source_domain: None,
        id: None,
        ts: atelier_kernel::logging::now_secs_f64(),
        level: "DEBUG".to_string(),
        layer: "ENGINES".to_string(),
        component: "heartbeat".to_string(),
        message: "tick-heartbeat".to_string(),
        phase: String::new(),
        console_only: true,
        ephemeral: false,
        key: None,
        frac: None,
        waiting: false,
        done: false,
    });

    // 3. Log ephemeral progress: downloading weights
    kernel.logger().progress(
        "ENGINES",
        "downloader",
        "Downloading model chunk 1/2",
        Some("dl_model_weights".to_string()),
        Some(0.50),
        false,
        false,
    );

    // Verify in-memory active progress map
    let active_prog = kernel.logger().active_progress();
    assert_eq!(active_prog.len(), 1);
    assert_eq!(active_prog[0].key.as_deref(), Some("dl_model_weights"));
    assert_eq!(active_prog[0].frac, Some(0.50));

    // Finish the progress
    kernel.logger().progress(
        "ENGINES",
        "downloader",
        "Download complete",
        Some("dl_model_weights".to_string()),
        Some(1.00),
        false,
        true, // done
    );

    // After done=true, active_progress clears the key
    assert!(kernel.logger().active_progress().is_empty());

    // 4. Test ingestion of structured log frame from child domain pipe
    let _domain_pipe = kernel.attach_domain("order");
    kernel.router().handle_raw_pipe_line_for_instance(
        "order",
        1,
        r#"{"frame_type":"log","record":{"source_domain":"spoofed","level":"INFO","layer":"DOMAIN","component":"order","message":"Order #1001 processed","phase":"finish"}}"#,
    );

    // Drain real-time subscriber messages
    let mut received_records = Vec::new();
    while let Ok(rec) = rx.try_recv() {
        received_records.push(rec);
    }

    // Subscriber should have received:
    // - 2 phase logs ("start", "finish")
    // - 2 progress logs (0.50, 1.00 done)
    // - 1 pipe log ("Order #1001 processed")
    // NOTE: console_only heartbeat was NOT sent to subscribers!
    assert_eq!(received_records.len(), 5);
    assert_eq!(
        received_records
            .iter()
            .find(|r| r.message == "Order #1001 processed")
            .unwrap()
            .source_domain
            .as_deref(),
        Some("order")
    );
    assert!(
        !received_records
            .iter()
            .any(|r| r.message == "tick-heartbeat")
    );

    // Wait briefly for LogStore async writer batch to flush to disk
    std::thread::sleep(Duration::from_millis(350));

    // 5. Query persisted `hub_logs`
    let stored_logs = kernel
        .logger()
        .query_logs(100, None, None, None, None, None)
        .unwrap();

    // Verify hub_logs contains only true historical logs, NOT ephemeral progress and NOT console_only!
    assert!(
        !stored_logs
            .iter()
            .any(|l| l.message.contains("Downloading"))
    );
    assert!(!stored_logs.iter().any(|l| l.message == "tick-heartbeat"));

    let start_log = stored_logs
        .iter()
        .find(|l| l.message == "Initializing neural weights")
        .expect("start log persisted");
    assert_eq!(start_log.phase, "start");
    assert_eq!(start_log.source_domain.as_deref(), Some("kernel"));
    assert_eq!(start_log.layer, "ENGINES");

    let finish_log = stored_logs
        .iter()
        .find(|l| l.message == "Weights loaded successfully")
        .expect("finish log persisted");
    assert_eq!(finish_log.phase, "finish");

    let pipe_log = stored_logs
        .iter()
        .find(|l| l.message == "Order #1001 processed")
        .expect("pipe log persisted");
    assert_eq!(pipe_log.component, "order");
    assert_eq!(pipe_log.phase, "finish");
    assert_eq!(pipe_log.source_domain.as_deref(), Some("order"));

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// 8. Tests that when an external OS process domain exits unexpectedly, the Kernel Supervisor
///    automatically restarts it, assigns a new instance_id, and restores registered routes.
#[test]
fn test_domain_auto_restart_on_unexpected_exit() {
    let kernel = AtelierKernel::new_in_memory().unwrap();
    let caller = kernel.attach_domain("test_client");

    // Python script that registers `ping`, handles messages, and crashes if told to
    let script = format!(
        r#"
{PY_IPC_PREAMBLE}
ipc_out.write(json.dumps({{"frame_type":"send","seq":1,"targets":["kernel.registry"],"message_type":"RegisterHandlers","params":{{"handlers":["ping"]}},"data":{{}}}}) + "\n")
ipc_out.flush()
for line in ipc_in:
    frame = json.loads(line)
    if frame.get("frame_type") == "deliver":
        msg_id = frame["message_id"]
        ipc_out.write(json.dumps({{"frame_type":"receipt","message_id":msg_id,"target":"resilient_sh.ping","status":"received","data":None}}) + "\n")
        ipc_out.write(json.dumps({{"frame_type":"receipt","message_id":msg_id,"target":"resilient_sh.ping","status":"started","data":None}}) + "\n")
        if frame.get("data", {{}}).get("crash") == True:
            ipc_out.flush()
            sys.exit(99)
        ipc_out.write(json.dumps({{"frame_type":"receipt","message_id":msg_id,"target":"resilient_sh.ping","status":"completed","data":{{"reply":"pong"}}}}) + "\n")
        ipc_out.flush()
"#
    );

    let pid1 = kernel
        .spawn_process_domain(DomainProcessSpec {
            domain_id: "resilient_sh".to_string(),
            command: "python3".to_string(),
            args: vec!["-c".to_string(), script],
            working_dir: None,
            auto_restart: true,
            max_restarts: Some(5),
            restart_delay_ms: 60,
        })
        .expect("Spawn resilient_sh");
    assert!(pid1 > 0);

    // 1. Wait for resilient_sh to become Active
    let mut active = false;
    for _ in 0..50 {
        if kernel.router().domain_status("resilient_sh") == Some(DomainStatus::Active) {
            active = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(active, "resilient_sh should become Active");

    let inst1 = kernel.router().domain_instance_id("resilient_sh").unwrap();

    // 2. Normal ping check
    let msg1 = caller.send_one(
        vec!["resilient_sh.ping".to_string()],
        "PingTest",
        json!({}),
        json!({"crash": false}),
    );
    let mut completed1 = false;
    for _ in 0..4 {
        if let Ok(KernelToDomainFrame::Receipt(r)) =
            caller.recv_frame_timeout(Duration::from_secs(2))
        {
            if r.message_id == msg1 && r.status == ContractStatus::Completed {
                completed1 = true;
                break;
            }
        }
    }
    assert!(completed1, "Initial ping should complete");

    // 3. Send crash message to trigger unexpected exit
    let _msg_crash = caller.send_one(
        vec!["resilient_sh.ping".to_string()],
        "CrashTest",
        json!({}),
        json!({"crash": true}),
    );

    // Drain receipts from crash
    std::thread::sleep(Duration::from_millis(150));
    let _ = caller.drain_frames();

    // 4. Wait for Supervisor to auto-restart resilient_sh
    let mut restarted = false;
    let mut inst2 = inst1;
    for _ in 0..50 {
        if kernel.router().domain_status("resilient_sh") == Some(DomainStatus::Active) {
            if let Some(id) = kernel.router().domain_instance_id("resilient_sh") {
                if id > inst1 {
                    restarted = true;
                    inst2 = id;
                    break;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(
        restarted,
        "Domain should be automatically restarted and become Active again"
    );
    assert!(
        inst2 > inst1,
        "Restarted instance must have higher generation instance_id"
    );
    assert!(kernel.supervisor().restart_count("resilient_sh") >= 1);

    // 5. Send message to the new instance and verify it handles it cleanly
    let msg2 = caller.send_one(
        vec!["resilient_sh.ping".to_string()],
        "PingTest2",
        json!({}),
        json!({"crash": false}),
    );
    let mut completed2 = false;
    for _ in 0..4 {
        if let Ok(KernelToDomainFrame::Receipt(r)) =
            caller.recv_frame_timeout(Duration::from_secs(2))
        {
            if r.message_id == msg2 && r.status == ContractStatus::Completed {
                completed2 = true;
                break;
            }
        }
    }
    assert!(
        completed2,
        "Ping after auto-restart must complete successfully"
    );

    // Clean stop
    assert!(kernel.supervisor().stop_process_domain("resilient_sh"));
}

/// 9. Tests that explicit `stop_process_domain` does NOT trigger an automatic restart.
#[test]
fn test_explicit_stop_does_not_auto_restart() {
    let kernel = AtelierKernel::new_in_memory().unwrap();

    let script = format!(
        r#"
{PY_IPC_PREAMBLE}
ipc_out.write(json.dumps({{"frame_type":"send","seq":1,"targets":["kernel.registry"],"message_type":"RegisterHandlers","params":{{"handlers":["ping"]}},"data":{{}}}}) + "\n")
ipc_out.flush()
for _ in ipc_in:
    pass
"#
    );

    kernel
        .spawn_process_domain(DomainProcessSpec {
            domain_id: "stop_test".to_string(),
            command: "python3".to_string(),
            args: vec!["-c".to_string(), script],
            working_dir: None,
            auto_restart: true,
            max_restarts: Some(5),
            restart_delay_ms: 50,
        })
        .expect("Spawn stop_test");

    for _ in 0..50 {
        if kernel.router().domain_status("stop_test") == Some(DomainStatus::Active) {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    assert_eq!(
        kernel.router().domain_status("stop_test"),
        Some(DomainStatus::Active)
    );

    // Explicit stop
    assert!(kernel.supervisor().stop_process_domain("stop_test"));
    assert!(kernel.supervisor().is_stopped("stop_test"));

    // Wait longer than restart_delay_ms
    std::thread::sleep(Duration::from_millis(150));

    // Must remain stopped / faulted, NOT auto-restarted
    assert_ne!(
        kernel.router().domain_status("stop_test"),
        Some(DomainStatus::Active)
    );
    assert!(kernel.router().active_routes().is_empty());
}

/// 10. Tests that automatic restart gives up when exceeding `max_restarts`.
#[test]
fn test_auto_restart_respects_max_restarts_limit() {
    let kernel = AtelierKernel::new_in_memory().unwrap();

    // Command that immediately fails every time
    kernel
        .spawn_process_domain(DomainProcessSpec {
            domain_id: "doomed_domain".to_string(),
            command: "python3".to_string(),
            args: vec!["-c".to_string(), "import sys; sys.exit(1)".to_string()],
            working_dir: None,
            auto_restart: true,
            max_restarts: Some(2),
            restart_delay_ms: 30,
        })
        .expect("Spawn doomed_domain");

    // Wait for retries to exhaust
    std::thread::sleep(Duration::from_millis(200));

    // Verify it attempted restarts up to limit and stopped
    let count = kernel.supervisor().restart_count("doomed_domain");
    assert!(
        count >= 2,
        "Expected at least 2 restart attempts, got {}",
        count
    );
    assert_eq!(
        kernel.router().domain_status("doomed_domain"),
        Some(DomainStatus::Faulted)
    );
}

/// 11. Tests the three built-in delivery persistence message types:
/// `Durable` (SQLite + memory), `Volatile` (memory only), and `Ephemeral` (in-flight memory only, evicted on completion).
#[test]
fn test_delivery_persistence_message_types_durable_volatile_ephemeral() {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join(format!("ledger-policy-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("kernel.db");

    let durable_id;
    let volatile_id;
    let ephemeral_id;

    {
        let kernel = AtelierKernel::open_with_db(&db_path).unwrap();
        let sender = kernel.attach_domain("sender");
        let worker = kernel.attach_domain("worker");
        worker.register_handlers(&["run"]);
        let _ = worker.drain_frames();

        // 1. Durable message: persisted in SQLite `kernel_ledger` and retained in memory
        durable_id = sender.send_one(
            vec!["worker.run".to_string()],
            MSG_DELIVERY_DURABLE,
            json!({}),
            json!({"job": "durable-1"}),
        );
        assert_eq!(
            kernel
                .ledger()
                .get_persisted_record(&durable_id)
                .unwrap()
                .status,
            ContractStatus::Sent
        );
        worker.report_receipt(&durable_id, "worker.run", ContractStatus::Received, None);
        worker.report_receipt(&durable_id, "worker.run", ContractStatus::Started, None);
        worker.report_receipt(
            &durable_id,
            "worker.run",
            ContractStatus::Completed,
            Some(json!({"ok": "durable"})),
        );
        assert_eq!(
            kernel
                .ledger()
                .get_record(&durable_id, "worker.run")
                .unwrap()
                .status,
            ContractStatus::Completed
        );
        assert_eq!(
            kernel
                .ledger()
                .get_persisted_record(&durable_id)
                .unwrap()
                .status,
            ContractStatus::Completed
        );

        // 2. Volatile message: retained in memory, NEVER written to SQLite `kernel_ledger`
        volatile_id = sender.send_one(
            vec!["worker.run".to_string()],
            MSG_DELIVERY_VOLATILE,
            json!({}),
            json!({"job": "volatile-1"}),
        );
        assert!(kernel.ledger().get_persisted_record(&volatile_id).is_none());
        worker.report_receipt(&volatile_id, "worker.run", ContractStatus::Received, None);
        worker.report_receipt(&volatile_id, "worker.run", ContractStatus::Started, None);
        worker.report_receipt(
            &volatile_id,
            "worker.run",
            ContractStatus::Completed,
            Some(json!({"ok": "volatile"})),
        );
        assert_eq!(
            kernel
                .ledger()
                .get_record(&volatile_id, "worker.run")
                .unwrap()
                .status,
            ContractStatus::Completed
        );
        assert!(kernel.ledger().get_persisted_record(&volatile_id).is_none());

        // 3. Ephemeral message: tracked in memory while in-flight, NEVER written to SQLite,
        // and automatically evicted from memory once Completed.
        ephemeral_id = sender.send_one(
            vec!["worker.run".to_string()],
            MSG_DELIVERY_EPHEMERAL,
            json!({}),
            json!({"token": "stream-1"}),
        );
        assert!(
            kernel
                .ledger()
                .get_record(&ephemeral_id, "worker.run")
                .is_some(),
            "Ephemeral message must be tracked in-memory while in-flight"
        );
        assert!(
            kernel
                .ledger()
                .get_persisted_record(&ephemeral_id)
                .is_none(),
            "Ephemeral message must never be persisted to SQLite"
        );
        worker.report_receipt(&ephemeral_id, "worker.run", ContractStatus::Received, None);
        worker.report_receipt(&ephemeral_id, "worker.run", ContractStatus::Started, None);
        worker.report_receipt(
            &ephemeral_id,
            "worker.run",
            ContractStatus::Completed,
            Some(json!({"ok": "ephemeral"})),
        );
        assert!(
            kernel
                .ledger()
                .get_record(&ephemeral_id, "worker.run")
                .is_none(),
            "Ephemeral message must be evicted from in-memory ledger upon reaching terminal status"
        );
        assert_eq!(kernel.ledger().persisted_count(), 1);
    }

    // Reopen kernel with the same SQLite database file: only the Durable delivery record survives across restart.
    let reopened = AtelierKernel::open_with_db(&db_path).unwrap();
    assert_eq!(reopened.ledger().persisted_count(), 1);
    let recovered = reopened
        .ledger()
        .get_record(&durable_id, "worker.run")
        .expect("Durable delivery record should be reloaded from SQLite on reopen");
    assert_eq!(recovered.status, ContractStatus::Completed);
    assert_eq!(recovered.data, Some(json!({"ok": "durable"})));
    assert!(
        reopened
            .ledger()
            .get_record(&volatile_id, "worker.run")
            .is_none()
    );
    assert!(
        reopened
            .ledger()
            .get_record(&ephemeral_id, "worker.run")
            .is_none()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// 12. Concurrency Stress Test:
/// Multi-threaded senders and workers concurrently dispatching `Durable`, `Volatile`, `Ephemeral`,
/// fan-out, and `kernel.storage` CRUD messages against a file-backed SQLite kernel.
#[test]
fn test_concurrent_multidomain_stress() {
    use std::sync::{Arc, Barrier};

    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join(format!("stress-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("stress.db");

    let kernel = Arc::new(AtelierKernel::open_with_db(&db_path).unwrap());
    kernel.logger().set_console_enabled(false);

    let num_workers = 4usize;
    let num_senders = 8usize;
    let msgs_per_sender = 60usize; // 480 outbound submissions (20 Durable, 20 Volatile, 20 Ephemeral per sender)

    // Attach and register 4 worker domains, each driven by its own background thread
    let mut worker_handles = Vec::new();
    for w_idx in 0..num_workers {
        let w_name = format!("worker_{w_idx}");
        let client = kernel.attach_domain(&w_name);
        client.register_handlers(&["process"]);
        let _ = client.drain_frames();

        let expected_deliveries = (num_senders * msgs_per_sender) / num_workers;
        worker_handles.push(std::thread::spawn(move || {
            let mut handled = 0usize;
            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            while handled < expected_deliveries && std::time::Instant::now() < deadline {
                let frames = client.drain_frames();
                if frames.is_empty() {
                    std::thread::yield_now();
                    continue;
                }
                for frame in frames {
                    if let KernelToDomainFrame::Deliver(msg) = frame {
                        client.report_receipt(
                            &msg.message_id,
                            &msg.target,
                            ContractStatus::Received,
                            None,
                        );
                        client.report_receipt(
                            &msg.message_id,
                            &msg.target,
                            ContractStatus::Started,
                            None,
                        );
                        client.report_receipt(
                            &msg.message_id,
                            &msg.target,
                            ContractStatus::Completed,
                            Some(json!({"worker": w_name, "seq": handled})),
                        );
                        handled += 1;
                    }
                }
            }
            assert_eq!(
                handled, expected_deliveries,
                "Worker {w_name} did not receive all expected deliveries"
            );
        }));
    }

    let barrier = Arc::new(Barrier::new(num_senders));
    let mut sender_handles = Vec::new();

    for s_idx in 0..num_senders {
        let k = Arc::clone(&kernel);
        let b = Arc::clone(&barrier);
        sender_handles.push(std::thread::spawn(move || {
            let s_name = format!("sender_{s_idx}");
            let client = k.attach_domain(&s_name);
            client.register_handlers(&["noop"]);
            let _ = client.drain_frames();

            let mut durable_ids = Vec::new();
            let mut volatile_ids = Vec::new();
            let mut ephemeral_ids = Vec::new();

            b.wait();

            for i in 0..msgs_per_sender {
                let target_worker = (s_idx + i) % num_workers;
                let target = format!("worker_{target_worker}.process");
                let msg_type = match i % 3 {
                    0 => MSG_DELIVERY_DURABLE,
                    1 => MSG_DELIVERY_VOLATILE,
                    _ => MSG_DELIVERY_EPHEMERAL,
                };
                let mid = client.send_one(
                    vec![target.clone()],
                    msg_type,
                    json!({"sender": s_idx, "idx": i}),
                    json!({"payload": i}),
                );
                match i % 3 {
                    0 => durable_ids.push((mid, target)),
                    1 => volatile_ids.push((mid, target)),
                    _ => ephemeral_ids.push((mid, target)),
                }

                // Interleave concurrent domain storage writes every 10 messages
                if i % 10 == 0 {
                    let _ = client.storage_insert(
                        json!({"record_id": format!("rec_{s_idx}_{i}"), "category": "stress_cat", "upsert": true}),
                        json!({"v": i}),
                    );
                }
            }

            // Wait until all 4-stage `Completed` receipts arrive at this sender
            let mut completed_count = 0usize;
            // Expect `msgs_per_sender` worker completions + `msgs_per_sender / 10` storage completions
            let expected_completed = msgs_per_sender + (msgs_per_sender / 10);
            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            while completed_count < expected_completed && std::time::Instant::now() < deadline {
                let frames = client.drain_frames();
                if frames.is_empty() {
                    std::thread::sleep(Duration::from_millis(2));
                    continue;
                }
                for frame in frames {
                    if let KernelToDomainFrame::Receipt(r) = frame
                        && r.status == ContractStatus::Completed
                    {
                        completed_count += 1;
                    }
                }
            }
            assert_eq!(
                completed_count, expected_completed,
                "Sender {s_name} did not receive all Completed receipts"
            );

            (durable_ids, volatile_ids, ephemeral_ids)
        }));
    }

    for wh in worker_handles {
        wh.join().expect("Worker thread panicked");
    }

    let mut all_durable = Vec::new();
    let mut all_volatile = Vec::new();
    let mut all_ephemeral = Vec::new();
    for sh in sender_handles {
        let (d, v, e) = sh.join().expect("Sender thread panicked");
        all_durable.extend(d);
        all_volatile.extend(v);
        all_ephemeral.extend(e);
    }

    let expected_per_tier = (num_senders * msgs_per_sender) / 3;
    assert_eq!(all_durable.len(), expected_per_tier);
    assert_eq!(all_volatile.len(), expected_per_tier);
    assert_eq!(all_ephemeral.len(), expected_per_tier);

    // Verify SQLite `kernel_ledger` has ONLY the `Durable` messages and all are `Completed`
    assert_eq!(kernel.ledger().persisted_count(), expected_per_tier);
    for (mid, target) in &all_durable {
        let mem_rec = kernel.ledger().get_record(mid, target).unwrap();
        let db_rec = kernel.ledger().get_persisted_record(mid).unwrap();
        assert_eq!(mem_rec.status, ContractStatus::Completed);
        assert_eq!(db_rec.status, ContractStatus::Completed);
    }

    // Verify `Volatile` messages are in memory (`Completed`), never in SQLite
    for (mid, target) in &all_volatile {
        let mem_rec = kernel.ledger().get_record(mid, target).unwrap();
        assert_eq!(mem_rec.status, ContractStatus::Completed);
        assert!(kernel.ledger().get_persisted_record(mid).is_none());
    }

    // Verify `Ephemeral` messages were evicted from memory upon terminal status and never in SQLite
    for (mid, target) in &all_ephemeral {
        assert!(kernel.ledger().get_record(mid, target).is_none());
        assert!(kernel.ledger().get_persisted_record(mid).is_none());
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// 13. Single-Domain Blocking / Head-of-Line Isolation Test:
/// Verifies that when one OS process domain hangs and stops reading its `stdin` pipe
/// (even when flooded with >512KB of messages exceeding the OS pipe buffer),
/// the Kernel Router and other healthy domains continue routing and completing messages without blocking.
#[test]
fn test_single_domain_blocking_head_of_line_isolation() {
    let kernel = AtelierKernel::new_in_memory().unwrap();
    kernel.logger().set_console_enabled(false);

    // 1. Spawn `blocked_domain`: registers `hang`, then sleeps without ever reading IPC read pipe again.
    let blocked_script = format!(
        r#"
{PY_IPC_PREAMBLE}
ipc_out.write(json.dumps({{"frame_type":"send","seq":1,"targets":["kernel.registry"],"message_type":"RegisterHandlers","params":{{"handlers":["hang"]}},"data":{{}}}}) + "\n")
ipc_out.flush()
# Read the registration receipts, then freeze completely without reading IPC pipe
for _ in range(4):
    ipc_in.readline()
time.sleep(3600)
"#
    );

    // 2. Spawn `healthy_domain`: registers `echo` and immediately completes every delivery.
    let healthy_script = format!(
        r#"
{PY_IPC_PREAMBLE}
ipc_out.write(json.dumps({{"frame_type":"send","seq":1,"targets":["kernel.registry"],"message_type":"RegisterHandlers","params":{{"handlers":["echo"]}},"data":{{}}}}) + "\n")
ipc_out.flush()

for raw in ipc_in:
    frame = json.loads(raw)
    if frame.get("frame_type") == "deliver":
        mid = frame["message_id"]
        target = frame["target"]
        for st in ("received", "started"):
            ipc_out.write(json.dumps({{
                "frame_type": "receipt",
                "message_id": mid,
                "target": target,
                "status": st,
                "data": None
            }}) + "\n")
        ipc_out.write(json.dumps({{
            "frame_type": "receipt",
            "message_id": mid,
            "target": target,
            "status": "completed",
            "data": {{"echo": frame.get("data")}}
        }}) + "\n")
        ipc_out.flush()
"#
    );

    kernel
        .spawn_process_domain(DomainProcessSpec {
            domain_id: "blocked_domain".to_string(),
            command: "python3".to_string(),
            args: vec!["-c".to_string(), blocked_script.to_string()],
            working_dir: None,
            auto_restart: false,
            max_restarts: Some(0),
            restart_delay_ms: 100,
        })
        .expect("Spawn blocked_domain");

    kernel
        .spawn_process_domain(DomainProcessSpec {
            domain_id: "healthy_domain".to_string(),
            command: "python3".to_string(),
            args: vec!["-c".to_string(), healthy_script.to_string()],
            working_dir: None,
            auto_restart: false,
            max_restarts: Some(0),
            restart_delay_ms: 100,
        })
        .expect("Spawn healthy_domain");

    // Wait for both domains to become Active
    for _ in 0..50 {
        if kernel.router().domain_status("blocked_domain") == Some(DomainStatus::Active)
            && kernel.router().domain_status("healthy_domain") == Some(DomainStatus::Active)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        kernel.router().domain_status("blocked_domain"),
        Some(DomainStatus::Active)
    );
    assert_eq!(
        kernel.router().domain_status("healthy_domain"),
        Some(DomainStatus::Active)
    );

    let caller = kernel.attach_domain("caller");
    caller.register_handlers(&["cb"]);
    let _ = caller.drain_frames();

    // Flood `blocked_domain.hang` with 80 messages x 10KB (~800KB > 64KB OS pipe buffer)
    let large_blob = "X".repeat(10_240);
    let flood_start = std::time::Instant::now();
    let mut blocked_msg_ids = Vec::new();
    for i in 0..80 {
        let mid = caller.send_one(
            vec!["blocked_domain.hang".to_string()],
            MSG_DELIVERY_DURABLE,
            json!({"idx": i}),
            json!({"blob": large_blob}),
        );
        blocked_msg_ids.push(mid);
    }
    assert!(
        flood_start.elapsed() < Duration::from_secs(1),
        "Flooding a blocked domain's pipe must NOT block the kernel router"
    );

    // Send a fan-out submission targeting BOTH the wedged domain and the healthy domain
    let fanout_ids = caller.send(
        vec![
            "blocked_domain.hang".to_string(),
            "healthy_domain.echo".to_string(),
        ],
        MSG_DELIVERY_DURABLE,
        json!({}),
        json!({"fanout": true}),
    );
    assert_eq!(fanout_ids.len(), 2);
    let fanout_blocked_id = fanout_ids[0].clone();
    let fanout_healthy_id = fanout_ids[1].clone();

    // Send 20 direct messages to `healthy_domain.echo` while `blocked_domain` is wedged
    let healthy_start = std::time::Instant::now();
    let mut healthy_msg_ids = vec![fanout_healthy_id];
    for i in 0..20 {
        let mid = caller.send_one(
            vec!["healthy_domain.echo".to_string()],
            MSG_DELIVERY_VOLATILE,
            json!({}),
            json!({"ping": i}),
        );
        healthy_msg_ids.push(mid);
    }

    // Wait for all 21 healthy deliveries (1 fan-out + 20 direct) to reach `Completed`
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        let _ = caller.drain_frames();
        let all_done = healthy_msg_ids.iter().all(|id| {
            kernel
                .ledger()
                .get_record(id, "healthy_domain.echo")
                .map(|r| r.status == ContractStatus::Completed)
                .unwrap_or(false)
        });
        if all_done {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    for id in &healthy_msg_ids {
        let rec = kernel
            .ledger()
            .get_record(id, "healthy_domain.echo")
            .expect("Healthy domain record must exist");
        assert_eq!(
            rec.status,
            ContractStatus::Completed,
            "Healthy domain must complete without being affected by blocked_domain"
        );
    }
    assert!(
        healthy_start.elapsed() < Duration::from_secs(2),
        "Healthy domain traffic must remain low-latency even when another domain is completely wedged"
    );

    // Meanwhile, all messages targeting `blocked_domain.hang` are still in `Sent` status
    assert_eq!(
        kernel
            .ledger()
            .get_record(&fanout_blocked_id, "blocked_domain.hang")
            .unwrap()
            .status,
        ContractStatus::Sent
    );

    // Now stop `blocked_domain` -> all 81 in-flight messages to `blocked_domain.hang` must transition to `ExecutionFailed`
    let _ = kernel.supervisor().stop_process_domain("blocked_domain");
    let _ = kernel.supervisor().stop_process_domain("healthy_domain");

    for id in blocked_msg_ids.iter().chain(std::iter::once(&fanout_blocked_id)) {
        let rec = kernel
            .ledger()
            .get_record(id, "blocked_domain.hang")
            .unwrap();
        assert_eq!(rec.status, ContractStatus::ExecutionFailed);
        let db_rec = kernel.ledger().get_persisted_record(id).unwrap();
        assert_eq!(db_rec.status, ContractStatus::ExecutionFailed);
    }
}

/// 14. Tests Machine Constitution (M1–M7) static & pre-boot checker (`check_constitution`).
#[test]
fn test_machine_constitution_checker_m1_to_m7() {
    let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // 1. Repository's own `kernel_boot.json` and `domains/` must be 100% M1–M7 compliant
    let repo_report = check_constitution(repo_root.join("kernel_boot.json"));
    assert!(
        repo_report.is_ok(),
        "Expected 0 constitutional violations in repo, got: {:?}",
        repo_report.violations
    );
    assert!(repo_report.checked_configs >= 1);
    assert!(repo_report.checked_files >= 4);

    // 2. Test M1, M4, M5, M6, M7 violations on synthetic broken variant
    let dir = repo_root
        .join("tmp")
        .join(format!("constitution-test-{}", uuid::Uuid::new_v4()));
    let domains_dir = dir.join("domains");
    std::fs::create_dir_all(&domains_dir).unwrap();

    // M6 violation: unknown field in `kernel_boot.json`
    let unknown_field_cfg = dir.join("unknown_field.json");
    std::fs::write(
        &unknown_field_cfg,
        r#"{"db_path": "./test.db", "unknown_option": 123, "domains": []}"#,
    )
    .unwrap();
    let rep_m6 = check_constitution(&unknown_field_cfg);
    assert!(!rep_m6.is_ok());
    assert!(rep_m6.violations.iter().any(|v| v.rule == "M6"));

    // M1 violation: invalid domain_id with hyphen
    let bad_id_cfg = dir.join("bad_id.json");
    std::fs::write(
        &bad_id_cfg,
        r#"{"db_path": "./test.db", "domains": [{"domain_id": "bad-domain", "command": "python3", "args": []}]}"#,
    )
    .unwrap();
    let rep_m1 = check_constitution(&bad_id_cfg);
    assert!(!rep_m1.is_ok());
    assert!(rep_m1.violations.iter().any(|v| v.rule == "M1"));

    // M4 & M5 violations in Python and JS domain files
    // Note: print() and console.log() are allowed (stdout is isolated from dedicated IPC pipes),
    // while direct manipulation of ATELIER_IPC_READ_FD / ATELIER_IPC_WRITE_FD is an M5 violation.
    let bad_py = domains_dir.join("bad_domain.py");
    std::fs::write(
        &bad_py,
        r#"
from ..other_domain import internal_helper
import sqlite3
import os

def handler(msg):
    print("normal stdout print is safe with dedicated dual anonymous pipes")
    raw_fd = os.environ.get("ATELIER_IPC_WRITE_FD")
"#,
    )
    .unwrap();

    let bad_js = domains_dir.join("bad_domain.js");
    std::fs::write(
        &bad_js,
        r#"
const secret = require('../other_domain/private');
const sqlite3 = require('sqlite3');
console.log("normal stdout console.log is safe");
const rawFd = process.env.ATELIER_IPC_READ_FD;
"#,
    )
    .unwrap();

    let rep_code = check_constitution(&domains_dir);
    assert!(!rep_code.is_ok());
    let rules: Vec<&str> = rep_code.violations.iter().map(|v| v.rule.as_str()).collect();
    assert_eq!(rules.iter().filter(|&&r| r == "M4").count(), 4);
    assert_eq!(rules.iter().filter(|&&r| r == "M5").count(), 2);

    // M7 violation: drifted SQLite table schema
    let drifted_db = dir.join("drifted.db");
    {
        let conn = rusqlite::Connection::open(&drifted_db).unwrap();
        conn.execute_batch(
            "CREATE TABLE store_order (record_id TEXT PRIMARY KEY, wrong_column TEXT);",
        )
        .unwrap();
    }
    let valid_py = domains_dir.join("valid_order.py");
    std::fs::remove_file(&bad_py).unwrap();
    std::fs::remove_file(&bad_js).unwrap();
    std::fs::write(
        &valid_py,
        "from atelier_sdk import AtelierDomain\ndomain = AtelierDomain()\n",
    )
    .unwrap();

    let drift_cfg = dir.join("kernel_boot.json");
    std::fs::write(
        &drift_cfg,
        r#"{
            "db_path": "./drifted.db",
            "domains": [
                {
                    "domain_id": "order",
                    "command": "python3",
                    "args": ["./domains/valid_order.py"]
                }
            ]
        }"#,
    )
    .unwrap();

    let rep_m7 = check_constitution(&drift_cfg);
    assert!(!rep_m7.is_ok());
    assert!(rep_m7.violations.iter().any(|v| v.rule == "M7"));

    let _ = std::fs::remove_dir_all(&dir);
}

/// 15. Query Available Handlers & Automatic Comment Description Extraction Test:
/// Verifies that:
/// - Python SDK automatically extracts function docstrings (`"""..."""`) and `#` comments above `@domain.handler`
/// - Node.js SDK automatically extracts leading `//` / `/** ... */` comments and function-body `//` comments
/// - Any domain can query available handlers and their descriptions via `kernel.registry` (`QueryHandlers`)
#[test]
fn test_query_available_handlers_and_auto_comment_descriptions() {
    use atelier_kernel::{KERNEL_REGISTRY_TARGET, MSG_QUERY_HANDLERS};

    let kernel = AtelierKernel::new_in_memory().unwrap();

    kernel
        .spawn_process_domain(DomainProcessSpec::new(
            "order",
            "python3",
            vec!["./domains/order_domain.py".to_string()],
        ))
        .expect("Failed to spawn Python order domain");

    kernel
        .spawn_process_domain(DomainProcessSpec::new(
            "inventory",
            "node",
            vec!["./domains/inventory_domain.js".to_string()],
        ))
        .expect("Failed to spawn Node.js inventory domain");

    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    while std::time::Instant::now() < deadline {
        if kernel.router().domain_status("order") == Some(DomainStatus::Active)
            && kernel.router().domain_status("inventory") == Some(DomainStatus::Active)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(
        kernel.router().domain_status("order"),
        Some(DomainStatus::Active)
    );
    assert_eq!(
        kernel.router().domain_status("inventory"),
        Some(DomainStatus::Active)
    );

    // 1. Check direct KernelRouter API for Python & Node.js handler descriptions
    assert_eq!(
        kernel.router().handler_description("order.create").as_deref(),
        Some("创建销售订单并持久化到本域存储表 store_order")
    );
    assert_eq!(
        kernel.router().handler_description("order.echo").as_deref(),
        Some("原样回显请求参数与载荷数据（连通性测试）")
    );
    assert_eq!(
        kernel
            .router()
            .handler_description("inventory.deduct")
            .as_deref(),
        Some("扣减指定 SKU 的库存并持久化最新库存快照")
    );
    assert_eq!(
        kernel
            .router()
            .handler_description("inventory.restock")
            .as_deref(),
        Some("补充指定 SKU 的库存数量")
    );
    assert_eq!(
        kernel
            .router()
            .handler_description("inventory.stock_info")
            .as_deref(),
        Some("查询当前所有 SKU 的实时库存账本")
    );

    // 2. Query available handlers and descriptions from another domain via `kernel.registry` (`QueryHandlers`)
    let caller = kernel.attach_domain("orchestrator");
    let query_all_ids = caller.send(
        vec![KERNEL_REGISTRY_TARGET.to_string()],
        MSG_QUERY_HANDLERS,
        json!({}),
        json!({}),
    );
    let frames = caller.drain_frames();
    let completed = frames
        .into_iter()
        .filter_map(|f| match f {
            KernelToDomainFrame::Receipt(r)
                if r.message_id == query_all_ids[0] && r.status == ContractStatus::Completed =>
            {
                r.data
            }
            _ => None,
        })
        .next()
        .expect("Expected Completed receipt for QueryHandlers");

    let routes = completed["routes"].as_array().unwrap();
    assert!(routes.contains(&json!("order.create")));
    assert!(routes.contains(&json!("inventory.deduct")));

    // 3. Query single target description via `params.target`
    let query_one_ids = caller.send(
        vec![KERNEL_REGISTRY_TARGET.to_string()],
        MSG_QUERY_HANDLERS,
        json!({ "target": "inventory.deduct" }),
        json!({}),
    );
    let one_frames = caller.drain_frames();
    let one_completed = one_frames
        .into_iter()
        .filter_map(|f| match f {
            KernelToDomainFrame::Receipt(r)
                if r.message_id == query_one_ids[0] && r.status == ContractStatus::Completed =>
            {
                r.data
            }
            _ => None,
        })
        .next()
        .expect("Expected Completed receipt for single-target QueryHandlers");

    assert_eq!(one_completed["count"], json!(1));
    assert_eq!(
        one_completed["description"],
        json!("扣减指定 SKU 的库存并持久化最新库存快照")
    );

    kernel.supervisor().stop_process_domain("order");
    kernel.supervisor().stop_process_domain("inventory");
}




