use std::time::Duration;

use atelier_kernel::{
    AtelierKernel, ContractStatus, DomainProcessSpec, DomainStatus, KernelToDomainFrame,
    MSG_STORAGE_DELETE, MSG_STORAGE_QUERY,
};
use serde_json::json;

/// 1. Tests domain handler registration, `<domain>.<handler>` routing, and `TargetInvalid` pre-send interception.
#[test]
fn test_registration_routing_and_target_invalid_interception() {
    let kernel = AtelierKernel::new_in_memory().unwrap();
    let sender = kernel.attach_domain("gateway");
    let worker = kernel.attach_domain("billing");

    // Before registration, `billing` is in `Spawning` state -> sending to `billing.charge` must return `TargetInvalid` without sending.
    let pre_msg_id = sender.send(
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
    let multi_msg_id = sender.send(
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
            assert_eq!(msg.message_id, multi_msg_id);
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
    let msg_id_1 = order.send_with_seq(
        42,
        vec!["inventory.reserve".to_string()],
        "ReserveStock",
        json!({"warehouse": "W1"}),
        json!({"sku": "SKU-9", "qty": 2}),
    );

    // Level 1 Idempotency: Resending with the same `(domain="order", seq=42)` must NOT re-deliver to `inventory`
    // and must return the exact same `message_id`!
    let msg_id_2 = order.send_with_seq(
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
    inventory.report_receipt(&msg_id_1, "inventory.reserve", ContractStatus::Received, None);
    inventory.report_receipt(&msg_id_1, "inventory.reserve", ContractStatus::Received, None);

    // Inventory reports `Started`
    inventory.report_receipt(&msg_id_1, "inventory.reserve", ContractStatus::Started, None);

    // Inventory reports `Completed` with output data, plus a late `Started` and duplicate `Completed`
    inventory.report_receipt(
        &msg_id_1,
        "inventory.reserve",
        ContractStatus::Completed,
        Some(json!({"reserved_id": "RES-777"})),
    );
    inventory.report_receipt(&msg_id_1, "inventory.reserve", ContractStatus::Started, None);
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

    // 4. Cross-domain isolation check: `analytics` domain queries for `u_1001` (even trying to pass `"domain": "user"`)
    other_domain.storage_query(json!({
        "domain": "user",
        "record_id": "u_1001"
    }));
    let other_completed = other_domain
        .drain_frames()
        .into_iter()
        .find_map(|f| match f {
            KernelToDomainFrame::Receipt(r) if r.status == ContractStatus::Completed => r.data,
            _ => None,
        })
        .unwrap();
    assert_eq!(
        other_completed["count"], 0,
        "Physical pipe identity must isolate per-domain tables"
    );

    // 5. Delete record via `StorageDelete` message
    user_domain.storage_delete(json!({"record_id": "u_1001"}));
    let delete_completed = user_domain
        .drain_frames()
        .into_iter()
        .find_map(|f| match f {
            KernelToDomainFrame::Receipt(r)
                if r.message_type == MSG_STORAGE_DELETE && r.status == ContractStatus::Completed =>
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

    // Spawn a real external OS process (using `/bin/sh -c`) that speaks the Atelier Kernel JSON-Lines protocol over stdin/stdout!
    // It registers handler `echo`, reads one delivered message from stdin, emits Received -> Started -> Completed, and then exits.
    let shell_script = r#"
        echo '{"frame_type":"send","seq":1,"targets":["kernel.registry"],"message_type":"RegisterHandlers","params":{"handlers":["echo"]},"data":{}}'
        while IFS= read -r line; do
            case "$line" in
                *'"frame_type":"deliver"'*)
                    msg_id=$(echo "$line" | sed -n 's/.*"message_id":"\([^"]*\)".*/\1/p')
                    echo "{\"frame_type\":\"receipt\",\"message_id\":\"$msg_id\",\"target\":\"ext_sh.echo\",\"status\":\"received\",\"data\":null}"
                    echo "{\"frame_type\":\"receipt\",\"message_id\":\"$msg_id\",\"target\":\"ext_sh.echo\",\"status\":\"started\",\"data\":null}"
                    echo "{\"frame_type\":\"receipt\",\"message_id\":\"$msg_id\",\"target\":\"ext_sh.echo\",\"status\":\"completed\",\"data\":{\"echoed_by\":\"os_pipe_sh\"}}"
                    break
                    ;;
            esac
        done
    "#;

    let pid = kernel
        .spawn_process_domain(DomainProcessSpec {
            domain_id: "ext_sh".to_string(),
            command: "/bin/sh".to_string(),
            args: vec!["-c".to_string(), shell_script.to_string()],
            working_dir: None,
        })
        .expect("Failed to spawn external shell domain process");
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
    let msg_id = caller.send(
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

    // Also verify stale raw pipe line from Instance 1 is ignored
    kernel.router().handle_raw_pipe_line_for_instance(
        "worker",
        inst_1,
        r#"{"frame_type":"send","seq":99,"targets":["kernel.registry"],"message_type":"UnregisterHandlers","params":{},"data":{}}"#,
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

