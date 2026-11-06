//! 宪法派生不变量 —— 重构的测试预言。
//!
//! 旧测试固化了实现事故（重启后查旧结果、无绑定回执回落到当前代际等），不作为重构判据。
//! 这里只断言宪法语义（见 CONSTITUTION.md C2/C3 与 docs/KERNEL_CONVERGENCE.md §4）：
//!   I1 唯一性：一条消息至多一份可读结果；
//!   I2 单调性：回执只沿 Sent→Received→Started→终态推进，终态恰一次且不可回退；
//!   I3 有界性：非持久终态不留常驻状态，提交去重索引有界。

use atelier_kernel::{
    AtelierKernel, ContractStatus as S, DeliveryPolicy, DomainToKernelFrame, KernelToDomainFrame as F,
    MSG_DELIVERY_VOLATILE, OutboundMessage,
};
use serde_json::json;
use std::time::Duration;

#[test]
fn i2_stage_monotonic_single_terminal() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let sender = k.attach_domain("i2_sender");
    let worker = k.attach_domain("i2_worker");
    worker.register_handlers(&["run"]);
    worker.drain_frames();

    let id = sender.send_one(vec!["i2_worker.run".into()], "Durable", json!({}), json!({}));
    let F::Deliver(m) = worker.recv_frame_timeout(Duration::from_secs(5)).unwrap() else {
        panic!("expected delivery")
    };
    // 乱序、回退、终态覆盖、重复，全部应被吸收
    worker.report_receipt(&m.message_id, &m.target, S::Started, None);
    worker.report_receipt(&m.message_id, &m.target, S::Received, None);
    worker.report_receipt(&m.message_id, &m.target, S::Completed, Some(json!({"ok": true})));
    worker.report_receipt(&m.message_id, &m.target, S::ExecutionFailed, None);
    worker.report_receipt(&m.message_id, &m.target, S::Completed, Some(json!({"ok": true})));

    let mut stages = Vec::new();
    let mut terminals = 0;
    while let Ok(frame) = sender.recv_frame_timeout(Duration::from_millis(300)) {
        if let F::Receipt(r) = frame {
            if r.message_id == id {
                if r.status.is_terminal() {
                    terminals += 1;
                }
                stages.push(r.status);
            }
        }
    }
    assert_eq!(terminals, 1, "exactly one terminal receipt: {stages:?}");
    assert_eq!(stages.last(), Some(&S::Completed));
    for w in stages.windows(2) {
        assert!(
            w[1].stage_rank() >= w[0].stage_rank(),
            "regression {w:?}"
        );
    }
}

#[test]
fn i1_durable_result_unique_and_queryable() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let sender = k.attach_domain("i1_sender");
    let worker = k.attach_domain("i1_worker");
    worker.register_handlers(&["run"]);
    worker.drain_frames();

    let before = k.ledger().retention_counts();
    let id = sender.send_one(vec!["i1_worker.run".into()], "Durable", json!({}), json!({}));
    let F::Deliver(m) = worker.recv_frame_timeout(Duration::from_secs(5)).unwrap() else {
        panic!("expected delivery")
    };
    worker.report_receipt(&m.message_id, &m.target, S::Completed, Some(json!({"kept": 1})));
    while let Ok(frame) = sender.recv_frame_timeout(Duration::from_millis(300)) {
        if matches!(frame, F::Receipt(_)) {}
    }

    let after = k.ledger().retention_counts();
    // Durable 结果可读且唯一
    let rec = k.ledger().get_persisted_record(&id).expect("durable result must survive");
    assert_eq!(rec.status, S::Completed);
    assert_eq!(rec.data, Some(json!({"kept": 1})));
    // 内存身份已回收（有界），且不因业务结果变大
    assert_eq!(after["child_links"], before["child_links"], "{before} -> {after}");
    assert_eq!(after["durable_records"], 0);
}

#[test]
fn sdk_declared_policy_overrides_string_inference() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let sender = k.attach_domain("decl_sender");
    let worker = k.attach_domain("decl_worker");
    worker.register_handlers(&["run"]);
    worker.drain_frames();

    // message_type "Run" 会被字符串推断为 Durable；SDK 声明 Volatile 必须覆盖它。
    let outbound = OutboundMessage {
        seq: 1,
        targets: vec!["decl_worker.run".into()],
        message_type: "Run".into(),
        delivery: Some(DeliveryPolicy::Volatile),
        params: json!({}),
        data: json!({}),
    };
    let ids = k
        .router()
        .handle_frame_for_instance(
            "decl_sender",
            sender.instance_id(),
            DomainToKernelFrame::Send(outbound),
        )
        .expect("valid instance");
    let id = &ids[0];
    assert!(
        k.ledger().get_persisted_record(id).is_none(),
        "declared Volatile must not be persisted"
    );

    let F::Deliver(m) = worker.recv_frame_timeout(Duration::from_secs(5)).unwrap() else {
        panic!("expected delivery")
    };
    worker.report_receipt(&m.message_id, &m.target, S::Completed, Some(json!({"ok": true})));
    while let Ok(frame) = sender.recv_frame_timeout(Duration::from_millis(300)) {
        let _ = frame;
    }

    let counts = k.ledger().retention_counts();
    assert_eq!(counts["durable_records"], 0, "{counts}");
    assert_eq!(counts["volatile_records"], 0, "{counts}");
    assert!(k.ledger().get_persisted_record(id).is_none());
}

#[test]
fn i3_nondurable_terminal_leaves_no_resident_state() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let sender = k.attach_domain("i3_sender");
    let worker = k.attach_domain("i3_worker");
    worker.register_handlers(&["run"]);
    worker.drain_frames();

    for seq in 1..=100u64 {
        let id = sender.send_one_with_seq(
            seq,
            vec!["i3_worker.run".into()],
            MSG_DELIVERY_VOLATILE,
            json!({}),
            json!({}),
        );
        let F::Deliver(m) = worker.recv_frame_timeout(Duration::from_secs(5)).unwrap() else {
            panic!("expected delivery")
        };
        worker.report_receipt(&m.message_id, &m.target, S::Completed, Some(json!({"n": seq})));
        assert_eq!(m.message_id, id);
        while let Ok(frame) = sender.recv_frame_timeout(Duration::from_millis(20)) {
            if matches!(frame, F::Receipt(_)) {}
        }
    }
    let counts = k.ledger().retention_counts();
    // 非持久终态不留常驻账本行，身份索引有界
    assert_eq!(counts["resident_records"], 0, "{counts}");
    assert_eq!(counts["volatile_records"], 0, "{counts}");
    assert_eq!(counts["child_links"], 0, "{counts}");
}

fn scratch_db(label: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!(
        "atelier-{label}-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("ledger.db");
    (root, path)
}

/// I1 的存储级断言：同一个 message_id 绝不能同时出现在 `messages`（在途）与 `audit`（历史）。
#[test]
fn i1_live_and_historical_rows_never_coexist_on_disk() {
    let (root, path) = scratch_db("i1-disk");
    let id;
    {
        let k = AtelierKernel::open_with_db(&path).unwrap();
        let sender = k.attach_domain("i1_disk_sender");
        let worker = k.attach_domain("i1_disk_worker");
        worker.register_handlers(&["run"]);
        worker.drain_frames();
        id = sender.send_one_with_seq(
            1,
            vec!["i1_disk_worker.run".into()],
            MSG_DELIVERY_VOLATILE,
            json!({}),
            json!({}),
        );
        let F::Deliver(m) = worker.recv_frame_timeout(Duration::from_secs(5)).unwrap() else {
            panic!("expected delivery")
        };
        assert_eq!(m.message_id, id);
        worker.report_receipt(&m.message_id, &m.target, S::Completed, Some(json!({"ok": 1})));
        let mut done = false;
        while let Ok(frame) = sender.recv_frame_timeout(Duration::from_secs(5)) {
            if matches!(&frame, F::Receipt(r) if r.status == S::Completed) {
                done = true;
                break;
            }
        }
        assert!(done, "terminal receipt never arrived");

        // Durable 提交走一条独立的 Durable 消息，确保 audit 里有真实历史行。
        let durable_id = sender.send_one_with_seq(
            2,
            vec!["i1_disk_worker.run".into()],
            "Run",
            json!({}),
            json!({}),
        );
        let F::Deliver(m) = worker.recv_frame_timeout(Duration::from_secs(5)).unwrap() else {
            panic!("expected durable delivery")
        };
        worker.report_receipt(&durable_id, &m.target, S::Completed, Some(json!({"ok": 2})));
        let mut done = false;
        while let Ok(frame) = sender.recv_frame_timeout(Duration::from_secs(5)) {
            if matches!(&frame, F::Receipt(r) if r.status == S::Completed && r.message_id == durable_id) {
                done = true;
                break;
            }
        }
        assert!(done, "durable terminal receipt never arrived");
        drop(k);
    }

    let db = rusqlite::Connection::open(&path).unwrap();
    let overlap: i64 = db
        .query_row(
            "SELECT count(*) FROM messages WHERE message_id IN (SELECT message_id FROM audit)",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(overlap, 0, "an id must never be both live and historical");
    let audited: i64 = db
        .query_row("SELECT count(*) FROM audit WHERE message_id=?1", [&id], |r| r.get(0))
        .unwrap_or(0);
    let live: i64 = db
        .query_row("SELECT count(*) FROM messages WHERE message_id=?1", [&id], |r| r.get(0))
        .unwrap();
    assert_eq!(live, 0, "terminal row must leave the live table");
    // volatile 终态本就既不留活行也不进历史
    assert_eq!(audited, 0, "volatile must never audit");
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 宪法 C3:54：历史终态结果留在 SQLite，按需按 id / logical 读取，不进入核的常驻状态。
///
/// 用一份 5 万行历史的账本启动：核内驻留记录必须为 0（历史不复活成状态），
/// 而历史仍能通过 `idx_audit_message` / `idx_audit_logical` 按需点查。
#[test]
fn historical_results_stay_on_disk_and_are_read_by_id() {
    let (root, path) = scratch_db("boot-history");
    drop(AtelierKernel::open_with_db(&path).unwrap()); // 建表
    {
        let db = rusqlite::Connection::open(&path).unwrap();
        db.execute_batch("BEGIN IMMEDIATE").unwrap();
        {
            let mut stmt = db
                .prepare(
                    "INSERT INTO audit(message_id,logical_id,seq,source_domain,target,message_type,policy,status,data,created_at) \
                     VALUES(?1,?2,?3,'d','w.run','Run','durable','completed',?4,0)",
                )
                .unwrap();
            let payload = "{\"payload\":\"x\"}";
            for i in 0..50_000i64 {
                stmt.execute(rusqlite::params![format!("m{i}"), format!("l{i}"), i, payload])
                    .unwrap();
            }
        }
        db.execute_batch("COMMIT").unwrap();
    }

    let k = AtelierKernel::open_with_db(&path).unwrap();
    let counts = k.ledger().retention_counts();
    assert_eq!(counts["resident_records"], 0, "history must not resurrect: {counts}");
    assert_eq!(counts["durable_records"], 0, "history must not resurrect: {counts}");

    // 按 message_id 点查
    let rec = k
        .ledger()
        .get_persisted_record("m42424")
        .expect("historical result must be readable by id");
    assert_eq!(rec.status, S::Completed);
    assert_eq!(rec.logical_id, "l42424");

    // 按 message_id 读回执（走 read_audit）
    assert_eq!(k.ledger().receipts_for_message("m42424").len(), 1);
    // 按 logical_id 读整条链路（走 idx_audit_logical）
    assert_eq!(k.ledger().receipts_for_logical("l42424").len(), 1);
    // 不存在的 id 不是错误，只是没有结果
    assert!(k.ledger().get_persisted_record("m-missing").is_none());

    drop(k);
    let _ = std::fs::remove_dir_all(root);
}
