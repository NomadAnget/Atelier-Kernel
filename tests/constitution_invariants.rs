//! 宪法派生不变量 —— 重构的测试预言。
//!
//! 旧测试固化了实现事故（重启后查旧结果、无绑定回执回落到当前代际等），不作为重构判据。
//! 这里只断言宪法语义（见 CONSTITUTION.md C2/C3 与 docs/KERNEL_CONVERGENCE.md §4）：
//!   I1 唯一性：一条消息至多一份可读结果；
//!   I2 单调性：回执只沿 Sent→Received→Started→终态推进，终态恰一次且不可回退；
//!   I3 有界性：非持久终态不留常驻状态，提交去重索引有界。

use atelier_kernel::{
    AtelierKernel, ContractStatus as S, KernelToDomainFrame as F, MSG_DELIVERY_VOLATILE,
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
