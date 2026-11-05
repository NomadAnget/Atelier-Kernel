//! Nonpersistent ledger retention acceptance.
//!
//! The nonpersistent ledger must stay bounded no matter how much history flows through it:
//! completed `Volatile` / `Ephemeral` messages are never retained, `null` payloads must not
//! escape the capacity accounting, and identity/session indexes must be reclaimed once the
//! result has been published.

use atelier_kernel::ledger::KernelLedger;
use atelier_kernel::protocol::ContractStatus;
use serde_json::{Value, json};

fn deliver(ledger: &KernelLedger, seq: u64, kind: &str, data: Option<Value>) {
    let (_, ids, _) = ledger.allocate_submission("sender", seq, 1);
    ledger.record_initial_target_state(
        &ids[0],
        seq,
        "sender",
        "worker.run",
        kind,
        ContractStatus::Sent,
        None,
    );
    ledger.advance_status("worker", &ids[0], "worker.run", ContractStatus::Received, None);
    ledger.advance_status("worker", &ids[0], "worker.run", ContractStatus::Started, None);
    ledger.advance_status("worker", &ids[0], "worker.run", ContractStatus::Completed, data);
}

fn report(label: &str, ledger: &KernelLedger) -> Value {
    let counts = ledger.retention_counts();
    println!(
        "{label}: resident={} submissions={} identities={} logical_groups={}",
        counts["resident_records"],
        counts["submissions"],
        counts["child_links"],
        counts["logical_groups"]
    );
    counts
}

/// 非持久账本被填满时按最旧淘汰，且被淘汰消息的身份索引必须一起回收，否则 MAX_LIVE 仍会被耗尽。
#[test]
fn eviction_reclaims_identity_rows() {
    let ledger = KernelLedger::new();
    let payload = json!({"blob": "x".repeat(96 * 1024)});
    for seq in 1..=800u64 {
        let (_, ids, _) = ledger.allocate_submission("sender", seq, 1);
        ledger.record_initial_target_state(
            &ids[0],
            seq,
            "sender",
            "kernel.telemetry",
            "Ephemeral",
            ContractStatus::Sent,
            Some(payload.clone()),
        );
    }
    let counts = report("eviction", &ledger);
    assert!(
        counts["nonpersistent_evicted"].as_u64().unwrap() > 0,
        "expected eviction under 64MB pressure: {counts}"
    );
    assert_eq!(
        counts["child_links"].as_u64().unwrap(),
        counts["resident_records"].as_u64().unwrap(),
        "evicted messages must not leave identity rows behind: {counts}"
    );
    assert!(
        counts["submissions"].as_i64().unwrap() <= 2_048,
        "evicted submissions must not keep their session index: {counts}"
    );
}

/// 入口即终态的非持久消息（例如发往未注册目标的 Volatile）不得留在内存账本里。
#[test]
fn terminal_at_ingress_volatile_does_not_leak() {
    let ledger = KernelLedger::new();
    for seq in 1..=1_000u64 {
        let (_, ids, _) = ledger.allocate_submission("sender", seq, 1);
        ledger.record_initial_target_state(
            &ids[0],
            seq,
            "sender",
            "worker.missing",
            "Volatile",
            ContractStatus::TargetInvalid,
            Some(json!({"error": "no_handler"})),
        );
    }
    let counts = report("terminal-at-ingress", &ledger);
    assert_eq!(counts["resident_records"], 0, "{counts}");
    assert_eq!(counts["volatile_records"], 0, "{counts}");
}

/// 一次 Ephemeral 查询完成，不能清掉其他来源用于重放的 Volatile 会话窗口。
#[test]
fn ephemeral_completion_does_not_wipe_volatile_replay_window() {
    let ledger = KernelLedger::new();
    let (logical, ids, _) = ledger.allocate_submission("sender", 5, 1);
    ledger.record_initial_target_state(
        &ids[0],
        5,
        "sender",
        "worker.run",
        "Volatile",
        ContractStatus::Sent,
        None,
    );
    ledger.advance_status(
        "worker",
        &ids[0],
        "worker.run",
        ContractStatus::Completed,
        Some(json!({"ok":true})),
    );
    assert_eq!(
        ledger.allocate_submission("sender", 5, 1),
        (logical.clone(), ids.clone(), true)
    );

    // 另一个来源完成一次 Ephemeral 查询（内核基础设施查询）
    let (_, qids, _) = ledger.allocate_submission("querier", 1, 1);
    ledger.record_initial_target_state(
        &qids[0],
        1,
        "querier",
        "kernel.runtime",
        "QueryRuntime",
        ContractStatus::Sent,
        None,
    );
    ledger.advance_status(
        "kernel",
        &qids[0],
        "kernel.runtime",
        ContractStatus::Completed,
        Some(json!({"ok":true})),
    );

    assert_eq!(
        ledger.allocate_submission("sender", 5, 1),
        (logical, ids, true),
        "an unrelated ephemeral completion must not wipe the volatile replay window"
    );
}

/// 连续发送 10 万条已完成 Volatile 消息：账本记录数不随总发送量线性增长。
#[test]
fn volatile_history_does_not_grow_ledger() {
    let ledger = KernelLedger::new();
    for seq in 1..=100_000u64 {
        deliver(&ledger, seq, "Volatile", None);
        if seq % 20_000 == 0 {
            report(&format!("volatile seq={seq}"), &ledger);
        }
    }
    let counts = report("volatile final", &ledger);
    assert_eq!(counts["resident_records"], 0, "{counts}");
    assert!(
        counts["submissions"].as_i64().unwrap() <= 2_048,
        "replay window must stay bounded: {counts}"
    );
    assert_eq!(counts["child_links"], 0, "{counts}");
    assert_eq!(counts["logical_groups"], 0, "{counts}");
}

/// 持续发送 data=null 的消息：不出现容量统计遗漏导致的无界增长。
#[test]
fn null_data_messages_do_not_leak_capacity() {
    let ledger = KernelLedger::new();
    for seq in 1..=50_000u64 {
        deliver(&ledger, seq, "Volatile", None);
    }
    let counts = report("null data final", &ledger);
    assert_eq!(counts["resident_records"], 0, "{counts}");
    assert!(
        counts["submissions"].as_i64().unwrap() <= 2_048,
        "replay window must stay bounded: {counts}"
    );
}

/// 在消息完成、回执发布后检查状态：不再需要的结果、身份、索引均被回收。
#[test]
fn terminal_results_identities_indexes_are_reclaimed() {
    let ledger = KernelLedger::new();
    for seq in 1..=1_000u64 {
        deliver(&ledger, seq, "Volatile", Some(json!({"payload":"x".repeat(64)})));
    }
    let counts = report("reclaimed", &ledger);
    assert_eq!(counts["resident_records"], 0, "{counts}");
    assert_eq!(counts["child_links"], 0, "{counts}");
    assert_eq!(counts["logical_groups"], 0, "{counts}");
    assert!(
        counts["submissions"].as_i64().unwrap() <= 2_048,
        "replay window must stay bounded: {counts}"
    );
}

/// 持续运行并混合 Durable / Volatile / Ephemeral：非持久历史堆积不影响正常 Durable 投递。
#[test]
fn mixed_policies_do_not_block_durable() {
    let ledger = KernelLedger::new();
    for seq in 1..=20_000u64 {
        match seq % 3 {
            0 => deliver(&ledger, seq, "Durable", Some(json!({"n": seq}))),
            1 => deliver(&ledger, seq, "Volatile", None),
            _ => deliver(&ledger, seq, "Ephemeral", None),
        }
    }
    let counts = report("mixed final", &ledger);
    // 非持久历史不得堆积
    assert_eq!(counts["resident_records"], 0, "{counts}");
    assert!(
        counts["submissions"].as_i64().unwrap() <= 2_048,
        "replay window must stay bounded: {counts}"
    );
    // 混合压力之后，Durable 仍然可以正常投递
    let (_, ids, _) = ledger.allocate_submission("sender", 999_999, 1);
    ledger.record_initial_target_state(
        &ids[0],
        999_999,
        "sender",
        "worker.run",
        "Durable",
        ContractStatus::Sent,
        None,
    );
    ledger.advance_status(
        "worker",
        &ids[0],
        "worker.run",
        ContractStatus::Completed,
        Some(json!({"ok":true})),
    );
    let record = ledger
        .get_persisted_record(&ids[0])
        .expect("durable delivery must survive under nonpersistent history pressure");
    assert_eq!(record.status, ContractStatus::Completed);
}
