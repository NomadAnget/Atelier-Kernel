use atelier_kernel::{AtelierKernel, ContractStatus as S, KernelToDomainFrame as F};
use serde_json::json;

#[test]
fn fanout_has_distinct_ids_shared_logical_identity_and_ordered_replay() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let sender = k.attach_domain("sender");
    let worker = k.attach_domain("worker");
    worker.register_handlers(&["a", "b"]);
    worker.drain_frames();
    let targets = vec![
        "worker.a".into(),
        "worker.b".into(),
        "missing.x".into(),
        "worker.a".into(),
    ];
    let ids = sender.send_with_seq(42, targets.clone(), "Run", json!({}), json!({"value":7}));
    assert_eq!(ids.len(), 3);
    assert_eq!(
        ids.iter().collect::<std::collections::HashSet<_>>().len(),
        3
    );
    let deliveries: Vec<_> = worker
        .drain_frames()
        .into_iter()
        .map(|f| match f {
            F::Deliver(m) => m,
            _ => panic!(),
        })
        .collect();
    assert_eq!(deliveries.len(), 2);
    let logical = &deliveries[0].logical_id;
    assert_ne!(logical, &ids[0]);
    for (i, m) in deliveries.iter().enumerate() {
        assert_eq!(m.message_id, ids[i]);
        assert_eq!(&m.logical_id, logical);
        assert_eq!(m.source_domain, "sender");
        assert_eq!(m.targets.len(), 3);
        assert_eq!(m.data, json!({"value":7}));
        let wire = serde_json::to_value(m).unwrap();
        assert_eq!(wire["logical_id"], *logical);
    }
    let receipts: Vec<_> = sender
        .drain_frames()
        .into_iter()
        .map(|f| match f {
            F::Receipt(r) => r,
            _ => panic!(),
        })
        .collect();
    assert_eq!(
        receipts
            .iter()
            .map(|r| r.message_id.clone())
            .collect::<Vec<_>>(),
        ids
    );
    assert_eq!(receipts[2].status, S::TargetInvalid);
    assert!(receipts.iter().all(|r| &r.logical_id == logical));
    worker.report_receipt(
        &ids[1],
        "worker.b",
        S::Completed,
        Some(json!({"second":true})),
    );
    sender.drain_frames();
    let replay = sender.send_with_seq(42, targets, "Run", json!({}), json!({"value":7}));
    assert_eq!(replay, ids);
    assert!(worker.drain_frames().is_empty());
    let replay: Vec<_> = sender
        .drain_frames()
        .into_iter()
        .map(|f| match f {
            F::Receipt(r) => r,
            _ => panic!(),
        })
        .collect();
    assert_eq!(
        replay
            .iter()
            .map(|r| r.message_id.clone())
            .collect::<Vec<_>>(),
        ids
    );
    assert_eq!(replay[0].status, S::Sent);
    assert_eq!(replay[1].status, S::Completed);
    assert_eq!(replay[2].status, S::TargetInvalid);
    assert!(replay.iter().all(|r| &r.logical_id == logical));
    let events = k.observer().snapshot();
    assert!(
        events
            .iter()
            .filter(|e| ids.contains(&e.message_id))
            .all(|e| &e.logical_id == logical)
    );
}

#[test]
fn child_receipts_cannot_cross_targets_or_overwrite_terminal_results() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let s = k.attach_domain("sender");
    let w = k.attach_domain("worker");
    let rogue = k.attach_domain("rogue");
    w.register_handlers(&["a", "b"]);
    w.drain_frames();
    let ids = s.send(
        vec!["worker.a".into(), "worker.b".into()],
        "Run",
        json!({}),
        json!({}),
    );
    w.drain_frames();
    s.drain_frames();
    w.report_receipt(
        &ids[0],
        "worker.b",
        S::Completed,
        Some(json!("wrong target")),
    );
    rogue.report_receipt(
        &ids[0],
        "worker.a",
        S::Completed,
        Some(json!("wrong source")),
    );
    assert!(s.drain_frames().is_empty());
    w.report_receipt(&ids[0], "worker.a", S::Received, None);
    w.report_receipt(&ids[0], "worker.a", S::Received, None);
    w.report_receipt(&ids[0], "worker.a", S::Completed, Some(json!("original")));
    w.report_receipt(&ids[0], "worker.a", S::Started, None);
    w.report_receipt(&ids[0], "worker.a", S::Completed, Some(json!("overwrite")));
    let stages: Vec<_> = s
        .drain_frames()
        .into_iter()
        .map(|f| match f {
            F::Receipt(r) => r.status,
            _ => panic!(),
        })
        .collect();
    assert_eq!(stages, vec![S::Received, S::Completed]);
    k.router().mark_domain_faulted("worker", "test crash");
    let failed = s.drain_frames();
    assert_eq!(failed.len(), 1);
    match &failed[0] {
        F::Receipt(r) => {
            assert_eq!(r.message_id, ids[1]);
            assert_eq!(r.status, S::ExecutionFailed);
        }
        _ => panic!(),
    }
    assert_eq!(
        k.ledger().get_record(&ids[0], "worker.a").unwrap().data,
        Some(json!("original"))
    );
}

#[test]
fn same_sequence_is_session_scoped_and_ids_survive_instance_collision_risk() {
    let mut all = std::collections::HashSet::new();
    for _ in 0..8 {
        let k = AtelierKernel::new_in_memory().unwrap();
        let s = k.attach_domain("sender");
        let ids = s.send_with_seq(9, vec!["missing.a".into()], "Run", json!({}), json!({}));
        assert!(all.insert(ids[0].clone()));
        assert_eq!(
            ids,
            s.send_with_seq(9, vec!["missing.a".into()], "Run", json!({}), json!({}))
        );
        let next = k.attach_domain("sender");
        let new_ids = next.send_with_seq(9, vec!["missing.a".into()], "Run", json!({}), json!({}));
        assert_ne!(ids, new_ids);
        assert!(all.insert(new_ids[0].clone()));
        let zero1 = next.send_with_seq(0, vec!["missing.a".into()], "Run", json!({}), json!({}));
        let zero2 = next.send_with_seq(0, vec!["missing.a".into()], "Run", json!({}), json!({}));
        assert_ne!(zero1, zero2);
    }
}

#[test]
fn malformed_submission_has_two_ids_and_replays_without_new_identity() {
    let k = AtelierKernel::new_in_memory().unwrap();
    let s = k.attach_domain("sender");
    let first = s.send_with_seq(17, vec![], "Run", json!({}), json!({}));
    let frames = s.drain_frames();
    assert_eq!(frames.len(), 1);
    let logical = match &frames[0] {
        F::Receipt(r) => {
            assert_eq!(r.status, S::ContractInvalid);
            assert_ne!(r.logical_id, r.message_id);
            r.logical_id.clone()
        }
        _ => panic!(),
    };
    assert_eq!(
        first,
        s.send_with_seq(17, vec![], "Run", json!({}), json!({}))
    );
    match &s.drain_frames()[0] {
        F::Receipt(r) => assert_eq!(r.logical_id, logical),
        _ => panic!(),
    };
}
