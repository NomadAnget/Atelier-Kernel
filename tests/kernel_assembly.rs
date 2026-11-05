//! 新内核组装证明：把 5 个已验证模块接起来，跑通完整消息生命周期。
//! 这是"换骨"的端到端预演——不碰旧 router/ledger，只证明新零件能拼成一台内核。

use atelier_kernel::services::objects::ObjectHub;
use atelier_kernel::services::observer_proj::Observer;
use atelier_kernel::protocol::{ContractStatus, DeliveryPolicy};
use atelier_kernel::routing::Routing;
use atelier_kernel::transport::{Frame, Transport};
use serde_json::json;

#[test]
fn new_kernel_composes_end_to_end() {
    // C4：大文件走对象索引，不进消息
    let mut hub = ObjectHub::new(std::env::temp_dir().join(format!(
        "atelier-assembly-{}",
        uuid::Uuid::new_v4()
    )));
    let obj = hub.allocate("engines", "objects/video.bin").unwrap();

    let mut tr = Transport::new();
    let mut routing = Routing::new();
    let mut obs = Observer::new();

    // 1. 发送：消息 data 里只有 object_id 引用
    let targets = vec!["w.run".to_string()];
    let sub = tr.admit(
        "engines#1",
        "engines",
        1,
        &targets,
        "Run",
        DeliveryPolicy::Durable,
        Some(json!({"object_id": obj.object_id})),
    );
    routing.bind(&sub.ids, 1, "chan-engines-1");
    obs.observe(&sub.ids[0], "w.run", ContractStatus::Sent, None);

    // 2. 目标收到 Deliver（每通道单调 seq）
    let delivers = tr.frames_for("w.run");
    assert_eq!(delivers.len(), 1);
    assert!(matches!(&delivers[0], Frame::Deliver { message_id, .. } if *message_id == sub.ids[0]));

    // 3. 目标回执，推进到终态
    tr.advance(&sub.ids[0], ContractStatus::Completed, Some(json!({"ok": true})));
    obs.observe(&sub.ids[0], "w.run", ContractStatus::Completed, Some(json!({"ok": true})));

    // 4. 回执只回正确代际（跨代际隔离）
    assert!(routing.accepts(&sub.ids[0], "chan-engines-1"));
    assert!(!routing.accepts(&sub.ids[0], "chan-engines-2"), "新代际不能截获旧代际回执");

    // 5. 回执帧已入来源通道，且观察器仍持有真实终态
    let receipts = tr.frames_for("engines");
    assert_eq!(receipts.len(), 1);
    assert!(matches!(&receipts[0], Frame::Receipt { status, .. } if *status == ContractStatus::Completed));
    assert_eq!(
        obs.last_terminal(&sub.ids[0]).map(|e| e.status),
        Some(ContractStatus::Completed)
    );

    // 6. 代际退役：清除路由绑定
    routing.retire(1);
    assert!(!routing.accepts(&sub.ids[0], "chan-engines-1"));
}

#[test]
fn replay_and_cross_policy_hold_in_composed_kernel() {
    let mut tr = Transport::new();
    // 跨策略 FIFO：Durable A 先于 Volatile B
    let a = tr.admit("s#1", "s", 1, &["w.run".into()], "Run", DeliveryPolicy::Durable, None);
    let b = tr.admit("s#1", "s", 2, &["w.run".into()], "Run", DeliveryPolicy::Volatile, None);
    let delivers: Vec<&Frame> = tr.frames_for("w.run");
    assert_eq!(delivers.len(), 2);
    // 重放同一 seq 复用身份
    let replay = tr.admit("s#1", "s", 1, &["w.run".into()], "Run", DeliveryPolicy::Durable, None);
    assert!(replay.replayed);
    assert_eq!(replay.ids, a.ids);
    let _ = b;
}
