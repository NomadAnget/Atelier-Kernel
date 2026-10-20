use atelier_kernel::{
    AtelierKernel, ContractStatus, DeliveryPolicy, KernelToDomainFrame, KERNEL_OBSERVER_TARGET,
    KERNEL_RUNTIME_TARGET, MSG_QUERY_DOMAIN_ASSEMBLY, MSG_QUERY_OBSERVER, MSG_QUERY_RUNTIME,
};
use serde_json::{json, Value};

#[test]
fn observer_queries_do_not_recursively_capture_previous_snapshots() {
    let kernel = AtelierKernel::new_in_memory().unwrap();
    kernel.logger().set_console_enabled(false);
    let client = kernel.attach_domain("observer_memory_probe");
    let mut sizes = Vec::new();
    for _ in 0..8 {
        let ids = client.send(
            vec![KERNEL_OBSERVER_TARGET.to_string()],
            MSG_QUERY_OBSERVER,
            json!({"limit":32}),
            json!({}),
        );
        let frames = client.drain_frames();
        let stages: Vec<_> = frames
            .iter()
            .filter_map(|frame| match frame {
                KernelToDomainFrame::Receipt(receipt) if receipt.message_id == ids[0] => {
                    Some(receipt.status)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            stages,
            vec![
                ContractStatus::Sent,
                ContractStatus::Received,
                ContractStatus::Started,
                ContractStatus::Completed
            ]
        );
        let data = frames
            .into_iter()
            .find_map(|frame| match frame {
                KernelToDomainFrame::Receipt(receipt)
                    if receipt.message_id == ids[0]
                        && receipt.status == ContractStatus::Completed =>
                {
                    receipt.data
                }
                _ => None,
            })
            .expect("query must return its real completed result");
        sizes.push(serde_json::to_vec(&data).unwrap().len());
        assert!(kernel
            .ledger()
            .get_record(&ids[0], KERNEL_OBSERVER_TARGET)
            .is_none());
        assert!(kernel.ledger().get_persisted_record(&ids[0]).is_none());
    }
    eprintln!("eight bounded query response sizes: {sizes:?}");
    let history = serde_json::to_value(kernel.observer().snapshot()).unwrap();
    let nested = history.as_array().unwrap().iter().any(|event| {
        event["target"] == KERNEL_OBSERVER_TARGET
            && event
                .get("details")
                .and_then(|details| details.get("events"))
                .is_some_and(Value::is_array)
    });
    assert!(
        !nested,
        "observer snapshots recursively captured previous query results; sizes={sizes:?}"
    );
    assert!(
        sizes.last().unwrap() < &32_768,
        "bounded metadata queries must remain small"
    );
}

#[test]
fn read_only_runtime_and_observer_queries_are_transient_without_changing_business_delivery() {
    for (target, kind) in [
        (KERNEL_OBSERVER_TARGET, MSG_QUERY_OBSERVER),
        (KERNEL_RUNTIME_TARGET, MSG_QUERY_RUNTIME),
        (KERNEL_RUNTIME_TARGET, MSG_QUERY_DOMAIN_ASSEMBLY),
    ] {
        assert_eq!(
            DeliveryPolicy::resolve(target, kind),
            DeliveryPolicy::Ephemeral
        );
    }
    assert_eq!(
        DeliveryPolicy::resolve("billing.charge", "ChargeOrder"),
        DeliveryPolicy::Durable
    );
    assert_eq!(
        DeliveryPolicy::resolve("billing.charge", "Volatile"),
        DeliveryPolicy::Volatile
    );
    let kernel = AtelierKernel::new_in_memory().unwrap();
    let client = kernel.attach_domain("query_probe");
    for kind in [MSG_QUERY_RUNTIME, MSG_QUERY_DOMAIN_ASSEMBLY] {
        let ids = client.send(
            vec![KERNEL_RUNTIME_TARGET.to_string()],
            kind,
            json!({}),
            json!({}),
        );
        assert!(client.drain_frames().iter().any(|frame|matches!(frame,
            KernelToDomainFrame::Receipt(receipt) if receipt.message_id==ids[0] && receipt.status==ContractStatus::Completed)));
    }
    assert_eq!(kernel.ledger().persisted_count(), 0);
    client.send(
        vec!["billing.charge".to_string()],
        "ChargeOrder",
        json!({}),
        json!({"amount":100}),
    );
    assert_eq!(
        kernel.ledger().persisted_count(),
        1,
        "business Durable receipt remains persisted even for invalid target"
    );
}
