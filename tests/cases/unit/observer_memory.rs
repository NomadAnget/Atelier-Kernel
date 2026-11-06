use atelier_kernel::{
    AtelierKernel, ContractStatus, DeliveryPolicy, KERNEL_OBSERVER_TARGET, KERNEL_RUNTIME_TARGET,
    KernelToDomainFrame, MSG_QUERY_DOMAIN_ASSEMBLY, MSG_QUERY_OBSERVER, MSG_QUERY_RUNTIME,
};
use serde_json::{Value, json};

#[test]
fn storage_read_results_are_returned_without_accumulating_query_payloads() {
    use atelier_kernel::{KERNEL_STORAGE_TARGET, MSG_STORAGE_QUERY};
    let kernel = AtelierKernel::new_in_memory().unwrap();
    kernel.logger().set_console_enabled(false);
    let client = kernel.attach_domain("storage_memory_probe");
    let payload = json!({"text":"x".repeat(256 * 1024)});
    client.storage_insert(
        json!({"record_id":"large", "category":"probe"}),
        payload.clone(),
    );
    client.drain_frames();
    for _ in 0..16 {
        let ids = client.send(
            vec![KERNEL_STORAGE_TARGET.into()],
            MSG_STORAGE_QUERY,
            json!({"record_id":"large"}),
            json!({}),
        );
        let receipts: Vec<_> = client
            .drain_frames()
            .into_iter()
            .filter_map(|f| match f {
                KernelToDomainFrame::Receipt(r) if r.message_id == ids[0] => Some(r),
                _ => None,
            })
            .collect();
        assert_eq!(
            receipts.iter().map(|r| r.status).collect::<Vec<_>>(),
            vec![
                ContractStatus::Sent,
                ContractStatus::Received,
                ContractStatus::Started,
                ContractStatus::Completed
            ]
        );
        assert_eq!(
            receipts.last().unwrap().data.as_ref().unwrap()["items"][0]["data"],
            payload
        );
        assert!(
            kernel
                .ledger()
                .get_record(&ids[0], KERNEL_STORAGE_TARGET)
                .is_none(),
            "completed infrastructure reads retain a second full result in the ledger"
        );
        let event = kernel.observer().events_for_message(&ids[0]).pop().unwrap();
        assert_eq!(event.status, ContractStatus::Completed);
        assert_eq!(event.logical_id, receipts.last().unwrap().logical_id);
        assert_ne!(event.logical_id, event.message_id);
        assert!(
            serde_json::to_vec(&event.details).unwrap().len() < 1024,
            "query observation retained a full storage result instead of bounded metadata"
        );
    }
}

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
        assert!(
            kernel
                .ledger()
                .get_record(&ids[0], KERNEL_OBSERVER_TARGET)
                .is_none()
        );
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
    use atelier_kernel::{
        KERNEL_LOGGER_TARGET, KERNEL_REGISTRY_TARGET, KERNEL_STORAGE_TARGET, MSG_QUERY_HANDLERS,
        MSG_QUERY_LOGS, MSG_QUERY_PROGRESS, MSG_STORAGE_INSERT, MSG_STORAGE_QUERY,
    };
    for (target, kind) in [
        (KERNEL_OBSERVER_TARGET, MSG_QUERY_OBSERVER),
        (KERNEL_RUNTIME_TARGET, MSG_QUERY_RUNTIME),
        (KERNEL_RUNTIME_TARGET, MSG_QUERY_DOMAIN_ASSEMBLY),
        (KERNEL_STORAGE_TARGET, MSG_STORAGE_QUERY),
        (KERNEL_REGISTRY_TARGET, MSG_QUERY_HANDLERS),
        (KERNEL_LOGGER_TARGET, MSG_QUERY_LOGS),
        (KERNEL_LOGGER_TARGET, MSG_QUERY_PROGRESS),
    ] {
        assert_eq!(
            DeliveryPolicy::resolve(target, kind),
            DeliveryPolicy::Ephemeral
        );
    }
    assert_eq!(
        DeliveryPolicy::resolve(KERNEL_STORAGE_TARGET, MSG_STORAGE_INSERT),
        DeliveryPolicy::Volatile
    );
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
    let counts = kernel.ledger().retention_counts();
    assert_eq!(counts["ephemeral_inflight"], 0);
    assert_eq!(counts["submissions"], 0);
    assert_eq!(counts["child_links"], 0);
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
