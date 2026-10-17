use atelier_kernel::{
    AtelierKernel, ContractStatus as S, KernelToDomainFrame as F, MSG_DELIVERY_VOLATILE,
};
use serde_json::json;
use std::{sync::Arc, thread, time::Duration};

#[test]
fn fast_concurrent_replies_preserve_caller_and_observer_order() {
    const SENDERS: usize = 4;
    const PER_SENDER: usize = 64;
    let kernel = Arc::new(AtelierKernel::new_in_memory().unwrap());
    let worker = kernel.attach_domain("fast_worker");
    worker.register_handlers(&["echo"]);
    worker.drain_frames();
    let clients: Vec<_> = (0..SENDERS)
        .map(|n| kernel.attach_domain(&format!("sender_{n}")))
        .collect();
    let responder = thread::spawn(move || {
        for _ in 0..SENDERS * PER_SENDER {
            let F::Deliver(msg) = worker.recv_frame_timeout(Duration::from_secs(10)).unwrap()
            else {
                panic!("expected delivery")
            };
            for stage in [S::Received, S::Started, S::Completed] {
                worker.report_receipt(&msg.message_id, &msg.target, stage, None);
            }
        }
    });
    let senders: Vec<_> = clients
        .into_iter()
        .map(|client| {
            let kernel = Arc::clone(&kernel);
            thread::spawn(move || {
                for _ in 0..PER_SENDER {
                    let id = client.send_one(
                        vec!["fast_worker.echo".into()],
                        MSG_DELIVERY_VOLATILE,
                        json!({}),
                        json!({}),
                    );
                    let mut actual = Vec::new();
                    for _ in 0..4 {
                        let F::Receipt(receipt) =
                            client.recv_frame_timeout(Duration::from_secs(10)).unwrap()
                        else {
                            panic!("expected receipt")
                        };
                        assert_eq!(receipt.message_id, id);
                        actual.push(receipt.status);
                    }
                    assert_eq!(actual, vec![S::Sent, S::Received, S::Started, S::Completed]);
                    let observed: Vec<_> = kernel
                        .observer()
                        .events_for_message(&id)
                        .iter()
                        .map(|e| e.status)
                        .collect();
                    assert_eq!(observed, actual);
                }
            })
        })
        .collect();
    for sender in senders {
        sender.join().unwrap();
    }
    responder.join().unwrap();
}

#[test]
fn immediate_target_fault_cannot_publish_failure_before_sent() {
    let kernel = Arc::new(AtelierKernel::new_in_memory().unwrap());
    let sender = kernel.attach_domain("fault_sender");
    for _ in 0..32 {
        let worker = kernel.attach_domain("fault_worker");
        worker.register_handlers(&["run"]);
        worker.drain_frames();
        let router = Arc::clone(kernel.router());
        let fault = thread::spawn(move || {
            assert!(matches!(
                worker.recv_frame_timeout(Duration::from_secs(10)).unwrap(),
                F::Deliver(_)
            ));
            router.mark_domain_faulted("fault_worker", "immediate protocol pipe EOF");
        });
        let id = sender.send_one(
            vec!["fault_worker.run".into()],
            MSG_DELIVERY_VOLATILE,
            json!({}),
            json!({}),
        );
        let mut statuses = Vec::new();
        for _ in 0..2 {
            let F::Receipt(receipt) = sender.recv_frame_timeout(Duration::from_secs(10)).unwrap()
            else {
                panic!("expected receipt")
            };
            assert_eq!(receipt.message_id, id);
            statuses.push(receipt.status);
        }
        fault.join().unwrap();
        assert_eq!(statuses, vec![S::Sent, S::ExecutionFailed]);
        let observed: Vec<_> = kernel
            .observer()
            .events_for_message(&id)
            .iter()
            .map(|e| e.status)
            .collect();
        assert_eq!(observed, statuses);
    }
}

#[test]
fn duplicate_submission_replay_cannot_regress_live_receipts() {
    let kernel = AtelierKernel::new_in_memory().unwrap();
    let sender = kernel.attach_domain("replay_sender");
    let worker = kernel.attach_domain("replay_worker");
    worker.register_handlers(&["run"]);
    worker.drain_frames();
    let responder = thread::spawn(move || {
        for _ in 0..32 {
            let F::Deliver(msg) = worker.recv_frame_timeout(Duration::from_secs(10)).unwrap()
            else {
                panic!("expected delivery")
            };
            for stage in [S::Received, S::Started, S::Completed] {
                worker.report_receipt(&msg.message_id, &msg.target, stage, None);
                thread::yield_now();
            }
        }
        assert!(
            worker.drain_frames().is_empty(),
            "duplicates must not execute again"
        );
    });
    for seq in 100..132 {
        let send = || {
            sender.send_one_with_seq(
                seq,
                vec!["replay_worker.run".into()],
                MSG_DELIVERY_VOLATILE,
                json!({}),
                json!({}),
            )
        };
        let id = send();
        for _ in 0..8 {
            assert_eq!(send(), id);
            thread::yield_now();
        }
        let mut previous = 0;
        loop {
            let F::Receipt(receipt) = sender.recv_frame_timeout(Duration::from_secs(10)).unwrap()
            else {
                panic!("expected receipt")
            };
            assert_eq!(receipt.message_id, id);
            assert!(
                receipt.status.stage_rank() >= previous,
                "replay regressed {previous} -> {:?}",
                receipt.status
            );
            previous = receipt.status.stage_rank();
            if receipt.status.is_terminal() {
                for frame in sender.drain_frames() {
                    let F::Receipt(receipt) = frame else {
                        panic!("expected receipt")
                    };
                    assert_eq!(receipt.message_id, id);
                    assert_eq!(receipt.status, S::Completed);
                }
                break;
            }
        }
    }
    responder.join().unwrap();
}
