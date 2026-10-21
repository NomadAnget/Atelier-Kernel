use atelier_kernel::{AtelierKernel, ContractStatus, KernelToDomainFrame};
use serde_json::json;
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::Duration;

#[test]
fn runtime_queries_finish_while_handler_registration_writes_the_domain_table() {
    let kernel = Arc::new(AtelierKernel::new_in_memory().unwrap());
    kernel.logger().set_console_enabled(false);
    let _domains: Vec<_> = (0..16)
        .map(|i| kernel.attach_domain(&format!("domain_{i}")))
        .collect();
    let barrier = Arc::new(Barrier::new(2));
    let (done, completed) = mpsc::channel();
    for reader in [true, false] {
        let kernel = kernel.clone();
        let barrier = barrier.clone();
        let done = done.clone();
        thread::spawn(move || {
            let client = kernel.attach_domain(if reader {
                "runtime_reader"
            } else {
                "registry_writer"
            });
            barrier.wait();
            for _ in 0..1000 {
                let id = if reader {
                    client.send_one(
                        vec!["kernel.runtime".into()],
                        "QueryRuntime",
                        json!({}),
                        json!({}),
                    )
                } else {
                    client.register_handlers(&["ping"])
                };
                assert!(client.drain_frames().iter().any(|frame| matches!(frame,
                    KernelToDomainFrame::Receipt(r) if r.message_id == id && r.status == ContractStatus::Completed)));
            }
            done.send(()).unwrap();
        });
    }
    for _ in 0..2 {
        completed
            .recv_timeout(Duration::from_secs(10))
            .expect("runtime/registration lock contention stalled a worker");
    }
}
