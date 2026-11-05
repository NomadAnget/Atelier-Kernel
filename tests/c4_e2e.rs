//! C4 多进程端到端：真实 Python 域经 SDK 用 `vfs://` 写文件、消息携带 object_id、另一域读回。

use atelier_kernel::{AtelierKernel, DomainProcessSpec, KernelBootConfig, KernelToDomainFrame as F};
use serde_json::json;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn python() -> &'static str {
    if cfg!(windows) { "python" } else { "python3" }
}

fn terminal(client: &atelier_kernel::DomainPipeClient, timeout: Duration) -> atelier_kernel::MessageReceipt {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(r) = client.drain_frames().into_iter().find_map(|f| match f {
            F::Receipt(r) if r.status.is_terminal() => Some(r),
            _ => None,
        }) {
            return r;
        }
        assert!(Instant::now() < deadline, "no terminal receipt");
        std::thread::sleep(Duration::from_millis(20));
    }
}

const PRODUCER: &str = r#"
from atelier_sdk import AtelierDomain
domain = AtelierDomain()

@domain.handler("write")
async def write(msg):
    domain.enable_file_interception()
    await domain.refresh_file_index()
    with open("vfs://out/payload.bin", "wb") as f:
        f.write(b"hello-object-42")
    return {"object_id": domain.file_object_id("vfs://out/payload.bin")}

domain.run()
"#;

const CONSUMER: &str = r#"
from atelier_sdk import AtelierDomain
domain = AtelierDomain()

@domain.handler("take")
async def take(msg):
    oid = msg.data["object_id"]
    res = await domain.request("kernel.storage", "StorageObjectGet", {"object_id": oid}, {})
    physical = res["data"]["physical_path"]
    with open(physical, "rb") as f:
        return {"content": f.read().decode()}

domain.run()
"#;

#[test]
fn c4_multi_process_object_round_trip() {
    let sdk = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sdk/python");
    let tmp = std::env::temp_dir().join(format!("c4-e2e-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).unwrap();
    let db = tmp.join("kernel.db");
    let project_root = tmp.join("data");
    let k = AtelierKernel::open_with_db_and_root(&db, &project_root).unwrap();

    let spec = |id: &str, script: &str| -> DomainProcessSpec {
        let mut s = DomainProcessSpec::new(
            id,
            python(),
            vec!["-c".into(), script.to_string()],
        );
        s.env.insert(
            "PYTHONPATH".into(),
            sdk.to_string_lossy().to_string(),
        );
        s.auto_restart = false;
        s
    };
    let cfg = KernelBootConfig {
        source_path: None,
        db_path: db.to_string_lossy().to_string(),
        project_root: project_root.to_string_lossy().to_string(),
        web_ui_bind: "127.0.0.1:0".into(),
        web_ui_enabled: false,
        domains: vec![
            spec("producer", PRODUCER),
            spec("consumer", CONSUMER),
        ],
    };
    k.supervisor().boot_from_config(&cfg);

    // 等两个域都注册并 Active
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let routes = k.router().active_routes();
        if routes.iter().any(|r| r == "producer.write") && routes.iter().any(|r| r == "consumer.take") {
            break;
        }
        assert!(Instant::now() < deadline, "domains did not register: {routes:?}");
        std::thread::sleep(Duration::from_millis(50));
    }

    let verifier = k.attach_domain("verifier");
    verifier.send(
        vec!["producer.write".into()],
        "Write",
        json!({}),
        json!({}),
    );
    let produced = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(produced.status, atelier_kernel::ContractStatus::Completed, "{:?}", produced.data);
    let object_id = produced.data.unwrap()["object_id"].as_str().unwrap().to_string();
    assert!(object_id.starts_with("obj_") || !object_id.is_empty());

    verifier.send(
        vec!["consumer.take".into()],
        "Take",
        json!({}),
        json!({"object_id": object_id}),
    );
    let taken = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(taken.status, atelier_kernel::ContractStatus::Completed, "{:?}", taken.data);
    assert_eq!(taken.data.unwrap()["content"], "hello-object-42");

    // 物理文件确实落在内核域根下：<project_root>/producer/<object_id>
    assert!(
        project_root.join("producer").join(&object_id).exists(),
        "physical file must live under the kernel-managed domain root"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}
