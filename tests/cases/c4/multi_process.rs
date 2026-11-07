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
domain = AtelierDomain("producer")

@domain.handler("write")
async def write(msg):
    domain.enable_file_interception()
    with open("vfs://out/payload.bin", "wb") as f:
        f.write(b"hello-object-42")
    meta = await domain.get_object("out/payload.bin")
    return {"owner": meta["owner"], "virtual_path": meta["virtual_path"],
            "physical_path": meta["physical_path"], "size": meta["size"]}

import asyncio
asyncio.run(domain.run_async())
"#;

const CONSUMER: &str = r#"
from atelier_sdk import AtelierDomain
domain = AtelierDomain("consumer")

@domain.handler("take")
async def take(msg):
    meta = await domain.get_object(msg.data["virtual_path"], msg.data["owner"])
    with open(meta["physical_path"], "rb") as f:
        return {"content": f.read().decode(), "size": meta["size"]}

import asyncio
asyncio.run(domain.run_async())
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
    let produced = produced.data.unwrap();
    let owner = produced["owner"].as_str().unwrap().to_string();
    let virtual_path = produced["virtual_path"].as_str().unwrap().to_string();
    let physical = produced["physical_path"].as_str().unwrap().to_string();
    assert_eq!(owner, "producer");
    assert_eq!(virtual_path, "out/payload.bin");
    assert!(std::path::Path::new(&physical).exists());

    verifier.send(
        vec!["consumer.take".into()],
        "Take",
        json!({}),
        json!({"owner": owner, "virtual_path": virtual_path}),
    );
    let taken = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(taken.status, atelier_kernel::ContractStatus::Completed, "{:?}", taken.data);
    let taken = taken.data.unwrap();
    assert_eq!(taken["content"], "hello-object-42");
    assert_eq!(taken["size"], 15);

    let _ = std::fs::remove_dir_all(&tmp);
}
