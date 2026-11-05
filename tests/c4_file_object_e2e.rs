//! C4 文件对象消息 e2e：生产方写 `vfs://` 并把**文件对象**放进消息，消费方收到原生文件对象。

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
domain = AtelierDomain("c4")

@domain.handler("write")
async def write(msg):
    domain.enable_file_interception()
    await domain.refresh_file_index()
    with open("vfs://out/payload.bin", "wb") as f:
        f.write(b"hello-file-object")
    with open("vfs://out/payload.bin", "rb") as f:
        res = await domain.request("consumer.take", "Take", {}, {"report": f})
    return {"content": res["data"]["content"], "is_file": res["data"]["is_file"]}

import asyncio
asyncio.run(domain.run_async())
"#;

const CONSUMER: &str = r#"
from atelier_sdk import AtelierDomain
domain = AtelierDomain("c4")

@domain.handler("take")
async def take(msg):
    f = msg.data["report"]
    content = f.read().decode()
    # 读写都允许：往同一个文件对象追加/改写，验证不被只读限制
    f.seek(0)
    f.write(b"X" * len(content))
    f.flush()
    return {"content": content, "is_file": hasattr(f, "write")}

import asyncio
asyncio.run(domain.run_async())
"#;

#[test]
fn c4_file_object_round_trip() {
    let sdk = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sdk/v2/python");
    let tmp = std::env::temp_dir().join(format!("c4-file-obj-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).unwrap();
    let db = tmp.join("kernel.db");
    let project_root = tmp.join("data");
    let k = AtelierKernel::open_with_db_and_root(&db, &project_root).unwrap();

    let spec = |id: &str, script: &str| -> DomainProcessSpec {
        let mut s = DomainProcessSpec::new(id, python(), vec!["-c".into(), script.to_string()]);
        s.env.insert("PYTHONPATH".into(), sdk.to_string_lossy().to_string());
        s.auto_restart = false;
        s
    };
    let cfg = KernelBootConfig {
        source_path: None,
        db_path: db.to_string_lossy().to_string(),
        project_root: project_root.to_string_lossy().to_string(),
        web_ui_bind: "127.0.0.1:0".into(),
        web_ui_enabled: false,
        domains: vec![spec("producer", PRODUCER), spec("consumer", CONSUMER)],
    };
    k.supervisor().boot_from_config(&cfg);

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
    verifier.send(vec!["producer.write".into()], "Write", json!({}), json!({}));
    let produced = terminal(&verifier, Duration::from_secs(15));
    assert_eq!(produced.status, atelier_kernel::ContractStatus::Completed, "{:?}", produced.data);
    let data = produced.data.unwrap();
    assert_eq!(data["content"], "hello-file-object");
    assert_eq!(data["is_file"], true);

    let _ = std::fs::remove_dir_all(&tmp);
}
