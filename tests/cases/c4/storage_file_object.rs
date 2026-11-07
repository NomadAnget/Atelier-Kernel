//! C4 文件对象 ↔ 本域存储表 e2e：insert 存入文件对象，query 读回仍是原生文件对象。

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

const DOMAIN: &str = r#"
from atelier_sdk import AtelierDomain
domain = AtelierDomain("c4")

def find_file(value):
    if isinstance(value, dict):
        for item in value.values():
            found = find_file(item)
            if found is not None:
                return found
    elif isinstance(value, (list, tuple)):
        for item in value:
            found = find_file(item)
            if found is not None:
                return found
    elif hasattr(value, "read"):
        return value
    return None

@domain.handler("check")
async def check(msg):
    domain.enable_file_interception()
    with open("vfs://out/payload.bin", "wb") as f:
        f.write(b"stored-file-object")
    with open("vfs://out/payload.bin", "rb") as f:
        await domain.storage_insert("rec-1", "evidence", {"file": f})
    rows = await domain.storage_query(record_id="rec-1")
    handle = find_file(rows)
    content = handle.read().decode() if handle is not None else None
    return {"content": content, "has_file_object": handle is not None}

import asyncio
asyncio.run(domain.run_async())
"#;

#[test]
fn c4_storage_file_object_round_trip() {
    let sdk = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sdk/python");
    let tmp = std::env::temp_dir().join(format!("c4-store-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).unwrap();
    let db = tmp.join("kernel.db");
    let project_root = tmp.join("data");
    let k = AtelierKernel::open_with_db_and_root(&db, &project_root).unwrap();

    let mut spec = DomainProcessSpec::new("store", python(), vec!["-c".into(), DOMAIN.to_string()]);
    spec.env.insert("PYTHONPATH".into(), sdk.to_string_lossy().to_string());
    spec.auto_restart = false;
    let cfg = KernelBootConfig {
        source_path: None,
        db_path: db.to_string_lossy().to_string(),
        project_root: project_root.to_string_lossy().to_string(),
        web_ui_bind: "127.0.0.1:0".into(),
        web_ui_enabled: false,
        domains: vec![spec],
    };
    k.supervisor().boot_from_config(&cfg);

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if k.router().active_routes().iter().any(|r| r == "store.check") {
            break;
        }
        assert!(Instant::now() < deadline, "domain did not register");
        std::thread::sleep(Duration::from_millis(50));
    }

    let verifier = k.attach_domain("verifier");
    verifier.send(vec!["store.check".into()], "Check", json!({}), json!({}));
    let receipt = terminal(&verifier, Duration::from_secs(20));
    assert_eq!(receipt.status, atelier_kernel::ContractStatus::Completed, "{:?}", receipt.data);
    let data = receipt.data.unwrap();
    assert_eq!(data["has_file_object"], true, "{data}");
    assert_eq!(data["content"], "stored-file-object", "{data}");

    let _ = std::fs::remove_dir_all(&tmp);
}
