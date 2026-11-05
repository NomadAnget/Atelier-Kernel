//! SDK v2 真实内核 e2e：注册处理器、send/request 主链路、四阶段回执。

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

domain = AtelierDomain("v2test")

@domain.handler("echo")
async def echo(msg):
    return {"echo": msg.data.get("value")}

@domain.handler("add")
async def add(msg):
    return {"sum": int(msg.data["a"]) + int(msg.data["b"])}

@domain.handler("file")
async def file_handler(msg):
    domain.enable_file_interception()
    await domain.refresh_file_index()
    with open("vfs://out/payload.bin", "wb") as f:
        f.write(b"v2-file-object")
    with open("vfs://out/payload.bin", "rb") as f:
        encoded = domain._encode_files({"f": f})
        decoded = await domain._decode_files(encoded)
    return {"content": decoded["f"].read().decode(), "encoded": "__atelier_file__" in encoded["f"]}

domain.run()
"#;

#[test]
fn sdk_v2_round_trip() {
    let v2 = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sdk/v2/python");
    let tmp = std::env::temp_dir().join(format!("sdk-v2-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).unwrap();
    let db = tmp.join("kernel.db");
    let project_root = tmp.join("data");
    let k = AtelierKernel::open_with_db_and_root(&db, &project_root).unwrap();

    let mut spec = DomainProcessSpec::new("v2test", python(), vec!["-c".into(), DOMAIN.to_string()]);
    spec.env.insert("PYTHONPATH".into(), v2.to_string_lossy().to_string());
    spec.env.insert("PYTHONIOENCODING".into(), "utf-8".into());
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
        if k.router().active_routes().iter().any(|r| r == "v2test.echo") {
            break;
        }
        assert!(Instant::now() < deadline, "v2 domain did not register");
        std::thread::sleep(Duration::from_millis(50));
    }

    let verifier = k.attach_domain("verifier");

    verifier.send(vec!["v2test.echo".into()], "Echo", json!({}), json!({"value": 42}));
    let echoed = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(echoed.status, atelier_kernel::ContractStatus::Completed, "{:?}", echoed.data);
    assert_eq!(echoed.data.unwrap()["echo"], 42);

    verifier.send(
        vec!["v2test.add".into()],
        "Add",
        json!({}),
        json!({"a": 20, "b": 22}),
    );
    let added = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(added.status, atelier_kernel::ContractStatus::Completed, "{:?}", added.data);
    assert_eq!(added.data.unwrap()["sum"], 42);

    verifier.send(vec!["v2test.file".into()], "File", json!({}), json!({}));
    let file_result = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(
        file_result.status,
        atelier_kernel::ContractStatus::Completed,
        "{:?}",
        file_result.data
    );
    let file_data = file_result.data.unwrap();
    assert_eq!(file_data["content"], "v2-file-object");
    assert_eq!(file_data["encoded"], true);

    let _ = std::fs::remove_dir_all(&tmp);
}
