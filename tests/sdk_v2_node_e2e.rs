//! SDK v2 Node 鐪熷疄鍐呮牳 e2e锛氭敞鍐屽鐞嗗櫒銆乻end/request 涓婚摼璺€佸洓闃舵鍥炴墽銆?
use atelier_kernel::{AtelierKernel, DomainProcessSpec, KernelBootConfig, KernelToDomainFrame as F};
use serde_json::json;
use std::path::PathBuf;
use std::time::{Duration, Instant};

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

#[test]
fn sdk_v2_node_round_trip() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let sdk = root.join("sdk/v2/nodejs/index.js");
    let script = format!(
        r#"
const {{ AtelierDomain }} = require({:?});
const domain = new AtelierDomain("v2node");
domain.handler("echo", async (msg) => ({{ echo: msg.data.value }}));
domain.handler("add", async (msg) => ({{ sum: Number(msg.data.a) + Number(msg.data.b) }}));
domain.handler("file", async () => {{
  domain.enableFileInterception();
  await domain.refreshFileIndex();
  const fsp = require("fs/promises");
  await fsp.writeFile("vfs://out/payload.bin", "v2-file-object");
  const handle = await fsp.open("vfs://out/payload.bin", "r+");
  const encoded = domain._encodeFiles({{ f: handle }});
  const decoded = await domain._decodeFiles(encoded);
  const buffer = Buffer.alloc(64);
  const {{ bytesRead }} = await decoded.f.read(buffer, 0, 64, 0);
  return {{ content: buffer.subarray(0, bytesRead).toString(), encoded: !!encoded.f.__atelier_file__, handle_path: String(handle.path || ""), ns_count: domain._namespace.byPhysical.size }};
}});
domain.run();
"#,
        sdk.to_string_lossy().replace('\\', "/")
    );

    let tmp = std::env::temp_dir().join(format!("sdk-v2-node-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).unwrap();
    let db = tmp.join("kernel.db");
    let project_root = tmp.join("data");
    let k = AtelierKernel::open_with_db_and_root(&db, &project_root).unwrap();

    let mut spec = DomainProcessSpec::new("v2node", "node", vec!["-e".into(), script]);
    spec.env.insert("PYTHONIOENCODING".into(), "utf-8".into());
    spec.env.insert(
        "PYTHON".into(),
        "G:/Atelier/atelier-solo/.venv/Scripts/python.exe".into(),
    );
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
        if k.router().active_routes().iter().any(|r| r == "v2node.echo") {
            break;
        }
        assert!(Instant::now() < deadline, "v2 node domain did not register");
        std::thread::sleep(Duration::from_millis(50));
    }

    let verifier = k.attach_domain("verifier");
    verifier.send(vec!["v2node.echo".into()], "Echo", json!({}), json!({"value": 42}));
    let echoed = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(echoed.status, atelier_kernel::ContractStatus::Completed, "{:?}", echoed.data);
    assert_eq!(echoed.data.unwrap()["echo"], 42);

    verifier.send(vec!["v2node.add".into()], "Add", json!({}), json!({"a": 20, "b": 22}));
    let added = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(added.status, atelier_kernel::ContractStatus::Completed, "{:?}", added.data);
    assert_eq!(added.data.unwrap()["sum"], 42);

    verifier.send(vec!["v2node.file".into()], "File", json!({}), json!({}));
    let file_result = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(
        file_result.status,
        atelier_kernel::ContractStatus::Completed,
        "{:?}",
        file_result.data
    );
    let file_data = file_result.data.unwrap();
    assert_eq!(file_data["content"], "v2-file-object", "{file_data}");
    assert_eq!(file_data["encoded"], true, "{file_data}");

    let _ = std::fs::remove_dir_all(&tmp);
}

