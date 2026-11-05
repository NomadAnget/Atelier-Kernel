//! 诊断：用旧 SDK 的 Node 域跑同一夹具，判断 transport 失败在 SDK 还是内核/e2e。

use atelier_kernel::{AtelierKernel, DomainProcessSpec, KernelBootConfig};
use serde_json::json;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[test]
fn old_sdk_node_transport_diagnostic() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let sdk = root.join("sdk/nodejs/index.js");
    let script = format!(
        r#"
const pkg = require({:?});
const domain = new pkg.AtelierDomain({{ domain: "oldnode" }});
domain.handler("echo", async (msg) => ({{ echo: msg.data.value }}));
domain.runAsync().catch((e) => console.error("old sdk error", e && e.message));
"#,
        sdk.to_string_lossy().replace('\\', "/")
    );

    let tmp = std::env::temp_dir().join(format!("old-node-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).unwrap();
    let db = tmp.join("kernel.db");
    let project_root = tmp.join("data");
    let k = AtelierKernel::open_with_db_and_root(&db, &project_root).unwrap();

    let mut spec = DomainProcessSpec::new("oldnode", "node", vec!["-e".into(), script]);
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

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut registered = false;
    while Instant::now() < deadline {
        if k.router().active_routes().iter().any(|r| r == "oldnode.echo") {
            registered = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    let _ = json!({});
    let _ = k;
    let _ = std::fs::remove_dir_all(&tmp);
    assert!(registered, "old SDK Node domain also failed to register");
}
