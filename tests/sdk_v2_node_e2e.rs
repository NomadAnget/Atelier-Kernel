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
domain.handler("storage", async (msg) => {{
  const recordId = "e2e-" + msg.message_id;
  await domain.storageInsert(recordId, "e2e", {{ value: 7 }});
  const query = await domain.storageQuery({{ record_id: recordId }});
  return {{ rows: (query.items || []).length, count: query.count }};
}});
domain.handler("query", async () => {{
  const handlers = await domain.queryHandlers();
  const runtime = await domain.queryRuntime();
  return {{ routes: handlers.routes || [], runtime_ok: !!runtime && typeof runtime === "object" }};
}});
domain.handler("notify", async () => {{
  const result = await domain.notify("info", "hello from v2 node");
  return {{ notified: !!result && typeof result === "object" }};
}});
domain.handler("c4", async () => {{
  const fs = require("fs");
  const put = await domain.putObject("e2e/obj-1");
  fs.writeFileSync(put.physical_path, "c4-payload");
  const got = await domain.getObject(put.object_id);
  const deleted = await domain.deleteObject(put.object_id);
  return {{ object_id: put.object_id, physical_path: put.physical_path, got_matches: got.physical_path === put.physical_path, size: got.size, deleted: !!deleted && typeof deleted === "object" }};
}});
domain.handler("wait", async () => {{
  const mid = await domain.send("v2node.echo", "Echo", {{}}, {{ value: 7 }});
  const receipt = await domain.waitResult(mid);
  domain.forgetResult(mid);
  return {{ echo: receipt.data.echo }};
}});
domain.handler("log", async () => {{
  domain.logger.info("e2e log line", {{ tag: "v2node" }});
  return {{ logged: true }};
}});
domain.handler("subscribe", async () => {{
  await domain.subscribeLogs();
  await domain.subscribeTelemetry();
  await domain.subscribeNotify();
  return {{ subscribed: true }};
}});
domain.handler("supervisor", async () => {{
  try {{
    await domain.supervisorRestart("no-such-domain");
  }} catch (error) {{
    return {{ routed: true, status: error && error.status || null }};
  }}
  return {{ routed: true, status: null }};
}});
domain.handler("stress", async () => {{
  for (let i = 0; i < 20; i += 1) {{
    const result = await domain.request("v2node.echo", "Echo", {{}}, {{ value: i }});
    if (result.data.echo !== i) return {{ failed_at: i, got: result.data }};
  }}
  return {{ ok: true, pending: domain._pending.size, results: domain._results.size }};
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

    verifier.send(vec!["v2node.storage".into()], "Storage", json!({}), json!({}));
    let stored = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(stored.status, atelier_kernel::ContractStatus::Completed, "{:?}", stored.data);
    let stored_data = stored.data.unwrap();
    assert!(stored_data["rows"].as_u64().unwrap_or(0) >= 1, "{stored_data}");

    verifier.send(vec!["v2node.query".into()], "Query", json!({}), json!({}));
    let queried = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(queried.status, atelier_kernel::ContractStatus::Completed, "{:?}", queried.data);
    let queried_data = queried.data.unwrap();
    let routes = queried_data["routes"].as_array().expect("routes");
    assert!(routes.iter().any(|r| r == "v2node.echo"), "{queried_data}");
    assert_eq!(queried_data["runtime_ok"], true, "{queried_data}");

    verifier.send(vec!["v2node.notify".into()], "Notify", json!({}), json!({}));
    let notified = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(notified.status, atelier_kernel::ContractStatus::Completed, "{:?}", notified.data);
    assert_eq!(notified.data.unwrap()["notified"], true);

    verifier.send(vec!["v2node.c4".into()], "C4", json!({}), json!({}));
    let c4 = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(c4.status, atelier_kernel::ContractStatus::Completed, "{:?}", c4.data);
    let c4_data = c4.data.unwrap();
    assert!(c4_data["object_id"].as_str().is_some_and(|s| !s.is_empty()), "{c4_data}");
    assert!(c4_data["physical_path"].as_str().is_some_and(|s| !s.is_empty()), "{c4_data}");
    assert_eq!(c4_data["got_matches"], true, "{c4_data}");
    assert_eq!(c4_data["size"], 10, "{c4_data}");
    assert_eq!(c4_data["deleted"], true, "{c4_data}");

    verifier.send(vec!["v2node.wait".into()], "Wait", json!({}), json!({}));
    let waited = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(waited.status, atelier_kernel::ContractStatus::Completed, "{:?}", waited.data);
    assert_eq!(waited.data.unwrap()["echo"], 7);

    verifier.send(vec!["v2node.log".into()], "Log", json!({}), json!({}));
    let logged = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(logged.status, atelier_kernel::ContractStatus::Completed, "{:?}", logged.data);

    verifier.send(vec!["v2node.subscribe".into()], "Subscribe", json!({}), json!({}));
    let subscribed = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(subscribed.status, atelier_kernel::ContractStatus::Completed, "{:?}", subscribed.data);
    assert_eq!(subscribed.data.unwrap()["subscribed"], true);

    verifier.send(vec!["v2node.supervisor".into()], "Supervisor", json!({}), json!({}));
    let supervised = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(supervised.status, atelier_kernel::ContractStatus::Completed, "{:?}", supervised.data);
    assert_eq!(supervised.data.unwrap()["routed"], true);

    verifier.send(vec!["v2node.stress".into()], "Stress", json!({}), json!({}));
    let stressed = terminal(&verifier, Duration::from_secs(30));
    assert_eq!(stressed.status, atelier_kernel::ContractStatus::Completed, "{:?}", stressed.data);
    let stress_data = stressed.data.unwrap();
    assert_eq!(stress_data["ok"], true, "{stress_data}");
    assert_eq!(stress_data["pending"], 0, "{stress_data}");
    assert_eq!(stress_data["results"], 0, "{stress_data}");

    let _ = std::fs::remove_dir_all(&tmp);
}

