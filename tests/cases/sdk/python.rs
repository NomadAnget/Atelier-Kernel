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
    listed = await domain.refresh_file_index()
    with open("vfs://out/payload.bin", "wb") as f:
        f.write(b"v2-file-object")
    tracked = domain.file_object_id("vfs://out/payload.bin")
    with open("vfs://out/payload.bin", "rb") as f:
        encoded = domain._encode_files({"f": f})
        decoded = await domain._decode_files(encoded)
    return {
        "content": decoded["f"].read().decode(),
        "encoded": "__atelier_file__" in encoded["f"],
        "tracked": isinstance(tracked, str) and len(tracked) == 32,
        "listed": isinstance(listed, dict),
    }

@domain.handler("api")
async def api_handler(msg):
    rid = "api-" + msg.message_id
    await domain.storage_insert(rid, "api", {"value": 1})
    await domain.storage_update(rid, {"value": 2})
    updated_rows = (await domain.storage_query(record_id=rid)).get("items") or []
    await domain.storage_delete(rid)
    gone_rows = (await domain.storage_query(record_id=rid)).get("items") or []
    observer = await domain.query_observer()
    progress = await domain.query_progress()
    events = []
    domain.on_notify(lambda payload: events.append("notify"))
    await domain.subscribe_notify()
    await domain.notify("info", "callback-probe")
    await asyncio.sleep(0.3)
    unsubscribed = all(
        isinstance(result, dict)
        for result in (
            await domain.unsubscribe_logs(),
            await domain.unsubscribe_telemetry(),
            await domain.unsubscribe_notify(),
            await domain.unsubscribe_observer(),
        )
    )
    try:
        domain.report_receipt("no-such-message", "v2test.api", "completed", {"probe": 1})
        receipt_ok = True
    except Exception:  # noqa: BLE001
        receipt_ok = False
    stop = await domain.supervisor_stop("no-such-domain")
    return {
        "updated": bool(updated_rows) and updated_rows[0].get("data", {}).get("value") == 2,
        "removed": not gone_rows,
        "observer_ok": isinstance(observer, dict),
        "progress_ok": isinstance(progress, dict),
        "unsubscribed": unsubscribed,
        "callback_ok": "notify" in events,
        "receipt_ok": receipt_ok,
        "stop_ok": isinstance(stop, dict) and stop.get("was_running") is False,
    }

@domain.handler("storage")
async def storage_handler(msg):
    record_id = "e2e-" + msg.message_id
    await domain.storage_insert(record_id, "e2e", {"value": 7})
    query = await domain.storage_query(record_id=record_id)
    return {"rows": len(query.get("items") or []), "count": query.get("count")}

@domain.handler("query")
async def query_handler(msg):
    handlers = await domain.query_handlers()
    runtime = await domain.query_runtime()
    # 能力 9：日志游标分页也要真能走通（参数名必须是内核字段 snake_case）
    page = await domain.query_logs(limit=1)
    return {
        "routes": handlers.get("routes") or [],
        "runtime_ok": isinstance(runtime, dict),
        "page_ok": isinstance(page, dict)
        and isinstance(page.get("logs"), list)
        and "has_more" in page
        and "next_before_id" in page,
    }

@domain.handler("notify")
async def notify_handler(msg):
    result = await domain.notify("info", "hello from v2")
    return {"notified": isinstance(result, dict)}

@domain.handler("c4")
async def c4_handler(msg):
    stage = "put"
    try:
        put = await domain.put_object("e2e/obj-1")
        stage = "write"
        with open(put["physical_path"], "wb") as f:
            f.write(b"c4-payload")
        stage = "get"
        got = await domain.get_object(put["object_id"])
        stage = "delete"
        deleted = await domain.delete_object(put["object_id"])
    except Exception as error:  # noqa: BLE001
        return {"failed_stage": stage, "error": str(error), "detail": getattr(error, "data", None)}
    return {
        "object_id": put["object_id"],
        "physical_path": put["physical_path"],
        "got_matches": got.get("physical_path") == put["physical_path"],
        "size": got.get("size"),
        "deleted": isinstance(deleted, dict),
    }

@domain.handler("wait")
async def wait_handler(msg):
    mid = await domain.send("v2test.echo", "Echo", {}, {"value": 7})
    receipt = await domain.wait_result(mid)
    domain.forget_result(mid)
    # 能力 5：批量等待（wait_results 只消费已存在的 id，不重新发送）
    batch_ids = await domain.send(["v2test.echo", "v2test.query"], "Echo", {}, {"value": 3})
    batch = await domain.wait_results(batch_ids)
    for one in batch_ids:
        domain.forget_result(one)
    return {
        "echo": receipt["data"]["echo"],
        "batch": len(batch),
        "batch_ok": all(one.get("status") == "completed" for one in batch),
    }

@domain.handler("log")
async def log_handler(msg):
    domain.logger.info("e2e log line", tag="v2")
    return {"logged": True}

@domain.handler("subscribe")
async def subscribe_handler(msg):
    await domain.subscribe_logs()
    await domain.subscribe_telemetry()
    await domain.subscribe_notify()
    return {"subscribed": True}

@domain.handler("supervisor")
async def supervisor_handler(msg):
    try:
        await domain.supervisor_restart("no-such-domain")
    except Exception as error:  # noqa: BLE001
        return {"routed": True, "status": getattr(error, "status", None)}
    return {"routed": True, "status": None}

@domain.handler("stress")
async def stress_handler(msg):
    for i in range(20):
        result = await domain.request("v2test.echo", "Echo", {}, {"value": i})
        if result["data"].get("echo") != i:
            return {"failed_at": i, "got": result["data"]}
    return {"ok": True, "pending": len(domain._pending), "results": len(domain._results)}

import asyncio
asyncio.run(domain.run_async())
"#;
#[test]
fn sdk_v2_round_trip() {
    let v2 = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sdk/python");
    let tmp = std::env::temp_dir().join(format!("sdk-v2-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).unwrap();
    let db = tmp.join("kernel.db");
    let project_root = tmp.join("data");
    let k = AtelierKernel::open_with_db_and_root(&db, &project_root).unwrap();

    let script_path = tmp.join("domain.py");
    std::fs::write(&script_path, DOMAIN).unwrap();
    let mut spec = DomainProcessSpec::new(
        "v2test",
        python(),
        vec![script_path.to_string_lossy().to_string()],
    );
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
        if Instant::now() >= deadline {
            panic!(
                "v2 domain did not register; active routes: {:?}",
                k.router().active_routes()
            );
        }
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
    assert_eq!(file_data["tracked"], true, "file_object_id: {file_data}");
    assert_eq!(file_data["listed"], true, "refresh_file_index: {file_data}");

    verifier.send(vec!["v2test.storage".into()], "Storage", json!({}), json!({}));
    let stored = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(stored.status, atelier_kernel::ContractStatus::Completed, "{:?}", stored.data);
    let stored_data = stored.data.unwrap();
    assert!(stored_data["rows"].as_u64().unwrap_or(0) >= 1, "{stored_data}");

    verifier.send(vec!["v2test.api".into()], "Api", json!({}), json!({}));
    let api = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(api.status, atelier_kernel::ContractStatus::Completed, "{:?}", api.data);
    let api_data = api.data.unwrap();
    assert_eq!(api_data["updated"], true, "storage_update: {api_data}");
    assert_eq!(api_data["removed"], true, "storage_delete: {api_data}");
    assert_eq!(api_data["observer_ok"], true, "query_observer: {api_data}");
    assert_eq!(api_data["progress_ok"], true, "query_progress: {api_data}");
    assert_eq!(api_data["unsubscribed"], true, "unsubscribe_*: {api_data}");
    assert_eq!(api_data["callback_ok"], true, "on_notify callback: {api_data}");
    assert_eq!(api_data["receipt_ok"], true, "report_receipt: {api_data}");
    assert_eq!(api_data["stop_ok"], true, "supervisor_stop: {api_data}");

    verifier.send(vec!["v2test.query".into()], "Query", json!({}), json!({}));
    let queried = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(queried.status, atelier_kernel::ContractStatus::Completed, "{:?}", queried.data);
    let queried_data = queried.data.unwrap();
    let routes = queried_data["routes"].as_array().expect("routes");
    assert!(routes.iter().any(|r| r == "v2test.echo"), "{queried_data}");
    assert_eq!(queried_data["runtime_ok"], true, "{queried_data}");
    assert_eq!(queried_data["page_ok"], true, "query_logs 分页响应不完整: {queried_data}");

    verifier.send(vec!["v2test.notify".into()], "Notify", json!({}), json!({}));
    let notified = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(notified.status, atelier_kernel::ContractStatus::Completed, "{:?}", notified.data);
    assert_eq!(notified.data.unwrap()["notified"], true);

    verifier.send(vec!["v2test.c4".into()], "C4", json!({}), json!({}));
    let c4 = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(c4.status, atelier_kernel::ContractStatus::Completed, "{:?}", c4.data);
    let c4_data = c4.data.unwrap();
    assert!(c4_data["object_id"].as_str().is_some_and(|s| !s.is_empty()), "{c4_data}");
    assert!(c4_data["physical_path"].as_str().is_some_and(|s| !s.is_empty()), "{c4_data}");
    assert_eq!(c4_data["got_matches"], true, "{c4_data}");
    assert_eq!(c4_data["size"], 10, "{c4_data}");
    assert_eq!(c4_data["deleted"], true, "{c4_data}");

    verifier.send(vec!["v2test.wait".into()], "Wait", json!({}), json!({}));
    let waited = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(waited.status, atelier_kernel::ContractStatus::Completed, "{:?}", waited.data);
    let waited_data = waited.data.unwrap();
    assert_eq!(waited_data["echo"], 7);
    assert_eq!(waited_data["batch"], 2, "wait_results 应返回两条回执: {waited_data}");
    assert_eq!(waited_data["batch_ok"], true, "{waited_data}");

    verifier.send(vec!["v2test.log".into()], "Log", json!({}), json!({}));
    let logged = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(logged.status, atelier_kernel::ContractStatus::Completed, "{:?}", logged.data);
    // 日志必须真的到达内核日志枢纽：这是 SDK 与内核之间唯一不产生回执的通道，
    // 走错帧类型时不会报错、只会静默丢日志，所以必须在这里断言。
    let logs = k.logger().recent_logs();
    assert!(
        logs.iter().any(|r| {
            r.source_domain.as_deref() == Some("v2test") && r.message.contains("e2e log line")
        }),
        "domain log did not reach the kernel log hub; recent={}",
        logs.len()
    );

    verifier.send(vec!["v2test.subscribe".into()], "Subscribe", json!({}), json!({}));
    let subscribed = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(subscribed.status, atelier_kernel::ContractStatus::Completed, "{:?}", subscribed.data);
    assert_eq!(subscribed.data.unwrap()["subscribed"], true);

    verifier.send(vec!["v2test.supervisor".into()], "Supervisor", json!({}), json!({}));
    let supervised = terminal(&verifier, Duration::from_secs(10));
    assert_eq!(supervised.status, atelier_kernel::ContractStatus::Completed, "{:?}", supervised.data);
    assert_eq!(supervised.data.unwrap()["routed"], true);

    verifier.send(vec!["v2test.stress".into()], "Stress", json!({}), json!({}));
    let stressed = terminal(&verifier, Duration::from_secs(30));
    assert_eq!(stressed.status, atelier_kernel::ContractStatus::Completed, "{:?}", stressed.data);
    let stress_data = stressed.data.unwrap();
    assert_eq!(stress_data["ok"], true, "{stress_data}");
    assert_eq!(stress_data["pending"], 0, "{stress_data}");
    assert_eq!(stress_data["results"], 0, "{stress_data}");

    let _ = std::fs::remove_dir_all(&tmp);
}
