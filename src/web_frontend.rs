use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    AtelierKernel, ContractStatus, DomainPipeClient, DomainStatus, KernelBootConfig, KernelMessage,
    KernelToDomainFrame, MessageReceipt,
};

const DASHBOARD_HTML: &str = include_str!("web_dashboard.html");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SendApiRequest {
    targets: Vec<String>,
    message_type: String,
    #[serde(default)]
    seq: u64,
    #[serde(default)]
    params: Value,
    #[serde(default)]
    data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DomainControlRequest {
    domain_id: String,
    action: String,
    #[serde(default)]
    enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
struct FlowSummary {
    message_id: String,
    seq: u64,
    source_domain: String,
    target: String,
    message_type: String,
    stages: Vec<ContractStatus>,
    latest_status: ContractStatus,
    details: Option<Value>,
    last_ts: i64,
}

/// Web Frontend Domain (`web_ui`) plugged into `AtelierKernel`.
///
/// Communicates with `AtelierKernel` strictly through its bound domain pipe (`domain_id = "web_ui"`),
/// while serving an interactive browser dashboard & real-time `KernelObserver` visualizer.
pub struct WebFrontendDomain {
    kernel: Arc<AtelierKernel>,
    client: Arc<DomainPipeClient>,
    inbox_history: Arc<Mutex<Vec<KernelMessage>>>,
    receipt_history: Arc<Mutex<Vec<MessageReceipt>>>,
}

impl WebFrontendDomain {
    /// Attaches the `web_ui` domain to `kernel`, registers `web_ui.push_alert` & `web_ui.metric_tick`,
    /// and boots all domains configured in `boot_config` (`kernel_boot.json`).
    pub fn attach_with_config(kernel: Arc<AtelierKernel>, boot_config: &KernelBootConfig) -> Self {
        let client = Arc::new(kernel.attach_domain("web_ui"));
        client.register_handlers(&["push_alert", "metric_tick"]);
        let _ = client.drain_frames();

        let inbox_history = Arc::new(Mutex::new(Vec::new()));
        let receipt_history = Arc::new(Mutex::new(Vec::new()));

        // Background pump for `web_ui`'s own domain pipe
        let client_bg = Arc::clone(&client);
        let inbox_bg = Arc::clone(&inbox_history);
        let receipt_bg = Arc::clone(&receipt_history);
        thread::Builder::new()
            .name("web-ui-domain-pipe-pump".to_string())
            .spawn(move || {
                while let Ok(frame) = client_bg.recv_frame() {
                    match frame {
                        KernelToDomainFrame::Deliver(msg) => {
                            // Emit 4-stage receipts back to Kernel for messages delivered to `web_ui.*`
                            client_bg.report_receipt(
                                &msg.message_id,
                                &msg.target,
                                ContractStatus::Received,
                                None,
                            );
                            client_bg.report_receipt(
                                &msg.message_id,
                                &msg.target,
                                ContractStatus::Started,
                                None,
                            );
                            client_bg.report_receipt(
                                &msg.message_id,
                                &msg.target,
                                ContractStatus::Completed,
                                Some(json!({"rendered_by": "web_ui"})),
                            );

                            let mut inbox = inbox_bg.lock();
                            inbox.insert(0, msg);
                            if inbox.len() > 40 {
                                inbox.truncate(40);
                            }
                        }
                        KernelToDomainFrame::Receipt(r) => {
                            let mut recs = receipt_bg.lock();
                            recs.insert(0, r);
                            if recs.len() > 50 {
                                recs.truncate(50);
                            }
                        }
                    }
                }
            })
            .ok();

        // Dynamically boot all external domains configured in `kernel_boot.json`
        kernel.supervisor().boot_from_config(boot_config);

        // Wait up to 800ms for configured domains to complete registration handshake
        for _ in 0..40 {
            let all_ready = boot_config.domains.iter().all(|spec| {
                kernel.router().domain_status(&spec.domain_id) == Some(DomainStatus::Active)
            });
            if all_ready {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }

        Self {
            kernel,
            client,
            inbox_history,
            receipt_history,
        }
    }

    /// Starts the local Web UI server for the `web_ui` domain.
    pub fn serve_http(self: Arc<Self>, bind_addr: &str) -> std::io::Result<()> {
        let listener = TcpListener::bind(bind_addr)?;
        println!("============================================================================");
        println!("  Atelier Kernel V3.1 — 网页前端域 (`web_ui`) & 实时内核观察页面已启动！");
        println!("  👉 请在浏览器打开: http://{bind_addr}");
        println!("----------------------------------------------------------------------------");
        println!("  当前已挂载域与自动路由列表: {:?}", self.kernel.router().active_routes());
        println!("============================================================================");

        for stream in listener.incoming().flatten() {
            let slf = Arc::clone(&self);
            thread::spawn(move || {
                let _ = slf.handle_http_connection(stream);
            });
        }
        Ok(())
    }

    fn handle_http_connection(&self, mut stream: TcpStream) -> std::io::Result<()> {
        let _ = stream.set_read_timeout(Some(Duration::from_secs(3)));
        let mut buf = [0u8; 65536];
        let n = stream.read(&mut buf)?;
        if n == 0 {
            return Ok(());
        }
        let req_str = String::from_utf8_lossy(&buf[..n]);
        let mut lines = req_str.lines();
        let first_line = lines.next().unwrap_or("");
        let parts: Vec<&str> = first_line.split_whitespace().collect();
        if parts.len() < 2 {
            return Ok(());
        }
        let method = parts[0];
        let path = parts[1];

        let body_str = req_str
            .split_once("\r\n\r\n")
            .map(|(_, b)| b)
            .or_else(|| req_str.split_once("\n\n").map(|(_, b)| b))
            .unwrap_or("");

        match (method, path) {
            ("GET", "/") | ("GET", "/index.html") => self.write_response(
                &mut stream,
                "200 OK",
                "text/html; charset=utf-8",
                DASHBOARD_HTML.as_bytes(),
            ),
            ("GET", p) if p.starts_with("/api/snapshot") => {
                let snap = self.build_snapshot_json();
                let bytes = serde_json::to_vec(&snap).unwrap_or_default();
                self.write_response(&mut stream, "200 OK", "application/json", &bytes)
            }
            ("POST", "/api/send") => {
                if let Ok(req) = serde_json::from_str::<SendApiRequest>(body_str) {
                    let params = if req.params.is_object() {
                        req.params
                    } else {
                        json!({})
                    };
                    let msg_id = if req.seq > 0 {
                        self.client.send_with_seq(
                            req.seq,
                            req.targets,
                            req.message_type,
                            params,
                            req.data,
                        )
                    } else {
                        self.client
                            .send(req.targets, req.message_type, params, req.data)
                    };
                    let resp = json!({ "ok": true, "message_id": msg_id });
                    let bytes = serde_json::to_vec(&resp).unwrap_or_default();
                    self.write_response(&mut stream, "200 OK", "application/json", &bytes)
                } else {
                    self.write_response(
                        &mut stream,
                        "400 Bad Request",
                        "application/json",
                        b"{\"error\":\"invalid json\"}",
                    )
                }
            }
            ("POST", "/api/domain_control") => {
                if let Ok(req) = serde_json::from_str::<DomainControlRequest>(body_str) {
                    self.handle_domain_control(req);
                    self.write_response(&mut stream, "200 OK", "application/json", b"{\"ok\":true}")
                } else {
                    self.write_response(
                        &mut stream,
                        "400 Bad Request",
                        "application/json",
                        b"{\"error\":\"invalid control payload\"}",
                    )
                }
            }
            ("GET", p) if p.starts_with("/api/logs/stream") => {
                self.handle_logs_stream(stream)
            }
            ("GET", p) if p.starts_with("/api/logs") => {
                self.handle_query_logs(&mut stream, p)
            }
            _ => self.write_response(&mut stream, "404 Not Found", "text/plain", b"Not Found"),
        }
    }

    fn handle_domain_control(&self, req: DomainControlRequest) {
        match req.action.as_str() {
            "stop" => {
                self.kernel.supervisor().stop_process_domain(&req.domain_id);
            }
            "start" | "restart" => {
                let _ = self.kernel.supervisor().respawn_domain(&req.domain_id);
            }
            "toggle_traffic" => {
                let enabled = req.enabled.unwrap_or(true);
                // Dynamically discover all active `.set_auto_traffic` handlers across any registered domains
                let targets: Vec<String> = self
                    .kernel
                    .router()
                    .active_routes()
                    .into_iter()
                    .filter(|r| r.ends_with(".set_auto_traffic"))
                    .collect();
                if !targets.is_empty() {
                    self.client.send(
                        targets,
                        "SetAutoTraffic",
                        json!({}),
                        json!({ "enabled": enabled }),
                    );
                }
            }
            "burst" => {
                // Dynamically pick up to 4 active business handlers across registered domains
                let targets: Vec<String> = self
                    .kernel
                    .router()
                    .active_routes()
                    .into_iter()
                    .filter(|r| !r.starts_with("web_ui.") && !r.ends_with(".set_auto_traffic"))
                    .take(4)
                    .collect();
                if !targets.is_empty() {
                    self.client.send(
                        targets,
                        "ManualTrafficBurst",
                        json!({ "burst": true }),
                        json!({ "sku": "ATELIER-GEAR-01", "qty": 1 }),
                    );
                }
            }
            _ => {}
        }
    }

    fn handle_logs_stream(&self, mut stream: TcpStream) -> std::io::Result<()> {
        let _ = stream.set_read_timeout(None);
        let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
        let header = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream; charset=utf-8\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\nAccess-Control-Allow-Origin: *\r\n\r\n";
        stream.write_all(header.as_bytes())?;
        stream.flush()?;

        // Send active ephemeral progress items first
        for prog in self.kernel.logger().active_progress() {
            let json_str = serde_json::to_string(&prog).unwrap_or_default();
            let chunk = format!("data: {json_str}\n\n");
            if stream.write_all(chunk.as_bytes()).is_err() || stream.flush().is_err() {
                return Ok(());
            }
        }

        let rx = self.kernel.logger().subscribe();
        while let Ok(rec) = rx.recv() {
            let json_str = serde_json::to_string(&rec).unwrap_or_default();
            let chunk = format!("data: {json_str}\n\n");
            if stream.write_all(chunk.as_bytes()).is_err() || stream.flush().is_err() {
                break;
            }
        }
        Ok(())
    }

    fn handle_query_logs(&self, stream: &mut TcpStream, path: &str) -> std::io::Result<()> {
        let query_str = path.split_once('?').map(|(_, q)| q).unwrap_or("");
        let mut level: Option<String> = None;
        let mut layer: Option<String> = None;
        let mut component: Option<String> = None;
        let mut phase: Option<String> = None;
        let mut limit = 200usize;

        for pair in query_str.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                match k {
                    "level" if !v.is_empty() => level = Some(v.to_string()),
                    "layer" if !v.is_empty() => layer = Some(v.to_string()),
                    "component" if !v.is_empty() => component = Some(v.to_string()),
                    "phase" if !v.is_empty() => phase = Some(v.to_string()),
                    "limit" => {
                        if let Ok(l) = v.parse::<usize>() {
                            limit = l;
                        }
                    }
                    _ => {}
                }
            }
        }

        let logs = self
            .kernel
            .logger()
            .query_logs(
                limit,
                level.as_deref(),
                layer.as_deref(),
                component.as_deref(),
                phase.as_deref(),
                None,
            )
            .unwrap_or_default();

        let active_progress = self.kernel.logger().active_progress();

        let resp = json!({
            "logs": logs,
            "active_progress": active_progress,
        });
        let bytes = serde_json::to_vec(&resp).unwrap_or_default();
        self.write_response(stream, "200 OK", "application/json", &bytes)
    }

    fn build_snapshot_json(&self) -> Value {
        let active_routes = self.kernel.router().active_routes();
        let all_events = self.kernel.observer().snapshot();
        let total_events = all_events.len();
        let invalid_count = all_events
            .iter()
            .filter(|e| e.status == ContractStatus::TargetInvalid)
            .count();

        // Dynamically combine all known domains from Router and Supervisor specs (zero hardcoded domain names)
        let router_domains = self.kernel.router().all_domains();
        let specs = self.kernel.supervisor().list_specs();
        let mut seen_domains = HashSet::new();
        let mut domains_json = Vec::new();

        for (domain_id, status, routes) in router_domains {
            seen_domains.insert(domain_id.clone());
            let spec_opt = self.kernel.supervisor().get_spec(&domain_id);
            let is_process = spec_opt.is_some();
            let runtime = match spec_opt {
                Some(spec) => format!("{} (Stdio 进程)", spec.command),
                None => "Rust (内置宿主域)".to_string(),
            };
            domains_json.push(json!({
                "domain_id": domain_id,
                "runtime": runtime,
                "is_process": is_process,
                "status": status,
                "routes": routes,
            }));
        }

        for spec in specs {
            if !seen_domains.contains(&spec.domain_id) {
                domains_json.push(json!({
                    "domain_id": spec.domain_id,
                    "runtime": format!("{} (Stdio 进程)", spec.command),
                    "is_process": true,
                    "status": DomainStatus::Stopped,
                    "routes": Vec::<String>::new(),
                }));
            }
        }

        // Build grouped 4-stage flows ordered from newest to oldest
        let mut flow_map: HashMap<(String, String), FlowSummary> = HashMap::new();
        let mut flow_order: Vec<(String, String)> = Vec::new();

        for ev in &all_events {
            let key = (ev.message_id.clone(), ev.target.clone());
            let entry = flow_map.entry(key.clone()).or_insert_with(|| {
                flow_order.push(key);
                FlowSummary {
                    message_id: ev.message_id.clone(),
                    seq: ev.seq,
                    source_domain: ev.source_domain.clone(),
                    target: ev.target.clone(),
                    message_type: ev.message_type.clone(),
                    stages: Vec::new(),
                    latest_status: ev.status,
                    details: None,
                    last_ts: ev.timestamp,
                }
            });
            if !entry.stages.contains(&ev.status) {
                entry.stages.push(ev.status);
            }
            entry.latest_status = ev.status;
            if ev.details.is_some() {
                entry.details = ev.details.clone();
            }
            entry.last_ts = ev.timestamp;
        }

        let flows: Vec<FlowSummary> = flow_order
            .into_iter()
            .rev()
            .take(45)
            .filter_map(|k| flow_map.remove(&k))
            .collect();

        let recent_events: Vec<_> = all_events.into_iter().rev().take(60).collect();
        let webui_inbox = self.inbox_history.lock().clone();
        let webui_receipts = self.receipt_history.lock().clone();
        let recent_logs = self.kernel.logger().recent_logs();
        let active_progress = self.kernel.logger().active_progress();

        json!({
            "active_routes": active_routes,
            "total_events": total_events,
            "invalid_count": invalid_count,
            "domains": domains_json,
            "flows": flows,
            "recent_events": recent_events,
            "webui_inbox": webui_inbox,
            "webui_receipts": webui_receipts,
            "recent_logs": recent_logs,
            "active_progress": active_progress,
        })
    }

    fn write_response(
        &self,
        stream: &mut TcpStream,
        status: &str,
        content_type: &str,
        body: &[u8],
    ) -> std::io::Result<()> {
        let header = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(header.as_bytes())?;
        stream.write_all(body)?;
        stream.flush()
    }
}
