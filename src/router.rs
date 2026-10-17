use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Sender;

use parking_lot::{Mutex, RwLock};
use serde_json::{Value, json};

use crate::ledger::{KernelLedger, ReceiptTransition};
use crate::logging::KernelLogHub;
use crate::observer::KernelObserver;
use crate::protocol::{
    ContractStatus, DomainStatus, DomainToKernelFrame, HandlerDescriptor, InboundReceipt,
    KERNEL_LOGGER_TARGET, KERNEL_NOTIFY_TARGET, KERNEL_REGISTRY_TARGET, KERNEL_STORAGE_TARGET,
    KERNEL_TELEMETRY_TARGET, KernelMessage, KernelToDomainFrame, MSG_QUERY_HANDLERS,
    MSG_QUERY_LOGS, MSG_QUERY_PROGRESS, MSG_REGISTER_HANDLERS, MSG_UNREGISTER_HANDLERS,
    MessageReceipt, OutboundMessage, now_millis, parse_target_address,
};
use crate::storage::DomainStorageEngine;

/// Internal registration state of a single managed Domain.
struct DomainEntry {
    /// Monotonically increasing process/pipe instance generation ID.
    instance_id: u64,
    status: DomainStatus,
    pipe_tx: Option<Sender<KernelToDomainFrame>>,
    /// Maps `handler_name -> description`.
    handlers: HashMap<String, String>,
}

/// Core Atelier Kernel Router: manages domain routes, generates message IDs,
/// enforces three-level idempotency, intercepts invalid targets (`TargetInvalid`),
/// routes 4-stage receipts, and dispatches storage messages.
pub struct KernelRouter {
    /// Serialize delivery transitions and publication to caller/observer together.
    /// Dedicated pipe I/O runs outside this gate on supervisor pump threads.
    receipt_publication: Mutex<()>,
    next_instance_id: AtomicU64,
    /// Per-domain serialization gate that closes the TOCTOU window between checking `instance_id`
    /// and processing an inbound frame or replacing a domain generation via `attach_domain_pipe`.
    domain_gates: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    domains: RwLock<HashMap<String, DomainEntry>>,
    /// `<domain>.<handler> -> domain` lookup table.
    routes: RwLock<HashMap<String, String>>,
    ledger: Arc<KernelLedger>,
    observer: Arc<KernelObserver>,
    storage: Arc<DomainStorageEngine>,
    logger: Arc<KernelLogHub>,
    /// In-memory ring buffer of recent notifications and telemetry frames for observation consoles.
    ui_inbox: RwLock<Vec<KernelMessage>>,
}

impl KernelRouter {
    pub fn new(
        ledger: Arc<KernelLedger>,
        observer: Arc<KernelObserver>,
        storage: Arc<DomainStorageEngine>,
        logger: Arc<KernelLogHub>,
    ) -> Self {
        Self {
            receipt_publication: Mutex::new(()),
            next_instance_id: AtomicU64::new(1),
            domain_gates: RwLock::new(HashMap::new()),
            domains: RwLock::new(HashMap::new()),
            routes: RwLock::new(HashMap::new()),
            ledger,
            observer,
            storage,
            logger,
            ui_inbox: RwLock::new(Vec::new()),
        }
    }

    pub fn ledger(&self) -> &Arc<KernelLedger> {
        &self.ledger
    }

    pub fn observer(&self) -> &Arc<KernelObserver> {
        &self.observer
    }

    pub fn storage(&self) -> &Arc<DomainStorageEngine> {
        &self.storage
    }

    pub fn logger(&self) -> &Arc<KernelLogHub> {
        &self.logger
    }

    /// Returns a snapshot of recently ingested system notifications and telemetry messages.
    pub fn ui_inbox(&self) -> Vec<KernelMessage> {
        self.ui_inbox.read().clone()
    }

    /// Ingests a message into the UI inbox ring buffer (bounded at 500 entries).
    pub fn push_ui_inbox(&self, msg: KernelMessage) {
        let mut inbox = self.ui_inbox.write();
        inbox.insert(0, msg);
        if inbox.len() > 500 {
            inbox.truncate(500);
        }
    }

    /// Returns the per-domain serialization mutex used to guarantee atomic generation isolation.
    fn domain_gate(&self, domain_id: &str) -> Arc<Mutex<()>> {
        if let Some(gate) = self.domain_gates.read().get(domain_id) {
            return Arc::clone(gate);
        }
        let mut gates = self.domain_gates.write();
        Arc::clone(
            gates
                .entry(domain_id.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    /// Attaches a physical pipe channel (`Kernel -> Domain`) for `domain_id` and returns
    /// the newly assigned process/pipe generation `instance_id`.
    ///
    /// Acquires the per-domain serialization gate so a domain replacement can NEVER interleave
    /// mid-execution of an old instance's `handle_raw_pipe_line_for_instance`.
    pub fn attach_domain_pipe(&self, domain_id: &str, pipe_tx: Sender<KernelToDomainFrame>) -> u64 {
        let gate = self.domain_gate(domain_id);
        let _domain_guard = gate.lock();

        let _ = self.storage.ensure_domain_table(domain_id);
        let mut domains = self.domains.write();
        let mut routes = self.routes.write();

        let instance_id = self.next_instance_id.fetch_add(1, Ordering::Relaxed);
        self.ledger.clear_session_dedup_for_domain(domain_id);

        let entry = domains
            .entry(domain_id.to_string())
            .or_insert_with(|| DomainEntry {
                instance_id,
                status: DomainStatus::Spawning,
                pipe_tx: None,
                handlers: HashMap::new(),
            });
        // Clean up any leftover routes from a prior generation before binding the new instance.
        for h in entry.handlers.keys() {
            routes.remove(&format!("{domain_id}.{h}"));
        }
        entry.handlers.clear();
        entry.instance_id = instance_id;
        entry.status = DomainStatus::Spawning;
        entry.pipe_tx = Some(pipe_tx);
        instance_id
    }

    /// Returns the current active generation `instance_id` for `domain_id`, if attached.
    pub fn domain_instance_id(&self, domain_id: &str) -> Option<u64> {
        self.domains.read().get(domain_id).map(|d| d.instance_id)
    }

    /// Returns the current lifecycle status of `domain_id`.
    pub fn domain_status(&self, domain_id: &str) -> Option<DomainStatus> {
        self.domains.read().get(domain_id).map(|d| d.status)
    }

    /// Returns all currently active `"<domain>.<handler>"` routes.
    pub fn active_routes(&self) -> Vec<String> {
        let mut list: Vec<String> = self.routes.read().keys().cloned().collect();
        list.sort();
        list
    }

    /// Returns a sorted snapshot of all known domains: `(domain_id, status, sorted_routes)`.
    pub fn all_domains(&self) -> Vec<(String, DomainStatus, Vec<String>)> {
        let domains = self.domains.read();
        let mut list: Vec<(String, DomainStatus, Vec<String>)> = domains
            .iter()
            .map(|(domain_id, entry)| {
                let mut routes: Vec<String> = entry
                    .handlers
                    .keys()
                    .map(|h| format!("{domain_id}.{h}"))
                    .collect();
                routes.sort();
                (domain_id.clone(), entry.status, routes)
            })
            .collect();
        list.sort_by(|a, b| a.0.cmp(&b.0));
        list
    }

    /// Queries registered handlers across active domains (with optional domain / target / handler filters),
    /// returning their route address, owning domain, handler name, description, and status.
    pub fn available_handlers(
        &self,
        domain_filter: Option<&str>,
        target_filter: Option<&str>,
        handler_filter: Option<&str>,
        include_inactive: bool,
    ) -> Vec<HandlerDescriptor> {
        let domains = self.domains.read();
        let mut descriptors = Vec::new();
        for (domain_id, entry) in domains.iter() {
            if !include_inactive && entry.status != DomainStatus::Active {
                continue;
            }
            if let Some(df) = domain_filter
                && !df.is_empty()
                && df != domain_id
            {
                continue;
            }
            for (handler, desc) in &entry.handlers {
                let target = format!("{domain_id}.{handler}");
                if let Some(tf) = target_filter
                    && !tf.is_empty()
                    && tf != target
                {
                    continue;
                }
                if let Some(hf) = handler_filter
                    && !hf.is_empty()
                    && hf != handler
                {
                    continue;
                }
                descriptors.push(HandlerDescriptor {
                    target,
                    domain: domain_id.clone(),
                    handler: handler.clone(),
                    description: desc.clone(),
                    status: entry.status,
                });
            }
        }
        descriptors.sort_by(|a, b| a.target.cmp(&b.target));
        descriptors
    }

    /// Looks up the human-readable description for a specific `<domain>.<handler>` target.
    pub fn handler_description(&self, target: &str) -> Option<String> {
        let (domain_id, handler) = parse_target_address(target)?;
        let domains = self.domains.read();
        domains
            .get(domain_id)
            .and_then(|entry| entry.handlers.get(handler).cloned())
    }

    /// Processes a raw JSON line from a specific process generation `instance_id` of `authenticated_domain`.
    /// Holds the per-domain serialization gate across both the `instance_id` check and frame execution,
    /// completely eliminating any TOCTOU window against `attach_domain_pipe`.
    pub fn handle_raw_pipe_line_for_instance(
        &self,
        authenticated_domain: &str,
        instance_id: u64,
        raw_line: &str,
    ) {
        let gate = self.domain_gate(authenticated_domain);
        let _domain_guard = gate.lock();
        if self.domain_instance_id(authenticated_domain) != Some(instance_id) {
            return;
        }
        self.handle_raw_pipe_line_inner(authenticated_domain, raw_line);
    }

    /// Processes a raw JSON line coming from the physical stdout pipe of `authenticated_domain`.
    pub fn handle_raw_pipe_line(&self, authenticated_domain: &str, raw_line: &str) {
        let gate = self.domain_gate(authenticated_domain);
        let _domain_guard = gate.lock();
        self.handle_raw_pipe_line_inner(authenticated_domain, raw_line);
    }

    fn handle_raw_pipe_line_inner(&self, authenticated_domain: &str, raw_line: &str) {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() {
            return;
        }
        match serde_json::from_str::<DomainToKernelFrame>(trimmed) {
            Ok(frame) => {
                self.handle_frame_inner(authenticated_domain, frame);
            }
            Err(err) => {
                let (_, ids, _) = self.ledger.allocate_submission(authenticated_domain, 0, 1);
                let msg_id = ids[0].clone();
                let err_data = Some(json!({
                    "error": "invalid_json_frame",
                    "reason": err.to_string(),
                }));
                let receipt = MessageReceipt {
                    logical_id: self.ledger.logical_id_for_message(&msg_id),
                    message_id: msg_id.clone(),
                    seq: 0,
                    target: "kernel.contract".to_string(),
                    message_type: "MalformedFrame".to_string(),
                    status: ContractStatus::ContractInvalid,
                    data: err_data.clone(),
                    timestamp: now_millis(),
                };
                self.observer.emit(
                    authenticated_domain,
                    "kernel.contract",
                    &msg_id,
                    &self.ledger.logical_id_for_message(&msg_id),
                    0,
                    "MalformedFrame",
                    ContractStatus::ContractInvalid,
                    err_data,
                );
                self.send_to_domain_pipe(
                    authenticated_domain,
                    KernelToDomainFrame::Receipt(receipt),
                );
            }
        }
    }

    /// Processes a structured `DomainToKernelFrame` for a specific `instance_id` of `authenticated_domain`.
    /// Holds the per-domain serialization gate across both the `instance_id` check and frame execution.
    pub fn handle_frame_for_instance(
        &self,
        authenticated_domain: &str,
        instance_id: u64,
        frame: DomainToKernelFrame,
    ) -> Option<Vec<String>> {
        let gate = self.domain_gate(authenticated_domain);
        let _domain_guard = gate.lock();
        if self.domain_instance_id(authenticated_domain) != Some(instance_id) {
            return None;
        }
        self.handle_frame_inner(authenticated_domain, frame)
    }

    /// Processes a structured `DomainToKernelFrame` from `authenticated_domain`.
    /// Returns ordered kernel-assigned child IDs when the frame is a `Send`.
    pub fn handle_frame(
        &self,
        authenticated_domain: &str,
        frame: DomainToKernelFrame,
    ) -> Option<Vec<String>> {
        let gate = self.domain_gate(authenticated_domain);
        let _domain_guard = gate.lock();
        self.handle_frame_inner(authenticated_domain, frame)
    }

    fn handle_frame_inner(
        &self,
        authenticated_domain: &str,
        frame: DomainToKernelFrame,
    ) -> Option<Vec<String>> {
        match frame {
            DomainToKernelFrame::Send(outbound) => {
                Some(self.handle_outbound_message(authenticated_domain, outbound))
            }
            DomainToKernelFrame::Receipt(inbound) => {
                self.handle_inbound_receipt(authenticated_domain, inbound);
                None
            }
            DomainToKernelFrame::Log(payload) => {
                let mut record = payload.into_record();
                record.source_domain = Some(authenticated_domain.to_string());
                if record.component.is_empty() {
                    record.component = authenticated_domain.to_string();
                }
                if record.layer.is_empty() {
                    record.layer = "DOMAIN".to_string();
                }
                self.logger.reinject(record);
                None
            }
        }
    }

    /// Handles a new message submission from `authenticated_domain`.
    /// Generates `message_id` in the kernel and guarantees idempotent delivery.
    pub fn handle_outbound_message(
        &self,
        authenticated_domain: &str,
        outbound: OutboundMessage,
    ) -> Vec<String> {
        let invalid = outbound.targets.is_empty()
            || outbound.message_type.trim().is_empty()
            || !outbound.params.is_object();
        let mut seen = HashSet::new();
        let targets: Vec<String> = if invalid {
            vec!["kernel.contract".into()]
        } else {
            outbound
                .targets
                .iter()
                .filter(|t| seen.insert((*t).clone()))
                .cloned()
                .collect()
        };
        let (logical_id, ids, duplicate) =
            self.ledger
                .allocate_submission(authenticated_domain, outbound.seq, targets.len());
        if duplicate {
            // Snapshot and replay under the publication gate too: otherwise a
            // stale replay can arrive after a newer live receipt.
            let _publication = self.receipt_publication.lock();
            for receipt in self.ledger.receipts_for_logical(&logical_id) {
                self.send_to_domain_pipe(
                    authenticated_domain,
                    KernelToDomainFrame::Receipt(receipt),
                );
            }
            return ids;
        }
        let ingress_ts = now_millis();
        for (target, message_id) in targets.iter().zip(&ids) {
            let kernel_msg = KernelMessage {
                logical_id: logical_id.clone(),
                message_id: message_id.clone(),
                source_domain: authenticated_domain.to_string(),
                target: target.clone(),
                targets: targets.clone(),
                message_type: outbound.message_type.clone(),
                params: outbound.params.clone(),
                timestamp: ingress_ts,
                data: outbound.data.clone(),
            };
            if invalid {
                let data = Some(
                    json!({"error":"contract_validation_failed","reason":"targets must be non-empty, message_type must be non-empty, and params must be a JSON object"}),
                );
                let (record, _) = self.ledger.record_initial_target_state(
                    message_id,
                    outbound.seq,
                    authenticated_domain,
                    target,
                    &outbound.message_type,
                    ContractStatus::ContractInvalid,
                    data.clone(),
                );
                self.observer.emit(
                    authenticated_domain,
                    target,
                    message_id,
                    &self.ledger.logical_id_for_message(message_id),
                    outbound.seq,
                    &outbound.message_type,
                    ContractStatus::ContractInvalid,
                    data,
                );
                self.send_to_domain_pipe(
                    authenticated_domain,
                    KernelToDomainFrame::Receipt(record.to_receipt()),
                );
            } else if target == KERNEL_REGISTRY_TARGET {
                self.dispatch_kernel_registry(authenticated_domain, outbound.seq, &kernel_msg);
            } else if target == KERNEL_STORAGE_TARGET {
                self.dispatch_kernel_storage(authenticated_domain, outbound.seq, &kernel_msg);
            } else if target == KERNEL_LOGGER_TARGET {
                self.dispatch_kernel_logger(authenticated_domain, outbound.seq, &kernel_msg);
            } else if target == KERNEL_TELEMETRY_TARGET
                || target == "web_ui.metric_tick"
                || (target.starts_with("web_ui")
                    && (kernel_msg.message_type == "MetricTick"
                        || kernel_msg.message_type == "StockTelemetry"
                        || kernel_msg.message_type == "SensorTelemetry"))
            {
                self.dispatch_kernel_telemetry(authenticated_domain, outbound.seq, &kernel_msg);
            } else if target == KERNEL_NOTIFY_TARGET
                || target == "web_ui.push_alert"
                || target == "web_ui"
                || target.starts_with("web_ui.")
            {
                self.dispatch_kernel_notify(authenticated_domain, outbound.seq, &kernel_msg);
            } else {
                self.dispatch_domain_target(authenticated_domain, outbound.seq, kernel_msg);
            }
        }
        ids
    }

    /// Dispatches a message targeting `kernel.registry` (`RegisterHandlers` / `UnregisterHandlers` / `QueryHandlers`).
    fn dispatch_kernel_registry(&self, source_domain: &str, seq: u64, msg: &KernelMessage) {
        // Emit Stage 1 (Sent), Stage 2 (Received), Stage 3 (Started)
        self.emit_kernel_internal_stages(source_domain, seq, msg);

        match msg.message_type.as_str() {
            MSG_REGISTER_HANDLERS => {
                let desc_map = msg.params.get("descriptions").and_then(Value::as_object);
                let handlers_opt =
                    msg.params
                        .get("handlers")
                        .and_then(Value::as_array)
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|item| {
                                    if let Some(name) = item.as_str() {
                                        let trimmed = name.trim();
                                        if trimmed.is_empty() || trimmed.contains('.') {
                                            return None;
                                        }
                                        let desc = desc_map
                                            .and_then(|m| m.get(trimmed))
                                            .and_then(Value::as_str)
                                            .unwrap_or("")
                                            .trim()
                                            .to_string();
                                        Some((trimmed.to_string(), desc))
                                    } else if let Some(obj) = item.as_object() {
                                        let name = obj
                                            .get("name")
                                            .or_else(|| obj.get("handler"))
                                            .and_then(Value::as_str)?
                                            .trim();
                                        if name.is_empty() || name.contains('.') {
                                            return None;
                                        }
                                        let desc = obj
                                            .get("description")
                                            .or_else(|| obj.get("doc"))
                                            .and_then(Value::as_str)
                                            .or_else(|| {
                                                desc_map
                                                    .and_then(|m| m.get(name))
                                                    .and_then(Value::as_str)
                                            })
                                            .unwrap_or("")
                                            .trim()
                                            .to_string();
                                        Some((name.to_string(), desc))
                                    } else {
                                        None
                                    }
                                })
                                .collect::<Vec<(String, String)>>()
                        });

                let Some(handlers) = handlers_opt.filter(|h| !h.is_empty()) else {
                    self.complete_kernel_internal_target(
                        source_domain,
                        msg,
                        ContractStatus::ExecutionFailed,
                        Some(json!({
                            "error": "invalid_handlers_param",
                            "reason": "params.handlers must be a non-empty array of handler names",
                        })),
                    );
                    return;
                };

                let _ = self.storage.ensure_domain_table(source_domain);

                let mut registered_routes = Vec::new();
                let mut registered_handlers = Vec::new();
                {
                    let mut domains = self.domains.write();
                    let mut routes = self.routes.write();
                    let entry =
                        domains
                            .entry(source_domain.to_string())
                            .or_insert_with(|| DomainEntry {
                                instance_id: self.next_instance_id.fetch_add(1, Ordering::Relaxed),
                                status: DomainStatus::Active,
                                pipe_tx: None,
                                handlers: HashMap::new(),
                            });
                    entry.status = DomainStatus::Active;

                    for (h, desc) in handlers {
                        let full_route = format!("{source_domain}.{h}");
                        entry.handlers.insert(h.clone(), desc.clone());
                        routes.insert(full_route.clone(), source_domain.to_string());
                        registered_routes.push(full_route.clone());
                        registered_handlers.push(json!({
                            "target": full_route,
                            "domain": source_domain,
                            "handler": h,
                            "description": desc,
                        }));
                    }
                }

                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({
                        "domain": source_domain,
                        "status": "active",
                        "registered_routes": registered_routes,
                        "handlers": registered_handlers,
                    })),
                );
            }
            MSG_UNREGISTER_HANDLERS => {
                let mut removed_routes = Vec::new();
                {
                    let mut domains = self.domains.write();
                    let mut routes = self.routes.write();
                    if let Some(entry) = domains.get_mut(source_domain) {
                        for (h, _) in entry.handlers.drain() {
                            let full_route = format!("{source_domain}.{h}");
                            routes.remove(&full_route);
                            removed_routes.push(full_route);
                        }
                    }
                }
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({
                        "domain": source_domain,
                        "unregistered_routes": removed_routes,
                    })),
                );
            }
            MSG_QUERY_HANDLERS => {
                let domain_filter = msg.params.get("domain").and_then(Value::as_str);
                let target_filter = msg.params.get("target").and_then(Value::as_str);
                let handler_filter = msg.params.get("handler").and_then(Value::as_str);
                let include_inactive = msg
                    .params
                    .get("include_inactive")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);

                let descriptors = self.available_handlers(
                    domain_filter,
                    target_filter,
                    handler_filter,
                    include_inactive,
                );
                let routes: Vec<String> = descriptors.iter().map(|d| d.target.clone()).collect();
                let single_description = if descriptors.len() == 1 {
                    Some(descriptors[0].description.clone())
                } else {
                    target_filter.and_then(|tf| self.handler_description(tf))
                };

                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({
                        "count": descriptors.len(),
                        "routes": routes,
                        "handlers": descriptors,
                        "description": single_description,
                    })),
                );
            }
            other => {
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::ExecutionFailed,
                    Some(json!({
                        "error": "unsupported_registry_message_type",
                        "message_type": other,
                    })),
                );
            }
        }
    }

    /// Dispatches a CRUD message targeting `kernel.storage` ("Storage as Messaging").
    fn dispatch_kernel_storage(&self, source_domain: &str, seq: u64, msg: &KernelMessage) {
        // Emit Stage 1 (Sent), Stage 2 (Received), Stage 3 (Started)
        self.emit_kernel_internal_stages(source_domain, seq, msg);

        match self.storage.execute_message(msg) {
            Ok(result_data) => {
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(result_data),
                );
            }
            Err(err) => {
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::ExecutionFailed,
                    Some(json!({
                        "error": "storage_execution_failed",
                        "reason": err.to_string(),
                    })),
                );
            }
        }
    }

    /// Dispatches a log or progress query message targeting `kernel.logger`.
    fn dispatch_kernel_logger(&self, source_domain: &str, seq: u64, msg: &KernelMessage) {
        self.emit_kernel_internal_stages(source_domain, seq, msg);

        match msg.message_type.as_str() {
            MSG_QUERY_LOGS => {
                let limit = msg
                    .params
                    .get("limit")
                    .and_then(Value::as_u64)
                    .unwrap_or(200) as usize;
                let level = msg.params.get("level").and_then(Value::as_str);
                let layer = msg.params.get("layer").and_then(Value::as_str);
                let component = msg.params.get("component").and_then(Value::as_str);
                let phase = msg.params.get("phase").and_then(Value::as_str);
                let since_ts = msg.params.get("since_ts").and_then(Value::as_f64);
                let domain_id = msg
                    .params
                    .get("domain_id")
                    .or_else(|| msg.params.get("domain"))
                    .or_else(|| msg.params.get("source_domain"))
                    .and_then(Value::as_str);

                match self.logger.query_logs_filtered(
                    limit, level, layer, component, phase, since_ts, domain_id,
                ) {
                    Ok(logs) => {
                        let total = logs.len();
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::Completed,
                            Some(json!({
                                "logs": logs,
                                "total": total,
                            })),
                        );
                    }
                    Err(err) => {
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::ExecutionFailed,
                            Some(json!({
                                "error": "query_logs_failed",
                                "reason": err.to_string(),
                            })),
                        );
                    }
                }
            }
            MSG_QUERY_PROGRESS => {
                let active = self.logger.active_progress();
                let count = active.len();
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({
                        "progress": active,
                        "count": count,
                    })),
                );
            }
            other => {
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::ExecutionFailed,
                    Some(json!({
                        "error": "unsupported_logger_message_type",
                        "message_type": other,
                    })),
                );
            }
        }
    }

    /// Dispatches a high-frequency metric/telemetry stream targeting `kernel.telemetry`.
    fn dispatch_kernel_telemetry(&self, source_domain: &str, seq: u64, msg: &KernelMessage) {
        self.emit_kernel_internal_stages(source_domain, seq, msg);
        self.push_ui_inbox(msg.clone());

        self.complete_kernel_internal_target(
            source_domain,
            msg,
            ContractStatus::Completed,
            Some(json!({
                "status": "recorded",
                "telemetry_type": msg.message_type,
                "source": source_domain,
            })),
        );
    }

    /// Dispatches an asynchronous system notification or alert targeting `kernel.notify`.
    fn dispatch_kernel_notify(&self, source_domain: &str, seq: u64, msg: &KernelMessage) {
        self.emit_kernel_internal_stages(source_domain, seq, msg);
        self.push_ui_inbox(msg.clone());

        let severity = msg
            .params
            .get("severity")
            .or_else(|| msg.params.get("level"))
            .and_then(Value::as_str)
            .unwrap_or("info");

        let text = msg
            .data
            .get("text")
            .or_else(|| msg.data.get("greeting"))
            .or_else(|| msg.data.get("message"))
            .and_then(Value::as_str)
            .unwrap_or(&msg.message_type);

        match severity.to_lowercase().as_str() {
            "warn" | "warning" => self.logger.warning("NOTIFY", source_domain, text),
            "error" | "critical" => self.logger.error("NOTIFY", source_domain, text),
            _ => self.logger.info("NOTIFY", source_domain, text),
        }

        self.complete_kernel_internal_target(
            source_domain,
            msg,
            ContractStatus::Completed,
            Some(json!({
                "status": "delivered",
                "notification_type": msg.message_type,
                "source": source_domain,
            })),
        );
    }

    /// Emits `Sent -> Received -> Started` receipts for kernel built-in targets (`kernel.registry`, `kernel.storage`, `kernel.logger`, etc.).
    fn emit_kernel_internal_stages(&self, source_domain: &str, seq: u64, msg: &KernelMessage) {
        let (sent_rec, created) = self.ledger.record_initial_target_state(
            &msg.message_id,
            seq,
            source_domain,
            &msg.target,
            &msg.message_type,
            ContractStatus::Sent,
            None,
        );
        if !created {
            return;
        }
        self.observer.emit(
            source_domain,
            &msg.target,
            &msg.message_id,
            &self.ledger.logical_id_for_message(&msg.message_id),
            seq,
            &msg.message_type,
            ContractStatus::Sent,
            None,
        );
        self.send_to_domain_pipe(
            source_domain,
            KernelToDomainFrame::Receipt(sent_rec.to_receipt()),
        );

        for stage in [ContractStatus::Received, ContractStatus::Started] {
            if let ReceiptTransition::Advanced { receipt, .. } =
                self.ledger
                    .advance_status("kernel", &msg.message_id, &msg.target, stage, None)
            {
                self.observer.emit(
                    source_domain,
                    &msg.target,
                    &msg.message_id,
                    &self.ledger.logical_id_for_message(&msg.message_id),
                    seq,
                    &msg.message_type,
                    stage,
                    None,
                );
                self.send_to_domain_pipe(source_domain, KernelToDomainFrame::Receipt(receipt));
            }
        }
    }

    fn complete_kernel_internal_target(
        &self,
        source_domain: &str,
        msg: &KernelMessage,
        final_status: ContractStatus,
        data: Option<Value>,
    ) {
        if let ReceiptTransition::Advanced { receipt, .. } = self.ledger.advance_status(
            "kernel",
            &msg.message_id,
            &msg.target,
            final_status,
            data.clone(),
        ) {
            self.observer.emit(
                source_domain,
                &msg.target,
                &msg.message_id,
                &self.ledger.logical_id_for_message(&msg.message_id),
                receipt.seq,
                &msg.message_type,
                final_status,
                data,
            );
            self.send_to_domain_pipe(source_domain, KernelToDomainFrame::Receipt(receipt));
        }
    }

    /// Validates and routes a message targeting a business domain handler (`"<domain>.<handler>"`).
    /// If the target does not exist or is not Active, does NOT send the message and immediately returns `TargetInvalid`.
    fn dispatch_domain_target(&self, source_domain: &str, seq: u64, msg: KernelMessage) {
        let publication = self.receipt_publication.lock();
        let target = msg.target.clone();

        // Check route table and target domain active pipe along with its generation `instance_id`.
        let target_pipe_opt = {
            let domains = self.domains.read();
            let routes = self.routes.read();
            routes.get(&target).and_then(|target_domain| {
                domains.get(target_domain).and_then(|entry| {
                    if entry.status == DomainStatus::Active {
                        entry.pipe_tx.clone().map(|tx| (entry.instance_id, tx))
                    } else {
                        None
                    }
                })
            })
        };

        let Some((target_instance_id, target_pipe)) = target_pipe_opt else {
            // Core Rule: Target does not exist or is not ready -> DO NOT SEND, return `TargetInvalid`.
            let err_data = Some(json!({
                "error": "target_not_found_or_inactive",
                "target": target,
            }));
            let (rec, created) = self.ledger.record_initial_target_state(
                &msg.message_id,
                seq,
                source_domain,
                &target,
                &msg.message_type,
                ContractStatus::TargetInvalid,
                err_data.clone(),
            );
            if created {
                self.observer.emit(
                    source_domain,
                    &target,
                    &msg.message_id,
                    &self.ledger.logical_id_for_message(&msg.message_id),
                    seq,
                    &msg.message_type,
                    ContractStatus::TargetInvalid,
                    err_data,
                );
                self.send_to_domain_pipe(
                    source_domain,
                    KernelToDomainFrame::Receipt(rec.to_receipt()),
                );
            }
            return;
        };

        // Level 2 Idempotency: Record `Sent` in ledger before writing to pipe.
        let (sent_rec, created) = self.ledger.record_initial_target_state(
            &msg.message_id,
            seq,
            source_domain,
            &target,
            &msg.message_type,
            ContractStatus::Sent,
            None,
        );
        if !created {
            return;
        }

        // Write `Deliver(KernelMessage)` to target domain pipe.
        if target_pipe
            .send(KernelToDomainFrame::Deliver(msg.clone()))
            .is_err()
        {
            // Target pipe broke right as we tried to send; only fault if `target_instance_id` is still current.
            // Fault publication takes the same gate, so release it before delegating.
            drop(publication);
            if let Some((target_domain, _)) = parse_target_address(&target) {
                self.mark_domain_faulted_for_instance(
                    target_domain,
                    Some(target_instance_id),
                    "target stdio pipe broken on send",
                );
            }
            return;
        }

        // Emit Stage 1 (`Sent`) receipt to sender and observer.
        self.observer.emit(
            source_domain,
            &target,
            &msg.message_id,
            &self.ledger.logical_id_for_message(&msg.message_id),
            seq,
            &msg.message_type,
            ContractStatus::Sent,
            None,
        );
        self.send_to_domain_pipe(
            source_domain,
            KernelToDomainFrame::Receipt(sent_rec.to_receipt()),
        );
    }

    /// Processes an inbound receipt (`Received`, `Started`, `Completed`, `ExecutionFailed`) reported by `reporting_domain`.
    pub fn handle_inbound_receipt(&self, reporting_domain: &str, inbound: InboundReceipt) {
        if !inbound.status.is_valid_inbound_receipt() {
            return;
        }

        let _publication = self.receipt_publication.lock();

        match self.ledger.advance_status(
            reporting_domain,
            &inbound.message_id,
            &inbound.target,
            inbound.status,
            inbound.data.clone(),
        ) {
            ReceiptTransition::Advanced {
                source_domain,
                receipt,
            } => {
                self.observer.emit(
                    &source_domain,
                    &receipt.target,
                    &receipt.message_id,
                    &self.ledger.logical_id_for_message(&receipt.message_id),
                    receipt.seq,
                    &receipt.message_type,
                    receipt.status,
                    receipt.data.clone(),
                );
                self.send_to_domain_pipe(&source_domain, KernelToDomainFrame::Receipt(receipt));
            }
            ReceiptTransition::DuplicateOrStale
            | ReceiptTransition::UnknownDelivery
            | ReceiptTransition::UnauthorizedTarget => {
                // Absorbed or rejected by kernel idempotency & physical identity guard.
            }
        }
    }

    /// Atomically revokes a faulted/stopped domain's routes and fails all in-flight deliveries targeting it,
    /// without checking generation `instance_id`.
    pub fn mark_domain_faulted(&self, domain_id: &str, reason: &str) -> bool {
        self.mark_domain_faulted_for_instance(domain_id, None, reason)
    }

    /// Atomically revokes a faulted/stopped domain's routes ONLY IF `expected_instance_id` (when provided)
    /// matches the currently active generation `instance_id` of `domain_id`.
    /// Prevents a stale reader thread from an old process generation from revoking a newly respawned domain's routes.
    pub fn mark_domain_faulted_for_instance(
        &self,
        domain_id: &str,
        expected_instance_id: Option<u64>,
        reason: &str,
    ) -> bool {
        let _publication = self.receipt_publication.lock();
        {
            let mut domains = self.domains.write();
            let mut routes = self.routes.write();
            let Some(entry) = domains.get_mut(domain_id) else {
                return false;
            };
            if let Some(expected_id) = expected_instance_id
                && entry.instance_id != expected_id
            {
                // Stale fault notification from an older process instance; ignore safely.
                return false;
            }
            entry.status = DomainStatus::Faulted;
            entry.pipe_tx = None;
            for h in entry.handlers.keys() {
                routes.remove(&format!("{domain_id}.{h}"));
            }
            entry.handlers.clear();
        }

        let failed_deliveries = self.ledger.fail_inflight_for_domain(domain_id, reason);
        for (source_domain, receipt) in failed_deliveries {
            self.observer.emit(
                &source_domain,
                &receipt.target,
                &receipt.message_id,
                &self.ledger.logical_id_for_message(&receipt.message_id),
                receipt.seq,
                &receipt.message_type,
                receipt.status,
                receipt.data.clone(),
            );
            self.send_to_domain_pipe(&source_domain, KernelToDomainFrame::Receipt(receipt));
        }
        true
    }

    fn send_to_domain_pipe(&self, domain_id: &str, frame: KernelToDomainFrame) {
        let pipe_opt = self
            .domains
            .read()
            .get(domain_id)
            .and_then(|d| d.pipe_tx.clone());
        if let Some(tx) = pipe_opt {
            let _ = tx.send(frame);
        }
    }
}
