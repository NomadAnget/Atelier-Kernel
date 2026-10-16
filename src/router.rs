use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Sender;

use parking_lot::RwLock;
use serde_json::{Value, json};

use crate::ledger::{KernelLedger, ReceiptTransition};
use crate::observer::KernelObserver;
use crate::protocol::{
    ContractStatus, DomainStatus, DomainToKernelFrame, InboundReceipt, KERNEL_REGISTRY_TARGET,
    KERNEL_STORAGE_TARGET, KernelMessage, KernelToDomainFrame, MSG_REGISTER_HANDLERS,
    MSG_UNREGISTER_HANDLERS, MessageReceipt, OutboundMessage, now_millis, parse_target_address,
};
use crate::storage::DomainStorageEngine;

/// Internal registration state of a single managed Domain.
struct DomainEntry {
    /// Monotonically increasing process/pipe instance generation ID.
    instance_id: u64,
    status: DomainStatus,
    pipe_tx: Option<Sender<KernelToDomainFrame>>,
    handlers: HashSet<String>,
}

/// Core Atelier Kernel Router: manages domain routes, generates message IDs,
/// enforces three-level idempotency, intercepts invalid targets (`TargetInvalid`),
/// routes 4-stage receipts, and dispatches storage messages.
pub struct KernelRouter {
    next_instance_id: AtomicU64,
    domains: RwLock<HashMap<String, DomainEntry>>,
    /// `<domain>.<handler> -> domain` lookup table.
    routes: RwLock<HashMap<String, String>>,
    ledger: Arc<KernelLedger>,
    observer: Arc<KernelObserver>,
    storage: Arc<DomainStorageEngine>,
}

impl KernelRouter {
    pub fn new(
        ledger: Arc<KernelLedger>,
        observer: Arc<KernelObserver>,
        storage: Arc<DomainStorageEngine>,
    ) -> Self {
        Self {
            next_instance_id: AtomicU64::new(1),
            domains: RwLock::new(HashMap::new()),
            routes: RwLock::new(HashMap::new()),
            ledger,
            observer,
            storage,
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

    /// Attaches a physical pipe channel (`Kernel -> Domain`) for `domain_id` and returns
    /// the newly assigned process/pipe generation `instance_id`.
    /// Called by the Kernel Supervisor when spawning a domain process.
    pub fn attach_domain_pipe(&self, domain_id: &str, pipe_tx: Sender<KernelToDomainFrame>) -> u64 {
        let instance_id = self.next_instance_id.fetch_add(1, Ordering::Relaxed);
        self.ledger.clear_session_dedup_for_domain(domain_id);
        let _ = self.storage.ensure_domain_table(domain_id);
        let mut domains = self.domains.write();
        let mut routes = self.routes.write();
        let entry = domains
            .entry(domain_id.to_string())
            .or_insert_with(|| DomainEntry {
                instance_id,
                status: DomainStatus::Spawning,
                pipe_tx: None,
                handlers: HashSet::new(),
            });
        // Clean up any leftover routes from a prior generation before binding the new instance.
        for h in &entry.handlers {
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
                    .iter()
                    .map(|h| format!("{domain_id}.{h}"))
                    .collect();
                routes.sort();
                (domain_id.clone(), entry.status, routes)
            })
            .collect();
        list.sort_by(|a, b| a.0.cmp(&b.0));
        list
    }

    /// Processes a raw JSON line from a specific process generation `instance_id` of `authenticated_domain`.
    /// Ignores lines coming from stale/replaced reader threads.
    pub fn handle_raw_pipe_line_for_instance(
        &self,
        authenticated_domain: &str,
        instance_id: u64,
        raw_line: &str,
    ) {
        if self.domain_instance_id(authenticated_domain) != Some(instance_id) {
            return;
        }
        self.handle_raw_pipe_line(authenticated_domain, raw_line);
    }

    /// Processes a raw JSON line coming from the physical stdout pipe of `authenticated_domain`.
    pub fn handle_raw_pipe_line(&self, authenticated_domain: &str, raw_line: &str) {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() {
            return;
        }
        match serde_json::from_str::<DomainToKernelFrame>(trimmed) {
            Ok(frame) => {
                self.handle_frame(authenticated_domain, frame);
            }
            Err(err) => {
                let msg_id = self.ledger.generate_message_id();
                let err_data = Some(json!({
                    "error": "invalid_json_frame",
                    "reason": err.to_string(),
                }));
                let receipt = MessageReceipt {
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

    /// Processes a structured `DomainToKernelFrame` from `authenticated_domain`.
    /// Returns the kernel-assigned `message_id` when the frame is a `Send`.
    pub fn handle_frame(
        &self,
        authenticated_domain: &str,
        frame: DomainToKernelFrame,
    ) -> Option<String> {
        match frame {
            DomainToKernelFrame::Send(outbound) => {
                Some(self.handle_outbound_message(authenticated_domain, outbound))
            }
            DomainToKernelFrame::Receipt(inbound) => {
                self.handle_inbound_receipt(authenticated_domain, inbound);
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
    ) -> String {
        // 1. Validate outer contract metadata.
        if outbound.targets.is_empty()
            || outbound.message_type.trim().is_empty()
            || !outbound.params.is_object()
        {
            let msg_id = self.ledger.generate_message_id();
            let err_data = Some(json!({
                "error": "contract_validation_failed",
                "reason": "targets must be non-empty, message_type must be non-empty, and params must be a JSON object",
            }));
            let (rec, _) = self.ledger.record_initial_target_state(
                &msg_id,
                outbound.seq,
                authenticated_domain,
                "kernel.contract",
                &outbound.message_type,
                ContractStatus::ContractInvalid,
                err_data.clone(),
            );
            self.observer.emit(
                authenticated_domain,
                "kernel.contract",
                &msg_id,
                outbound.seq,
                &outbound.message_type,
                ContractStatus::ContractInvalid,
                err_data,
            );
            self.send_to_domain_pipe(
                authenticated_domain,
                KernelToDomainFrame::Receipt(rec.to_receipt()),
            );
            return msg_id;
        }

        // 2. Level 1 Idempotency: allocate kernel `message_id` or deduplicate by `(authenticated_domain, seq)`.
        let (message_id, is_duplicate_submission) = self
            .ledger
            .allocate_or_get_message_id(authenticated_domain, outbound.seq);

        if is_duplicate_submission {
            // Replay current authoritative receipts for this `message_id` without re-delivering!
            for receipt in self.ledger.receipts_for_message(&message_id) {
                self.send_to_domain_pipe(
                    authenticated_domain,
                    KernelToDomainFrame::Receipt(receipt),
                );
            }
            return message_id;
        }

        // 3. Deduplicate target list while preserving order.
        let mut seen_targets = HashSet::new();
        let unique_targets: Vec<String> = outbound
            .targets
            .iter()
            .filter(|t| seen_targets.insert((*t).clone()))
            .cloned()
            .collect();

        let ingress_ts = now_millis();

        // 4. Route each target independently.
        for target in &unique_targets {
            let kernel_msg = KernelMessage {
                message_id: message_id.clone(),
                source_domain: authenticated_domain.to_string(),
                target: target.clone(),
                targets: unique_targets.clone(),
                message_type: outbound.message_type.clone(),
                params: outbound.params.clone(),
                timestamp: ingress_ts,
                data: outbound.data.clone(),
            };

            if target == KERNEL_REGISTRY_TARGET {
                self.dispatch_kernel_registry(authenticated_domain, outbound.seq, &kernel_msg);
            } else if target == KERNEL_STORAGE_TARGET {
                self.dispatch_kernel_storage(authenticated_domain, outbound.seq, &kernel_msg);
            } else {
                self.dispatch_domain_target(authenticated_domain, outbound.seq, kernel_msg);
            }
        }

        message_id
    }

    /// Dispatches a message targeting `kernel.registry` (`RegisterHandlers` / `UnregisterHandlers`).
    fn dispatch_kernel_registry(&self, source_domain: &str, seq: u64, msg: &KernelMessage) {
        // Emit Stage 1 (Sent), Stage 2 (Received), Stage 3 (Started)
        self.emit_kernel_internal_stages(source_domain, seq, msg);

        match msg.message_type.as_str() {
            MSG_REGISTER_HANDLERS => {
                let handlers_opt = msg
                    .params
                    .get("handlers")
                    .and_then(Value::as_array)
                    .map(|arr| {
                        arr.iter()
                            .filter_map(Value::as_str)
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty() && !s.contains('.'))
                            .collect::<Vec<String>>()
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
                                handlers: HashSet::new(),
                            });
                    entry.status = DomainStatus::Active;

                    for h in handlers {
                        let full_route = format!("{source_domain}.{h}");
                        entry.handlers.insert(h);
                        routes.insert(full_route.clone(), source_domain.to_string());
                        registered_routes.push(full_route);
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
                    })),
                );
            }
            MSG_UNREGISTER_HANDLERS => {
                let mut removed_routes = Vec::new();
                {
                    let mut domains = self.domains.write();
                    let mut routes = self.routes.write();
                    if let Some(entry) = domains.get_mut(source_domain) {
                        for h in entry.handlers.drain() {
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

    /// Emits `Sent -> Received -> Started` receipts for kernel built-in targets (`kernel.registry`, `kernel.storage`).
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
        let target = msg.target.clone();

        // Check route table and target domain active pipe along with its generation `instance_id`.
        let target_pipe_opt = {
            let routes = self.routes.read();
            let domains = self.domains.read();
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
            for h in &entry.handlers {
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
