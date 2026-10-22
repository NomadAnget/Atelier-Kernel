use crate::outbox::PipeSender;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use serde_json::{Value, json};

use crate::ledger::{KernelLedger, ReceiptTransition};
use crate::logging::{KernelLogHub, LogQuery, LogRecord};
use crate::observer::KernelObserver;
use crate::protocol::{
    ContractStatus, DomainStatus, DomainToKernelFrame, HandlerDescriptor, InboundReceipt,
    KERNEL_LOGGER_TARGET, KERNEL_NOTIFY_TARGET, KERNEL_OBSERVER_TARGET, KERNEL_REGISTRY_TARGET,
    KERNEL_RUNTIME_TARGET, KERNEL_STORAGE_TARGET, KERNEL_SUPERVISOR_TARGET,
    KERNEL_TELEMETRY_TARGET, KernelMessage, KernelToDomainFrame, MSG_QUERY_DOMAIN_ASSEMBLY,
    MSG_QUERY_HANDLERS, MSG_QUERY_LOGS, MSG_QUERY_OBSERVER, MSG_QUERY_PROGRESS, MSG_QUERY_RUNTIME,
    MSG_REGISTER_HANDLERS, MSG_RESTART_DOMAIN, MSG_START_DOMAIN, MSG_STOP_DOMAIN,
    MSG_SUBSCRIBE_LOGS, MSG_SUBSCRIBE_NOTIFY, MSG_SUBSCRIBE_OBSERVER, MSG_SUBSCRIBE_TELEMETRY,
    MSG_SYSTEM_LOG_EVENT, MSG_SYSTEM_NOTIFY_EVENT, MSG_SYSTEM_OBSERVER_EVENT,
    MSG_SYSTEM_TELEMETRY_EVENT, MSG_UNREGISTER_HANDLERS, MSG_UNSUBSCRIBE_LOGS,
    MSG_UNSUBSCRIBE_NOTIFY, MSG_UNSUBSCRIBE_OBSERVER, MSG_UNSUBSCRIBE_TELEMETRY, MSG_UPDATE_DOMAIN,
    MessageReceipt, ObservationEvent, OutboundMessage, now_millis, parse_target_address,
};
use crate::storage::DomainStorageEngine;

struct ControlPermit {
    count: Arc<AtomicUsize>,
}
impl Drop for ControlPermit {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Filter options registered by a domain subscribing to kernel log events.
#[derive(Debug, Clone)]
pub struct LogSubscriptionFilter {
    pub min_level: Option<u8>,
    pub source_domain: Option<String>,
}

fn level_to_weight(level: &str) -> u8 {
    match level.trim().to_uppercase().as_str() {
        "DEBUG" => 10,
        "INFO" => 20,
        "WARN" | "WARNING" => 30,
        "ERROR" => 40,
        "CRITICAL" | "FATAL" => 50,
        _ => 0,
    }
}

impl LogSubscriptionFilter {
    pub fn new(level: Option<&str>, source_domain: Option<&str>) -> Self {
        let min_level = level.and_then(|l| {
            let trimmed = l.trim();
            if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("ALL") {
                None
            } else {
                let w = level_to_weight(trimmed);
                if w > 0 { Some(w) } else { None }
            }
        });
        let source_domain = source_domain.and_then(|d| {
            let trimmed = d.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        });
        Self {
            min_level,
            source_domain,
        }
    }

    pub fn matches(&self, record: &LogRecord) -> bool {
        if let Some(min_w) = self.min_level {
            let rec_w = level_to_weight(&record.level);
            if rec_w > 0 && rec_w < min_w {
                return false;
            }
        }
        if let Some(expected_domain) = &self.source_domain {
            let matches_src = record.source_domain.as_deref() == Some(expected_domain.as_str())
                || record.component.eq_ignore_ascii_case(expected_domain);
            if !matches_src {
                return false;
            }
        }
        true
    }
}

#[derive(Clone)]
pub struct CandidateEntry {
    pub instance_id: u64,
    pub pipe_tx: PipeSender,
    pub handlers: HashMap<String, String>,
    pub ready: bool,
}

#[derive(Clone)]
pub struct DrainingEntry {
    pub instance_id: u64,
    pub pipe_tx: PipeSender,
    pub started_at: Instant,
}

fn generation_pipe(
    domains: &RwLock<HashMap<String, DomainEntry>>,
    key: &str,
) -> Option<PipeSender> {
    let (domain, generation) = key
        .rsplit_once('#')
        .and_then(|(d, g)| g.parse::<u64>().ok().map(|g| (d, g)))
        .unwrap_or((key, 0));
    let map = domains.read();
    let entry = map.get(domain)?;
    if generation == 0 || entry.instance_id == generation {
        entry.pipe_tx.clone()
    } else if let Some(candidate) = entry
        .candidate
        .as_ref()
        .filter(|c| c.instance_id == generation)
    {
        Some(candidate.pipe_tx.clone())
    } else {
        entry
            .draining
            .iter()
            .find(|d| d.instance_id == generation)
            .map(|d| d.pipe_tx.clone())
    }
}

/// Internal registration state of a single managed Domain.
struct DomainEntry {
    /// Monotonically increasing process/pipe instance generation ID.
    instance_id: u64,
    status: DomainStatus,
    pipe_tx: Option<PipeSender>,
    /// Maps `handler_name -> description`.
    handlers: HashMap<String, String>,
    candidate: Option<CandidateEntry>,
    draining: Vec<DrainingEntry>,
}

/// Core Atelier Kernel Router: manages domain routes, generates message IDs,
/// enforces three-level idempotency, intercepts invalid targets (`TargetInvalid`),
/// routes 4-stage receipts, and dispatches storage messages.
pub struct KernelRouter {
    /// Serialize delivery transitions and publication to caller/observer together.
    /// Dedicated pipe I/O runs outside this gate on supervisor pump threads.
    next_instance_id: AtomicU64,
    /// Per-domain serialization gate that closes the TOCTOU window between checking `instance_id`
    /// and processing an inbound frame or replacing a domain generation via `attach_domain_pipe`.
    domain_gates: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    domains: Arc<RwLock<HashMap<String, DomainEntry>>>,
    /// `<domain>.<handler> -> domain` lookup table.
    routes: RwLock<HashMap<String, String>>,
    ledger: Arc<KernelLedger>,
    observer: Arc<KernelObserver>,
    storage: Arc<DomainStorageEngine>,
    logger: Arc<KernelLogHub>,
    /// In-memory ring buffer of recent notifications and telemetry frames for observation consoles.
    ui_inbox: RwLock<Vec<KernelMessage>>,
    /// Active domain subscriptions for kernel log events.
    log_subscribers: Arc<RwLock<HashMap<String, LogSubscriptionFilter>>>,
    /// Active domain subscriptions for kernel telemetry events.
    telemetry_subscribers: Arc<RwLock<HashSet<String>>>,
    /// Active domain subscriptions for kernel notification events.
    notify_subscribers: Arc<RwLock<HashSet<String>>>,
    /// Active domain subscriptions for kernel observation events.
    observer_subscribers: Arc<RwLock<HashSet<String>>>,
    /// Shutdown signal for the background log fanout thread.
    stop_log_pump: Arc<AtomicBool>,
    /// Handle to the background log fanout worker thread.
    log_pump_handle: Mutex<Option<thread::JoinHandle<()>>>,
    /// Handle to the background observer fanout worker thread.
    observer_pump_handle: Mutex<Option<thread::JoinHandle<()>>>,
    /// Optional back-reference to domain supervisor for runtime queries and domain control.
    supervisor: RwLock<Option<(Weak<crate::supervisor::DomainSupervisor>, Arc<AtomicBool>)>>,
    control_inflight: Arc<AtomicUsize>,
}

impl KernelRouter {
    pub fn new(
        ledger: Arc<KernelLedger>,
        observer: Arc<KernelObserver>,
        storage: Arc<DomainStorageEngine>,
        logger: Arc<KernelLogHub>,
    ) -> Self {
        let domains = Arc::new(RwLock::new(HashMap::<String, DomainEntry>::new()));
        let log_subscribers =
            Arc::new(RwLock::new(HashMap::<String, LogSubscriptionFilter>::new()));
        let telemetry_subscribers = Arc::new(RwLock::new(HashSet::new()));
        let notify_subscribers = Arc::new(RwLock::new(HashSet::new()));
        let observer_subscribers = Arc::new(RwLock::new(HashSet::<String>::new()));
        let stop_log_pump = Arc::new(AtomicBool::new(false));

        let rx = logger.subscribe();
        let log_subs_worker = Arc::clone(&log_subscribers);
        let domains_worker = Arc::clone(&domains);
        let stop_flag = Arc::clone(&stop_log_pump);

        let pump_handle = thread::Builder::new()
            .name("kernel-log-fanout".to_string())
            .spawn(move || {
                static COUNTER: AtomicU64 = AtomicU64::new(1);
                while !stop_flag.load(Ordering::Relaxed) {
                    match rx.recv_timeout(Duration::from_millis(50)) {
                        Ok(record) => {
                            let subs = log_subs_worker.read().clone();
                            if subs.is_empty() {
                                continue;
                            }
                            let data = match serde_json::to_value(&record) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            for (domain_id, filter) in subs {
                                if !filter.matches(&record) {
                                    continue;
                                }
                                if let Some(tx) = generation_pipe(&domains_worker, &domain_id) {
                                    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
                                    let now = now_millis();
                                    let msg = KernelMessage {
                                        logical_id: format!("sys_log_{}_{}", now, id),
                                        message_id: format!("sys_log_{}_{}", now, id),
                                        source_domain: record
                                            .source_domain
                                            .clone()
                                            .unwrap_or_else(|| "kernel".to_string()),
                                        target: KERNEL_LOGGER_TARGET.to_string(),
                                        targets: vec![KERNEL_LOGGER_TARGET.to_string()],
                                        message_type: MSG_SYSTEM_LOG_EVENT.to_string(),
                                        params: json!({}),
                                        timestamp: now,
                                        data: data.clone(),
                                    };
                                    let _ = tx.send(KernelToDomainFrame::Deliver(msg));
                                }
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
            })
            .expect("cannot start kernel stream fanout");

        let obs_rx = observer.subscribe();
        let obs_subs_worker = Arc::clone(&observer_subscribers);
        let obs_domains_worker = Arc::clone(&domains);
        let obs_stop_flag = Arc::clone(&stop_log_pump);

        let observer_pump_handle = thread::Builder::new()
            .name("kernel-observer-fanout".to_string())
            .spawn(move || {
                while !obs_stop_flag.load(Ordering::Relaxed) {
                    match obs_rx.recv_timeout(Duration::from_millis(50)) {
                        Ok(event) => {
                            let subs = obs_subs_worker.read().clone();
                            if subs.is_empty() {
                                continue;
                            }
                            let data = match serde_json::to_value(&event) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            for domain_id in subs {
                                if let Some(tx) = generation_pipe(&obs_domains_worker, &domain_id) {
                                    let msg = KernelMessage {
                                        logical_id: event.logical_id.clone(),
                                        message_id: format!("obs_{}", event.event_id),
                                        source_domain: "kernel".to_string(),
                                        target: KERNEL_OBSERVER_TARGET.to_string(),
                                        targets: vec![KERNEL_OBSERVER_TARGET.to_string()],
                                        message_type: MSG_SYSTEM_OBSERVER_EVENT.to_string(),
                                        params: json!({}),
                                        timestamp: event.timestamp,
                                        data: data.clone(),
                                    };
                                    let frame = KernelToDomainFrame::Deliver(msg);
                                    // Enqueue failures close this generation's outbox and retain its failure reason.
                                    // The supervisor converges it outside publication/domain locks; do not fault
                                    // synchronously here (that would recursively publish under the same locks).
                                    let _ = tx.send(frame);
                                }
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
            })
            .expect("cannot start kernel stream fanout");

        Self {
            next_instance_id: AtomicU64::new(1),
            domain_gates: RwLock::new(HashMap::new()),
            domains,
            routes: RwLock::new(HashMap::new()),
            ledger,
            observer,
            storage,
            logger,
            ui_inbox: RwLock::new(Vec::new()),
            log_subscribers,
            telemetry_subscribers,
            notify_subscribers,
            observer_subscribers,
            stop_log_pump,
            log_pump_handle: Mutex::new(Some(pump_handle)),
            observer_pump_handle: Mutex::new(Some(observer_pump_handle)),
            supervisor: RwLock::new(None),
            control_inflight: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn finish_shutdown(&self) {
        // The supervisor's closing flag rejects new input. Wait out frames that entered
        // before it was set, then their bounded lifecycle workers. No lock is held while waiting.
        let gates: Vec<_> = self.domain_gates.read().values().cloned().collect();
        for gate in gates {
            drop(gate.lock());
        }
        while self.control_inflight.load(Ordering::Acquire) != 0 {
            thread::sleep(Duration::from_millis(1));
        }
    }
    pub fn set_supervisor(&self, supervisor: Weak<crate::supervisor::DomainSupervisor>) {
        let closing = supervisor.upgrade().map_or_else(
            || Arc::new(AtomicBool::new(true)),
            |owner| owner.shutdown_signal(),
        );
        *self.supervisor.write() = Some((supervisor, closing));
    }

    pub fn supervisor(&self) -> Option<Arc<crate::supervisor::DomainSupervisor>> {
        self.supervisor
            .read()
            .as_ref()
            .and_then(|(owner, _)| owner.upgrade())
    }

    pub fn observer_subscribers_count(&self) -> usize {
        self.observer_subscribers.read().len()
    }

    pub fn log_subscribers_count(&self) -> usize {
        self.log_subscribers.read().len()
    }

    pub fn telemetry_subscribers_count(&self) -> usize {
        self.telemetry_subscribers.read().len()
    }

    pub fn notify_subscribers_count(&self) -> usize {
        self.notify_subscribers.read().len()
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
        if serde_json::to_vec(&msg).map_or(true, |bytes| bytes.len() > 256 * 1024) {
            return;
        }
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
    pub fn attach_domain_pipe(&self, domain_id: &str, pipe_tx: PipeSender) -> u64 {
        let gate = self.domain_gate(domain_id);
        let _domain_guard = gate.lock();

        if let Some(previous) = self.domain_instance_id(domain_id) {
            self.mark_domain_faulted_locked(
                domain_id,
                Some(previous),
                "domain process generation replaced",
            );
        }
        let mut domains = self.domains.write();
        let mut routes = self.routes.write();

        let instance_id = self.next_instance_id.fetch_add(1, Ordering::Relaxed);

        let entry = domains
            .entry(domain_id.to_string())
            .or_insert_with(|| DomainEntry {
                instance_id,
                status: DomainStatus::Spawning,
                pipe_tx: None,
                handlers: HashMap::new(),
                candidate: None,
                draining: Vec::new(),
            });
        // Clean up any leftover routes from a prior generation before binding the new instance.
        for h in entry.handlers.keys() {
            routes.remove(&format!("{domain_id}.{h}"));
        }
        entry.handlers.clear();
        entry.candidate = None;
        entry.draining.clear();
        entry.instance_id = instance_id;
        entry.status = DomainStatus::Spawning;
        entry.pipe_tx = Some(pipe_tx);

        self.remove_subscriptions(domain_id, instance_id);

        instance_id
    }

    /// Attaches a candidate pipe for a zero-downtime rolling update.
    pub fn attach_candidate_pipe(&self, domain_id: &str, pipe_tx: PipeSender) -> u64 {
        let gate = self.domain_gate(domain_id);
        let _domain_guard = gate.lock();

        let candidate_instance_id = self.next_instance_id.fetch_add(1, Ordering::Relaxed);
        let mut domains = self.domains.write();
        let entry = domains
            .entry(domain_id.to_string())
            .or_insert_with(|| DomainEntry {
                instance_id: 0,
                status: DomainStatus::Spawning,
                pipe_tx: None,
                handlers: HashMap::new(),
                candidate: None,
                draining: Vec::new(),
            });
        entry.candidate = Some(CandidateEntry {
            instance_id: candidate_instance_id,
            pipe_tx,
            handlers: HashMap::new(),
            ready: false,
        });
        candidate_instance_id
    }

    /// Checks if a candidate instance has registered its handlers and is ready.
    pub fn is_candidate_ready(&self, domain: &str, generation: u64) -> bool {
        let pipe = {
            let map = self.domains.read();
            map.get(domain)
                .and_then(|e| e.candidate.as_ref())
                .filter(|c| c.instance_id == generation && c.ready)
                .map(|c| c.pipe_tx.clone())
        };
        pipe.is_some_and(|tx| tx.failure().is_none())
    }

    /// Atomically commits candidate instance, moving old instance into draining, and updating routes.
    pub fn commit_candidate(&self, domain_id: &str, candidate_instance_id: u64) -> bool {
        let gate = self.domain_gate(domain_id);
        let _domain_guard = gate.lock();

        let mut domains = self.domains.write();
        let mut routes = self.routes.write();
        if let Some(entry) = domains.get_mut(domain_id) {
            if entry
                .candidate
                .as_ref()
                .is_some_and(|c| c.instance_id == candidate_instance_id)
            {
                if let Some(candidate) = entry.candidate.take() {
                    if let Some(old_pipe) = entry.pipe_tx.take() {
                        if entry.instance_id > 0 {
                            entry.draining.push(DrainingEntry {
                                instance_id: entry.instance_id,
                                pipe_tx: old_pipe,
                                started_at: Instant::now(),
                            });
                        }
                    }
                    for h in entry.handlers.keys() {
                        routes.remove(&format!("{domain_id}.{h}"));
                    }
                    entry.instance_id = candidate.instance_id;
                    entry.pipe_tx = Some(candidate.pipe_tx);
                    entry.handlers = candidate.handlers;
                    entry.status = DomainStatus::Active;
                    for h in entry.handlers.keys() {
                        routes.insert(format!("{domain_id}.{h}"), domain_id.to_string());
                    }
                    return true;
                }
            }
        }
        false
    }

    /// Aborts a candidate instance without altering the active routes.
    pub fn abort_candidate(&self, domain: &str, generation: u64) -> bool {
        let gate = self.domain_gate(domain);
        let _domain_guard = gate.lock();
        let candidate = {
            let mut domains = self.domains.write();
            domains.get_mut(domain).and_then(|e| {
                if e.candidate
                    .as_ref()
                    .is_some_and(|c| c.instance_id == generation)
                {
                    e.candidate.take()
                } else {
                    None
                }
            })
        };
        if let Some(candidate) = candidate {
            candidate.pipe_tx.close();
            self.remove_subscriptions(domain, generation);
            self.ledger
                .clear_session_dedup_for_domain(&format!("{domain}#{generation}"));
            true
        } else {
            false
        }
    }

    /// Removes an old instance from draining once all in-flight messages have been reaped.
    pub fn cleanup_draining_instance(&self, domain_id: &str, instance_id: u64) {
        let gate = self.domain_gate(domain_id);
        let _domain_guard = gate.lock();
        let old = {
            let mut domains = self.domains.write();
            domains.get_mut(domain_id).and_then(|e| {
                e.draining
                    .iter()
                    .position(|d| d.instance_id == instance_id)
                    .map(|i| e.draining.remove(i))
            })
        };
        if let Some(old) = old {
            old.pipe_tx.close();
            for (source, receipt) in self.ledger.fail_inflight_for_channel(
                domain_id,
                old.pipe_tx.channel_id(),
                "retired process generation",
            ) {
                self.observer.emit(
                    &source,
                    &receipt.target,
                    &receipt.message_id,
                    &receipt.logical_id,
                    receipt.seq,
                    &receipt.message_type,
                    receipt.status,
                    receipt.data.clone(),
                );
                self.send_to_domain_pipe(&source, KernelToDomainFrame::Receipt(receipt));
            }
        }
        self.remove_subscriptions(domain_id, instance_id);
        self.ledger
            .clear_session_dedup_for_domain(&format!("{domain_id}#{instance_id}"));
    }

    /// Validates whether an instance_id belongs to the active, candidate, or draining generation.
    pub fn is_valid_instance_for_domain(&self, domain_id: &str, instance_id: u64) -> bool {
        // Share the supervisor's existing signal, never temporarily own the supervisor
        // while holding an input gate (its last Arc could otherwise run Drop recursively).
        if self
            .supervisor
            .read()
            .as_ref()
            .is_some_and(|(_, closing)| closing.load(Ordering::Acquire))
        {
            return false;
        }
        let domains = self.domains.read();
        if let Some(entry) = domains.get(domain_id) {
            if entry.instance_id == instance_id {
                return entry.pipe_tx.is_some();
            }
            if let Some(c) = &entry.candidate {
                if c.instance_id == instance_id {
                    return true;
                }
            }
            if entry.draining.iter().any(|d| d.instance_id == instance_id) {
                return true;
            }
        }
        false
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
        if !self.is_valid_instance_for_domain(authenticated_domain, instance_id) {
            return;
        }
        self.handle_raw_pipe_line_inner(authenticated_domain, instance_id, raw_line);
    }

    /// Processes a raw JSON line coming from the physical stdout pipe of `authenticated_domain`.
    pub fn handle_raw_pipe_line(&self, authenticated_domain: &str, raw_line: &str) {
        let gate = self.domain_gate(authenticated_domain);
        let _domain_guard = gate.lock();
        let instance = self.domain_instance_id(authenticated_domain).unwrap_or(0);
        if self.is_valid_instance_for_domain(authenticated_domain, instance) {
            self.handle_raw_pipe_line_inner(authenticated_domain, instance, raw_line);
        }
    }

    fn handle_raw_pipe_line_inner(
        &self,
        authenticated_domain: &str,
        instance_id: u64,
        raw_line: &str,
    ) {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() {
            return;
        }
        match serde_json::from_str::<DomainToKernelFrame>(trimmed) {
            Ok(frame) => {
                self.handle_frame_inner(authenticated_domain, instance_id, frame);
            }
            Err(err) => {
                let msg_id = self.ledger.generate_message_id();
                let err_data = Some(json!({
                    "error": "invalid_json_frame",
                    "reason": err.to_string(),
                }));
                let receipt = MessageReceipt {
                    logical_id: msg_id.replacen("msg_", "logical_", 1),
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
                    &msg_id.replacen("msg_", "logical_", 1),
                    0,
                    "MalformedFrame",
                    ContractStatus::ContractInvalid,
                    err_data,
                );
                self.send_to_instance(
                    authenticated_domain,
                    Some(instance_id),
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
        if !self.is_valid_instance_for_domain(authenticated_domain, instance_id) {
            return None;
        }
        self.handle_frame_inner(authenticated_domain, instance_id, frame)
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
        let instance = self.domain_instance_id(authenticated_domain).unwrap_or(0);
        if !self.is_valid_instance_for_domain(authenticated_domain, instance) {
            return None;
        }
        self.handle_frame_inner(authenticated_domain, instance, frame)
    }

    fn handle_frame_inner(
        &self,
        authenticated_domain: &str,
        instance_id: u64,
        frame: DomainToKernelFrame,
    ) -> Option<Vec<String>> {
        match frame {
            DomainToKernelFrame::Send(outbound) => Some(self.handle_outbound_message_inner(
                authenticated_domain,
                instance_id,
                outbound,
            )),
            DomainToKernelFrame::Receipt(inbound) => {
                self.handle_inbound_receipt_inner(authenticated_domain, Some(instance_id), inbound);
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
        self.handle_frame(authenticated_domain, DomainToKernelFrame::Send(outbound))
            .unwrap_or_default()
    }
    fn handle_outbound_message_inner(
        &self,
        authenticated_domain: &str,
        instance_id: u64,
        outbound: OutboundMessage,
    ) -> Vec<String> {
        let invalid = outbound.targets.is_empty()
            || outbound.message_type.trim().is_empty()
            || !outbound.params.is_object()
            || outbound.targets.len() > 1024;
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
        let durable = targets.iter().all(|t| {
            crate::protocol::DeliveryPolicy::resolve(t, &outbound.message_type)
                == crate::protocol::DeliveryPolicy::Durable
        });
        let (logical_id, ids, duplicate) = match self.ledger.try_allocate_submission(
            &format!("{authenticated_domain}#{instance_id}"),
            outbound.seq,
            targets.len(),
            durable,
        ) {
            Ok(value) => value,
            Err(error) => {
                let logical = self.ledger.generate_message_id();
                let ids: Vec<_> = targets
                    .iter()
                    .map(|_| self.ledger.generate_message_id())
                    .collect();
                for (target, id) in targets.iter().zip(&ids) {
                    self.send_to_instance_inner(authenticated_domain,Some(instance_id),KernelToDomainFrame::Receipt(MessageReceipt {
                        logical_id:logical.clone(),message_id:id.clone(),seq:outbound.seq,target:target.clone(),message_type:outbound.message_type.clone(),
                        status:ContractStatus::ExecutionFailed,data:Some(json!({"error":"kernel_admission_failed","reason":error.to_string(),"persisted":false})),timestamp:now_millis(),
                    }), true);
                }
                return ids;
            }
        };
        if duplicate {
            // Snapshot and replay under the publication gate too: otherwise a
            // stale replay can arrive after a newer live receipt.
            for id in &ids {
                let gate = self.ledger.message_gate(id);
                let _guard = gate.lock();
                for receipt in self.ledger.receipts_for_message(id) {
                    self.send_to_instance(
                        authenticated_domain,
                        Some(instance_id),
                        KernelToDomainFrame::Receipt(receipt),
                    );
                }
            }
            return ids;
        }
        self.ledger.bind_origin(&ids, instance_id);
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
                let (record, _) = self.ledger.record_initial_target_state_locked(
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
                    &logical_id,
                    outbound.seq,
                    &outbound.message_type,
                    record.status,
                    record.data.clone(),
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
            } else if target == KERNEL_RUNTIME_TARGET {
                self.dispatch_kernel_runtime(authenticated_domain, outbound.seq, &kernel_msg);
            } else if target == KERNEL_OBSERVER_TARGET {
                self.dispatch_kernel_observer(authenticated_domain, outbound.seq, &kernel_msg);
            } else if target == KERNEL_SUPERVISOR_TARGET {
                // The request has been authenticated and its identities allocated under
                // the source gate. Lifecycle work may replace that very source domain;
                // execute it outside the gate, without weakening pipe identity checks.
                if let Some(supervisor) = self.supervisor() {
                    if self
                        .control_inflight
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                            (count < 32).then_some(count + 1)
                        })
                        .is_err()
                    {
                        if self.emit_kernel_internal_stages(
                            authenticated_domain,
                            outbound.seq,
                            &kernel_msg,
                        ) {
                            self.complete_kernel_internal_target(authenticated_domain, &kernel_msg, ContractStatus::ExecutionFailed,
                                Some(json!({"error":"supervisor_busy","reason":"Lifecycle control capacity reached"})));
                        }
                        continue;
                    }
                    let permit = ControlPermit {
                        count: Arc::clone(&self.control_inflight),
                    };
                    let router = Arc::clone(supervisor.router());
                    let source = authenticated_domain.to_string();
                    let seq = outbound.seq;
                    let request = kernel_msg.clone();
                    if let Err(error) = thread::Builder::new()
                        .name("kernel-lifecycle-control".into())
                        .spawn(move || {
                            let _permit = permit;
                            router.dispatch_kernel_supervisor(&source, seq, &request);
                        })
                    {
                        if self.emit_kernel_internal_stages(authenticated_domain, seq, &kernel_msg)
                        {
                            self.complete_kernel_internal_target(authenticated_domain, &kernel_msg, ContractStatus::ExecutionFailed,
                                Some(json!({"error":"supervisor_worker_failed","reason":error.to_string()})));
                        }
                    }
                } else {
                    self.dispatch_kernel_supervisor(
                        authenticated_domain,
                        outbound.seq,
                        &kernel_msg,
                    );
                }
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
        if !self.emit_kernel_internal_stages(source_domain, seq, msg) {
            return;
        }

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

                if let Err(error) = self.storage.ensure_domain_table(source_domain) {
                    self.complete_kernel_internal_target(source_domain,msg,ContractStatus::ExecutionFailed,Some(json!({"error":"domain_storage_unavailable","reason":error.to_string()})));
                    return;
                }

                let mut registered_routes = Vec::new();
                let mut registered_handlers = Vec::new();
                let mut candidate_pipe_opt = None;
                {
                    let mut domains = self.domains.write();
                    if let Some(entry) = domains.get_mut(source_domain) {
                        if let Some(cand) = &mut entry.candidate {
                            if Some(cand.instance_id)
                                == self.ledger.source_instance(&msg.message_id)
                            {
                                cand.handlers.clear();
                                for (h, desc) in &handlers {
                                    let full_route = format!("{source_domain}.{h}");
                                    cand.handlers.insert(h.clone(), desc.clone());
                                    registered_routes.push(full_route.clone());
                                    registered_handlers.push(json!({
                                        "target": full_route,
                                        "domain": source_domain,
                                        "handler": h,
                                        "description": desc,
                                    }));
                                }
                                cand.ready = true;
                                candidate_pipe_opt = Some(cand.pipe_tx.clone());
                            }
                        }
                    }
                }

                if let Some(cand_pipe) = candidate_pipe_opt {
                    if let ReceiptTransition::Advanced { receipt, .. } =
                        self.ledger.advance_status_locked(
                            "kernel",
                            &msg.message_id,
                            &msg.target,
                            ContractStatus::Completed,
                            Some(json!({
                                "domain": source_domain,
                                "status": "candidate_ready",
                                "registered_routes": registered_routes,
                                "handlers": registered_handlers,
                            })),
                        )
                    {
                        self.observer.emit(
                            source_domain,
                            &msg.target,
                            &msg.message_id,
                            &msg.logical_id,
                            receipt.seq,
                            &msg.message_type,
                            ContractStatus::Completed,
                            receipt.data.clone(),
                        );
                        let _ = cand_pipe; // All receipt stages use the same authenticated origin path.
                        self.send_to_domain_pipe(
                            source_domain,
                            KernelToDomainFrame::Receipt(receipt),
                        );
                    }
                    return;
                }

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
                                candidate: None,
                                draining: Vec::new(),
                            });
                    if self
                        .ledger
                        .source_instance(&msg.message_id)
                        .is_some_and(|origin| origin != 0 && origin != entry.instance_id)
                    {
                        drop(routes);
                        drop(domains);
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::ExecutionFailed,
                            Some(json!({"error":"stale_generation_registration"})),
                        );
                        return;
                    }
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
                        if self
                            .ledger
                            .source_instance(&msg.message_id)
                            .is_none_or(|origin| origin == 0 || origin == entry.instance_id)
                        {
                            for (h, _) in entry.handlers.drain() {
                                let full_route = format!("{source_domain}.{h}");
                                routes.remove(&full_route);
                                removed_routes.push(full_route);
                            }
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
        if !self.emit_kernel_internal_stages(source_domain, seq, msg) {
            return;
        }

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
        if !self.emit_kernel_internal_stages(source_domain, seq, msg) {
            return;
        }

        match msg.message_type.as_str() {
            MSG_SUBSCRIBE_LOGS => {
                let level = msg.params.get("level").and_then(Value::as_str);
                let filter_domain = msg
                    .params
                    .get("source_domain")
                    .or_else(|| msg.params.get("domain"))
                    .and_then(Value::as_str);
                let filter = LogSubscriptionFilter::new(level, filter_domain);
                self.log_subscribers
                    .write()
                    .insert(self.subscription_key(source_domain, msg), filter);

                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({
                        "status": "subscribed",
                        "target": KERNEL_LOGGER_TARGET,
                    })),
                );
            }
            MSG_UNSUBSCRIBE_LOGS => {
                self.log_subscribers
                    .write()
                    .remove(&self.subscription_key(source_domain, msg));
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({
                        "status": "unsubscribed",
                        "target": KERNEL_LOGGER_TARGET,
                    })),
                );
            }
            MSG_QUERY_LOGS => {
                let query = match LogQuery::from_params(&msg.params) {
                    Ok(query) => query,
                    Err(error) => {
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::ContractInvalid,
                            Some(json!({"error":"invalid_log_query","reason":error.to_string()})),
                        );
                        return;
                    }
                };
                match self.logger.query_logs_page(&query) {
                    Ok(page) => {
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::Completed,
                            Some(
                                json!({"logs":page.logs,"total":page.total,"has_more":page.has_more,
                                "next_before_id":page.next_before_id}),
                            ),
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
        if !self.emit_kernel_internal_stages(source_domain, seq, msg) {
            return;
        }

        match msg.message_type.as_str() {
            MSG_SUBSCRIBE_TELEMETRY => {
                self.telemetry_subscribers
                    .write()
                    .insert(self.subscription_key(source_domain, msg));
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({
                        "status": "subscribed",
                        "target": KERNEL_TELEMETRY_TARGET,
                    })),
                );
            }
            MSG_UNSUBSCRIBE_TELEMETRY => {
                self.telemetry_subscribers
                    .write()
                    .remove(&self.subscription_key(source_domain, msg));
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({
                        "status": "unsubscribed",
                        "target": KERNEL_TELEMETRY_TARGET,
                    })),
                );
            }
            _ => {
                self.push_ui_inbox(msg.clone());

                let subs: Vec<String> = self.telemetry_subscribers.read().iter().cloned().collect();
                if !subs.is_empty() {
                    static COUNTER: AtomicU64 = AtomicU64::new(1);
                    let telem_data =
                        if msg.data.is_object() && !msg.data.as_object().unwrap().is_empty() {
                            let mut d = msg.data.clone();
                            if let Some(obj) = d.as_object_mut() {
                                obj.entry("metric_name".to_string())
                                    .or_insert_with(|| json!(msg.message_type));
                                obj.entry("source_domain".to_string())
                                    .or_insert_with(|| json!(source_domain));
                            }
                            d
                        } else {
                            json!({
                                "metric_name": msg.message_type,
                                "source_domain": source_domain,
                                "params": msg.params,
                                "data": msg.data,
                            })
                        };
                    for sub_domain in subs {
                        let now = now_millis();
                        let sub_id = COUNTER.fetch_add(1, Ordering::Relaxed);
                        let event_msg = KernelMessage {
                            logical_id: format!("sys_telem_{}_{}", now, sub_id),
                            message_id: format!("sys_telem_{}_{}", now, sub_id),
                            source_domain: source_domain.to_string(),
                            target: KERNEL_TELEMETRY_TARGET.to_string(),
                            targets: vec![KERNEL_TELEMETRY_TARGET.to_string()],
                            message_type: MSG_SYSTEM_TELEMETRY_EVENT.to_string(),
                            params: msg.params.clone(),
                            timestamp: now,
                            data: telem_data.clone(),
                        };
                        if let Some(tx) = generation_pipe(&self.domains, &sub_domain) {
                            let _ = tx.send(KernelToDomainFrame::Deliver(event_msg));
                        }
                    }
                }

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
        }
    }

    /// Dispatches an asynchronous system notification or alert targeting `kernel.notify`.
    fn dispatch_kernel_notify(&self, source_domain: &str, seq: u64, msg: &KernelMessage) {
        if !self.emit_kernel_internal_stages(source_domain, seq, msg) {
            return;
        }

        match msg.message_type.as_str() {
            MSG_SUBSCRIBE_NOTIFY => {
                self.notify_subscribers
                    .write()
                    .insert(self.subscription_key(source_domain, msg));
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({
                        "status": "subscribed",
                        "target": KERNEL_NOTIFY_TARGET,
                    })),
                );
            }
            MSG_UNSUBSCRIBE_NOTIFY => {
                self.notify_subscribers
                    .write()
                    .remove(&self.subscription_key(source_domain, msg));
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({
                        "status": "unsubscribed",
                        "target": KERNEL_NOTIFY_TARGET,
                    })),
                );
            }
            _ => {
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

                let subs: Vec<String> = self.notify_subscribers.read().iter().cloned().collect();
                if !subs.is_empty() {
                    static COUNTER: AtomicU64 = AtomicU64::new(1);
                    let alert_data = json!({
                        "notification_type": msg.message_type,
                        "source_domain": source_domain,
                        "severity": severity,
                        "message": text,
                        "details": msg.data,
                        "params": msg.params,
                    });
                    for sub_domain in subs {
                        let now = now_millis();
                        let sub_id = COUNTER.fetch_add(1, Ordering::Relaxed);
                        let event_msg = KernelMessage {
                            logical_id: format!("sys_notify_{}_{}", now, sub_id),
                            message_id: format!("sys_notify_{}_{}", now, sub_id),
                            source_domain: source_domain.to_string(),
                            target: KERNEL_NOTIFY_TARGET.to_string(),
                            targets: vec![KERNEL_NOTIFY_TARGET.to_string()],
                            message_type: MSG_SYSTEM_NOTIFY_EVENT.to_string(),
                            params: msg.params.clone(),
                            timestamp: now,
                            data: alert_data.clone(),
                        };
                        if let Some(tx) = generation_pipe(&self.domains, &sub_domain) {
                            let _ = tx.send(KernelToDomainFrame::Deliver(event_msg));
                        }
                    }
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
        }
    }

    /// Dispatches a domain assembly query message targeting `kernel.runtime`.
    fn dispatch_kernel_runtime(&self, source_domain: &str, seq: u64, msg: &KernelMessage) {
        if !self.emit_kernel_internal_stages(source_domain, seq, msg) {
            return;
        }

        match msg.message_type.as_str() {
            MSG_QUERY_RUNTIME | MSG_QUERY_DOMAIN_ASSEMBLY | _ => {}
        }

        let target_domain = msg
            .params
            .get("domain")
            .or_else(|| msg.params.get("domain_id"))
            .or_else(|| msg.data.get("domain"))
            .or_else(|| msg.data.get("domain_id"))
            .and_then(Value::as_str);

        let entries: Vec<_> = self
            .domains
            .read()
            .iter()
            .map(|(id, e)| (id.clone(), e.status, e.instance_id, e.handlers.clone()))
            .collect();
        let supervisor_opt = self.supervisor();

        let mut domain_list = Vec::new();
        for (d_id, status, instance_id, handlers) in &entries {
            if let Some(filter) = target_domain {
                if d_id != filter {
                    continue;
                }
            }
            let mut routes: Vec<String> = handlers.keys().map(|h| format!("{d_id}.{h}")).collect();
            routes.sort();

            let process_info = if let Some(sup) = &supervisor_opt {
                if let Some(spec) = sup.get_spec(d_id) {
                    json!({
                        "pid": sup.get_pid(d_id),
                        "command": spec.command,
                        "args": spec.args,
                        "working_dir": spec.working_dir,
                        "auto_restart": spec.auto_restart,
                        "max_restarts": spec.max_restarts,
                        "restart_delay_ms": spec.restart_delay_ms,
                        "restart_count": sup.restart_count(d_id),
                        "is_stopped": sup.is_stopped(d_id),
                    })
                } else {
                    json!({
                        "pid": sup.get_pid(d_id),
                        "in_process": true,
                    })
                }
            } else {
                Value::Null
            };

            domain_list.push(json!({
                "domain_id": d_id,
                "status": format!("{:?}", status),
                "instance_id": instance_id,
                "routes": routes,
                "handlers": handlers,
                "process": process_info,
            }));
        }
        // Receipt publication reads domains again. A queued registration writer
        // makes a recursive read block forever while this snapshot owns the lock.
        domain_list.sort_by(|a, b| a["domain_id"].as_str().cmp(&b["domain_id"].as_str()));

        let single_domain = if domain_list.len() == 1 && target_domain.is_some() {
            Some(domain_list[0].clone())
        } else {
            None
        };

        let response_data = json!({
            "count": domain_list.len(),
            "domains": domain_list,
            "domain": single_domain,
            "ledger": self.ledger.retention_counts(),
            "logging": {"persistence_error":self.logger.persistence_error()},
        });

        self.complete_kernel_internal_target(
            source_domain,
            msg,
            ContractStatus::Completed,
            Some(response_data),
        );
    }

    /// Dispatches a message lifecycle observation message targeting `kernel.observer`.
    fn dispatch_kernel_observer(&self, source_domain: &str, seq: u64, msg: &KernelMessage) {
        if !self.emit_kernel_internal_stages(source_domain, seq, msg) {
            return;
        }

        match msg.message_type.as_str() {
            MSG_SUBSCRIBE_OBSERVER => {
                self.observer_subscribers
                    .write()
                    .insert(self.subscription_key(source_domain, msg));
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({
                        "status": "subscribed",
                        "target": KERNEL_OBSERVER_TARGET,
                    })),
                );
            }
            MSG_UNSUBSCRIBE_OBSERVER => {
                self.observer_subscribers
                    .write()
                    .remove(&self.subscription_key(source_domain, msg));
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({
                        "status": "unsubscribed",
                        "target": KERNEL_OBSERVER_TARGET,
                    })),
                );
            }
            MSG_QUERY_OBSERVER | _ => {
                let msg_id_filter = msg.params.get("message_id").and_then(Value::as_str);
                let target_filter = msg.params.get("target").and_then(Value::as_str);
                let source_filter = msg.params.get("source_domain").and_then(Value::as_str);
                let limit = msg
                    .params
                    .get("limit")
                    .and_then(Value::as_u64)
                    .unwrap_or(100) as usize;

                let all_events = self.observer.snapshot();
                let mut filtered: Vec<ObservationEvent> = all_events
                    .into_iter()
                    .filter(|e| {
                        if let Some(mid) = msg_id_filter {
                            if e.message_id != mid {
                                return false;
                            }
                        }
                        if let Some(tf) = target_filter {
                            if e.target != tf {
                                return false;
                            }
                        }
                        if let Some(sf) = source_filter {
                            if e.source_domain != sf {
                                return false;
                            }
                        }
                        true
                    })
                    .collect();

                if filtered.len() > limit {
                    filtered = filtered.split_off(filtered.len() - limit);
                }

                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({
                        "count": filtered.len(),
                        "events": filtered,
                    })),
                );
            }
        }
    }

    /// Dispatches a process lifecycle management message targeting `kernel.supervisor`.
    fn dispatch_kernel_supervisor(&self, source_domain: &str, seq: u64, msg: &KernelMessage) {
        if !self.emit_kernel_internal_stages(source_domain, seq, msg) {
            return;
        }

        let sup = match self.supervisor() {
            Some(s) => s,
            None => {
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::ExecutionFailed,
                    Some(json!({
                        "error": "supervisor_unavailable",
                        "reason": "Kernel supervisor component is not attached",
                    })),
                );
                return;
            }
        };

        let target_domain = msg
            .params
            .get("domain")
            .or_else(|| msg.params.get("domain_id"))
            .or_else(|| msg.data.get("domain"))
            .or_else(|| msg.data.get("domain_id"))
            .and_then(Value::as_str);

        match msg.message_type.as_str() {
            MSG_START_DOMAIN => {
                let domain_id = match target_domain {
                    Some(d) => d,
                    None => {
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::ExecutionFailed,
                            Some(json!({ "error": "missing_domain", "reason": "domain parameter required" })),
                        );
                        return;
                    }
                };

                let spec_val = msg.params.get("spec").or_else(|| msg.data.get("spec"));
                let res = if let Some(sv) = spec_val {
                    match serde_json::from_value::<crate::supervisor::DomainProcessSpec>(sv.clone())
                    {
                        Ok(mut spec) => {
                            spec.domain_id = domain_id.to_string();
                            sup.spawn_process_domain(spec)
                        }
                        Err(e) => {
                            self.complete_kernel_internal_target(
                                source_domain,
                                msg,
                                ContractStatus::ExecutionFailed,
                                Some(json!({ "error": "invalid_spec", "reason": e.to_string() })),
                            );
                            return;
                        }
                    }
                } else {
                    sup.respawn_domain(domain_id)
                };

                match res {
                    Ok(pid) => {
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::Completed,
                            Some(
                                json!({ "domain_id": domain_id, "pid": pid, "status": "started" }),
                            ),
                        );
                    }
                    Err(e) => {
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::ExecutionFailed,
                            Some(json!({ "error": "spawn_failed", "reason": e.to_string() })),
                        );
                    }
                }
            }
            MSG_STOP_DOMAIN => {
                let domain_id = match target_domain {
                    Some(d) => d,
                    None => {
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::ExecutionFailed,
                            Some(json!({ "error": "missing_domain", "reason": "domain parameter required" })),
                        );
                        return;
                    }
                };
                let stopped = sup.stop_process_domain(domain_id);
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::Completed,
                    Some(json!({ "domain_id": domain_id, "status": "stopped", "was_running": stopped })),
                );
            }
            MSG_RESTART_DOMAIN => {
                let domain_id = match target_domain {
                    Some(d) => d,
                    None => {
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::ExecutionFailed,
                            Some(json!({ "error": "missing_domain", "reason": "domain parameter required" })),
                        );
                        return;
                    }
                };
                match sup.respawn_domain(domain_id) {
                    Ok(pid) => {
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::Completed,
                            Some(json!({ "domain_id": domain_id, "pid": pid, "status": "restarted" })),
                        );
                    }
                    Err(e) => {
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::ExecutionFailed,
                            Some(json!({ "error": "restart_failed", "reason": e.to_string() })),
                        );
                    }
                }
            }
            MSG_UPDATE_DOMAIN => {
                let domain_id = match target_domain {
                    Some(d) => d,
                    None => {
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::ExecutionFailed,
                            Some(json!({ "error": "missing_domain", "reason": "domain parameter required" })),
                        );
                        return;
                    }
                };

                let spec_val = msg.params.get("spec").or_else(|| msg.data.get("spec"));
                let new_spec = if let Some(sv) = spec_val {
                    match serde_json::from_value::<crate::supervisor::DomainProcessSpec>(sv.clone())
                    {
                        Ok(mut spec) => {
                            spec.domain_id = domain_id.to_string();
                            Some(spec)
                        }
                        Err(e) => {
                            self.complete_kernel_internal_target(
                                source_domain,
                                msg,
                                ContractStatus::ExecutionFailed,
                                Some(json!({ "error": "invalid_spec", "reason": e.to_string() })),
                            );
                            return;
                        }
                    }
                } else {
                    None
                };

                let timeout_ms = msg
                    .params
                    .get("timeout_ms")
                    .or_else(|| msg.data.get("timeout_ms"))
                    .and_then(Value::as_u64)
                    .unwrap_or(5000);
                let probe_handler = msg
                    .params
                    .get("probe_handler")
                    .or_else(|| msg.data.get("probe_handler"))
                    .and_then(Value::as_str);

                match sup.update_process_domain(
                    domain_id,
                    new_spec,
                    probe_handler,
                    Duration::from_millis(timeout_ms),
                ) {
                    Ok(update_res) => {
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::Completed,
                            Some(json!({
                                "domain_id": update_res.domain_id,
                                "new_pid": update_res.new_pid,
                                "old_pid": update_res.old_pid,
                                "instance_id": update_res.instance_id,
                                "status": update_res.status,
                            })),
                        );
                    }
                    Err(e) => {
                        self.complete_kernel_internal_target(
                            source_domain,
                            msg,
                            ContractStatus::ExecutionFailed,
                            Some(json!({
                                "domain_id": domain_id,
                                "status": "rolled_back",
                                "error": "update_failed",
                                "reason": e.to_string(),
                            })),
                        );
                    }
                }
            }
            _ => {
                self.complete_kernel_internal_target(
                    source_domain,
                    msg,
                    ContractStatus::ContractInvalid,
                    Some(json!({
                        "error": "unknown_message_type",
                        "reason": format!("kernel.supervisor does not support {}", msg.message_type),
                    })),
                );
            }
        }
    }

    /// Emits `Sent -> Received -> Started` receipts for kernel built-in targets (`kernel.registry`, `kernel.storage`, `kernel.logger`, etc.).
    fn emit_kernel_internal_stages(
        &self,
        source_domain: &str,
        seq: u64,
        msg: &KernelMessage,
    ) -> bool {
        let gate = self.ledger.message_gate(&msg.message_id);
        let _publication = gate.lock();
        let (sent_rec, created) = self.ledger.record_initial_target_state_locked(
            &msg.message_id,
            seq,
            source_domain,
            &msg.target,
            &msg.message_type,
            ContractStatus::Sent,
            None,
        );
        if !created {
            self.send_to_domain_pipe(
                source_domain,
                KernelToDomainFrame::Receipt(sent_rec.to_receipt()),
            );
            return false;
        }
        self.observer.emit(
            source_domain,
            &msg.target,
            &msg.message_id,
            &msg.logical_id,
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
            if let ReceiptTransition::Advanced { receipt, .. } = self.ledger.advance_status_locked(
                "kernel",
                &msg.message_id,
                &msg.target,
                stage,
                None,
            ) {
                self.observer.emit(
                    source_domain,
                    &msg.target,
                    &msg.message_id,
                    &msg.logical_id,
                    seq,
                    &msg.message_type,
                    receipt.status,
                    receipt.data.clone(),
                );
                let succeeded = receipt.status == stage;
                self.send_to_domain_pipe(source_domain, KernelToDomainFrame::Receipt(receipt));
                if !succeeded {
                    return false;
                }
            }
        }
        true
    }

    fn complete_kernel_internal_target(
        &self,
        source_domain: &str,
        msg: &KernelMessage,
        final_status: ContractStatus,
        data: Option<Value>,
    ) {
        let gate = self.ledger.message_gate(&msg.message_id);
        let _publication = gate.lock();
        if let ReceiptTransition::Advanced { receipt, .. } = self.ledger.advance_status_locked(
            "kernel",
            &msg.message_id,
            &msg.target,
            final_status,
            data.clone(),
        ) {
            // Query payloads belong to the requester, not a second full result cache.
            let observed_data = if crate::protocol::DeliveryPolicy::is_infrastructure_query(
                &msg.target,
                &msg.message_type,
            ) && final_status == ContractStatus::Completed
            {
                data.as_ref().map(|value| {
                    let count = value
                        .get("count")
                        .and_then(Value::as_u64)
                        .unwrap_or_else(|| {
                            ["items", "events", "domains", "handlers", "logs", "progress"]
                                .iter()
                                .find_map(|key| value.get(key).and_then(Value::as_array))
                                .map_or(0, |items| items.len() as u64)
                        });
                    json!({ "count": count })
                })
            } else {
                data
            };
            self.observer.emit(
                source_domain,
                &msg.target,
                &msg.message_id,
                &msg.logical_id,
                receipt.seq,
                &msg.message_type,
                receipt.status,
                if receipt.status == final_status
                    && !self.ledger.has_unpersisted_failure(&receipt.message_id)
                {
                    observed_data
                } else {
                    receipt.data.clone()
                },
            );
            self.send_to_domain_pipe(source_domain, KernelToDomainFrame::Receipt(receipt));
        }
    }

    /// Validates and routes a message targeting a business domain handler (`"<domain>.<handler>"`).
    /// If the target does not exist or is not Active, does NOT send the message and immediately returns `TargetInvalid`.
    fn dispatch_domain_target(&self, source_domain: &str, seq: u64, msg: KernelMessage) {
        let gate = self.ledger.message_gate(&msg.message_id);
        let _publication = gate.lock();
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

        let Some((_target_instance_id, target_pipe)) = target_pipe_opt else {
            // Core Rule: Target does not exist or is not ready -> DO NOT SEND, return `TargetInvalid`.
            let err_data = Some(json!({
                "error": "target_not_found_or_inactive",
                "target": target,
            }));
            let (rec, created) = self.ledger.record_initial_target_state_locked(
                &msg.message_id,
                seq,
                source_domain,
                &target,
                &msg.message_type,
                ContractStatus::TargetInvalid,
                err_data.clone(),
            );
            if created || rec.status == ContractStatus::ExecutionFailed {
                self.observer.emit(
                    source_domain,
                    &target,
                    &msg.message_id,
                    &msg.logical_id,
                    seq,
                    &msg.message_type,
                    rec.status,
                    rec.data.clone(),
                );
                self.send_to_domain_pipe(
                    source_domain,
                    KernelToDomainFrame::Receipt(rec.to_receipt()),
                );
            }
            return;
        };

        // Reliable admission commits the record and complete envelope in the same transaction.
        let (sent_rec, created) = match target_pipe.accept(&self.ledger, &msg, seq) {
            Ok(result) => result,
            Err(error) => {
                let (mut failed, _) = self.ledger.record_initial_target_state_locked(
                    &msg.message_id,
                    seq,
                    source_domain,
                    &target,
                    &msg.message_type,
                    ContractStatus::ExecutionFailed,
                    Some(json!({"error":"target_output_unavailable","reason":error.0})),
                );
                if !failed.status.is_terminal() {
                    if let Some((owner, _)) = parse_target_address(&target) {
                        if let ReceiptTransition::Advanced { receipt, .. } =
                            self.ledger.advance_status_locked(
                                owner,
                                &msg.message_id,
                                &target,
                                ContractStatus::ExecutionFailed,
                                Some(json!({"error":"target_output_unavailable","reason":error.0})),
                            )
                        {
                            failed.status = receipt.status;
                            failed.updated_at = receipt.timestamp;
                            failed.data = receipt.data;
                        }
                    }
                }
                self.observer.emit(
                    source_domain,
                    &target,
                    &msg.message_id,
                    &msg.logical_id,
                    seq,
                    &msg.message_type,
                    failed.status,
                    failed.data.clone(),
                );
                self.send_to_domain_pipe(
                    source_domain,
                    KernelToDomainFrame::Receipt(failed.to_receipt()),
                );
                // The closed outbox already carries the generation's failure. The monitor
                // revokes it outside source/message locks, avoiding A->B / B->A lock cycles.
                return;
            }
        };
        if !created {
            self.observer.emit(
                source_domain,
                &target,
                &msg.message_id,
                &msg.logical_id,
                seq,
                &msg.message_type,
                sent_rec.status,
                sent_rec.data.clone(),
            );
            self.send_to_domain_pipe(
                source_domain,
                KernelToDomainFrame::Receipt(sent_rec.to_receipt()),
            );
            return;
        }

        // Emit Stage 1 (`Sent`) receipt to sender and observer.
        self.observer.emit(
            source_domain,
            &target,
            &msg.message_id,
            &msg.logical_id,
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
        self.handle_inbound_receipt_inner(reporting_domain, None, inbound)
    }
    fn handle_inbound_receipt_inner(
        &self,
        reporting_domain: &str,
        instance: Option<u64>,
        inbound: InboundReceipt,
    ) {
        if let Some(instance) = instance {
            let Some(pipe) = self.pipe_for_instance(reporting_domain, instance) else {
                return;
            };
            if !self
                .ledger
                .accepts_channel(&inbound.message_id, pipe.channel_id())
            {
                return;
            }
        }
        if !inbound.status.is_valid_inbound_receipt() {
            return;
        }

        let gate = self.ledger.message_gate(&inbound.message_id);
        let _publication = gate.lock();

        match self.ledger.advance_status_locked(
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
                    &receipt.logical_id,
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
        let gate = self.domain_gate(domain_id);
        let _domain_guard = gate.lock();
        self.mark_domain_faulted_locked(domain_id, expected_instance_id, reason)
    }
    fn mark_domain_faulted_locked(
        &self,
        domain_id: &str,
        expected_instance_id: Option<u64>,
        reason: &str,
    ) -> bool {
        if let Some(expected) = expected_instance_id {
            let inactive = {
                let mut domains = self.domains.write();
                domains.get_mut(domain_id).and_then(|entry| {
                    if entry.instance_id == expected {
                        return None;
                    }
                    if entry
                        .candidate
                        .as_ref()
                        .is_some_and(|c| c.instance_id == expected)
                    {
                        entry.candidate.take().map(|c| c.pipe_tx)
                    } else {
                        entry
                            .draining
                            .iter()
                            .position(|d| d.instance_id == expected)
                            .map(|i| entry.draining.remove(i).pipe_tx)
                    }
                })
            };
            if let Some(pipe) = inactive {
                pipe.close();
                self.remove_subscriptions(domain_id, expected);
                for (source, receipt) in
                    self.ledger
                        .fail_inflight_for_channel(domain_id, pipe.channel_id(), reason)
                {
                    self.observer.emit(
                        &source,
                        &receipt.target,
                        &receipt.message_id,
                        &receipt.logical_id,
                        receipt.seq,
                        &receipt.message_type,
                        receipt.status,
                        receipt.data.clone(),
                    );
                    self.send_to_domain_pipe(&source, KernelToDomainFrame::Receipt(receipt));
                }
                self.ledger
                    .clear_session_dedup_for_domain(&format!("{domain_id}#{expected}"));
                return true;
            }
        }
        let old_pipe;
        let generation;
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
            generation = entry.instance_id;
            eprintln!(
                "kernel domain {domain_id} generation {} disconnected: {reason}",
                entry.instance_id
            );
            entry.status = DomainStatus::Faulted;
            old_pipe = entry.pipe_tx.take();

            for h in entry.handlers.keys() {
                routes.remove(&format!("{domain_id}.{h}"));
            }
            entry.handlers.clear();
        }

        self.remove_subscriptions(domain_id, generation);

        let failed_deliveries = if let Some(tx) = old_pipe {
            tx.close();
            self.ledger
                .fail_inflight_for_channel(domain_id, tx.channel_id(), reason)
        } else {
            Vec::new()
        };
        for (source_domain, receipt) in failed_deliveries {
            self.observer.emit(
                &source_domain,
                &receipt.target,
                &receipt.message_id,
                &receipt.logical_id,
                receipt.seq,
                &receipt.message_type,
                receipt.status,
                receipt.data.clone(),
            );
            self.send_to_domain_pipe(&source_domain, KernelToDomainFrame::Receipt(receipt));
        }
        self.ledger
            .clear_session_dedup_for_domain(&format!("{domain_id}#{generation}"));
        true
    }

    pub(crate) fn failed_output_queues(&self) -> Vec<(String, u64, String)> {
        let pipes: Vec<_> = self
            .domains
            .read()
            .iter()
            .filter_map(|(id, entry)| {
                entry
                    .pipe_tx
                    .as_ref()
                    .map(|tx| (id.clone(), entry.instance_id, tx.clone()))
            })
            .collect();
        pipes
            .into_iter()
            .filter_map(|(id, generation, tx)| tx.failure().map(|reason| (id, generation, reason)))
            .collect()
    }

    fn subscription_key(&self, domain: &str, msg: &KernelMessage) -> String {
        format!(
            "{domain}#{}",
            self.ledger.source_instance(&msg.message_id).unwrap_or(0)
        )
    }
    fn remove_subscriptions(&self, domain: &str, generation: u64) {
        let key = format!("{domain}#{generation}");
        self.log_subscribers.write().remove(&key);
        self.telemetry_subscribers.write().remove(&key);
        self.notify_subscribers.write().remove(&key);
        self.observer_subscribers.write().remove(&key);
    }
    fn pipe_for_instance(&self, domain: &str, instance: u64) -> Option<PipeSender> {
        self.domains.read().get(domain).and_then(|entry| {
            if entry.instance_id == instance {
                entry.pipe_tx.clone()
            } else if let Some(candidate) = entry
                .candidate
                .as_ref()
                .filter(|c| c.instance_id == instance)
            {
                Some(candidate.pipe_tx.clone())
            } else {
                entry
                    .draining
                    .iter()
                    .find(|d| d.instance_id == instance)
                    .map(|d| d.pipe_tx.clone())
            }
        })
    }
    fn send_to_instance(&self, domain: &str, instance: Option<u64>, frame: KernelToDomainFrame) {
        let failure = matches!(&frame, KernelToDomainFrame::Receipt(r) if self.ledger.has_unpersisted_failure(&r.message_id));
        self.send_to_instance_inner(domain, instance, frame, failure);
    }
    fn send_to_instance_inner(
        &self,
        domain: &str,
        instance: Option<u64>,
        frame: KernelToDomainFrame,
        unpersisted: bool,
    ) {
        let pipe = match instance {
            Some(id) if id > 0 => self.pipe_for_instance(domain, id),
            _ => self
                .domains
                .read()
                .get(domain)
                .and_then(|e| e.pipe_tx.clone()),
        };
        if let Some(tx) = pipe {
            // Enqueue failures close this generation's outbox and retain its failure reason.
            // The supervisor converges it outside publication/domain locks; do not fault
            // synchronously here (that would recursively publish under the same locks).
            let _ = if unpersisted {
                tx.send_unpersisted_failure(frame)
            } else {
                tx.send(frame)
            };
        }
    }
    fn send_to_domain_pipe(&self, domain_id: &str, frame: KernelToDomainFrame) {
        let origin = match &frame {
            KernelToDomainFrame::Receipt(r) => self.ledger.source_instance(&r.message_id),
            _ => None,
        };
        let terminal = match &frame {
            KernelToDomainFrame::Receipt(r) if r.status.is_terminal() => Some(r.clone()),
            _ => None,
        };
        self.send_to_instance(domain_id, origin, frame);
        if let Some(receipt) = terminal {
            // Publication retires live routing identity, not the committed Durable result.
            // Failed persistence keeps its live failure identity while the source is alive.
            // A failed/missing old pipe must never turn into mail for a new generation.
            self.ledger.finish_publication(&receipt);
        }
    }
}

impl Drop for KernelRouter {
    fn drop(&mut self) {
        self.stop_log_pump.store(true, Ordering::SeqCst);
        if let Some(handle) = self.log_pump_handle.lock().take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.observer_pump_handle.lock().take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod control_generation_tests {
    use super::*;

    #[test]
    fn old_control_completion_cannot_enter_replacement_pipe() {
        let kernel = crate::AtelierKernel::new_in_memory().unwrap();
        let old = kernel.attach_domain("controller");
        let (_, ids, _) = kernel
            .ledger()
            .allocate_submission("controller-session", 2, 1);
        let old_id = ids[0].clone();
        kernel.ledger().bind_origin(&ids, old.instance_id());
        let replacement = kernel.attach_domain("controller");
        let receipt = MessageReceipt {
            logical_id: "logical-old".into(),
            message_id: old_id,
            seq: 2,
            target: KERNEL_SUPERVISOR_TARGET.into(),
            message_type: MSG_RESTART_DOMAIN.into(),
            status: ContractStatus::Completed,
            data: Some(json!({"status":"restarted"})),
            timestamp: now_millis(),
        };
        kernel
            .router()
            .send_to_domain_pipe("controller", KernelToDomainFrame::Receipt(receipt.clone()));
        assert!(replacement.drain_frames().is_empty());
        let mut current = receipt;
        current.message_id = "current-control".into();
        kernel
            .router()
            .send_to_domain_pipe("controller", KernelToDomainFrame::Receipt(current));
        assert_eq!(replacement.drain_frames().len(), 1);
    }
}
#[cfg(test)]
mod generation_acceptance_tests {
    use super::*;
    fn register(router: &KernelRouter, generation: u64, name: &str) -> Vec<String> {
        router
            .handle_frame_for_instance(
                "worker",
                generation,
                DomainToKernelFrame::Send(OutboundMessage {
                    seq: 1,
                    targets: vec![KERNEL_REGISTRY_TARGET.into()],
                    message_type: MSG_REGISTER_HANDLERS.into(),
                    params: json!({"handlers":[name]}),
                    data: json!({}),
                }),
            )
            .unwrap()
    }
    #[test]
    fn candidate_sequence_registration_and_receipts_are_isolated_from_old_generation() {
        let k = crate::AtelierKernel::new_in_memory().unwrap();
        let old = k.attach_domain("worker");
        old.register_handlers(&["old"]);
        old.drain_frames();
        let sender = k.attach_domain("sender");
        let pending = sender.send_one(
            vec!["worker.old".into()],
            "Durable",
            json!({}),
            json!({"owner":"old"}),
        );
        old.drain_frames();
        sender.drain_frames();
        let (tx, rx) = crate::outbox::channel(k.ledger().db_conn.as_ref().unwrap().clone());
        let candidate = k.router().attach_candidate_pipe("worker", tx);
        let ids = register(k.router(), candidate, "new");
        assert!(k.router().is_candidate_ready("worker", candidate));
        assert_eq!(k.router().active_routes(), vec!["worker.old"]);
        let mut candidate_completed = false;
        while let Ok(frame) = rx.try_recv() {
            if let KernelToDomainFrame::Receipt(r) = frame {
                if r.message_id == ids[0] && r.status == ContractStatus::Completed {
                    candidate_completed = true;
                }
            }
            rx.acknowledge().unwrap();
        }
        assert!(
            candidate_completed,
            "candidate registration collided with old seq=1"
        );
        assert!(
            old.drain_frames().is_empty(),
            "candidate reply leaked to old pipe"
        );
        assert!(k.router().commit_candidate("worker", candidate));
        let current = sender.send_one(
            vec!["worker.new".into()],
            "Durable",
            json!({}),
            json!({"owner":"new"}),
        );
        sender.drain_frames();
        old.report_receipt(
            &current,
            "worker.new",
            ContractStatus::Completed,
            Some(json!({"wrong":true})),
        );
        assert_eq!(
            k.ledger()
                .get_record(&current, "worker.new")
                .unwrap()
                .status,
            ContractStatus::Sent,
            "draining old generation claimed new generation's message"
        );
        k.router()
            .cleanup_draining_instance("worker", old.instance_id());
        assert_eq!(
            k.ledger()
                .get_record(&pending, "worker.old")
                .unwrap()
                .status,
            ContractStatus::ExecutionFailed
        );
        assert_eq!(
            k.ledger()
                .get_record(&current, "worker.new")
                .unwrap()
                .status,
            ContractStatus::Sent,
            "old generation cleanup failed new work"
        );
        k.router().handle_frame_for_instance(
            "worker",
            candidate,
            DomainToKernelFrame::Receipt(InboundReceipt {
                message_id: current.clone(),
                target: "worker.new".into(),
                status: ContractStatus::Completed,
                data: Some(json!({"right":true})),
            }),
        );
        assert_eq!(
            k.ledger().get_record(&current, "worker.new").unwrap().data,
            Some(json!({"right":true}))
        );
    }
    #[test]
    fn candidate_eof_removes_candidate_readiness_without_revoking_active_routes() {
        let k = crate::AtelierKernel::new_in_memory().unwrap();
        let old = k.attach_domain("worker");
        old.register_handlers(&["old"]);
        old.drain_frames();
        let (tx, _rx) = crate::outbox::channel(k.ledger().db_conn.as_ref().unwrap().clone());
        let candidate = k.router().attach_candidate_pipe("worker", tx);
        register(k.router(), candidate, "new");
        assert!(k.router().is_candidate_ready("worker", candidate));
        assert!(k.router().mark_domain_faulted_for_instance(
            "worker",
            Some(candidate),
            "candidate IPC EOF"
        ));
        assert!(!k.router().is_candidate_ready("worker", candidate));
        assert_eq!(
            k.router().domain_status("worker"),
            Some(DomainStatus::Active)
        );
        assert_eq!(k.router().active_routes(), vec!["worker.old"]);
    }
}

#[cfg(test)]
mod receipt_publication_failure_tests {
    use super::*;
    #[test]
    fn failed_terminal_send_keeps_durable_results_by_both_ids_after_restart() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tmp/core-reliability");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("receipt-publication-{}.db", uuid::Uuid::new_v4()));
        let mut saved = Vec::new();
        {
            let k = crate::AtelierKernel::open_with_db(&path).unwrap();
            let sender = k.attach_domain("sender");
            let worker = k.attach_domain("worker");
            worker.register_handlers(&["run"]);
            worker.drain_frames();
            for (seq, status) in [
                (71, ContractStatus::Completed),
                (72, ContractStatus::ExecutionFailed),
            ] {
                let id = sender.send_with_seq(
                    seq,
                    vec!["worker.run".into()],
                    "Durable",
                    json!({}),
                    json!({}),
                )[0]
                .clone();
                worker.drain_frames();
                sender.drain_frames();
                // Use an obsolete generation with no pipe, without killing the live source session.
                // This deterministically fails send_to_instance, including before the monitor runs.
                k.ledger().bind_origin(&[id.clone()], u64::MAX / 2);
                let logical = k.ledger().logical_id_for_message(&id);
                let data = json!({"result":"must survive failed receipt publication", "seq":seq});
                worker.report_receipt(&id, "worker.run", status, Some(data.clone()));
                assert!(sender.drain_frames().is_empty());
                assert!(
                    k.ledger().source_instance(&id).is_none(),
                    "terminal identity should be reclaimed"
                );
                let record = k.ledger().get_persisted_record(&id).unwrap();
                assert_eq!(record.status, status);
                assert_eq!(record.data, Some(data.clone()));
                let by_logical = k.ledger().receipts_for_logical(&logical);
                assert_eq!(by_logical.len(), 1);
                assert_eq!(by_logical[0].message_id, id);
                assert_eq!(by_logical[0].data, Some(data.clone()));
                // Explicit same-session replay is allowed; it must not execute work again.
                assert_eq!(
                    sender.send_with_seq(
                        seq,
                        vec!["worker.run".into()],
                        "Durable",
                        json!({}),
                        json!({})
                    ),
                    [id.clone()]
                );
                assert!(worker.drain_frames().is_empty());
                assert!(sender.drain_frames().iter().any(|f| matches!(f, KernelToDomainFrame::Receipt(r) if r.message_id==id && r.status==status)));
                saved.push((id, logical, status, data));
            }
            let replacement = k.attach_domain("sender");
            assert!(
                replacement.drain_frames().is_empty(),
                "old results are not unsolicited mail to a new process"
            );
        }
        let k = crate::AtelierKernel::open_with_db(&path).unwrap();
        for (id, logical, status, data) in saved {
            assert_eq!(k.ledger().logical_id_for_message(&id), logical);
            let by_id = k.ledger().receipts_for_message(&id);
            let by_logical = k.ledger().receipts_for_logical(&logical);
            assert_eq!(by_id.len(), 1);
            assert_eq!(by_logical.len(), 1);
            for r in [by_id[0].clone(), by_logical[0].clone()] {
                assert_eq!(r.message_id, id);
                assert_eq!(r.status, status);
                assert_eq!(r.data, Some(data.clone()));
            }
        }
    }
    #[test]
    fn failed_unpersisted_terminal_send_retains_live_failure_identity() {
        let k = crate::AtelierKernel::new_in_memory().unwrap();
        let sender = k.attach_domain("sender");
        let worker = k.attach_domain("worker");
        worker.register_handlers(&["run"]);
        worker.drain_frames();
        let id = sender.send_one(vec!["worker.run".into()], "Durable", json!({}), json!({}));
        worker.drain_frames();
        sender.drain_frames();
        k.ledger().bind_origin(&[id.clone()], u64::MAX / 2);
        k.ledger().db_conn.as_ref().unwrap().lock().execute_batch(
            "CREATE TRIGGER reject_terminal BEFORE UPDATE ON kernel_ledger BEGIN SELECT RAISE(FAIL,'injected terminal write failure'); END;"
        ).unwrap();
        worker.report_receipt(
            &id,
            "worker.run",
            ContractStatus::Completed,
            Some(json!({"not_committed":true})),
        );
        assert!(sender.drain_frames().is_empty());
        assert!(
            k.ledger().source_instance(&id).is_some(),
            "unpersisted failure needs live identity for replay"
        );
        let receipt = k.ledger().receipts_for_message(&id).pop().unwrap();
        assert_eq!(receipt.status, ContractStatus::ExecutionFailed);
        assert_eq!(receipt.data.unwrap()["persisted"], false);
        assert_eq!(
            k.ledger().get_persisted_record(&id).unwrap().status,
            ContractStatus::Sent
        );
    }
}
