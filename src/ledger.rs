use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::protocol::{ContractStatus, MessageReceipt, now_millis, parse_target_address};

/// Authoritative per-target delivery record stored in the Kernel Ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryRecord {
    pub logical_id: String,
    pub message_id: String,
    pub seq: u64,
    pub source_domain: String,
    pub target: String,
    pub message_type: String,
    pub status: ContractStatus,
    pub data: Option<Value>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl DeliveryRecord {
    /// Converts the current ledger record into a `MessageReceipt`.
    pub fn to_receipt(&self) -> MessageReceipt {
        MessageReceipt {
            logical_id: self.logical_id.clone(),
            message_id: self.message_id.clone(),
            seq: self.seq,
            target: self.target.clone(),
            message_type: self.message_type.clone(),
            status: self.status,
            data: self.data.clone(),
            timestamp: self.updated_at,
        }
    }
}

/// Outcome of attempting to advance a `message_id` record via an inbound receipt.
#[derive(Debug, Clone)]
pub enum ReceiptTransition {
    /// State advanced monotonically; contains `(source_domain, MessageReceipt)` to forward to sender.
    Advanced {
        source_domain: String,
        receipt: MessageReceipt,
    },
    /// Receipt was a duplicate or older than current state; safely absorbed by kernel idempotency.
    DuplicateOrStale,
    /// `message_id` does not exist in the ledger.
    UnknownDelivery,
    /// Target domain attempted to report a receipt for another domain's handler.
    UnauthorizedTarget,
}

/// Kernel ID Generator and Three-Level Idempotency & State Ledger.
pub struct KernelLedger {
    next_id_seq: AtomicU64,
    instance_id: String,
    /// Session-scoped submission deduplication, preserving ordered split identities.
    submissions: RwLock<HashMap<(String, u64), (String, Vec<String>)>>,
    deliveries: RwLock<HashMap<String, DeliveryRecord>>,
    logical_messages: RwLock<HashMap<String, Vec<String>>>,
    child_logical: RwLock<HashMap<String, String>>,
}

impl Default for KernelLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl KernelLedger {
    pub fn new() -> Self {
        Self {
            next_id_seq: AtomicU64::new(1),
            instance_id: uuid::Uuid::new_v4().simple().to_string(),
            submissions: RwLock::new(HashMap::new()),
            deliveries: RwLock::new(HashMap::new()),
            logical_messages: RwLock::new(HashMap::new()),
            child_logical: RwLock::new(HashMap::new()),
        }
    }

    /// Atomically allocates the logical identity and all ordered child IDs.
    /// Router serializes each authenticated source session across allocation and dispatch.
    pub fn allocate_submission(
        &self,
        source: &str,
        seq: u64,
        count: usize,
    ) -> (String, Vec<String>, bool) {
        let mut submissions = self.submissions.write();
        let key = (source.to_string(), seq);
        if seq > 0 {
            if let Some((logical, children)) = submissions.get(&key) {
                return (logical.clone(), children.clone(), true);
            }
        }
        let logical = self.generate_message_id().replacen("msg_", "logical_", 1);
        let children: Vec<_> = (0..count).map(|_| self.generate_message_id()).collect();
        self.logical_messages
            .write()
            .insert(logical.clone(), children.clone());
        let mut parents = self.child_logical.write();
        for child in &children {
            parents.insert(child.clone(), logical.clone());
        }
        if seq > 0 {
            submissions.insert(key, (logical.clone(), children.clone()));
        }
        (logical, children, false)
    }

    pub fn logical_id_for_message(&self, message_id: &str) -> String {
        self.child_logical
            .read()
            .get(message_id)
            .cloned()
            .unwrap_or_else(|| message_id.to_string())
    }

    /// Generates a globally unique kernel message ID (instance UUID plus local sequence).
    pub fn generate_message_id(&self) -> String {
        let seq = self.next_id_seq.fetch_add(1, Ordering::Relaxed);
        let ts = now_millis();
        format!("msg_{ts}_{}_{seq:06}", self.instance_id)
    }

    /// Clears `(source_domain, seq)` session deduplication entries when a domain process restarts.
    pub fn clear_session_dedup_for_domain(&self, domain: &str) {
        self.submissions.write().retain(|(d, _), _| d != domain);
    }

    /// Registers an initial per-target delivery state (`Sent`, `TargetInvalid`, or `ContractInvalid`)
    /// if `message_id` has not yet been recorded.
    ///
    /// Returns `(DeliveryRecord, is_newly_created)`.
    pub fn record_initial_target_state(
        &self,
        message_id: &str,
        seq: u64,
        source_domain: &str,
        target: &str,
        message_type: &str,
        initial_status: ContractStatus,
        data: Option<Value>,
    ) -> (DeliveryRecord, bool) {
        let key = message_id.to_string();
        let mut deliveries = self.deliveries.write();
        if let Some(existing) = deliveries.get(&key) {
            return (existing.clone(), false);
        }

        let now = now_millis();
        let record = DeliveryRecord {
            logical_id: self.logical_id_for_message(message_id),
            message_id: message_id.to_string(),
            seq,
            source_domain: source_domain.to_string(),
            target: target.to_string(),
            message_type: message_type.to_string(),
            status: initial_status,
            data,
            created_at: now,
            updated_at: now,
        };
        deliveries.insert(key, record.clone());
        drop(deliveries);

        (record, true)
    }

    /// Advances the state of `message_id` monotonically, verifying that `reporting_domain`
    /// owns `target`.
    pub fn advance_status(
        &self,
        reporting_domain: &str,
        message_id: &str,
        target: &str,
        new_status: ContractStatus,
        data: Option<Value>,
    ) -> ReceiptTransition {
        // Verify that the reporting pipe's domain matches the `<domain>.<handler>` prefix.
        let Some((target_domain, _)) = parse_target_address(target) else {
            return ReceiptTransition::UnauthorizedTarget;
        };
        if target_domain != reporting_domain {
            return ReceiptTransition::UnauthorizedTarget;
        }

        let key = message_id.to_string();
        let mut deliveries = self.deliveries.write();
        let Some(record) = deliveries.get_mut(&key) else {
            return ReceiptTransition::UnknownDelivery;
        };

        if record.target != target {
            return ReceiptTransition::UnauthorizedTarget;
        }

        // Terminal states cannot be overwritten, and stage rank must strictly increase.
        if record.status.is_terminal() || new_status.stage_rank() <= record.status.stage_rank() {
            return ReceiptTransition::DuplicateOrStale;
        }

        record.status = new_status;
        if data.is_some() {
            record.data = data;
        }
        record.updated_at = now_millis();

        ReceiptTransition::Advanced {
            source_domain: record.source_domain.clone(),
            receipt: record.to_receipt(),
        }
    }

    /// Replay all child receipts in original submission order.
    pub fn receipts_for_logical(&self, logical_id: &str) -> Vec<MessageReceipt> {
        let ids = self
            .logical_messages
            .read()
            .get(logical_id)
            .cloned()
            .unwrap_or_default();
        let records = self.deliveries.read();
        ids.iter()
            .filter_map(|id| records.get(id).map(DeliveryRecord::to_receipt))
            .collect()
    }

    pub fn receipts_for_message(&self, message_id: &str) -> Vec<MessageReceipt> {
        self.deliveries
            .read()
            .get(message_id)
            .map(|r| vec![r.to_receipt()])
            .unwrap_or_default()
    }

    pub fn get_record(&self, message_id: &str, target: &str) -> Option<DeliveryRecord> {
        self.deliveries
            .read()
            .get(message_id)
            .filter(|r| r.target == target)
            .cloned()
    }

    /// Marks all non-terminal deliveries targeting `crashed_domain` as `ExecutionFailed`.
    /// Returns a list of `(source_domain, MessageReceipt)` to notify waiting senders.
    pub fn fail_inflight_for_domain(
        &self,
        crashed_domain: &str,
        reason: &str,
    ) -> Vec<(String, MessageReceipt)> {
        let mut notifications = Vec::new();
        let now = now_millis();
        let mut deliveries = self.deliveries.write();

        for record in deliveries.values_mut() {
            if !record.status.is_terminal()
                && let Some((target_domain, _)) = parse_target_address(&record.target)
                && target_domain == crashed_domain
            {
                record.status = ContractStatus::ExecutionFailed;
                record.data = Some(json!({
                    "error": "target_domain_faulted",
                    "domain": crashed_domain,
                    "reason": reason,
                }));
                record.updated_at = now;
                notifications.push((record.source_domain.clone(), record.to_receipt()));
            }
        }

        notifications
    }
}
