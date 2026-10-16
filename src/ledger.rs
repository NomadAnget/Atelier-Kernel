use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::protocol::{ContractStatus, MessageReceipt, now_millis, parse_target_address};

/// Authoritative per-target delivery record stored in the Kernel Ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryRecord {
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

/// Outcome of attempting to advance a `(message_id, target)` record via an inbound receipt.
#[derive(Debug, Clone)]
pub enum ReceiptTransition {
    /// State advanced monotonically; contains `(source_domain, MessageReceipt)` to forward to sender.
    Advanced {
        source_domain: String,
        receipt: MessageReceipt,
    },
    /// Receipt was a duplicate or older than current state; safely absorbed by kernel idempotency.
    DuplicateOrStale,
    /// `(message_id, target)` does not exist in the ledger.
    UnknownDelivery,
    /// Target domain attempted to report a receipt for another domain's handler.
    UnauthorizedTarget,
}

/// Kernel ID Generator and Three-Level Idempotency & State Ledger.
pub struct KernelLedger {
    next_id_seq: AtomicU64,
    /// Level 1 Idempotency: `(source_domain, seq) -> message_id` (when `seq > 0`).
    submission_dedup: RwLock<HashMap<(String, u64), String>>,
    /// Level 2 & 3 Idempotency: `(message_id, target) -> DeliveryRecord`.
    deliveries: RwLock<HashMap<(String, String), DeliveryRecord>>,
    /// Index of `message_id -> Vec<target>` preserving target order.
    message_targets: RwLock<HashMap<String, Vec<String>>>,
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
            submission_dedup: RwLock::new(HashMap::new()),
            deliveries: RwLock::new(HashMap::new()),
            message_targets: RwLock::new(HashMap::new()),
        }
    }

    /// Allocates a new kernel-generated `message_id` or returns an existing one if `(source_domain, seq)`
    /// was already submitted (`seq > 0`).
    ///
    /// Returns `(message_id, is_duplicate_submission)`.
    pub fn allocate_or_get_message_id(&self, source_domain: &str, seq: u64) -> (String, bool) {
        if seq > 0 {
            let key = (source_domain.to_string(), seq);
            if let Some(existing_id) = self.submission_dedup.read().get(&key).cloned() {
                return (existing_id, true);
            }
            let mut dedup = self.submission_dedup.write();
            if let Some(existing_id) = dedup.get(&key).cloned() {
                return (existing_id, true);
            }
            let new_id = self.generate_message_id();
            dedup.insert(key, new_id.clone());
            (new_id, false)
        } else {
            (self.generate_message_id(), false)
        }
    }

    /// Generates a globally unique, monotonically ordered Kernel Message ID.
    pub fn generate_message_id(&self) -> String {
        let seq = self.next_id_seq.fetch_add(1, Ordering::Relaxed);
        let ts = now_millis();
        format!("msg_{ts}_{seq:06}")
    }

    /// Clears `(source_domain, seq)` session deduplication entries when a domain process restarts.
    pub fn clear_session_dedup_for_domain(&self, domain: &str) {
        self.submission_dedup
            .write()
            .retain(|(d, _), _| d != domain);
    }

    /// Registers an initial per-target delivery state (`Sent`, `TargetInvalid`, or `ContractInvalid`)
    /// if `(message_id, target)` has not yet been recorded.
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
        let key = (message_id.to_string(), target.to_string());
        let mut deliveries = self.deliveries.write();
        if let Some(existing) = deliveries.get(&key) {
            return (existing.clone(), false);
        }

        let now = now_millis();
        let record = DeliveryRecord {
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

        let mut msg_targets = self.message_targets.write();
        let targets = msg_targets.entry(message_id.to_string()).or_default();
        if !targets.iter().any(|t| t == target) {
            targets.push(target.to_string());
        }

        (record, true)
    }

    /// Advances the state of `(message_id, target)` monotonically, verifying that `reporting_domain`
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

        let key = (message_id.to_string(), target.to_string());
        let mut deliveries = self.deliveries.write();
        let Some(record) = deliveries.get_mut(&key) else {
            return ReceiptTransition::UnknownDelivery;
        };

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

    /// Returns all current `MessageReceipt`s for a given `message_id` (used for idempotent replay on duplicate `seq`).
    pub fn receipts_for_message(&self, message_id: &str) -> Vec<MessageReceipt> {
        let targets = self
            .message_targets
            .read()
            .get(message_id)
            .cloned()
            .unwrap_or_default();
        let deliveries = self.deliveries.read();
        targets
            .into_iter()
            .filter_map(|target| {
                deliveries
                    .get(&(message_id.to_string(), target))
                    .map(DeliveryRecord::to_receipt)
            })
            .collect()
    }

    /// Inspects a specific `(message_id, target)` record.
    pub fn get_record(&self, message_id: &str, target: &str) -> Option<DeliveryRecord> {
        self.deliveries
            .read()
            .get(&(message_id.to_string(), target.to_string()))
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
