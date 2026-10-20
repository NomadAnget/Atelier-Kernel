use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::protocol::{
    ContractStatus, DeliveryPolicy, MessageReceipt, now_millis, parse_target_address,
};
use crate::storage::{StorageError, verify_sqlite_table_columns};

pub const KERNEL_LEDGER_TABLE: &str = "kernel_ledger";
pub const KERNEL_LEDGER_COLUMNS: &[&str] = &[
    "message_id",
    "logical_id",
    "seq",
    "source_domain",
    "target",
    "message_type",
    "status",
    "data",
    "created_at",
    "updated_at",
];

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
///
/// Enforces tiered persistence policies based on `DeliveryPolicy`:
/// - `Durable`: persisted to SQLite `kernel_ledger`; restored terminal payloads are read lazily.
/// - `Volatile`: retained in-memory only; never written to SQLite.
/// - `Ephemeral`: tracked in-memory while in-flight and automatically evicted upon reaching a terminal state.
pub struct KernelLedger {
    next_id_seq: AtomicU64,
    instance_id: String,
    /// Session-scoped submission deduplication, preserving ordered split identities.
    submissions: RwLock<HashMap<(String, u64), (String, Vec<String>)>>,
    deliveries: RwLock<HashMap<String, DeliveryRecord>>,
    logical_messages: RwLock<HashMap<String, Vec<String>>>,
    child_logical: RwLock<HashMap<String, String>>,
    db_conn: Option<Mutex<Connection>>,
}

impl Default for KernelLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl KernelLedger {
    pub fn new() -> Self {
        Self::open_with_db(None).unwrap_or_else(|_| Self {
            next_id_seq: AtomicU64::new(1),
            instance_id: uuid::Uuid::new_v4().simple().to_string(),
            submissions: RwLock::new(HashMap::new()),
            deliveries: RwLock::new(HashMap::new()),
            logical_messages: RwLock::new(HashMap::new()),
            child_logical: RwLock::new(HashMap::new()),
            db_conn: None,
        })
    }

    /// Initializes the Kernel Ledger backed by an optional SQLite file path (or in-memory SQLite when `None`).
    pub fn open_with_db(db_path: Option<&Path>) -> Result<Self, StorageError> {
        let conn = match db_path {
            Some(path) => {
                if let Some(parent) = path.parent()
                    && !parent.as_os_str().is_empty()
                {
                    let _ = std::fs::create_dir_all(parent);
                }
                Connection::open(path)?
            }
            None => Connection::open_in_memory()?,
        };
        conn.execute_batch(&format!(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;
             CREATE TABLE IF NOT EXISTS {KERNEL_LEDGER_TABLE} (
                 message_id    TEXT PRIMARY KEY,
                 logical_id    TEXT NOT NULL,
                 seq           INTEGER NOT NULL,
                 source_domain TEXT NOT NULL,
                 target        TEXT NOT NULL,
                 message_type  TEXT NOT NULL,
                 status        TEXT NOT NULL,
                 data          TEXT,
                 created_at    INTEGER NOT NULL,
                 updated_at    INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_{KERNEL_LEDGER_TABLE}_logical ON {KERNEL_LEDGER_TABLE}(logical_id);
             CREATE INDEX IF NOT EXISTS idx_{KERNEL_LEDGER_TABLE}_status ON {KERNEL_LEDGER_TABLE}(status);"
        ))?;
        verify_sqlite_table_columns(&conn, KERNEL_LEDGER_TABLE, KERNEL_LEDGER_COLUMNS)?;

        let mut deliveries_map = HashMap::new();
        let mut logical_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut child_map = HashMap::new();

        {
            let mut stmt = conn.prepare(&format!(
                "SELECT message_id, logical_id, seq, source_domain, target, message_type, status,
                        CASE WHEN status IN ('completed','target_invalid','contract_invalid','execution_failed')
                             THEN NULL ELSE data END AS data, created_at, updated_at
                 FROM {KERNEL_LEDGER_TABLE}
                 ORDER BY created_at ASC, message_id ASC"
            ))?;
            let rows = stmt.query_map([], Self::row_to_record)?;
            for item in rows {
                let record = item?;
                child_map.insert(record.message_id.clone(), record.logical_id.clone());
                logical_map
                    .entry(record.logical_id.clone())
                    .or_default()
                    .push(record.message_id.clone());
                if !record.status.is_terminal() {
                    deliveries_map.insert(record.message_id.clone(), record);
                }
            }
        }

        Ok(Self {
            next_id_seq: AtomicU64::new(1),
            instance_id: uuid::Uuid::new_v4().simple().to_string(),
            submissions: RwLock::new(HashMap::new()),
            deliveries: RwLock::new(deliveries_map),
            logical_messages: RwLock::new(logical_map),
            child_logical: RwLock::new(child_map),
            db_conn: Some(Mutex::new(conn)),
        })
    }

    fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeliveryRecord> {
        let status_raw: String = row.get(6)?;
        let status: ContractStatus =
            serde_json::from_value(Value::String(status_raw)).unwrap_or(ContractStatus::Sent);
        let data_raw: Option<String> = row.get(7)?;
        let data = data_raw.and_then(|s| serde_json::from_str(&s).ok());
        Ok(DeliveryRecord {
            message_id: row.get(0)?,
            logical_id: row.get(1)?,
            seq: row.get::<_, i64>(2)? as u64,
            source_domain: row.get(3)?,
            target: row.get(4)?,
            message_type: row.get(5)?,
            status,
            data,
            created_at: row.get(8)?,
            updated_at: row.get(9)?,
        })
    }

    fn persist_record(&self, record: &DeliveryRecord) {
        let Some(ref conn_mutex) = self.db_conn else {
            return;
        };
        let status_str = serde_json::to_value(record.status)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_else(|| "sent".to_string());
        let data_str = record
            .data
            .as_ref()
            .and_then(|d| serde_json::to_string(d).ok());
        let conn = conn_mutex.lock();
        let _ = conn.execute(
            &format!(
                "INSERT INTO {KERNEL_LEDGER_TABLE}
                 (message_id, logical_id, seq, source_domain, target, message_type, status, data, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(message_id) DO UPDATE SET
                     status = excluded.status,
                     data = excluded.data,
                     updated_at = excluded.updated_at"
            ),
            params![
                record.message_id,
                record.logical_id,
                record.seq as i64,
                record.source_domain,
                record.target,
                record.message_type,
                status_str,
                data_str,
                record.created_at,
                record.updated_at,
            ],
        );
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

    /// Diagnostic collection counts only; never clones or serializes business payloads.
    pub fn retention_counts(&self) -> Value {
        let (resident, capacity, mut durable, mut volatile, mut ephemeral) = {
            let records = self.deliveries.read();
            (records.len(), records.capacity(), 0usize, 0usize, 0usize)
        };
        for record in self.deliveries.read().values() {
            match DeliveryPolicy::resolve(&record.target, &record.message_type) {
                DeliveryPolicy::Durable => durable += 1,
                DeliveryPolicy::Volatile => volatile += 1,
                DeliveryPolicy::Ephemeral => ephemeral += 1,
            }
        }
        let submissions = self.submissions.read();
        let logical = self.logical_messages.read();
        let parents = self.child_logical.read();
        json!({"resident_records":resident, "record_capacity":capacity,
            "durable_records":durable, "volatile_records":volatile, "ephemeral_inflight":ephemeral,
            "submissions":submissions.len(), "submission_capacity":submissions.capacity(),
            "logical_groups":logical.len(), "logical_capacity":logical.capacity(),
            "child_links":parents.len(), "child_capacity":parents.capacity()})
    }

    fn retire_ephemeral_identity(&self, record: &DeliveryRecord) {
        // Match allocation's lock order; callers have already released deliveries.
        let mut submissions = self.submissions.write();
        let mut logical = self.logical_messages.write();
        let mut parents = self.child_logical.write();
        parents.remove(&record.message_id);
        if logical.get(&record.logical_id)
            .is_some_and(|ids| ids.iter().all(|id| !parents.contains_key(id)))
        {
            logical.remove(&record.logical_id);
            let key = (record.source_domain.clone(), record.seq);
            if submissions.get(&key).is_some_and(|(id, _)| id == &record.logical_id) {
                submissions.remove(&key);
            }
        }
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
        let policy = DeliveryPolicy::resolve(target, message_type);
        if let Some(existing) = self.get_persisted_record(message_id) {
            return (existing, false);
        }
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

        // Ephemeral messages that are already terminal on ingress (e.g. TargetInvalid / ContractInvalid)
        // do not stay in the in-memory ledger.
        if !(policy == DeliveryPolicy::Ephemeral && initial_status.is_terminal()) {
            deliveries.insert(key, record.clone());
        }
        drop(deliveries);

        if policy == DeliveryPolicy::Durable {
            self.persist_record(&record);
        }

        if policy == DeliveryPolicy::Ephemeral && initial_status.is_terminal() {
            self.retire_ephemeral_identity(&record);
        }

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
        if target_domain != reporting_domain && !(reporting_domain == "kernel" && target_domain == "web_ui") {
            return ReceiptTransition::UnauthorizedTarget;
        }

        let key = message_id.to_string();
        let mut deliveries = self.deliveries.write();
        let Some(record) = deliveries.get_mut(&key) else {
            drop(deliveries);
            return match self.get_persisted_record(message_id) {
                Some(record) if record.target != target => ReceiptTransition::UnauthorizedTarget,
                Some(record) if record.status.is_terminal() => ReceiptTransition::DuplicateOrStale,
                _ => ReceiptTransition::UnknownDelivery,
            };
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

        let policy = DeliveryPolicy::resolve(&record.target, &record.message_type);
        let updated_record = record.clone();
        let source_domain = record.source_domain.clone();
        let receipt = record.to_receipt();

        if policy == DeliveryPolicy::Ephemeral && new_status.is_terminal() {
            deliveries.remove(&key);
        }
        drop(deliveries);

        if policy == DeliveryPolicy::Durable {
            self.persist_record(&updated_record);
        }

        if policy == DeliveryPolicy::Ephemeral && new_status.is_terminal() {
            self.retire_ephemeral_identity(&updated_record);
        }

        ReceiptTransition::Advanced {
            source_domain,
            receipt,
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
        ids.iter()
            .filter_map(|id| self.record_for_message(id).map(|record| record.to_receipt()))
            .collect()
    }

    pub fn receipts_for_message(&self, message_id: &str) -> Vec<MessageReceipt> {
        self.record_for_message(message_id)
            .map(|r| vec![r.to_receipt()])
            .unwrap_or_default()
    }

    pub fn get_record(&self, message_id: &str, target: &str) -> Option<DeliveryRecord> {
        self.record_for_message(message_id)
            .filter(|r| r.target == target)
    }

    fn record_for_message(&self, message_id: &str) -> Option<DeliveryRecord> {
        let resident = self.deliveries.read().get(message_id).cloned();
        resident.or_else(|| self.get_persisted_record(message_id))
    }

    /// Queries the persistent SQLite `kernel_ledger` table for `message_id`.
    /// Returns `Some(DeliveryRecord)` only for `Durable` deliveries.
    pub fn get_persisted_record(&self, message_id: &str) -> Option<DeliveryRecord> {
        let conn_mutex = self.db_conn.as_ref()?;
        let conn = conn_mutex.lock();
        conn.query_row(
            &format!(
                "SELECT message_id, logical_id, seq, source_domain, target, message_type, status, data, created_at, updated_at
                 FROM {KERNEL_LEDGER_TABLE}
                 WHERE message_id = ?1"
            ),
            params![message_id],
            Self::row_to_record,
        )
        .optional()
        .ok()
        .flatten()
    }

    /// Returns the number of persisted delivery records in the SQLite `kernel_ledger` table.
    pub fn persisted_count(&self) -> usize {
        let Some(ref conn_mutex) = self.db_conn else {
            return 0;
        };
        let conn = conn_mutex.lock();
        conn.query_row(
            &format!("SELECT COUNT(*) FROM {KERNEL_LEDGER_TABLE}"),
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|n| n as usize)
        .unwrap_or(0)
    }

    /// Marks all non-terminal deliveries targeting `crashed_domain` as `ExecutionFailed`.
    /// Returns a list of `(source_domain, MessageReceipt)` to notify waiting senders.
    pub fn fail_inflight_for_domain(
        &self,
        crashed_domain: &str,
        reason: &str,
    ) -> Vec<(String, MessageReceipt)> {
        let mut notifications = Vec::new();
        let mut durable_updates = Vec::new();
        let mut ephemeral_removals = Vec::new();
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

                match DeliveryPolicy::resolve(&record.target, &record.message_type) {
                    DeliveryPolicy::Durable => durable_updates.push(record.clone()),
                    DeliveryPolicy::Ephemeral => ephemeral_removals.push(record.clone()),
                    DeliveryPolicy::Volatile => {}
                }
            }
        }

        for record in &ephemeral_removals {
            deliveries.remove(&record.message_id);
        }
        drop(deliveries);

        for record in ephemeral_removals {
            self.retire_ephemeral_identity(&record);
        }

        for rec in durable_updates {
            self.persist_record(&rec);
        }

        notifications
    }
}

#[cfg(test)]
mod restoration_tests {
    use super::*;

    #[test]
    fn ephemeral_identity_indexes_live_only_while_children_are_inflight() {
        let ledger = KernelLedger::new();
        for seq in 1..=100 {
            let (logical, ids, duplicate) = ledger.allocate_submission("sender", seq, 2);
            assert!(!duplicate);
            assert_eq!(ledger.allocate_submission("sender", seq, 2), (logical.clone(), ids.clone(), true));
            for id in &ids {
                ledger.record_initial_target_state(id, seq, "sender", "worker.run", "Ephemeral", ContractStatus::Sent, None);
            }
            for (index, id) in ids.iter().enumerate() {
                let ReceiptTransition::Advanced {receipt, ..} = ledger.advance_status("worker", id, "worker.run", ContractStatus::Completed, Some(json!({"ok":true}))) else {panic!("completion rejected")};
                assert_eq!(receipt.logical_id, logical);
                assert_eq!(receipt.message_id, *id);
                if index == 0 {
                    assert_eq!(ledger.allocate_submission("sender", seq, 2), (logical.clone(), ids.clone(), true),
                        "a completed sibling must not retire an in-flight submission");
                }
            }
            assert_eq!(ledger.deliveries.read().len(), 0);
            assert_eq!(ledger.submissions.read().len(), 0, "terminal Ephemeral submissions leaked dedup metadata");
            assert_eq!(ledger.child_logical.read().len(), 0);
            assert_eq!(ledger.logical_messages.read().len(), 0);
        }
        let (_, ids, duplicate) = ledger.allocate_submission("sender", 100, 1);
        assert!(!duplicate, "completed Ephemeral sequence no longer reserves session dedup state");
        ledger.record_initial_target_state(&ids[0], 100, "sender", "worker.missing", "Ephemeral", ContractStatus::TargetInvalid, None);
        assert!(ledger.submissions.read().is_empty(), "invalid targets also retire transient identities");
        let (logical, ids, _) = ledger.allocate_submission("sender", 101, 1);
        ledger.record_initial_target_state(&ids[0], 101, "sender", "worker.run", "Ephemeral", ContractStatus::Sent, None);
        let failures = ledger.fail_inflight_for_domain("worker", "crash");
        assert_eq!(failures[0].1.logical_id, logical);
        assert!(ledger.submissions.read().is_empty());
        assert!(ledger.child_logical.read().is_empty());
        assert!(ledger.logical_messages.read().is_empty());
    }

    #[test]
    fn mixed_and_non_ephemeral_submissions_keep_original_identity_and_replay() {
        let ledger = KernelLedger::new();
        let (logical, ids, _) = ledger.allocate_submission("sender", 1, 2);
        for (id, target) in ids.iter().zip(["kernel.storage", "worker.run"]) {
            ledger.record_initial_target_state(id, 1, "sender", target, "StorageQuery", ContractStatus::Sent, None);
        }
        ledger.advance_status("kernel", &ids[0], "kernel.storage", ContractStatus::Completed, Some(json!({"items":[]})));
        ledger.advance_status("worker", &ids[1], "worker.run", ContractStatus::Completed, Some(json!({"ok":true})));
        assert_eq!(ledger.allocate_submission("sender", 1, 2), (logical.clone(), ids.clone(), true));
        let replay = ledger.receipts_for_logical(&logical);
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].message_id, ids[1]);
        assert_eq!(replay[0].logical_id, logical);
        assert_eq!(replay[0].data, Some(json!({"ok":true})));
        let (logical, ids, _) = ledger.allocate_submission("sender", 2, 1);
        ledger.record_initial_target_state(&ids[0], 2, "sender", "worker.run", "Volatile", ContractStatus::Sent, None);
        ledger.advance_status("worker", &ids[0], "worker.run", ContractStatus::Completed, Some(json!({"ok":true})));
        assert_eq!(ledger.allocate_submission("sender", 2, 1), (logical.clone(), ids.clone(), true));
        assert_eq!(ledger.receipts_for_logical(&logical)[0].status, ContractStatus::Completed);
    }

    #[test]
    fn terminal_payloads_restore_lazily_without_losing_identity_receipts_or_pending_work() {
        let folder = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tmp")
            .join(format!("ledger-lazy-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&folder).unwrap();
        let path = folder.join("kernel.db");
        let payload = json!({"text":"x".repeat(128 * 1024), "nested":[{"value":42}]});
        let (logical, ids) = {
            let ledger = KernelLedger::open_with_db(Some(&path)).unwrap();
            let (logical, ids, _) = ledger.allocate_submission("sender", 7, 3);
            for id in &ids {
                ledger.record_initial_target_state(id, 7, "sender", "worker.run", "Durable", ContractStatus::Sent, None);
            }
            for (index, status) in [ContractStatus::Completed, ContractStatus::ExecutionFailed].into_iter().enumerate() {
                for stage in [ContractStatus::Received, ContractStatus::Started, status] {
                    ledger.advance_status("worker", &ids[index], "worker.run", stage, Some(payload.clone()));
                }
            }
            assert_eq!(ledger.persisted_count(), 3);
            (logical, ids)
        };
        let ledger = KernelLedger::open_with_db(Some(&path)).unwrap();
        assert_eq!(ledger.deliveries.read().len(), 1, "only unfinished work is resident after restore");
        assert_eq!(ledger.logical_id_for_message(&ids[0]), logical);
        let receipts = ledger.receipts_for_logical(&logical);
        assert_eq!(receipts.iter().map(|receipt| receipt.message_id.clone()).collect::<Vec<_>>(), ids);
        assert_eq!(receipts[0].status, ContractStatus::Completed);
        assert_eq!(receipts[1].status, ContractStatus::ExecutionFailed);
        assert_eq!(receipts[2].status, ContractStatus::Sent);
        assert_eq!(receipts[0].data.as_ref(), Some(&payload));
        assert_eq!(ledger.receipts_for_message(&ids[1])[0].data.as_ref(), Some(&payload));
        assert_eq!(ledger.get_record(&ids[0], "worker.run").unwrap().data, Some(payload.clone()));
        assert!(ledger.get_record(&ids[0], "other.run").is_none());
        assert!(matches!(ledger.advance_status("worker", &ids[0], "worker.run", ContractStatus::Started, None), ReceiptTransition::DuplicateOrStale));
        let (existing, created) = ledger.record_initial_target_state(&ids[0], 7, "sender", "worker.run", "Durable", ContractStatus::Sent, None);
        assert!(!created);
        assert_eq!(existing.status, ContractStatus::Completed);
        assert_eq!(existing.data.as_ref(), Some(&payload));
        assert_eq!(ledger.deliveries.read().len(), 1, "reading history must not populate a permanent result cache");
        assert!(matches!(ledger.advance_status("worker", &ids[2], "worker.run", ContractStatus::Completed, Some(json!({"ok":true}))), ReceiptTransition::Advanced { .. }));
        assert_eq!(ledger.persisted_count(), 3);
    }
}
