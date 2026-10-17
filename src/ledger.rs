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
/// - `Durable`: persisted to SQLite `kernel_ledger` table and retained in-memory.
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
                "SELECT message_id, logical_id, seq, source_domain, target, message_type, status, data, created_at, updated_at
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
                deliveries_map.insert(record.message_id.clone(), record);
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

        for (key, record) in deliveries.iter_mut() {
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
                    DeliveryPolicy::Ephemeral => ephemeral_removals.push(key.clone()),
                    DeliveryPolicy::Volatile => {}
                }
            }
        }

        for key in ephemeral_removals {
            deliveries.remove(&key);
        }
        drop(deliveries);

        for rec in durable_updates {
            self.persist_record(&rec);
        }

        notifications
    }
}
