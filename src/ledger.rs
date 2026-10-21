use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
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

/// The ledger is the sole owner of message facts. Durable records and session indexes
/// live in SQLite; a private in-memory database owns nonpersistent records and live aliases.
/// No historical result or child index is restored into Rust collections.
pub struct KernelLedger {
    next_id_seq: AtomicU64,
    instance_id: String,
    memory: Mutex<Connection>,
    pub(crate) db_conn: Option<Arc<Mutex<Connection>>>,
    gates: Mutex<HashMap<String, std::sync::Weak<Mutex<()>>>>,
}
const MAX_LIVE: i64 = 65_536;
const MAX_TRANSIENT_BYTES: i64 = 64 * 1024 * 1024;
const SELECT_RECORD: &str = "SELECT message_id,logical_id,seq,source_domain,target,message_type,status,data,created_at,updated_at FROM kernel_ledger";
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS kernel_ledger(
 message_id TEXT PRIMARY KEY,logical_id TEXT NOT NULL,seq INTEGER NOT NULL,source_domain TEXT NOT NULL,
 target TEXT NOT NULL,message_type TEXT NOT NULL,status TEXT NOT NULL,data TEXT,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
 CREATE INDEX IF NOT EXISTS idx_kernel_ledger_logical ON kernel_ledger(logical_id);
 CREATE INDEX IF NOT EXISTS idx_kernel_ledger_status ON kernel_ledger(status);
 CREATE TABLE IF NOT EXISTS kernel_sessions(source TEXT NOT NULL,seq INTEGER NOT NULL,logical_id TEXT NOT NULL,children TEXT NOT NULL,PRIMARY KEY(source,seq));";
impl Default for KernelLedger {
    fn default() -> Self {
        Self::new()
    }
}
impl KernelLedger {
    pub fn new() -> Self {
        Self::open_with_db(None).expect("cannot initialize kernel ledger")
    }
    pub fn open_with_db(path: Option<&Path>) -> Result<Self, StorageError> {
        let mut db = match path {
            Some(p) => {
                if let Some(parent) = p.parent().filter(|p| !p.as_os_str().is_empty()) {
                    std::fs::create_dir_all(parent)?;
                }
                Connection::open(p)?
            }
            None => Connection::open_in_memory()?,
        };
        db.execute_batch(
            "PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;PRAGMA busy_timeout=5000;",
        )?;
        db.execute_batch(SCHEMA)?;
        verify_sqlite_table_columns(&db, KERNEL_LEDGER_TABLE, KERNEL_LEDGER_COLUMNS)?;
        verify_sqlite_table_columns(
            &db,
            "kernel_sessions",
            &["source", "seq", "logical_id", "children"],
        )?;
        let bad:Option<String>=db.query_row("SELECT message_id FROM kernel_ledger WHERE (data IS NOT NULL AND NOT json_valid(data)) OR status NOT IN ('sent','received','started','completed','target_invalid','contract_invalid','execution_failed') LIMIT 1",[],|r|r.get(0)).optional()?;
        if let Some(id) = bad {
            return Err(StorageError::CorruptLedgerRecord(id));
        }
        crate::outbox::initialize(&db)?;
        // Use the same protocol decoder as live IPC; do not maintain a second JSON schema.
        {
            let mut statement =
                db.prepare("SELECT id,delivery_id,priority,frame FROM kernel_outbox ORDER BY id")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let id: i64 = row.get(0)?;
                let delivery: Option<String> = row.get(1)?;
                let priority: i64 = row.get(2)?;
                let raw: String = row.get(3)?;
                let valid = if raw.len() > 16 * 1024 * 1024 {
                    false
                } else {
                    serde_json::from_str::<crate::protocol::KernelToDomainFrame>(&raw).is_ok_and(
                        |frame| match frame {
                            crate::protocol::KernelToDomainFrame::Deliver(message) => {
                                priority == 1
                                    && delivery.as_deref() == Some(message.message_id.as_str())
                            }
                            crate::protocol::KernelToDomainFrame::Receipt(_) => {
                                priority == 0 && delivery.is_none()
                            }
                        },
                    )
                };
                if !valid {
                    return Err(StorageError::CorruptLedgerRecord(format!("outbox:{id}")));
                }
            }
        }
        // Every old process generation is disconnected. Recover facts, never blindly repeat effects.
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute("UPDATE kernel_ledger SET status='execution_failed',data=?1,updated_at=?2 WHERE status IN ('sent','received','started')",
            params![json!({"error":"kernel_restarted_with_inflight_delivery","execution_outcome":"unknown","reason":"previous process generation disconnected; no automatic replay"}).to_string(),now_millis()])?;
        tx.execute("DELETE FROM kernel_outbox", [])?;
        tx.execute("DELETE FROM kernel_sessions", [])?;
        tx.commit()?;
        let memory = Connection::open_in_memory()?;
        memory.execute_batch(SCHEMA)?;
        memory.execute_batch(
            "ALTER TABLE kernel_sessions ADD COLUMN durable INTEGER NOT NULL DEFAULT 0;",
        )?;
        memory.execute_batch("CREATE TABLE identities(message_id TEXT PRIMARY KEY,logical_id TEXT NOT NULL,source TEXT NOT NULL,seq INTEGER NOT NULL,origin INTEGER NOT NULL DEFAULT 0,channel TEXT,record TEXT);
            CREATE INDEX identities_logical ON identities(logical_id);
            CREATE TABLE highwater(source TEXT PRIMARY KEY,seq INTEGER NOT NULL);")?;
        Ok(Self {
            next_id_seq: AtomicU64::new(1),
            instance_id: uuid::Uuid::new_v4().simple().to_string(),
            memory: Mutex::new(memory),
            db_conn: Some(Arc::new(Mutex::new(db))),
            gates: Mutex::new(HashMap::new()),
        })
    }
    pub(crate) fn message_gate(&self, id: &str) -> Arc<Mutex<()>> {
        let mut gates = self.gates.lock();
        if let Some(gate) = gates.get(id).and_then(|w| w.upgrade()) {
            return gate;
        }
        if gates.len() >= 1024 {
            gates.retain(|_, w| w.strong_count() > 0);
        }
        let gate = Arc::new(Mutex::new(()));
        gates.insert(id.into(), Arc::downgrade(&gate));
        gate
    }
    fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeliveryRecord> {
        let status: String = row.get(6)?;
        let raw: Option<String> = row.get(7)?;
        let error = |e: serde_json::Error| {
            rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(e))
        };
        Ok(DeliveryRecord {
            message_id: row.get(0)?,
            logical_id: row.get(1)?,
            seq: row.get::<_, i64>(2)? as u64,
            source_domain: row.get(3)?,
            target: row.get(4)?,
            message_type: row.get(5)?,
            status: serde_json::from_value(Value::String(status)).map_err(error)?,
            data: raw
                .map(|s| serde_json::from_str(&s).map_err(error))
                .transpose()?,
            created_at: row.get(8)?,
            updated_at: row.get(9)?,
        })
    }
    fn read(db: &Connection, id: &str) -> rusqlite::Result<Option<DeliveryRecord>> {
        db.query_row(
            &format!("{SELECT_RECORD} WHERE message_id=?1"),
            [id],
            Self::row_to_record,
        )
        .optional()
    }
    fn write(db: &Connection, r: &DeliveryRecord) -> Result<(), StorageError> {
        db.execute("INSERT INTO kernel_ledger(message_id,logical_id,seq,source_domain,target,message_type,status,data,created_at,updated_at)
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(message_id) DO UPDATE SET status=excluded.status,data=excluded.data,updated_at=excluded.updated_at",
            params![r.message_id,r.logical_id,r.seq as i64,r.source_domain,r.target,r.message_type,serde_json::to_value(r.status)?.as_str(),r.data.as_ref().map(serde_json::to_string).transpose()?,r.created_at,r.updated_at])?;
        Ok(())
    }
    fn failure(r: &DeliveryRecord, error: impl ToString) -> DeliveryRecord {
        let mut failed = r.clone();
        failed.status = ContractStatus::ExecutionFailed;
        failed.updated_at = now_millis();
        failed.data = Some(
            json!({"error":"kernel_persistence_failed","reason":error.to_string().chars().take(2048).collect::<String>(),"persisted":false,"previous_status":r.status,"execution_outcome":"unknown"}),
        );
        failed
    }
    fn remember_failure(&self, r: &DeliveryRecord) {
        // Failure feedback never depends on the disk which failed. Space is reserved at ingress.
        Self::write(&self.memory.lock(), r).expect("kernel cannot retain reserved failure state");
    }
    pub fn generate_message_id(&self) -> String {
        format!(
            "msg_{}_{}_{:020}",
            now_millis(),
            self.instance_id,
            self.next_id_seq.fetch_add(1, Ordering::Relaxed)
        )
    }
    pub fn allocate_submission(
        &self,
        source: &str,
        seq: u64,
        count: usize,
    ) -> (String, Vec<String>, bool) {
        self.try_allocate_submission(source, seq, count, false)
            .expect("kernel submission resources exhausted")
    }
    pub(crate) fn try_allocate_submission(
        &self,
        source: &str,
        seq: u64,
        count: usize,
        durable: bool,
    ) -> Result<(String, Vec<String>, bool), StorageError> {
        let allocation_gate = self.message_gate(&format!("submission:{source}:{seq}"));
        let _allocation = allocation_gate.lock();
        let older = {
            let mem = self.memory.lock();
            if seq > 0 {
                if let Some(found) = Self::submission(&mem, source, seq)? {
                    return Ok((found.0, found.1, true));
                }
            }
            let high: i64 = mem.query_row(
                "SELECT coalesce((SELECT seq FROM highwater WHERE source=?1),0)",
                [source],
                |r| r.get(0),
            )?;
            seq > 0 && seq <= high as u64
        };
        if older {
            if let Some(found) =
                Self::submission(&self.db_conn.as_ref().unwrap().lock(), source, seq)?
            {
                return Ok((found.0, found.1, true));
            }
        }
        let logical = self.generate_message_id().replacen("msg_", "logical_", 1);
        let ids: Vec<_> = (0..count).map(|_| self.generate_message_id()).collect();
        {
            let mut mem = self.memory.lock();
            let used:i64=mem.query_row("SELECT (SELECT count(*) FROM identities)+(SELECT count(*) FROM kernel_sessions)+(SELECT count(*) FROM kernel_ledger WHERE status IN ('completed','execution_failed','contract_invalid','target_invalid'))",[],|r|r.get(0))?;
            if count > 1024 || used + count as i64 > MAX_LIVE {
                return Err(StorageError::ResourceExhausted(
                    "live message identities".into(),
                ));
            }
            let tx = mem.transaction()?;
            if seq > 0 {
                tx.execute(
                    "INSERT INTO kernel_sessions VALUES(?1,?2,?3,?4,?5)",
                    params![
                        source,
                        seq as i64,
                        logical,
                        serde_json::to_string(&ids)?,
                        durable
                    ],
                )?;
            }
            for id in &ids {
                tx.execute(
                    "INSERT INTO identities(message_id,logical_id,source,seq) VALUES(?1,?2,?3,?4)",
                    params![id, logical, source, seq as i64],
                )?;
            }
            tx.execute("INSERT INTO highwater VALUES(?1,?2) ON CONFLICT(source) DO UPDATE SET seq=max(seq,excluded.seq)",params![source,seq as i64])?;
            tx.commit()?;
        }
        Ok((logical, ids, false))
    }
    fn submission(
        db: &Connection,
        source: &str,
        seq: u64,
    ) -> Result<Option<(String, Vec<String>)>, StorageError> {
        let row: Option<(String, String)> = db
            .query_row(
                "SELECT logical_id,children FROM kernel_sessions WHERE source=?1 AND seq=?2",
                params![source, seq as i64],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        row.map(|(id, text)| Ok((id, serde_json::from_str(&text)?)))
            .transpose()
    }
    pub fn logical_id_for_message(&self, id: &str) -> String {
        let found: Option<String> = self
            .memory
            .lock()
            .query_row(
                "SELECT logical_id FROM identities WHERE message_id=?1",
                [id],
                |r| r.get(0),
            )
            .optional()
            .expect("memory identity query failed");
        found
            .or_else(|| {
                Self::read(&self.memory.lock(), id)
                    .expect("memory identity receipt")
                    .map(|r| r.logical_id)
            })
            .or_else(|| self.get_persisted_record(id).map(|r| r.logical_id))
            .unwrap_or_else(|| id.into())
    }
    pub(crate) fn bind_origin(&self, ids: &[String], instance: u64) {
        let mem = self.memory.lock();
        for id in ids {
            mem.execute(
                "UPDATE identities SET origin=?1 WHERE message_id=?2",
                params![instance as i64, id],
            )
            .expect("memory origin binding");
        }
    }
    pub(crate) fn source_instance(&self, id: &str) -> Option<u64> {
        self.memory
            .lock()
            .query_row(
                "SELECT origin FROM identities WHERE message_id=?1",
                [id],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .expect("memory origin lookup")
            .map(|v| v as u64)
    }
    fn metadata(&self, id: &str) -> Option<DeliveryRecord> {
        let raw: Option<String> = self
            .memory
            .lock()
            .query_row(
                "SELECT record FROM identities WHERE message_id=?1",
                [id],
                |r| r.get(0),
            )
            .optional()
            .expect("live metadata lookup")
            .flatten();
        raw.map(|text| serde_json::from_str(&text).expect("internal live record encoding"))
    }
    fn update_metadata(&self, r: &DeliveryRecord) {
        let mut meta = r.clone();
        meta.data = None;
        self.memory
            .lock()
            .execute(
                "UPDATE identities SET record=?1 WHERE message_id=?2",
                params![
                    serde_json::to_string(&meta).expect("live metadata encoding"),
                    r.message_id
                ],
            )
            .expect("live metadata update");
    }
    pub(crate) fn accepts_channel(&self, id: &str, channel: &str) -> bool {
        self.memory
            .lock()
            .query_row(
                "SELECT coalesce(channel=?1,0) FROM identities WHERE message_id=?2",
                params![channel, id],
                |r| r.get::<_, bool>(0),
            )
            .optional()
            .expect("live channel lookup")
            .unwrap_or(false)
    }
    pub(crate) fn finish_publication(&self, r: &MessageReceipt) {
        if !r.status.is_terminal() {
            return;
        }
        let policy = DeliveryPolicy::resolve(&r.target, &r.message_type);
        let mem = self.memory.lock();
        let abandoned:bool=mem.query_row("SELECT NOT EXISTS(SELECT 1 FROM identities i JOIN highwater h ON h.source=i.source WHERE i.message_id=?1)",[&r.message_id],|row|row.get(0)).expect("source lifetime query");
        if r.data.as_ref().is_some_and(|v| v["persisted"] == false) && !abandoned {
            return;
        }
        if abandoned {
            mem.execute(
                "DELETE FROM kernel_ledger WHERE message_id=?1 AND status='execution_failed' AND json_extract(data,'$.error')='kernel_persistence_failed'",
                [&r.message_id],
            )
            .expect("exited source result cleanup");
        }
        mem.execute(
            "DELETE FROM identities WHERE message_id=?1",
            [&r.message_id],
        )
        .expect("live context cleanup");
        mem.execute("DELETE FROM kernel_sessions WHERE logical_id=?1 AND (?2 OR durable=1) AND NOT EXISTS(SELECT 1 FROM identities WHERE logical_id=?1)",params![r.logical_id,policy==DeliveryPolicy::Ephemeral]).expect("finished session cleanup");
    }
    fn retire(&self, r: &DeliveryRecord, _policy: DeliveryPolicy) {
        // Router retains origin through terminal publication; standalone ledger users have no pipe.
        if r.status.is_terminal() && self.source_instance(&r.message_id).unwrap_or(0) == 0 {
            self.finish_publication(&r.to_receipt());
        }
    }
    pub fn clear_session_dedup_for_domain(&self, source: &str) {
        {
            let mem = self.memory.lock();
            mem.execute("DELETE FROM kernel_ledger WHERE status='execution_failed' AND json_extract(data,'$.error')='kernel_persistence_failed' AND message_id IN (SELECT value FROM kernel_sessions,json_each(children) WHERE source=?1)",[source]).expect("finished source results cleanup");
            mem.execute("DELETE FROM identities WHERE source=?1 AND record IS NOT NULL AND json_extract(record,'$.status') IN ('completed','execution_failed','contract_invalid','target_invalid')",[source]).expect("finished source context cleanup");
            mem.execute("DELETE FROM kernel_sessions WHERE source=?1", [source])
                .expect("memory session cleanup");
            mem.execute("DELETE FROM highwater WHERE source=?1", [source])
                .expect("memory session cleanup");
        }
        if let Err(e) = self
            .db_conn
            .as_ref()
            .unwrap()
            .lock()
            .execute("DELETE FROM kernel_sessions WHERE source=?1", [source])
        {
            eprintln!("kernel session cleanup failed; admission will remain explicit: {e}");
        }
    }
    pub fn retention_counts(&self) -> Value {
        let mem = self.memory.lock();
        let count = |sql: &str| {
            mem.query_row(sql, [], |r| r.get::<_, i64>(0))
                .expect("ledger diagnostic query")
        };
        let records = count("SELECT count(*) FROM kernel_ledger");
        let ids = count("SELECT count(*) FROM identities");
        let mut durable = 0;
        let mut volatile = 0;
        let mut ephemeral = 0;
        let mut statement = mem
            .prepare("SELECT target,message_type,status FROM kernel_ledger")
            .expect("resident policy query");
        for row in statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .expect("resident policies")
        {
            let (target, kind, status) = row.expect("resident policy metadata");
            match DeliveryPolicy::resolve(&target, &kind) {
                DeliveryPolicy::Durable => durable += 1,
                DeliveryPolicy::Volatile => volatile += 1,
                DeliveryPolicy::Ephemeral => {
                    if matches!(status.as_str(), "sent" | "received" | "started") {
                        ephemeral += 1;
                    }
                }
            }
        }
        json!({"resident_records":records,"record_capacity":records,"durable_records":durable,"volatile_records":volatile,"ephemeral_inflight":ephemeral,
            "submissions":count("SELECT count(*) FROM kernel_sessions"),"submission_capacity":MAX_LIVE,"logical_groups":count("SELECT count(DISTINCT logical_id) FROM identities"),"logical_capacity":ids,"child_links":ids,"child_capacity":ids})
    }
    pub fn record_initial_target_state(
        &self,
        id: &str,
        seq: u64,
        source: &str,
        target: &str,
        kind: &str,
        status: ContractStatus,
        data: Option<Value>,
    ) -> (DeliveryRecord, bool) {
        let gate = self.message_gate(id);
        let _mutation = gate.lock();
        self.record_initial_target_state_locked(id, seq, source, target, kind, status, data)
    }
    pub(crate) fn record_initial_target_state_locked(
        &self,
        id: &str,
        seq: u64,
        source: &str,
        target: &str,
        kind: &str,
        status: ContractStatus,
        data: Option<Value>,
    ) -> (DeliveryRecord, bool) {
        let now = now_millis();
        let r = DeliveryRecord {
            logical_id: self.logical_id_for_message(id),
            message_id: id.into(),
            seq,
            source_domain: source.into(),
            target: target.into(),
            message_type: kind.into(),
            status,
            data,
            created_at: now,
            updated_at: now,
        };
        self.record_initial(r, None)
    }
    pub(crate) fn record_delivery(
        &self,
        msg: &crate::protocol::KernelMessage,
        seq: u64,
        channel: &str,
    ) -> (DeliveryRecord, bool) {
        let now = now_millis();
        let r = DeliveryRecord {
            logical_id: msg.logical_id.clone(),
            message_id: msg.message_id.clone(),
            seq,
            source_domain: msg.source_domain.clone(),
            target: msg.target.clone(),
            message_type: msg.message_type.clone(),
            status: ContractStatus::Sent,
            data: None,
            created_at: now,
            updated_at: now,
        };
        self.memory
            .lock()
            .execute(
                "UPDATE identities SET channel=?1 WHERE message_id=?2",
                params![channel, msg.message_id],
            )
            .expect("target generation binding");
        self.record_initial(r, Some((channel, msg)))
    }
    fn record_initial(
        &self,
        r: DeliveryRecord,
        out: Option<(&str, &crate::protocol::KernelMessage)>,
    ) -> (DeliveryRecord, bool) {
        let policy = DeliveryPolicy::resolve(&r.target, &r.message_type);
        if self.source_instance(&r.message_id).is_none() && !r.status.is_terminal() {
            let mem = self.memory.lock();
            let used: i64 = mem
                .query_row("SELECT count(*) FROM identities", [], |row| row.get(0))
                .expect("live capacity");
            if used >= MAX_LIVE {
                return (Self::failure(&r, "live message capacity exhausted"), false);
            }
            mem.execute("INSERT OR IGNORE INTO identities(message_id,logical_id,source,seq) VALUES(?1,?2,?3,?4)",params![r.message_id,r.logical_id,r.source_domain,r.seq as i64]).expect("standalone live context");
        }
        if let Some(existing) =
            Self::read(&self.memory.lock(), &r.message_id).expect("memory ledger query")
        {
            return (existing, false);
        }
        if policy == DeliveryPolicy::Durable {
            let session: Option<(String, i64, String, String)> = {
                let mem = self.memory.lock();
                mem.query_row("SELECT source,seq,logical_id,children FROM kernel_sessions WHERE logical_id=?1 AND durable=1",[&r.logical_id],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).optional().expect("live submission lookup")
            };
            let result = (|| -> Result<Option<DeliveryRecord>, StorageError> {
                let mut db = self.db_conn.as_ref().unwrap().lock();
                let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                if let Some(existing) = Self::read(&tx, &r.message_id)? {
                    return Ok(Some(existing));
                }
                Self::write(&tx, &r)?;
                if let Some((source, seq, logical, children)) = &session {
                    tx.execute(
                        "INSERT OR IGNORE INTO kernel_sessions VALUES(?1,?2,?3,?4)",
                        params![source, seq, logical, children],
                    )?;
                }
                if let Some((channel, msg)) = out {
                    crate::outbox::insert_frame(
                        &tx,
                        channel,
                        &crate::protocol::KernelToDomainFrame::Deliver(msg.clone()),
                    )?;
                }
                tx.commit()?;
                Ok(None)
            })();
            match result {
                Ok(Some(old)) => return (old, false),
                Err(e) => {
                    let failed = Self::failure(&r, e);
                    self.remember_failure(&failed);
                    self.update_metadata(&failed);
                    return (failed, false);
                }
                Ok(None) => {}
            }
        } else if !(policy == DeliveryPolicy::Ephemeral && r.status.is_terminal()) {
            let mem = self.memory.lock();
            let bytes: i64 = mem
                .query_row(
                    "SELECT coalesce(sum(length(data)),0) FROM kernel_ledger",
                    [],
                    |r| r.get(0),
                )
                .expect("memory quota query");
            if bytes + r.data.as_ref().map_or(0, |v| v.to_string().len() as i64)
                > MAX_TRANSIENT_BYTES
            {
                let failed = Self::failure(&r, "nonpersistent ledger capacity exhausted");
                drop(mem);
                self.remember_failure(&failed);
                self.update_metadata(&failed);
                return (failed, false);
            }
            Self::write(&mem, &r).expect("cannot write in-memory ledger");
        }
        self.update_metadata(&r);
        self.retire(&r, policy);
        (r, true)
    }
    pub fn advance_status(
        &self,
        domain: &str,
        id: &str,
        target: &str,
        status: ContractStatus,
        data: Option<Value>,
    ) -> ReceiptTransition {
        let gate = self.message_gate(id);
        let _mutation = gate.lock();
        self.advance_status_locked(domain, id, target, status, data)
    }
    pub(crate) fn advance_status_locked(
        &self,
        domain: &str,
        id: &str,
        target: &str,
        status: ContractStatus,
        data: Option<Value>,
    ) -> ReceiptTransition {
        let kernel_alias =
            domain == "kernel" && (target == "web_ui" || target.starts_with("web_ui."));
        if !kernel_alias && parse_target_address(target).is_none_or(|(owner, _)| owner != domain) {
            return ReceiptTransition::UnauthorizedTarget;
        }
        let resident_terminal = Self::read(&self.memory.lock(), id)
            .expect("memory receipt query")
            .is_some_and(|r| r.status.is_terminal());
        let Some(old) = self.record_for_message(id) else {
            return ReceiptTransition::UnknownDelivery;
        };
        if old.target != target {
            return ReceiptTransition::UnauthorizedTarget;
        }
        if !resident_terminal
            && old.status == ContractStatus::ExecutionFailed
            && old.data.as_ref().is_some_and(|d| d["persisted"] == false)
        {
            return ReceiptTransition::Advanced {
                source_domain: old.source_domain.clone(),
                receipt: old.to_receipt(),
            };
        }
        if old.target != target {
            return ReceiptTransition::UnauthorizedTarget;
        }
        if old.status.is_terminal() || status.stage_rank() <= old.status.stage_rank() {
            return ReceiptTransition::DuplicateOrStale;
        }
        let mut r = old.clone();
        r.status = status;
        r.updated_at = now_millis();
        if data.is_some() {
            r.data = data;
        }
        self.commit_transition(&mut r, &old);
        ReceiptTransition::Advanced {
            source_domain: r.source_domain.clone(),
            receipt: r.to_receipt(),
        }
    }
    fn commit_transition(&self, r: &mut DeliveryRecord, old: &DeliveryRecord) {
        let policy = DeliveryPolicy::resolve(&r.target, &r.message_type);
        if policy == DeliveryPolicy::Durable {
            let result = (|| -> Result<(), StorageError> {
                let mut db = self.db_conn.as_ref().unwrap().lock();
                let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                Self::write(&tx, r)?;
                if r.status.is_terminal() {
                    tx.execute(
                        "DELETE FROM kernel_outbox WHERE delivery_id=?1",
                        [&r.message_id],
                    )?;
                }
                tx.commit()?;
                Ok(())
            })();
            if let Err(e) = result {
                *r = Self::failure(old, e);
                self.remember_failure(r);
            }
        } else {
            let mem = self.memory.lock();
            if policy == DeliveryPolicy::Ephemeral && r.status.is_terminal() {
                mem.execute(
                    "DELETE FROM kernel_ledger WHERE message_id=?1",
                    [&r.message_id],
                )
                .expect("transient record cleanup");
            } else {
                let used:i64=mem.query_row("SELECT coalesce(sum(length(data)),0) FROM kernel_ledger WHERE message_id<>?1",[&r.message_id],|row|row.get(0)).expect("memory data budget");
                if used + r.data.as_ref().map_or(0, |v| v.to_string().len() as i64)
                    > MAX_TRANSIENT_BYTES
                {
                    *r = Self::failure(old, "nonpersistent result capacity exhausted");
                }
                Self::write(&mem, r).expect("memory transition");
            }
        }
        self.update_metadata(r);
        self.retire(r, policy);
    }
    fn record_for_message(&self, id: &str) -> Option<DeliveryRecord> {
        let resident = { Self::read(&self.memory.lock(), id).expect("memory ledger query") };
        resident.or_else(|| self.get_persisted_record(id))
    }
    pub fn get_record(&self, id: &str, target: &str) -> Option<DeliveryRecord> {
        self.record_for_message(id).filter(|r| r.target == target)
    }
    pub fn get_persisted_record(&self, id: &str) -> Option<DeliveryRecord> {
        let result = Self::read(&self.db_conn.as_ref().unwrap().lock(), id);
        match result {
            Ok(record) => record,
            Err(error) => {
                let meta = self.metadata(id).unwrap_or_else(|| {
                    panic!("cannot read persistent historical delivery {id}: {error}")
                });
                let failed = Self::failure(&meta, error);
                self.remember_failure(&failed);
                Some(failed)
            }
        }
    }
    pub fn persisted_count(&self) -> usize {
        self.db_conn
            .as_ref()
            .unwrap()
            .lock()
            .query_row("SELECT count(*) FROM kernel_ledger", [], |r| {
                r.get::<_, i64>(0)
            })
            .expect("cannot count persistent ledger") as usize
    }
    pub fn receipts_for_message(&self, id: &str) -> Vec<MessageReceipt> {
        self.record_for_message(id)
            .map(|r| vec![r.to_receipt()])
            .unwrap_or_default()
    }
    pub fn receipts_for_logical(&self, logical: &str) -> Vec<MessageReceipt> {
        let ids: Vec<String> = {
            let mem = self.memory.lock();
            let mut stmt = mem
                .prepare("SELECT message_id FROM identities WHERE logical_id=?1 ORDER BY rowid")
                .expect("identity lookup");
            stmt.query_map([logical], |r| r.get(0))
                .expect("identity query")
                .collect::<Result<_, _>>()
                .expect("identity decode")
        };
        let mut records: Vec<_> = ids
            .iter()
            .filter_map(|id| self.record_for_message(id))
            .collect();
        for connection in [&self.memory, &**self.db_conn.as_ref().unwrap()] {
            let db = connection.lock();
            let mut stmt = db
                .prepare(&format!(
                    "{SELECT_RECORD} WHERE logical_id=?1 ORDER BY rowid"
                ))
                .expect("logical lookup");
            for item in stmt
                .query_map([logical], Self::row_to_record)
                .expect("logical query")
            {
                let r = item.expect("logical decode");
                if !records.iter().any(|old| old.message_id == r.message_id) {
                    records.push(r);
                }
            }
        }
        records.into_iter().map(|r| r.to_receipt()).collect()
    }
    pub fn fail_inflight_for_domain(
        &self,
        domain: &str,
        reason: &str,
    ) -> Vec<(String, MessageReceipt)> {
        self.fail_live(domain, None, reason)
    }
    pub(crate) fn fail_inflight_for_channel(
        &self,
        domain: &str,
        channel: &str,
        reason: &str,
    ) -> Vec<(String, MessageReceipt)> {
        self.fail_live(domain, Some(channel), reason)
    }
    fn fail_live(
        &self,
        domain: &str,
        channel: Option<&str>,
        reason: &str,
    ) -> Vec<(String, MessageReceipt)> {
        // Only bounded live metadata is needed to close a dead generation, even if disk is unavailable.
        let pending: Vec<String> = {
            let mem = self.memory.lock();
            let mut stmt=mem.prepare("SELECT message_id FROM identities WHERE record IS NOT NULL AND (?1 IS NULL OR channel=?1)").expect("live failure lookup");
            stmt.query_map([channel], |r| r.get(0))
                .expect("live failure query")
                .collect::<Result<_, _>>()
                .expect("live failure decode")
        };
        let mut notifications = Vec::new();
        for id in pending {
            let gate = self.message_gate(&id);
            let _guard = gate.lock();
            let Some(meta) = self.metadata(&id) else {
                continue;
            };
            if meta.status.is_terminal()
                || parse_target_address(&meta.target).is_none_or(|(d, _)| d != domain)
            {
                continue;
            }
            if let Some(old) = self.record_for_message(&id) {
                let mut failed = old.clone();
                if !old.status.is_terminal() {
                    failed.status = ContractStatus::ExecutionFailed;
                    failed.updated_at = now_millis();
                    failed.data = Some(
                        json!({"error":"target_domain_faulted","domain":domain,"reason":reason,"execution_outcome":"unknown"}),
                    );
                    self.commit_transition(&mut failed, &old);
                }
                notifications.push((failed.source_domain.clone(), failed.to_receipt()));
            }
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
            assert_eq!(
                ledger.allocate_submission("sender", seq, 2),
                (logical.clone(), ids.clone(), true)
            );
            for id in &ids {
                ledger.record_initial_target_state(
                    id,
                    seq,
                    "sender",
                    "worker.run",
                    "Ephemeral",
                    ContractStatus::Sent,
                    None,
                );
            }
            for (index, id) in ids.iter().enumerate() {
                let ReceiptTransition::Advanced { receipt, .. } = ledger.advance_status(
                    "worker",
                    id,
                    "worker.run",
                    ContractStatus::Completed,
                    Some(json!({"ok":true})),
                ) else {
                    panic!("completion rejected")
                };
                assert_eq!(receipt.logical_id, logical);
                assert_eq!(receipt.message_id, *id);
                if index == 0 {
                    assert_eq!(
                        ledger.allocate_submission("sender", seq, 2),
                        (logical.clone(), ids.clone(), true),
                        "a completed sibling must not retire an in-flight submission"
                    );
                }
            }
            assert_eq!(
                ledger.retention_counts()["resident_records"]
                    .as_u64()
                    .unwrap(),
                0
            );
            assert_eq!(
                ledger.retention_counts()["submissions"].as_u64().unwrap(),
                0,
                "terminal Ephemeral submissions leaked dedup metadata"
            );
            assert_eq!(
                ledger.retention_counts()["child_links"].as_u64().unwrap(),
                0
            );
            assert_eq!(
                ledger.retention_counts()["logical_groups"]
                    .as_u64()
                    .unwrap(),
                0
            );
        }
        let (_, ids, duplicate) = ledger.allocate_submission("sender", 100, 1);
        assert!(
            !duplicate,
            "completed Ephemeral sequence no longer reserves session dedup state"
        );
        ledger.record_initial_target_state(
            &ids[0],
            100,
            "sender",
            "worker.missing",
            "Ephemeral",
            ContractStatus::TargetInvalid,
            None,
        );
        assert!(
            ledger.retention_counts()["submissions"] == 0,
            "invalid targets also retire transient identities"
        );
        let (logical, ids, _) = ledger.allocate_submission("sender", 101, 1);
        ledger.record_initial_target_state(
            &ids[0],
            101,
            "sender",
            "worker.run",
            "Ephemeral",
            ContractStatus::Sent,
            None,
        );
        let failures = ledger.fail_inflight_for_domain("worker", "crash");
        assert_eq!(failures[0].1.logical_id, logical);
        assert!(ledger.retention_counts()["submissions"] == 0);
        assert!(ledger.retention_counts()["child_links"] == 0);
        assert!(ledger.retention_counts()["logical_groups"] == 0);
    }

    #[test]
    fn mixed_and_non_ephemeral_submissions_keep_original_identity_and_replay() {
        let ledger = KernelLedger::new();
        let (logical, ids, _) = ledger.allocate_submission("sender", 1, 2);
        for (id, target) in ids.iter().zip(["kernel.storage", "worker.run"]) {
            ledger.record_initial_target_state(
                id,
                1,
                "sender",
                target,
                "StorageQuery",
                ContractStatus::Sent,
                None,
            );
        }
        ledger.advance_status(
            "kernel",
            &ids[0],
            "kernel.storage",
            ContractStatus::Completed,
            Some(json!({"items":[]})),
        );
        ledger.advance_status(
            "worker",
            &ids[1],
            "worker.run",
            ContractStatus::Completed,
            Some(json!({"ok":true})),
        );
        assert_eq!(
            ledger.allocate_submission("sender", 1, 2),
            (logical.clone(), ids.clone(), true)
        );
        let replay = ledger.receipts_for_logical(&logical);
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].message_id, ids[1]);
        assert_eq!(replay[0].logical_id, logical);
        assert_eq!(replay[0].data, Some(json!({"ok":true})));
        let (logical, ids, _) = ledger.allocate_submission("sender", 2, 1);
        ledger.record_initial_target_state(
            &ids[0],
            2,
            "sender",
            "worker.run",
            "Volatile",
            ContractStatus::Sent,
            None,
        );
        ledger.advance_status(
            "worker",
            &ids[0],
            "worker.run",
            ContractStatus::Completed,
            Some(json!({"ok":true})),
        );
        assert_eq!(
            ledger.allocate_submission("sender", 2, 1),
            (logical.clone(), ids.clone(), true)
        );
        assert_eq!(
            ledger.receipts_for_logical(&logical)[0].status,
            ContractStatus::Completed
        );
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
                ledger.record_initial_target_state(
                    id,
                    7,
                    "sender",
                    "worker.run",
                    "Durable",
                    ContractStatus::Sent,
                    None,
                );
            }
            for (index, status) in [ContractStatus::Completed, ContractStatus::ExecutionFailed]
                .into_iter()
                .enumerate()
            {
                for stage in [ContractStatus::Received, ContractStatus::Started, status] {
                    ledger.advance_status(
                        "worker",
                        &ids[index],
                        "worker.run",
                        stage,
                        Some(payload.clone()),
                    );
                }
            }
            assert_eq!(ledger.persisted_count(), 3);
            (logical, ids)
        };
        let ledger = KernelLedger::open_with_db(Some(&path)).unwrap();
        assert_eq!(
            ledger.retention_counts()["resident_records"]
                .as_u64()
                .unwrap(),
            0,
            "old inflight work is failed in storage without restoring payloads"
        );
        assert_eq!(ledger.logical_id_for_message(&ids[0]), logical);
        let receipts = ledger.receipts_for_logical(&logical);
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.message_id.clone())
                .collect::<Vec<_>>(),
            ids
        );
        assert_eq!(receipts[0].status, ContractStatus::Completed);
        assert_eq!(receipts[1].status, ContractStatus::ExecutionFailed);
        assert_eq!(receipts[2].status, ContractStatus::ExecutionFailed);
        assert_eq!(
            receipts[2].data.as_ref().unwrap()["execution_outcome"],
            "unknown"
        );
        assert_eq!(receipts[0].data.as_ref(), Some(&payload));
        assert_eq!(
            ledger.receipts_for_message(&ids[1])[0].data.as_ref(),
            Some(&payload)
        );
        assert_eq!(
            ledger.get_record(&ids[0], "worker.run").unwrap().data,
            Some(payload.clone())
        );
        assert!(ledger.get_record(&ids[0], "other.run").is_none());
        assert!(matches!(
            ledger.advance_status(
                "worker",
                &ids[0],
                "worker.run",
                ContractStatus::Started,
                None
            ),
            ReceiptTransition::DuplicateOrStale
        ));
        let (existing, created) = ledger.record_initial_target_state(
            &ids[0],
            7,
            "sender",
            "worker.run",
            "Durable",
            ContractStatus::Sent,
            None,
        );
        assert!(!created);
        assert_eq!(existing.status, ContractStatus::Completed);
        assert_eq!(existing.data.as_ref(), Some(&payload));
        assert_eq!(
            ledger.retention_counts()["resident_records"]
                .as_u64()
                .unwrap(),
            0,
            "reading history must not populate a permanent result cache"
        );
        assert!(matches!(
            ledger.advance_status(
                "worker",
                &ids[2],
                "worker.run",
                ContractStatus::Completed,
                Some(json!({"ok":true}))
            ),
            ReceiptTransition::DuplicateOrStale
        ));
        assert_eq!(ledger.persisted_count(), 3);
    }
}
#[cfg(test)]
mod storage_exhaustion_acceptance {
    use super::*;
    #[test]
    fn real_sqlite_full_rejects_entire_admission_without_delivery_or_partial_ledger() {
        let k = crate::AtelierKernel::new_in_memory().unwrap();
        let sender = k.attach_domain("sender");
        let worker = k.attach_domain("worker");
        worker.register_handlers(&["run"]);
        worker.drain_frames();
        {
            let db = k.ledger().db_conn.as_ref().unwrap().lock();
            let pages: i64 = db
                .query_row("PRAGMA page_count", [], |row| row.get(0))
                .unwrap();
            db.execute_batch(&format!("PRAGMA max_page_count={pages}"))
                .unwrap();
        }
        let id = sender.send_one(
            vec!["worker.run".into()],
            "Durable",
            serde_json::json!({}),
            serde_json::json!({"large":"x".repeat(1024*1024)}),
        );
        assert!(worker.drain_frames().is_empty());
        assert!(k.ledger().get_persisted_record(&id).is_none());
        assert!(sender.drain_frames().iter().any(|f|matches!(f,crate::KernelToDomainFrame::Receipt(r) if r.message_id==id && r.status==ContractStatus::ExecutionFailed && r.data.as_ref().unwrap()["persisted"]==false)));
    }
    #[test]
    fn corrupted_live_result_fails_explicitly_without_overwriting_unsafe_disk_fact() {
        let k = crate::AtelierKernel::new_in_memory().unwrap();
        let sender = k.attach_domain("sender");
        let worker = k.attach_domain("worker");
        worker.register_handlers(&["run"]);
        worker.drain_frames();
        let id = sender.send_one(
            vec!["worker.run".into()],
            "Durable",
            serde_json::json!({}),
            serde_json::json!({"original":"input"}),
        );
        worker.drain_frames();
        sender.drain_frames();
        k.ledger()
            .db_conn
            .as_ref()
            .unwrap()
            .lock()
            .execute(
                "UPDATE kernel_ledger SET data='{broken' WHERE message_id=?1",
                [&id],
            )
            .unwrap();
        worker.report_receipt(
            &id,
            "worker.run",
            ContractStatus::Completed,
            Some(serde_json::json!({"should_not_be_success":true})),
        );
        let frames = sender.drain_frames();
        assert!(frames.iter().any(|f|matches!(f,crate::KernelToDomainFrame::Receipt(r) if r.message_id==id && r.status==ContractStatus::ExecutionFailed && r.data.as_ref().unwrap()["persisted"]==false)));
        assert!(!frames.iter().any(|f|matches!(f,crate::KernelToDomainFrame::Receipt(r) if r.status==ContractStatus::Completed)));
        let raw: String = k
            .ledger()
            .db_conn
            .as_ref()
            .unwrap()
            .lock()
            .query_row(
                "SELECT data FROM kernel_ledger WHERE message_id=?1",
                [&id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(raw, "{broken");
    }
}

#[cfg(test)]
mod receipt_channel_acceptance {
    use super::*;
    #[test]
    fn domain_receipt_cannot_claim_internal_delivery_without_target_channel() {
        let ledger = KernelLedger::new();
        let (_, ids, _) = ledger.allocate_submission("sender", 1, 1);
        ledger.record_initial_target_state(
            &ids[0],
            1,
            "sender",
            "kernel.supervisor",
            "RestartDomain",
            ContractStatus::Sent,
            None,
        );
        assert!(!ledger.accepts_channel(&ids[0], "unrelated-domain-pipe"));
    }
}
