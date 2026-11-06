//! 账本 —— 三表一入口（messages / audit + 内存 submissions 去重），由 `nucleus` 派生。
//!
//! 旧"四表六出口"（`kernel_ledger` / `identities` / `kernel_sessions` / `highwater`）已删除。
//! 现在只有核（`Nucleus`）持有消息权威状态；本模块是核的一个门面（facade）：
//!   - 内存活跃 + 内存审计 + 提交去重（SubmissionCache）全部在 `Nucleus` 内；
//!   - Durable 子集镜像到 SQLite `messages`（活）与 `audit`（终态，只增）；
//!   - 代际绑定（origin / channel / persistence_failed）是核外的路由推导，不参与状态机。
//!
//! 对外方法签名不变（见 docs/ledger-core-rewrite.md §1 的 29 个能力），语义按宪法收敛。

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::core::nucleus::{Allocation, AuditRecord, Message, Nucleus};
use crate::core::protocol::{
    ContractStatus, DeliveryPolicy, KernelMessage, MessageReceipt, now_millis, parse_target_address,
};
use crate::services::storage::{StorageError, verify_sqlite_table_columns};

/// 权威活消息表（唯一表名，M7 检查用）。
pub const KERNEL_LEDGER_TABLE: &str = "messages";
pub const KERNEL_LEDGER_COLUMNS: &[&str] = &[
    "message_id",
    "logical_id",
    "seq",
    "source_domain",
    "target",
    "message_type",
    "policy",
    "status",
    "data",
    "created_at",
    "updated_at",
];
/// Durable 终态审计表（只增）。
pub const AUDIT_TABLE: &str = "audit";
pub const AUDIT_COLUMNS: &[&str] = &[
    "id",
    "message_id",
    "logical_id",
    "seq",
    "source_domain",
    "target",
    "message_type",
    "policy",
    "status",
    "data",
    "created_at",
];

const MAX_LIVE: usize = 65_536;
const MAX_CHILDREN_PER_SUBMISSION: usize = 1_024;

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS messages(
  message_id TEXT PRIMARY KEY,logical_id TEXT NOT NULL,seq INTEGER NOT NULL,source_domain TEXT NOT NULL,
  target TEXT NOT NULL,message_type TEXT NOT NULL,policy TEXT NOT NULL DEFAULT 'durable',status TEXT NOT NULL,
  data TEXT,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
  CREATE INDEX IF NOT EXISTS idx_messages_logical ON messages(logical_id);
  CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status);
  CREATE TABLE IF NOT EXISTS audit(
    id INTEGER PRIMARY KEY AUTOINCREMENT,message_id TEXT NOT NULL,logical_id TEXT NOT NULL,seq INTEGER NOT NULL,
    source_domain TEXT NOT NULL,target TEXT NOT NULL,message_type TEXT NOT NULL,policy TEXT NOT NULL,
    status TEXT NOT NULL,data TEXT,created_at INTEGER NOT NULL);
  CREATE INDEX IF NOT EXISTS idx_audit_message ON audit(message_id);
  CREATE INDEX IF NOT EXISTS idx_audit_logical ON audit(logical_id);";

fn policy_str(p: DeliveryPolicy) -> &'static str {
    match p {
        DeliveryPolicy::Durable => "durable",
        DeliveryPolicy::Volatile => "volatile",
        DeliveryPolicy::Ephemeral => "ephemeral",
    }
}

fn status_str(s: ContractStatus) -> &'static str {
    match s {
        ContractStatus::Sent => "sent",
        ContractStatus::Received => "received",
        ContractStatus::Started => "started",
        ContractStatus::Completed => "completed",
        ContractStatus::TargetInvalid => "target_invalid",
        ContractStatus::ContractInvalid => "contract_invalid",
        ContractStatus::ExecutionFailed => "execution_failed",
    }
}

fn parse_policy(raw: &str) -> Result<DeliveryPolicy, StorageError> {
    serde_json::from_value(Value::String(raw.to_string()))
        .map_err(|e| StorageError::CorruptLedgerRecord(e.to_string()))
}

fn parse_status(raw: &str) -> Result<ContractStatus, StorageError> {
    serde_json::from_value(Value::String(raw.to_string()))
        .map_err(|e| StorageError::CorruptLedgerRecord(e.to_string()))
}

/// Authoritative per-target delivery record (the facade's public row view).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryRecord {
    pub logical_id: String,
    pub message_id: String,
    pub seq: u64,
    pub source_domain: String,
    pub target: String,
    pub message_type: String,
    #[serde(default)]
    pub policy: DeliveryPolicy,
    pub status: ContractStatus,
    pub data: Option<Value>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl DeliveryRecord {
    pub fn to_receipt(&self) -> MessageReceipt {
        MessageReceipt {
            logical_id: self.logical_id.clone(),
            message_id: self.message_id.clone(),
            seq: self.seq,
            target: self.target.clone(),
            message_type: self.message_type.clone(),
            policy: self.policy,
            status: self.status,
            data: self.data.clone(),
            timestamp: self.updated_at,
        }
    }
}

/// Outcome of advancing a `message_id` record via an inbound receipt.
#[derive(Debug, Clone)]
pub enum ReceiptTransition {
    Advanced {
        source_domain: String,
        receipt: MessageReceipt,
    },
    DuplicateOrStale,
    UnknownDelivery,
    UnauthorizedTarget,
}

/// 代际绑定（路由推导，非状态机的一部分）：origin=进程代际，channel=目标管道。
#[derive(Default, Clone)]
struct Binding {
    logical_id: String,
    origin: u64,
    channel: Option<String>,
    persistence_failed: bool,
    /// 该 id 已提交终态。写入时标记，代际退役据此清理，不需要回查历史。
    terminal: bool,
}

pub struct KernelLedger {
    next_id_seq: AtomicU64,
    instance_id: String,
    nucleus: Mutex<Nucleus>,
    bindings: Mutex<HashMap<String, Binding>>,
    pub(crate) db_conn: Option<Arc<Mutex<Connection>>>,
    gates: Mutex<HashMap<String, std::sync::Weak<Mutex<()>>>>,
    evicted: AtomicU64,
    _owner: Option<std::fs::File>,
}

impl Default for KernelLedger {
    fn default() -> Self {
        Self::new()
    }
}

fn record_from_message(m: &Message) -> DeliveryRecord {
    DeliveryRecord {
        logical_id: m.logical_id.clone(),
        message_id: m.message_id.clone(),
        seq: m.seq,
        source_domain: m.source.clone(),
        target: m.target.clone(),
        message_type: m.message_type.clone(),
        policy: m.policy,
        status: m.status,
        data: m.data.clone(),
        created_at: m.created_at,
        updated_at: m.updated_at,
    }
}

fn record_from_audit(a: &AuditRecord) -> DeliveryRecord {
    DeliveryRecord {
        logical_id: a.logical_id.clone(),
        message_id: a.message_id.clone(),
        seq: a.seq,
        source_domain: a.source.clone(),
        target: a.target.clone(),
        message_type: a.message_type.clone(),
        policy: a.policy,
        status: a.status,
        data: a.data.clone(),
        created_at: a.created_at,
        updated_at: a.created_at,
    }
}

impl KernelLedger {
    pub fn new() -> Self {
        Self::open_with_db(None).expect("cannot initialize kernel ledger")
    }

    pub fn open_with_db(path: Option<&Path>) -> Result<Self, StorageError> {
        let prepared = path
            .map(|path| -> Result<_, StorageError> {
                if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(path)?;
                let canonical = path.canonicalize()?;
                // SQLite WAL 数据库必须是单一文件系统名；硬链接别名有不同 WAL/lock 名，拒绝。
                #[cfg(unix)]
                let metadata = std::fs::metadata(&canonical)?;
                #[cfg(unix)]
                let links = { use std::os::unix::fs::MetadataExt; metadata.nlink() };
                #[cfg(windows)]
                let links = {
                    use std::os::windows::io::AsRawHandle;
                    #[repr(C)]
                    #[derive(Default)]
                    struct FileInformation {
                        attributes: u32, creation: [u32; 2], access: [u32; 2], write: [u32; 2],
                        volume: u32, size_high: u32, size_low: u32, links: u32, index_high: u32, index_low: u32,
                    }
                    #[link(name = "kernel32")]
                    unsafe extern "system" {
                        fn GetFileInformationByHandle(handle: *mut std::ffi::c_void, info: *mut FileInformation) -> i32;
                    }
                    let file = std::fs::File::open(&canonical)?;
                    let mut info = FileInformation::default();
                    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
                        return Err(std::io::Error::last_os_error().into());
                    }
                    info.links as u64
                };
                #[cfg(not(any(unix, windows)))]
                let links = 1;
                if links > 1 {
                    return Err(StorageError::Io(std::io::Error::other(
                        "hard-linked SQLite database is unsafe; use its canonical path or a symbolic link",
                    )));
                }
                let mut lock_path = canonical.as_os_str().to_os_string();
                lock_path.push(".kernel-owner.lock");
                let owner = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(std::path::PathBuf::from(lock_path))?;
                owner.try_lock().map_err(|error| {
                    StorageError::Io(std::io::Error::other(format!(
                        "cannot acquire exclusive kernel database owner for {}: {error}",
                        canonical.display()
                    )))
                })?;
                Ok((canonical, owner))
            })
            .transpose()?;

        let mut db = match prepared.as_ref().map(|(path, _)| path) {
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
        verify_sqlite_table_columns(&db, AUDIT_TABLE, AUDIT_COLUMNS)?;
        crate::services::outbox::initialize(&db)?;

        // 损坏的持久传输在启动清理前显式拒绝（沿用协议解码器，不维护第二套 JSON schema）。
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
                    serde_json::from_str::<crate::core::protocol::KernelToDomainFrame>(&raw).is_ok_and(
                        |frame| match frame {
                            crate::core::protocol::KernelToDomainFrame::Deliver(message) => {
                                priority == 1
                                    && delivery.as_deref() == Some(message.message_id.as_str())
                            }
                            crate::core::protocol::KernelToDomainFrame::Receipt(_) => {
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

        // 损坏显式拒绝（C3）：非法状态或 JSON 直接报错，不回退为 Sent。
        // 只查 `messages`（在途行，界由 MAX_LIVE 控制，启动本来就要逐行读它）。
        // 历史 `audit` 的坏行校验**不在启动路径上**：那会把每次启动变成一次全历史遍历，
        // 与 C3:54「历史按需读取」冲突；改由 `--check` 调 `verify_row_integrity` 负责。
        if let Some(id) = live_row_violation(&db)? {
            return Err(StorageError::CorruptLedgerRecord(id));
        }

        // 启动恢复（旧出口 5 坍缩为一个事务）：活消息按终态/非终态收敛进 audit，messages 清空。
        // "留日志不留状态"——历史终态结果保留于 audit，非终态明确终结为 ExecutionFailed。
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        {
            let mut stmt = tx.prepare(
                "SELECT message_id,logical_id,seq,source_domain,target,message_type,policy,status,data,created_at,updated_at FROM messages",
            )?;
            let rows = stmt
                .query_map([], messages_row_to_record)?
                .collect::<Result<Vec<_>, _>>()?;
            for mut rec in rows {
                if !rec.status.is_terminal() {
                    rec.status = ContractStatus::ExecutionFailed;
                    rec.updated_at = now_millis();
                    rec.data = Some(json!({
                        "error": "kernel_restarted_with_inflight_delivery",
                        "execution_outcome": "unknown",
                        "reason": "previous process generation disconnected; no automatic replay"
                    }));
                }
                tx.execute("DELETE FROM messages WHERE message_id=?1", [&rec.message_id])?;
                insert_audit_tx(&tx, &rec)?;
            }
        }
        tx.commit()?;

        // 宪法 C3:54：历史身份及结果直接查 SQLite，启动不建立全量内存索引，
        // 也不把全部历史结果一次性展开到内存。核从空状态开始，终态历史按需点查。
        let nucleus = Nucleus::new();

        Ok(Self {
            next_id_seq: AtomicU64::new(1),
            instance_id: uuid::Uuid::new_v4().simple().to_string(),
            nucleus: Mutex::new(nucleus),
            bindings: Mutex::new(HashMap::new()),
            db_conn: Some(Arc::new(Mutex::new(db))),
            gates: Mutex::new(HashMap::new()),
            evicted: AtomicU64::new(0),
            _owner: prepared.map(|(_, owner)| owner),
        })
    }

    pub(crate) fn database_path(&self) -> Option<std::path::PathBuf> {
        self.db_conn
            .as_ref()
            .unwrap()
            .lock()
            .path()
            .map(std::path::PathBuf::from)
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
        self.try_allocate_submission(source, seq, count, true)
            .expect("kernel submission resources exhausted")
    }

    /// `dedup_required` 为真时启用高水位过期判定（Durable/Volatile 及混合提交）；
    /// 纯 Ephemeral 提交不启用：宪法 C3:56 允许同序号重放作为新的瞬态投递。
    pub(crate) fn try_allocate_submission(
        &self,
        source: &str,
        seq: u64,
        count: usize,
        dedup_required: bool,
    ) -> Result<(String, Vec<String>, bool), StorageError> {
        if count > MAX_CHILDREN_PER_SUBMISSION {
            return Err(StorageError::ResourceExhausted(
                "live message identities".into(),
            ));
        }
        // 全局唯一 id 由 facade 生成（含 UUID + 时间 + 单调序号），核只做去重窗口。
        let logical = self.generate_message_id().replacen("msg_", "logical_", 1);
        let ids: Vec<String> = (0..count).map(|_| self.generate_message_id()).collect();
        let mut n = self.nucleus.lock();
        if n.retention().0.saturating_add(count) > MAX_LIVE {
            return Err(StorageError::ResourceExhausted(
                "live message identities".into(),
            ));
        }
        match n.allocate_submission_with_ids(source, seq, logical, ids, dedup_required) {
            Allocation::New { logical_id, ids } => Ok((logical_id, ids, false)),
            Allocation::Replay { logical_id, ids } => Ok((logical_id, ids, true)),
            // 滑出窗口的过期重放：显式拒绝，绝不重新分发身份。
            Allocation::Stale { high_water } => Err(StorageError::StaleSubmission {
                key: source.to_string(),
                seq,
                high: high_water,
            }),
        }
    }

    pub(crate) fn bind_origin(&self, ids: &[String], instance: u64) {
        let mut b = self.bindings.lock();
        for id in ids {
            b.entry(id.clone()).or_default().origin = instance;
        }
    }

    pub(crate) fn source_instance(&self, id: &str) -> Option<u64> {
        self.bindings.lock().get(id).map(|b| b.origin)
    }

    pub(crate) fn accepts_channel(&self, id: &str, channel: &str) -> bool {
        self.bindings
            .lock()
            .get(id)
            .is_some_and(|b| b.channel.as_deref() == Some(channel))
    }

    pub(crate) fn has_unpersisted_failure(&self, id: &str) -> bool {
        self.bindings
            .lock()
            .get(id)
            .is_some_and(|b| b.persistence_failed)
    }

    pub(crate) fn finish_publication(&self, r: &MessageReceipt) {
        if !r.status.is_terminal() {
            return;
        }
        // 未持久化失败的活行身份保留：源仍活着时，后续回执需要它重放失败结果；
        // 由 clear_session_dedup_for_domain 在代际退役时清理。
        if self.has_unpersisted_failure(&r.message_id) {
            return;
        }
        // 回执已发布：活行身份随终态回收，Durable 结果留在 audit，不依赖任何已删除行。
        self.bindings.lock().remove(&r.message_id);
    }

    pub fn clear_session_dedup_for_domain(&self, source: &str) {
        self.nucleus.lock().retire_dedup(source);
        let generation = source
            .rsplit_once('#')
            .and_then(|(_, g)| g.parse::<u64>().ok());
        let Some(g) = generation else {
            return;
        };
        // 只清理"未持久化失败"或"已终态"的绑定；在途/孤儿绑定保留，杜绝旧回执误入新代际。
        let snapshot: Vec<(String, Binding)> = {
            let b = self.bindings.lock();
            b.iter()
                .filter(|(_, bind)| bind.origin == g)
                .map(|(id, bind)| (id.clone(), bind.clone()))
                .collect()
        };
        let mut to_remove = HashSet::new();
        for (id, bind) in snapshot {
            // 终态标记在提交时写好，退役时零回查。
            if bind.persistence_failed || bind.terminal {
                to_remove.insert(id);
            }
        }
        self.bindings.lock().retain(|id, _| !to_remove.contains(id));
    }

    pub fn retention_counts(&self) -> Value {
        let n = self.nucleus.lock();
        let (active, submissions) = n.retention();
        let mut durable = 0usize;
        let mut volatile = 0usize;
        let mut ephemeral = 0usize;
        let mut logical_groups = HashSet::new();
        for m in n.active_snapshot() {
            logical_groups.insert(m.logical_id.clone());
            match m.policy {
                DeliveryPolicy::Durable => durable += 1,
                DeliveryPolicy::Volatile => volatile += 1,
                DeliveryPolicy::Ephemeral => {
                    if !m.status.is_terminal() {
                        ephemeral += 1;
                    }
                }
            }
        }
        let child_links = self.bindings.lock().len();
        json!({
            "resident_records": active,
            "record_capacity": active,
            "durable_records": durable,
            "volatile_records": volatile,
            "ephemeral_inflight": ephemeral,
            "nonpersistent_evicted": self.evicted.load(Ordering::Relaxed),
            "submissions": submissions,
            "submission_capacity": MAX_LIVE,
            "logical_groups": logical_groups.len(),
            "logical_capacity": child_links,
            "child_links": child_links,
            "child_capacity": child_links,
        })
    }

    pub fn record_initial_target_state(
        &self,
        id: &str,
        logical_id: &str,
        seq: u64,
        source: &str,
        target: &str,
        kind: &str,
        status: ContractStatus,
        data: Option<Value>,
    ) -> (DeliveryRecord, bool) {
        let gate = self.message_gate(id);
        let _mutation = gate.lock();
        let policy = DeliveryPolicy::resolve(target, kind);
        self.record_initial_target_state_locked(
            id, logical_id, seq, source, target, kind, policy, status, data,
        )
    }

    pub(crate) fn record_initial_target_state_locked(
        &self,
        id: &str,
        logical_id: &str,
        seq: u64,
        source: &str,
        target: &str,
        kind: &str,
        policy: DeliveryPolicy,
        status: ContractStatus,
        data: Option<Value>,
    ) -> (DeliveryRecord, bool) {
        // 同 `record_delivery`：新建态只可能是新 id，因此用 O(1) 的 active 守卫，
        // 不扫描审计历史（这条路径上 kernel.registry/kernel.storage/kernel.logger
        // 等内核内部目标每个消息都会走到，扫历史会直接拖垮日志与存储吞吐）。
        if let Some(existing) = self.nucleus.lock().get(id).cloned() {
            return (record_from_message(&existing), false);
        }
        let now = now_millis();
        let r = DeliveryRecord {
            logical_id: logical_id.to_string(),
            message_id: id.into(),
            seq,
            source_domain: source.into(),
            target: target.into(),
            message_type: kind.into(),
            policy,
            status,
            data: data.clone(),
            created_at: now,
            updated_at: now,
        };
        {
            let mut b = self.bindings.lock();
            let binding = b.entry(id.to_string()).or_default();
            binding.logical_id = r.logical_id.clone();
            // 入口即终态时在这里就打上标记，退役清理不必回查。
            binding.terminal = r.status.is_terminal();
        }

        if r.status.is_terminal() {
            // 入口即终态（TargetInvalid / ContractInvalid）：不建活跃行，只在盘上留历史。
            if r.policy == DeliveryPolicy::Durable {
                if let Err(e) = self.append_audit_disk(&r) {
                    self.set_persistence_failed(id);
                    return (Self::failure(&r, e), false);
                }
            }
            if r.policy == DeliveryPolicy::Ephemeral {
                // 这条路径不经过 `Nucleus::advance`，必须在这里补一次瞬态身份回收，
                // 否则已完成（入口即终态）的 Ephemeral 会一直占着去重窗口，
                // 让同 seq 重提拿回旧失败回执而不是新瞬态投递（C3:56）。
                self.nucleus.lock().reclaim_ephemeral(&r.logical_id);
            }
            self.maybe_retire_binding(id);
            return (r, true);
        }

        self.nucleus.lock().put(
            id.to_string(),
            r.logical_id.clone(),
            source.to_string(),
            seq,
            target.to_string(),
            kind.to_string(),
            policy,
            status,
            data,
        );
        if r.policy == DeliveryPolicy::Durable {
            if let Err(e) = self.persist_live(&r) {
                return (self.mark_unpersisted_failure(&r, e), false);
            }
        }
        (r, true)
    }

    pub(crate) fn record_delivery(
        &self,
        msg: &KernelMessage,
        seq: u64,
        channel: &str,
        channel_seq: u64,
    ) -> (DeliveryRecord, bool) {
        let now = now_millis();
        let r = DeliveryRecord {
            logical_id: msg.logical_id.clone(),
            message_id: msg.message_id.clone(),
            seq,
            source_domain: msg.source_domain.clone(),
            target: msg.target.clone(),
            message_type: msg.message_type.clone(),
            policy: msg.delivery,
            status: ContractStatus::Sent,
            data: None,
            created_at: now,
            updated_at: now,
        };
        {
            let mut b = self.bindings.lock();
            let binding = b.entry(msg.message_id.clone()).or_default();
            binding.channel = Some(channel.to_string());
            binding.logical_id = msg.logical_id.clone();
        }
        // 每个 message_id 只会被 accept 一次：id 由提交分配当场生成、fanout 目标已去重、
        // outbox 重放走自己的表不经过 accept。所以这里只需要 O(1) 的 active 守卫，
        // 不再扫描审计历史——真正的重复写入由 messages 主键冲突显式失败。
        if let Some(existing) = self.nucleus.lock().get(&msg.message_id).cloned() {
            return (record_from_message(&existing), false);
        }
        self.nucleus.lock().put(
            msg.message_id.clone(),
            msg.logical_id.clone(),
            msg.source_domain.clone(),
            seq,
            msg.target.clone(),
            msg.message_type.clone(),
            msg.delivery,
            ContractStatus::Sent,
            None,
        );
        if msg.delivery == DeliveryPolicy::Durable {
            // 持久化成功先于投递（C3）：账本行与完整信封同一事务落盘。
            let result = (|| -> Result<(), StorageError> {
                let mut db = self.db_conn.as_ref().unwrap().lock();
                let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                write_messages_tx(&tx, &r)?;
                crate::services::outbox::insert_frame(
                    &tx,
                    channel,
                    channel_seq,
                    &crate::core::protocol::KernelToDomainFrame::Deliver(msg.clone()),
                )?;
                tx.commit()?;
                Ok(())
            })();
            if let Err(e) = result {
                return (self.mark_unpersisted_failure(&r, e), false);
            }
        }
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
        let Some(old) = self.record_for_message(id) else {
            return ReceiptTransition::UnknownDelivery;
        };
        if old.target != target {
            return ReceiptTransition::UnauthorizedTarget;
        }
        if old.status.is_terminal() || status.stage_rank() <= old.status.stage_rank() {
            return ReceiptTransition::DuplicateOrStale;
        }
        let r = self.commit_advance(id, &old, status, data);
        ReceiptTransition::Advanced {
            source_domain: r.source_domain.clone(),
            receipt: r.to_receipt(),
        }
    }

    /// 唯一的状态推进出口：终态 = 转审计（Durable）/ 清除（其余）+ 回收去重窗口。
    fn commit_advance(
        &self,
        id: &str,
        old: &DeliveryRecord,
        status: ContractStatus,
        data: Option<Value>,
    ) -> DeliveryRecord {
        let mut r = old.clone();
        r.status = status;
        r.updated_at = now_millis();
        if data.is_some() {
            r.data = data;
        }
        if old.policy == DeliveryPolicy::Durable {
            if let Err(e) = self.persist_advance(&r) {
                return self.mark_unpersisted_failure(&r, e);
            }
        }
        self.nucleus
            .lock()
            .advance(id, status, r.data.clone());
        if status.is_terminal() {
            // 终态已提交：写入时就地打标记，代际退役时零回查（历史在盘上，不该为清理去翻它）。
            self.mark_binding_terminal(id);
            // 独立账本使用（无路由器 origin 绑定）时，终态即回收活行身份。
            self.maybe_retire_binding(id);
        }
        r
    }

    fn mark_binding_terminal(&self, id: &str) {
        if let Some(b) = self.bindings.lock().get_mut(id) {
            b.terminal = true;
        }
    }

    fn maybe_retire_binding(&self, id: &str) {
        if self.source_instance(id).unwrap_or(0) == 0 {
            self.bindings.lock().remove(id);
        }
    }

    pub fn get_record(&self, id: &str, target: &str) -> Option<DeliveryRecord> {
        self.record_for_message(id).filter(|r| r.target == target)
    }

    pub fn get_persisted_record(&self, id: &str) -> Option<DeliveryRecord> {
        self.read_disk(id)
    }

    pub fn persisted_count(&self) -> usize {
        let db = self.db_conn.as_ref().unwrap().lock();
        let messages: i64 = db
            .query_row("SELECT count(*) FROM messages", [], |r| r.get(0))
            .unwrap_or(0);
        let audit: i64 = db
            .query_row("SELECT count(*) FROM audit", [], |r| r.get(0))
            .unwrap_or(0);
        (messages + audit) as usize
    }

    pub fn receipts_for_message(&self, id: &str) -> Vec<MessageReceipt> {
        self.record_for_message(id)
            .map(|r| vec![r.to_receipt()])
            .unwrap_or_default()
    }

    pub fn receipts_for_logical(&self, logical: &str) -> Vec<MessageReceipt> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        // 在途（含 Volatile/Ephemeral，它们不落 messages 表）来自核；终态历史来自 SQLite。
        {
            let n = self.nucleus.lock();
            for m in n.active_snapshot() {
                if m.logical_id == logical && seen.insert(m.message_id.clone()) {
                    out.push(record_from_message(&m).to_receipt());
                }
            }
        }
        if let Some(db) = self.db_conn.as_ref() {
            let db = db.lock();
            for table in ["messages", "audit"] {
                let mut stmt = db
                    .prepare(&format!(
                        "SELECT message_id,logical_id,seq,source_domain,target,message_type,policy,status,data,created_at,{} FROM {table} WHERE logical_id=?1",
                        if table == "messages" { "updated_at" } else { "created_at" }
                    ))
                    .ok();
                if let Some(mut s) = stmt.take() {
                    let rows = s
                        .query_map([logical], |row| {
                            if table == "messages" {
                                messages_row_to_record(row)
                            } else {
                                audit_row_to_record(row).map(|a| record_from_audit(&a))
                            }
                        })
                        .ok();
                    if let Some(rows) = rows {
                        for item in rows.flatten() {
                            if seen.insert(item.message_id.clone()) {
                                out.push(item.to_receipt());
                            }
                        }
                    }
                }
            }
        }
        out
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
        let ids: Vec<String> = {
            let b = self.bindings.lock();
            b.iter()
                .filter(|(_, b)| channel.is_none_or(|c| b.channel.as_deref() == Some(c)))
                .map(|(id, _)| id.clone())
                .collect()
        };
        let mut notifications = Vec::new();
        for id in ids {
            let gate = self.message_gate(&id);
            let _guard = gate.lock();
            let Some(old) = self.record_for_message(&id) else {
                continue;
            };
            if old.status.is_terminal()
                || parse_target_address(&old.target).is_none_or(|(d, _)| d != domain)
            {
                continue;
            }
            let data = Some(json!({
                "error": "target_domain_faulted",
                "domain": domain,
                "reason": reason,
                "execution_outcome": "unknown"
            }));
            let r = self.commit_advance(&id, &old, ContractStatus::ExecutionFailed, data);
            notifications.push((r.source_domain.clone(), r.to_receipt()));
        }
        notifications
    }

    /// 统一读路径：在途看核，终态看盘。
    ///
    /// 一个 id 只有一个权威来源——活跃行在核内，终态历史在 SQLite 的 `audit` 表。
    /// I1 保证二者互斥（终态事务里 `DELETE messages` + `INSERT audit` 原子发生），
    /// 所以这里不存在"两个来源给出不同答案"的可能。
    fn record_for_message(&self, id: &str) -> Option<DeliveryRecord> {
        {
            let n = self.nucleus.lock();
            if let Some(m) = n.get(id) {
                return Some(record_from_message(m));
            }
        }
        self.read_audit(id)
    }

    /// 按 message_id 对终态历史做索引点查（`idx_audit_message`）。
    /// 宪法 C3:54：历史结果只在盘上，按需取载荷，不建内存索引。
    fn read_audit(&self, id: &str) -> Option<DeliveryRecord> {
        let db = self.db_conn.as_ref()?.lock();
        db.query_row(
            "SELECT message_id,logical_id,seq,source_domain,target,message_type,policy,status,data,created_at FROM audit WHERE message_id=?1 LIMIT 1",
            [id],
            audit_row_to_record,
        )
        .optional()
        .ok()
        .flatten()
        .map(|a| record_from_audit(&a))
    }

    fn read_disk(&self, id: &str) -> Option<DeliveryRecord> {
        let db = self.db_conn.as_ref()?.lock();
        let msg: Option<DeliveryRecord> = db
            .query_row(
                "SELECT message_id,logical_id,seq,source_domain,target,message_type,policy,status,data,created_at,updated_at FROM messages WHERE message_id=?1",
                [id],
                messages_row_to_record,
            )
            .optional()
            .ok()
            .flatten();
        if msg.is_some() {
            return msg;
        }
        db.query_row(
            "SELECT message_id,logical_id,seq,source_domain,target,message_type,policy,status,data,created_at FROM audit WHERE message_id=?1",
            [id],
            audit_row_to_record,
        )
        .optional()
        .ok()
        .flatten()
        .map(|a| record_from_audit(&a))
    }

    fn persist_live(&self, r: &DeliveryRecord) -> Result<(), StorageError> {
        let db = self.db_conn.as_ref().unwrap().lock();
        write_messages_tx(&db, r)
    }

    fn persist_advance(&self, r: &DeliveryRecord) -> Result<(), StorageError> {
        let mut db = self.db_conn.as_ref().unwrap().lock();
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if r.status.is_terminal() {
            tx.execute("DELETE FROM messages WHERE message_id=?1", [&r.message_id])?;
            tx.execute("DELETE FROM kernel_outbox WHERE delivery_id=?1", [&r.message_id])?;
            insert_audit_tx(&tx, r)?;
        } else {
            tx.execute(
                "UPDATE messages SET status=?1,data=?2,updated_at=?3 WHERE message_id=?4",
                params![
                    status_str(r.status),
                    r.data.as_ref().map(serde_json::to_string).transpose()?,
                    r.updated_at,
                    r.message_id
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn append_audit_disk(&self, r: &DeliveryRecord) -> Result<(), StorageError> {
        let db = self.db_conn.as_ref().unwrap().lock();
        insert_audit_tx(&db, r)
    }

    fn set_persistence_failed(&self, id: &str) {
        if let Some(b) = self.bindings.lock().get_mut(id) {
            b.persistence_failed = true;
        }
    }

    fn mark_unpersisted_failure(&self, r: &DeliveryRecord, e: impl ToString) -> DeliveryRecord {
        let failed = Self::failure(r, e);
        self.set_persistence_failed(&r.message_id);
        // 就地标记为失败但保留活行：结果没有落盘，所以它不是历史（不进 audit），
        // 但仍须可读，否则来源域连这次失败都无法重放。
        self.nucleus.lock().mark_live_failure(&r.message_id, failed.data.clone());
        failed
    }

    fn failure(r: &DeliveryRecord, error: impl ToString) -> DeliveryRecord {
        let mut failed = r.clone();
        failed.status = ContractStatus::ExecutionFailed;
        failed.updated_at = now_millis();
        failed.data = Some(json!({
            "error": "kernel_persistence_failed",
            "reason": error.to_string().chars().take(2048).collect::<String>(),
            "persisted": false,
            "previous_status": r.status,
            "execution_outcome": "unknown"
        }));
        failed
    }
}

fn table_present(db: &Connection, table: &str) -> bool {
    db.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
        [table],
        |r| r.get::<_, i64>(0),
    )
    .map(|c| c > 0)
    .unwrap_or(false)
}

/// 在途行（`messages`）的坏行探测：非法 status 或非法 JSON。表不存在时视为无坏行。
fn live_row_violation(db: &Connection) -> Result<Option<String>, StorageError> {
    if !table_present(db, "messages") {
        return Ok(None);
    }
    Ok(db
        .query_row(
            "SELECT message_id FROM messages WHERE (data IS NOT NULL AND NOT json_valid(data)) \
             OR status NOT IN ('sent','received','started','completed','target_invalid','contract_invalid','execution_failed') LIMIT 1",
            [],
            |r| r.get(0),
        )
        .optional()?)
}

/// 历史行（`audit`）的坏行探测。表不存在时视为无坏行。
fn audit_row_violation(db: &Connection) -> Result<Option<String>, StorageError> {
    if !table_present(db, "audit") {
        return Ok(None);
    }
    Ok(db
        .query_row(
            "SELECT message_id FROM audit WHERE (data IS NOT NULL AND NOT json_valid(data)) \
             OR status NOT IN ('completed','target_invalid','contract_invalid','execution_failed') LIMIT 1",
            [],
            |r| r.get(0),
        )
        .optional()?)
}

/// 行级完整性校验（供 `--check` 使用）：坏行必须显式暴露，不能被静默重解释。
///
/// 启动路径只校验在途行；历史行在这里按需遍历，因此不会拖慢每次启动
/// （宪法 C3:54：历史终态结果按需读取，不建立全量内存索引）。
pub fn verify_row_integrity(db: &Connection) -> Result<(), StorageError> {
    if let Some(id) = live_row_violation(db)? {
        return Err(StorageError::CorruptLedgerRecord(id));
    }
    if let Some(id) = audit_row_violation(db)? {
        return Err(StorageError::CorruptLedgerRecord(id));
    }
    Ok(())
}

fn messages_row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeliveryRecord> {
    let status: String = row.get(7)?;
    let policy: String = row.get(6)?;
    let data: Option<String> = row.get(8)?;
    Ok(DeliveryRecord {
        message_id: row.get(0)?,
        logical_id: row.get(1)?,
        seq: row.get::<_, i64>(2)? as u64,
        source_domain: row.get(3)?,
        target: row.get(4)?,
        message_type: row.get(5)?,
        policy: parse_policy(&policy).map_err(to_sql_error)?,
        status: parse_status(&status).map_err(to_sql_error)?,
        data: data
            .map(|s| serde_json::from_str(&s))
            .transpose()
            .map_err(to_sql_json_error)?,
        created_at: row.get(9)?,
        updated_at: row.get(10)?,
    })
}

fn audit_row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<AuditRecord> {
    let status: String = row.get(7)?;
    let policy: String = row.get(6)?;
    let data: Option<String> = row.get(8)?;
    Ok(AuditRecord {
        message_id: row.get(0)?,
        logical_id: row.get(1)?,
        seq: row.get::<_, i64>(2)? as u64,
        source: row.get(3)?,
        target: row.get(4)?,
        message_type: row.get(5)?,
        policy: parse_policy(&policy).map_err(to_sql_error)?,
        status: parse_status(&status).map_err(to_sql_error)?,
        data: data
            .map(|s| serde_json::from_str(&s))
            .transpose()
            .map_err(to_sql_json_error)?,
        created_at: row.get(9)?,
    })
}

fn to_sql_error(e: StorageError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
}

fn to_sql_json_error(e: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(8, rusqlite::types::Type::Text, Box::new(e))
}

fn write_messages_tx(db: &Connection, r: &DeliveryRecord) -> Result<(), StorageError> {
    // 只用于**首次**落盘。同一 message_id 重复写入必须显式失败（主键冲突），
    // 不允许用 upsert 静默覆盖一条已经存在的账本行：那会掩盖重复投递。
    // 后续状态推进走 `persist_advance` 的 UPDATE。
    db.execute(
        "INSERT INTO messages(message_id,logical_id,seq,source_domain,target,message_type,policy,status,data,created_at,updated_at) \
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            r.message_id,
            r.logical_id,
            r.seq as i64,
            r.source_domain,
            r.target,
            r.message_type,
            policy_str(r.policy),
            status_str(r.status),
            r.data.as_ref().map(serde_json::to_string).transpose()?,
            r.created_at,
            r.updated_at
        ],
    )?;
    Ok(())
}

fn insert_audit_tx(db: &Connection, r: &DeliveryRecord) -> Result<(), StorageError> {
    db.execute(
        "INSERT INTO audit(message_id,logical_id,seq,source_domain,target,message_type,policy,status,data,created_at) \
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            r.message_id,
            r.logical_id,
            r.seq as i64,
            r.source_domain,
            r.target,
            r.message_type,
            policy_str(r.policy),
            status_str(r.status),
            r.data.as_ref().map(serde_json::to_string).transpose()?,
            r.created_at
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_ingress_durable_lands_in_audit_only() {
        let ledger = KernelLedger::new();
        let (logical, ids, dup) = ledger.allocate_submission("sender#1", 1, 1);
        assert!(!dup);
        ledger.bind_origin(&ids, 1);
        let (rec, created) = ledger.record_initial_target_state_locked(
            &ids[0],
            &logical,
            1,
            "sender",
            "worker.missing",
            "Run",
            DeliveryPolicy::Durable,
            ContractStatus::TargetInvalid,
            Some(json!({"error": "target_not_found"})),
        );
        assert!(created);
        assert_eq!(rec.status, ContractStatus::TargetInvalid);
        // 不占活跃行，只在审计里
        assert_eq!(ledger.retention_counts()["resident_records"], 0);
        assert_eq!(
            ledger.get_persisted_record(&ids[0]).unwrap().logical_id,
            logical
        );
    }

    #[test]
    fn ephemeral_submission_retires_dedup_after_all_terminal() {
        let ledger = KernelLedger::new();
        for seq in 1..=100u64 {
            let (logical, ids, dup) = ledger.allocate_submission("sender#1", seq, 2);
            assert!(!dup);
            ledger.bind_origin(&ids, 1);
            for id in &ids {
                ledger.record_initial_target_state_locked(
                    id, &logical, seq, "sender", "worker.run", "Ephemeral",
                    DeliveryPolicy::Ephemeral, ContractStatus::Sent, None,
                );
            }
            for id in &ids {
                assert!(matches!(
                    ledger.advance_status("worker", id, "worker.run", ContractStatus::Completed, Some(json!({"ok": true}))),
                    ReceiptTransition::Advanced { .. }
                ));
            }
            assert_eq!(ledger.retention_counts()["resident_records"], 0);
            let _ = logical;
        }
        // 终态 Ephemeral 去重已回收
        assert_eq!(ledger.retention_counts()["submissions"], 0);
    }

    #[test]
    fn durable_replay_survives_terminal_and_reads_audit() {
        let ledger = KernelLedger::new();
        let (logical, ids, _) = ledger.allocate_submission("sender#1", 1, 1);
        ledger.bind_origin(&ids, 1);
        ledger.record_initial_target_state_locked(
            &ids[0], &logical, 1, "sender", "worker.run", "Run",
            DeliveryPolicy::Durable, ContractStatus::Sent, None,
        );
        ledger.advance_status("worker", &ids[0], "worker.run", ContractStatus::Completed, Some(json!({"kept": true})));
        // Durable 终态从审计读回，唯一且不可覆盖
        let replay = ledger.receipts_for_message(&ids[0]);
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].status, ContractStatus::Completed);
        assert_eq!(replay[0].data, Some(json!({"kept": true})));
        assert_eq!(replay[0].logical_id, logical);
    }

    #[test]
    fn domain_receipt_cannot_claim_internal_delivery_without_channel() {
        let ledger = KernelLedger::new();
        let (logical, ids, _) = ledger.allocate_submission("sender#1", 1, 1);
        ledger.record_initial_target_state(
            &ids[0], &logical, 1, "sender", "kernel.supervisor", "RestartDomain",
            ContractStatus::Sent, None,
        );
        assert!(!ledger.accepts_channel(&ids[0], "unrelated-domain-pipe"));
    }
}
