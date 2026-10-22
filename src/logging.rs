use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::storage::StorageError;

pub const LOGS_TABLE: &str = "hub_logs";

/// Standard Unix timestamp in seconds with fractional precision (matches Python `time.time()`).
pub fn now_secs_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn default_info_level() -> String {
    "INFO".to_string()
}

/// Outcome of an ephemeral progress update; independent of message receipts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressState {
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Unified Multi-State Structured Log Record.
///
/// Supports 3 core operational patterns:
/// 1. **Hierarchical / Lifecycle Logs**: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` with `layer`, `component`, and `phase` execution pairing.
/// 2. **Ephemeral Progress**: `ephemeral=true` with `key`, `frac`, `waiting`, and `done`. Dispatched only in-memory / SSE, never written to SQLite `hub_logs`.
/// 3. **Console Only**: `console_only=true` (high-frequency heartbeats). Printed only to terminal, excluded from both `hub_logs` and SSE fanout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    /// Kernel-bound origin; None for historical records predating this field.
    #[serde(default)]
    pub source_domain: Option<String>,
    #[serde(default)]
    pub id: Option<i64>,
    #[serde(default = "now_secs_f64")]
    pub ts: f64,
    #[serde(default = "default_info_level")]
    pub level: String,
    #[serde(default)]
    pub layer: String,
    #[serde(default)]
    pub component: String,
    #[serde(default)]
    pub message: String,
    /// Execution lifecycle pairing marker (`"start"` / `"finish"` / `""`).
    #[serde(default)]
    pub phase: String,
    /// High-frequency telemetry visible only on console/terminal (no DB, no SSE).
    #[serde(default)]
    pub console_only: bool,
    /// Live progress/telemetry dispatched only via SSE/pub-sub (never pollutes SQLite disk tables).
    #[serde(default)]
    pub ephemeral: bool,
    /// Stable key for in-place frontend progress bar updating.
    #[serde(default)]
    pub key: Option<String>,
    /// Determinate progress fraction in range [0.0, 1.0]. None indicates indeterminate spinner.
    #[serde(default)]
    pub frac: Option<f64>,
    /// True when the task is waiting/queuing before active execution.
    #[serde(default)]
    pub waiting: bool,
    /// True when the ephemeral task is finished (signals termination, not necessarily success).
    #[serde(default)]
    pub done: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ProgressState>,
}

impl Default for LogRecord {
    fn default() -> Self {
        Self {
            source_domain: None,
            id: None,
            ts: now_secs_f64(),
            level: "INFO".to_string(),
            layer: String::new(),
            component: String::new(),
            message: String::new(),
            phase: String::new(),
            console_only: false,
            ephemeral: false,
            key: None,
            frac: None,
            waiting: false,
            done: false,
            state: None,
        }
    }
}

/// Payload wrapper for log frames arriving from child domains over IPC.
/// Tolerates either `{"frame_type": "log", "record": {...}}` or flattened `{"frame_type": "log", ...}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogFramePayload {
    #[serde(default)]
    pub record: Option<LogRecord>,
    #[serde(flatten)]
    pub flat: Option<LogRecord>,
}

impl LogFramePayload {
    pub fn from_record(record: LogRecord) -> Self {
        Self {
            record: Some(record),
            flat: None,
        }
    }

    pub fn into_record(self) -> LogRecord {
        if let Some(r) = self.record {
            r
        } else if let Some(f) = self.flat {
            f
        } else {
            LogRecord::default()
        }
    }
}

pub const LOG_PAGE_MAX_ROWS: usize = 2000;
pub const LOG_PAGE_MAX_BYTES: usize = 2 * 1024 * 1024;

/// Stateless history selector. Cursor IDs refer to one database and unchanged filters.
#[derive(Debug)]
pub struct LogQuery<'a> {
    pub limit: usize,
    pub before_id: Option<i64>,
    pub level: Option<&'a str>,
    pub layer: Option<&'a str>,
    pub component: Option<&'a str>,
    pub phase: Option<&'a str>,
    pub since_ts: Option<f64>,
    pub source_domain: Option<&'a str>,
}
impl Default for LogQuery<'_> {
    fn default() -> Self {
        Self {
            limit: 200,
            before_id: None,
            level: None,
            layer: None,
            component: None,
            phase: None,
            since_ts: None,
            source_domain: None,
        }
    }
}
impl<'a> LogQuery<'a> {
    pub fn from_params(params: &'a serde_json::Value) -> Result<Self, StorageError> {
        let invalid =
            |name| StorageError::InvalidLogQuery(format!("{name} must be a positive integer"));
        let limit = match params.get("limit") {
            None => 200,
            Some(value) => value
                .as_u64()
                .filter(|n| *n > 0)
                .ok_or_else(|| invalid("limit"))?
                .min(LOG_PAGE_MAX_ROWS as u64) as usize,
        };
        let before_id = match params.get("before_id") {
            None | Some(serde_json::Value::Null) => None,
            Some(value) => Some(
                value
                    .as_i64()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| invalid("before_id"))?,
            ),
        };
        let since_ts = match params.get("since_ts") {
            None | Some(serde_json::Value::Null) => None,
            Some(value) => Some(value.as_f64().filter(|n| n.is_finite()).ok_or_else(|| {
                StorageError::InvalidLogQuery("since_ts must be a finite number".into())
            })?),
        };
        Ok(Self {
            limit,
            before_id,
            since_ts,
            level: params.get("level").and_then(serde_json::Value::as_str),
            layer: params.get("layer").and_then(serde_json::Value::as_str),
            component: params.get("component").and_then(serde_json::Value::as_str),
            phase: params.get("phase").and_then(serde_json::Value::as_str),
            source_domain: params
                .get("domain_id")
                .or_else(|| params.get("domain"))
                .or_else(|| params.get("source_domain"))
                .and_then(serde_json::Value::as_str),
        })
    }
}
#[derive(Debug, Serialize)]
pub struct LogPage {
    pub logs: Vec<LogRecord>,
    pub total: usize,
    pub has_more: bool,
    pub next_before_id: Option<i64>,
}

/// Durable journal with asynchronous, bounded-batch import into `hub_logs`.
///
/// Follows the black-box logging guarantee:
/// - Acceptance waits for an independent journal commit, not the application's SQLite write lock.
/// - Records remain journaled until hub_logs and its import cursor commit together.
/// - Real-time subscriber drops never remove pending persistence records.
pub struct LogStore {
    tx: SyncSender<()>,
    spool: Arc<crate::log_spool::LogSpool>,
    stop: Arc<AtomicBool>,
    writer_handle: Option<thread::JoinHandle<()>>,
    query_conn: Mutex<Connection>,
    query_gate: Option<Arc<Mutex<()>>>,
}

impl LogStore {
    /// Initializes `LogStore` backed by a file path or in-memory SQLite.
    pub fn new(db_path: Option<&Path>) -> Result<Self, StorageError> {
        let (conn_writer, conn_reader) = match db_path {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        std::fs::create_dir_all(parent)?;
                    }
                }
                let w = Connection::open(path)?;
                let r = Connection::open(path)?;
                w.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; PRAGMA busy_timeout = 5000;")?;
                r.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000;")?;
                (w, r)
            }
            None => {
                let unique_id = uuid::Uuid::new_v4().simple();
                let uri = format!("file:hub_logs_{unique_id}?mode=memory&cache=shared");
                let w = Connection::open(&uri)?;
                let r = Connection::open(&uri)?;
                (w, r)
            }
        };

        // Create table and indexes on both connections
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {LOGS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                level TEXT NOT NULL,
                layer TEXT DEFAULT '',
                component TEXT DEFAULT '',
                message TEXT NOT NULL,
                phase TEXT DEFAULT '',
                source_domain TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_{LOGS_TABLE}_ts ON {LOGS_TABLE}(ts);
            CREATE INDEX IF NOT EXISTS idx_{LOGS_TABLE}_level ON {LOGS_TABLE}(level);
            CREATE INDEX IF NOT EXISTS idx_{LOGS_TABLE}_layer ON {LOGS_TABLE}(layer);"
        );
        conn_writer.execute_batch(&ddl)?;
        conn_reader.execute_batch(&ddl)?;

        // Additive migration for legacy databases; preserve unknown origins of pre-upgrade rows.
        let present = {
            let mut stmt = conn_writer.prepare("PRAGMA table_info(hub_logs)")?;
            let names = stmt.query_map([], |row| row.get::<_, String>(1))?;
            names
                .collect::<Result<Vec<_>, _>>()?
                .iter()
                .any(|n| n == "source_domain")
        };
        if !present {
            conn_writer.execute_batch("ALTER TABLE hub_logs ADD COLUMN source_domain TEXT;")?;
        }

        let spool = crate::log_spool::LogSpool::open(db_path, &conn_writer)?;
        let (tx, rx) = mpsc::sync_channel::<()>(1); // Wakeups only; never stores log payloads.
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker_spool = spool.clone();
        // Shared-cache in-memory SQLite returns SQLITE_LOCKED rather than waiting on WAL.
        let query_gate = db_path.is_none().then(|| Arc::new(Mutex::new(())));
        let worker_gate = query_gate.clone();
        let writer_handle = thread::Builder::new()
            .name("hub-logstore-writer".into())
            .spawn(move || {
                Self::writer_loop(conn_writer, rx, worker_stop, worker_spool, worker_gate)
            })?;
        Ok(Self {
            tx,
            spool,
            stop,
            writer_handle: Some(writer_handle),
            query_conn: Mutex::new(conn_reader),
            query_gate,
        })
    }

    /// Accept a persistent record only after committing it to the independent disk journal.
    /// Ephemeral and console-only records preserve their original no-persistence contract.
    pub fn emit(&self, record: LogRecord) -> Result<(), StorageError> {
        if record.console_only || record.ephemeral {
            return Ok(());
        }
        if let Err(error) = self.spool.append(&record) {
            self.spool.append_failed(&error);
            return Err(error);
        }
        let _ = self.tx.try_send(());
        Ok(())
    }

    /// Confirm that all records accepted before this call reached hub_logs, or report failure.
    pub fn flush(&self, timeout: Duration) -> Result<(), StorageError> {
        let _ = self.tx.try_send(());
        self.spool.flush(timeout)
    }

    pub fn persistence_error(&self) -> Option<String> {
        self.spool.error()
    }

    fn writer_loop(
        mut conn: Connection,
        rx: Receiver<()>,
        stop: Arc<AtomicBool>,
        spool: Arc<crate::log_spool::LogSpool>,
        gate: Option<Arc<Mutex<()>>>,
    ) {
        loop {
            let result = {
                let _guard = gate.as_ref().map(|g| g.lock());
                spool.import_batch(&mut conn)
            };
            match result {
                Ok(true) => continue,
                Ok(false) => {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                }
                Err(error) => {
                    spool.diagnostic(&error);
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                }
            }
            // A failed transaction keeps the entire journal batch intact for retry/restart.
            let _ = rx.recv_timeout(Duration::from_millis(200));
        }
    }

    /// Queries historical records from `hub_logs`.
    pub fn query(
        &self,
        limit: usize,
        level: Option<&str>,
        layer: Option<&str>,
        component: Option<&str>,
        phase: Option<&str>,
        since_ts: Option<f64>,
    ) -> Result<Vec<LogRecord>, StorageError> {
        self.query_with_domain(limit, level, layer, component, phase, since_ts, None)
    }

    /// Queries historical records from `hub_logs` with optional source_domain filter.
    pub fn query_with_domain(
        &self,
        limit: usize,
        level: Option<&str>,
        layer: Option<&str>,
        component: Option<&str>,
        phase: Option<&str>,
        since_ts: Option<f64>,
        source_domain: Option<&str>,
    ) -> Result<Vec<LogRecord>, StorageError> {
        Ok(self
            .query_page(&LogQuery {
                limit,
                level,
                layer,
                component,
                phase,
                since_ts,
                source_domain,
                ..LogQuery::default()
            })?
            .logs)
    }

    /// Reads a bounded, stable-ID page directly from SQLite. Does not flush or mix live copies.
    pub fn query_page(&self, query: &LogQuery<'_>) -> Result<LogPage, StorageError> {
        let LogQuery {
            limit,
            before_id,
            level,
            layer,
            component,
            phase,
            since_ts,
            source_domain,
        } = *query;
        if limit == 0
            || before_id.is_some_and(|id| id <= 0)
            || since_ts.is_some_and(|ts| !ts.is_finite())
        {
            return Err(StorageError::InvalidLogQuery(
                "invalid limit, before_id or since_ts".into(),
            ));
        }
        let limit = limit.min(LOG_PAGE_MAX_ROWS);
        let _guard = self.query_gate.as_ref().map(|g| g.lock());
        let conn = self.query_conn.lock();
        let mut sql = format!(
            "SELECT id, ts, level, layer, component, message, phase, source_domain FROM {LOGS_TABLE} WHERE 1=1"
        );
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(lvl) = level {
            if !lvl.is_empty() {
                sql.push_str(" AND level = ?");
                params_vec.push(Box::new(lvl.to_uppercase()));
            }
        }
        if let Some(lyr) = layer {
            if !lyr.is_empty() {
                sql.push_str(" AND layer = ?");
                params_vec.push(Box::new(lyr.to_string()));
            }
        }
        if let Some(cmp) = component {
            if !cmp.is_empty() {
                sql.push_str(" AND component = ?");
                params_vec.push(Box::new(cmp.to_string()));
            }
        }
        if let Some(ph) = phase {
            if !ph.is_empty() {
                sql.push_str(" AND phase = ?");
                params_vec.push(Box::new(ph.to_string()));
            }
        }
        if let Some(ts) = since_ts {
            sql.push_str(" AND ts >= ?");
            params_vec.push(Box::new(ts));
        }
        if let Some(domain) = source_domain {
            if !domain.is_empty() {
                sql.push_str(" AND (source_domain = ? OR component = ?)");
                params_vec.push(Box::new(domain.to_string()));
                params_vec.push(Box::new(domain.to_string()));
            }
        }

        if let Some(id) = before_id {
            sql.push_str(" AND id < ?");
            params_vec.push(Box::new(id));
        }
        sql.push_str(" ORDER BY id DESC LIMIT ?");
        params_vec.push(Box::new((limit + 1) as i64));

        let mut stmt = conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|b| b.as_ref()).collect();

        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            Ok(LogRecord {
                source_domain: row.get(7)?,
                id: Some(row.get(0)?),
                ts: row.get(1)?,
                level: row.get(2)?,
                layer: row.get(3)?,
                component: row.get(4)?,
                message: row.get(5)?,
                phase: row.get(6)?,
                console_only: false,
                ephemeral: false,
                key: None,
                frac: None,
                waiting: false,
                done: false,
                state: None,
            })
        })?;

        let mut records = Vec::new();
        // Includes array punctuation and page metadata; leaves ample space below IPC frame limits.
        let mut bytes = 256;
        let mut has_more = false;
        for row in rows {
            let record = row?;
            if records.len() == limit {
                has_more = true;
                break;
            }
            let size = serde_json::to_vec(&record)?.len() + 1;
            if bytes + size > LOG_PAGE_MAX_BYTES {
                if records.is_empty() {
                    return Err(StorageError::ResourceExhausted(format!(
                        "log record id={} exceeds {} byte history page budget",
                        record.id.unwrap(),
                        LOG_PAGE_MAX_BYTES
                    )));
                }
                has_more = true;
                break;
            }
            bytes += size;
            records.push(record);
        }
        let next_before_id = if has_more {
            records.last().and_then(|r| r.id)
        } else {
            None
        };
        Ok(LogPage {
            total: records.len(),
            logs: records,
            has_more,
            next_before_id,
        })
    }
}

impl Drop for LogStore {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = self.tx.try_send(());
        if let Some(handle) = self.writer_handle.take() {
            let _ = handle.join();
        }
    }
}

/// Top-level Unified Kernel Logging Hub.
///
/// Coordinates:
/// - **Console**: Compact single-line colored output.
/// - **Fanout Stream**: Real-time pub-sub to Web UI SSE & Observability clients.
/// - **Ephemeral Telemetry**: In-memory active progress map for live progress bars.
/// - **Persistence**: `hub_logs` batch store.
pub struct KernelLogHub {
    store: Arc<LogStore>,
    subscribers: Mutex<Vec<std::sync::mpsc::SyncSender<LogRecord>>>,
    recent_logs: Mutex<Vec<LogRecord>>,
    active_progress: Mutex<HashMap<(String, String), LogRecord>>,
    console_enabled: AtomicBool,
}

impl KernelLogHub {
    pub fn new(db_path: Option<&Path>) -> Result<Self, StorageError> {
        let store = Arc::new(LogStore::new(db_path)?);
        Ok(Self {
            store,
            subscribers: Mutex::new(Vec::new()),
            recent_logs: Mutex::new(Vec::new()),
            active_progress: Mutex::new(HashMap::new()),
            console_enabled: AtomicBool::new(true),
        })
    }

    /// Sets whether console logging to stdout is enabled.
    pub fn set_console_enabled(&self, enabled: bool) {
        self.console_enabled.store(enabled, Ordering::SeqCst);
    }

    /// Subscribes to the real-time stream of all log events (both structured logs and ephemeral progress).
    pub fn subscribe(&self) -> Receiver<LogRecord> {
        let (tx, rx) = mpsc::sync_channel(128);
        let mut subscribers = self.subscribers.lock();
        if subscribers.len() < 64 {
            subscribers.push(tx);
        }
        rx
    }

    /// Core relay entry point: reinjects a structured log record from remote child domains or internal kernel subsystems.
    /// Preserves complete field fidelity (level, layer, component, phase, key, frac, waiting, done).
    pub fn reinject(&self, mut record: LogRecord) {
        if record.source_domain.is_none() {
            record.source_domain = Some("kernel".to_string());
        }
        if record.ts <= 0.0 {
            record.ts = now_secs_f64();
        }

        // 1. Console Output:
        // Ephemeral progress lines do NOT print to console (avoids flooding terminal).
        // console_only lines print ONLY to console.
        if self.console_enabled.load(Ordering::Relaxed) && !record.ephemeral {
            println!("{}", Self::format_console(&record));
        }

        if record.console_only {
            return;
        }

        // 2. Ephemeral Progress: in-memory tracking & SSE fanout (NEVER enters SQLite hub_logs)
        if record.ephemeral {
            let state = record.state.unwrap_or(if record.done {
                ProgressState::Completed
            } else {
                ProgressState::Running
            });
            record.state = Some(state);
            record.done = state != ProgressState::Running;
            if record.done {
                record.waiting = false;
            }
            if let Some(key) = &record.key {
                let key = (
                    record.source_domain.clone().unwrap_or_default(),
                    key.clone(),
                );
                let mut progress_map = self.active_progress.lock();
                if record.done {
                    progress_map.remove(&key);
                } else if (progress_map.len() < 1024 || progress_map.contains_key(&key))
                    && serde_json::to_vec(&record).is_ok_and(|bytes| bytes.len() <= 65_536)
                {
                    progress_map.insert(key.clone(), record.clone());
                }
            }
            self.broadcast(record);
            return;
        }

        // 3. Accept into the durable journal before announcing a persistent live record.
        if self.store.emit(record.clone()).is_err() {
            return;
        }
        {
            let mut recent = self.recent_logs.lock();
            if recent.len() >= 500 {
                recent.remove(0);
            }
            if serde_json::to_vec(&record).is_ok_and(|bytes| bytes.len() <= 65_536) {
                recent.push(record.clone());
            }
        }

        self.broadcast(record);
    }

    fn broadcast(&self, record: LogRecord) {
        if serde_json::to_vec(&record).map_or(true, |v| v.len() > 65_536) {
            return;
        }
        let mut subs = self.subscribers.lock();
        subs.retain(|tx| match tx.try_send(record.clone()) {
            Ok(()) | Err(mpsc::TrySendError::Full(_)) => true,
            Err(mpsc::TrySendError::Disconnected(_)) => false,
        });
    }

    /// Returns a snapshot of currently active in-flight progress records.
    pub fn active_progress(&self) -> Vec<LogRecord> {
        self.active_progress.lock().values().cloned().collect()
    }

    /// Legacy live snapshot for the built-in console; never merged into persisted history pages.
    pub fn recent_logs(&self) -> Vec<LogRecord> {
        self.recent_logs.lock().clone()
    }

    /// Infrastructure health; an append failure remains visible until this host restarts.
    pub fn persistence_error(&self) -> Option<String> {
        self.store.persistence_error()
    }

    /// Queries persisted logs from SQLite `hub_logs`.
    pub fn query_logs(
        &self,
        limit: usize,
        level: Option<&str>,
        layer: Option<&str>,
        component: Option<&str>,
        phase: Option<&str>,
        since_ts: Option<f64>,
    ) -> Result<Vec<LogRecord>, StorageError> {
        self.store
            .query(limit, level, layer, component, phase, since_ts)
    }

    /// Compatibility selector; only stable persisted records participate in history queries.
    pub fn query_logs_filtered(
        &self,
        limit: usize,
        level: Option<&str>,
        layer: Option<&str>,
        component: Option<&str>,
        phase: Option<&str>,
        since_ts: Option<f64>,
        source_domain: Option<&str>,
    ) -> Result<Vec<LogRecord>, StorageError> {
        self.store.query_with_domain(
            limit,
            level,
            layer,
            component,
            phase,
            since_ts,
            source_domain,
        )
    }

    pub fn query_logs_page(&self, query: &LogQuery<'_>) -> Result<LogPage, StorageError> {
        self.store.query_page(query)
    }

    // --- Ergonomic Kernel Logging Helpers ---

    pub fn debug(&self, layer: &str, component: &str, message: impl Into<String>) {
        self.log_level("DEBUG", layer, component, message.into(), "");
    }

    pub fn info(&self, layer: &str, component: &str, message: impl Into<String>) {
        self.log_level("INFO", layer, component, message.into(), "");
    }

    pub fn warning(&self, layer: &str, component: &str, message: impl Into<String>) {
        self.log_level("WARNING", layer, component, message.into(), "");
    }

    pub fn error(&self, layer: &str, component: &str, message: impl Into<String>) {
        self.log_level("ERROR", layer, component, message.into(), "");
    }

    pub fn phase(
        &self,
        level: &str,
        layer: &str,
        component: &str,
        phase: &str,
        message: impl Into<String>,
    ) {
        self.log_level(level, layer, component, message.into(), phase);
    }

    pub fn progress(
        &self,
        layer: &str,
        component: &str,
        message: impl Into<String>,
        key: Option<String>,
        frac: Option<f64>,
        waiting: bool,
        done: bool,
    ) {
        self.reinject(LogRecord {
            source_domain: Some("kernel".to_string()),
            id: None,
            ts: now_secs_f64(),
            level: "INFO".to_string(),
            layer: layer.to_string(),
            component: component.to_string(),
            message: message.into(),
            phase: String::new(),
            console_only: false,
            ephemeral: true,
            key,
            frac,
            waiting,
            done,
            state: None,
        });
    }

    fn log_level(&self, level: &str, layer: &str, component: &str, message: String, phase: &str) {
        self.reinject(LogRecord {
            source_domain: Some("kernel".to_string()),
            id: None,
            ts: now_secs_f64(),
            level: level.to_string(),
            layer: layer.to_string(),
            component: component.to_string(),
            message,
            phase: phase.to_string(),
            console_only: false,
            ephemeral: false,
            key: None,
            frac: None,
            waiting: false,
            done: false,
            state: None,
        });
    }

    /// Formats a log record for console display with ANSI color codes.
    pub fn format_console(record: &LogRecord) -> String {
        let dt = chrono::DateTime::from_timestamp(
            record.ts as i64,
            ((record.ts.fract() * 1_000_000_000.0) as u32).min(999_999_999),
        )
        .unwrap_or_else(chrono::Utc::now);
        let time_str = dt.format("%H:%M:%S").to_string();

        let tag = if !record.layer.is_empty() || !record.component.is_empty() {
            format!(" [{}/{}]", record.layer, record.component)
        } else {
            String::new()
        };

        let phase_tag = if !record.phase.is_empty() {
            format!(" <{}>", record.phase)
        } else {
            String::new()
        };

        let (color, reset) = match record.level.to_uppercase().as_str() {
            "DEBUG" => ("\x1b[2m", "\x1b[0m"),
            "INFO" => ("\x1b[36m", "\x1b[0m"),
            "WARNING" | "WARN" => ("\x1b[33m", "\x1b[0m"),
            "ERROR" => ("\x1b[31m", "\x1b[0m"),
            "CRITICAL" => ("\x1b[41m", "\x1b[0m"),
            _ => ("", ""),
        };

        format!(
            "{time_str} {color}{:<8}{reset}{tag}{phase_tag} {}",
            record.level.to_uppercase(),
            record.message
        )
    }
}

#[cfg(test)]
mod subscriber_bounds_tests {
    use super::*;
    #[test]
    fn slow_log_subscriber_is_bounded_and_recovers_after_drain() {
        let hub = KernelLogHub::new(None).unwrap();
        let slow = hub.subscribe();
        let fast = hub.subscribe();
        for i in 0..10_000 {
            hub.broadcast(LogRecord {
                message: format!("event {i}"),
                ..LogRecord::default()
            });
            assert!(fast.try_recv().is_ok());
        }
        assert_eq!(slow.try_iter().count(), 128);
        hub.broadcast(LogRecord {
            message: "after-drain".into(),
            ..LogRecord::default()
        });
        assert_eq!(slow.try_recv().unwrap().message, "after-drain");
        // Large records may still be persisted; they must not multiply unboundedly per observer.
        hub.broadcast(LogRecord {
            message: "x".repeat(70_000),
            ..LogRecord::default()
        });
        assert!(slow.try_recv().is_err());
    }
}
#[cfg(test)]
mod persistence_independence_test {
    use super::*;
    #[test]
    fn full_live_subscriber_does_not_remove_successfully_persisted_history() {
        let hub = KernelLogHub::new(None).unwrap();
        let slow = hub.subscribe();
        for _ in 0..128 {
            hub.broadcast(LogRecord::default());
        }
        hub.reinject(LogRecord {
            message: "persisted-with-full-live-queue".into(),
            ..LogRecord::default()
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let rows = hub.query_logs(20, None, None, None, None, None).unwrap();
            if rows
                .iter()
                .any(|r| r.message == "persisted-with-full-live-queue")
            {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            !slow
                .try_iter()
                .any(|r| r.message == "persisted-with-full-live-queue")
        );
    }
}

#[cfg(test)]
mod progress_state_tests {
    use super::*;
    #[test]
    fn terminal_states_clear_before_broadcast_and_preserve_fraction() {
        for state in [
            ProgressState::Completed,
            ProgressState::Failed,
            ProgressState::Cancelled,
        ] {
            let hub = KernelLogHub::new(None).unwrap();
            let rx = hub.subscribe();
            let record = LogRecord {
                source_domain: Some("a".into()),
                ephemeral: true,
                key: Some("shared".into()),
                frac: Some(0.4),
                waiting: true,
                ..Default::default()
            };
            hub.reinject(record.clone());
            assert_eq!(rx.recv().unwrap().state, Some(ProgressState::Running));
            hub.reinject(LogRecord {
                source_domain: Some("b".into()),
                ..record.clone()
            });
            rx.recv().unwrap();
            hub.reinject(LogRecord {
                state: Some(state),
                ..record
            });
            let terminal = rx.recv().unwrap();
            assert_eq!(terminal.state, Some(state));
            assert!(terminal.done);
            assert!(!terminal.waiting);
            assert_eq!(terminal.frac, Some(0.4));
            let active = hub.active_progress();
            assert_eq!(active.len(), 1);
            assert_eq!(active[0].source_domain.as_deref(), Some("b"));
        }
    }
    #[test]
    fn legacy_done_and_explicit_running_precedence() {
        let hub = KernelLogHub::new(None).unwrap();
        let rx = hub.subscribe();
        let record = LogRecord {
            ephemeral: true,
            key: Some("k".into()),
            done: true,
            ..Default::default()
        };
        hub.reinject(LogRecord {
            state: Some(ProgressState::Running),
            ..record.clone()
        });
        assert!(!rx.recv().unwrap().done);
        assert_eq!(hub.active_progress().len(), 1);
        hub.reinject(record);
        assert_eq!(rx.recv().unwrap().state, Some(ProgressState::Completed));
        assert!(hub.active_progress().is_empty());
        assert!(
            serde_json::from_value::<LogRecord>(serde_json::json!({"state":"unknown"})).is_err()
        );
        assert!(
            serde_json::from_value::<LogRecord>(serde_json::json!({"done":true}))
                .unwrap()
                .state
                .is_none()
        );
    }
}
