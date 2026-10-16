use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use rusqlite::{params, Connection};
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
    /// True when the ephemeral task is finished (signals UI to complete/clear the progress row).
    #[serde(default)]
    pub done: bool,
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

/// Asynchronous, bounded batch persistence store for `hub_logs` SQLite table.
///
/// Follows the black-box logging guarantee:
/// - Write operations never block the calling domain or kernel router.
/// - Saturated write queue drops records gracefully rather than stalling business processes.
/// - Self-managed WAL connection isolated from domain storage transactions.
pub struct LogStore {
    tx: SyncSender<LogRecord>,
    stop: Arc<AtomicBool>,
    writer_handle: Option<thread::JoinHandle<()>>,
    query_conn: Mutex<Connection>,
}

impl LogStore {
    /// Initializes `LogStore` backed by a file path or in-memory SQLite.
    pub fn new(db_path: Option<&Path>) -> Result<Self, StorageError> {
        let (conn_writer, conn_reader) = match db_path {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                }
                let w = Connection::open(path)?;
                let r = Connection::open(path)?;
                w.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
                r.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
                (w, r)
            }
            None => {
                let w = Connection::open_in_memory()?;
                // In-memory mode: use shared cache or clone DDL
                let r = Connection::open_in_memory()?;
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
                phase TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_{LOGS_TABLE}_ts ON {LOGS_TABLE}(ts);
            CREATE INDEX IF NOT EXISTS idx_{LOGS_TABLE}_level ON {LOGS_TABLE}(level);
            CREATE INDEX IF NOT EXISTS idx_{LOGS_TABLE}_layer ON {LOGS_TABLE}(layer);"
        );
        conn_writer.execute_batch(&ddl)?;
        conn_reader.execute_batch(&ddl)?;

        // Additive migration; preserve unknown origins of pre-upgrade rows.
        for conn in [&conn_writer, &conn_reader] {
            let present = {
                let mut stmt = conn.prepare("PRAGMA table_info(hub_logs)")?;
                let names = stmt.query_map([], |row| row.get::<_, String>(1))?;
                names.collect::<Result<Vec<_>, _>>()?.iter().any(|n| n == "source_domain")
            };
            if !present {
                conn.execute_batch("ALTER TABLE hub_logs ADD COLUMN source_domain TEXT;")?;
            }
        }

        let (tx, rx) = mpsc::sync_channel::<LogRecord>(10_000);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);

        let writer_handle = thread::Builder::new()
            .name("hub-logstore-writer".to_string())
            .spawn(move || {
                Self::writer_loop(conn_writer, rx, stop_clone);
            })
            .ok();

        Ok(Self {
            tx,
            stop,
            writer_handle,
            query_conn: Mutex::new(conn_reader),
        })
    }

    /// Emits a record to the bounded SQLite persistence queue.
    /// Non-blocking: returns immediately; drops records if queue capacity (10,000) is reached.
    pub fn emit(&self, record: LogRecord) {
        if record.console_only || record.ephemeral {
            return;
        }
        let _ = self.tx.try_send(record);
    }

    /// Flushes any pending log records and waits for persistence to finish.
    pub fn flush(&self, timeout: Duration) {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            // Check if queue has drained
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn writer_loop(
        mut conn: Connection,
        rx: Receiver<LogRecord>,
        stop: Arc<AtomicBool>,
    ) {
        let batch_size = 200;
        let flush_interval = Duration::from_millis(200);
        let mut buffer: Vec<LogRecord> = Vec::with_capacity(batch_size);
        let mut last_flush = std::time::Instant::now();

        loop {
            let timeout = flush_interval
                .checked_sub(last_flush.elapsed())
                .unwrap_or(Duration::from_millis(10));

            match rx.recv_timeout(timeout) {
                Ok(rec) => {
                    buffer.push(rec);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }

            let should_flush = buffer.len() >= batch_size
                || last_flush.elapsed() >= flush_interval
                || (stop.load(Ordering::SeqCst) && !buffer.is_empty());

            if should_flush && !buffer.is_empty() {
                if let Ok(tx) = conn.transaction() {
                    let insert_res = {
                        let stmt = tx.prepare_cached(
                            &format!(
                                "INSERT INTO {LOGS_TABLE} (ts, level, layer, component, message, phase, source_domain)
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
                            )
                        );
                        if let Ok(mut statement) = stmt {
                            for rec in &buffer {
                                let _ = statement.execute(params![
                                    rec.ts,
                                    rec.level,
                                    rec.layer,
                                    rec.component,
                                    rec.message,
                                    rec.phase,
                                    rec.source_domain,
                                ]);
                            }
                            Ok(())
                        } else {
                            Err(())
                        }
                    };
                    if insert_res.is_ok() {
                        let _ = tx.commit();
                    } else {
                        let _ = tx.rollback();
                    }
                }
                buffer.clear();
                last_flush = std::time::Instant::now();
            }

            if stop.load(Ordering::SeqCst) && rx.try_recv().is_err() && buffer.is_empty() {
                break;
            }
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

        sql.push_str(" ORDER BY id DESC LIMIT ?");
        params_vec.push(Box::new(limit.clamp(1, 2000) as i64));

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
            })
        })?;

        let mut records = Vec::new();
        for r in rows {
            records.push(r?);
        }
        Ok(records)
    }
}

impl Drop for LogStore {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
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
    subscribers: Mutex<Vec<Sender<LogRecord>>>,
    recent_logs: Mutex<Vec<LogRecord>>,
    active_progress: Mutex<HashMap<String, LogRecord>>,
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
        let (tx, rx) = mpsc::channel();
        self.subscribers.lock().push(tx);
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
            if let Some(key) = &record.key {
                let mut progress_map = self.active_progress.lock();
                if record.done {
                    progress_map.remove(key);
                } else {
                    progress_map.insert(key.clone(), record.clone());
                }
            }
            self.broadcast(record);
            return;
        }

        // 3. Regular Structured Log: recent ring buffer + SSE broadcast + SQLite persistence
        {
            let mut recent = self.recent_logs.lock();
            if recent.len() >= 500 {
                recent.remove(0);
            }
            recent.push(record.clone());
        }

        self.broadcast(record.clone());
        self.store.emit(record);
    }

    fn broadcast(&self, record: LogRecord) {
        let mut subs = self.subscribers.lock();
        subs.retain(|tx| tx.send(record.clone()).is_ok());
    }

    /// Returns a snapshot of currently active in-flight progress records.
    pub fn active_progress(&self) -> Vec<LogRecord> {
        self.active_progress.lock().values().cloned().collect()
    }

    /// Returns a snapshot of recent in-memory structured logs.
    pub fn recent_logs(&self) -> Vec<LogRecord> {
        self.recent_logs.lock().clone()
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

    pub fn phase(&self, level: &str, layer: &str, component: &str, phase: &str, message: impl Into<String>) {
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
