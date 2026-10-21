//! Durable append buffer independent from the application's SQLite writer lock.
use crate::{logging::LogRecord, storage::StorageError};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub(crate) const CURSOR_COLUMNS: &[&str] = &["source_id", "last_seq"];
pub(crate) struct LogSpool {
    db: Mutex<Connection>,
    source: String,
    accepted: AtomicU64,
    imported: AtomicU64,
    error: Mutex<Option<String>>,
    append_failure: Mutex<Option<String>>,
}
impl LogSpool {
    pub fn open(path: Option<&Path>, target: &Connection) -> Result<Arc<Self>, StorageError> {
        let db = match path {
            Some(p) => {
                let mut name = p.as_os_str().to_os_string();
                name.push(".log-spool.sqlite");
                Connection::open(std::path::PathBuf::from(name))?
            }
            None => Connection::open_in_memory()?,
        };
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
            CREATE TABLE IF NOT EXISTS log_spool_metadata(id INTEGER PRIMARY KEY,source_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS pending_logs(seq INTEGER PRIMARY KEY AUTOINCREMENT,record TEXT NOT NULL);")?;
        crate::storage::verify_sqlite_table_columns(
            &db,
            "log_spool_metadata",
            &["id", "source_id"],
        )?;
        crate::storage::verify_sqlite_table_columns(&db, "pending_logs", &["seq", "record"])?;
        let existing: Option<String> = db
            .query_row(
                "SELECT source_id FROM log_spool_metadata WHERE id=1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if existing.is_none()
            && db.query_row("SELECT count(*) FROM pending_logs", [], |r| {
                r.get::<_, i64>(0)
            })? > 0
        {
            return Err(StorageError::LogPersistence(
                "pending journal has no source identity; refusing unsafe replay".into(),
            ));
        }
        let source = existing.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        db.execute(
            "INSERT OR IGNORE INTO log_spool_metadata(id,source_id) VALUES(1,?1)",
            [&source],
        )?;
        target.execute_batch("CREATE TABLE IF NOT EXISTS kernel_log_import(source_id TEXT PRIMARY KEY,last_seq INTEGER NOT NULL);")?;
        crate::storage::verify_sqlite_table_columns(target, "kernel_log_import", CURSOR_COLUMNS)?;
        let imported: u64 = target
            .query_row(
                "SELECT last_seq FROM kernel_log_import WHERE source_id=?1",
                [&source],
                |r| Ok(r.get::<_, i64>(0)? as u64),
            )
            .optional()?
            .unwrap_or(0);
        let accepted: u64 =
            db.query_row("SELECT coalesce(max(seq),0) FROM pending_logs", [], |r| {
                Ok(r.get::<_, i64>(0)? as u64)
            })?;
        Ok(Arc::new(Self {
            db: Mutex::new(db),
            source,
            accepted: AtomicU64::new(accepted.max(imported)),
            imported: AtomicU64::new(imported),
            error: Mutex::new(None),
            append_failure: Mutex::new(None),
        }))
    }
    pub fn append(&self, record: &LogRecord) -> Result<(), StorageError> {
        let text = serde_json::to_string(record)?;
        let db = self.db.lock();
        db.execute("INSERT INTO pending_logs(record) VALUES(?1)", [text])?;
        self.accepted
            .store(db.last_insert_rowid() as u64, Ordering::Release);
        Ok(())
    }
    pub fn diagnostic(&self, error: impl ToString) {
        let text = error.to_string();
        let mut current = self.error.lock();
        if current.as_ref() != Some(&text) {
            eprintln!("kernel log persistence error: {text}");
        }
        *current = Some(text);
    }
    pub fn error(&self) -> Option<String> {
        self.append_failure
            .lock()
            .clone()
            .or_else(|| self.error.lock().clone())
    }
    pub fn append_failed(&self, error: impl ToString) {
        *self.append_failure.lock() = Some(error.to_string());
        self.diagnostic(error);
    }
    pub fn import_batch(&self, target: &mut Connection) -> Result<bool, StorageError> {
        // At most 200 records / approximately 2 MiB, plus one possibly oversized record.
        let batch: Vec<(u64, LogRecord)> = {
            let db = self.db.lock();
            let mut query =
                db.prepare("SELECT seq,record FROM pending_logs ORDER BY seq LIMIT 200")?;
            let mut rows = query.query([])?;
            let mut batch = Vec::new();
            let mut bytes = 0;
            while let Some(row) = rows.next()? {
                let seq = row.get::<_, i64>(0)? as u64;
                let text: String = row.get(1)?;
                bytes += text.len();
                batch.push((seq, serde_json::from_str(&text)?));
                if bytes >= 2 * 1024 * 1024 {
                    break;
                }
            }
            batch
        };
        if batch.is_empty() {
            return Ok(false);
        }
        let last = batch.last().unwrap().0;
        let tx = target.transaction()?;
        let cursor: u64 = tx
            .query_row(
                "SELECT last_seq FROM kernel_log_import WHERE source_id=?1",
                [&self.source],
                |r| Ok(r.get::<_, i64>(0)? as u64),
            )
            .optional()?
            .unwrap_or(0);
        {
            let mut insert=tx.prepare_cached("INSERT INTO hub_logs(ts,level,layer,component,message,phase,source_domain) VALUES(?1,?2,?3,?4,?5,?6,?7)")?;
            for (seq, r) in &batch {
                if *seq > cursor {
                    insert.execute(params![
                        r.ts,
                        r.level,
                        r.layer,
                        r.component,
                        r.message,
                        r.phase,
                        r.source_domain
                    ])?;
                }
            }
        }
        let committed = last.max(cursor);
        tx.execute("INSERT INTO kernel_log_import(source_id,last_seq) VALUES(?1,?2) ON CONFLICT(source_id) DO UPDATE SET last_seq=excluded.last_seq",params![self.source,committed as i64])?;
        tx.commit()?;
        // Publish actual hub_logs commit before cleanup. If cleanup fails, replay consults cursor.
        self.imported.store(committed, Ordering::Release);
        self.db
            .lock()
            .execute("DELETE FROM pending_logs WHERE seq<=?1", [committed as i64])?;
        *self.error.lock() = None;
        Ok(true)
    }
    pub fn flush(&self, timeout: Duration) -> Result<(), StorageError> {
        let target = self.accepted.load(Ordering::Acquire);
        let deadline = Instant::now() + timeout;
        while self.imported.load(Ordering::Acquire) < target {
            if Instant::now() >= deadline {
                return Err(StorageError::LogPersistence(format!(
                    "flush timed out before importing sequence {target}: {}",
                    self.error()
                        .unwrap_or_else(|| "writer still pending".into())
                )));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        if let Some(error) = self.append_failure.lock().clone() {
            return Err(StorageError::LogPersistence(error));
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn target() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE TABLE hub_logs(ts REAL,level TEXT,layer TEXT,component TEXT,message TEXT,phase TEXT,source_domain TEXT);").unwrap();
        c
    }
    fn record(text: &str) -> LogRecord {
        LogRecord {
            message: text.into(),
            ..LogRecord::default()
        }
    }
    #[test]
    fn failed_batch_rolls_back_and_keeps_all_records_for_retry() {
        let mut c = target();
        let spool = LogSpool::open(None, &c).unwrap();
        c.execute_batch("CREATE TRIGGER reject_record BEFORE INSERT ON hub_logs WHEN NEW.message='bad' BEGIN SELECT RAISE(FAIL,'injected insert failure'); END;").unwrap();
        for text in ["first", "bad", "last"] {
            spool.append(&record(text)).unwrap();
        }
        assert!(spool.import_batch(&mut c).is_err());
        assert_eq!(
            c.query_row("SELECT count(*) FROM hub_logs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            spool
                .db
                .lock()
                .query_row("SELECT count(*) FROM pending_logs", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            3
        );
        c.execute_batch("DROP TRIGGER reject_record;").unwrap();
        spool.import_batch(&mut c).unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM hub_logs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3
        );
        spool.flush(Duration::ZERO).unwrap();
    }
    #[test]
    fn committed_batch_replayed_after_cleanup_failure_is_not_duplicated() {
        let mut c = target();
        let spool = LogSpool::open(None, &c).unwrap();
        spool.append(&record("once")).unwrap();
        spool.db.lock().execute_batch("CREATE TRIGGER reject_cleanup BEFORE DELETE ON pending_logs BEGIN SELECT RAISE(FAIL,'injected cleanup failure'); END;").unwrap();
        assert!(spool.import_batch(&mut c).is_err());
        assert_eq!(
            c.query_row("SELECT count(*) FROM hub_logs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        spool
            .db
            .lock()
            .execute_batch("DROP TRIGGER reject_cleanup;")
            .unwrap();
        spool.import_batch(&mut c).unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM hub_logs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
    #[test]
    fn failed_append_is_explicit_and_never_acknowledged_as_accepted() {
        let c = target();
        let spool = LogSpool::open(None, &c).unwrap();
        spool.db.lock().execute_batch("CREATE TRIGGER reject_append BEFORE INSERT ON pending_logs BEGIN SELECT RAISE(FAIL,'injected disk write failure'); END;").unwrap();
        assert!(spool.append(&record("not accepted")).is_err());
        assert_eq!(spool.accepted.load(Ordering::Acquire), 0);
    }
}
