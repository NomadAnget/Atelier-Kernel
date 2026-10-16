use std::path::Path;

use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use crate::protocol::{
    KernelMessage, MSG_STORAGE_DELETE, MSG_STORAGE_INSERT, MSG_STORAGE_QUERY, MSG_STORAGE_UPDATE,
    now_millis,
};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("invalid domain identifier for table creation: {0}")]
    InvalidDomainName(String),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported storage message_type '{0}'")]
    UnsupportedStorageOperation(String),
    #[error("missing required parameter '{0}' for storage operation")]
    MissingParameter(&'static str),
    #[error("record '{0}' already exists in domain '{1}'")]
    DuplicateRecord(String, String),
}

/// Stored row representation returned by `StorageQuery`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRecord {
    pub record_id: String,
    pub message_id: String,
    pub category: String,
    pub message_type: String,
    pub params: Value,
    pub data: Value,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Per-Domain SQLite Storage Engine implementing "Storage as Messaging".
///
/// Upper-layer contract metadata defines the fixed table structure (`store_<domain>`),
/// while the inner `data` packet is stored as standard JSON so business data structure
/// evolution never alters kernel behavior or database schema.
pub struct DomainStorageEngine {
    conn: Mutex<Connection>,
}

impl DomainStorageEngine {
    /// Opens an in-memory SQLite storage engine.
    pub fn new_in_memory() -> Result<Self, StorageError> {
        let conn = Connection::open_in_memory()?;
        Self::init_connection(conn)
    }

    /// Opens a file-backed SQLite storage engine at `db_path`.
    pub fn open_file(db_path: impl AsRef<Path>) -> Result<Self, StorageError> {
        if let Some(parent) = db_path.as_ref().parent()
            && !parent.as_os_str().is_empty()
        {
            let _ = std::fs::create_dir_all(parent);
        }
        let conn = Connection::open(db_path)?;
        Self::init_connection(conn)
    }

    fn init_connection(conn: Connection) -> Result<Self, StorageError> {
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Ensures the dedicated storage table `store_<domain>` exists for `domain`.
    pub fn ensure_domain_table(&self, domain: &str) -> Result<String, StorageError> {
        let table = Self::table_name(domain)?;
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                record_id    TEXT PRIMARY KEY,
                message_id   TEXT NOT NULL,
                category     TEXT NOT NULL,
                message_type TEXT NOT NULL,
                params       TEXT NOT NULL,
                data         TEXT NOT NULL,
                created_at   INTEGER NOT NULL,
                updated_at   INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_{table}_category ON {table}(category);
            CREATE INDEX IF NOT EXISTS idx_{table}_msg_id ON {table}(message_id);"
        );
        self.conn.lock().execute_batch(&ddl)?;
        Ok(table)
    }

    /// Executes a CRUD message sent to `kernel.storage` against the authenticated `msg.source_domain` table.
    pub fn execute_message(&self, msg: &KernelMessage) -> Result<Value, StorageError> {
        let domain = &msg.source_domain;
        let table = self.ensure_domain_table(domain)?;

        match msg.message_type.as_str() {
            MSG_STORAGE_INSERT => self.handle_insert(domain, &table, msg),
            MSG_STORAGE_QUERY => self.handle_query(&table, msg),
            MSG_STORAGE_UPDATE => self.handle_update(&table, msg),
            MSG_STORAGE_DELETE => self.handle_delete(&table, msg),
            other => Err(StorageError::UnsupportedStorageOperation(other.to_string())),
        }
    }

    fn handle_insert(
        &self,
        domain: &str,
        table: &str,
        msg: &KernelMessage,
    ) -> Result<Value, StorageError> {
        let record_id = msg
            .params
            .get("record_id")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(&msg.message_id);

        let category = msg
            .params
            .get("category")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(&msg.message_type);

        let upsert = msg
            .params
            .get("upsert")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let params_str = serde_json::to_string(&msg.params)?;
        let data_str = serde_json::to_string(&msg.data)?;
        let now = now_millis();

        let conn = self.conn.lock();

        // Check existing record for idempotency on `message_id` or `record_id`.
        let existing_msg_id: Option<String> = conn
            .query_row(
                &format!("SELECT message_id FROM {table} WHERE record_id = ?1"),
                params![record_id],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(prev_msg_id) = existing_msg_id {
            if prev_msg_id == msg.message_id {
                return Ok(json!({
                    "domain": domain,
                    "record_id": record_id,
                    "inserted": false,
                    "idempotent_replay": true,
                }));
            }
            if !upsert {
                return Err(StorageError::DuplicateRecord(
                    record_id.to_string(),
                    domain.to_string(),
                ));
            }
            conn.execute(
                &format!(
                    "UPDATE {table}
                     SET message_id = ?1, category = ?2, message_type = ?3, params = ?4, data = ?5, updated_at = ?6
                     WHERE record_id = ?7"
                ),
                params![
                    msg.message_id,
                    category,
                    msg.message_type,
                    params_str,
                    data_str,
                    now,
                    record_id
                ],
            )?;
            return Ok(json!({
                "domain": domain,
                "record_id": record_id,
                "inserted": false,
                "upserted": true,
            }));
        }

        conn.execute(
            &format!(
                "INSERT INTO {table} (record_id, message_id, category, message_type, params, data, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
            ),
            params![
                record_id,
                msg.message_id,
                category,
                msg.message_type,
                params_str,
                data_str,
                now,
                now
            ],
        )?;

        Ok(json!({
            "domain": domain,
            "record_id": record_id,
            "category": category,
            "inserted": true,
        }))
    }

    fn handle_query(&self, table: &str, msg: &KernelMessage) -> Result<Value, StorageError> {
        let record_id = msg.params.get("record_id").and_then(Value::as_str);
        let category = msg.params.get("category").and_then(Value::as_str);
        let filter = msg.params.get("filter").and_then(Value::as_object);
        let limit = msg
            .params
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(500) as usize;

        let conn = self.conn.lock();
        let mut stmt = conn.prepare(&format!(
            "SELECT record_id, message_id, category, message_type, params, data, created_at, updated_at
             FROM {table}
             ORDER BY created_at ASC, record_id ASC"
        ))?;

        let rows = stmt.query_map([], |row| {
            let params_raw: String = row.get(4)?;
            let data_raw: String = row.get(5)?;
            Ok(StoredRecord {
                record_id: row.get(0)?,
                message_id: row.get(1)?,
                category: row.get(2)?,
                message_type: row.get(3)?,
                params: serde_json::from_str(&params_raw).unwrap_or(Value::Null),
                data: serde_json::from_str(&data_raw).unwrap_or(Value::Null),
                created_at: row.get(6)?,
                updated_at: row.get(7)?,
            })
        })?;

        let mut matched = Vec::new();
        for item in rows {
            let record = item?;
            if let Some(rid) = record_id
                && record.record_id != rid
            {
                continue;
            }
            if let Some(cat) = category
                && record.category != cat
            {
                continue;
            }
            if let Some(filter_map) = filter
                && !matches_json_filter(&record, filter_map)
            {
                continue;
            }
            matched.push(record);
            if matched.len() >= limit {
                break;
            }
        }

        Ok(json!({
            "count": matched.len(),
            "items": matched,
        }))
    }

    fn handle_update(&self, table: &str, msg: &KernelMessage) -> Result<Value, StorageError> {
        let record_id = msg
            .params
            .get("record_id")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .ok_or(StorageError::MissingParameter("record_id"))?;

        let patch_mode = msg
            .params
            .get("patch")
            .and_then(Value::as_bool)
            .unwrap_or(true);

        let conn = self.conn.lock();
        let existing_data: Option<String> = conn
            .query_row(
                &format!("SELECT data FROM {table} WHERE record_id = ?1"),
                params![record_id],
                |row| row.get(0),
            )
            .optional()?;

        let Some(existing_raw) = existing_data else {
            return Ok(json!({
                "record_id": record_id,
                "updated_count": 0,
            }));
        };

        let mut final_data: Value = serde_json::from_str(&existing_raw).unwrap_or(Value::Null);
        if patch_mode {
            merge_json_objects(&mut final_data, &msg.data);
        } else {
            final_data = msg.data.clone();
        }

        let new_data_str = serde_json::to_string(&final_data)?;
        let now = now_millis();

        let updated = conn.execute(
            &format!(
                "UPDATE {table}
                 SET message_id = ?1, data = ?2, updated_at = ?3
                 WHERE record_id = ?4"
            ),
            params![msg.message_id, new_data_str, now, record_id],
        )?;

        Ok(json!({
            "record_id": record_id,
            "updated_count": updated,
            "data": final_data,
        }))
    }

    fn handle_delete(&self, table: &str, msg: &KernelMessage) -> Result<Value, StorageError> {
        let record_id = msg.params.get("record_id").and_then(Value::as_str);
        let category = msg.params.get("category").and_then(Value::as_str);

        let conn = self.conn.lock();
        let deleted = if let Some(rid) = record_id {
            conn.execute(
                &format!("DELETE FROM {table} WHERE record_id = ?1"),
                params![rid],
            )?
        } else if let Some(cat) = category {
            conn.execute(
                &format!("DELETE FROM {table} WHERE category = ?1"),
                params![cat],
            )?
        } else {
            return Err(StorageError::MissingParameter("record_id or category"));
        };

        Ok(json!({
            "deleted_count": deleted,
        }))
    }

    fn table_name(domain: &str) -> Result<String, StorageError> {
        if domain.is_empty()
            || !domain
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(StorageError::InvalidDomainName(domain.to_string()));
        }
        Ok(format!("store_{domain}"))
    }
}

fn matches_json_filter(record: &StoredRecord, filter: &serde_json::Map<String, Value>) -> bool {
    for (k, expected) in filter {
        let in_data = record.data.get(k) == Some(expected);
        let in_params = record.params.get(k) == Some(expected);
        if !in_data && !in_params {
            return false;
        }
    }
    true
}

fn merge_json_objects(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(t_map), Value::Object(p_map)) => {
            for (k, v) in p_map {
                merge_json_objects(t_map.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (t, p) => {
            *t = p.clone();
        }
    }
}
