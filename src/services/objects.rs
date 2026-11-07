//! ObjectHub —— C4 存储枢纽：逻辑路径 → 元数据 + 平铺物理文件的索引。
//!
//! 模型（见 docs/CONSTITUTION.md C4 / docs/kernel/C4_OBJECT_API_DESIGN.md）：
//!   - 业务域只用**逻辑路径**寻址；`object_id` 只在内核内部与物理文件名里出现。
//!   - 物理落盘：`<project_root>/<owner>/<object_id>`，平铺、无后缀；
//!     业务域的目录结构是虚拟的，只存在于本索引。
//!   - **先建索引、再原子替换**：`put` 先把行写进表，再把暂存文件 rename 到物理路径，
//!     因此域根里不会出现"有文件无索引"的孤儿；覆盖写复用同一 id/路径，不产生旧文件。
//!   - 元数据（size / file_type / file_mtime）由内核维护：`put` 时 stat，`get` 时再 stat 一次；
//!     **不读文件内容、不算哈希**。
//!   - 删除对象 = 删物理文件 + 删索引行，幂等；读取/查询/删除允许跨域，创建只写本域。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use parking_lot::Mutex;
use rusqlite::{Connection, params};

use crate::services::storage::StorageError;

const OBJECTS_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS objects(
  object_id TEXT PRIMARY KEY,owner TEXT NOT NULL,virtual_path TEXT NOT NULL,
  physical_path TEXT NOT NULL,size INTEGER NOT NULL DEFAULT 0,
  file_type TEXT NOT NULL DEFAULT '',file_mtime INTEGER NOT NULL DEFAULT 0,
  ref_count INTEGER NOT NULL DEFAULT 0,created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(owner,virtual_path));";

/// 业务域可见的文件对象（不含 `object_id`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileObject {
    pub owner: String,
    pub virtual_path: String,
    pub physical_path: String,
    pub size: u64,
    pub file_type: String,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Clone)]
struct Object {
    object_id: String,
    owner: String,
    virtual_path: String,
    physical_path: String,
    size: u64,
    file_type: String,
    file_mtime: u64,
    created_at: u64,
    updated_at: u64,
}

impl Object {
    fn view(&self) -> FileObject {
        FileObject {
            owner: self.owner.clone(),
            virtual_path: self.virtual_path.clone(),
            physical_path: self.physical_path.clone(),
            size: self.size,
            file_type: self.file_type.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetOutcome {
    Found(FileObject),
    /// 索引行在、物理文件消失：显式报错，不隐式删行。
    FileMissing,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    Unknown,
    UnlinkFailed(String),
}

pub struct ObjectHub {
    objects: BTreeMap<String, Object>,
    by_path: BTreeMap<(String, String), String>,
    next_id: u64,
    root: PathBuf,
    staging_root: PathBuf,
    db: Option<Arc<Mutex<Connection>>>,
}

/// 逻辑路径后缀 → MIME 类型（不读文件内容；未知回退 octet-stream）。
pub fn file_type_for(virtual_path: &str) -> String {
    let ext = Path::new(virtual_path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let mime = match ext.as_str() {
        "mp4" | "m4v" => "video/mp4",
        "mkv" => "video/x-matroska",
        "mov" => "video/quicktime",
        "webm" => "video/webm",
        "avi" => "video/x-msvideo",
        "flv" => "video/x-flv",
        "ts" => "video/mp2t",
        "mp3" => "audio/mpeg",
        "m4a" => "audio/mp4",
        "wav" => "audio/wav",
        "flac" => "audio/flac",
        "ogg" | "opus" => "audio/ogg",
        "aac" => "audio/aac",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "webp" => "image/webp",
        "gif" => "image/gif",
        "bmp" => "image/bmp",
        "srt" => "application/x-subrip",
        "vtt" => "text/vtt",
        "ass" | "ssa" => "text/x-ssa",
        "json" => "application/json",
        "txt" | "log" => "text/plain",
        "md" => "text/markdown",
        "html" => "text/html",
        "css" => "text/css",
        "js" | "mjs" => "text/javascript",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "gz" => "application/gzip",
        "tar" => "application/x-tar",
        "csv" => "text/csv",
        "xml" => "application/xml",
        _ => "application/octet-stream",
    };
    mime.to_string()
}

fn now_millis() -> u64 {
    crate::core::protocol::now_millis() as u64
}

fn mtime_millis(path: &Path) -> u64 {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl ObjectHub {
    pub fn new(project_root: PathBuf) -> Self {
        let staging_root = project_root.join(".staging");
        ObjectHub {
            objects: BTreeMap::new(),
            by_path: BTreeMap::new(),
            next_id: 1,
            root: project_root,
            staging_root,
            db: None,
        }
    }

    /// 从 SQLite 载入对象索引（跨重启存在），并做增量列迁移。
    pub fn with_db(project_root: PathBuf, db: Arc<Mutex<Connection>>) -> Result<Self, StorageError> {
        let staging_root = project_root.join(".staging");
        {
            let guard = db.lock();
            guard.execute_batch(OBJECTS_SCHEMA)?;
            let mut columns: Vec<String> = Vec::new();
            {
                let mut stmt = guard.prepare("PRAGMA table_info(objects)")?;
                let rows = stmt.query_map([], |r| r.get::<_, String>(1))?;
                for name in rows {
                    columns.push(name?);
                }
            }
            for (column, ddl) in [
                ("file_type", "ALTER TABLE objects ADD COLUMN file_type TEXT NOT NULL DEFAULT ''"),
                ("file_mtime", "ALTER TABLE objects ADD COLUMN file_mtime INTEGER NOT NULL DEFAULT 0"),
                (
                    "updated_at",
                    "ALTER TABLE objects ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0",
                ),
            ] {
                if !columns.iter().any(|c| c == column) {
                    guard.execute(ddl, [])?;
                }
            }
            guard.execute(
                "UPDATE objects SET updated_at=created_at WHERE updated_at=0",
                [],
            )?;
        }
        let mut hub = ObjectHub {
            objects: BTreeMap::new(),
            by_path: BTreeMap::new(),
            next_id: 1,
            root: project_root,
            staging_root,
            db: Some(db.clone()),
        };
        let rows = {
            let guard = db.lock();
            let mut stmt = guard.prepare(
                "SELECT object_id,owner,virtual_path,physical_path,size,file_type,file_mtime,created_at,updated_at FROM objects ORDER BY rowid",
            )?;
            stmt.query_map([], |r| {
                Ok(Object {
                    object_id: r.get(0)?,
                    owner: r.get(1)?,
                    virtual_path: r.get(2)?,
                    physical_path: r.get(3)?,
                    size: r.get::<_, i64>(4)? as u64,
                    file_type: r.get(5)?,
                    file_mtime: r.get::<_, i64>(6)? as u64,
                    created_at: r.get::<_, i64>(7)? as u64,
                    updated_at: r.get::<_, i64>(8)? as u64,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        for obj in rows {
            if let Some(n) = obj.object_id.strip_prefix("obj_").and_then(|s| s.parse::<u64>().ok()) {
                hub.next_id = hub.next_id.max(n + 1);
            }
            hub.by_path.insert(
                (obj.owner.clone(), obj.virtual_path.clone()),
                obj.object_id.clone(),
            );
            hub.objects.insert(obj.object_id.clone(), obj);
        }
        Ok(hub)
    }

    fn alloc_id(&mut self) -> String {
        let object_id = format!("obj_{}", self.next_id);
        self.next_id += 1;
        object_id
    }

    fn physical_for(&self, owner: &str, object_id: &str) -> PathBuf {
        self.root.join(owner).join(object_id)
    }

    /// 校验暂存文件：必须在 `<project_root>/.staging/<owner>/` 下且已存在。
    fn checked_staging(&self, owner: &str, staging_path: &str) -> Result<PathBuf, String> {
        let staging = Path::new(staging_path);
        if !staging.is_absolute() {
            return Err("object_invalid_staging: staging_path 必须是绝对路径".into());
        }
        let file = std::fs::canonicalize(staging)
            .map_err(|e| format!("object_source_missing: {e}"))?;
        if !file.is_file() {
            return Err("object_source_missing: 暂存路径不是文件".into());
        }
        let expected_root = self.staging_root.join(owner);
        let expected = std::fs::canonicalize(&expected_root)
            .map_err(|_| "object_invalid_staging: 暂存目录不存在".to_string())?;
        if !file.starts_with(&expected) {
            return Err("object_invalid_staging: 暂存文件不在本域暂存目录".into());
        }
        Ok(file)
    }

    fn persist_put(&self, obj: &Object) -> Result<(), String> {
        if let Some(db) = &self.db {
            db.lock()
                .execute(
                    "INSERT INTO objects(object_id,owner,virtual_path,physical_path,size,file_type,file_mtime,ref_count,created_at,updated_at) \
                     VALUES(?1,?2,?3,?4,?5,?6,?7,0,?8,?9) \
                     ON CONFLICT(owner,virtual_path) DO UPDATE SET physical_path=?4,size=?5,file_type=?6,file_mtime=?7,updated_at=?9",
                    params![
                        obj.object_id,
                        obj.owner,
                        obj.virtual_path,
                        obj.physical_path,
                        obj.size as i64,
                        obj.file_type,
                        obj.file_mtime as i64,
                        obj.created_at as i64,
                        obj.updated_at as i64
                    ],
                )
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn persist_meta(&self, object_id: &str, size: u64, file_mtime: u64, updated_at: u64) {
        if let Some(db) = &self.db {
            let _ = db.lock().execute(
                "UPDATE objects SET size=?1,file_mtime=?2,updated_at=?3 WHERE object_id=?4",
                params![size as i64, file_mtime as i64, updated_at as i64, object_id],
            );
        }
    }

    fn delete_row(&self, object_id: &str) {
        if let Some(db) = &self.db {
            let _ = db
                .lock()
                .execute("DELETE FROM objects WHERE object_id=?", [object_id]);
        }
    }

    /// 创建或覆盖：**先索引、后原子替换**，覆盖时复用同一 id/物理路径。
    pub fn put(
        &mut self,
        owner: &str,
        virtual_path: &str,
        staging_path: &str,
    ) -> Result<FileObject, String> {
        let staging = self.checked_staging(owner, staging_path)?;
        let existing = self
            .by_path
            .get(&(owner.to_string(), virtual_path.to_string()))
            .cloned();
        let created = existing.is_none();
        let object_id = match existing {
            Some(id) => id,
            None => self.alloc_id(),
        };
        let physical = self
            .objects
            .get(&object_id)
            .map(|o| PathBuf::from(&o.physical_path))
            .unwrap_or_else(|| self.physical_for(owner, &object_id));
        let now = now_millis();
        let file_type = file_type_for(virtual_path);
        if created {
            let obj = Object {
                object_id: object_id.clone(),
                owner: owner.to_string(),
                virtual_path: virtual_path.to_string(),
                physical_path: physical.to_string_lossy().to_string(),
                size: 0,
                file_type: file_type.clone(),
                file_mtime: 0,
                created_at: now,
                updated_at: now,
            };
            self.objects.insert(object_id.clone(), obj.clone());
            self.by_path
                .insert((owner.to_string(), virtual_path.to_string()), object_id.clone());
            self.persist_put(&obj)?;
        }
        if let Some(parent) = physical.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("object_rename_failed: {e}"))?;
        }
        if let Err(error) = std::fs::rename(&staging, &physical) {
            if created {
                self.objects.remove(&object_id);
                self.by_path
                    .remove(&(owner.to_string(), virtual_path.to_string()));
                self.delete_row(&object_id);
            }
            return Err(format!("object_rename_failed: {error}"));
        }
        let size = std::fs::metadata(&physical).map(|m| m.len()).unwrap_or(0);
        let file_mtime = mtime_millis(&physical);
        let updated_at = now_millis();
        let view = match self.objects.get_mut(&object_id) {
            Some(object) => {
                object.size = size;
                object.file_mtime = file_mtime;
                object.file_type = file_type;
                object.updated_at = updated_at;
                object.view()
            }
            None => return Err("object_put_failed: 索引行丢失".into()),
        };
        self.persist_meta(&object_id, size, file_mtime, updated_at);
        Ok(view)
    }

    /// 读取：命中后 stat 一次刷新 size/mtime；文件缺失返回 `FileMissing`（不删行）。
    pub fn get(&mut self, owner: &str, virtual_path: &str) -> GetOutcome {
        let Some(object_id) = self
            .by_path
            .get(&(owner.to_string(), virtual_path.to_string()))
            .cloned()
        else {
            return GetOutcome::Unknown;
        };
        let Some(object) = self.objects.get(&object_id) else {
            return GetOutcome::Unknown;
        };
        let physical = PathBuf::from(&object.physical_path);
        let Ok(meta) = std::fs::metadata(&physical) else {
            return GetOutcome::FileMissing;
        };
        let size = meta.len();
        let file_mtime = mtime_millis(&physical);
        if size != object.size || file_mtime != object.file_mtime {
            let updated_at = now_millis();
            if let Some(object) = self.objects.get_mut(&object_id) {
                object.size = size;
                object.file_mtime = file_mtime;
                object.updated_at = updated_at;
            }
            self.persist_meta(&object_id, size, file_mtime, updated_at);
        }
        match self.objects.get(&object_id) {
            Some(object) => GetOutcome::Found(object.view()),
            None => GetOutcome::Unknown,
        }
    }

    /// 列表：只读表（不逐文件 stat）。
    pub fn list(&self, owner: &str) -> Vec<FileObject> {
        self.objects
            .values()
            .filter(|o| o.owner == owner)
            .map(Object::view)
            .collect()
    }

    /// 删除：删文件 + 删行，幂等。
    pub fn delete(&mut self, owner: &str, virtual_path: &str) -> DeleteOutcome {
        let Some(object_id) = self
            .by_path
            .get(&(owner.to_string(), virtual_path.to_string()))
            .cloned()
        else {
            return DeleteOutcome::Unknown;
        };
        let Some(object) = self.objects.get(&object_id) else {
            return DeleteOutcome::Unknown;
        };
        match std::fs::remove_file(&object.physical_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return DeleteOutcome::UnlinkFailed(e.to_string()),
        }
        self.objects.remove(&object_id);
        self.by_path
            .remove(&(owner.to_string(), virtual_path.to_string()));
        self.delete_row(&object_id);
        DeleteOutcome::Deleted
    }

    pub fn len(&self) -> usize {
        self.objects.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("atelier-obj-{tag}-{}", uuid::Uuid::new_v4()))
    }

    fn staging(root: &Path, owner: &str, name: &str, bytes: &[u8]) -> String {
        let dir = root.join(".staging").join(owner);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        path.to_string_lossy().to_string()
    }

    #[test]
    fn put_allocates_flat_id_and_records_metadata() {
        let root = temp_root("put");
        std::fs::create_dir_all(root.join("engines")).unwrap();
        let mut hub = ObjectHub::new(root.clone());
        let src = staging(&root, "engines", "a.bin", b"hello");
        let obj = hub.put("engines", "out/video.bin", &src).unwrap();
        assert_eq!(obj.virtual_path, "out/video.bin");
        assert!(obj
            .physical_path
            .starts_with(&root.join("engines").to_string_lossy().to_string()));
        assert_eq!(obj.size, 5);
        assert_eq!(obj.file_type, "application/octet-stream");
        assert!(!Path::new(&src).exists(), "staging moved into the object file");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn overwrite_reuses_the_same_object_path() {
        let root = temp_root("overwrite");
        std::fs::create_dir_all(root.join("engines")).unwrap();
        let mut hub = ObjectHub::new(root.clone());
        let first = hub
            .put("engines", "a.mp4", &staging(&root, "engines", "1", b"old"))
            .unwrap();
        let second = hub
            .put("engines", "a.mp4", &staging(&root, "engines", "2", b"new-content"))
            .unwrap();
        assert_eq!(first.physical_path, second.physical_path);
        assert_eq!(second.size, 11);
        assert_eq!(second.file_type, "video/mp4");
        assert_eq!(std::fs::read(&second.physical_path).unwrap(), b"new-content");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn get_refreshes_size_after_external_write() {
        let root = temp_root("refresh");
        std::fs::create_dir_all(root.join("engines")).unwrap();
        let mut hub = ObjectHub::new(root.clone());
        let obj = hub
            .put("engines", "a.bin", &staging(&root, "engines", "1", b"abc"))
            .unwrap();
        std::fs::write(&obj.physical_path, b"abcdef").unwrap();
        match hub.get("engines", "a.bin") {
            GetOutcome::Found(fresh) => assert_eq!(fresh.size, 6),
            other => panic!("unexpected {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn missing_file_reports_without_dropping_the_row() {
        let root = temp_root("missing");
        std::fs::create_dir_all(root.join("engines")).unwrap();
        let mut hub = ObjectHub::new(root.clone());
        let obj = hub
            .put("engines", "a.bin", &staging(&root, "engines", "1", b"x"))
            .unwrap();
        std::fs::remove_file(&obj.physical_path).unwrap();
        assert_eq!(hub.get("engines", "a.bin"), GetOutcome::FileMissing);
        assert_eq!(hub.list("engines").len(), 1, "row kept");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn delete_removes_file_and_row_idempotently() {
        let root = temp_root("delete");
        std::fs::create_dir_all(root.join("engines")).unwrap();
        let mut hub = ObjectHub::new(root.clone());
        let obj = hub
            .put("engines", "a.bin", &staging(&root, "engines", "1", b"x"))
            .unwrap();
        assert_eq!(hub.delete("engines", "a.bin"), DeleteOutcome::Deleted);
        assert!(!Path::new(&obj.physical_path).exists());
        assert!(hub.list("engines").is_empty());
        assert_eq!(hub.delete("engines", "a.bin"), DeleteOutcome::Unknown);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn staging_must_live_in_the_owners_staging_dir() {
        let root = temp_root("staging");
        std::fs::create_dir_all(root.join("engines")).unwrap();
        let mut hub = ObjectHub::new(root.clone());
        let rogue = staging(&root, "other", "1", b"x");
        assert!(hub.put("engines", "a.bin", &rogue).is_err());
        assert!(hub.put("engines", "a.bin", "/nonexistent/file").is_err());
        assert!(hub.list("engines").is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn index_survives_restart() {
        let root = temp_root("restart");
        std::fs::create_dir_all(root.join("engines")).unwrap();
        let db = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let (physical, size) = {
            let mut hub = ObjectHub::with_db(root.clone(), db.clone()).unwrap();
            let obj = hub
                .put("engines", "a/b.bin", &staging(&root, "engines", "1", b"data"))
                .unwrap();
            (obj.physical_path, obj.size)
        };
        let mut hub2 = ObjectHub::with_db(root.clone(), db).unwrap();
        match hub2.get("engines", "a/b.bin") {
            GetOutcome::Found(obj) => {
                assert_eq!(obj.physical_path, physical);
                assert_eq!(obj.size, size);
            }
            other => panic!("unexpected {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }
}
