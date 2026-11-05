//! ObjectHub —— C4 存储枢纽对象索引：一个「绝对路径索引」。
//!
//! 大文件不进管道；对象就是域自己磁盘上的一个文件。内核**不复制、不移动、不接管**物理文件，
//! 只把文件的绝对路径 + size + checksum 记进 `objects` 索引表，并把「文件对象」（即该绝对路径）
//! 交给接收域当普通文件使用（Python 的 `open(path)`、Node 的 `fs.createReadStream(path)`）。
//! 消息 `data` 里只带 `object_id` 引用，字节不进管道。
//!
//! 契约（见 CONSTITUTION.md C4）：
//!   - 写权随域（M3）：只有归属域能删索引行；跨域读取只读；
//!   - 无 TTL、无硬配额；索引删除时机由业务域显式决定；
//!   - `checksum` 由内核流式计算（SHA-256，常数内存）；
//!   - 引用未清零前不得删索引；
//!   - 物理文件的生命周期归业务域：内核删索引不删文件。

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::{Connection, params};
use sha2::{Digest, Sha256};

use crate::storage::StorageError;

const OBJECTS_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS objects(
  object_id TEXT PRIMARY KEY,owner TEXT NOT NULL,path TEXT NOT NULL,
  size INTEGER NOT NULL,checksum TEXT NOT NULL,ref_count INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL);";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object {
    pub object_id: String,
    pub owner: String,
    /// 域文件系统中的绝对路径；内核只索引，不拥有该文件。
    pub path: String,
    pub size: u64,
    pub checksum: String,
    pub ref_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRef {
    pub object_id: String,
    pub size: u64,
    pub checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    Forbidden,  // 非归属域
    Referenced, // 引用未清零
    Unknown,
}

pub struct ObjectHub {
    objects: BTreeMap<String, Object>,
    next_id: u64,
    db: Option<Arc<Mutex<Connection>>>,
}

impl Default for ObjectHub {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectHub {
    pub fn new() -> Self {
        ObjectHub {
            objects: BTreeMap::new(),
            next_id: 1,
            db: None,
        }
    }

    /// 从 SQLite 载入对象索引（跨重启存在）。
    pub fn with_db(db: Arc<Mutex<Connection>>) -> Result<Self, StorageError> {
        db.lock().execute_batch(OBJECTS_SCHEMA)?;
        let mut hub = ObjectHub {
            objects: BTreeMap::new(),
            next_id: 1,
            db: Some(db.clone()),
        };
        let rows = {
            let guard = db.lock();
            let mut stmt = guard.prepare(
                "SELECT object_id,owner,path,size,checksum,ref_count FROM objects ORDER BY rowid",
            )?;
            stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)? as u64,
                    r.get::<_, String>(4)?,
                    r.get::<_, i64>(5)? as u64,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        for (object_id, owner, path, size, checksum, ref_count) in rows {
            if let Some(n) = object_id.strip_prefix("obj_").and_then(|s| s.parse::<u64>().ok()) {
                hub.next_id = hub.next_id.max(n + 1);
            }
            hub.objects.insert(
                object_id.clone(),
                Object { object_id, owner, path, size, checksum, ref_count },
            );
        }
        Ok(hub)
    }

    fn alloc_id(&mut self) -> String {
        let object_id = format!("obj_{}", self.next_id);
        self.next_id += 1;
        object_id
    }

    /// 登记一个域文件：只索引绝对路径，不复制、不移动；checksum 流式计算（常数内存）。
    /// 返回的 `object_id` 就是域可当作普通文件使用的「文件对象」引用。
    pub fn register_file(&mut self, owner: &str, source_path: &Path) -> Result<ObjectRef, String> {
        let absolute = source_path
            .canonicalize()
            .map_err(|e| format!("object path must exist and be addressable: {e}"))?;
        let (size, checksum) = hash_file(&absolute)?;
        let object_id = self.alloc_id();
        let obj = Object {
            object_id: object_id.clone(),
            owner: owner.to_string(),
            path: absolute.to_string_lossy().to_string(),
            size,
            checksum: checksum.clone(),
            ref_count: 0,
        };
        self.objects.insert(object_id.clone(), obj.clone());
        if let Some(db) = &self.db {
            db.lock()
                .execute(
                    "INSERT INTO objects(object_id,owner,path,size,checksum,ref_count,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![
                        obj.object_id, obj.owner, obj.path, obj.size as i64, obj.checksum,
                        obj.ref_count as i64, crate::protocol::now_millis()
                    ],
                )
                .map_err(|e| e.to_string())?;
        }
        Ok(ObjectRef { object_id, size, checksum })
    }

    /// 内存内注册（测试/结构证明用）：给定字节与假路径，不写文件。
    pub fn register(&mut self, owner: &str, path: String, bytes: &[u8]) -> ObjectRef {
        let checksum = checksum(bytes);
        let object_id = self.alloc_id();
        let size = bytes.len() as u64;
        self.objects.insert(
            object_id.clone(),
            Object {
                object_id: object_id.clone(),
                owner: owner.to_string(),
                path,
                size,
                checksum: checksum.clone(),
                ref_count: 0,
            },
        );
        ObjectRef { object_id, size, checksum }
    }

    /// 跨域只读：任何域都能解析引用（不可写）。
    pub fn resolve(&self, object_id: &str) -> Option<&Object> {
        self.objects.get(object_id)
    }

    /// 取引用：ref_count +1。
    pub fn acquire(&mut self, object_id: &str) -> bool {
        let ok = match self.objects.get_mut(object_id) {
            Some(o) => {
                o.ref_count += 1;
                true
            }
            None => false,
        };
        if ok && self.db.is_some() {
            let _ = self.db.as_ref().unwrap().lock().execute(
                "UPDATE objects SET ref_count=ref_count+1 WHERE object_id=?1",
                [object_id],
            );
        }
        ok
    }

    /// 放引用：ref_count -1（不减到负）。
    pub fn release(&mut self, object_id: &str) -> bool {
        let ok = match self.objects.get_mut(object_id) {
            Some(o) if o.ref_count > 0 => {
                o.ref_count -= 1;
                true
            }
            _ => false,
        };
        if ok && self.db.is_some() {
            let _ = self.db.as_ref().unwrap().lock().execute(
                "UPDATE objects SET ref_count=max(0,ref_count-1) WHERE object_id=?1",
                [object_id],
            );
        }
        ok
    }

    /// 显式删除：只有归属域能删；引用未清零不得删。
    /// 只移除索引行——物理文件归业务域所有，内核不删除、不移动它。
    pub fn delete(&mut self, object_id: &str, by_domain: &str) -> DeleteOutcome {
        match self.objects.get(object_id) {
            None => DeleteOutcome::Unknown,
            Some(o) if o.owner != by_domain => DeleteOutcome::Forbidden,
            Some(o) if o.ref_count > 0 => DeleteOutcome::Referenced,
            Some(_) => {
                self.objects.remove(object_id);
                if let Some(db) = &self.db {
                    let _ = db
                        .lock()
                        .execute("DELETE FROM objects WHERE object_id=?1", [object_id]);
                }
                DeleteOutcome::Deleted
            }
        }
    }

    pub fn len(&self) -> usize {
        self.objects.len()
    }
}

/// 内核计算的确定性校验和：SHA-256（十六进制）。
fn checksum(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// 流式计算文件 checksum（常数内存，不把整个文件读进内存）。
fn hash_file(path: &Path) -> Result<(u64, String), String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut size = 0u64;
    loop {
        let n = file.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size += n as u64;
    }
    Ok((size, format!("{:x}", hasher.finalize())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_isolation_and_readonly_cross_domain() {
        let mut hub = ObjectHub::new();
        let r = hub.register("engines", "/objects/blob".into(), b"large binary payload");
        assert!(hub.resolve(&r.object_id).is_some());
        assert_eq!(hub.resolve(&r.object_id).unwrap().owner, "engines");
        assert_eq!(hub.delete(&r.object_id, "publishers"), DeleteOutcome::Forbidden);
        assert!(hub.resolve(&r.object_id).is_some());
    }

    #[test]
    fn kernel_computes_checksum_and_size() {
        let mut hub = ObjectHub::new();
        let bytes = b"hello object hub";
        let r = hub.register("engines", "/o".into(), bytes);
        assert_eq!(r.size, bytes.len() as u64);
        assert_eq!(r.checksum, checksum(bytes));
        assert_eq!(hub.resolve(&r.object_id).unwrap().checksum, r.checksum);
    }

    #[test]
    fn no_delete_while_referenced_and_explicit_delete() {
        let mut hub = ObjectHub::new();
        let r = hub.register("engines", "/o".into(), b"data");
        assert!(hub.acquire(&r.object_id));
        assert_eq!(hub.delete(&r.object_id, "engines"), DeleteOutcome::Referenced);
        assert!(hub.release(&r.object_id));
        assert_eq!(hub.delete(&r.object_id, "engines"), DeleteOutcome::Deleted);
        assert!(hub.resolve(&r.object_id).is_none());
    }

    #[test]
    fn object_lifetime_is_independent_of_message() {
        let mut hub = ObjectHub::new();
        let r = hub.register("engines", "/o".into(), b"payload");
        assert_eq!(hub.len(), 1);
        for _ in 0..1000 {
            hub.register("engines", "/o".into(), b"x");
        }
        assert_eq!(hub.len(), 1001);
        assert_eq!(hub.delete(&r.object_id, "engines"), DeleteOutcome::Deleted);
    }

    #[test]
    fn register_file_indexes_absolute_path_and_persists_across_restart() {
        let dir = std::env::temp_dir().join(format!("atelier-obj-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let src = dir.join("source.bin");
        std::fs::write(&src, b"persisted object bytes").unwrap();
        let canonical = src.canonicalize().unwrap();

        let db = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let mut hub = ObjectHub::with_db(db.clone()).unwrap();
        let r = hub.register_file("engines", &src).unwrap();
        assert_eq!(r.size, 22);
        let obj = hub.resolve(&r.object_id).unwrap();
        // 内核只记绝对路径，不复制、不移动：源文件原地保留
        assert_eq!(Path::new(&obj.path), canonical.as_path());
        assert!(src.exists(), "kernel must not move or delete the domain file");

        // 跨重启：重新从 SQLite 载入
        let hub2 = ObjectHub::with_db(db).unwrap();
        let obj2 = hub2.resolve(&r.object_id).expect("object index survives restart");
        assert_eq!(obj2.owner, "engines");
        assert_eq!(obj2.checksum, r.checksum);

        // 删除只移除索引，不删物理文件
        let mut hub2 = hub2;
        assert_eq!(hub2.delete(&r.object_id, "engines"), DeleteOutcome::Deleted);
        assert!(src.exists(), "index delete must not remove the domain file");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
