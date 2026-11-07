//! ObjectHub —— C4 存储枢纽：虚拟路径 → 唯一 id → 平铺物理文件的索引。
//!
//! 模型（见 docs/CONSTITUTION.md C4）：
//!   - 启动配置只有一个项目根 `project_root`（默认 `./data`）；域根 = `<project_root>/<domain>/`。
//!   - 物理落盘：**平铺的唯一 id 文件** `<project_root>/<domain>/<object_id>`，无子目录。
//!   - 业务域的目录结构（如 `users/avatars/a.png`）是**虚拟**的，只存在于本索引：
//!     `(owner, virtual_path) → object_id → physical_path`。
//!   - **创建**只发生在归属域自己的命名空间；读取、写入（更新已有对象）、查询、删除都可跨域。
//!   - **删除对象 = 删除索引行 + 删除物理文件**（幂等；文件已不存在也算删除成功）。
//!   - `size` 由调用方在创建/更新时给出；内核不读文件、不做隐式自愈。

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::{Connection, params};

use crate::services::storage::StorageError;

const OBJECTS_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS objects(
  object_id TEXT PRIMARY KEY,owner TEXT NOT NULL,virtual_path TEXT NOT NULL,
  physical_path TEXT NOT NULL,size INTEGER NOT NULL DEFAULT 0,
  ref_count INTEGER NOT NULL DEFAULT 0,created_at INTEGER NOT NULL,
  UNIQUE(owner,virtual_path));";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object {
    pub object_id: String,
    pub owner: String,
    /// 业务域自己的虚拟路径（仅存在于索引里）。
    pub virtual_path: String,
    /// 平铺的唯一 id 物理文件绝对路径。
    pub physical_path: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRef {
    pub object_id: String,
    pub virtual_path: String,
    pub physical_path: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    Unknown,
    /// 索引还在、物理文件删不掉（权限/句柄占用）；索引保留，调用方可重试。
    UnlinkFailed(String),
}

pub struct ObjectHub {
    objects: BTreeMap<String, Object>,
    by_path: BTreeMap<(String, String), String>,
    next_id: u64,
    root: PathBuf,
    db: Option<Arc<Mutex<Connection>>>,
}

impl ObjectHub {
    pub fn new(project_root: PathBuf) -> Self {
        ObjectHub {
            objects: BTreeMap::new(),
            by_path: BTreeMap::new(),
            next_id: 1,
            root: project_root,
            db: None,
        }
    }

    /// 从 SQLite 载入对象索引（跨重启存在）。
    pub fn with_db(project_root: PathBuf, db: Arc<Mutex<Connection>>) -> Result<Self, StorageError> {
        db.lock().execute_batch(OBJECTS_SCHEMA)?;
        let mut hub = ObjectHub {
            objects: BTreeMap::new(),
            by_path: BTreeMap::new(),
            next_id: 1,
            root: project_root,
            db: Some(db.clone()),
        };
        let rows = {
            let guard = db.lock();
            let mut stmt = guard.prepare(
                "SELECT object_id,owner,virtual_path,physical_path,size FROM objects ORDER BY rowid",
            )?;
            stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)? as u64,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        for (object_id, owner, virtual_path, physical_path, size) in rows {
            if let Some(n) = object_id.strip_prefix("obj_").and_then(|s| s.parse::<u64>().ok()) {
                hub.next_id = hub.next_id.max(n + 1);
            }
            hub.by_path
                .insert((owner.clone(), virtual_path.clone()), object_id.clone());
            hub.objects.insert(
                object_id.clone(),
                Object { object_id, owner, virtual_path, physical_path, size },
            );
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

    /// 在**归属域自己的命名空间**创建对象（幂等：同一 owner+virtual_path 复用原 id）。
    /// 不创建文件本身，只创建域目录；返回物理路径供调用方写入。
    pub fn create(&mut self, owner: &str, virtual_path: &str) -> Result<ObjectRef, String> {
        if let Some(id) = self
            .by_path
            .get(&(owner.to_string(), virtual_path.to_string()))
            .cloned()
        {
            if let Some(obj) = self.objects.get(&id) {
                return Ok(ObjectRef {
                    object_id: obj.object_id.clone(),
                    virtual_path: obj.virtual_path.clone(),
                    physical_path: obj.physical_path.clone(),
                    size: obj.size,
                });
            }
        }
        let object_id = self.alloc_id();
        let physical = self.physical_for(owner, &object_id);
        if let Some(parent) = physical.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let obj = Object {
            object_id: object_id.clone(),
            owner: owner.to_string(),
            virtual_path: virtual_path.to_string(),
            physical_path: physical.to_string_lossy().to_string(),
            size: 0,
        };
        self.objects.insert(object_id.clone(), obj.clone());
        self.by_path
            .insert((owner.to_string(), virtual_path.to_string()), object_id.clone());
        if let Some(db) = &self.db {
            db.lock()
                .execute(
                    "INSERT INTO objects(object_id,owner,virtual_path,physical_path,size,ref_count,created_at) VALUES(?1,?2,?3,?4,?5,0,?6)",
                    params![
                        obj.object_id, obj.owner, obj.virtual_path, obj.physical_path,
                        obj.size as i64, crate::core::protocol::now_millis()
                    ],
                )
                .map_err(|e| e.to_string())?;
        }
        Ok(ObjectRef {
            object_id,
            virtual_path: obj.virtual_path,
            physical_path: obj.physical_path,
            size: 0,
        })
    }

    /// 登记调用方已经写好的平铺文件（SDK 生成唯一 id）。
    ///
    /// 同一 `(owner, virtual_path)` 再次登记：**替换**为本次的 object_id 与物理文件，
    /// 旧物理文件删除。这样域重启后重写同一虚拟路径不会留下指向旧文件的索引。
    pub fn register_external(
        &mut self,
        owner: &str,
        virtual_path: &str,
        object_id: &str,
        size: u64,
    ) -> Result<ObjectRef, String> {
        if let Some(existing) = self
            .by_path
            .get(&(owner.to_string(), virtual_path.to_string()))
            .cloned()
        {
            if existing == object_id {
                if let Some(obj) = self.objects.get_mut(object_id) {
                    obj.size = size;
                    if let Some(db) = &self.db {
                        let _ = db.lock().execute(
                            "UPDATE objects SET size=?1 WHERE object_id=?2",
                            params![size as i64, object_id],
                        );
                    }
                    return Ok(ObjectRef {
                        object_id: obj.object_id.clone(),
                        virtual_path: obj.virtual_path.clone(),
                        physical_path: obj.physical_path.clone(),
                        size,
                    });
                }
            }
            // 覆盖写：旧文件与旧索引一起换掉。
            if let Some(old) = self.objects.get(&existing) {
                let _ = std::fs::remove_file(&old.physical_path);
            }
            self.remove(&existing);
        }
        let physical = self.physical_for(owner, object_id);
        if let Some(parent) = physical.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let obj = Object {
            object_id: object_id.to_string(),
            owner: owner.to_string(),
            virtual_path: virtual_path.to_string(),
            physical_path: physical.to_string_lossy().to_string(),
            size,
        };
        self.objects.insert(object_id.to_string(), obj.clone());
        self.by_path
            .insert((owner.to_string(), virtual_path.to_string()), object_id.to_string());
        if let Some(db) = &self.db {
            db.lock()
                .execute(
                    "INSERT INTO objects(object_id,owner,virtual_path,physical_path,size,ref_count,created_at) VALUES(?1,?2,?3,?4,?5,0,?6)",
                    params![
                        obj.object_id, obj.owner, obj.virtual_path, obj.physical_path,
                        obj.size as i64, crate::core::protocol::now_millis()
                    ],
                )
                .map_err(|e| e.to_string())?;
        }
        Ok(ObjectRef {
            object_id: object_id.to_string(),
            virtual_path: virtual_path.to_string(),
            physical_path: obj.physical_path,
            size,
        })
    }

    /// 列出某个域的全部对象（跨域查询允许）。
    pub fn list_owner(&self, owner: &str) -> Vec<Object> {
        self.objects
            .values()
            .filter(|o| o.owner == owner)
            .cloned()
            .collect()
    }

    /// 更新已有对象的大小（跨域写入允许；对象必须存在）。
    pub fn commit(&mut self, object_id: &str, size: u64) -> Result<(), String> {
        let Some(obj) = self.objects.get_mut(object_id) else {
            return Err(format!("unknown object '{object_id}'"));
        };
        obj.size = size;
        if let Some(db) = &self.db {
            db.lock()
                .execute(
                    "UPDATE objects SET size=?1 WHERE object_id=?2",
                    params![size as i64, object_id],
                )
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// 按归属域 + 虚拟路径解析。
    pub fn resolve_path(&mut self, owner: &str, virtual_path: &str) -> Option<Object> {
        let id = self
            .by_path
            .get(&(owner.to_string(), virtual_path.to_string()))?
            .clone();
        self.resolve(&id)
    }

    /// 按唯一 id 解析（跨域读取允许）。
    ///
    /// 文件存在时用 metadata 刷新 size（只读元数据，不读内容）；文件缺失时返回 `None`，
    /// **不删除索引行**——清理必须走显式 `delete`。
    pub fn resolve(&mut self, object_id: &str) -> Option<Object> {
        let obj = self.objects.get(object_id)?.clone();
        if let Ok(meta) = std::fs::metadata(&obj.physical_path) {
            if meta.len() != obj.size {
                if let Some(current) = self.objects.get_mut(object_id) {
                    current.size = meta.len();
                }
                if let Some(db) = &self.db {
                    let _ = db.lock().execute(
                        "UPDATE objects SET size=?1 WHERE object_id=?2",
                        params![meta.len() as i64, object_id],
                    );
                }
            }
            return self.objects.get(object_id).cloned();
        }
        None
    }

    /// 删除对象：删物理文件 + 删索引行；跨域允许，幂等。
    pub fn delete(&mut self, object_id: &str) -> DeleteOutcome {
        let Some(obj) = self.objects.get(object_id).cloned() else {
            return DeleteOutcome::Unknown;
        };
        let physical = std::path::Path::new(&obj.physical_path);
        match std::fs::remove_file(physical) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return DeleteOutcome::UnlinkFailed(e.to_string()),
        }
        self.remove(object_id);
        DeleteOutcome::Deleted
    }

    /// 按虚拟路径删除（需给出归属域；默认调用域由 router 决定）。
    pub fn delete_path(&mut self, owner: &str, virtual_path: &str) -> DeleteOutcome {
        let Some(id) = self
            .by_path
            .get(&(owner.to_string(), virtual_path.to_string()))
            .cloned()
        else {
            return DeleteOutcome::Unknown;
        };
        self.delete(&id)
    }

    fn remove(&mut self, object_id: &str) {
        if let Some(obj) = self.objects.remove(object_id) {
            self.by_path.remove(&(obj.owner, obj.virtual_path));
            if let Some(db) = &self.db {
                let _ = db
                    .lock()
                    .execute("DELETE FROM objects WHERE object_id=?1", [object_id]);
            }
        }
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

    #[test]
    fn create_maps_virtual_path_to_flat_unique_file() {
        let root = temp_root("create");
        let mut hub = ObjectHub::new(root.clone());
        let r = hub.create("engines", "users/avatars/a.png").unwrap();
        assert!(r.object_id.starts_with("obj_"));
        assert_eq!(
            std::path::Path::new(&r.physical_path),
            root.join("engines").join(&r.object_id)
        );
        assert_eq!(r.virtual_path, "users/avatars/a.png");
        let again = hub.create("engines", "users/avatars/a.png").unwrap();
        assert_eq!(again.object_id, r.object_id);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn resolve_path_and_cross_domain_resolve() {
        let root = temp_root("resolve");
        let mut hub = ObjectHub::new(root.clone());
        let r = hub.create("engines", "out/video.bin").unwrap();
        std::fs::write(&r.physical_path, b"binary").unwrap();
        hub.commit(&r.object_id, 6).unwrap();

        let by_path = hub.resolve_path("engines", "out/video.bin").unwrap();
        assert_eq!(by_path.object_id, r.object_id);
        assert_eq!(by_path.size, 6);
        let cross = hub.resolve(&r.object_id).unwrap();
        assert_eq!(cross.owner, "engines");
        assert!(hub.resolve_path("other", "out/video.bin").is_none());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn delete_removes_the_physical_file_and_allows_cross_domain() {
        let root = temp_root("delete");
        let mut hub = ObjectHub::new(root.clone());
        let r = hub.create("engines", "a.bin").unwrap();
        std::fs::write(&r.physical_path, b"x").unwrap();
        assert_eq!(hub.delete(&r.object_id), DeleteOutcome::Deleted);
        assert!(!std::path::Path::new(&r.physical_path).exists(), "physical file must be removed");
        assert!(hub.resolve(&r.object_id).is_none());
        // 幂等：再删一次是 Unknown，不报错
        assert_eq!(hub.delete(&r.object_id), DeleteOutcome::Unknown);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn registering_the_same_path_replaces_the_old_file() {
        let root = temp_root("replace");
        let mut hub = ObjectHub::new(root.clone());
        let first = hub.register_external("engines", "a.bin", "u1", 1).unwrap();
        std::fs::write(&first.physical_path, b"old").unwrap();
        let second = hub.register_external("engines", "a.bin", "u2", 3).unwrap();
        assert_eq!(second.object_id, "u2");
        assert_ne!(second.physical_path, first.physical_path);
        assert!(!std::path::Path::new(&first.physical_path).exists(), "stale file must be removed");
        std::fs::write(&second.physical_path, b"new").unwrap();
        let resolved = hub.resolve_path("engines", "a.bin").unwrap();
        assert_eq!(resolved.object_id, "u2");
        assert_eq!(resolved.size, 3);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn deleting_a_missing_file_still_drops_the_index() {
        let root = temp_root("gone");
        let mut hub = ObjectHub::new(root.clone());
        let r = hub.create("engines", "a.bin").unwrap();
        assert_eq!(hub.delete(&r.object_id), DeleteOutcome::Deleted);
        assert!(hub.resolve_path("engines", "a.bin").is_none());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn index_survives_restart() {
        let root = temp_root("restart");
        std::fs::create_dir_all(&root).unwrap();
        let db = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let (object_id, physical) = {
            let mut hub = ObjectHub::with_db(root.clone(), db.clone()).unwrap();
            let r = hub.create("engines", "a/b.bin").unwrap();
            std::fs::write(&r.physical_path, b"data").unwrap();
            hub.commit(&r.object_id, 4).unwrap();
            (r.object_id, r.physical_path)
        };
        let mut hub2 = ObjectHub::with_db(root.clone(), db).unwrap();
        let obj = hub2.resolve_path("engines", "a/b.bin").expect("index survives restart");
        assert_eq!(obj.object_id, object_id);
        assert_eq!(obj.physical_path, physical);
        assert_eq!(obj.size, 4);
        let _ = std::fs::remove_dir_all(&root);
    }
}
