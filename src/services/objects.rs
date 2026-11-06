//! ObjectHub —— C4 存储枢纽：虚拟路径 → 唯一 id → 平铺物理文件的索引。
//!
//! 模型（见 docs/CONSTITUTION.md C4）：
//!   - 启动配置只有一个项目根 `project_root`（默认 `./data`）；域根 = `<project_root>/<domain>/`。
//!   - 物理落盘：**平铺的唯一 id 文件** `<project_root>/<domain>/<object_id>`，无子目录。
//!   - 业务域的目录结构（如 `users/avatars/a.png`）是**虚拟**的，只存在于本索引：
//!     `(owner, virtual_path) → object_id → physical_path`。
//!   - 内核不复制、不移动、不读取文件内容；`size` 由调用方在 `commit` 时给出。
//!   - 使用前自愈：物理文件不存在时清掉失效索引行。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
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
    pub ref_count: u64,
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
    Forbidden,  // 非归属域
    Referenced, // 引用未清零
    Unknown,
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
                "SELECT object_id,owner,virtual_path,physical_path,size,ref_count FROM objects ORDER BY rowid",
            )?;
            stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)? as u64,
                    r.get::<_, i64>(5)? as u64,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        for (object_id, owner, virtual_path, physical_path, size, ref_count) in rows {
            if let Some(n) = object_id.strip_prefix("obj_").and_then(|s| s.parse::<u64>().ok()) {
                hub.next_id = hub.next_id.max(n + 1);
            }
            hub.by_path
                .insert((owner.clone(), virtual_path.clone()), object_id.clone());
            hub.objects.insert(
                object_id.clone(),
                Object { object_id, owner, virtual_path, physical_path, size, ref_count },
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

    /// 为一个虚拟路径分配唯一 id（幂等：同一 owner+virtual_path 复用原 id）。
    /// 不创建文件本身，只创建域目录；返回物理路径供调用方写入。
    pub fn allocate(&mut self, owner: &str, virtual_path: &str) -> Result<ObjectRef, String> {
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
            ref_count: 0,
        };
        self.objects.insert(object_id.clone(), obj.clone());
        self.by_path
            .insert((owner.to_string(), virtual_path.to_string()), object_id.clone());
        if let Some(db) = &self.db {
            db.lock()
                .execute(
                    "INSERT INTO objects(object_id,owner,virtual_path,physical_path,size,ref_count,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![
                        obj.object_id, obj.owner, obj.virtual_path, obj.physical_path,
                        obj.size as i64, obj.ref_count as i64, crate::core::protocol::now_millis()
                    ],
                )
                .map_err(|e| e.to_string())?;
        }
        Ok(ObjectRef {
            object_id,
            virtual_path: virtual_path.to_string(),
            physical_path: obj.physical_path,
            size: 0,
        })
    }

    /// 方案 A：SDK 已写入平铺文件、自带全局唯一 object_id，这里只登记映射。
    /// 同一 virtual_path 已存在时复用原 id（覆盖语义，更新 size）。
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
            if let Some(obj) = self.objects.get_mut(&existing) {
                obj.size = size;
                if let Some(db) = &self.db {
                    let _ = db.lock().execute(
                        "UPDATE objects SET size=?1 WHERE object_id=?2",
                        params![size as i64, existing],
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
            ref_count: 0,
        };
        self.objects.insert(object_id.to_string(), obj.clone());
        self.by_path
            .insert((owner.to_string(), virtual_path.to_string()), object_id.to_string());
        if let Some(db) = &self.db {
            db.lock()
                .execute(
                    "INSERT INTO objects(object_id,owner,virtual_path,physical_path,size,ref_count,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![
                        obj.object_id, obj.owner, obj.virtual_path, obj.physical_path,
                        obj.size as i64, obj.ref_count as i64, crate::core::protocol::now_millis()
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

    /// 列出某域的全部对象（SDK 启动时拉一次做本地缓存）。
    pub fn list_owner(&self, owner: &str) -> Vec<Object> {
        self.objects
            .values()
            .filter(|o| o.owner == owner)
            .cloned()
            .collect()
    }

    /// 写入完成后登记实际大小。
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

    /// 按归属域 + 虚拟路径解析（域自己的读路径）。
    pub fn resolve_path(&mut self, owner: &str, virtual_path: &str) -> Option<Object> {
        let id = self
            .by_path
            .get(&(owner.to_string(), virtual_path.to_string()))?
            .clone();
        self.resolve(&id)
    }

    /// 按唯一 id 解析（跨域只读）。
    /// 使用前自愈 + 由内核按 `metadata` 取 size（内核拥有物理文件）。
    pub fn resolve(&mut self, object_id: &str) -> Option<Object> {
        let path = Path::new(&self.objects.get(object_id)?.physical_path).to_path_buf();
        if !path.exists() {
            self.remove(object_id);
            return None;
        }
        if let Ok(meta) = std::fs::metadata(&path) {
            if let Some(obj) = self.objects.get_mut(object_id) {
                obj.size = meta.len();
            }
        }
        self.objects.get(object_id).cloned()
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

    /// 显式删除：只有归属域能删；引用未清零不得删。只移除索引行。
    pub fn delete(&mut self, object_id: &str, by_domain: &str) -> DeleteOutcome {
        match self.objects.get(object_id) {
            None => DeleteOutcome::Unknown,
            Some(o) if o.owner != by_domain => DeleteOutcome::Forbidden,
            Some(o) if o.ref_count > 0 => DeleteOutcome::Referenced,
            Some(_) => {
                self.remove(object_id);
                DeleteOutcome::Deleted
            }
        }
    }

    /// 按虚拟路径删除（归属域）。
    pub fn delete_path(&mut self, owner: &str, virtual_path: &str) -> DeleteOutcome {
        let Some(id) = self
            .by_path
            .get(&(owner.to_string(), virtual_path.to_string()))
            .cloned()
        else {
            return DeleteOutcome::Unknown;
        };
        self.delete(&id, owner)
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
    fn allocate_maps_virtual_path_to_flat_unique_file() {
        let root = temp_root("alloc");
        let mut hub = ObjectHub::new(root.clone());
        let r = hub.allocate("engines", "users/avatars/a.png").unwrap();
        assert!(r.object_id.starts_with("obj_"));
        // 物理路径平铺在域根下，文件名就是唯一 id，没有子目录
        assert_eq!(
            Path::new(&r.physical_path),
            root.join("engines").join(&r.object_id)
        );
        assert_eq!(r.virtual_path, "users/avatars/a.png");
        // 幂等：同一虚拟路径复用同一 id
        let again = hub.allocate("engines", "users/avatars/a.png").unwrap();
        assert_eq!(again.object_id, r.object_id);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn resolve_path_and_cross_domain_resolve() {
        let root = temp_root("resolve");
        let mut hub = ObjectHub::new(root.clone());
        let r = hub.allocate("engines", "out/video.bin").unwrap();
        // 域自己写入物理文件
        std::fs::write(&r.physical_path, b"binary").unwrap();
        hub.commit(&r.object_id, 6).unwrap();

        let by_path = hub.resolve_path("engines", "out/video.bin").unwrap();
        assert_eq!(by_path.object_id, r.object_id);
        assert_eq!(by_path.size, 6);
        // 跨域按 id 只读解析
        let cross = hub.resolve(&r.object_id).unwrap();
        assert_eq!(cross.owner, "engines");
        // 其他域的虚拟路径查不到（命名空间隔离）
        assert!(hub.resolve_path("other", "out/video.bin").is_none());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn resolve_self_heals_rows_whose_file_is_gone() {
        let root = temp_root("stale");
        let mut hub = ObjectHub::new(root.clone());
        let r = hub.allocate("engines", "a.bin").unwrap();
        std::fs::write(&r.physical_path, b"x").unwrap();
        assert!(hub.resolve(&r.object_id).is_some());
        std::fs::remove_file(&r.physical_path).unwrap();
        assert!(hub.resolve(&r.object_id).is_none(), "stale row cleaned");
        assert!(hub.resolve_path("engines", "a.bin").is_none());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn delete_is_index_only_and_write_isolated() {
        let root = temp_root("delete");
        let mut hub = ObjectHub::new(root.clone());
        let r = hub.allocate("engines", "a.bin").unwrap();
        std::fs::write(&r.physical_path, b"x").unwrap();
        assert_eq!(hub.delete(&r.object_id, "publishers"), DeleteOutcome::Forbidden);
        assert_eq!(hub.delete(&r.object_id, "engines"), DeleteOutcome::Deleted);
        // 物理文件仍在（内核不删域文件）
        assert!(Path::new(&r.physical_path).exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn index_survives_restart() {
        let root = temp_root("restart");
        std::fs::create_dir_all(&root).unwrap();
        let db = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let (object_id, physical) = {
            let mut hub = ObjectHub::with_db(root.clone(), db.clone()).unwrap();
            let r = hub.allocate("engines", "a/b.bin").unwrap();
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
