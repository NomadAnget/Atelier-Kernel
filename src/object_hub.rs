//! ObjectHub —— C4 存储枢纽对象索引：核外的一个有界索引物。
//!
//! 大文件不进管道，消息 `data` 里只放 `object_id` 引用；对象索引与消息生命周期正交
//! （消息删除 ≠ 对象删除，反之亦然）。契约（见 CONSTITUTION.md C4）：
//!   - 写权随域（M3）：只有归属域能删除；跨域读取只读；
//!   - 无 TTL、无硬配额；删除时机由业务域显式决定；
//!   - `checksum` 由内核计算；
//!   - 引用未清零前不得删除。

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object {
    pub object_id: String,
    pub owner: String,
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
    Forbidden,   // 非归属域
    Referenced,  // 引用未清零
    Unknown,
}

pub struct ObjectHub {
    objects: BTreeMap<String, Object>,
    next_id: u64,
}

impl Default for ObjectHub {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectHub {
    pub fn new() -> Self {
        ObjectHub { objects: BTreeMap::new(), next_id: 1 }
    }

    /// 注册一个对象：内核计算 checksum，域只拿到引用，不知道物理位置。
    pub fn register(&mut self, owner: &str, path: String, bytes: &[u8]) -> ObjectRef {
        let checksum = checksum(bytes);
        let object_id = format!("obj_{}", self.next_id);
        self.next_id += 1;
        let size = bytes.len() as u64;
        self.objects.insert(
            object_id.clone(),
            Object { object_id: object_id.clone(), owner: owner.to_string(), path, size, checksum: checksum.clone(), ref_count: 0 },
        );
        ObjectRef { object_id, size, checksum }
    }

    /// 跨域只读：任何域都能解析引用（不可写）。
    pub fn resolve(&self, object_id: &str) -> Option<&Object> {
        self.objects.get(object_id)
    }

    /// 取引用：ref_count +1。
    pub fn acquire(&mut self, object_id: &str) -> bool {
        match self.objects.get_mut(object_id) {
            Some(o) => { o.ref_count += 1; true }
            None => false,
        }
    }

    /// 放引用：ref_count -1（不减到负）。
    pub fn release(&mut self, object_id: &str) -> bool {
        match self.objects.get_mut(object_id) {
            Some(o) if o.ref_count > 0 => { o.ref_count -= 1; true }
            Some(_) => false,
            None => false,
        }
    }

    /// 显式删除：只有归属域能删；引用未清零不得删。
    pub fn delete(&mut self, object_id: &str, by_domain: &str) -> DeleteOutcome {
        match self.objects.get(object_id) {
            None => DeleteOutcome::Unknown,
            Some(o) if o.owner != by_domain => DeleteOutcome::Forbidden,
            Some(o) if o.ref_count > 0 => DeleteOutcome::Referenced,
            Some(_) => {
                self.objects.remove(object_id);
                DeleteOutcome::Deleted
            }
        }
    }

    pub fn len(&self) -> usize {
        self.objects.len()
    }
}

/// 内核计算的确定性校验和（结构证明用；真实实现换 SHA-256）。
fn checksum(bytes: &[u8]) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{h:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_isolation_and_readonly_cross_domain() {
        let mut hub = ObjectHub::new();
        let r = hub.register("engines", "/objects/blob".into(), b"large binary payload");
        // 跨域读取只读：任何域都能 resolve
        assert!(hub.resolve(&r.object_id).is_some());
        assert_eq!(hub.resolve(&r.object_id).unwrap().owner, "engines");
        // 非归属域不能删
        assert_eq!(hub.delete(&r.object_id, "publishers"), DeleteOutcome::Forbidden);
        assert!(hub.resolve(&r.object_id).is_some());
    }

    #[test]
    fn kernel_computes_checksum_and_size() {
        let mut hub = ObjectHub::new();
        let bytes = b"hello object hub";
        let r = hub.register("engines", "/o".into(), bytes);
        assert_eq!(r.size, bytes.len() as u64);
        assert!(!r.checksum.is_empty());
        assert_eq!(hub.resolve(&r.object_id).unwrap().checksum, r.checksum);
    }

    #[test]
    fn no_delete_while_referenced_and_explicit_delete() {
        let mut hub = ObjectHub::new();
        let r = hub.register("engines", "/o".into(), b"data");
        assert!(hub.acquire(&r.object_id));
        // 引用未清零：归属域也不能删
        assert_eq!(hub.delete(&r.object_id, "engines"), DeleteOutcome::Referenced);
        // 显式释放后归属域可删
        assert!(hub.release(&r.object_id));
        assert_eq!(hub.delete(&r.object_id, "engines"), DeleteOutcome::Deleted);
        assert!(hub.resolve(&r.object_id).is_none());
    }

    #[test]
    fn object_lifetime_is_independent_of_message() {
        // 对象索引与消息生命周期完全解耦：这里没有 admit/advance，对象照常注册/删除。
        let mut hub = ObjectHub::new();
        let r = hub.register("engines", "/o".into(), b"payload");
        assert_eq!(hub.len(), 1);
        // 没有 TTL、没有配额：注册任意多个都不会因容量被拒
        for _ in 0..1000 {
            hub.register("engines", "/o".into(), b"x");
        }
        assert_eq!(hub.len(), 1001);
        assert_eq!(hub.delete(&r.object_id, "engines"), DeleteOutcome::Deleted);
    }
}
