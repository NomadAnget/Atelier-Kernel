//! Nucleus —— 内核的最小核（kernel-within-the-kernel）。
//!
//! 只保证一条规则：
//! > 每条消息有且仅有一行权威状态；状态单调推进；终态即离开活跃集合（声明 Durable 则转审计）；活跃集合有界。
//!
//! 核只有三个基础操作，且全部是全函数（任意输入都有确定输出、无副作用泄漏）：
//!   - `admit`     —— 收进活跃集合（去重键命中窗口则复用原 id）
//!   - `advance`   —— 单调推进；推进到终态即自动"转审计或清除 + 回收去重窗口"
//!   - `retire_source` —— 退役一个 source 的活跃行（代际退役的批量收敛）
//!
//! 一切外围能力（传输 outbox、存储 C4、observer、supervisor）都是对这三个操作的
//! 确定性组合与只读投影。**只有核能改消息状态。**

use std::collections::BTreeMap;

use crate::protocol::{ContractStatus, DeliveryPolicy};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub message_id: String,
    pub logical_id: String,
    pub source: String, // 纯域 id（不是去重键）
    pub seq: u64,
    pub target: String,
    pub message_type: String,
    pub policy: DeliveryPolicy,
    pub status: ContractStatus,
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    pub message_id: String,
    pub logical_id: String,
    pub source: String,
    pub target: String,
    pub status: ContractStatus,
    pub data: Option<serde_json::Value>,
}

/// 持久层：核本身只做状态机，持久化是核外的一个 backend。
/// Durable 活行与审计由它保存/读取；Volatile/Ephemeral 永不过它。
pub trait Store {
    fn load_active(&self) -> Vec<Message> {
        Vec::new()
    }
    fn load_audit(&self) -> Vec<AuditRecord> {
        Vec::new()
    }
    fn save_active(&mut self, _m: &Message) {}
    fn delete_active(&mut self, _id: &str) {}
    fn append_audit(&mut self, _r: &AuditRecord) {}
}

/// 测试用的内存持久层。
#[derive(Default)]
pub struct VecStore {
    pub active: Vec<Message>,
    pub audit: Vec<AuditRecord>,
}

impl Store for VecStore {
    fn load_active(&self) -> Vec<Message> {
        self.active.clone()
    }
    fn load_audit(&self) -> Vec<AuditRecord> {
        self.audit.clone()
    }
    fn save_active(&mut self, m: &Message) {
        self.active.retain(|x| x.message_id != m.message_id);
        self.active.push(m.clone());
    }
    fn delete_active(&mut self, id: &str) {
        self.active.retain(|x| x.message_id != id);
    }
    fn append_audit(&mut self, r: &AuditRecord) {
        self.audit.push(r.clone());
    }
}

#[derive(Debug, Clone)]
pub struct Submission {
    pub logical_id: String,
    pub ids: Vec<String>,
    /// `true` 表示命中去重窗口，复用原 id，未产生新活跃行。
    pub replayed: bool,
}

pub enum Advance {
    /// 状态推进（含终态），返回回执来源域与回执数据。
    Advanced { source: String, status: ContractStatus, data: Option<serde_json::Value>, target: String },
    /// 回退、重复或已在终态，被幂等吸收。
    DuplicateOrStale,
    /// 未知 id。
    Unknown,
}

/// 有界去重窗口：每个 source（去重键）只保留最近 `window` 个 seq 的提交身份。
const REPLAY_WINDOW: usize = 2_048;

/// SubmissionCache —— 提交身份/去重/高水位：核外的一个推导。
/// 旧 `allocate_submission` 映射到这里；它不持有消息状态，只记录"哪次提交对应哪些 id"。
pub struct SubmissionCache {
    /// 去重窗口 `(dedup_key, seq) -> Submission`，按 seq 有界。
    submissions: BTreeMap<(String, u64), Submission>,
    /// 单调高水位：会话行被回收后仍记住"到过的最高 seq"，防止老 seq 被误判成新提交。
    high_water: BTreeMap<String, u64>,
    next_id: u64,
}

impl Default for SubmissionCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SubmissionCache {
    pub fn new() -> Self {
        SubmissionCache { submissions: BTreeMap::new(), high_water: BTreeMap::new(), next_id: 1 }
    }

    fn generate_id(&mut self) -> String {
        let id = format!("msg_{}", self.next_id);
        self.next_id += 1;
        id
    }

    /// 分配提交身份：去重命中窗口则复用原身份，否则分配新 id。
    /// 返回 `(logical_id, ids, replayed)`。
    pub fn allocate(&mut self, dedup_key: &str, seq: u64, count: usize) -> (String, Vec<String>, bool) {
        if let Some(existing) = self.submissions.get(&(dedup_key.to_string(), seq)) {
            return (existing.logical_id.clone(), existing.ids.clone(), true);
        }
        let logical_id = self.generate_id().replacen("msg_", "logical_", 1);
        let ids: Vec<String> = (0..count).map(|_| self.generate_id()).collect();
        let sub = Submission { logical_id: logical_id.clone(), ids: ids.clone(), replayed: false };
        self.submissions.insert((dedup_key.to_string(), seq), sub);
        self.high_water
            .entry(dedup_key.to_string())
            .and_modify(|h| *h = (*h).max(seq))
            .or_insert(seq);
        self.trim_window(dedup_key);
        (logical_id, ids, false)
    }

    /// 某 logical 的全部子消息终态后，回收其提交身份。
    pub fn reclaim_logical(&mut self, logical_id: &str) {
        self.submissions.retain(|_, s| s.logical_id != logical_id);
    }

    /// 退役一个去重键：清掉其提交身份与高水位。
    pub fn retire(&mut self, dedup_key: &str) {
        self.submissions.retain(|(k, _), _| k != dedup_key);
        self.high_water.remove(dedup_key);
    }

    /// 有界化：每个去重键只保留最近 `REPLAY_WINDOW` 个 seq 的提交身份。
    fn trim_window(&mut self, dedup_key: &str) {
        let keys: Vec<u64> = self
            .submissions
            .keys()
            .filter(|(k, _)| k == dedup_key)
            .map(|(_, s)| *s)
            .collect();
        if keys.len() > REPLAY_WINDOW {
            let mut sorted = keys;
            sorted.sort_unstable();
            let keep_from = sorted[sorted.len() - REPLAY_WINDOW];
            let stale: Vec<(String, u64)> = self
                .submissions
                .keys()
                .filter(|(k, s)| k == dedup_key && *s < keep_from)
                .cloned()
                .collect();
            for k in stale {
                self.submissions.remove(&k);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.submissions.len()
    }
}

pub struct Nucleus {
    active: BTreeMap<String, Message>,
    audit: Vec<AuditRecord>,
    cache: SubmissionCache,
    store: Option<Box<dyn Store>>,
}

impl Default for Nucleus {
    fn default() -> Self {
        Self::new()
    }
}

impl Nucleus {
    pub fn new() -> Self {
        Nucleus {
            active: BTreeMap::new(),
            audit: Vec::new(),
            cache: SubmissionCache::new(),
            store: None,
        }
    }

    /// 挂一个持久层 backend（Durable 活行与审计由它保存）。
    pub fn with_store<S: Store + 'static>(mut self, store: S) -> Self {
        self.store = Some(Box::new(store));
        self
    }

    /// 从持久层恢复（启动恢复这个出口的推导）：
    /// 载入 Durable 活行与审计；把非终态活行收敛为终态（`execution_outcome: unknown`）。
    /// 去重窗口与高水位是每代际状态，重启后从空开始——这正是"内核重启清会话"的语义。
    pub fn recover<S: Store + 'static>(store: S) -> Self {
        let mut n = Nucleus::new();
        for a in store.load_audit() {
            n.audit.push(a);
        }
        for m in store.load_active() {
            n.active.insert(m.message_id.clone(), m);
        }
        n.store = Some(Box::new(store));
        let ids: Vec<String> = n
            .active
            .values()
            .filter(|m| !m.status.is_terminal())
            .map(|m| m.message_id.clone())
            .collect();
        for id in ids {
            let _ = n.advance(
                &id,
                ContractStatus::ExecutionFailed,
                Some(serde_json::json!({"error":"kernel_restarted","execution_outcome":"unknown"})),
            );
        }
        n
    }

    /// 消息级基础操作：收进一条消息的活跃行。
    /// 这是核的最小粒度——提交（多 target、去重、重放）在 `admit` 里组合它。
    /// 换骨时旧 API 的 `record_initial_target_state` 就映射到这里（解法 B）。
    pub fn put(
        &mut self,
        message_id: String,
        logical_id: String,
        source: String,
        seq: u64,
        target: String,
        message_type: String,
        policy: DeliveryPolicy,
        status: ContractStatus,
        data: Option<serde_json::Value>,
    ) {
        self.active.insert(
            message_id.clone(),
            Message {
                message_id,
                logical_id,
                source,
                seq,
                target,
                message_type,
                policy,
                status,
                data,
            },
        );
    }

    /// 收进一个提交。`dedup_key` 是 source 的去重键（`{domain}#{generation}`），
    /// `source` 是纯域 id；二者显式分离，绝不混用。
    pub fn admit(
        &mut self,
        dedup_key: &str,
        source: &str,
        seq: u64,
        targets: &[String],
        message_type: &str,
        policy: DeliveryPolicy,
        data: Option<serde_json::Value>,
    ) -> Submission {
        let (logical_id, ids, replayed) = self.cache.allocate(dedup_key, seq, targets.len());
        if replayed {
            return Submission { logical_id, ids, replayed: true };
        }
        for (target, id) in targets.iter().zip(&ids) {
            self.put(
                id.clone(),
                logical_id.clone(),
                source.to_string(),
                seq,
                target.clone(),
                message_type.to_string(),
                policy,
                ContractStatus::Sent,
                data.clone(),
            );
        }
        if policy == DeliveryPolicy::Durable {
            if let Some(store) = self.store.as_mut() {
                for id in &ids {
                    if let Some(m) = self.active.get(id) {
                        store.save_active(m);
                    }
                }
            }
        }
        Submission { logical_id, ids, replayed: false }
    }

    /// 单调推进一条消息；推进到终态即自动"转审计或清除 + 回收去重窗口"。
    pub fn advance(
        &mut self,
        message_id: &str,
        status: ContractStatus,
        data: Option<serde_json::Value>,
    ) -> Advance {
        let Some(current) = self.active.get(message_id).cloned() else {
            return Advance::Unknown;
        };
        if current.status.is_terminal() || status.stage_rank() <= current.status.stage_rank() {
            return Advance::DuplicateOrStale;
        }
        let mut next = current.clone();
        next.status = status;
        if data.is_some() {
            next.data = data;
        }
        if status.is_terminal() {
            // 终态：Durable 转审计，其余清除。
            if next.policy == DeliveryPolicy::Durable {
                let rec = AuditRecord {
                    message_id: next.message_id.clone(),
                    logical_id: next.logical_id.clone(),
                    source: next.source.clone(),
                    target: next.target.clone(),
                    status,
                    data: next.data.clone(),
                };
                self.audit.push(rec.clone());
                if let Some(store) = self.store.as_mut() {
                    store.append_audit(&rec);
                    store.delete_active(message_id);
                }
            }
            self.active.remove(message_id);
            // 回收该 logical 的去重窗口：所有子消息都终态后删除提交身份。
            let logical = next.logical_id.clone();
            let all_children_done = !self.active.values().any(|m| m.logical_id == logical);
            if all_children_done {
                self.cache.reclaim_logical(&logical);
            }
        } else {
            self.active.insert(message_id.to_string(), next.clone());
        }
        Advance::Advanced {
            source: next.source.clone(),
            status,
            data: next.data.clone(),
            target: next.target.clone(),
        }
    }

    /// 退役一个 source（去重键）：其活跃行批量收敛为终态。
    pub fn retire_source(&mut self, dedup_key: &str) {
        let ids: Vec<String> = self
            .active
            .iter()
            .filter(|(_, m)| m.source == dedup_key)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            let _ = self.advance(
                &id,
                ContractStatus::ExecutionFailed,
                Some(serde_json::json!({"error":"source_retired","execution_outcome":"unknown"})),
            );
        }
        self.cache.retire(dedup_key);
    }

    /// 读活跃或审计结果（活行优先）。
    pub fn get(&self, message_id: &str) -> Option<&Message> {
        self.active.get(message_id)
    }

    pub fn get_audit(&self, message_id: &str) -> Option<&AuditRecord> {
        self.audit.iter().find(|r| r.message_id == message_id)
    }

    /// 有界性/唯一性诊断（I3）。
    pub fn retention(&self) -> (usize, usize, usize) {
        (self.active.len(), self.cache.len(), self.audit.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn target(name: &str) -> String {
        name.to_string()
    }

    #[test]
    fn i1_uniqueness_active_and_audit_never_coexist() {
        let mut n = Nucleus::new();
        let sub = n.admit("d#1", "d", 1, &[target("w.run")], "Run", DeliveryPolicy::Durable, None);
        let id = &sub.ids[0];
        assert!(n.get(id).is_some());
        assert!(n.get_audit(id).is_none());
        n.advance(id, ContractStatus::Completed, Some(json!({"ok":true})));
        // 终态：活跃清除，审计追加，二者不同时存在
        assert!(n.get(id).is_none());
        assert!(n.get_audit(id).is_some());
    }

    #[test]
    fn i2_monotonic_terminal_once() {
        let mut n = Nucleus::new();
        let sub = n.admit("d#1", "d", 1, &[target("w.run")], "Run", DeliveryPolicy::Durable, None);
        let id = &sub.ids[0];
        assert!(matches!(n.advance(id, ContractStatus::Received, None), Advance::Advanced { .. }));
        // 回退被吸收
        assert!(matches!(n.advance(id, ContractStatus::Received, None), Advance::DuplicateOrStale));
        assert!(matches!(n.advance(id, ContractStatus::Completed, Some(json!({"ok":true}))), Advance::Advanced { .. }));
        // 终态后再推进：消息已离开活跃集合，不会再次推进（Unknown 与 DuplicateOrStale 都表示"无状态变更"）。
        assert!(!matches!(n.advance(id, ContractStatus::ExecutionFailed, None), Advance::Advanced { .. }));
        assert!(n.get(id).is_none());
        assert!(n.get_audit(id).is_some());
    }

    #[test]
    fn i3_bounded_active_and_dedup() {
        let mut n = Nucleus::new();
        for seq in 1..=10_000u64 {
            let sub = n.admit("d#1", "d", seq, &[target("w.run")], "Run", DeliveryPolicy::Volatile, None);
            let id = sub.ids[0].clone();
            n.advance(&id, ContractStatus::Completed, None);
        }
        let (active, submissions, audit) = n.retention();
        assert_eq!(active, 0, "terminal volatile leaves no active row");
        assert!(submissions <= REPLAY_WINDOW, "dedup window bounded: {submissions}");
        assert_eq!(audit, 0, "volatile never audits");
    }

    #[test]
    fn replay_within_window_reuses_identity() {
        let mut n = Nucleus::new();
        let first = n.admit("d#1", "d", 7, &[target("w.run")], "Run", DeliveryPolicy::Volatile, None);
        let replay = n.admit("d#1", "d", 7, &[target("w.run")], "Run", DeliveryPolicy::Volatile, None);
        assert!(replay.replayed);
        assert_eq!(replay.logical_id, first.logical_id);
        assert_eq!(replay.ids, first.ids);
    }

    #[test]
    fn restart_closes_inflight_and_preserves_audit() {
        let mut store = VecStore::default();
        store.audit.push(AuditRecord {
            message_id: "m1".into(),
            logical_id: "l1".into(),
            source: "d".into(),
            target: "w.run".into(),
            status: ContractStatus::Completed,
            data: Some(json!({"ok": true})),
        });
        store.active.push(Message {
            message_id: "m2".into(),
            logical_id: "l2".into(),
            source: "d".into(),
            seq: 2,
            target: "w.run".into(),
            message_type: "Run".into(),
            policy: DeliveryPolicy::Durable,
            status: ContractStatus::Sent,
            data: None,
        });

        let n = Nucleus::recover(store);
        // 在途 Durable 被收敛为终态并转审计
        assert!(n.get("m2").is_none());
        let aud = n.get_audit("m2").expect("inflight durable closed to audit");
        assert_eq!(aud.status, ContractStatus::ExecutionFailed);
        assert_eq!(aud.data.as_ref().unwrap()["execution_outcome"], "unknown");
        // 历史审计保留
        assert_eq!(n.get_audit("m1").unwrap().status, ContractStatus::Completed);
        // 活跃集合为空
        assert_eq!(n.retention().0, 0);
    }
}
