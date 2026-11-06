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

use crate::core::protocol::{ContractStatus, DeliveryPolicy, now_millis};

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
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    pub message_id: String,
    pub logical_id: String,
    pub source: String,
    pub seq: u64,
    pub target: String,
    pub message_type: String,
    pub policy: DeliveryPolicy,
    pub status: ContractStatus,
    pub data: Option<serde_json::Value>,
    pub created_at: i64,
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

/// 一次提交身份分配的结果。
///
/// `Stale` 是必须存在的第三态：`seq` 已滑出窗口（身份被回收）但仍低于高水位，
/// 说明它是一次**过期重放**，而不是新提交。此时绝不能分发新身份，否则同一次提交
/// 会被执行两次（`docs/ledger-core-rewrite.md` §2:79 明确要求高水位参与判定）。
pub enum Allocation {
    /// 窗口内未命中：分配了全新身份。
    New { logical_id: String, ids: Vec<String> },
    /// 窗口内命中：复用原身份，不产生新的活跃行。
    Replay { logical_id: String, ids: Vec<String> },
    /// 过期重放：`seq <= high_water` 但身份已离开窗口。
    Stale { high_water: u64 },
}

/// SubmissionCache —— 提交身份/去重/高水位：核外的一个推导。
/// 旧 `allocate_submission` 映射到这里；它不持有消息状态，只记录"哪次提交对应哪些 id"。
pub struct SubmissionCache {
    /// 去重窗口 `(dedup_key, seq) -> Submission`，按 seq 有界。
    submissions: BTreeMap<(String, u64), Submission>,
    /// 单调高水位：会话行被回收后仍记住"到过的最高 seq"，用于识别滑出窗口的过期重放。
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

    /// 去重判定：窗口命中 → 复用；否则若该提交要求去重且 `seq` 低于高水位 → 过期。
    /// 返回 `None` 表示可以安全分配新身份。
    ///
    /// `dedup_required == false`（纯 Ephemeral 提交）不做过期判定：宪法 C3:56 规定
    /// 瞬态消息"同一序号再次提交是一次新瞬态投递"，不能当成重放拒绝。
    fn replay(&self, dedup_key: &str, seq: u64, dedup_required: bool) -> Option<Allocation> {
        if seq == 0 {
            return None;
        }
        if let Some(existing) = self.submissions.get(&(dedup_key.to_string(), seq)) {
            return Some(Allocation::Replay {
                logical_id: existing.logical_id.clone(),
                ids: existing.ids.clone(),
            });
        }
        if dedup_required {
            if let Some(&high_water) = self.high_water.get(dedup_key) {
                if seq <= high_water {
                    return Some(Allocation::Stale { high_water });
                }
            }
        }
        None
    }

    /// 分配提交身份：去重命中窗口则复用原身份，否则分配新 id。
    /// `seq == 0` 表示调用方不提供序号去重，每次都分配新身份（旧 `try_allocate_submission` 语义）。
    pub fn allocate(
        &mut self,
        dedup_key: &str,
        seq: u64,
        count: usize,
        dedup_required: bool,
    ) -> Allocation {
        if let Some(settled) = self.replay(dedup_key, seq, dedup_required) {
            return settled;
        }
        let logical_id = self.generate_id().replacen("msg_", "logical_", 1);
        let ids: Vec<String> = (0..count).map(|_| self.generate_id()).collect();
        self.insert_submission(dedup_key, seq, logical_id.clone(), ids.clone(), dedup_required);
        Allocation::New { logical_id, ids }
    }

    /// 用调用方预生成的全局唯一 id 分配（facade 走这条路径，id 由 `KernelLedger::generate_message_id` 生成）。
    pub fn allocate_with(
        &mut self,
        dedup_key: &str,
        seq: u64,
        logical_id: String,
        ids: Vec<String>,
        dedup_required: bool,
    ) -> Allocation {
        if let Some(settled) = self.replay(dedup_key, seq, dedup_required) {
            return settled;
        }
        self.insert_submission(dedup_key, seq, logical_id.clone(), ids.clone(), dedup_required);
        Allocation::New { logical_id, ids }
    }

    fn insert_submission(
        &mut self,
        dedup_key: &str,
        seq: u64,
        logical_id: String,
        ids: Vec<String>,
        dedup_required: bool,
    ) {
        if seq == 0 {
            return;
        }
        self.submissions
            .insert((dedup_key.to_string(), seq), Submission { logical_id, ids, replayed: false });
        if dedup_required {
            self.high_water
                .entry(dedup_key.to_string())
                .and_modify(|h| *h = (*h).max(seq))
                .or_insert(seq);
        }
        self.trim_window(dedup_key);
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

/// 核只做状态机：持有**活跃行**与提交身份，不持有任何历史。
///
/// 终态结果的唯一权威是 SQLite 的 `audit` 表（宪法 C3:54：历史身份及结果直接查 SQLite，
/// 不建立全量内存索引，不把全部历史结果一次性展开到内存）。因此这里没有 `audit` 字段，
/// 也没有持久层 backend——持久化完全由 facade（`KernelLedger`）在核外完成。
pub struct Nucleus {
    active: BTreeMap<String, Message>,
    cache: SubmissionCache,
}

impl Default for Nucleus {
    fn default() -> Self {
        Self::new()
    }
}

impl Nucleus {
    pub fn new() -> Self {
        Nucleus { active: BTreeMap::new(), cache: SubmissionCache::new() }
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
        let now = now_millis();
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
                created_at: now,
                updated_at: now,
            },
        );
    }

    /// 只分配提交身份，不建活跃行（旧 `allocate_submission` 的核级映射）。
    pub fn allocate_submission(
        &mut self,
        dedup_key: &str,
        seq: u64,
        count: usize,
        dedup_required: bool,
    ) -> Allocation {
        self.cache.allocate(dedup_key, seq, count, dedup_required)
    }

    /// 用调用方预生成的全局唯一 id 分配（facade 的 `try_allocate_submission` 走这里）。
    pub fn allocate_submission_with_ids(
        &mut self,
        dedup_key: &str,
        seq: u64,
        logical_id: String,
        ids: Vec<String>,
        dedup_required: bool,
    ) -> Allocation {
        self.cache.allocate_with(dedup_key, seq, logical_id, ids, dedup_required)
    }

    /// 把一条活行就地标记为失败，但**保留在活跃集合**。
    ///
    /// 用于"持久化失败"：这条结果没有落盘，所以它不是历史（不能进 `audit`），
    /// 但它必须继续可读，否则来源域连失败都无法重放（宪法 C3:48：显式失败，不静默兜底）。
    /// 退役时由 `retire_source` 显式清除，避免驻留泄漏。
    pub fn mark_live_failure(&mut self, message_id: &str, data: Option<serde_json::Value>) -> bool {
        let Some(current) = self.active.get_mut(message_id) else {
            return false;
        };
        current.status = ContractStatus::ExecutionFailed;
        current.updated_at = now_millis();
        if data.is_some() {
            current.data = data;
        }
        true
    }

    /// 由子消息 id 反查提交 logical_id。
    pub fn logical_id_for(&self, message_id: &str) -> Option<String> {
        self.cache
            .submissions
            .values()
            .find(|s| s.ids.iter().any(|id| id == message_id))
            .map(|s| s.logical_id.clone())
    }

    pub fn active_snapshot(&self) -> Vec<Message> {
        self.active.values().cloned().collect()
    }

    /// 退役去重键（只清提交身份/高水位，不动消息状态）。
    pub fn retire_dedup(&mut self, dedup_key: &str) {
        self.cache.retire(dedup_key);
    }

    /// 收进一个提交。`dedup_key` 是 source 的去重键（`{domain}#{generation}`），
    /// `source` 是纯域 id；二者显式分离，绝不混用。
    ///
    /// `Err(high_water)` 表示这是一次已滑出窗口的过期重放：调用方必须显式拒绝，
    /// 不能把它当作新提交重新执行。
    pub fn admit(
        &mut self,
        dedup_key: &str,
        source: &str,
        seq: u64,
        targets: &[String],
        message_type: &str,
        policy: DeliveryPolicy,
        data: Option<serde_json::Value>,
    ) -> Result<Submission, u64> {
        let dedup_required = policy != DeliveryPolicy::Ephemeral;
        let (logical_id, ids) =
            match self.cache.allocate(dedup_key, seq, targets.len(), dedup_required) {
                Allocation::New { logical_id, ids } => (logical_id, ids),
                Allocation::Replay { logical_id, ids } => {
                    return Ok(Submission { logical_id, ids, replayed: true });
                }
                Allocation::Stale { high_water } => return Err(high_water),
            };
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
        Ok(Submission { logical_id, ids, replayed: false })
    }

    /// 单调推进一条消息；推进到终态即清除活行并回收去重窗口。
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
        next.updated_at = now_millis();
        if data.is_some() {
            next.data = data;
        }
        if status.is_terminal() {
            // 终态：活行立即清除。Durable 的历史结果由 facade 在同一个事务里写入 `audit` 表，
            // 核内不保留任何历史副本（权威唯一性：核只存活跃，盘只存历史）。
            self.active.remove(message_id);
            // 回收该 logical 的去重窗口：仅 Ephemeral 到达终态即回收；
            // Durable/Volatile 保留去重与重放契约，由 REPLAY_WINDOW 有界化（宪法 C3:56）。
            if next.policy == DeliveryPolicy::Ephemeral {
                self.reclaim_ephemeral(&next.logical_id);
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

    /// 瞬态身份回收：该 logical 已无在途子消息时，释放其提交身份（含去重 seq）。
    ///
    /// 与 `advance` 终态分支同一规则，供**入口即终态**（未建活跃行、不经过 `advance`）
    /// 的路径复用。C3:56：瞬态身份随在途生命周期回收，完成后同序号重提是新瞬态投递；
    /// 在途期间仍保留身份，重提复用原身份。
    pub fn reclaim_ephemeral(&mut self, logical_id: &str) {
        if !self.active.values().any(|m| m.logical_id == logical_id) {
            self.cache.reclaim_logical(logical_id);
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
            // 未落盘的失败行可能已经是终态、无法再被 advance 推进；退役时必须显式清掉。
            self.active.remove(&id);
        }
        self.cache.retire(dedup_key);
    }

    /// 读活跃行（历史结果不在这里，按需查 SQLite）。
    pub fn get(&self, message_id: &str) -> Option<&Message> {
        self.active.get(message_id)
    }

    /// 有界性诊断（I3）：(活跃行, 提交身份)。
    pub fn retention(&self) -> (usize, usize) {
        (self.active.len(), self.cache.len())
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
    fn i1_terminal_removes_the_active_row() {
        let mut n = Nucleus::new();
        let sub = n.admit("d#1", "d", 1, &[target("w.run")], "Run", DeliveryPolicy::Durable, None).unwrap();
        let id = &sub.ids[0];
        assert!(n.get(id).is_some());
        n.advance(id, ContractStatus::Completed, Some(json!({"ok":true})));
        // 终态：活行立即离开核。历史结果由 facade 写盘，核内不留副本。
        assert!(n.get(id).is_none());
        assert_eq!(n.retention().0, 0);
    }

    #[test]
    fn i2_monotonic_terminal_once() {
        let mut n = Nucleus::new();
        let sub = n.admit("d#1", "d", 1, &[target("w.run")], "Run", DeliveryPolicy::Durable, None).unwrap();
        let id = &sub.ids[0];
        assert!(matches!(n.advance(id, ContractStatus::Received, None), Advance::Advanced { .. }));
        // 回退被吸收
        assert!(matches!(n.advance(id, ContractStatus::Received, None), Advance::DuplicateOrStale));
        assert!(matches!(n.advance(id, ContractStatus::Completed, Some(json!({"ok":true}))), Advance::Advanced { .. }));
        // 终态后再推进：消息已离开活跃集合，不会再次推进（Unknown 与 DuplicateOrStale 都表示"无状态变更"）。
        assert!(!matches!(n.advance(id, ContractStatus::ExecutionFailed, None), Advance::Advanced { .. }));
        assert!(n.get(id).is_none());
    }

    #[test]
    fn i3_bounded_active_and_dedup() {
        let mut n = Nucleus::new();
        for seq in 1..=10_000u64 {
            let sub = n.admit("d#1", "d", seq, &[target("w.run")], "Run", DeliveryPolicy::Volatile, None).unwrap();
            let id = sub.ids[0].clone();
            n.advance(&id, ContractStatus::Completed, None);
        }
        let (active, submissions) = n.retention();
        assert_eq!(active, 0, "terminal volatile leaves no active row");
        assert!(submissions <= REPLAY_WINDOW, "dedup window bounded: {submissions}");
    }

    #[test]
    fn replay_within_window_reuses_identity() {
        let mut n = Nucleus::new();
        let first = n.admit("d#1", "d", 7, &[target("w.run")], "Run", DeliveryPolicy::Volatile, None).unwrap();
        let replay = n.admit("d#1", "d", 7, &[target("w.run")], "Run", DeliveryPolicy::Volatile, None).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.logical_id, first.logical_id);
        assert_eq!(replay.ids, first.ids);
    }

    /// 滑出窗口的过期重放必须被拒绝，不能重新分配身份（高水位参与判定）。
    #[test]
    fn stale_seq_beyond_the_window_is_rejected() {
        let mut n = Nucleus::new();
        for seq in 1..=REPLAY_WINDOW as u64 + 1 {
            n.admit("d#1", "d", seq, &[target("w.run")], "Run", DeliveryPolicy::Durable, None).unwrap();
        }
        // seq=1 已被窗口淘汰但低于高水位
        let stale = n.admit("d#1", "d", 1, &[target("w.run")], "Run", DeliveryPolicy::Durable, None);
        assert!(
            matches!(stale, Err(high) if high == REPLAY_WINDOW as u64 + 1),
            "stale seq must be rejected with the high-water mark"
        );
        // 纯 Ephemeral 不受高水位限制（C3:56）
        assert!(n.admit("e#1", "e", 1, &[target("w.run")], "Run", DeliveryPolicy::Ephemeral, None).is_ok());
    }
}
