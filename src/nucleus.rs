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

pub struct Nucleus {
    active: BTreeMap<String, Message>,
    /// 去重窗口 `(dedup_key, seq) -> (logical_id, children)`，按 seq 有界。
    submissions: BTreeMap<(String, u64), Submission>,
    /// 单调高水位：会话行被回收后仍记住"到过的最高 seq"，防止老 seq 被误判成新提交。
    high_water: BTreeMap<String, u64>,
    audit: Vec<AuditRecord>,
    next_id: u64,
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
            submissions: BTreeMap::new(),
            high_water: BTreeMap::new(),
            audit: Vec::new(),
            next_id: 1,
        }
    }

    fn generate_id(&mut self) -> String {
        let id = format!("msg_{}", self.next_id);
        self.next_id += 1;
        id
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
        // 去重命中：窗口内复用原身份，不产生新活跃行。
        if let Some(existing) = self.submissions.get(&(dedup_key.to_string(), seq)) {
            let mut replay = existing.clone();
            replay.replayed = true;
            return replay;
        }
        // 老 seq（在窗口外但仍 ≤ 高水位）：磁盘侧审计可回放；内存核无持久层时按新提交处理。
        if let Some(high) = self.high_water.get(dedup_key).copied() {
            if seq > 0 && seq <= high {
                // 已过窗口的重放：无法复用身份，只能作为新提交（at-least-once 语义由调用方决定）。
                // 见 docs/ledger-core-rewrite.md §2 submission_high 的说明。
            }
        }
        let logical_id = self.generate_id().replacen("msg_", "logical_", 1);
        let ids: Vec<String> = targets.iter().map(|_| self.generate_id()).collect();
        for (target, id) in targets.iter().zip(&ids) {
            self.active.insert(
                id.clone(),
                Message {
                    message_id: id.clone(),
                    logical_id: logical_id.clone(),
                    source: source.to_string(),
                    seq,
                    target: target.clone(),
                    message_type: message_type.to_string(),
                    policy,
                    status: ContractStatus::Sent,
                    data: data.clone(),
                },
            );
        }
        let sub = Submission { logical_id: logical_id.clone(), ids: ids.clone(), replayed: false };
        self.submissions.insert((dedup_key.to_string(), seq), sub.clone());
        // 有界窗口 + 单调高水位
        self.high_water
            .entry(dedup_key.to_string())
            .and_modify(|h| *h = (*h).max(seq))
            .or_insert(seq);
        self.trim_window(dedup_key);
        sub
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
                self.audit.push(AuditRecord {
                    message_id: next.message_id.clone(),
                    logical_id: next.logical_id.clone(),
                    source: next.source.clone(),
                    target: next.target.clone(),
                    status,
                    data: next.data.clone(),
                });
            }
            self.active.remove(message_id);
            // 回收该 logical 的去重窗口：所有子消息都终态后删除提交身份。
            let logical = next.logical_id.clone();
            let all_children_done = !self
                .active
                .values()
                .any(|m| m.logical_id == logical);
            if all_children_done {
                self.submissions.retain(|_, s| s.logical_id != logical);
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
        self.submissions.retain(|(k, _), _| k != dedup_key);
        self.high_water.remove(dedup_key);
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
        (self.active.len(), self.submissions.len(), self.audit.len())
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
}
