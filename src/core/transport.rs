//! Transport —— 传输层：核的一个纯投影。
//!
//! 它不持有任何消息状态，只观察核的两个操作，产出确定有序的帧：
//!   - `admit`    → 每个目标一条 `Deliver` 帧；
//!   - `advance`  → 一条 `Receipt` 帧发给来源。
//!
//! 帧顺序由**每通道单调 seq**决定，与消息策略（Durable/Volatile/Ephemeral）无关。
//! 这从结构上消灭了旧实现"内存 spool 先于 SQLite 行出队"导致的跨策略乱序（审计 A2）。

use crate::core::nucleus::{Advance, Nucleus, Submission};
use crate::core::protocol::{ContractStatus, DeliveryPolicy};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Deliver {
        channel: String,
        seq: u64,
        message_id: String,
        target: String,
    },
    Receipt {
        channel: String,
        seq: u64,
        message_id: String,
        status: ContractStatus,
    },
}

pub struct Transport {
    nucleus: Nucleus,
    per_channel_seq: BTreeMap<String, u64>,
    /// 确定性帧日志：按入队顺序记录，用于验证 FIFO。
    pub log: Vec<Frame>,
}

impl Default for Transport {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport {
    pub fn new() -> Self {
        Transport {
            nucleus: Nucleus::new(),
            per_channel_seq: BTreeMap::new(),
            log: Vec::new(),
        }
    }

    pub fn with_store<S: crate::core::nucleus::Store + 'static>(self, store: S) -> Self {
        Transport { nucleus: self.nucleus.with_store(store), ..self }
    }

    fn next_seq(&mut self, channel: &str) -> u64 {
        let seq = self.per_channel_seq.entry(channel.to_string()).or_insert(0);
        *seq += 1;
        *seq
    }

    /// admit 的一个投影：分配身份 + 按目标产出 Deliver 帧。
    pub fn admit(
        &mut self,
        dedup_key: &str,
        source: &str,
        seq: u64,
        targets: &[String],
        message_type: &str,
        policy: DeliveryPolicy,
        data: Option<Value>,
    ) -> Submission {
        let sub = self
            .nucleus
            .admit(dedup_key, source, seq, targets, message_type, policy, data);
        if !sub.replayed {
            for (target, id) in targets.iter().zip(&sub.ids) {
                let fseq = self.next_seq(target);
                self.log.push(Frame::Deliver {
                    channel: target.clone(),
                    seq: fseq,
                    message_id: id.clone(),
                    target: target.clone(),
                });
            }
        }
        sub
    }

    /// advance 的一个投影：状态推进 + 终态时给来源产出 Receipt 帧。
    pub fn advance(&mut self, message_id: &str, status: ContractStatus, data: Option<Value>) -> Advance {
        let adv = self.nucleus.advance(message_id, status, data);
        if let Advance::Advanced { source, status, .. } = &adv {
            let fseq = self.next_seq(source);
            self.log.push(Frame::Receipt {
                channel: source.clone(),
                seq: fseq,
                message_id: message_id.to_string(),
                status: *status,
            });
        }
        adv
    }

    /// 某通道的帧序列（FIFO 验证用）。
    pub fn frames_for(&self, channel: &str) -> Vec<&Frame> {
        self.log.iter().filter(|f| frame_channel(f) == channel).collect()
    }
}

fn frame_channel(frame: &Frame) -> &str {
    match frame {
        Frame::Deliver { channel, .. } => channel,
        Frame::Receipt { channel, .. } => channel,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn t(name: &str) -> String {
        name.to_string()
    }

    /// 跨策略 FIFO：Durable A 先于 Volatile B 送达同一目标，与策略无关（审计 A2 的结构性消除）。
    #[test]
    fn cross_policy_fifo_is_preserved() {
        let mut tr = Transport::new();
        let a = tr.admit("s#1", "s", 1, &[t("w.run")], "Run", DeliveryPolicy::Durable, None);
        let b = tr.admit("s#1", "s", 2, &[t("w.run")], "Run", DeliveryPolicy::Volatile, None);

        let delivers: Vec<&Frame> = tr
            .frames_for("w.run")
            .into_iter()
            .filter(|f| matches!(f, Frame::Deliver { .. }))
            .collect();
        assert_eq!(delivers.len(), 2);
        match (delivers[0], delivers[1]) {
            (Frame::Deliver { message_id: a_id, seq: aseq, .. }, Frame::Deliver { message_id: b_id, seq: bseq, .. }) => {
                assert_eq!(*a_id, a.ids[0]);
                assert_eq!(*b_id, b.ids[0]);
                assert!(aseq < bseq, "per-channel seq must be monotonic");
            }
            _ => panic!("expected two delivers"),
        }

        // 终态回执也走同一通道的单调 seq，顺序不因策略变化。
        tr.advance(&a.ids[0], ContractStatus::Completed, Some(json!({"ok": true})));
        tr.advance(&b.ids[0], ContractStatus::Completed, None);
        let receipts: Vec<&Frame> = tr
            .frames_for("s")
            .into_iter()
            .filter(|f| matches!(f, Frame::Receipt { .. }))
            .collect();
        assert_eq!(receipts.len(), 2);
        let (r0, r1) = (receipts[0], receipts[1]);
        assert!(frame_seq(r0) < frame_seq(r1));
    }

    fn frame_seq(f: &Frame) -> u64 {
        match f {
            Frame::Deliver { seq, .. } | Frame::Receipt { seq, .. } => *seq,
        }
    }
}
