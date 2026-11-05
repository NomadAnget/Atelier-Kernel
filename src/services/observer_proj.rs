//! Observer —— 观察流：`advance` 流的一个有界、可丢的只读投影。
//!
//! 它从不成为真相源：只记录 `advance` 已经发生的事实（status/data），供诊断与
//! "结果被回收后从观察器还原真实终态"用。约束（C2/C3）：历史按条数与字节有界；
//! 慢消费者/超预算事件可丢，但不阻塞业务、不反写状态。

use crate::core::protocol::ContractStatus;
use serde_json::Value;
use std::collections::VecDeque;

const MAX_EVENTS: usize = 8_192;
const MAX_BYTES: usize = 16 * 1024 * 1024; // 与帧上限一致，避免一个快照撑爆管道

#[derive(Debug, Clone)]
pub struct ObservationEvent {
    pub message_id: String,
    pub target: String,
    pub status: ContractStatus,
    pub data: Option<Value>,
}

pub struct Observer {
    events: VecDeque<ObservationEvent>,
    bytes: usize,
}

impl Default for Observer {
    fn default() -> Self {
        Self::new()
    }
}

impl Observer {
    pub fn new() -> Self {
        Observer { events: VecDeque::new(), bytes: 0 }
    }

    /// 记录一次状态推进。只读：不改变消息状态本身。
    pub fn observe(&mut self, message_id: &str, target: &str, status: ContractStatus, data: Option<Value>) {
        let event = ObservationEvent {
            message_id: message_id.to_string(),
            target: target.to_string(),
            status,
            data,
        };
        let size = event_bytes(&event);
        self.events.push_back(event);
        self.bytes += size;
        // 有界：超预算丢最旧
        while self.events.len() > MAX_EVENTS || self.bytes > MAX_BYTES {
            if let Some(front) = self.events.pop_front() {
                self.bytes -= event_bytes(&front);
            }
        }
    }

    /// 某消息的观测历史（结果被回收后，从这里还原真实终态）。
    pub fn events_for_message(&self, message_id: &str) -> Vec<ObservationEvent> {
        self.events
            .iter()
            .filter(|e| e.message_id == message_id)
            .cloned()
            .collect()
    }

    /// 最近的终态观测（若有）。
    pub fn last_terminal(&self, message_id: &str) -> Option<ObservationEvent> {
        self.events_for_message(message_id)
            .into_iter()
            .rev()
            .find(|e| e.status.is_terminal())
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }
}

fn event_bytes(e: &ObservationEvent) -> usize {
    e.message_id.len()
        + e.target.len()
        + e.data.as_ref().map_or(0, |d| d.to_string().len())
        + 8
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn observer_is_bounded_and_lossy() {
        let mut o = Observer::new();
        for i in 0..20_000u64 {
            o.observe(&format!("m{i}"), "w.run", ContractStatus::Completed, Some(json!({"n": i})));
        }
        assert!(o.len() <= MAX_EVENTS, "event count bounded: {}", o.len());
        // 最旧的被丢弃，最新的保留
        assert!(o.events_for_message("m19999").last().is_some());
        assert!(o.events_for_message("m0").is_empty());
    }

    #[test]
    fn observer_restores_terminal_truth_after_reap() {
        let mut o = Observer::new();
        o.observe("m1", "w.run", ContractStatus::Sent, None);
        o.observe("m1", "w.run", ContractStatus::Completed, Some(json!({"ok": true})));
        // 消息行已被回收（核里 active 删了），但观察器仍持有真实终态
        let last = o.last_terminal("m1").expect("observer keeps terminal truth");
        assert_eq!(last.status, ContractStatus::Completed);
        assert_eq!(last.data, Some(json!({"ok": true})));
    }
}
