use crate::core::protocol::{ContractStatus, ObservationEvent, now_millis};
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
const MAX_EVENT: usize = 262_144;
const MAX_HISTORY_BYTES: usize = 16 * 1024 * 1024;
struct History {
    next_id: u64,
    state_revision: u64,
    stream_id: String,
    domain_states: BTreeMap<String, Value>,
    state_subscribers: Vec<SyncSender<()>>,
    events: VecDeque<(ObservationEvent, usize)>,
    bytes: usize,
    subscribers: Vec<SyncSender<ObservationEvent>>,
}
/// Best-effort observations own no delivery state. One bounded history, one nonblocking fanout.
pub struct KernelObserver {
    state: Mutex<History>,
    max_history: usize,
    /// 订阅者通道满时被丢弃的事件总数：观察流有损，这个计数让它可见而不是静默。
    dropped_total: std::sync::atomic::AtomicU64,
    /// 自启动以来产生的观察事件总数（含随后被丢弃的）。
    emitted_total: std::sync::atomic::AtomicU64,
}
impl Default for KernelObserver {
    fn default() -> Self {
        Self::new(10_000)
    }
}
impl KernelObserver {
    pub fn new(max_history: usize) -> Self {
        Self {
            state: Mutex::new(History {
                next_id: 1,
                state_revision: 0,
                stream_id: uuid::Uuid::new_v4().to_string(),
                domain_states: BTreeMap::new(),
                state_subscribers: Vec::new(),
                events: VecDeque::new(),
                bytes: 0,
                subscribers: Vec::new(),
            }),
            max_history: max_history.max(64),
            dropped_total: std::sync::atomic::AtomicU64::new(0),
            emitted_total: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// 观察流自启动以来丢弃的订阅事件数（订阅者消费不及时导致）。
    pub fn dropped_total(&self) -> u64 {
        self.dropped_total.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 观察流自启动以来产生的观察事件数（含被丢弃的）。
    pub fn emitted_total(&self) -> u64 {
        self.emitted_total.load(std::sync::atomic::Ordering::Relaxed)
    }
    /// Coalesced change notification: one wakeup always represents the latest full state.
    pub(crate) fn subscribe_domain_states(&self) -> Receiver<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.state.lock().state_subscribers.push(tx);
        rx
    }
    pub(crate) fn record_domain_state(
        &self,
        domain: &str,
        generation: u64,
        status: crate::core::protocol::DomainStatus,
        reason: Option<&str>,
    ) {
        let change = {
            let mut s = self.state.lock();
            let status = serde_json::to_value(status).unwrap();
            if s.domain_states
                .get(domain)
                .is_some_and(|v| v["instance_id"] == generation && v["status"] == status)
            {
                return;
            }
            s.state_revision += 1;
            let state = serde_json::json!({"domain_id":domain,"instance_id":generation,"generation":generation,"status":status,"revision":s.state_revision,"timestamp":now_millis(),"reason":reason.map(|s|s.chars().take(1024).collect::<String>())});
            s.domain_states.insert(domain.into(), state.clone());
            s.state_subscribers.retain(|tx| match tx.try_send(()) {
                Ok(()) | Err(TrySendError::Full(_)) => true,
                Err(TrySendError::Disconnected(_)) => false,
            });
            state
        };
        self.emit(
            "kernel",
            "kernel.runtime",
            "",
            "",
            0,
            "DomainStatusChanged",
            ContractStatus::Completed,
            Some(change),
        );
    }
    pub fn domain_state_snapshot(&self) -> ObservationEvent {
        let s = self.state.lock();
        ObservationEvent {
            logical_id: String::new(),
            event_id: s.next_id.saturating_sub(1),
            timestamp: now_millis(),
            source_domain: "kernel".into(),
            target: "kernel.runtime".into(),
            message_id: String::new(),
            seq: 0,
            message_type: "DomainStateSnapshot".into(),
            status: ContractStatus::Completed,
            details: Some(
                serde_json::json!({"stream_id":s.stream_id,"revision":s.state_revision,"domains":s.domain_states.values().collect::<Vec<_>>()}),
            ),
        }
    }
    pub fn subscribe(&self) -> Receiver<ObservationEvent> {
        let (tx, rx) = mpsc::sync_channel(self.max_history.min(64));
        let mut state = self.state.lock();
        if state.subscribers.len() < 64 {
            state.subscribers.push(tx);
        }
        rx
    }
    #[allow(clippy::too_many_arguments)]
    pub fn emit(
        &self,
        source: &str,
        target: &str,
        message_id: &str,
        logical_id: &str,
        seq: u64,
        kind: &str,
        status: ContractStatus,
        details: Option<Value>,
    ) -> ObservationEvent {
        let mut state = self.state.lock();
        let event = ObservationEvent {
            event_id: state.next_id,
            timestamp: now_millis(),
            source_domain: source.into(),
            target: target.into(),
            message_id: message_id.into(),
            logical_id: logical_id.into(),
            seq,
            message_type: kind.into(),
            status,
            details,
        };
        state.next_id = state.next_id.saturating_add(1);
        self.emitted_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Count without materializing another potentially huge serialized business result.
        struct Counter(usize);
        impl std::io::Write for Counter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > MAX_EVENT.saturating_sub(self.0) {
                    return Err(std::io::Error::other("oversized observation"));
                }
                self.0 += bytes.len();
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut size = Counter(0);
        if serde_json::to_writer(&mut size, &event).is_err() {
            return event;
        }
        while state.events.len() >= self.max_history || state.bytes + size.0 > MAX_HISTORY_BYTES {
            if let Some((_, bytes)) = state.events.pop_front() {
                state.bytes -= bytes;
            } else {
                break;
            }
        }
        state.bytes += size.0;
        state.events.push_back((event.clone(), size.0));
        let mut dropped_here = 0u64;
        state.subscribers.retain(|tx| match tx.try_send(event.clone()) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                dropped_here += 1;
                true
            }
            Err(TrySendError::Disconnected(_)) => false,
        });
        if dropped_here > 0 {
            drop(state);
            self.dropped_total
                .fetch_add(dropped_here, std::sync::atomic::Ordering::Relaxed);
        }
        event
    }
    pub fn snapshot(&self) -> Vec<ObservationEvent> {
        self.state
            .lock()
            .events
            .iter()
            .map(|(event, _)| event.clone())
            .collect()
    }
    pub fn events_for_message(&self, id: &str) -> Vec<ObservationEvent> {
        self.state
            .lock()
            .events
            .iter()
            .filter(|(event, _)| event.message_id == id)
            .map(|(event, _)| event.clone())
            .collect()
    }
}

#[cfg(test)]
mod domain_state_tests {
    use super::*;
    use crate::core::protocol::DomainStatus;
    #[test]
    fn state_notifications_coalesce_to_current_snapshot_and_deduplicate_repeated_state() {
        let observer = KernelObserver::new(64);
        let wake = observer.subscribe_domain_states();
        for generation in 1..1000 {
            observer.record_domain_state("worker", generation, DomainStatus::Active, None);
        }
        assert!(wake.try_recv().is_ok());
        assert!(wake.try_recv().is_err());
        let image = observer.domain_state_snapshot().details.unwrap();
        assert_eq!(image["revision"], 999);
        assert_eq!(image["domains"][0]["instance_id"], 999);
        observer.record_domain_state("worker", 999, DomainStatus::Active, None);
        assert!(wake.try_recv().is_err());
        assert_eq!(
            observer.domain_state_snapshot().details.unwrap()["revision"],
            999
        );
    }
}
