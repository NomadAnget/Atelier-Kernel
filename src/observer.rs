use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};

use parking_lot::Mutex;
use serde_json::Value;

use crate::protocol::{ContractStatus, ObservationEvent, now_millis};

/// Dedicated Kernel Message Observation component (`KernelObserver`).
///
/// Observes all message lifecycle transitions (`Sent -> Received -> Started -> Completed`
/// as well as `TargetInvalid`, `ContractInvalid`, and `ExecutionFailed`) in real time
/// without participating in or blocking business logic.
pub struct KernelObserver {
    next_event_id: AtomicU64,
    subscribers: Mutex<Vec<Sender<ObservationEvent>>>,
    history: Mutex<Vec<ObservationEvent>>,
    max_history: usize,
}

impl Default for KernelObserver {
    fn default() -> Self {
        Self::new(10_000)
    }
}

impl KernelObserver {
    /// Creates a new `KernelObserver` with a bounded in-memory ring buffer size.
    pub fn new(max_history: usize) -> Self {
        Self {
            next_event_id: AtomicU64::new(1),
            subscribers: Mutex::new(Vec::new()),
            history: Mutex::new(Vec::new()),
            max_history: max_history.max(64),
        }
    }

    /// Subscribes to the real-time stream of `ObservationEvent`s across the entire kernel.
    pub fn subscribe(&self) -> Receiver<ObservationEvent> {
        let (tx, rx) = mpsc::channel();
        self.subscribers.lock().push(tx);
        rx
    }

    /// Emits a new observation event to all active real-time subscribers and records it in history.
    #[allow(clippy::too_many_arguments)]
    pub fn emit(
        &self,
        source_domain: &str,
        target: &str,
        message_id: &str,
        logical_id: &str,
        seq: u64,
        message_type: &str,
        status: ContractStatus,
        details: Option<Value>,
    ) -> ObservationEvent {
        let event = ObservationEvent {
            event_id: self.next_event_id.fetch_add(1, Ordering::Relaxed),
            timestamp: now_millis(),
            source_domain: source_domain.to_string(),
            target: target.to_string(),
            message_id: message_id.to_string(),
            logical_id: logical_id.to_string(),
            seq,
            message_type: message_type.to_string(),
            status,
            details,
        };

        {
            let mut history = self.history.lock();
            if history.len() >= self.max_history {
                history.remove(0);
            }
            history.push(event.clone());
        }

        // Broadcast to real-time listeners; prune disconnected subscribers automatically.
        let mut subs = self.subscribers.lock();
        subs.retain(|tx| tx.send(event.clone()).is_ok());

        event
    }

    /// Returns a snapshot of all recorded observation events.
    pub fn snapshot(&self) -> Vec<ObservationEvent> {
        self.history.lock().clone()
    }

    /// Returns all recorded observation events for a specific `message_id`.
    pub fn events_for_message(&self, message_id: &str) -> Vec<ObservationEvent> {
        self.history
            .lock()
            .iter()
            .filter(|e| e.message_id == message_id)
            .cloned()
            .collect()
    }
}
