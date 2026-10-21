use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};

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
    subscribers: Mutex<Vec<SyncSender<ObservationEvent>>>,
    history: Mutex<Vec<ObservationEvent>>,
    history_bytes: AtomicU64,
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
            history_bytes: AtomicU64::new(0),
            max_history: max_history.max(64),
        }
    }

    /// Subscribes to the real-time stream of `ObservationEvent`s across the entire kernel.
    pub fn subscribe(&self) -> Receiver<ObservationEvent> {
        let (tx, rx) = mpsc::sync_channel(self.max_history.min(1024));
        self.subscribers.lock().push(tx);
        rx
    }

    /// Best-effort observation: bounded history/queues; full queues or oversized events may drop.
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

        // Observation is lossy: oversized events never enter history or subscriber queues.
        let size = serde_json::to_vec(&event).map_or(usize::MAX, |value| value.len());
        if size > 262_144 { return event; }
        {
            let mut history = self.history.lock();
            let mut bytes = self.history_bytes.load(Ordering::Relaxed) as usize;
            while !history.is_empty() && (history.len() >= self.max_history || bytes + size > 16_777_216) {
                let removed = history.remove(0);
                bytes = bytes.saturating_sub(serde_json::to_vec(&removed).map_or(0, |value| value.len()));
            }
            history.push(event.clone());
            self.history_bytes.store((bytes + size) as u64, Ordering::Relaxed);
        }

        // Broadcast to real-time listeners; prune disconnected subscribers automatically.
        let mut subs = self.subscribers.lock();
        subs.retain(|tx| match tx.try_send(event.clone()) {
            Ok(()) | Err(TrySendError::Full(_)) => true,
            Err(TrySendError::Disconnected(_)) => false,
        });

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
