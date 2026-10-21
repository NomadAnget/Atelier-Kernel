//! Per-generation transport queues. Durable payloads are spooled, never an unbounded RAM channel.
use crate::protocol::{DeliveryPolicy, KernelToDomainFrame};
use parking_lot::{Condvar, Mutex};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{RecvError, RecvTimeoutError, TryRecvError};
use std::time::{Duration, Instant};

pub const OUTBOX_COLUMNS: &[&str] = &["id", "channel", "delivery_id", "priority", "frame"];

const MEMORY_BYTES: usize = 2 * 1024 * 1024;
const EVENT_BYTES: usize = 256 * 1024;
const MEMORY_ITEMS: usize = 1024;

pub(crate) fn initialize(conn: &Connection) -> Result<(), crate::storage::StorageError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS kernel_outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL,
        delivery_id TEXT, priority INTEGER NOT NULL, frame TEXT NOT NULL);
        ",
    )?;
    crate::storage::verify_sqlite_table_columns(conn, "kernel_outbox", OUTBOX_COLUMNS)?;
    conn.execute_batch("CREATE INDEX IF NOT EXISTS idx_kernel_outbox_channel ON kernel_outbox(channel,priority,id);")?;
    Ok(())
}

pub(crate) fn insert_frame(db:&Connection,channel:&str,frame:&KernelToDomainFrame)->Result<(),crate::storage::StorageError> {
    let (_,lane)=classify(frame);
    let id=match frame {KernelToDomainFrame::Deliver(m)=>Some(m.message_id.as_str()),_=>None};
    db.execute("INSERT INTO kernel_outbox(channel,delivery_id,priority,frame) VALUES(?1,?2,?3,?4)",params![channel,id,lane as i64,serde_json::to_string(frame)?])?;
    Ok(())
}

#[derive(Debug)]
pub struct QueueError(pub String);
struct MemoryItem {
    frame: KernelToDomainFrame,
    bytes: usize,
    lane: usize,
}
enum Lease {
    Disk(i64, bool),
    Memory(usize, usize),
}
struct State {
    memory: [VecDeque<MemoryItem>; 3],
    bytes: [usize; 3],
    counts: [usize; 3],
    pending: usize,
    sent: [i64;2],
    pending_receipts: usize,
    turn: usize,
    lease: Option<Lease>,
    closed: bool,
    failure: Option<String>,
}
struct Inner {
    channel: String,
    db: Arc<Mutex<Connection>>,
    state: Mutex<State>,
    wake: Condvar,
    senders: AtomicUsize,
}
pub struct PipeSender(Arc<Inner>);
pub struct PipeReceiver(Arc<Inner>);
impl Clone for PipeSender {
    fn clone(&self) -> Self {
        self.0.senders.fetch_add(1, Ordering::Relaxed);
        Self(self.0.clone())
    }
}
impl Drop for PipeSender {
    fn drop(&mut self) {
        if self.0.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.wake.notify_all();
        }
    }
}

pub fn channel(db: Arc<Mutex<Connection>>) -> (PipeSender, PipeReceiver) {
    let inner = Arc::new(Inner {
        channel: uuid::Uuid::new_v4().to_string(),
        db,
        state: Mutex::new(State {
            memory: std::array::from_fn(|_| VecDeque::new()),
            bytes: [0; 3],
            counts: [0; 3],
            pending: 0,
            sent: [0;2],
            pending_receipts: 0,
            turn: 0,
            lease: None,
            closed: false,
            failure: None,
        }),
        wake: Condvar::new(),
        senders: AtomicUsize::new(1),
    });
    (PipeSender(inner.clone()), PipeReceiver(inner))
}
fn classify(frame: &KernelToDomainFrame) -> (bool, usize) {
    match frame {
        KernelToDomainFrame::Receipt(r) => {
            // Storage failure feedback must not depend on the storage that just failed.
            let failed_storage = r.status == crate::protocol::ContractStatus::ExecutionFailed
                && r.data.as_ref().is_some_and(|d| {
                    d.get("error").and_then(|v| v.as_str()) == Some("kernel_persistence_failed")
                        && d.get("persisted") == Some(&serde_json::Value::Bool(false))
                });
            (
                !failed_storage
                    && DeliveryPolicy::resolve(&r.target, &r.message_type)
                        == DeliveryPolicy::Durable,
                0,
            )
        }
        KernelToDomainFrame::Deliver(m) => {
            if m.target.starts_with("kernel.")
                && matches!(
                    m.message_type.as_str(),
                    "LogEvent" | "ObservationEvent" | "TelemetryEvent" | "NotifyEvent"
                )
            {
                (false, 2)
            } else {
                (
                    DeliveryPolicy::resolve(&m.target, &m.message_type) == DeliveryPolicy::Durable,
                    1,
                )
            }
        }
    }
}
impl PipeSender {
    pub(crate) fn accept(&self,ledger:&crate::ledger::KernelLedger,msg:&crate::protocol::KernelMessage,seq:u64)->Result<(crate::ledger::DeliveryRecord,bool),QueueError> {
        if DeliveryPolicy::resolve(&msg.target,&msg.message_type)!=DeliveryPolicy::Durable {
            let (record,created)=ledger.record_delivery(msg,seq,&self.0.channel);
            if created {self.send(KernelToDomainFrame::Deliver(msg.clone()))?;}
            return Ok((record,created));
        }
        let mut state=self.0.state.lock();
        if state.closed{return Err(QueueError("domain output disconnected".into()));}
        let (record,created)=ledger.record_delivery(msg,seq,&self.0.channel);
        if created {state.pending+=1;self.0.wake.notify_one();}
        Ok((record,created))
    }
    pub fn send(&self, frame: KernelToDomainFrame) -> Result<(), QueueError> {
        let (mut durable, lane) = classify(&frame);
        let encoded = serde_json::to_string(&frame).map_err(|e| QueueError(e.to_string()))?;
        let mut s = self.0.state.lock();
        if s.closed {
            return Err(QueueError(
                s.failure
                    .clone()
                    .unwrap_or_else(|| "domain output disconnected".into()),
            ));
        }
        // Receipt state/results are already authoritative in the ledger. Keep the ordinary
        // control path independent from SQLite; only backlog beyond its reserved window spools.
        if durable
            && lane == 0
            && s.pending_receipts == 0
            && encoded.len() <= MEMORY_BYTES
            && s.bytes[0] <= MEMORY_BYTES - encoded.len()
            && s.counts[0] < MEMORY_ITEMS
        {
            durable = false;
        }
        if durable {
            let result=insert_frame(&self.0.db.lock(),&self.0.channel,&frame);
            if let Err(e) = result {
                let error = format!("kernel_outbox_persistence_failed: {e}");
                s.failure = Some(error.clone());
                s.closed = true;
                self.0.wake.notify_all();
                return Err(QueueError(error));
            }
            s.pending += 1;
            if lane == 0 {
                s.pending_receipts += 1;
            }
        } else {
            let limit = if lane == 2 { EVENT_BYTES } else { MEMORY_BYTES };
            if encoded.len() > limit
                || s.bytes[lane] > limit - encoded.len()
                || s.counts[lane] >= MEMORY_ITEMS
            {
                if lane == 2 {
                    return Err(QueueError("lossy system event omitted".into()));
                }
                let error = "nonpersistent domain output budget exhausted".to_string();
                s.failure = Some(error.clone());
                s.closed = true;
                self.0.wake.notify_all();
                return Err(QueueError(error));
            }
            s.bytes[lane] += encoded.len();
            s.counts[lane] += 1;
            s.memory[lane].push_back(MemoryItem {
                frame,
                bytes: encoded.len(),
                lane,
            });
        }
        self.0.wake.notify_one();
        Ok(())
    }
    pub fn failure(&self) -> Option<String> {
        let s = self.0.state.lock();
        s.failure
            .clone()
            .or_else(|| s.closed.then(|| "domain output disconnected".into()))
    }
    pub fn close(&self) {
        close(&self.0);
    }
    #[cfg(test)]
    fn counts(&self) -> (usize, usize) {
        let s = self.0.state.lock();
        (s.pending, s.bytes.iter().sum())
    }
}
impl PipeReceiver {
    fn next(&self, timeout: Duration) -> Result<KernelToDomainFrame, RecvTimeoutError> {
        let deadline = Instant::now() + timeout;
        let mut s = self.0.state.lock();
        loop {
            if s.closed {
                return Err(RecvTimeoutError::Disconnected);
            }
            assert!(
                s.lease.is_none(),
                "a pipe frame must be acknowledged after physical write"
            );
            // Control receipts have their own capacity and priority; preserve order within each lane.
            if let Some(item) = s.memory[0].pop_front() {
                s.lease = Some(Lease::Memory(item.lane, item.bytes));
                return Ok(item.frame);
            }
            // Rotate persistent business, transient business and live events. A durable flood
            // cannot indefinitely starve status responses or the live diagnostic stream.
            s.turn = (s.turn + 1) % 3;
            if s.pending > 0 && s.turn != 0 {
                let lanes = if s.turn == 1 { [1, 2] } else { [2, 1] };
                for lane in lanes {
                    if let Some(item) = s.memory[lane].pop_front() {
                        s.lease = Some(Lease::Memory(item.lane, item.bytes));
                        return Ok(item.frame);
                    }
                }
            }
            if s.pending > 0 {
                let row: rusqlite::Result<Option<(i64,String,i64)>>=self.0.db.lock().query_row(
                    "SELECT id,frame,priority FROM kernel_outbox WHERE channel=?1 AND ((priority=0 AND id>?2) OR (priority=1 AND id>?3)) ORDER BY priority,id LIMIT 1",
                    params![self.0.channel,s.sent[0],s.sent[1]],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional();
                match row {
                    Ok(Some((id, text, priority))) => match serde_json::from_str(&text) {
                        Ok(frame) => {
                            s.lease = Some(Lease::Disk(id, priority == 0));
                            return Ok(frame);
                        }
                        Err(e) => {
                            s.failure = Some(format!("corrupt outbox: {e}"));
                            s.closed = true;
                        }
                    },
                    Ok(None) => {
                        s.failure = Some("outbox pending row missing".into());
                        s.closed = true;
                    }
                    Err(e) => {
                        s.failure = Some(format!("outbox read failed: {e}"));
                        s.closed = true;
                    }
                }
                continue;
            }
            for lane in [1, 2] {
                if let Some(item) = s.memory[lane].pop_front() {
                    s.lease = Some(Lease::Memory(item.lane, item.bytes));
                    return Ok(item.frame);
                }
            }
            if self.0.senders.load(Ordering::Acquire) == 0 {
                return Err(RecvTimeoutError::Disconnected);
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(RecvTimeoutError::Timeout);
            }
            self.0.wake.wait_for(&mut s, deadline - now);
        }
    }
    pub fn recv(&self) -> Result<KernelToDomainFrame, RecvError> {
        loop {
            match self.next(Duration::from_secs(60)) {
                Ok(f) => return Ok(f),
                Err(RecvTimeoutError::Timeout) => {}
                Err(_) => return Err(RecvError),
            }
        }
    }
    pub fn recv_timeout(&self, timeout: Duration) -> Result<KernelToDomainFrame, RecvTimeoutError> {
        self.next(timeout)
    }
    pub fn try_recv(&self) -> Result<KernelToDomainFrame, TryRecvError> {
        self.next(Duration::ZERO).map_err(|e| match e {
            RecvTimeoutError::Timeout => TryRecvError::Empty,
            _ => TryRecvError::Disconnected,
        })
    }
    pub fn acknowledge(&self) -> Result<(), QueueError> {
        let mut s = self.0.state.lock();
        match s.lease.take() {
            Some(Lease::Disk(id, receipt)) => {
                if let Err(e) = self.0.db.lock().execute(
                    "DELETE FROM kernel_outbox WHERE id=?1 AND channel=?2 AND (delivery_id IS NULL OR NOT EXISTS(SELECT 1 FROM kernel_ledger WHERE message_id=delivery_id))",
                    params![id, self.0.channel],
                ) {
                    let error = format!("outbox acknowledgement failed: {e}");
                    s.failure = Some(error.clone());
                    s.closed = true;
                    return Err(QueueError(error));
                }
                s.sent[if receipt {0}else{1}]=id;
                s.pending -= 1;
                if receipt {
                    s.pending_receipts -= 1;
                }
            }
            Some(Lease::Memory(lane, bytes)) => {
                s.bytes[lane] -= bytes;
                s.counts[lane] -= 1;
            }
            None => {}
        }
        Ok(())
    }
}
fn close(inner: &Inner) {
    let mut s = inner.state.lock();
    s.closed = true;
    s.memory.iter_mut().for_each(VecDeque::clear);
    s.bytes = [0; 3];
    s.counts = [0; 3];
    s.lease = None;
    // Lifecycle ledger failure is recorded by the router before discarding this generation's queue.
    if let Err(e) = inner.db.lock().execute(
        "DELETE FROM kernel_outbox WHERE channel=?1 AND (delivery_id IS NULL OR EXISTS(SELECT 1 FROM kernel_ledger WHERE message_id=delivery_id AND status IN ('completed','execution_failed','contract_invalid','target_invalid')))",
        [&inner.channel],
    ) {
        s.failure = Some(format!("outbox cleanup failed: {e}"));
    } else {
        s.pending = 0;
        s.pending_receipts = 0;
    }
    inner.wake.notify_all();
}
impl Drop for PipeReceiver {
    fn drop(&mut self) {
        close(&self.0);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AtelierKernel, ContractStatus, KernelMessage};
    use serde_json::json;
    fn frame(kind: &str, i: usize) -> KernelToDomainFrame {
        KernelToDomainFrame::Deliver(KernelMessage {
            logical_id: format!("l{i}"),
            message_id: format!("m{i}"),
            source_domain: "sender".into(),
            target: if kind == "LogEvent" {
                "kernel.logger"
            } else {
                "worker.run"
            }
            .into(),
            targets: vec!["worker.run".into()],
            message_type: kind.into(),
            params: json!({"index":i}),
            timestamp: 0,
            data: json!({"text":"x".repeat(8192)}),
        })
    }
    #[test]
    fn durable_backlog_is_in_sqlite_and_no_payload_is_removed_before_write_ack() {
        let kernel = AtelierKernel::new_in_memory().unwrap();
        let db = kernel.ledger().db_conn.as_ref().unwrap().clone();
        let (tx, rx) = channel(db.clone());
        for i in 0..1500 {
            tx.send(frame("Durable", i)).unwrap();
        }
        assert_eq!(tx.counts(), (1500, 0));
        let count: i64 = db
            .lock()
            .query_row("SELECT count(*) FROM kernel_outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1500);
        for i in 0..1500 {
            match rx.try_recv().unwrap() {
                KernelToDomainFrame::Deliver(m) => assert_eq!(m.params["index"], i),
                _ => panic!(),
            };
            assert_eq!(tx.counts().0, 1500 - i);
            rx.acknowledge().unwrap();
        }
        assert_eq!(tx.counts(), (0, 0));
    }
    #[test]
    fn transient_limit_is_explicit_and_system_event_overflow_is_lossy() {
        let kernel = AtelierKernel::new_in_memory().unwrap();
        let db = kernel.ledger().db_conn.as_ref().unwrap().clone();
        let (tx, _rx) = channel(db.clone());
        let mut accepted = 0;
        for i in 0..1500 {
            if tx.send(frame("Ephemeral", i)).is_err() {
                break;
            }
            accepted += 1;
        }
        assert!(accepted > 0 && accepted < 1500);
        assert!(tx.failure().is_some());
        assert!(tx.counts().1 <= MEMORY_BYTES);
        let (events, rx) = channel(db.clone());
        for i in 0..5000 {
            let _ = events.send(frame("LogEvent", i));
        }
        assert!(events.failure().is_none());
        assert!(events.counts().1 <= EVENT_BYTES);
        // Lossy events cannot consume the separately reserved receipt/business capacity.
        events.send(frame("Ephemeral", 7)).unwrap();
        let disk: i64 = db
            .lock()
            .query_row("SELECT count(*) FROM kernel_outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(disk, 0);
        rx.try_recv().unwrap();
        rx.acknowledge().unwrap();
    }
    #[test]
    fn sustained_durable_backlog_does_not_starve_live_events() {
        let kernel = AtelierKernel::new_in_memory().unwrap();
        let (tx, rx) = channel(kernel.ledger().db_conn.as_ref().unwrap().clone());
        for i in 0..100 {
            tx.send(frame("Durable", i)).unwrap();
        }
        tx.send(frame("LogEvent", 1)).unwrap();
        let mut event = false;
        for _ in 0..3 {
            if let KernelToDomainFrame::Deliver(m) = rx.try_recv().unwrap() {
                event |= m.message_type == "LogEvent";
            }
            rx.acknowledge().unwrap();
        }
        assert!(event);
        assert!(tx.counts().0 > 90);
    }
    #[test]
    fn replacing_generation_fails_pending_work_and_never_replays_it() {
        let kernel = AtelierKernel::new_in_memory().unwrap();
        let sender = kernel.attach_domain("sender");
        let worker = kernel.attach_domain("worker");
        worker.register_handlers(&["run"]);
        worker.drain_frames();
        let ids = sender.send(
            vec!["worker.run".into()],
            "Durable",
            json!({}),
            json!({"secret":"original"}),
        );
        let replacement = kernel.attach_domain("worker");
        replacement.register_handlers(&["run"]);
        assert!(
            !replacement
                .drain_frames()
                .iter()
                .any(|f| matches!(f, KernelToDomainFrame::Deliver(_)))
        );
        assert!(worker.drain_frames().is_empty());
        assert_eq!(
            kernel
                .ledger()
                .get_persisted_record(&ids[0])
                .unwrap()
                .status,
            ContractStatus::ExecutionFailed
        );
        assert!(sender.drain_frames().iter().any(|f|matches!(f,KernelToDomainFrame::Receipt(r) if r.message_id==ids[0] && r.status==ContractStatus::ExecutionFailed)));
    }
    #[test]
    fn startup_closes_stale_generation_without_automatic_replay() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE kernel_ledger(message_id TEXT,status TEXT,data TEXT); INSERT INTO kernel_ledger VALUES('pending','sent',NULL);").unwrap();
        initialize(&conn).unwrap();
        conn.execute("INSERT INTO kernel_outbox(channel,delivery_id,priority,frame) VALUES ('old','pending',1,'{}')",[]).unwrap();
        initialize(&conn).unwrap();
        let status: String = conn
            .query_row("SELECT status FROM kernel_ledger", [], |r| r.get(0))
            .unwrap();
        assert_eq!(status, "execution_failed");
        let pending: i64 = conn
            .query_row("SELECT count(*) FROM kernel_outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pending, 0);
    }
    #[test]
    fn outbox_insert_failure_is_explicit_and_does_not_accept_payload() {
        let kernel = AtelierKernel::new_in_memory().unwrap();
        let db = kernel.ledger().db_conn.as_ref().unwrap().clone();
        let (tx, _rx) = channel(db.clone());
        db.lock().execute_batch("CREATE TRIGGER reject_queue BEFORE INSERT ON kernel_outbox BEGIN SELECT RAISE(FAIL,'disk fault injected'); END;").unwrap();
        assert!(
            tx.send(frame("Durable", 1))
                .unwrap_err()
                .0
                .contains("persistence_failed")
        );
        assert_eq!(tx.counts(), (0, 0));
    }
}
#[cfg(test)]
mod control_and_schema_tests {
    use super::*;
    use crate::{AtelierKernel, ContractStatus, MessageReceipt};
    #[test]
    fn receipts_spill_after_reserved_window_and_preserve_order() {
        let k = AtelierKernel::new_in_memory().unwrap();
        let (tx, rx) = channel(k.ledger().db_conn.as_ref().unwrap().clone());
        for i in 0..3000 {
            tx.send(KernelToDomainFrame::Receipt(MessageReceipt {
                logical_id: format!("l{i}"),
                message_id: format!("m{i}"),
                seq: i,
                target: "worker.run".into(),
                message_type: "Durable".into(),
                status: ContractStatus::Completed,
                data: Some(serde_json::json!({"payload":"x".repeat(2048)})),
                timestamp: 0,
            }))
            .unwrap();
        }
        assert!(tx.counts().0 > 0);
        assert!(tx.counts().1 <= MEMORY_BYTES);
        for i in 0..3000 {
            match rx.try_recv().unwrap() {
                KernelToDomainFrame::Receipt(r) => assert_eq!(r.seq, i),
                _ => panic!(),
            };
            rx.acknowledge().unwrap();
        }
        assert_eq!(tx.counts(), (0, 0));
    }
    #[test]
    fn outbox_schema_drift_is_rejected_before_startup_cleanup() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE kernel_outbox(id INTEGER);")
            .unwrap();
        assert!(matches!(
            initialize(&conn),
            Err(crate::StorageError::SchemaDrift { .. })
        ));
    }
}
