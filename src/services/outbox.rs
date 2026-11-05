//! Per-generation transport queues. Durable payloads are spooled, never an unbounded RAM channel.
use crate::core::protocol::{DeliveryPolicy, KernelToDomainFrame};
use parking_lot::{Condvar, Mutex};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{RecvError, RecvTimeoutError, TryRecvError};
use std::time::{Duration, Instant};

pub const OUTBOX_COLUMNS: &[&str] = &["id", "channel", "delivery_id", "priority", "seq", "frame"];

const MEMORY_BYTES: usize = 2 * 1024 * 1024;
const EVENT_BYTES: usize = 256 * 1024;
const MEMORY_ITEMS: usize = 1024;

pub(crate) fn initialize(conn: &Connection) -> Result<(), crate::services::storage::StorageError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS kernel_outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL,
        delivery_id TEXT, priority INTEGER NOT NULL, seq INTEGER NOT NULL DEFAULT 0, frame TEXT NOT NULL);
        ",
    )?;
    crate::services::storage::verify_sqlite_table_columns(conn, "kernel_outbox", OUTBOX_COLUMNS)?;
    conn.execute_batch("CREATE INDEX IF NOT EXISTS idx_kernel_outbox_channel ON kernel_outbox(channel,priority,seq,id);")?;
    Ok(())
}

pub(crate) fn insert_frame(
    db: &Connection,
    channel: &str,
    seq: u64,
    frame: &KernelToDomainFrame,
) -> Result<(), crate::services::storage::StorageError> {
    let (_, lane) = classify(frame);
    let id = match frame {
        KernelToDomainFrame::Deliver(m) => Some(m.message_id.as_str()),
        _ => None,
    };
    db.execute(
        "INSERT INTO kernel_outbox(channel,delivery_id,priority,seq,frame) VALUES(?1,?2,?3,?4,?5)",
        params![channel, id, lane as i64, seq as i64, serde_json::to_string(frame)?],
    )?;
    Ok(())
}

#[derive(Debug)]
pub struct QueueError(pub String);
struct MemoryItem {
    frame: KernelToDomainFrame,
    bytes: usize,
    lane: usize,
    /// 业务帧（lane 1）的每通道单调序号；其他 lane 为 0。
    seq: u64,
}
enum Lease {
    Disk(i64, bool, u64),
    Memory(usize, usize, u64),
    StateSnapshot,
    Temporary(i64, usize, u64),
}
struct State {
    memory: [VecDeque<MemoryItem>; 3],
    bytes: [usize; 3],
    counts: [usize; 3],
    domain_snapshot: Option<KernelToDomainFrame>,
    snapshot_bytes: usize,
    snapshot_inflight: usize,
    temporary: Option<Connection>,
    temporary_pending: [usize; 2],
    pending: usize,
    sent: [i64; 2],
    /// 每通道业务帧单调序号：`business_seq` 为下一个待分配，`sent_business_seq` 为已投递到的序号。
    business_seq: u64,
    sent_business_seq: u64,
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
            domain_snapshot: None,
            snapshot_bytes: 0,
            snapshot_inflight: 0,
            temporary: None,
            temporary_pending: [0; 2],
            pending: 0,
            sent: [0; 2],
            business_seq: 1,
            sent_business_seq: 0,
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
        KernelToDomainFrame::Receipt(r) => (r.policy == DeliveryPolicy::Durable, 0),
        KernelToDomainFrame::Deliver(m) => {
            if m.target.starts_with("kernel.")
                && matches!(
                    m.message_type.as_str(),
                    "LogEvent" | "ObservationEvent" | "TelemetryEvent" | "NotifyEvent"
                )
            {
                (false, 2)
            } else {
                (m.delivery == DeliveryPolicy::Durable, 1)
            }
        }
    }
}
// Temporary transport overflow belongs only to this generation. It is neither a
// durable ledger nor a replay queue; SQLite deletes its temporary file on close.
fn enqueue_transient(
    s: &mut State,
    frame: KernelToDomainFrame,
    bytes: usize,
    lane: usize,
    seq: u64,
) -> Result<(), QueueError> {
    let limit = if lane == 2 {
        EVENT_BYTES.saturating_sub(s.snapshot_bytes.saturating_add(s.snapshot_inflight))
    } else {
        MEMORY_BYTES
    };
    let fits = bytes <= limit && s.bytes[lane] <= limit - bytes && s.counts[lane] < MEMORY_ITEMS;
    if lane == 2 {
        if !fits {
            return Err(QueueError("lossy system event omitted".into()));
        }
    } else if !fits || s.temporary_pending[lane] != 0 {
        if s.temporary.is_none() {
            let db = Connection::open("")
                .map_err(|e| QueueError(format!("temporary transport open failed: {e}")))?;
            db.execute_batch("PRAGMA cache_size=-512; CREATE TABLE frames(id INTEGER PRIMARY KEY, lane INTEGER NOT NULL, seq INTEGER NOT NULL, frame TEXT NOT NULL);")
                .map_err(|e| QueueError(format!("temporary transport initialize failed: {e}")))?;
            s.temporary = Some(db);
        }
        let db = s.temporary.as_ref().unwrap();
        db.execute(
            "INSERT INTO frames(lane,seq,frame) VALUES(?1,?2,?3)",
            params![
                lane as i64,
                seq as i64,
                serde_json::to_string(&frame).map_err(|e| QueueError(e.to_string()))?
            ],
        )
        .map_err(|e| QueueError(format!("temporary transport write failed: {e}")))?;
        s.temporary_pending[lane] += 1;
        return Ok(());
    }
    s.bytes[lane] += bytes;
    s.counts[lane] += 1;
    s.memory[lane].push_back(MemoryItem { frame, bytes, lane, seq });
    Ok(())
}

/// 临时溢出区某 lane 的队首 seq（用于业务帧按 seq 出队比较）。
fn peek_temp_seq(s: &State, lane: usize) -> Option<u64> {
    if s.temporary_pending[lane] == 0 {
        return None;
    }
    s.temporary
        .as_ref()
        .unwrap()
        .query_row(
            "SELECT seq FROM frames WHERE lane=?1 ORDER BY seq,id LIMIT 1",
            [lane as i64],
            |r| r.get::<_, i64>(0).map(|v| v as u64),
        )
        .optional()
        .ok()
        .flatten()
}

fn take_temporary(
    s: &mut State,
    lane: usize,
) -> Result<Option<KernelToDomainFrame>, RecvTimeoutError> {
    if s.temporary_pending[lane] == 0 {
        return Ok(None);
    }
    let result: Result<KernelToDomainFrame, String> = (|| {
        let (id, seq, text): (i64, i64, String) = s
            .temporary
            .as_ref()
            .unwrap()
            .query_row(
                "SELECT id,seq,frame FROM frames WHERE lane=?1 ORDER BY seq,id LIMIT 1",
                [lane as i64],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(|e| e.to_string())?;
        let frame = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        s.lease = Some(Lease::Temporary(id, lane, seq as u64));
        Ok(frame)
    })();
    match result {
        Ok(frame) => Ok(Some(frame)),
        Err(e) => {
            s.failure = Some(format!("temporary transport read failed: {e}"));
            s.closed = true;
            Err(RecvTimeoutError::Disconnected)
        }
    }
}
impl PipeSender {
    pub(crate) fn accept(
        &self,
        ledger: &crate::services::ledger::KernelLedger,
        msg: &crate::core::protocol::KernelMessage,
        seq: u64,
    ) -> Result<(crate::services::ledger::DeliveryRecord, bool), QueueError> {
        let mut state = self.0.state.lock();
        if state.closed {
            return Err(QueueError("domain output disconnected".into()));
        }
        let channel_seq = state.business_seq;
        let (record, created) = ledger.record_delivery(msg, seq, &self.0.channel, channel_seq);
        if created {
            state.business_seq += 1;
            if msg.delivery == DeliveryPolicy::Durable {
                state.pending += 1;
                self.0.wake.notify_one();
            } else {
                let frame = KernelToDomainFrame::Deliver(msg.clone());
                let bytes = serde_json::to_string(&frame)
                    .map_err(|e| QueueError(e.to_string()))?
                    .len();
                enqueue_transient(&mut state, frame, bytes, 1, channel_seq)?;
                self.0.wake.notify_one();
            }
        }
        Ok((record, created))
    }
    pub fn send(&self, frame: KernelToDomainFrame) -> Result<(), QueueError> {
        self.send_inner(frame, false)
    }
    pub(crate) fn send_unpersisted_failure(
        &self,
        frame: KernelToDomainFrame,
    ) -> Result<(), QueueError> {
        self.send_inner(frame, true)
    }
    fn send_inner(&self, frame: KernelToDomainFrame, unpersisted: bool) -> Result<(), QueueError> {
        let (mut durable, lane) = classify(&frame);
        durable &= !unpersisted;
        let encoded = serde_json::to_string(&frame).map_err(|e| QueueError(e.to_string()))?;
        let mut s = self.0.state.lock();
        if s.closed {
            return Err(QueueError(
                s.failure
                    .clone()
                    .unwrap_or_else(|| "domain output disconnected".into()),
            ));
        }
        // Current domain state is a replaceable state image, not a durable event queue.
        if let KernelToDomainFrame::Deliver(message) = &frame {
            if message.target == "kernel.observer"
                && message.message_type == "ObservationEvent"
                && message.data["message_type"] == "DomainStateSnapshot"
            {
                let revision = message.data["details"]["revision"].as_u64().unwrap_or(0);
                let existing = s.domain_snapshot.as_ref().and_then(|f| match f {
                    KernelToDomainFrame::Deliver(m) => m.data["details"]["revision"].as_u64(),
                    _ => None,
                });
                if existing.is_none_or(|old| revision >= old) {
                    s.snapshot_bytes = encoded.len();
                    s.domain_snapshot = Some(frame);
                    // Reserve room for the authoritative image by evicting lossy events,
                    // rather than adding an unaccounted second observation backlog.
                    let budget = EVENT_BYTES
                        .saturating_sub(s.snapshot_bytes.saturating_add(s.snapshot_inflight));
                    while s.bytes[2] > budget {
                        if let Some(item) = s.memory[2].pop_back() {
                            s.bytes[2] -= item.bytes;
                            s.counts[2] -= 1;
                        } else {
                            break;
                        }
                    }
                    self.0.wake.notify_one();
                }
                return Ok(());
            }
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
            let seq = if lane == 1 { s.business_seq } else { 0 };
            let result = insert_frame(&self.0.db.lock(), &self.0.channel, seq, &frame);
            if let Err(e) = result {
                let error = format!("kernel_outbox_persistence_failed: {e}");
                s.failure = Some(error.clone());
                s.closed = true;
                self.0.wake.notify_all();
                return Err(QueueError(error));
            }
            if lane == 1 {
                s.business_seq += 1;
            }
            s.pending += 1;
            if lane == 0 {
                s.pending_receipts += 1;
            }
        } else {
            let seq = if lane == 1 { s.business_seq } else { 0 };
            enqueue_transient(&mut s, frame, encoded.len(), lane, seq)?;
            if lane == 1 {
                s.business_seq += 1;
            }
        }
        self.0.wake.notify_one();
        Ok(())
    }
    pub(crate) fn channel_id(&self) -> &str {
        &self.0.channel
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
        (
            s.pending,
            s.bytes.iter().sum::<usize>() + s.snapshot_bytes + s.snapshot_inflight,
        )
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
                s.lease = Some(Lease::Memory(item.lane, item.bytes, item.seq));
                return Ok(item.frame);
            }
            if let Some(frame) = take_temporary(&mut s, 0)? {
                return Ok(frame);
            }
            if let Some(frame) = s.domain_snapshot.take() {
                s.snapshot_inflight = s.snapshot_bytes;
                s.snapshot_bytes = 0;
                s.lease = Some(Lease::StateSnapshot);
                return Ok(frame);
            }
            // Rotate business (seq-ordered across memory/temp/sqlite) and lossy live events.
            // A durable flood cannot indefinitely starve status responses or the live stream.
            s.turn = (s.turn + 1) % 3;
            if s.turn != 0 {
                let lanes = if s.turn == 1 { [1usize, 2] } else { [2, 1] };
                for lane in lanes {
                    if lane == 1 {
                        if let Some(frame) = self.deliver_business(&mut s)? {
                            return Ok(frame);
                        }
                    } else if let Some(item) = s.memory[2].pop_front() {
                        s.lease = Some(Lease::Memory(item.lane, item.bytes, item.seq));
                        return Ok(item.frame);
                    }
                }
            }
            // Durable receipts spilled to SQLite (priority 0). Business is fully handled above.
            if s.pending_receipts > 0 {
                let row: rusqlite::Result<Option<(i64, String)>> = self.0.db.lock().query_row(
                    "SELECT id,frame FROM kernel_outbox WHERE channel=?1 AND priority=0 AND id>?2 ORDER BY id LIMIT 1",
                    params![self.0.channel, s.sent[0]],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                ).optional();
                match row {
                    Ok(Some((id, text))) => match serde_json::from_str(&text) {
                        Ok(frame) => {
                            s.lease = Some(Lease::Disk(id, true, 0));
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
            if let Some(frame) = self.deliver_business(&mut s)? {
                return Ok(frame);
            }
            if let Some(item) = s.memory[2].pop_front() {
                s.lease = Some(Lease::Memory(item.lane, item.bytes, item.seq));
                return Ok(item.frame);
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

    /// 业务帧按每通道单调 seq 出队：Durable(SQLite) 与 Volatile/Ephemeral(memory/temp) 共享同一序号，
    /// 跨策略乱序（审计 A2）从结构上消失。
    fn deliver_business(
        &self,
        s: &mut State,
    ) -> Result<Option<KernelToDomainFrame>, RecvTimeoutError> {
        let mem_seq = s.memory[1].front().map(|i| i.seq);
        let temp_seq = peek_temp_seq(s, 1);
        let disk: Option<(u64, i64, String)> = {
            let db = self.0.db.lock();
            db.query_row(
                "SELECT seq,id,frame FROM kernel_outbox WHERE channel=?1 AND priority=1 AND seq>?2 ORDER BY seq,id LIMIT 1",
                params![self.0.channel, s.sent_business_seq as i64],
                |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)?, r.get::<_, String>(2)?)),
            )
            .optional()
            .ok()
            .flatten()
        };
        let disk_seq = disk.as_ref().map(|(seq, _, _)| *seq);
        let mut candidates: Vec<(u64, u8)> = Vec::new();
        if let Some(seq) = mem_seq {
            candidates.push((seq, 0));
        }
        if let Some(seq) = temp_seq {
            candidates.push((seq, 1));
        }
        if let Some(seq) = disk_seq {
            candidates.push((seq, 2));
        }
        let Some(&(_, which)) = candidates.iter().min_by_key(|(seq, _)| *seq) else {
            return Ok(None);
        };
        match which {
            0 => {
                let item = s.memory[1].pop_front().unwrap();
                s.lease = Some(Lease::Memory(item.lane, item.bytes, item.seq));
                Ok(Some(item.frame))
            }
            1 => take_temporary(s, 1),
            _ => {
                let (seq, id, text) = disk.unwrap();
                match serde_json::from_str::<KernelToDomainFrame>(&text) {
                    Ok(frame) => {
                        s.lease = Some(Lease::Disk(id, false, seq));
                        Ok(Some(frame))
                    }
                    Err(e) => {
                        s.failure = Some(format!("corrupt outbox: {e}"));
                        s.closed = true;
                        Err(RecvTimeoutError::Disconnected)
                    }
                }
            }
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
            Some(Lease::Disk(id, receipt, seq)) => {
                if let Err(e) = self.0.db.lock().execute(
                    "DELETE FROM kernel_outbox WHERE id=?1 AND channel=?2 AND (delivery_id IS NULL OR NOT EXISTS(SELECT 1 FROM messages WHERE message_id=delivery_id))",
                    params![id, self.0.channel],
                ) {
                    let error = format!("outbox acknowledgement failed: {e}");
                    s.failure = Some(error.clone());
                    s.closed = true;
                    return Err(QueueError(error));
                }
                if receipt {
                    s.sent[0] = id;
                    s.pending_receipts -= 1;
                } else {
                    s.sent_business_seq = s.sent_business_seq.max(seq);
                }
                s.pending -= 1;
            }
            Some(Lease::Temporary(id, lane, seq)) => {
                s.temporary
                    .as_ref()
                    .unwrap()
                    .execute("DELETE FROM frames WHERE id=?1", [id])
                    .map_err(|e| {
                        QueueError(format!("temporary transport acknowledgement failed: {e}"))
                    })?;
                s.temporary_pending[lane] -= 1;
                if s.temporary_pending == [0; 2] {
                    s.temporary = None;
                }
                if lane == 1 {
                    s.sent_business_seq = s.sent_business_seq.max(seq);
                }
            }
            Some(Lease::StateSnapshot) => {
                s.snapshot_inflight = 0;
            }
            Some(Lease::Memory(lane, bytes, seq)) => {
                s.bytes[lane] -= bytes;
                s.counts[lane] -= 1;
                if lane == 1 {
                    s.sent_business_seq = s.sent_business_seq.max(seq);
                }
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
    s.domain_snapshot = None;
    s.snapshot_bytes = 0;
    s.snapshot_inflight = 0;
    s.temporary = None;
    s.temporary_pending = [0; 2];
    // Only already committed terminal envelopes are cleaned here. The ledger closes live
    // deliveries after transport closure and removes their envelopes in the terminal transaction.
    if let Err(e) = inner.db.lock().execute(
        "DELETE FROM kernel_outbox WHERE channel=?1 AND (delivery_id IS NULL OR NOT EXISTS(SELECT 1 FROM messages WHERE message_id=delivery_id))",
        [&inner.channel],
    ) {
        s.failure.get_or_insert_with(|| format!("outbox cleanup failed: {e}"));
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
            delivery: DeliveryPolicy::from_message_type(kind),
            params: json!({"index":i}),
            timestamp: 0,
            data: json!({"text":"x".repeat(8192)}),
        })
    }
    /// 审计 A2：Durable 与 Volatile/Ephemeral 共享每通道单调 seq，跨策略 FIFO 按投递顺序，
    /// 与存储层（SQLite / 内存）无关。
    #[test]
    fn cross_policy_business_fifo_is_preserved() {
        let kernel = AtelierKernel::new_in_memory().unwrap();
        let db = kernel.ledger().db_conn.as_ref().unwrap().clone();
        let (tx, rx) = channel(db);
        // Durable(0) → Volatile(1) → Durable(2) → Volatile(3) → Ephemeral(4)，交替入队。
        let order = ["Durable", "Volatile", "Durable", "Volatile", "Ephemeral"];
        for (i, kind) in order.iter().enumerate() {
            tx.send(frame(kind, i)).unwrap();
        }
        let mut seen = Vec::new();
        for _ in 0..order.len() {
            match rx.try_recv().unwrap() {
                KernelToDomainFrame::Deliver(m) => {
                    seen.push(m.params["index"].as_u64().unwrap() as usize)
                }
                _ => panic!(),
            }
            rx.acknowledge().unwrap();
        }
        assert_eq!(seen, vec![0, 1, 2, 3, 4], "跨策略业务帧必须按投递顺序（A2）");
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
    fn transient_backlog_spills_without_disconnect_and_system_events_remain_lossy() {
        let kernel = AtelierKernel::new_in_memory().unwrap();
        let db = kernel.ledger().db_conn.as_ref().unwrap().clone();
        let (tx, transient_rx) = channel(db.clone());
        let mut accepted = 0;
        for i in 0..1500 {
            if tx.send(frame("Ephemeral", i)).is_err() {
                break;
            }
            accepted += 1;
        }
        assert_eq!(accepted, 1500);
        assert!(tx.failure().is_none());
        for i in 0..1500 {
            match transient_rx.try_recv().unwrap() {
                KernelToDomainFrame::Deliver(m) => assert_eq!(m.params["index"], i),
                _ => panic!(),
            }
            transient_rx.acknowledge().unwrap();
        }
        let mut large = frame("Ephemeral", 1501);
        if let KernelToDomainFrame::Deliver(ref mut m) = large {
            m.data = json!({"text":"x".repeat(12*1024*1024)});
        }
        tx.send(large).unwrap();
        transient_rx.try_recv().unwrap();
        transient_rx.acknowledge().unwrap();
        assert!(tx.failure().is_none());
        assert!(tx.0.state.lock().temporary.is_none());
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
    fn transport_schema_initialization_does_not_mutate_ledger_facts() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE messages(message_id TEXT,status TEXT,data TEXT); INSERT INTO messages VALUES('pending','sent',NULL);").unwrap();
        initialize(&conn).unwrap();
        let status: String = conn
            .query_row("SELECT status FROM messages", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            status, "sent",
            "startup recovery belongs to the ledger's atomic transaction, not transport schema setup"
        );
        // End-to-end process recovery is covered in tests/core_reliability.rs and outbox_backpressure.rs.
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
                policy: crate::core::protocol::DeliveryPolicy::Durable,
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

#[cfg(test)]
mod first_failure_diagnostics_test {
    use super::*;
    #[test]
    fn cleanup_failure_does_not_replace_original_outbox_error() {
        let db = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        initialize(&db.lock()).unwrap();
        let (tx, _rx) = channel(db.clone());
        tx.0.state.lock().failure = Some("original output failure".into());
        db.lock().execute_batch("DROP TABLE kernel_outbox").unwrap();
        tx.close();
        assert_eq!(tx.failure().as_deref(), Some("original output failure"));
    }
}

#[cfg(test)]
mod domain_state_slot_tests {
    use super::*;
    #[test]
    fn latest_domain_state_survives_lossy_event_saturation_without_persistence() {
        let k = crate::AtelierKernel::new_in_memory().unwrap();
        let db = k.ledger().db_conn.as_ref().unwrap().clone();
        let (tx, rx) = channel(db.clone());
        for i in 0..1000 {
            let message = crate::core::protocol::KernelMessage {
                logical_id: String::new(),
                message_id: format!("event_{i}"),
                source_domain: "kernel".into(),
                target: "kernel.observer".into(),
                targets: vec!["kernel.observer".into()],
                message_type: "ObservationEvent".into(),
                delivery: DeliveryPolicy::Ephemeral,
                params: serde_json::json!({}),
                timestamp: 0,
                data: serde_json::json!({"padding":"x".repeat(8192)}),
            };
            let _ = tx.send(KernelToDomainFrame::Deliver(message));
        }
        for revision in 0..500 {
            let event = k.observer().domain_state_snapshot();
            let mut value = serde_json::to_value(event).unwrap();
            value["details"]["revision"] = revision.into();
            tx.send(KernelToDomainFrame::Deliver(
                crate::core::protocol::KernelMessage {
                    logical_id: String::new(),
                    message_id: String::new(),
                    source_domain: "kernel".into(),
                    target: "kernel.observer".into(),
                    targets: vec!["kernel.observer".into()],
                    message_type: "ObservationEvent".into(),
                    delivery: DeliveryPolicy::Ephemeral,
                    params: serde_json::json!({}),
                    timestamp: 0,
                    data: value,
                },
            ))
            .unwrap();
        }
        match rx.try_recv().unwrap() {
            KernelToDomainFrame::Deliver(m) => assert_eq!(m.data["details"]["revision"], 499),
            _ => panic!(),
        };
        rx.acknowledge().unwrap();
        assert!(tx.failure().is_none());
        let persisted: i64 = db
            .lock()
            .query_row("SELECT count(*) FROM kernel_outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(persisted, 0);
    }
}

#[cfg(test)]
mod domain_state_reservation_tests {
    use super::*;
    use serde_json::json;
    fn event(data: serde_json::Value) -> KernelToDomainFrame {
        KernelToDomainFrame::Deliver(crate::core::protocol::KernelMessage {
            logical_id: String::new(),
            message_id: String::new(),
            source_domain: "kernel".into(),
            target: "kernel.observer".into(),
            targets: vec!["kernel.observer".into()],
            message_type: "ObservationEvent".into(),
            delivery: DeliveryPolicy::Ephemeral,
            params: json!({}),
            timestamp: 0,
            data,
        })
    }
    #[test]
    fn pending_and_inflight_snapshot_reserve_space_before_live_events_arrive() {
        for inflight in [false, true] {
            let k = crate::AtelierKernel::new_in_memory().unwrap();
            let (tx, rx) = channel(k.ledger().db_conn.as_ref().unwrap().clone());
            tx.send(event(
                json!({"message_type":"DomainStateSnapshot","details":{"revision":1,"domains":[]}}),
            ))
            .unwrap();
            if inflight {
                rx.try_recv().unwrap();
            }
            for _ in 0..1000 {
                let _ = tx.send(event(json!({"payload":"x".repeat(8192)})));
            }
            assert!(
                tx.counts().1 <= EVENT_BYTES,
                "inflight={inflight}, bytes={}",
                tx.counts().1
            );
            if inflight {
                rx.acknowledge().unwrap();
            } else {
                match rx.try_recv().unwrap() {
                    KernelToDomainFrame::Deliver(m) => assert_eq!(m.data["details"]["revision"], 1),
                    _ => panic!(),
                };
                rx.acknowledge().unwrap();
            }
        }
    }
}
