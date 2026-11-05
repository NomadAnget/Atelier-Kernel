use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc;

use parking_lot::Mutex;
use serde_json::{Value, json};

use crate::core::protocol::{
    ContractStatus, DomainToKernelFrame, InboundReceipt, KERNEL_REGISTRY_TARGET,
    KERNEL_STORAGE_TARGET, KernelToDomainFrame, MSG_REGISTER_HANDLERS, MSG_STORAGE_DELETE,
    MSG_STORAGE_INSERT, MSG_STORAGE_QUERY, MSG_STORAGE_UPDATE, OutboundMessage,
};
use crate::router::KernelRouter;
use crate::services::outbox::PipeReceiver;

pub struct DomainPipeClient {
    domain_id: String,
    instance_id: u64,
    router: Arc<KernelRouter>,
    inbox: Mutex<PipeReceiver>,
    next_seq: std::sync::atomic::AtomicU64,
}

impl DomainPipeClient {
    pub(crate) fn new(
        domain_id: String,
        instance_id: u64,
        router: Arc<KernelRouter>,
        inbox: Mutex<PipeReceiver>,
    ) -> Self {
        Self {
            domain_id,
            instance_id,
            router,
            inbox,
            next_seq: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub fn domain_id(&self) -> &str {
        &self.domain_id
    }

    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Primitive 1: Registers the list of message handlers this domain can process.
    pub fn register_handlers(&self, handlers: &[&str]) -> String {
        let handler_list: Vec<Value> = handlers
            .iter()
            .map(|h| Value::String((*h).into()))
            .collect();
        self.send_one(
            vec![KERNEL_REGISTRY_TARGET.to_string()],
            MSG_REGISTER_HANDLERS,
            json!({ "handlers": handler_list }),
            json!({}),
        )
    }

    /// Primitive 2: Sends a two-layer message to one or more `"<domain>.<handler>"` targets.
    /// Returns ordered kernel-assigned child `message_id`s.
    pub fn send(
        &self,
        targets: Vec<String>,
        message_type: impl Into<String>,
        params: Value,
        data: Value,
    ) -> Vec<String> {
        let seq = self.next_seq();
        self.send_with_seq(seq, targets, message_type, params, data)
    }

    /// Sends a message using an explicit domain-local sequence number `seq` (for testing idempotency).
    pub fn send_with_seq(
        &self,
        seq: u64,
        targets: Vec<String>,
        message_type: impl Into<String>,
        params: Value,
        data: Value,
    ) -> Vec<String> {
        let outbound = OutboundMessage {
            seq,
            targets,
            message_type: message_type.into(),
            delivery: None,
            params,
            data,
        };
        self.router
            .handle_frame_for_instance(
                &self.domain_id,
                self.instance_id,
                DomainToKernelFrame::Send(outbound),
            )
            .unwrap_or_default()
    }

    /// Convenience for an explicitly single-target submission.
    pub fn send_one(
        &self,
        targets: Vec<String>,
        message_type: impl Into<String>,
        params: Value,
        data: Value,
    ) -> String {
        assert_eq!(targets.len(), 1, "send_one requires one target");
        self.send(targets, message_type, params, data)
            .into_iter()
            .next()
            .unwrap_or_default()
    }

    pub fn send_one_with_seq(
        &self,
        seq: u64,
        targets: Vec<String>,
        message_type: impl Into<String>,
        params: Value,
        data: Value,
    ) -> String {
        assert_eq!(targets.len(), 1, "send_one_with_seq requires one target");
        self.send_with_seq(seq, targets, message_type, params, data)
            .into_iter()
            .next()
            .unwrap_or_default()
    }

    /// Primitive 3: Reports a lifecycle receipt (`Received`, `Started`, `Completed`, `ExecutionFailed`) back to the Kernel.
    pub fn report_receipt(
        &self,
        message_id: impl Into<String>,
        target: impl Into<String>,
        status: ContractStatus,
        data: Option<Value>,
    ) {
        let receipt = InboundReceipt {
            message_id: message_id.into(),
            target: target.into(),
            status,
            data,
        };
        self.router.handle_frame_for_instance(
            &self.domain_id,
            self.instance_id,
            DomainToKernelFrame::Receipt(receipt),
        );
    }

    /// Helper: Sends a `StorageInsert` message to `kernel.storage`.
    pub fn storage_insert(&self, params: Value, data: Value) -> String {
        self.send_one(
            vec![KERNEL_STORAGE_TARGET.to_string()],
            MSG_STORAGE_INSERT,
            params,
            data,
        )
    }

    /// Helper: Sends a `StorageQuery` message to `kernel.storage`.
    pub fn storage_query(&self, params: Value) -> String {
        self.send_one(
            vec![KERNEL_STORAGE_TARGET.to_string()],
            MSG_STORAGE_QUERY,
            params,
            json!({}),
        )
    }

    /// Helper: Sends a `StorageUpdate` message to `kernel.storage`.
    pub fn storage_update(&self, params: Value, data: Value) -> String {
        self.send_one(
            vec![KERNEL_STORAGE_TARGET.to_string()],
            MSG_STORAGE_UPDATE,
            params,
            data,
        )
    }

    /// Helper: Sends a `StorageDelete` message to `kernel.storage`.
    pub fn storage_delete(&self, params: Value) -> String {
        self.send_one(
            vec![KERNEL_STORAGE_TARGET.to_string()],
            MSG_STORAGE_DELETE,
            params,
            json!({}),
        )
    }

    /// Receives the next frame (`Deliver(KernelMessage)` or `Receipt(MessageReceipt)`) from the Kernel pipe.
    pub fn recv_frame(&self) -> Result<KernelToDomainFrame, mpsc::RecvError> {
        let inbox = self.inbox.lock();
        let frame = inbox.recv()?;
        inbox.acknowledge().map_err(|_| mpsc::RecvError)?;
        Ok(frame)
    }

    /// Receives the next frame with a timeout.
    pub fn recv_frame_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<KernelToDomainFrame, mpsc::RecvTimeoutError> {
        let inbox = self.inbox.lock();
        let frame = inbox.recv_timeout(timeout)?;
        inbox
            .acknowledge()
            .map_err(|_| mpsc::RecvTimeoutError::Disconnected)?;
        Ok(frame)
    }

    /// Drains all currently queued frames from this domain's pipe without blocking.
    pub fn drain_frames(&self) -> Vec<KernelToDomainFrame> {
        let mut out = Vec::new();
        let inbox = self.inbox.lock();
        while let Ok(frame) = inbox.try_recv() {
            if inbox.acknowledge().is_err() {
                break;
            }
            out.push(frame);
        }
        out
    }
}
