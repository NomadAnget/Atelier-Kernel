use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::logging::LogFramePayload;

/// Built-in kernel target for domain handler registration.
pub const KERNEL_REGISTRY_TARGET: &str = "kernel.registry";
/// Built-in kernel target for message-driven domain storage CRUD.
pub const KERNEL_STORAGE_TARGET: &str = "kernel.storage";
/// Built-in kernel target for querying logs and active task progress.
pub const KERNEL_LOGGER_TARGET: &str = "kernel.logger";
/// Built-in kernel target for streaming runtime telemetry and metrics.
pub const KERNEL_TELEMETRY_TARGET: &str = "kernel.telemetry";
/// Built-in kernel target for system notifications and alerts.
pub const KERNEL_NOTIFY_TARGET: &str = "kernel.notify";
/// Built-in kernel target for domain assembly and runtime state query.
pub const KERNEL_RUNTIME_TARGET: &str = "kernel.runtime";
/// Built-in kernel target for global message lifecycle observation.
pub const KERNEL_OBSERVER_TARGET: &str = "kernel.observer";
/// Built-in kernel target for domain supervisor lifecycle control.
pub const KERNEL_SUPERVISOR_TARGET: &str = "kernel.supervisor";

/// Built-in lifecycle / registry message types.
pub const MSG_REGISTER_HANDLERS: &str = "RegisterHandlers";
pub const MSG_UNREGISTER_HANDLERS: &str = "UnregisterHandlers";
pub const MSG_QUERY_HANDLERS: &str = "QueryHandlers";

/// Built-in runtime query message types.
pub const MSG_QUERY_RUNTIME: &str = "QueryRuntime";
pub const MSG_QUERY_DOMAIN_ASSEMBLY: &str = "QueryDomainAssembly";

/// Built-in observer message types.
pub const MSG_QUERY_OBSERVER: &str = "QueryObserver";
pub const MSG_SUBSCRIBE_OBSERVER: &str = "SubscribeObserver";
pub const MSG_UNSUBSCRIBE_OBSERVER: &str = "UnsubscribeObserver";
pub const MSG_SYSTEM_OBSERVER_EVENT: &str = "ObservationEvent";

/// Built-in supervisor control message types.
pub const MSG_START_DOMAIN: &str = "StartDomain";
pub const MSG_STOP_DOMAIN: &str = "StopDomain";
pub const MSG_RESTART_DOMAIN: &str = "RestartDomain";
pub const MSG_UPDATE_DOMAIN: &str = "UpdateDomain";

/// Built-in logging message types.
pub const MSG_QUERY_LOGS: &str = "QueryLogs";
pub const MSG_QUERY_PROGRESS: &str = "QueryProgress";
pub const MSG_SUBSCRIBE_LOGS: &str = "SubscribeLogs";
pub const MSG_UNSUBSCRIBE_LOGS: &str = "UnsubscribeLogs";

/// Built-in telemetry message types.
pub const MSG_SUBSCRIBE_TELEMETRY: &str = "SubscribeTelemetry";
pub const MSG_UNSUBSCRIBE_TELEMETRY: &str = "UnsubscribeTelemetry";

/// Built-in notification message types.
pub const MSG_SUBSCRIBE_NOTIFY: &str = "SubscribeNotify";
pub const MSG_UNSUBSCRIBE_NOTIFY: &str = "UnsubscribeNotify";

/// Built-in system event message types pushed to subscribed domain pipes.
pub const MSG_SYSTEM_LOG_EVENT: &str = "LogEvent";
pub const MSG_SYSTEM_TELEMETRY_EVENT: &str = "TelemetryEvent";
pub const MSG_SYSTEM_NOTIFY_EVENT: &str = "NotifyEvent";

/// Built-in storage CRUD message types ("Storage as Messaging").
pub const MSG_STORAGE_INSERT: &str = "StorageInsert";
pub const MSG_STORAGE_QUERY: &str = "StorageQuery";
pub const MSG_STORAGE_UPDATE: &str = "StorageUpdate";
pub const MSG_STORAGE_DELETE: &str = "StorageDelete";

/// Built-in domain message delivery persistence / QoS types.
pub const MSG_DELIVERY_DURABLE: &str = "Durable";
pub const MSG_DELIVERY_VOLATILE: &str = "Volatile";
pub const MSG_DELIVERY_EPHEMERAL: &str = "Ephemeral";

/// Metadata descriptor for a registered domain handler (`<domain>.<handler>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandlerDescriptor {
    /// Full route address `"<domain>.<handler>"`.
    pub target: String,
    /// Owning domain identifier.
    pub domain: String,
    /// Handler name within the domain.
    pub handler: String,
    /// Human/developer-readable description extracted from function docstring or comments.
    pub description: String,
    /// Current lifecycle status of the owning domain.
    pub status: DomainStatus,
}

/// Delivery persistence policy resolved from `message_type` for domain messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPolicy {
    /// Persisted in SQLite; only bounded live metadata and explicit failures reside in memory.
    Durable,
    /// Tracked in the in-memory ledger and observer only; never written to SQLite `kernel_ledger`.
    Volatile,
    /// High-frequency transient stream: tracked in-memory only while in-flight, never written to SQLite,
    /// and automatically evicted from the in-memory ledger upon reaching a terminal status.
    Ephemeral,
}

impl DeliveryPolicy {
    pub(crate) fn is_infrastructure_query(target: &str, message_type: &str) -> bool {
        match target {
            KERNEL_OBSERVER_TARGET => message_type == MSG_QUERY_OBSERVER,
            KERNEL_RUNTIME_TARGET => {
                matches!(message_type, MSG_QUERY_RUNTIME | MSG_QUERY_DOMAIN_ASSEMBLY)
            }
            KERNEL_REGISTRY_TARGET => message_type == MSG_QUERY_HANDLERS,
            KERNEL_STORAGE_TARGET => message_type == MSG_STORAGE_QUERY,
            KERNEL_LOGGER_TARGET => matches!(message_type, MSG_QUERY_LOGS | MSG_QUERY_PROGRESS),
            _ => false,
        }
    }

    /// Resolves the delivery persistence policy from `target` and `message_type`.
    pub fn resolve(target: &str, message_type: &str) -> Self {
        if Self::is_infrastructure_query(target, message_type) {
            return Self::Ephemeral;
        }
        if target == KERNEL_REGISTRY_TARGET
            || target == KERNEL_STORAGE_TARGET
            || target == KERNEL_LOGGER_TARGET
            || target == KERNEL_NOTIFY_TARGET
            || target == "web_ui.push_alert"
        {
            return Self::Volatile;
        }
        if target == KERNEL_TELEMETRY_TARGET || target == "web_ui.metric_tick" {
            return Self::Ephemeral;
        }
        Self::from_message_type(message_type)
    }

    /// Maps a `message_type` string to its `DeliveryPolicy`.
    /// Defaults custom business message types to `Durable` for backwards compatibility.
    pub fn from_message_type(message_type: &str) -> Self {
        if message_type.eq_ignore_ascii_case(MSG_DELIVERY_VOLATILE) {
            Self::Volatile
        } else if message_type.eq_ignore_ascii_case(MSG_DELIVERY_EPHEMERAL) {
            Self::Ephemeral
        } else {
            Self::Durable
        }
    }
}

/// Lifecycle states of a managed Domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainStatus {
    /// Process spawned by kernel, awaiting `RegisterHandlers` message.
    Spawning,
    /// Handlers registered and ready to receive messages.
    Active,
    /// Process crashed or pipe closed unexpectedly; routes revoked.
    Faulted,
    /// Cleanly stopped by kernel supervisor.
    Stopped,
}

/// Global contract and four-stage receipt status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractStatus {
    // --- Standard 4-Stage Lifecycle Receipts ---
    /// Stage 1: Kernel validated contract & target and wrote message to target domain pipe.
    Sent,
    /// Stage 2: Target domain read message from pipe.
    Received,
    /// Stage 3: Target domain began executing the matched handler function.
    Started,
    /// Stage 4: Target domain handler finished execution (terminal state).
    Completed,

    // --- Global Contract Rejection & Exception Statuses (Terminal) ---
    /// Target domain or handler does not exist or is not Active (kernel does NOT send message).
    TargetInvalid,
    /// Malformed frame or invalid contract metadata.
    ContractInvalid,
    /// Target handler returned an error, crashed mid-flight, or timed out.
    ExecutionFailed,
}

impl ContractStatus {
    /// Monotonic stage rank used by the kernel ledger to reject duplicate or out-of-order regressions.
    pub fn stage_rank(self) -> u8 {
        match self {
            ContractStatus::Sent => 1,
            ContractStatus::Received => 2,
            ContractStatus::Started => 3,
            ContractStatus::Completed
            | ContractStatus::TargetInvalid
            | ContractStatus::ContractInvalid
            | ContractStatus::ExecutionFailed => 4,
        }
    }

    /// Returns true if this status represents a terminal state for `message_id`.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            ContractStatus::Completed
                | ContractStatus::TargetInvalid
                | ContractStatus::ContractInvalid
                | ContractStatus::ExecutionFailed
        )
    }

    /// Returns true if a target domain is allowed to report this status in an `InboundReceipt`.
    pub fn is_valid_inbound_receipt(self) -> bool {
        matches!(
            self,
            ContractStatus::Received
                | ContractStatus::Started
                | ContractStatus::Completed
                | ContractStatus::ExecutionFailed
        )
    }
}

/// Top-level JSON-Lines frame sent from a Domain (`stdout`) to the Kernel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "frame_type", rename_all = "snake_case")]
pub enum DomainToKernelFrame {
    /// Submit a new message for routing or storage.
    Send(OutboundMessage),
    /// Report execution progress/completion receipt for a delivered message.
    Receipt(InboundReceipt),
    /// Structured multi-state log frame or ephemeral progress telemetry.
    Log(LogFramePayload),
}

/// Top-level JSON-Lines frame sent from the Kernel to a Domain (`stdin`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "frame_type", rename_all = "snake_case")]
pub enum KernelToDomainFrame {
    /// Deliver a routed message to a target domain handler.
    Deliver(KernelMessage),
    /// Push a lifecycle receipt or contract status back to the sender domain.
    Receipt(MessageReceipt),
}

/// Outbound message submitted by a Domain (no `message_id` or `source_domain` needed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboundMessage {
    /// Domain-local sequence number for pairing kernel-assigned `message_id` and submission deduplication.
    #[serde(default)]
    pub seq: u64,
    /// Target locations in `"<domain>.<handler>"` format.
    pub targets: Vec<String>,
    /// Message type (business message type or kernel storage CRUD type).
    pub message_type: String,
    /// Upper-layer contract parameter list (JSON Object).
    #[serde(default = "default_empty_object")]
    pub params: Value,
    /// Inner-layer actual data packet (standard JSON).
    #[serde(default = "default_empty_object")]
    pub data: Value,
}

/// Two-layer Kernel Message after the Kernel assigns `message_id` and binds `source_domain` from the pipe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelMessage {
    /// Kernel submission identity shared by independently identified child messages.
    pub logical_id: String,
    // ========= Upper Layer: Kernel Contract Metadata =========
    /// Globally unique child message ID generated by the Kernel.
    pub message_id: String,
    /// Authenticated sender domain bound from the physical OS pipe.
    pub source_domain: String,
    /// Specific target `"<domain>.<handler>"` for this delivery.
    pub target: String,
    /// Full list of targets originally requested.
    pub targets: Vec<String>,
    /// Message type identifier.
    pub message_type: String,
    /// Parameter list (JSON Object).
    pub params: Value,
    /// Kernel ingress timestamp (Unix milliseconds).
    pub timestamp: i64,

    // ========= Inner Layer: Standard JSON Data Packet =========
    /// Actual business data payload. Domain schema changes only affect this inner JSON.
    pub data: Value,
}

/// Receipt frame reported by a target Domain back to the Kernel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundReceipt {
    /// Kernel-assigned child message ID.
    pub message_id: String,
    /// Target `"<domain>.<handler>"` reporting this status.
    pub target: String,
    /// Reported stage (`Received`, `Started`, `Completed`, or `ExecutionFailed`).
    pub status: ContractStatus,
    /// Optional return data packet (when `Completed`) or error diagnostic JSON (when `ExecutionFailed`).
    #[serde(default)]
    pub data: Option<Value>,
}

/// Standardized receipt frame delivered by the Kernel to the sender Domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageReceipt {
    /// Kernel submission identity shared by independently identified child messages.
    pub logical_id: String,
    /// Kernel-assigned child message ID.
    pub message_id: String,
    /// Sender's original `seq` number for correlation.
    pub seq: u64,
    /// Specific target `"<domain>.<handler>"` that produced this status.
    pub target: String,
    /// Message type of the original message.
    pub message_type: String,
    /// Current contract or lifecycle status.
    pub status: ContractStatus,
    /// Optional response payload or error diagnostic JSON.
    pub data: Option<Value>,
    /// Kernel timestamp when this receipt was recorded (Unix milliseconds).
    pub timestamp: i64,
}

/// Real-time event emitted by the dedicated `KernelObserver` component.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationEvent {
    /// Kernel submission identity shared by independently identified child messages.
    pub logical_id: String,
    /// Monotonically increasing observation event sequence ID.
    pub event_id: u64,
    /// Event timestamp (Unix milliseconds).
    pub timestamp: i64,
    /// Authenticated sender domain.
    pub source_domain: String,
    /// Target `"<domain>.<handler>"`.
    pub target: String,
    /// Kernel-generated child message ID.
    pub message_id: String,
    /// Sender sequence number.
    pub seq: u64,
    /// Message type.
    pub message_type: String,
    /// Observed contract / lifecycle status.
    pub status: ContractStatus,
    /// Optional payload or diagnostic details.
    pub details: Option<Value>,
}

fn default_empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

/// Splits a `"<domain>.<handler>"` target string into `(domain, handler)`.
pub fn parse_target_address(target: &str) -> Option<(&str, &str)> {
    let (domain, handler) = target.split_once('.')?;
    if domain.trim().is_empty() || handler.trim().is_empty() || handler.contains('.') {
        return None;
    }
    Some((domain, handler))
}

/// Returns current Unix timestamp in milliseconds.
pub fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
