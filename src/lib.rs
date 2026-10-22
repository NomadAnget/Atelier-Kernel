pub mod discipline;
pub mod frontend;
pub mod ledger;
mod log_spool;
pub mod logging;
pub mod observer;
pub mod outbox;
pub mod protocol;
pub mod router;
pub mod storage;
pub mod supervisor;
pub mod web_frontend;

use std::path::Path;
use std::sync::Arc;

pub use discipline::{ConstitutionReport, ConstitutionViolation, check_constitution};
pub use frontend::InteractiveFrontendDomain;
pub use ledger::{DeliveryRecord, KernelLedger, ReceiptTransition};
pub use logging::{KernelLogHub, LogFramePayload, LogPage, LogQuery, LogRecord, LogStore, ProgressState};
pub use observer::KernelObserver;
pub use protocol::{
    ContractStatus, DeliveryPolicy, DomainStatus, DomainToKernelFrame, HandlerDescriptor,
    InboundReceipt, KERNEL_LOGGER_TARGET, KERNEL_NOTIFY_TARGET, KERNEL_OBSERVER_TARGET,
    KERNEL_REGISTRY_TARGET, KERNEL_RUNTIME_TARGET, KERNEL_STORAGE_TARGET, KERNEL_SUPERVISOR_TARGET,
    KERNEL_TELEMETRY_TARGET, KernelMessage, KernelToDomainFrame, MSG_DELIVERY_DURABLE,
    MSG_DELIVERY_EPHEMERAL, MSG_DELIVERY_VOLATILE, MSG_QUERY_DOMAIN_ASSEMBLY, MSG_QUERY_HANDLERS,
    MSG_QUERY_LOGS, MSG_QUERY_OBSERVER, MSG_QUERY_PROGRESS, MSG_QUERY_RUNTIME,
    MSG_REGISTER_HANDLERS, MSG_RESTART_DOMAIN, MSG_START_DOMAIN, MSG_STOP_DOMAIN,
    MSG_STORAGE_DELETE, MSG_STORAGE_INSERT, MSG_STORAGE_QUERY, MSG_STORAGE_UPDATE,
    MSG_SUBSCRIBE_LOGS, MSG_SUBSCRIBE_NOTIFY, MSG_SUBSCRIBE_OBSERVER, MSG_SUBSCRIBE_TELEMETRY,
    MSG_SYSTEM_LOG_EVENT, MSG_SYSTEM_NOTIFY_EVENT, MSG_SYSTEM_OBSERVER_EVENT,
    MSG_SYSTEM_TELEMETRY_EVENT, MSG_UNREGISTER_HANDLERS, MSG_UNSUBSCRIBE_LOGS,
    MSG_UNSUBSCRIBE_NOTIFY, MSG_UNSUBSCRIBE_OBSERVER, MSG_UNSUBSCRIBE_TELEMETRY, MSG_UPDATE_DOMAIN,
    MessageReceipt, ObservationEvent, OutboundMessage,
};
pub use router::{CandidateEntry, DrainingEntry, KernelRouter};
pub use storage::{DomainStorageEngine, StorageError, StoredRecord};
pub use supervisor::{
    DomainPipeClient, DomainProcessSpec, DomainSupervisor, DomainUpdateResult, KernelBootConfig,
    SupervisorError,
};
pub use web_frontend::WebFrontendDomain;

/// Top-level Atelier Kernel host combining:
/// 1. Domain Supervisor (OS child process `Stdio` pipe lifecycle & physical identity binding)
/// 2. Kernel Router (`<domain>.<handler>` routing & `TargetInvalid` pre-send interception)
/// 3. Kernel Ledger (Kernel-generated `message_id` & three-level idempotent delivery)
/// 4. Kernel Observer (Dedicated real-time message lifecycle observation)
/// 5. Domain Storage Engine (Per-domain SQLite table creation & message-driven CRUD)
/// 6. Kernel Logger & LogStore (Structured multi-state logging, ephemeral progress & SQLite hub_logs)
pub struct AtelierKernel {
    ledger: Arc<KernelLedger>,
    observer: Arc<KernelObserver>,
    storage: Arc<DomainStorageEngine>,
    logger: Arc<KernelLogHub>,
    router: Arc<KernelRouter>,
    supervisor: Arc<DomainSupervisor>,
}

impl AtelierKernel {
    /// Creates a new Atelier Kernel backed by an in-memory SQLite database.
    pub fn new_in_memory() -> Result<Self, StorageError> {
        let ledger = Arc::new(KernelLedger::open_with_db(None)?);
        let storage = Arc::new(DomainStorageEngine::new_in_memory()?);
        let logger = Arc::new(KernelLogHub::new(None)?);
        Ok(Self::from_components(ledger, storage, logger))
    }

    /// Creates a new Atelier Kernel backed by a persistent SQLite database file.
    pub fn open_with_db(db_path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let ledger = Arc::new(KernelLedger::open_with_db(Some(db_path.as_ref()))?);
        let path = ledger.database_path().expect("file-backed database path");
        let storage = Arc::new(DomainStorageEngine::open_file(&path)?);
        let logger = Arc::new(KernelLogHub::new(Some(&path))?);
        Ok(Self::from_components(ledger, storage, logger))
    }

    fn from_components(
        ledger: Arc<KernelLedger>,
        storage: Arc<DomainStorageEngine>,
        logger: Arc<KernelLogHub>,
    ) -> Self {
        let observer = Arc::new(KernelObserver::default());
        let router = Arc::new(KernelRouter::new(
            Arc::clone(&ledger),
            Arc::clone(&observer),
            Arc::clone(&storage),
            Arc::clone(&logger),
        ));
        let supervisor = Arc::new(DomainSupervisor::new(Arc::clone(&router)));
        router.set_supervisor(Arc::downgrade(&supervisor));

        Self {
            ledger,
            observer,
            storage,
            logger,
            router,
            supervisor,
        }
    }

    pub fn ledger(&self) -> &Arc<KernelLedger> {
        &self.ledger
    }

    pub fn observer(&self) -> &Arc<KernelObserver> {
        &self.observer
    }

    pub fn storage(&self) -> &Arc<DomainStorageEngine> {
        &self.storage
    }

    pub fn logger(&self) -> &Arc<KernelLogHub> {
        &self.logger
    }

    pub fn router(&self) -> &Arc<KernelRouter> {
        &self.router
    }

    pub fn supervisor(&self) -> &Arc<DomainSupervisor> {
        &self.supervisor
    }

    /// Spawns an external language-agnostic OS child process Domain connected via `Stdio` pipes.
    pub fn spawn_process_domain(&self, spec: DomainProcessSpec) -> Result<u32, SupervisorError> {
        self.supervisor.spawn_process_domain(spec)
    }

    /// Attaches an in-process Domain endpoint bound to `domain_id`.
    pub fn attach_domain(&self, domain_id: &str) -> DomainPipeClient {
        self.supervisor.attach_in_process_domain(domain_id)
    }
}

impl Drop for AtelierKernel {
    fn drop(&mut self) {
        // Signal while this host still owns its supervisor, independent of background Arcs.
        self.supervisor.shutdown();
        self.router.finish_shutdown();
    }
}
