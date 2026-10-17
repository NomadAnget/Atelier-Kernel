pub mod frontend;
pub mod ledger;
pub mod logging;
pub mod observer;
pub mod protocol;
pub mod router;
pub mod storage;
pub mod supervisor;
pub mod web_frontend;

use std::path::Path;
use std::sync::Arc;

pub use frontend::InteractiveFrontendDomain;
pub use ledger::{DeliveryRecord, KernelLedger, ReceiptTransition};
pub use logging::{KernelLogHub, LogFramePayload, LogRecord, LogStore};
pub use observer::KernelObserver;
pub use web_frontend::WebFrontendDomain;
pub use protocol::{
    ContractStatus, DeliveryPolicy, DomainStatus, DomainToKernelFrame, InboundReceipt,
    KERNEL_REGISTRY_TARGET, KERNEL_STORAGE_TARGET, KernelMessage, KernelToDomainFrame,
    MSG_DELIVERY_DURABLE, MSG_DELIVERY_EPHEMERAL, MSG_DELIVERY_VOLATILE, MSG_REGISTER_HANDLERS,
    MSG_STORAGE_DELETE, MSG_STORAGE_INSERT, MSG_STORAGE_QUERY, MSG_STORAGE_UPDATE,
    MSG_UNREGISTER_HANDLERS, MessageReceipt, ObservationEvent, OutboundMessage,
};
pub use router::KernelRouter;
pub use storage::{DomainStorageEngine, StorageError, StoredRecord};
pub use supervisor::{
    DomainPipeClient, DomainProcessSpec, DomainSupervisor, KernelBootConfig, SupervisorError,
};

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
        let storage = Arc::new(DomainStorageEngine::open_file(db_path.as_ref())?);
        let logger = Arc::new(KernelLogHub::new(Some(db_path.as_ref()))?);
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
