use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::thread;

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use crate::protocol::{
    ContractStatus, DomainToKernelFrame, InboundReceipt, KERNEL_REGISTRY_TARGET,
    KERNEL_STORAGE_TARGET, KernelToDomainFrame, MSG_REGISTER_HANDLERS, MSG_STORAGE_DELETE,
    MSG_STORAGE_INSERT, MSG_STORAGE_QUERY, MSG_STORAGE_UPDATE, OutboundMessage,
};
use crate::router::KernelRouter;

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("failed to read boot config '{path}': {source}")]
    ConfigReadFailed {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to parse boot config '{path}': {source}")]
    ConfigParseFailed {
        path: String,
        source: serde_json::Error,
    },
    #[error("failed to spawn process for domain '{domain}': {source}")]
    SpawnFailed {
        domain: String,
        source: std::io::Error,
    },
    #[error("failed to capture stdio pipes for domain '{0}'")]
    StdioCaptureFailed(String),
    #[error("unknown domain spec for '{0}'")]
    UnknownDomainSpec(String),
}

/// External Kernel Boot Configuration (`kernel_boot.json`).
/// Adding a new domain only requires adding an entry here and providing the executable/script,
/// without modifying or recompiling Rust kernel source code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelBootConfig {
    #[serde(default = "default_db_path")]
    pub db_path: String,
    #[serde(default = "default_web_bind")]
    pub web_ui_bind: String,
    #[serde(default)]
    pub domains: Vec<DomainProcessSpec>,
}

fn default_db_path() -> String {
    "./data/atelier_kernel.db".to_string()
}

fn default_web_bind() -> String {
    "127.0.0.1:8787".to_string()
}

impl KernelBootConfig {
    /// Loads `KernelBootConfig` from a JSON file path and resolves relative `db_path`
    /// and `working_dir` entries relative to the configuration file's parent directory.
    pub fn load_from_file(path: impl AsRef<std::path::Path>) -> Result<Self, SupervisorError> {
        let path_ref = path.as_ref();
        let path_str = path_ref.display().to_string();
        let raw = std::fs::read_to_string(path_ref).map_err(|source| {
            SupervisorError::ConfigReadFailed {
                path: path_str.clone(),
                source,
            }
        })?;
        let mut cfg: Self =
            serde_json::from_str(&raw).map_err(|source| SupervisorError::ConfigParseFailed {
                path: path_str,
                source,
            })?;

        if let Some(base_dir) = path_ref
            .parent()
            .filter(|p| !p.as_os_str().is_empty() && *p != std::path::Path::new("."))
        {
            let db_p = std::path::Path::new(&cfg.db_path);
            if db_p.is_relative() {
                cfg.db_path = base_dir.join(db_p).to_string_lossy().to_string();
            }
            for spec in &mut cfg.domains {
                match &spec.working_dir {
                    None => {
                        spec.working_dir = Some(base_dir.to_string_lossy().to_string());
                    }
                    Some(dir) if std::path::Path::new(dir).is_relative() => {
                        spec.working_dir =
                            Some(base_dir.join(dir).to_string_lossy().to_string());
                    }
                    _ => {}
                }
            }
        }

        Ok(cfg)
    }
}

/// Configuration for spawning a language-agnostic OS process Domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainProcessSpec {
    pub domain_id: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub working_dir: Option<String>,
}

struct ManagedProcess {
    instance_id: u64,
    child: Arc<Mutex<Child>>,
    stopping: Arc<AtomicBool>,
}

/// Kernel Domain Supervisor: takes over starting and managing the lifecycle of
/// language-agnostic Domains via OS parent-child anonymous `Stdio` pipes.
pub struct DomainSupervisor {
    router: Arc<KernelRouter>,
    specs: RwLock<HashMap<String, DomainProcessSpec>>,
    processes: Mutex<HashMap<String, ManagedProcess>>,
}

impl DomainSupervisor {
    pub fn new(router: Arc<KernelRouter>) -> Self {
        Self {
            router,
            specs: RwLock::new(HashMap::new()),
            processes: Mutex::new(HashMap::new()),
        }
    }

    pub fn router(&self) -> &Arc<KernelRouter> {
        &self.router
    }

    /// Boots all domains declared in `config.domains`.
    pub fn boot_from_config(&self, config: &KernelBootConfig) -> Vec<(String, Result<u32, SupervisorError>)> {
        let mut results = Vec::new();
        for spec in &config.domains {
            let domain_id = spec.domain_id.clone();
            let res = self.spawn_process_domain(spec.clone());
            results.push((domain_id, res));
        }
        results
    }

    /// Respawns a previously registered process domain by its `domain_id`.
    pub fn respawn_domain(&self, domain_id: &str) -> Result<u32, SupervisorError> {
        let spec = self
            .specs
            .read()
            .get(domain_id)
            .cloned()
            .ok_or_else(|| SupervisorError::UnknownDomainSpec(domain_id.to_string()))?;
        self.spawn_process_domain(spec)
    }

    /// Returns the registered `DomainProcessSpec` for `domain_id`, if it is an external process domain.
    pub fn get_spec(&self, domain_id: &str) -> Option<DomainProcessSpec> {
        self.specs.read().get(domain_id).cloned()
    }

    /// Returns all registered external `DomainProcessSpec`s.
    pub fn list_specs(&self) -> Vec<DomainProcessSpec> {
        let mut list: Vec<DomainProcessSpec> = self.specs.read().values().cloned().collect();
        list.sort_by(|a, b| a.domain_id.cmp(&b.domain_id));
        list
    }

    /// Spawns a language-agnostic child process for `spec.domain_id` and binds its
    /// `stdin` / `stdout` pipes directly to `spec.domain_id` in the Kernel.
    pub fn spawn_process_domain(&self, spec: DomainProcessSpec) -> Result<u32, SupervisorError> {
        let domain_id = spec.domain_id.clone();
        self.specs.write().insert(domain_id.clone(), spec.clone());

        // Stop any existing process for this domain first.
        let _ = self.stop_process_domain(&domain_id);

        let mut cmd = Command::new(&spec.command);
        cmd.args(&spec.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        if let Some(dir) = &spec.working_dir {
            cmd.current_dir(dir);
        }

        let mut child = cmd.spawn().map_err(|source| SupervisorError::SpawnFailed {
            domain: domain_id.clone(),
            source,
        })?;

        let pid = child.id();
        let child_stdin = child
            .stdin
            .take()
            .ok_or_else(|| SupervisorError::StdioCaptureFailed(domain_id.clone()))?;
        let child_stdout = child
            .stdout
            .take()
            .ok_or_else(|| SupervisorError::StdioCaptureFailed(domain_id.clone()))?;

        let (tx, rx) = mpsc::channel::<KernelToDomainFrame>();
        let instance_id = self.router.attach_domain_pipe(&domain_id, tx);

        let stopping = Arc::new(AtomicBool::new(false));
        let child_arc = Arc::new(Mutex::new(child));

        self.processes.lock().insert(
            domain_id.clone(),
            ManagedProcess {
                instance_id,
                child: Arc::clone(&child_arc),
                stopping: Arc::clone(&stopping),
            },
        );

        // Writer thread: Kernel -> Domain ChildStdin (JSON-Lines)
        let writer_domain = domain_id.clone();
        thread::Builder::new()
            .name(format!("kernel-pipe-tx-{writer_domain}-{instance_id}"))
            .spawn(move || {
                let mut stdin = child_stdin;
                while let Ok(frame) = rx.recv() {
                    if let Ok(line) = serde_json::to_string(&frame) {
                        if writeln!(stdin, "{line}").is_err() || stdin.flush().is_err() {
                            break;
                        }
                    }
                }
            })
            .ok();

        // Reader thread: Domain ChildStdout -> Kernel (physically bound to `(domain_id, instance_id)`)
        let reader_domain = domain_id;
        let router_ref = Arc::clone(&self.router);
        let stopping_ref = Arc::clone(&stopping);
        thread::Builder::new()
            .name(format!("kernel-pipe-rx-{reader_domain}-{instance_id}"))
            .spawn(move || {
                let reader = BufReader::new(child_stdout);
                for line in reader.lines() {
                    match line {
                        Ok(raw_line) => {
                            router_ref.handle_raw_pipe_line_for_instance(
                                &reader_domain,
                                instance_id,
                                &raw_line,
                            );
                        }
                        Err(_) => break,
                    }
                }
                // When stdout EOF is reached, if not intentionally stopped by kernel,
                // mark domain as Faulted ONLY IF `instance_id` is still the current generation.
                if !stopping_ref.load(Ordering::SeqCst) {
                    router_ref.mark_domain_faulted_for_instance(
                        &reader_domain,
                        Some(instance_id),
                        "domain process exited or closed stdout pipe",
                    );
                }
            })
            .ok();

        Ok(pid)
    }

    /// Stops a managed child process domain cleanly and revokes its routes.
    pub fn stop_process_domain(&self, domain_id: &str) -> bool {
        let managed_opt = self.processes.lock().remove(domain_id);
        if let Some(managed) = managed_opt {
            managed.stopping.store(true, Ordering::SeqCst);
            if let Some(mut child) = managed.child.try_lock() {
                let _ = child.kill();
                let _ = child.wait();
            }
            self.router.mark_domain_faulted_for_instance(
                domain_id,
                Some(managed.instance_id),
                "domain stopped by kernel supervisor",
            );
            true
        } else {
            false
        }
    }

    /// Attaches an in-process domain pipe (useful for embedded Rust domains and fast integration tests).
    /// Just like OS child processes, the returned `DomainPipeClient` has its identity physically bound
    /// to `domain_id` by the Kernel.
    pub fn attach_in_process_domain(&self, domain_id: &str) -> DomainPipeClient {
        let (tx, rx) = mpsc::channel::<KernelToDomainFrame>();
        let instance_id = self.router.attach_domain_pipe(domain_id, tx);
        DomainPipeClient {
            domain_id: domain_id.to_string(),
            instance_id,
            router: Arc::clone(&self.router),
            inbox: Mutex::new(rx),
            next_seq: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

impl Drop for DomainSupervisor {
    fn drop(&mut self) {
        let domain_ids: Vec<String> = self.processes.lock().keys().cloned().collect();
        for id in domain_ids {
            self.stop_process_domain(&id);
        }
    }
}

/// Ergonomic handle representing a Domain's bound pipe endpoint.
/// Exposes the exact 3 primitives a Domain needs:
/// 1. `register_handlers`
/// 2. `send` (plus convenience storage message helpers)
/// 3. `report_receipt` / `recv_frame`
pub struct DomainPipeClient {
    domain_id: String,
    instance_id: u64,
    router: Arc<KernelRouter>,
    inbox: Mutex<Receiver<KernelToDomainFrame>>,
    next_seq: std::sync::atomic::AtomicU64,
}

impl DomainPipeClient {
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
        let handler_list: Vec<Value> = handlers.iter().map(|h| Value::String((*h).into())).collect();
        self.send(
            vec![KERNEL_REGISTRY_TARGET.to_string()],
            MSG_REGISTER_HANDLERS,
            json!({ "handlers": handler_list }),
            json!({}),
        )
    }

    /// Primitive 2: Sends a two-layer message to one or more `"<domain>.<handler>"` targets.
    /// Returns the kernel-assigned `message_id`.
    pub fn send(
        &self,
        targets: Vec<String>,
        message_type: impl Into<String>,
        params: Value,
        data: Value,
    ) -> String {
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
    ) -> String {
        let outbound = OutboundMessage {
            seq,
            targets,
            message_type: message_type.into(),
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
        self.send(
            vec![KERNEL_STORAGE_TARGET.to_string()],
            MSG_STORAGE_INSERT,
            params,
            data,
        )
    }

    /// Helper: Sends a `StorageQuery` message to `kernel.storage`.
    pub fn storage_query(&self, params: Value) -> String {
        self.send(
            vec![KERNEL_STORAGE_TARGET.to_string()],
            MSG_STORAGE_QUERY,
            params,
            json!({}),
        )
    }

    /// Helper: Sends a `StorageUpdate` message to `kernel.storage`.
    pub fn storage_update(&self, params: Value, data: Value) -> String {
        self.send(
            vec![KERNEL_STORAGE_TARGET.to_string()],
            MSG_STORAGE_UPDATE,
            params,
            data,
        )
    }

    /// Helper: Sends a `StorageDelete` message to `kernel.storage`.
    pub fn storage_delete(&self, params: Value) -> String {
        self.send(
            vec![KERNEL_STORAGE_TARGET.to_string()],
            MSG_STORAGE_DELETE,
            params,
            json!({}),
        )
    }

    /// Receives the next frame (`Deliver(KernelMessage)` or `Receipt(MessageReceipt)`) from the Kernel pipe.
    pub fn recv_frame(&self) -> Result<KernelToDomainFrame, mpsc::RecvError> {
        self.inbox.lock().recv()
    }

    /// Receives the next frame with a timeout.
    pub fn recv_frame_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<KernelToDomainFrame, mpsc::RecvTimeoutError> {
        self.inbox.lock().recv_timeout(timeout)
    }

    /// Drains all currently queued frames from this domain's pipe without blocking.
    pub fn drain_frames(&self) -> Vec<KernelToDomainFrame> {
        let mut out = Vec::new();
        let inbox = self.inbox.lock();
        while let Ok(frame) = inbox.try_recv() {
            out.push(frame);
        }
        out
    }
}
