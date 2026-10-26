use crate::outbox::{self, PipeReceiver};
use crate::port_forwarder::{Forwarder, PortBinding};
use crate::process_diagnostics::{ProcessDiagnostics, bounded, io_error};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use crate::logging::LogRecord;
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
    #[error("invalid boot config '{path}': {reason}")]
    ConfigValidationFailed { path: String, reason: String },
    #[error("failed to spawn process for domain '{domain}': {source}")]
    SpawnFailed {
        domain: String,
        source: std::io::Error,
    },
    #[error("failed to capture stdio pipes for domain '{0}'")]
    StdioCaptureFailed(String),
    #[error("unknown domain spec for '{0}'")]
    UnknownDomainSpec(String),
    #[error("domain update validation failed for '{domain}': {reason}")]
    UpdateValidationFailed { domain: String, reason: String },
}

/// External Kernel Boot Configuration (`kernel_boot.json`).
/// Adding a new domain only requires adding an entry here and providing the executable/script,
/// without modifying or recompiling Rust kernel source code.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KernelBootConfig {
    /// Host-only origin; never part of the boot JSON or domain API.
    #[serde(skip)]
    pub source_path: Option<std::path::PathBuf>,
    #[serde(default = "default_db_path")]
    pub db_path: String,
    #[serde(default = "default_web_bind")]
    pub web_ui_bind: String,
    #[serde(default)]
    pub web_ui_enabled: bool,
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
    /// Loads `KernelBootConfig` from a JSON file path, enforces M1/M6 explicit configuration rules,
    /// and resolves relative `db_path` and `working_dir` entries relative to the configuration file's parent directory.
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
                path: path_str.clone(),
                source,
            })?;

        cfg.source_path = Some(if path_ref.is_absolute() {
            path_ref.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|source| SupervisorError::ConfigReadFailed {
                    path: path_str.clone(),
                    source,
                })?
                .join(path_ref)
        });
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
                        spec.working_dir = Some(base_dir.join(dir).to_string_lossy().to_string());
                    }
                    _ => {}
                }
            }
        }

        cfg.validate(&path_str)?;
        Ok(cfg)
    }

    /// Validates constitutional rules M1 (domain identifier format & uniqueness) and M6 (explicit config).
    pub fn validate(&self, path_str: &str) -> Result<(), SupervisorError> {
        let mut seen = HashSet::new();
        for spec in &self.domains {
            spec.validate()
                .map_err(|reason| SupervisorError::ConfigValidationFailed {
                    path: path_str.into(),
                    reason,
                })?;
            let id = spec.domain_id.as_str();
            if !seen.insert(id.to_string()) {
                return Err(SupervisorError::ConfigValidationFailed {
                    path: path_str.to_string(),
                    reason: format!("[M1/M6] duplicate domain_id '{id}' in domains list"),
                });
            }
        }
        Ok(())
    }
}

pub fn default_auto_restart() -> bool {
    true
}

pub fn default_max_restarts() -> Option<u32> {
    Some(10)
}

pub fn default_restart_delay_ms() -> u64 {
    500
}

/// Configuration for spawning a language-agnostic OS process Domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainProcessSpec {
    pub domain_id: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    /// Per-process overrides of inherited environment; kernel IPC keys are reserved.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Public ingress bindings owned by the kernel, not by a domain generation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<PortBinding>,
    #[serde(default)]
    pub working_dir: Option<String>,
    #[serde(default = "default_auto_restart")]
    pub auto_restart: bool,
    #[serde(default = "default_max_restarts")]
    pub max_restarts: Option<u32>,
    #[serde(default = "default_restart_delay_ms")]
    pub restart_delay_ms: u64,
}

impl Default for DomainProcessSpec {
    fn default() -> Self {
        Self {
            domain_id: String::new(),
            command: String::new(),
            args: Vec::new(),
            env: BTreeMap::new(),
            ports: Vec::new(),
            working_dir: None,
            auto_restart: default_auto_restart(),
            max_restarts: default_max_restarts(),
            restart_delay_ms: default_restart_delay_ms(),
        }
    }
}

impl DomainProcessSpec {
    fn validate(&self) -> Result<(), String> {
        if self.domain_id.is_empty()
            || !self
                .domain_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(format!(
                "[M1] invalid domain_id {:?}: expected non-empty ASCII alphanumeric or '_'",
                self.domain_id
            ));
        }
        if self.command.trim().is_empty()
            || self.command.contains('\0')
            || self.args.iter().any(|arg| arg.contains('\0'))
        {
            return Err("[M6] empty command or NUL in process arguments".into());
        }
        let mut bindings = HashSet::new();
        for binding in &self.ports {
            if binding.name.is_empty()
                || !bindings.insert(&binding.name)
                || binding.listen.port() == 0
            {
                return Err(
                    "[M6] port bindings require unique nonempty names and nonzero public ports"
                        .into(),
                );
            }
        }
        let mut env_names = HashSet::new();
        for (key, value) in &self.env {
            let normalized = key.to_ascii_uppercase();
            if key.is_empty() || key.contains(['=', '\0']) || value.contains('\0') {
                return Err("[M6] invalid environment key or NUL in environment value".into());
            }
            if normalized.starts_with("ATELIER_IPC_")
                || normalized == "ATELIER_PORT_BINDINGS"
                || normalized == "ATELIER_MANAGED_READY"
            {
                return Err(
                    "[C1] ATELIER_IPC_* environment keys are reserved by the kernel".into(),
                );
            }
            if !env_names.insert(normalized) {
                return Err("[M6] environment keys must not differ only by ASCII case".into());
            }
        }
        if let Some(dir) = &self.working_dir {
            if dir.contains('\0') || !std::path::Path::new(dir).is_dir() {
                return Err(format!("[M6] working_dir is not a directory: {dir:?}"));
            }
        }
        Ok(())
    }
    pub fn new(
        domain_id: impl Into<String>,
        command: impl Into<String>,
        args: Vec<String>,
    ) -> Self {
        Self {
            domain_id: domain_id.into(),
            command: command.into(),
            args,
            env: BTreeMap::new(),
            ports: Vec::new(),
            working_dir: None,
            auto_restart: default_auto_restart(),
            max_restarts: default_max_restarts(),
            restart_delay_ms: default_restart_delay_ms(),
        }
    }
}

/// Result of a smooth domain rolling update or rollback.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainUpdateResult {
    pub domain_id: String,
    pub new_pid: u32,
    pub old_pid: Option<u32>,
    pub instance_id: u64,
    pub status: String,
}

#[cfg(unix)]
unsafe extern "C" {
    fn fcntl(fd: std::os::raw::c_int, cmd: std::os::raw::c_int, ...) -> std::os::raw::c_int;
}

#[cfg(unix)]
fn clear_cloexec(fd: std::os::raw::c_int) -> std::io::Result<()> {
    const F_SETFD: std::os::raw::c_int = 2;
    let rc = unsafe { fcntl(fd, F_SETFD, 0 as std::os::raw::c_int) };
    if rc == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn allocate_non_stdio_pipes() -> std::io::Result<(
    std::io::PipeReader,
    std::io::PipeWriter,
    std::io::PipeReader,
    std::io::PipeWriter,
)> {
    use std::os::fd::AsRawFd;
    let mut held = Vec::new();
    loop {
        let (k2d_rx, k2d_tx) = std::io::pipe()?;
        let (d2k_rx, d2k_tx) = std::io::pipe()?;
        if k2d_rx.as_raw_fd() >= 3 && d2k_tx.as_raw_fd() >= 3 {
            drop(held);
            return Ok((k2d_rx, k2d_tx, d2k_rx, d2k_tx));
        }
        held.push((k2d_rx, k2d_tx, d2k_rx, d2k_tx));
        if held.len() > 8 {
            return Err(std::io::Error::other(
                "failed to allocate IPC pipe descriptors >= 3",
            ));
        }
    }
}

#[cfg(windows)]
unsafe extern "system" {
    fn SetHandleInformation(hObject: *mut std::ffi::c_void, dwMask: u32, dwFlags: u32) -> i32;
}

#[cfg(windows)]
const HANDLE_FLAG_INHERIT: u32 = 0x0000_0001;

#[cfg(windows)]
static PIPE_INHERITANCE: Mutex<()> = Mutex::new(());
const MAX_IPC_FRAME: usize = 16 * 1024 * 1024;
#[cfg(unix)]
fn nonblocking<T: std::os::fd::AsRawFd>(io: &T) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    const NONBLOCK: i32 = 4;
    #[cfg(not(target_os = "macos"))]
    const NONBLOCK: i32 = 2048;
    let fd = io.as_raw_fd();
    let flags = unsafe { fcntl(fd, 3) };
    if flags < 0 || unsafe { fcntl(fd, 4, flags | NONBLOCK) } < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}
#[cfg(windows)]
fn cancel_worker(handle: &thread::JoinHandle<()>) {
    use std::os::windows::io::AsRawHandle;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CancelSynchronousIo(thread: *mut std::ffi::c_void) -> i32;
    }
    unsafe {
        CancelSynchronousIo(handle.as_raw_handle());
    }
}
// IPC reads must be interruptible on every supported OS. Unlike diagnostic readers,
// this endpoint drains bytes already in the pipe before transport failure is published.
#[cfg(not(windows))]
fn ipc_reader(input: std::io::PipeReader) -> impl std::io::Read {
    input
}
#[cfg(windows)]
fn ipc_reader(input: std::io::PipeReader) -> impl std::io::Read {
    struct Reader(std::io::PipeReader);
    impl std::io::Read for Reader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            use std::os::windows::io::AsRawHandle;
            #[link(name = "kernel32")]
            unsafe extern "system" {
                fn PeekNamedPipe(
                    handle: *mut std::ffi::c_void,
                    buffer: *mut std::ffi::c_void,
                    size: u32,
                    read: *mut u32,
                    available: *mut u32,
                    left: *mut u32,
                ) -> i32;
            }
            if buf.is_empty() {
                return Ok(0);
            }
            let mut available = 0;
            let ok = unsafe {
                PeekNamedPipe(
                    self.0.as_raw_handle(),
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null_mut(),
                    &mut available,
                    std::ptr::null_mut(),
                )
            };
            if ok == 0 {
                let error = std::io::Error::last_os_error();
                return if matches!(error.raw_os_error(), Some(109 | 232)) {
                    Ok(0)
                } else {
                    Err(error)
                };
            }
            if available == 0 {
                return Err(std::io::ErrorKind::WouldBlock.into());
            }
            let count = buf.len().min(available as usize);
            self.0.read(&mut buf[..count])
        }
    }
    Reader(input)
}
fn pump_lines<R: std::io::Read>(
    mut input: R,
    stop: &AtomicBool,
    drain: Option<&AtomicBool>,
    mut line: impl FnMut(&str),
) -> std::io::Result<()> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 8192];
    let mut drained = 0usize;
    while !stop.load(Ordering::Acquire) {
        match input.read(&mut chunk) {
            Ok(0) => {
                if !bytes.is_empty() {
                    let text = std::str::from_utf8(&bytes)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    line(text);
                }
                return Ok(());
            }
            Ok(n) => {
                if drain.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                    drained += n;
                    if drained > MAX_IPC_FRAME {
                        return Err(std::io::Error::other("IPC failure drain exceeds limit"));
                    }
                }
                for part in chunk[..n].split_inclusive(|b| *b == b'\n') {
                    if bytes.len() + part.len() > MAX_IPC_FRAME {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "IPC/diagnostic frame exceeds limit",
                        ));
                    }
                    bytes.extend_from_slice(part);
                    if bytes.last() == Some(&b'\n') {
                        let text = std::str::from_utf8(&bytes)
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                        line(text);
                        bytes.clear();
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if drain.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                    return Err(std::io::Error::other(
                        "domain transport ended after draining buffered frames",
                    ));
                }
                thread::sleep(Duration::from_millis(5))
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
fn write_frame(
    output: &mut std::io::PipeWriter,
    frame: &KernelToDomainFrame,
    stop: &AtomicBool,
) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(frame).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_IPC_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "outbound frame exceeds IPC limit: {} bytes > {}",
                bytes.len(),
                MAX_IPC_FRAME
            ),
        ));
    }
    let mut offset = 0;
    while offset < bytes.len() {
        if stop.load(Ordering::Acquire) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "process stopping",
            ));
        }
        match output.write(&bytes[offset..]) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "closed domain pipe",
                ));
            }
            Ok(n) => offset += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5))
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    output.flush()
}
/// One owner for Child and all its workers. Workers never hold this owner or Child.
struct ManagedProcess {
    instance_id: u64,
    pid: u32,
    child: Option<Child>,
    diagnostics: Arc<ProcessDiagnostics>,
    exit_status: Option<std::process::ExitStatus>,
    stop: Arc<AtomicBool>,
    drain: Arc<AtomicBool>,
    output: crate::outbox::PipeSender,
    workers: Vec<thread::JoinHandle<()>>,
    app_ready: Arc<AtomicBool>,
    bindings: Vec<(Arc<Forwarder>, SocketAddr)>,
    ingress_active: bool,
}
impl ManagedProcess {
    fn exited(&mut self) -> bool {
        // Process exit and writer failure are not proof that the receive pipe is empty.
        // Let its sole reader consume buffered frames before revoking this generation.
        let exited = self.observe_exit();
        let output_failure = self.output.failure();
        if let Some(reason) = &output_failure {
            self.diagnostics
                .record("outbox", json!({"message":bounded(reason)}), true);
        }
        if self.workers.first().is_some_and(|w| w.is_finished())
            || output_failure.is_some()
            || exited
        {
            self.drain.store(true, Ordering::Release);
        }
        self.workers.get(1).is_some_and(|w| w.is_finished())
    }

    fn observe_exit(&mut self) -> bool {
        if self.exit_status.is_some() {
            return true;
        }
        match self.child.as_mut().map(|c| c.try_wait()) {
            Some(Ok(Some(status))) => {
                self.exit_status = Some(status);
                self.diagnostics.exit(status, false);
                true
            }
            Some(Ok(None)) => false,
            Some(Err(error)) => {
                self.diagnostics
                    .record("child_poll", io_error(&error), true);
                true
            }
            None => true,
        }
    }

    fn terminate(&mut self) {
        // Observe natural exit before any administrative kill; cleanup is not the cause.
        self.observe_exit();
        self.stop.store(true, Ordering::Release);
        self.output.close();
        #[cfg(windows)]
        for worker in &self.workers {
            cancel_worker(worker);
        }
        if let Some(child) = self.child.as_mut() {
            let cleanup = self.exit_status.is_none();
            if cleanup {
                self.diagnostics
                    .record("cleanup_requested", json!({"action":"kill"}), false);
                if let Err(error) = child.kill() {
                    self.diagnostics
                        .record("child_kill", io_error(&error), false);
                }
            }
            loop {
                match child.wait() {
                    Ok(status) => {
                        self.exit_status = Some(status);
                        self.diagnostics.exit(status, cleanup);
                        self.child = None;
                        break;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        self.diagnostics
                            .record("child_wait", io_error(&error), false);
                        break; // Preserve ownership for the supervisor's next cleanup attempt.
                    }
                }
            }
        }
        for worker in self.workers.drain(..) {
            if let Err(error) = worker.join() {
                eprintln!("kernel worker panicked for pid {}: {error:?}", self.pid);
            }
        }
    }
}
impl Drop for ManagedProcess {
    fn drop(&mut self) {
        self.terminate();
    }
}
struct DomainState {
    spec: DomainProcessSpec,
    process: Option<ManagedProcess>,
    // Ownership moves out during cleanup, but PID visibility must survive until wait/join.
    cleanup_pid: Option<u32>,
    stopped: bool,
    restart_count: u32,
    last_spawn: Option<Instant>,
    restart_at: Option<Instant>,
}
impl DomainState {
    fn take_for_cleanup(&mut self) -> Option<ManagedProcess> {
        let process = self.process.take()?;
        self.cleanup_pid = Some(process.pid);
        Some(process)
    }

    fn pid(&self) -> Option<u32> {
        self.process.as_ref().map(|p| p.pid).or(self.cleanup_pid)
    }
}
struct DomainControl {
    operation: Mutex<()>,
    state: Mutex<DomainState>,
}
struct RetiringProcess {
    domain: String,
    process: ManagedProcess,
    deadline: Instant,
}
struct DomainSupervisorInner {
    forwarders: Mutex<HashMap<(String, String), (PortBinding, Arc<Forwarder>)>>,
    retiring: Mutex<Vec<RetiringProcess>>,
    boot_config_path: RwLock<Option<std::path::PathBuf>>,
    router: Arc<KernelRouter>,
    domains: RwLock<HashMap<String, Arc<DomainControl>>>,
    shutdown: Arc<AtomicBool>,
}
impl DomainSupervisorInner {
    fn control(&self, id: &str) -> Option<Arc<DomainControl>> {
        self.domains.read().get(id).cloned()
    }
    fn ensure(&self, spec: &DomainProcessSpec) -> Result<Arc<DomainControl>, SupervisorError> {
        spec.validate()
            .map_err(|reason| SupervisorError::ConfigValidationFailed {
                path: "dynamic process specification".into(),
                reason,
            })?;
        let mut domains = self.domains.write();
        if domains.len() >= 1024 && !domains.contains_key(&spec.domain_id) {
            return Err(SupervisorError::SpawnFailed {
                domain: spec.domain_id.clone(),
                source: std::io::Error::other("kernel managed domain registry capacity exhausted"),
            });
        }
        Ok(domains
            .entry(spec.domain_id.clone())
            .or_insert_with(|| {
                Arc::new(DomainControl {
                    operation: Mutex::new(()),
                    state: Mutex::new(DomainState {
                        spec: spec.clone(),
                        process: None,
                        cleanup_pid: None,
                        stopped: false,
                        restart_count: 0,
                        last_spawn: None,
                        restart_at: None,
                    }),
                })
            })
            .clone())
    }
    fn spawn_child_pipes(
        &self,
        domain_id: &str,
        spec: &DomainProcessSpec,
    ) -> Result<(Child, std::io::PipeWriter, std::io::PipeReader), SupervisorError> {
        #[cfg(windows)]
        let _inheritance = PIPE_INHERITANCE.lock();

        #[cfg(unix)]
        let (k2d_rx, k2d_tx, d2k_rx, d2k_tx) =
            allocate_non_stdio_pipes().map_err(|source| SupervisorError::SpawnFailed {
                domain: domain_id.to_string(),
                source,
            })?;

        #[cfg(windows)]
        let (k2d_rx, k2d_tx, d2k_rx, d2k_tx) = {
            let (k2d_rx, k2d_tx) =
                std::io::pipe().map_err(|source| SupervisorError::SpawnFailed {
                    domain: domain_id.to_string(),
                    source,
                })?;
            let (d2k_rx, d2k_tx) =
                std::io::pipe().map_err(|source| SupervisorError::SpawnFailed {
                    domain: domain_id.to_string(),
                    source,
                })?;
            (k2d_rx, k2d_tx, d2k_rx, d2k_tx)
        };

        #[cfg(unix)]
        let (r_fd, w_fd) = {
            use std::os::fd::AsRawFd;
            let r = k2d_rx.as_raw_fd();
            let w = d2k_tx.as_raw_fd();
            (r, w)
        };

        #[cfg(windows)]
        let (r_handle, w_handle) = {
            use std::os::windows::io::AsRawHandle;
            let r_h = k2d_rx.as_raw_handle();
            let w_h = d2k_tx.as_raw_handle();
            unsafe {
                if SetHandleInformation(r_h as *mut _, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT)
                    == 0
                    || SetHandleInformation(w_h as *mut _, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT)
                        == 0
                {
                    return Err(SupervisorError::SpawnFailed {
                        domain: domain_id.to_string(),
                        source: std::io::Error::last_os_error(),
                    });
                }
            }
            (r_h as usize, w_h as usize)
        };

        let configure_cmd = |cmd: &mut Command| {
            cmd.args(&spec.args)
                .env_remove("ATELIER_PORT_BINDINGS")
                .env_remove("ATELIER_MANAGED_READY")
                .envs(&spec.env)
                .env_remove("ATELIER_IPC_READ_FD")
                .env_remove("ATELIER_IPC_WRITE_FD")
                .env_remove("ATELIER_IPC_HANDLE_MODE")
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            if let Some(dir) = &spec.working_dir {
                cmd.current_dir(dir);
            }
            #[cfg(unix)]
            {
                cmd.env("ATELIER_IPC_READ_FD", r_fd.to_string())
                    .env("ATELIER_IPC_WRITE_FD", w_fd.to_string());
                // Only the child clears CLOEXEC. Concurrent Unix spawns cannot inherit each other's pipes.
                use std::os::unix::process::CommandExt;
                unsafe {
                    cmd.pre_exec(move || {
                        clear_cloexec(r_fd)?;
                        clear_cloexec(w_fd)?;
                        Ok(())
                    });
                }
            }
            #[cfg(windows)]
            {
                cmd.env("ATELIER_IPC_READ_FD", r_handle.to_string())
                    .env("ATELIER_IPC_WRITE_FD", w_handle.to_string())
                    .env("ATELIER_IPC_HANDLE_MODE", "win32");
            }
        };

        let command_str = if cfg!(windows) && spec.command == "python3" {
            "python"
        } else {
            &spec.command
        };
        let mut cmd = Command::new(command_str);
        configure_cmd(&mut cmd);

        let child_res = match cmd.spawn() {
            Ok(c) => Ok(c),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let alt_cmd = if command_str == "python3" {
                    Some("python")
                } else if command_str == "python" {
                    Some("python3")
                } else {
                    None
                };

                if let Some(alt) = alt_cmd {
                    let mut fallback = Command::new(alt);
                    configure_cmd(&mut fallback);
                    fallback
                        .spawn()
                        .map_err(|source| SupervisorError::SpawnFailed {
                            domain: domain_id.to_string(),
                            source,
                        })
                } else {
                    Err(SupervisorError::SpawnFailed {
                        domain: domain_id.to_string(),
                        source: e,
                    })
                }
            }
            Err(e) => Err(SupervisorError::SpawnFailed {
                domain: domain_id.to_string(),
                source: e,
            }),
        };

        // Explicitly drop the child-side pipe endpoints in the parent process immediately
        // so the Kernel only holds (k2d_tx, d2k_rx) and receives immediate EOF when the domain exits.
        drop(k2d_rx);
        drop(d2k_tx);

        let child = child_res?;
        Ok((child, k2d_tx, d2k_rx))
    }

    fn launch(
        &self,
        spec: &DomainProcessSpec,
        candidate: bool,
    ) -> Result<ManagedProcess, SupervisorError> {
        let id = &spec.domain_id;
        let mut child_spec = spec.clone();
        child_spec.env.remove("ATELIER_PORT_BINDINGS");
        child_spec.env.remove("ATELIER_MANAGED_READY");
        let mut bindings = Vec::new();
        let mut endpoints = serde_json::Map::new();
        for binding in &spec.ports {
            let relay = {
                let mut map = self.forwarders.lock();
                let key = (id.clone(), binding.name.clone());
                if let Some((existing, relay)) = map.get(&key) {
                    if existing != binding {
                        return Err(SupervisorError::UpdateValidationFailed { domain:id.clone(), reason:"changing an existing public binding requires a separate binding migration".into() });
                    }
                    relay.clone()
                } else {
                    let relay = Forwarder::spawn(binding.listen).map_err(|source| {
                        SupervisorError::SpawnFailed {
                            domain: id.clone(),
                            source,
                        }
                    })?;
                    map.insert(key, (binding.clone(), relay.clone()));
                    relay
                }
            };
            let reservation = TcpListener::bind("127.0.0.1:0").map_err(|source| {
                SupervisorError::SpawnFailed {
                    domain: id.clone(),
                    source,
                }
            })?;
            let address =
                reservation
                    .local_addr()
                    .map_err(|source| SupervisorError::SpawnFailed {
                        domain: id.clone(),
                        source,
                    })?;
            endpoints.insert(
                binding.name.clone(),
                json!({"host":address.ip().to_string(),"port":address.port()}),
            );
            bindings.push((relay, address));
            drop(reservation);
        }
        if !bindings.is_empty() {
            child_spec.env.insert(
                "ATELIER_PORT_BINDINGS".into(),
                serde_json::Value::Object(endpoints).to_string(),
            );
            child_spec
                .env
                .insert("ATELIER_MANAGED_READY".into(), "1".into());
        }
        let (mut child, mut tx, rx) = self.spawn_child_pipes(id, &child_spec)?;
        let pid = child.id();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let (sender, inbox) =
            outbox::channel(self.router.ledger().db_conn.as_ref().unwrap().clone());
        let generation = if candidate {
            self.router.attach_candidate_pipe(id, sender.clone())
        } else {
            self.router.attach_domain_pipe(id, sender.clone())
        };
        let mut process = ManagedProcess {
            instance_id: generation,
            pid,
            child: Some(child),
            diagnostics: Arc::new(ProcessDiagnostics::new(id, pid, generation)),
            exit_status: None,
            stop: Arc::new(AtomicBool::new(false)),
            drain: Arc::new(AtomicBool::new(false)),
            output: sender,
            workers: Vec::new(),
            app_ready: Arc::new(AtomicBool::new(false)),
            bindings,
            ingress_active: false,
        };
        let result = (|| -> std::io::Result<()> {
            #[cfg(unix)]
            {
                nonblocking(&tx)?;
                nonblocking(&rx)?;
                if let Some(io) = &stdout {
                    nonblocking(io)?;
                }
                if let Some(io) = &stderr {
                    nonblocking(io)?;
                }
            }
            let stop = process.stop.clone();
            let drain = process.drain.clone();
            let diagnostics = process.diagnostics.clone();
            let output = process.output.clone();
            process.workers.push(
                thread::Builder::new()
                    .name(format!("kernel-tx-{id}-{generation}"))
                    .spawn(move || {
                        while !stop.load(Ordering::Acquire) {
                            match inbox.recv_timeout(Duration::from_millis(50)) {
                                Ok(frame) => {
                                    if let Err(error) = write_frame(&mut tx, &frame, &stop) {
                                        if !stop.load(Ordering::Acquire) {
                                            diagnostics.record("tx_write", io_error(&error), true);
                                        }
                                        break;
                                    }
                                    if let Err(error) = inbox.acknowledge() {
                                        diagnostics.record("tx_acknowledge", json!({"message":bounded(error.0)}), true);
                                        break;
                                    }
                                }
                                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                                Err(_) => {
                                    if !stop.load(Ordering::Acquire) {
                                        diagnostics.record("outbox", json!({"message":bounded(output.failure().unwrap_or_else(|| "output queue disconnected".into()))}), true);
                                    }
                                    break;
                                }
                            }
                        }
                        if !stop.load(Ordering::Acquire) {
                            drain.store(true, Ordering::Release);
                        }
                    })?,
            );
            let diagnostics = process.diagnostics.clone();
            let weak = Arc::downgrade(&self.router);
            let stop = process.stop.clone();
            let domain = id.clone();
            let drain = process.drain.clone();
            let app_ready = process.app_ready.clone();
            process.workers.push(
                thread::Builder::new()
                    .name(format!("kernel-rx-{id}-{generation}"))
                    .spawn(move || {
                        let result = pump_lines(ipc_reader(rx), &stop, Some(&drain), |line| {
                            if line.len() < 8192 {
                                if let Ok(value)=serde_json::from_str::<Value>(line) {
                                    if value.get("frame_type").and_then(Value::as_str)==Some("ready") {
                                        if let Some(error)=value.get("error").and_then(Value::as_str) {
                                            diagnostics.record("application_startup",json!({"message":bounded(error)}),true);
                                            drain.store(true,Ordering::Release);
                                        } else { app_ready.store(true,Ordering::Release); }
                                        return;
                                    }
                                }
                            }
                            if let Some(router) = weak.upgrade() {
                                router.handle_raw_pipe_line_for_instance(&domain, generation, line);
                            }
                        });
                        if !stop.load(Ordering::Acquire) {
                            // Only the synthesized drain marker is secondary; retain real
                            // read/decoding errors even when a concurrent drain has begun.
                            let draining = matches!(&result, Err(error) if error.to_string() == "domain transport ended after draining buffered frames");
                            let details = match &result {
                                Ok(()) => json!({"eof":true,"draining":draining}),
                                Err(error) => io_error(error),
                            };
                            diagnostics.record(
                                if draining { "rx_drain" } else { "rx_read" },
                                details,
                                !draining,
                            );
                            if let Some(router) = weak.upgrade() {
                                router.mark_domain_faulted_for_instance(
                                    &domain,
                                    Some(generation),
                                    &format!("domain IPC closed: {result:?}"),
                                );
                            }
                        }
                    })?,
            );
            for (label, input) in [
                (
                    "stdout",
                    stdout.map(|s| Box::new(s) as Box<dyn std::io::Read + Send>),
                ),
                (
                    "stderr",
                    stderr.map(|s| Box::new(s) as Box<dyn std::io::Read + Send>),
                ),
            ] {
                if let Some(input) = input {
                    let weak = Arc::downgrade(&self.router);
                    let stop = process.stop.clone();
                    let domain = id.clone();
                    process.workers.push(
                        thread::Builder::new()
                            .name(format!("kernel-{label}-{id}-{generation}"))
                            .spawn(move || {
                                let result = pump_lines(input, &stop, None, |text| {
                                    if let Some(router) = weak.upgrade() {
                                        router.logger().reinject(LogRecord {
                                            source_domain: Some(domain.clone()),
                                            component: domain.clone(),
                                            layer: "SUBPROCESS".into(),
                                            level: if label == "stderr" {
                                                "WARNING"
                                            } else {
                                                "INFO"
                                            }
                                            .into(),
                                            message: text.trim_end().into(),
                                            ..LogRecord::default()
                                        });
                                    }
                                });
                                if let Err(error) = result {
                                    if !stop.load(Ordering::Acquire) {
                                        eprintln!("kernel {domain} {label} reader failed: {error}");
                                    }
                                }
                            })?,
                    );
                }
            }
            Ok(())
        })();
        if let Err(source) = result {
            if candidate {
                self.router.abort_candidate(id, generation);
            } else {
                self.router.mark_domain_faulted_for_instance(
                    id,
                    Some(generation),
                    "cannot start domain workers",
                );
            }
            return Err(SupervisorError::SpawnFailed {
                domain: id.clone(),
                source,
            });
        }
        Ok(process)
    }
    fn stop_owned(
        &self,
        id: &str,
        control: &DomainControl,
        mut process: ManagedProcess,
        reason: &str,
    ) -> bool {
        for (relay, _) in &process.bindings {
            let _ = relay.switch(None);
        }
        if control.state.lock().stopped {
            self.router
                .mark_domain_stopped_for_instance(id, Some(process.instance_id), reason);
        } else {
            self.router
                .mark_domain_faulted_for_instance(id, Some(process.instance_id), reason);
        }
        process.terminate();
        // No state/router locks are held while killing, waiting, or joining workers.
        let mut state = control.state.lock();
        if process.child.is_some() {
            state.process = Some(process);
            false
        } else {
            state.cleanup_pid = None;
            true
        }
    }
    fn monitor(&self) {
        let forwarders: Vec<_> = self
            .forwarders
            .lock()
            .values()
            .map(|(_, p)| p.clone())
            .collect();
        for relay in forwarders {
            if let Err(error) = relay.ensure_alive() {
                eprintln!("kernel port forwarder recovery failed: {error}");
            }
        }
        let retiring = std::mem::take(&mut *self.retiring.lock());
        for mut old in retiring {
            let drained = old
                .process
                .bindings
                .iter()
                .all(|(relay, backend)| relay.connections(*backend).is_ok_and(|n| n == 0));
            if drained || Instant::now() >= old.deadline || self.shutdown.load(Ordering::Acquire) {
                self.router
                    .cleanup_draining_instance(&old.domain, old.process.instance_id);
                old.process.terminate();
                if old.process.child.is_some() {
                    self.retiring.lock().push(old);
                }
            } else {
                self.retiring.lock().push(old);
            }
        }
        for (id, generation, reason) in self.router.failed_output_queues() {
            // A managed receiver owns the drain-before-fault boundary. In-process domains
            // have no reader worker, so their output failures still converge here directly.
            let managed = self.control(&id).is_some_and(|control| {
                // Launch/update owns workers before installing them in DomainState.
                // Do not race that owner while its lifecycle operation is in progress.
                let Some(_operation) = control.operation.try_lock() else {
                    return true;
                };
                control
                    .state
                    .lock()
                    .process
                    .as_ref()
                    .is_some_and(|p| p.instance_id == generation)
            });
            if !managed {
                self.router
                    .mark_domain_faulted_for_instance(&id, Some(generation), &reason);
            }
        }
        let domains: Vec<_> = self
            .domains
            .read()
            .iter()
            .map(|(id, c)| (id.clone(), c.clone()))
            .collect();
        for (id, control) in domains {
            {
                let mut state = control.state.lock();
                if let Some(process) = state.process.as_mut() {
                    if !process.ingress_active
                        && !process.bindings.is_empty()
                        && process.app_ready.load(Ordering::Acquire)
                    {
                        let switched = process
                            .bindings
                            .iter()
                            .all(|(relay, backend)| relay.switch(Some(*backend)).is_ok());
                        process.ingress_active = switched;
                    }
                }
            }

            if self.shutdown.load(Ordering::Acquire) {
                break;
            }
            let Some(_operation) = control.operation.try_lock() else {
                continue;
            };
            let failed = {
                let mut state = control.state.lock();
                let faulted =
                    self.router.domain_status(&id) == Some(crate::protocol::DomainStatus::Faulted);
                if state
                    .process
                    .as_mut()
                    .is_some_and(|p| faulted || p.exited())
                {
                    state.take_for_cleanup()
                } else {
                    None
                }
            };
            if let Some(process) = failed {
                if !self.stop_owned(&id, &control, process, "domain disconnected") {
                    continue;
                }
                let mut state = control.state.lock();
                if state
                    .last_spawn
                    .is_some_and(|at| at.elapsed() > Duration::from_secs(5))
                {
                    state.restart_count = 0;
                }
                if !state.stopped && state.spec.auto_restart {
                    // Public counter includes the final denied restart attempt, as before.
                    state.restart_count = state.restart_count.saturating_add(1);
                    if state
                        .spec
                        .max_restarts
                        .is_none_or(|n| state.restart_count <= n)
                    {
                        state.restart_at = Some(
                            Instant::now() + Duration::from_millis(state.spec.restart_delay_ms),
                        );
                    }
                }
            }
            let restart = {
                let mut state = control.state.lock();
                if !state.stopped && state.restart_at.is_some_and(|at| Instant::now() >= at) {
                    state.restart_at = None;
                    Some(state.spec.clone())
                } else {
                    None
                }
            };
            if let Some(spec) = restart {
                match self.launch(&spec, false) {
                    Ok(process) => {
                        let mut s = control.state.lock();
                        s.process = Some(process);
                        s.last_spawn = Some(Instant::now());
                    }
                    Err(error) => {
                        eprintln!("kernel auto-restart {id} failed: {error}");
                        self.router.record_spawn_failure(&id, &error.to_string());
                        let mut s = control.state.lock();
                        s.restart_count = s.restart_count.saturating_add(1);
                        if s.spec.max_restarts.is_none_or(|max| s.restart_count <= max) {
                            s.restart_at = Some(
                                Instant::now() + Duration::from_millis(s.spec.restart_delay_ms),
                            );
                        }
                    }
                }
            }
        }
    }
}
/// Owns its monitor. The monitor holds only a Weak reference and exits before process teardown.
pub struct DomainSupervisor {
    inner: Arc<DomainSupervisorInner>,
    monitor: Mutex<Option<thread::JoinHandle<()>>>,
}
impl DomainSupervisor {
    pub fn new(router: Arc<KernelRouter>) -> Self {
        let inner = Arc::new(DomainSupervisorInner {
            boot_config_path: RwLock::new(None),
            forwarders: Mutex::new(HashMap::new()),
            retiring: Mutex::new(Vec::new()),
            router,
            domains: RwLock::new(HashMap::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        });
        let weak = Arc::downgrade(&inner);
        let stop = inner.shutdown.clone();
        let handle = thread::Builder::new()
            .name("kernel-supervisor-monitor".into())
            .spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    if let Some(inner) = weak.upgrade() {
                        inner.monitor();
                    } else {
                        break;
                    }
                    thread::sleep(Duration::from_millis(25));
                }
            })
            .expect("cannot start kernel process monitor");
        Self {
            inner,
            monitor: Mutex::new(Some(handle)),
        }
    }
    pub(crate) fn shutdown_signal(&self) -> Arc<AtomicBool> {
        self.inner.shutdown.clone()
    }
    pub(crate) fn is_shutting_down(&self) -> bool {
        self.inner.shutdown.load(Ordering::Acquire)
    }
    pub(crate) fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        if let Some(worker) = self.monitor.lock().take() {
            let _ = worker.join();
        }
        let ids: Vec<_> = self.inner.domains.read().keys().cloned().collect();
        for id in ids {
            self.stop_process_domain(&id);
        }
        let retiring = std::mem::take(&mut *self.inner.retiring.lock());
        for mut old in retiring {
            old.process.terminate();
        }
        self.inner.forwarders.lock().clear();
    }
    fn require_running(&self, domain: &str) -> Result<(), SupervisorError> {
        if self.is_shutting_down() {
            return Err(SupervisorError::SpawnFailed {
                domain: domain.into(),
                source: std::io::Error::other("kernel host is closing"),
            });
        }
        Ok(())
    }
    pub fn router(&self) -> &Arc<KernelRouter> {
        &self.inner.router
    }
    pub fn is_stopped(&self, id: &str) -> bool {
        self.inner
            .control(id)
            .is_some_and(|c| c.state.lock().stopped)
    }
    pub fn restart_count(&self, id: &str) -> u32 {
        self.inner
            .control(id)
            .map_or(0, |c| c.state.lock().restart_count)
    }
    pub fn boot_from_config(
        &self,
        config: &KernelBootConfig,
    ) -> Vec<(String, Result<u32, SupervisorError>)> {
        *self.inner.boot_config_path.write() = config.source_path.clone();
        config
            .domains
            .iter()
            .map(|s| (s.domain_id.clone(), self.spawn_process_domain(s.clone())))
            .collect()
    }
    pub fn respawn_domain(&self, id: &str) -> Result<u32, SupervisorError> {
        let spec = self
            .get_spec(id)
            .ok_or_else(|| SupervisorError::UnknownDomainSpec(id.into()))?;
        self.spawn_process_domain(spec)
    }
    pub fn get_spec(&self, id: &str) -> Option<DomainProcessSpec> {
        self.inner.control(id).map(|c| c.state.lock().spec.clone())
    }
    pub fn list_specs(&self) -> Vec<DomainProcessSpec> {
        let controls: Vec<_> = self.inner.domains.read().values().cloned().collect();
        let mut specs: Vec<_> = controls
            .into_iter()
            .map(|c| c.state.lock().spec.clone())
            .collect();
        specs.sort_by(|a, b| a.domain_id.cmp(&b.domain_id));
        specs
    }
    pub fn spawn_process_domain(&self, spec: DomainProcessSpec) -> Result<u32, SupervisorError> {
        let id = spec.domain_id.clone();
        self.require_running(&id)?;
        let control = self.inner.ensure(&spec)?;
        let _operation = control.operation.lock();
        self.require_running(&id)?;
        let previous = {
            let mut state = control.state.lock();
            state.stopped = false;
            state.restart_count = 0;
            state.restart_at = None;
            state.take_for_cleanup()
        };
        if let Some(process) = previous {
            if !self
                .inner
                .stop_owned(&id, &control, process, "domain restarted")
            {
                return Err(SupervisorError::SpawnFailed {
                    domain: id,
                    source: std::io::Error::other("previous domain process could not be reaped"),
                });
            }
        }
        let process = match self.inner.launch(&spec, false) {
            Ok(process) => process,
            Err(error) => {
                self.inner
                    .router
                    .record_spawn_failure(&id, &error.to_string());
                return Err(error);
            }
        };
        let pid = process.pid;
        let mut state = control.state.lock();
        state.spec = spec;
        state.process = Some(process);
        state.last_spawn = Some(Instant::now());
        Ok(pid)
    }
    pub fn stop_process_domain(&self, id: &str) -> bool {
        let Some(control) = self.inner.control(id) else {
            return false;
        };
        let _operation = control.operation.lock();
        let process = {
            let mut state = control.state.lock();
            state.stopped = true;
            state.restart_at = None;
            state.take_for_cleanup()
        };
        if let Some(process) = process {
            self.inner
                .stop_owned(id, &control, process, "domain stopped by kernel supervisor")
        } else {
            self.inner.router.mark_domain_stopped_for_instance(
                id,
                None,
                "domain stopped by kernel supervisor",
            );
            false
        }
    }
    pub fn attach_in_process_domain(&self, id: &str) -> DomainPipeClient {
        let (tx, rx) =
            outbox::channel(self.inner.router.ledger().db_conn.as_ref().unwrap().clone());
        let generation = self.inner.router.attach_domain_pipe(id, tx);
        DomainPipeClient {
            domain_id: id.into(),
            instance_id: generation,
            router: self.inner.router.clone(),
            inbox: Mutex::new(rx),
            next_seq: std::sync::atomic::AtomicU64::new(1),
        }
    }
    pub fn get_pid(&self, id: &str) -> Option<u32> {
        self.inner.control(id).and_then(|c| c.state.lock().pid())
    }
    pub fn update_process_domain(
        &self,
        id: &str,
        new_spec: Option<DomainProcessSpec>,
        _probe_handler: Option<&str>,
        timeout: Duration,
    ) -> Result<DomainUpdateResult, SupervisorError> {
        let spec = match new_spec {
            Some(spec) => spec,
            None => {
                let path = self.inner.boot_config_path.read().clone();
                if let Some(path) = path {
                    KernelBootConfig::load_from_file(path)?
                        .domains
                        .into_iter()
                        .find(|spec| spec.domain_id == id)
                        .ok_or_else(|| SupervisorError::UnknownDomainSpec(id.into()))?
                } else {
                    // Embedded hosts without a file retain their explicit in-memory configuration.
                    self.get_spec(id)
                        .ok_or_else(|| SupervisorError::UnknownDomainSpec(id.into()))?
                }
            }
        };
        if spec.domain_id != id {
            return Err(SupervisorError::UpdateValidationFailed {
                domain: id.into(),
                reason: "candidate domain identity mismatch".into(),
            });
        }
        self.require_running(id)?;
        let control = self.inner.ensure(&spec)?;
        let operation = control.operation.lock();
        self.require_running(id)?;
        let binding_changed = {
            let state = control.state.lock();
            state.process.is_some() && state.spec.ports != spec.ports
        };
        if binding_changed {
            return Err(SupervisorError::UpdateValidationFailed{domain:id.into(),reason:"public binding changes require host binding migration; existing process retained".into()});
        }
        let old_pid = control.state.lock().process.as_ref().map(|p| p.pid);
        if old_pid.is_none() {
            drop(operation);
            let pid = self.spawn_process_domain(spec)?;
            return Ok(DomainUpdateResult {
                domain_id: id.into(),
                new_pid: pid,
                old_pid: None,
                instance_id: self.inner.router.domain_instance_id(id).unwrap_or(0),
                status: "started".into(),
            });
        }
        let mut candidate = self.inner.launch(&spec, true)?;
        let generation = candidate.instance_id;
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline
            && !candidate.exited()
            && !candidate.drain.load(Ordering::Acquire)
            && !self.inner.shutdown.load(Ordering::Acquire)
        {
            if self.inner.router.is_candidate_ready(id, generation)
                && (candidate.bindings.is_empty() || candidate.app_ready.load(Ordering::Acquire))
            {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        if self.is_shutting_down()
            || candidate.exited()
            || candidate.drain.load(Ordering::Acquire)
            || !self.inner.router.is_candidate_ready(id, generation)
            || (!candidate.bindings.is_empty() && !candidate.app_ready.load(Ordering::Acquire))
        {
            self.inner.router.abort_candidate(id, generation);
            // Process exit may be observed before the receive worker has consumed
            // the final application-startup error frame already in its pipe.
            // Give that worker a bounded chance to finish before reporting the
            // candidate failure; do not stop it or discard its buffered input.
            if candidate.exit_status.is_some() {
                let evidence_deadline = Instant::now() + Duration::from_millis(300);
                while !candidate
                    .workers
                    .get(1)
                    .is_some_and(|worker| worker.is_finished())
                    && Instant::now() < evidence_deadline
                {
                    thread::sleep(Duration::from_millis(5));
                }
            }
            let failure = format!(
                "pid={}, exit={:?}, first_fault={}, transport={:?}",
                candidate.pid,
                candidate.exit_status,
                candidate.diagnostics.first_fault().unwrap_or(Value::Null),
                candidate.output.failure()
            );
            candidate.terminate();
            return Err(SupervisorError::UpdateValidationFailed {
                domain: id.into(),
                reason: format!("candidate exited or failed application readiness: {failure}"),
            });
        }
        let previous_bindings = control
            .state
            .lock()
            .process
            .as_ref()
            .map(|p| p.bindings.clone())
            .unwrap_or_default();
        let switched = candidate
            .bindings
            .iter()
            .try_for_each(|(relay, backend)| relay.switch(Some(*backend)));
        if let Err(error) = switched {
            for (relay, backend) in &previous_bindings {
                let _ = relay.switch(Some(*backend));
            }
            self.inner.router.abort_candidate(id, generation);
            candidate.terminate();
            return Err(SupervisorError::UpdateValidationFailed {
                domain: id.into(),
                reason: format!("ingress switch failed: {error}"),
            });
        }
        if !self.inner.router.commit_candidate(id, generation) {
            for (relay, backend) in &previous_bindings {
                let _ = relay.switch(Some(*backend));
            }
            self.inner.router.abort_candidate(id, generation);
            candidate.terminate();
            return Err(SupervisorError::UpdateValidationFailed {
                domain: id.into(),
                reason: "candidate lost readiness before commit".into(),
            });
        }
        candidate.ingress_active = true;
        let pid = candidate.pid;
        let previous = {
            let mut state = control.state.lock();
            state.spec = spec;
            state.stopped = false;
            state.restart_count = 0;
            state.last_spawn = Some(Instant::now());
            state.process.replace(candidate)
        };
        if let Some(mut old) = previous {
            if old.bindings.is_empty() {
                thread::sleep(Duration::from_millis(600));
                self.inner
                    .router
                    .cleanup_draining_instance(id, old.instance_id);
                old.terminate();
            } else {
                self.inner.retiring.lock().push(RetiringProcess {
                    domain: id.into(),
                    process: old,
                    deadline: Instant::now() + Duration::from_secs(30),
                });
            }
        }
        Ok(DomainUpdateResult {
            domain_id: id.into(),
            new_pid: pid,
            old_pid,
            instance_id: generation,
            status: "updated".into(),
        })
    }
}
impl Drop for DomainSupervisor {
    fn drop(&mut self) {
        self.shutdown();
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
    inbox: Mutex<PipeReceiver>,
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

#[cfg(test)]
mod domain_resource_acceptance {
    use super::*;
    #[test]
    fn dynamic_domain_metadata_has_explicit_limit_without_breaking_existing_control() {
        let k = crate::AtelierKernel::new_in_memory().unwrap();
        for n in 0..1024 {
            k.supervisor()
                .inner
                .ensure(&DomainProcessSpec::new(
                    format!("bounded_{n}"),
                    "never-executed",
                    vec![],
                ))
                .unwrap();
        }
        assert!(
            k.supervisor()
                .inner
                .ensure(&DomainProcessSpec::new(
                    "over-limit",
                    "never-executed",
                    vec![]
                ))
                .is_err()
        );
        assert!(
            k.supervisor()
                .inner
                .ensure(&DomainProcessSpec::new(
                    "bounded_0",
                    "never-executed",
                    vec![]
                ))
                .is_ok()
        );
        assert_eq!(k.supervisor().list_specs().len(), 1024);
    }
}

#[cfg(test)]
mod exit_drain_regression {
    use super::*;
    #[test]
    fn exited_child_must_not_cancel_its_buffered_receipts() {
        let k = crate::AtelierKernel::new_in_memory().unwrap();
        let mut child = Command::new(if cfg!(windows) { "python" } else { "python3" })
            .args(["-c", "print('received\\nstarted\\ncompleted')"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let input = child.stdout.take().unwrap();
        let pid = child.id();
        let stop = Arc::new(AtomicBool::new(false));
        let (release, paused) = mpsc::channel();
        let (lines, records) = mpsc::channel();
        let flag = stop.clone();
        let rx = thread::spawn(move || {
            paused.recv().unwrap();
            pump_lines(input, &flag, None, |line| {
                lines.send(line.trim().to_owned()).unwrap();
            })
            .unwrap();
        });
        child.wait().unwrap(); // OS exit is certain, while all receipts remain unread.
        let (output, _inbox) = outbox::channel(k.ledger().db_conn.as_ref().unwrap().clone());
        let mut process = ManagedProcess {
            instance_id: 1,
            pid,
            child: Some(child),
            diagnostics: Arc::new(ProcessDiagnostics::new("probe", pid, 1)),
            exit_status: None,
            stop,
            drain: Arc::new(AtomicBool::new(false)),
            output,
            workers: vec![thread::spawn(|| {}), rx],
            app_ready: Arc::new(AtomicBool::new(false)),
            bindings: Vec::new(),
            ingress_active: false,
        };
        let premature = process.exited();
        release.send(()).unwrap(); // Always release before assertion, including on the old code.
        let deadline = Instant::now() + Duration::from_secs(2);
        while !process.workers[1].is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            !premature,
            "OS exit or a finished writer must not cancel unread inbound frames"
        );
        assert_eq!(
            records.try_iter().collect::<Vec<_>>(),
            ["received", "started", "completed"]
        );
    }
}

#[cfg(test)]
mod bounded_transport_drain_test {
    use super::*;
    #[test]
    fn failed_transport_drains_available_frames_without_waiting_for_inherited_writer() {
        use std::io::Write;
        let (input, mut inherited_writer) = std::io::pipe().unwrap();
        #[cfg(unix)]
        nonblocking(&input).unwrap();
        inherited_writer
            .write_all(b"received\nstarted\ncompleted\n")
            .unwrap();
        let mut lines = Vec::new();
        let result = pump_lines(
            ipc_reader(input),
            &AtomicBool::new(false),
            Some(&AtomicBool::new(true)),
            |line| lines.push(line.trim().to_owned()),
        );
        assert!(result.is_err());
        assert_eq!(lines, ["received", "started", "completed"]);
        // The writer is intentionally still alive: no timeout or EOF was needed to finish.
        drop(inherited_writer);
    }
}

#[cfg(test)]
mod diagnostic_pipe_tests {
    use super::*;
    #[test]
    fn broken_pipe_preserves_operating_system_error() {
        let (reader, mut writer) = std::io::pipe().unwrap();
        drop(reader);
        let frame: KernelToDomainFrame = serde_json::from_value(json!({
            "frame_type":"receipt","message_id":"probe","logical_id":"probe","seq":1,
            "source_domain":"kernel","target":"probe.ping","message_type":"Durable",
            "status":"completed","timestamp":0,"data":null
        }))
        .unwrap();
        let error = write_frame(&mut writer, &frame, &AtomicBool::new(false)).unwrap_err();
        #[cfg(unix)]
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
        let details = io_error(&error);
        assert_eq!(details["raw_os_error"], json!(error.raw_os_error()));
        assert_ne!(
            details["message"],
            "domain transport ended after draining buffered frames"
        );
    }
}

#[cfg(test)]
mod process_reaping_tests {
    use super::*;

    // Hold a real worker join after kill/wait. This makes the previously racy
    // process.take -> terminate gap observable without sleeps or test-only hooks.
    fn check_cleanup(mode: &str) {
        let kernel = crate::AtelierKernel::new_in_memory().unwrap();
        let spec: DomainProcessSpec = serde_json::from_value(json!({
            "domain_id":"reaping", "command":"python3",
            "args":["-c", "import time; time.sleep(30)"],
            "auto_restart": mode == "automatic", "restart_delay_ms": 0
        }))
        .unwrap();
        let supervisor = kernel.supervisor().clone();
        let observer = kernel.attach_domain("observer");
        let pid = supervisor.spawn_process_domain(spec).unwrap();
        let control = supervisor.inner.control("reaping").unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        {
            let mut state = control.state.lock();
            let process = state.process.as_mut().unwrap();
            let stop = process.stop.clone();
            process.workers.push(thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    thread::yield_now();
                }
                let _ = entered_tx.send(());
                let _ = release_rx.recv();
            }));
        }
        let action = if mode == "stop" || mode == "restart" {
            let supervisor = supervisor.clone();
            let restart = mode == "restart";
            Some(thread::spawn(move || {
                if restart {
                    supervisor.respawn_domain("reaping").map(Some)
                } else {
                    assert!(supervisor.stop_process_domain("reaping"));
                    Ok(None)
                }
            }))
        } else {
            supervisor.inner.router.mark_domain_faulted_for_instance(
                "reaping",
                None,
                "controlled cleanup regression",
            );
            None
        };
        let entered = entered_rx.recv_timeout(Duration::from_secs(5));
        // A stuck query must fail this test rather than trapping it behind a held lifecycle lock.
        let query = supervisor.clone();
        let (answer_tx, answer_rx) = mpsc::channel();
        let reader = thread::spawn(move || {
            let pid = query.get_pid("reaping");
            let ids = observer.send(
                vec!["kernel.runtime".into()],
                "QueryRuntime",
                json!({}),
                json!({}),
            );
            let runtime_ok = observer.drain_frames().iter().any(|frame| {
                matches!(frame,
                KernelToDomainFrame::Receipt(receipt) if receipt.message_id == ids[0]
                    && receipt.status == crate::protocol::ContractStatus::Completed)
            });
            let _ = answer_tx.send((pid, runtime_ok));
        });
        let answer = answer_rx.recv_timeout(Duration::from_secs(1));
        let state_available = control.state.try_lock().is_some();
        release_tx.send(()).unwrap(); // Release before any assertion, even on the broken implementation.
        reader.join().unwrap();
        let new_pid = action.and_then(|a| a.join().unwrap().unwrap());
        assert!(
            entered.is_ok(),
            "cleanup did not reach the controlled worker"
        );
        assert_eq!(
            answer.unwrap(),
            (Some(pid), true),
            "PID disappeared before cleanup completed"
        );
        assert!(state_available, "cleanup held the public state lock");
        let deadline = Instant::now() + Duration::from_secs(5);
        while supervisor.get_pid("reaping") == Some(pid) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        let current = supervisor.get_pid("reaping");
        assert_ne!(current, Some(pid));
        if mode == "stop" || mode == "fault" {
            assert_eq!(current, None);
        }
        if let Some(new_pid) = new_pid {
            assert_eq!(current, Some(new_pid));
        }
        if mode == "automatic" {
            while supervisor.get_pid("reaping").is_none() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            assert!(supervisor.get_pid("reaping").is_some());
        }
        #[cfg(target_os = "linux")]
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    }
    #[test]
    fn stopped_pid_remains_visible_until_cleanup_completes() {
        check_cleanup("stop");
    }
    #[test]
    fn restarted_pid_remains_visible_until_cleanup_completes() {
        check_cleanup("restart");
    }
    #[test]
    fn faulted_pid_remains_visible_until_cleanup_completes() {
        check_cleanup("fault");
    }
    #[test]
    fn automatic_restart_waits_for_cleanup() {
        check_cleanup("automatic");
    }
}
