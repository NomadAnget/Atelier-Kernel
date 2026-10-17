use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use crate::logging::{LogRecord, now_secs_f64};
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
            let id = spec.domain_id.trim();
            if id.is_empty()
                || !id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return Err(SupervisorError::ConfigValidationFailed {
                    path: path_str.to_string(),
                    reason: format!(
                        "[M1] invalid domain_id '{}': must be non-empty ASCII alphanumeric or '_'",
                        spec.domain_id
                    ),
                });
            }
            if !seen.insert(id.to_string()) {
                return Err(SupervisorError::ConfigValidationFailed {
                    path: path_str.to_string(),
                    reason: format!("[M1/M6] duplicate domain_id '{id}' in domains list"),
                });
            }
            if spec.command.trim().is_empty() {
                return Err(SupervisorError::ConfigValidationFailed {
                    path: path_str.to_string(),
                    reason: format!("[M6] domain '{id}' has an empty command"),
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
            working_dir: None,
            auto_restart: default_auto_restart(),
            max_restarts: default_max_restarts(),
            restart_delay_ms: default_restart_delay_ms(),
        }
    }
}

impl DomainProcessSpec {
    pub fn new(
        domain_id: impl Into<String>,
        command: impl Into<String>,
        args: Vec<String>,
    ) -> Self {
        Self {
            domain_id: domain_id.into(),
            command: command.into(),
            args,
            working_dir: None,
            auto_restart: default_auto_restart(),
            max_restarts: default_max_restarts(),
            restart_delay_ms: default_restart_delay_ms(),
        }
    }
}

#[derive(Clone)]
struct ManagedProcess {
    instance_id: u64,
    child: Arc<Mutex<Child>>,
    stopping: Arc<AtomicBool>,
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

#[derive(Debug, Clone)]
struct RestartTracker {
    consecutive_restarts: u32,
    last_spawn_instant: Option<Instant>,
}

struct RestartRequest {
    domain_id: String,
    failed_instance_id: u64,
    exit_status: Option<String>,
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
    fn SetHandleInformation(
        hObject: *mut std::ffi::c_void,
        dwMask: u32,
        dwFlags: u32,
    ) -> i32;
}

#[cfg(windows)]
const HANDLE_FLAG_INHERIT: u32 = 0x0000_0001;

struct DomainSupervisorInner {
    router: Arc<KernelRouter>,
    specs: RwLock<HashMap<String, DomainProcessSpec>>,
    processes: Mutex<HashMap<String, ManagedProcess>>,
    stopped_domains: Mutex<HashSet<String>>,
    restart_trackers: Mutex<HashMap<String, RestartTracker>>,
    spawn_lock: Mutex<()>,
}

impl DomainSupervisorInner {
    fn stop_process_internal(&self, domain_id: &str) -> bool {
        let managed_opt = self.processes.lock().remove(domain_id);
        if let Some(managed) = managed_opt {
            managed.stopping.store(true, Ordering::SeqCst);
            {
                let mut child = managed.child.lock();
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

    fn spawn_child_pipes(
        &self,
        domain_id: &str,
        spec: &DomainProcessSpec,
    ) -> Result<(Child, std::io::PipeWriter, std::io::PipeReader), SupervisorError> {
        let _spawn_guard = self.spawn_lock.lock();

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
            clear_cloexec(r).map_err(|source| SupervisorError::SpawnFailed {
                domain: domain_id.to_string(),
                source,
            })?;
            clear_cloexec(w).map_err(|source| SupervisorError::SpawnFailed {
                domain: domain_id.to_string(),
                source,
            })?;
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
                    || SetHandleInformation(
                        w_h as *mut _,
                        HANDLE_FLAG_INHERIT,
                        HANDLE_FLAG_INHERIT,
                    ) == 0
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

    fn spawn_process_internal(
        &self,
        spec: DomainProcessSpec,
        restart_tx: Sender<RestartRequest>,
    ) -> Result<u32, SupervisorError> {
        let domain_id = spec.domain_id.clone();
        self.specs.write().insert(domain_id.clone(), spec.clone());

        // Stop any existing process for this domain first.
        let _ = self.stop_process_internal(&domain_id);

        let (mut child, k2d_tx, d2k_rx) = self.spawn_child_pipes(&domain_id, &spec)?;

        let pid = child.id();
        let child_stdout = child.stdout.take();
        let child_stderr = child.stderr.take();

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

        {
            let mut trackers = self.restart_trackers.lock();
            let tracker = trackers
                .entry(domain_id.clone())
                .or_insert_with(|| RestartTracker {
                    consecutive_restarts: 0,
                    last_spawn_instant: None,
                });
            tracker.last_spawn_instant = Some(Instant::now());
        }

        // Writer thread: Kernel -> Domain dedicated pipe A (k2d_tx, JSON-Lines)
        let writer_domain = domain_id.clone();
        thread::Builder::new()
            .name(format!("kernel-pipe-tx-{writer_domain}-{instance_id}"))
            .spawn(move || {
                let mut pipe_tx = k2d_tx;
                while let Ok(frame) = rx.recv() {
                    if let Ok(line) = serde_json::to_string(&frame) {
                        if writeln!(pipe_tx, "{line}").is_err() || pipe_tx.flush().is_err() {
                            break;
                        }
                    }
                }
            })
            .ok();

        // Reader thread: Domain -> Kernel dedicated pipe B (d2k_rx, physically bound to `(domain_id, instance_id)`)
        let reader_domain = domain_id.clone();
        let router_ref = Arc::clone(&self.router);
        let stopping_ref = Arc::clone(&stopping);
        let child_for_reader = Arc::clone(&child_arc);
        let restart_tx_for_reader = restart_tx;
        thread::Builder::new()
            .name(format!("kernel-pipe-rx-{reader_domain}-{instance_id}"))
            .spawn(move || {
                let reader = BufReader::new(d2k_rx);
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
                // When dedicated IPC pipe EOF is reached, if not intentionally stopped by kernel,
                // mark domain as Faulted ONLY IF `instance_id` is still the current generation.
                if !stopping_ref.load(Ordering::SeqCst) {
                    router_ref.mark_domain_faulted_for_instance(
                        &reader_domain,
                        Some(instance_id),
                        "domain process exited or closed IPC pipe",
                    );

                    // Reap child process to get exit status and prevent zombie process
                    let exit_status = {
                        let mut lock = child_for_reader.lock();
                        lock.wait().ok().map(|s| s.to_string())
                    };

                    let _ = restart_tx_for_reader.send(RestartRequest {
                        domain_id: reader_domain,
                        failed_instance_id: instance_id,
                        exit_status,
                    });
                }
            })
            .ok();

        // Diagnostic Stdout text pump: captures third-party print() / C++ stdout output without polluting the IPC pipe
        if let Some(child_stdout) = child_stdout {
            let stdout_domain = domain_id.clone();
            let logger_ref = Arc::clone(self.router.logger());
            let stopping_ref = Arc::clone(&stopping);
            thread::Builder::new()
                .name(format!("kernel-pipe-out-{stdout_domain}-{instance_id}"))
                .spawn(move || {
                    let reader = BufReader::new(child_stdout);
                    for line in reader.lines().flatten() {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        if stopping_ref.load(Ordering::SeqCst) {
                            break;
                        }
                        logger_ref.reinject(LogRecord {
                            source_domain: Some(stdout_domain.clone()),
                            id: None,
                            ts: now_secs_f64(),
                            level: "INFO".to_string(),
                            layer: "SUBPROC".to_string(),
                            component: stdout_domain.clone(),
                            message: trimmed.to_string(),
                            phase: String::new(),
                            console_only: false,
                            ephemeral: false,
                            key: None,
                            frac: None,
                            waiting: false,
                            done: false,
                        });
                    }
                })
                .ok();
        }

        // Diagnostic Stderr text pump: captures unhandled stderr / warnings / tracebacks and routes into unified logger
        if let Some(child_stderr) = child_stderr {
            let stderr_domain = domain_id;
            let logger_ref = Arc::clone(self.router.logger());
            let stopping_ref = Arc::clone(&stopping);
            thread::Builder::new()
                .name(format!("kernel-pipe-err-{stderr_domain}-{instance_id}"))
                .spawn(move || {
                    let reader = BufReader::new(child_stderr);
                    for line in reader.lines().flatten() {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        if stopping_ref.load(Ordering::SeqCst) {
                            break;
                        }
                        logger_ref.reinject(LogRecord {
                            source_domain: Some(stderr_domain.clone()),
                            id: None,
                            ts: now_secs_f64(),
                            level: "WARN".to_string(),
                            layer: "SUBPROC".to_string(),
                            component: stderr_domain.clone(),
                            message: trimmed.to_string(),
                            phase: String::new(),
                            console_only: false,
                            ephemeral: false,
                            key: None,
                            frac: None,
                            waiting: false,
                            done: false,
                        });
                    }
                })
                .ok();
        }

        Ok(pid)
    }

    fn handle_restart_request(
        self: &Arc<Self>,
        req: RestartRequest,
        restart_tx: &Sender<RestartRequest>,
    ) {
        // 1. Is this domain intentionally stopped?
        if self.stopped_domains.lock().contains(&req.domain_id) {
            return;
        }

        // 2. Check current process instance
        let current_instance = {
            let procs = self.processes.lock();
            procs
                .get(&req.domain_id)
                .map(|p| (p.instance_id, p.stopping.load(Ordering::SeqCst)))
        };

        if let Some((inst_id, stopping)) = current_instance {
            if stopping || inst_id != req.failed_instance_id {
                // Either intentionally stopping or already replaced by a newer instance.
                return;
            }
        }

        // 3. Look up spec
        let spec = match self.specs.read().get(&req.domain_id).cloned() {
            Some(s) => s,
            None => return,
        };

        if !spec.auto_restart {
            self.router.logger().reinject(LogRecord {
                source_domain: Some("kernel".to_string()),
                id: None,
                ts: now_secs_f64(),
                level: "INFO".to_string(),
                layer: "SUPERVISOR".to_string(),
                component: req.domain_id.clone(),
                message: format!(
                    "Domain '{}' exited unexpectedly (instance_id={}), auto-restart disabled by configuration.",
                    req.domain_id, req.failed_instance_id
                ),
                phase: String::new(),
                console_only: false,
                ephemeral: false,
                key: None,
                frac: None,
                waiting: false,
                done: false,
            });
            return;
        }

        // 4. Rate-limiting & max restarts check
        let now = Instant::now();
        let (attempt, allow_restart) = {
            let mut trackers = self.restart_trackers.lock();
            let tracker = trackers
                .entry(req.domain_id.clone())
                .or_insert_with(|| RestartTracker {
                    consecutive_restarts: 0,
                    last_spawn_instant: None,
                });

            // If the process had been running stably for > 5 seconds before crashing,
            // reset consecutive restarts count.
            if let Some(spawned_at) = tracker.last_spawn_instant {
                if now.duration_since(spawned_at) > Duration::from_secs(5) {
                    tracker.consecutive_restarts = 0;
                }
            }

            tracker.consecutive_restarts += 1;
            let attempt = tracker.consecutive_restarts;

            let allowed = match spec.max_restarts {
                Some(max) => attempt <= max,
                None => true,
            };
            (attempt, allowed)
        };

        if !allow_restart {
            self.router.logger().reinject(LogRecord {
                source_domain: Some("kernel".to_string()),
                id: None,
                ts: now_secs_f64(),
                level: "ERROR".to_string(),
                layer: "SUPERVISOR".to_string(),
                component: req.domain_id.clone(),
                message: format!(
                    "Domain '{}' exceeded maximum restart attempts ({:?}). Giving up automatic restart.",
                    req.domain_id, spec.max_restarts
                ),
                phase: String::new(),
                console_only: false,
                ephemeral: false,
                key: None,
                frac: None,
                waiting: false,
                done: false,
            });
            return;
        }

        let delay_ms = spec.restart_delay_ms;
        let exit_desc = req.exit_status.unwrap_or_else(|| "unknown".to_string());
        self.router.logger().reinject(LogRecord {
            source_domain: Some("kernel".to_string()),
            id: None,
            ts: now_secs_f64(),
            level: "WARN".to_string(),
            layer: "SUPERVISOR".to_string(),
            component: req.domain_id.clone(),
            message: format!(
                "Domain '{}' exited unexpectedly (status: {}, instance_id={}). Auto-restarting in {}ms (attempt {})...",
                req.domain_id,
                exit_desc,
                req.failed_instance_id,
                delay_ms,
                attempt
            ),
            phase: String::new(),
            console_only: false,
            ephemeral: false,
            key: None,
            frac: None,
            waiting: false,
            done: false,
        });

        // Spawn a background restart thread so this delay does not block other domains
        let inner_clone = Arc::clone(self);
        let domain_id = req.domain_id;
        let failed_instance_id = req.failed_instance_id;
        let restart_tx_for_thread = restart_tx.clone();
        thread::Builder::new()
            .name(format!("kernel-supervisor-restart-{domain_id}"))
            .spawn(move || {
                if delay_ms > 0 {
                    thread::sleep(Duration::from_millis(delay_ms));
                }
                if inner_clone.stopped_domains.lock().contains(&domain_id) {
                    return;
                }
                let should_spawn = {
                    let procs = inner_clone.processes.lock();
                    match procs.get(&domain_id) {
                        Some(p) => {
                            !p.stopping.load(Ordering::SeqCst)
                                && p.instance_id == failed_instance_id
                        }
                        None => true,
                    }
                };
                if should_spawn {
                    match inner_clone.spawn_process_internal(spec, restart_tx_for_thread) {
                        Ok(new_pid) => {
                            inner_clone.router.logger().reinject(LogRecord {
                                source_domain: Some("kernel".to_string()),
                                id: None,
                                ts: now_secs_f64(),
                                level: "INFO".to_string(),
                                layer: "SUPERVISOR".to_string(),
                                component: domain_id.clone(),
                                message: format!(
                                    "Domain '{}' automatically restarted successfully (pid={})",
                                    domain_id, new_pid
                                ),
                                phase: String::new(),
                                console_only: false,
                                ephemeral: false,
                                key: None,
                                frac: None,
                                waiting: false,
                                done: false,
                            });
                        }
                        Err(err) => {
                            inner_clone.router.logger().reinject(LogRecord {
                                source_domain: Some("kernel".to_string()),
                                id: None,
                                ts: now_secs_f64(),
                                level: "ERROR".to_string(),
                                layer: "SUPERVISOR".to_string(),
                                component: domain_id.clone(),
                                message: format!(
                                    "Domain '{}' auto-restart spawn failed: {}",
                                    domain_id, err
                                ),
                                phase: String::new(),
                                console_only: false,
                                ephemeral: false,
                                key: None,
                                frac: None,
                                waiting: false,
                                done: false,
                            });
                        }
                    }
                }
            })
            .ok();
    }
}

/// Kernel Domain Supervisor: takes over starting and managing the lifecycle of
/// language-agnostic Domains via OS parent-child anonymous `Stdio` pipes.
pub struct DomainSupervisor {
    inner: Arc<DomainSupervisorInner>,
    restart_tx: Sender<RestartRequest>,
}

impl DomainSupervisor {
    pub fn new(router: Arc<KernelRouter>) -> Self {
        let inner = Arc::new(DomainSupervisorInner {
            router,
            specs: RwLock::new(HashMap::new()),
            processes: Mutex::new(HashMap::new()),
            stopped_domains: Mutex::new(HashSet::new()),
            restart_trackers: Mutex::new(HashMap::new()),
            spawn_lock: Mutex::new(()),
        });
        let (restart_tx, restart_rx) = mpsc::channel::<RestartRequest>();

        let monitor_inner = Arc::clone(&inner);
        let monitor_tx = restart_tx.clone();
        thread::Builder::new()
            .name("kernel-supervisor-monitor".to_string())
            .spawn(move || {
                while let Ok(req) = restart_rx.recv() {
                    monitor_inner.handle_restart_request(req, &monitor_tx);
                }
            })
            .ok();

        Self { inner, restart_tx }
    }

    pub fn router(&self) -> &Arc<KernelRouter> {
        &self.inner.router
    }

    /// Returns true if `domain_id` was explicitly stopped by kernel supervisor.
    pub fn is_stopped(&self, domain_id: &str) -> bool {
        self.inner.stopped_domains.lock().contains(domain_id)
    }

    /// Returns the consecutive restart attempt count for `domain_id`.
    pub fn restart_count(&self, domain_id: &str) -> u32 {
        self.inner
            .restart_trackers
            .lock()
            .get(domain_id)
            .map(|t| t.consecutive_restarts)
            .unwrap_or(0)
    }

    /// Boots all domains declared in `config.domains`.
    pub fn boot_from_config(
        &self,
        config: &KernelBootConfig,
    ) -> Vec<(String, Result<u32, SupervisorError>)> {
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
            .inner
            .specs
            .read()
            .get(domain_id)
            .cloned()
            .ok_or_else(|| SupervisorError::UnknownDomainSpec(domain_id.to_string()))?;
        self.inner.stopped_domains.lock().remove(domain_id);
        self.inner.restart_trackers.lock().remove(domain_id);
        self.spawn_process_domain(spec)
    }

    /// Returns the registered `DomainProcessSpec` for `domain_id`, if it is an external process domain.
    pub fn get_spec(&self, domain_id: &str) -> Option<DomainProcessSpec> {
        self.inner.specs.read().get(domain_id).cloned()
    }

    /// Returns all registered external `DomainProcessSpec`s.
    pub fn list_specs(&self) -> Vec<DomainProcessSpec> {
        let mut list: Vec<DomainProcessSpec> = self.inner.specs.read().values().cloned().collect();
        list.sort_by(|a, b| a.domain_id.cmp(&b.domain_id));
        list
    }

    /// Spawns a language-agnostic child process for `spec.domain_id` and binds its
    /// `stdin` / `stdout` pipes directly to `spec.domain_id` in the Kernel.
    pub fn spawn_process_domain(&self, spec: DomainProcessSpec) -> Result<u32, SupervisorError> {
        self.inner.stopped_domains.lock().remove(&spec.domain_id);
        self.inner.restart_trackers.lock().remove(&spec.domain_id);
        self.inner
            .spawn_process_internal(spec, self.restart_tx.clone())
    }

    /// Stops a managed child process domain cleanly and revokes its routes.
    pub fn stop_process_domain(&self, domain_id: &str) -> bool {
        self.inner
            .stopped_domains
            .lock()
            .insert(domain_id.to_string());
        self.inner.stop_process_internal(domain_id)
    }

    /// Attaches an in-process domain pipe (useful for embedded Rust domains and fast integration tests).
    /// Just like OS child processes, the returned `DomainPipeClient` has its identity physically bound
    /// to `domain_id` by the Kernel.
    pub fn attach_in_process_domain(&self, domain_id: &str) -> DomainPipeClient {
        let (tx, rx) = mpsc::channel::<KernelToDomainFrame>();
        let instance_id = self.inner.router.attach_domain_pipe(domain_id, tx);
        DomainPipeClient {
            domain_id: domain_id.to_string(),
            instance_id,
            router: Arc::clone(&self.inner.router),
            inbox: Mutex::new(rx),
            next_seq: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Returns the OS process ID for a managed domain, if currently running.
    pub fn get_pid(&self, domain_id: &str) -> Option<u32> {
        self.inner
            .processes
            .lock()
            .get(domain_id)
            .map(|p| p.child.lock().id())
    }

    /// Performs a zero-downtime rolling update with validation and automatic rollback.
    pub fn update_process_domain(
        &self,
        domain_id: &str,
        new_spec: Option<DomainProcessSpec>,
        _probe_handler: Option<&str>,
        timeout: Duration,
    ) -> Result<DomainUpdateResult, SupervisorError> {
        let spec = match new_spec {
            Some(s) => {
                self.inner.specs.write().insert(domain_id.to_string(), s.clone());
                s
            }
            None => self
                .get_spec(domain_id)
                .ok_or_else(|| SupervisorError::UnknownDomainSpec(domain_id.to_string()))?,
        };

        let old_managed = self.inner.processes.lock().get(domain_id).cloned();
        let old_pid = old_managed.as_ref().map(|m| m.child.lock().id());
        let old_instance_id = old_managed.as_ref().map(|m| m.instance_id);

        if old_managed.is_none() {
            let pid = self.spawn_process_domain(spec)?;
            let inst_id = self.inner.router.domain_instance_id(domain_id).unwrap_or(0);
            return Ok(DomainUpdateResult {
                domain_id: domain_id.to_string(),
                new_pid: pid,
                old_pid: None,
                instance_id: inst_id,
                status: "started".to_string(),
            });
        }

        // Spawn candidate child process
        let (child, k2d_tx, d2k_rx) = self.inner.spawn_child_pipes(domain_id, &spec)?;
        let candidate_pid = child.id();

        let (cand_tx, cand_rx) = mpsc::channel::<KernelToDomainFrame>();
        let candidate_instance_id = self.inner.router.attach_candidate_pipe(domain_id, cand_tx);

        let stopping = Arc::new(AtomicBool::new(false));
        let child_arc = Arc::new(Mutex::new(child));

        // Candidate writer thread
        let writer_domain = domain_id.to_string();
        thread::Builder::new()
            .name(format!("kernel-cand-tx-{writer_domain}-{candidate_instance_id}"))
            .spawn(move || {
                let mut pipe_tx = k2d_tx;
                while let Ok(frame) = cand_rx.recv() {
                    if let Ok(line) = serde_json::to_string(&frame) {
                        if writeln!(pipe_tx, "{line}").is_err() || pipe_tx.flush().is_err() {
                            break;
                        }
                    }
                }
            })
            .ok();

        // Candidate reader thread
        let reader_domain = domain_id.to_string();
        let router_ref = Arc::clone(&self.inner.router);
        let stopping_ref = Arc::clone(&stopping);
        let child_for_reader = Arc::clone(&child_arc);
        thread::Builder::new()
            .name(format!("kernel-cand-rx-{reader_domain}-{candidate_instance_id}"))
            .spawn(move || {
                let reader = BufReader::new(d2k_rx);
                for line in reader.lines() {
                    match line {
                        Ok(raw_line) => {
                            router_ref.handle_raw_pipe_line_for_instance(
                                &reader_domain,
                                candidate_instance_id,
                                &raw_line,
                            );
                        }
                        Err(_) => break,
                    }
                }
                if !stopping_ref.load(Ordering::SeqCst) {
                    let mut lock = child_for_reader.lock();
                    let _ = lock.wait();
                }
            })
            .ok();

        // Validation loop: probe readiness and health
        let start_time = Instant::now();
        let mut validated = false;
        while start_time.elapsed() < timeout {
            // Check if candidate process terminated prematurely
            {
                let mut lock = child_arc.lock();
                if let Ok(Some(_)) = lock.try_wait() {
                    break;
                }
            }
            if self.inner.router.is_candidate_ready(domain_id, candidate_instance_id) {
                validated = true;
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }

        if !validated {
            // Rollback! Terminate candidate, keep old instance serving
            stopping.store(true, Ordering::SeqCst);
            {
                let mut lock = child_arc.lock();
                let _ = lock.kill();
                let _ = lock.wait();
            }
            self.inner.router.abort_candidate(domain_id, candidate_instance_id);
            return Err(SupervisorError::UpdateValidationFailed {
                domain: domain_id.to_string(),
                reason: "Candidate instance failed readiness/probe check within timeout".to_string(),
            });
        }

        // Validation succeeded! Atomically switch routes in router
        self.inner.router.commit_candidate(domain_id, candidate_instance_id);

        let new_managed = ManagedProcess {
            instance_id: candidate_instance_id,
            child: Arc::clone(&child_arc),
            stopping: Arc::clone(&stopping),
        };
        self.inner.processes.lock().insert(domain_id.to_string(), new_managed);

        // Reset restart trackers
        {
            let mut trackers = self.inner.restart_trackers.lock();
            let tracker = trackers
                .entry(domain_id.to_string())
                .or_insert_with(|| RestartTracker {
                    consecutive_restarts: 0,
                    last_spawn_instant: None,
                });
            tracker.last_spawn_instant = Some(Instant::now());
        }

        // Gracefully drain and terminate old process in background thread
        if let Some(old_p) = old_managed {
            let domain_drain = domain_id.to_string();
            let old_inst = old_instance_id.unwrap_or(0);
            let router_drain = Arc::clone(&self.inner.router);
            thread::Builder::new()
                .name(format!("kernel-drain-{domain_drain}-{old_inst}"))
                .spawn(move || {
                    thread::sleep(Duration::from_millis(600));
                    old_p.stopping.store(true, Ordering::SeqCst);
                    {
                        let mut lock = old_p.child.lock();
                        let _ = lock.kill();
                        let _ = lock.wait();
                    }
                    router_drain.cleanup_draining_instance(&domain_drain, old_inst);
                })
                .ok();
        }

        Ok(DomainUpdateResult {
            domain_id: domain_id.to_string(),
            new_pid: candidate_pid,
            old_pid,
            instance_id: candidate_instance_id,
            status: "updated".to_string(),
        })
    }
}

impl Drop for DomainSupervisor {
    fn drop(&mut self) {
        let domain_ids: Vec<String> = self.inner.processes.lock().keys().cloned().collect();
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
