use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::services::ports::PortBinding;

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
    /// C4 项目根：所有域文件集中到 `<project_root>/<domain>/`；默认 `./data`。
    #[serde(default = "default_project_root")]
    pub project_root: String,
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

fn default_project_root() -> String {
    "./data".to_string()
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
            let root_p = std::path::Path::new(&cfg.project_root);
            if root_p.is_relative() {
                cfg.project_root = base_dir.join(root_p).to_string_lossy().to_string();
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
    pub(crate) fn validate(&self) -> Result<(), String> {
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
