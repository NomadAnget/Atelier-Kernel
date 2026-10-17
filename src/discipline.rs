use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::ledger::{KERNEL_LEDGER_COLUMNS, KERNEL_LEDGER_TABLE};
use crate::storage::{DOMAIN_TABLE_COLUMNS, verify_sqlite_table_columns};
use crate::supervisor::{KernelBootConfig, SupervisorError};

/// Single machine-constitution violation detected by `check_constitution`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstitutionViolation {
    /// Constitutional rule code (`"M1"`, `"M4"`, `"M5"`, `"M6"`, `"M7"`).
    pub rule: String,
    /// Relative or absolute file/table location where the violation occurred.
    pub location: String,
    /// Human-readable diagnostic description.
    pub detail: String,
}

impl std::fmt::Display for ConstitutionViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}: {}", self.rule, self.location, self.detail)
    }
}

/// Complete report returned by `check_constitution`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConstitutionReport {
    pub target: String,
    pub checked_configs: usize,
    pub checked_files: usize,
    pub checked_tables: usize,
    pub violations: Vec<ConstitutionViolation>,
}

impl ConstitutionReport {
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }
}

/// Runs the Atelier Kernel Machine Constitution (M1–M7) static and pre-boot check
/// against a `kernel_boot.json` file, a variant root directory, or a domain directory/file.
pub fn check_constitution(target: impl AsRef<Path>) -> ConstitutionReport {
    let target_path = target.as_ref();
    let mut report = ConstitutionReport {
        target: target_path.display().to_string(),
        ..Default::default()
    };

    if !target_path.exists() {
        report.violations.push(ConstitutionViolation {
            rule: "M6".to_string(),
            location: target_path.display().to_string(),
            detail: "target path does not exist".to_string(),
        });
        return report;
    }

    let mut files_to_scan = BTreeSet::new();

    if target_path.is_file() {
        let is_json = target_path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.eq_ignore_ascii_case("json"))
            .unwrap_or(false);
        if is_json {
            check_boot_config_file(target_path, &mut report, &mut files_to_scan);
            if let Some(parent) = target_path.parent() {
                let domains_dir = parent.join("domains");
                if domains_dir.is_dir() {
                    collect_domain_source_files(&domains_dir, &mut files_to_scan);
                }
            }
        } else {
            files_to_scan.insert(target_path.to_path_buf());
        }
    } else if target_path.is_dir() {
        let boot_json = target_path.join("kernel_boot.json");
        if boot_json.is_file() {
            check_boot_config_file(&boot_json, &mut report, &mut files_to_scan);
            let domains_dir = target_path.join("domains");
            if domains_dir.is_dir() {
                collect_domain_source_files(&domains_dir, &mut files_to_scan);
            }
        } else {
            collect_domain_source_files(target_path, &mut files_to_scan);
        }
    }

    for file_path in files_to_scan {
        scan_domain_source_file(&file_path, &mut report);
    }

    report
}

fn check_boot_config_file(
    cfg_path: &Path,
    report: &mut ConstitutionReport,
    files_to_scan: &mut BTreeSet<PathBuf>,
) {
    report.checked_configs += 1;
    let loc = cfg_path.display().to_string();

    let cfg = match KernelBootConfig::load_from_file(cfg_path) {
        Ok(c) => c,
        Err(SupervisorError::ConfigValidationFailed { reason, .. }) => {
            let rule = if reason.starts_with("[M1]") {
                "M1"
            } else {
                "M6"
            };
            report.violations.push(ConstitutionViolation {
                rule: rule.to_string(),
                location: loc,
                detail: reason,
            });
            return;
        }
        Err(err) => {
            report.violations.push(ConstitutionViolation {
                rule: "M6".to_string(),
                location: loc,
                detail: err.to_string(),
            });
            return;
        }
    };

    let base_dir = cfg_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));

    for spec in &cfg.domains {
        let work_dir = spec
            .working_dir
            .as_deref()
            .map(Path::new)
            .unwrap_or(base_dir);
        if !work_dir.exists() {
            report.violations.push(ConstitutionViolation {
                rule: "M6".to_string(),
                location: format!("{} (domain '{}')", loc, spec.domain_id),
                detail: format!(
                    "working_dir '{}' does not exist",
                    work_dir.display()
                ),
            });
            continue;
        }

        for arg in &spec.args {
            if arg.starts_with('-') {
                continue;
            }
            let is_script = arg.ends_with(".py")
                || arg.ends_with(".js")
                || arg.ends_with(".mjs")
                || arg.ends_with(".cjs")
                || arg.ends_with(".sh");
            if is_script {
                let script_path = if Path::new(arg).is_absolute() {
                    PathBuf::from(arg)
                } else {
                    work_dir.join(arg)
                };
                if !script_path.is_file() {
                    report.violations.push(ConstitutionViolation {
                        rule: "M6".to_string(),
                        location: format!("{} (domain '{}')", loc, spec.domain_id),
                        detail: format!(
                            "entry script '{}' does not exist",
                            script_path.display()
                        ),
                    });
                } else {
                    files_to_scan.insert(script_path);
                }
            }
        }
    }

    // M7: If the SQLite database file already exists, verify that core tables and domain tables have no schema drift.
    let db_path = Path::new(&cfg.db_path);
    if db_path.is_file() {
        check_sqlite_schema_drift(db_path, &cfg, report);
    }
}

fn check_sqlite_schema_drift(
    db_path: &Path,
    cfg: &KernelBootConfig,
    report: &mut ConstitutionReport,
) {
    let Ok(conn) = Connection::open(db_path) else {
        return;
    };

    // Check `kernel_ledger` if it exists
    if table_exists(&conn, KERNEL_LEDGER_TABLE) {
        report.checked_tables += 1;
        if let Err(err) =
            verify_sqlite_table_columns(&conn, KERNEL_LEDGER_TABLE, KERNEL_LEDGER_COLUMNS)
        {
            report.violations.push(ConstitutionViolation {
                rule: "M7".to_string(),
                location: format!("{}::{}", db_path.display(), KERNEL_LEDGER_TABLE),
                detail: err.to_string(),
            });
        }
    }

    // Check `store_<domain>` tables if they exist
    for spec in &cfg.domains {
        let table = format!("store_{}", spec.domain_id);
        if table_exists(&conn, &table) {
            report.checked_tables += 1;
            if let Err(err) = verify_sqlite_table_columns(&conn, &table, DOMAIN_TABLE_COLUMNS) {
                report.violations.push(ConstitutionViolation {
                    rule: "M7".to_string(),
                    location: format!("{}::{}", db_path.display(), table),
                    detail: err.to_string(),
                });
            }
        }
    }
}

fn table_exists(conn: &Connection, table: &str) -> bool {
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
        rusqlite::params![table],
        |row| row.get::<_, i64>(0),
    )
    .map(|c| c > 0)
    .unwrap_or(false)
}

fn collect_domain_source_files(dir: &Path, out: &mut BTreeSet<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with('.')
            || matches!(
                name,
                "__pycache__" | "node_modules" | "target" | "dist" | "sdk" | "tests" | "tmp"
            )
        {
            continue;
        }
        if path.is_dir() {
            collect_domain_source_files(&path, out);
        } else if path.is_file() {
            if matches!(name, "atelier_sdk.py" | "atelier_sdk.js") {
                continue;
            }
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if matches!(ext, "py" | "js" | "mjs" | "cjs") {
                out.insert(path);
            }
        }
    }
}

fn scan_domain_source_file(path: &Path, report: &mut ConstitutionReport) {
    let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
        return;
    };
    if matches!(file_name, "atelier_sdk.py" | "atelier_sdk.js") {
        return;
    }
    let Ok(source) = fs::read_to_string(path) else {
        return;
    };
    report.checked_files += 1;

    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    match ext {
        "py" => scan_python_source(path, &source, report),
        "js" | "mjs" | "cjs" => scan_js_source(path, &source, report),
        _ => {}
    }
}

fn scan_python_source(path: &Path, source: &str, report: &mut ConstitutionReport) {
    let mut in_triple_double = false;
    let mut in_triple_single = false;

    for (idx, raw_line) in source.lines().enumerate() {
        let line_no = idx + 1;
        let location = format!("{}:{}", path.display(), line_no);
        let code = strip_python_line(raw_line, &mut in_triple_double, &mut in_triple_single);
        let trimmed = code.trim();
        if trimmed.is_empty() {
            continue;
        }

        // M4: Forbid cross-domain parent relative imports (`from ..<other>`)
        if trimmed.starts_with("from ..") && !trimmed.starts_with("from ..atelier_sdk") {
            report.violations.push(ConstitutionViolation {
                rule: "M4".to_string(),
                location: location.clone(),
                detail: format!(
                    "cross-domain relative import '{}' is forbidden; domains must communicate via kernel messages",
                    trimmed
                ),
            });
        }

        // M4: Forbid direct sqlite3 imports in business domains
        if trimmed == "import sqlite3"
            || trimmed.starts_with("import sqlite3 ")
            || trimmed.starts_with("from sqlite3 ")
        {
            report.violations.push(ConstitutionViolation {
                rule: "M4".to_string(),
                location: location.clone(),
                detail:
                    "direct sqlite3 access in domain code is forbidden; use kernel.storage messages"
                        .to_string(),
            });
        }

        // M5: Forbid direct manipulation of dedicated IPC pipe descriptors in business domain code
        if raw_line.contains("ATELIER_IPC_READ_FD") || raw_line.contains("ATELIER_IPC_WRITE_FD") {
            report.violations.push(ConstitutionViolation {
                rule: "M5".to_string(),
                location,
                detail: "direct manipulation of ATELIER_IPC_READ_FD / ATELIER_IPC_WRITE_FD in domain code is forbidden; use AtelierDomain SDK".to_string(),
            });
        }
    }
}

fn scan_js_source(path: &Path, source: &str, report: &mut ConstitutionReport) {
    let mut in_block_comment = false;

    for (idx, raw_line) in source.lines().enumerate() {
        let line_no = idx + 1;
        let location = format!("{}:{}", path.display(), line_no);
        let code = strip_js_line(raw_line, &mut in_block_comment);
        let trimmed = code.trim();
        if trimmed.is_empty() {
            continue;
        }

        // M4: Forbid cross-domain parent require/import (`../<other>`)
        if (trimmed.contains("require(\"../")
            || trimmed.contains("require('../")
            || trimmed.contains("from \"../")
            || trimmed.contains("from '../"))
            && !trimmed.contains("atelier_sdk")
            && !trimmed.contains("atelier-sdk")
        {
            report.violations.push(ConstitutionViolation {
                rule: "M4".to_string(),
                location: location.clone(),
                detail: "cross-domain parent import/require is forbidden; domains must communicate via kernel messages".to_string(),
            });
        }

        // M4: Forbid direct SQLite package imports in business domain code
        if trimmed.contains("require(\"sqlite3\")")
            || trimmed.contains("require('sqlite3')")
            || trimmed.contains("require(\"better-sqlite3\")")
            || trimmed.contains("require('better-sqlite3')")
        {
            report.violations.push(ConstitutionViolation {
                rule: "M4".to_string(),
                location: location.clone(),
                detail: "direct SQLite database access in domain code is forbidden; use kernel.storage messages".to_string(),
            });
        }

        // M5: Forbid direct manipulation of dedicated IPC pipe descriptors in business domain code
        if trimmed.contains("ATELIER_IPC_READ_FD") || trimmed.contains("ATELIER_IPC_WRITE_FD") {
            report.violations.push(ConstitutionViolation {
                rule: "M5".to_string(),
                location,
                detail: "direct manipulation of ATELIER_IPC_READ_FD / ATELIER_IPC_WRITE_FD in domain code is forbidden; use AtelierDomain SDK".to_string(),
            });
        }
    }
}

/// Strips Python `#` comments and triple-quoted docstrings (`"""` / `'''`) from a single line.
fn strip_python_line(
    line: &str,
    in_triple_double: &mut bool,
    in_triple_single: &mut bool,
) -> String {
    let mut out = String::new();
    let bytes = line.as_bytes();
    let mut i = 0;
    let mut in_single = false;
    let mut in_double = false;

    while i < bytes.len() {
        if *in_triple_double {
            if i + 3 <= bytes.len() && &bytes[i..i + 3] == b"\"\"\"" {
                *in_triple_double = false;
                i += 3;
            } else {
                i += 1;
            }
            continue;
        }
        if *in_triple_single {
            if i + 3 <= bytes.len() && &bytes[i..i + 3] == b"'''" {
                *in_triple_single = false;
                i += 3;
            } else {
                i += 1;
            }
            continue;
        }

        if !in_single && !in_double {
            if i + 3 <= bytes.len() && &bytes[i..i + 3] == b"\"\"\"" {
                *in_triple_double = true;
                i += 3;
                continue;
            }
            if i + 3 <= bytes.len() && &bytes[i..i + 3] == b"'''" {
                *in_triple_single = true;
                i += 3;
                continue;
            }
            if bytes[i] == b'#' {
                break;
            }
        }

        if bytes[i] == b'\\' && (in_single || in_double) {
            i += 2;
            continue;
        }
        if bytes[i] == b'\'' && !in_double {
            in_single = !in_single;
            out.push(' ');
            i += 1;
            continue;
        }
        if bytes[i] == b'"' && !in_single {
            in_double = !in_double;
            out.push(' ');
            i += 1;
            continue;
        }

        if !in_single && !in_double {
            out.push(bytes[i] as char);
        }
        i += 1;
    }

    out
}

/// Strips JS `//` comments and `/* ... */` block comments from a single line.
fn strip_js_line(line: &str, in_block_comment: &mut bool) -> String {
    let mut out = String::new();
    let bytes = line.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if *in_block_comment {
            if i + 2 <= bytes.len() && &bytes[i..i + 2] == b"*/" {
                *in_block_comment = false;
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }
        if i + 2 <= bytes.len() && &bytes[i..i + 2] == b"/*" {
            *in_block_comment = true;
            i += 2;
            continue;
        }
        if i + 2 <= bytes.len() && &bytes[i..i + 2] == b"//" {
            break;
        }
        out.push(bytes[i] as char);
        i += 1;
    }

    out
}
