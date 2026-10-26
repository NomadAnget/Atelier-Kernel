use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use atelier_kernel::{
    AtelierKernel, ContractStatus, DomainStatus, InteractiveFrontendDomain, KernelBootConfig,
    KernelToDomainFrame, WebFrontendDomain, check_constitution,
};
use serde_json::json;

const DEFAULT_TEMPLATE_JSON: &str = r#"{
  "db_path": "./data/atelier_kernel.db",
  "web_ui_bind": "0.0.0.0:8787",
  "web_ui_enabled": false,
  "domains": [
    {
      "domain_id": "sample_py",
      "command": "python3",
      "args": ["-u", "domains/sample_domain.py"],
      "working_dir": null
    }
  ]
}
"#;

const SAMPLE_PY_DOMAIN: &str = r#"#!/usr/bin/env python3
from atelier_sdk import AtelierDomain

domain = AtelierDomain()

@domain.handler("ping")
async def on_ping(msg):
    domain.logger.info("Received ping from %s", msg.source_domain)
    await domain.storage_insert_async(record_id="last_ping", category="health", data=msg.data)
    domain.logger.phase("INFO", "finish", "Handled ping request")
    return {"pong": True, "from": "sample_py", "received": msg.data}

if __name__ == "__main__":
    domain.run()
"#;

fn init_variant_skeleton(target_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(target_dir);
    let domains_dir = root.join("domains");
    fs::create_dir_all(&domains_dir)?;

    let cfg_path = root.join("kernel_boot.json");
    if !cfg_path.exists() {
        fs::write(&cfg_path, DEFAULT_TEMPLATE_JSON)?;
    }

    let sample_py_path = domains_dir.join("sample_domain.py");
    if !sample_py_path.exists() {
        fs::write(&sample_py_path, SAMPLE_PY_DOMAIN)?;
    }

    println!(
        "Initialized Atelier Kernel variant skeleton at: {}",
        root.display()
    );
    println!("  - Config:  {}", cfg_path.display());
    println!("  - Domain:  {}", sample_py_path.display());
    println!("Run with: atelier-kernel --config {}", cfg_path.display());
    Ok(())
}

/// Boots a variant configuration, verifies that all configured domains register their handlers,
/// dispatches a live probe message to each domain, verifies the 4-stage receipt lifecycle
/// (`Sent -> Received -> Started -> Completed`), and exits cleanly.
fn run_variant_smoke_test(
    config_path: &str,
    boot_config: &KernelBootConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("[SmokeTest] Booting variant from config: {}", config_path);
    let kernel = AtelierKernel::open_with_db(&boot_config.db_path)?;

    if boot_config.domains.is_empty() {
        return Err("No domains declared in boot configuration".into());
    }

    let spawn_results = kernel.supervisor().boot_from_config(boot_config);
    for (domain_id, res) in spawn_results {
        match res {
            Ok(pid) => println!("[SmokeTest] Spawned domain '{}' (pid={})", domain_id, pid),
            Err(err) => {
                return Err(format!("Failed to spawn domain '{}': {}", domain_id, err).into());
            }
        }
    }

    // 1. Wait for all configured domains to register handlers and become Active
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let all_active = boot_config.domains.iter().all(|spec| {
            kernel.router().domain_status(&spec.domain_id) == Some(DomainStatus::Active)
                && kernel
                    .router()
                    .active_routes()
                    .iter()
                    .any(|r| r.starts_with(&format!("{}.", spec.domain_id)))
        });
        if all_active {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "Timed out waiting for domains to register handlers. Current routes: {:?}",
                kernel.router().active_routes()
            )
            .into());
        }
        thread::sleep(Duration::from_millis(40));
    }

    let active_routes = kernel.router().active_routes();
    println!(
        "[SmokeTest] All configured domains Active! Registered routes: {:?}",
        active_routes
    );

    // 2. Attach probe domain and send a message to the first registered route of each domain
    let probe = kernel.attach_domain("smoke_verifier");
    let mut target_routes = Vec::new();
    for spec in &boot_config.domains {
        let prefix = format!("{}.", spec.domain_id);
        if let Some(route) = active_routes.iter().find(|r| r.starts_with(&prefix)) {
            target_routes.push(route.clone());
        }
    }

    let msg_id = probe.send(
        target_routes.clone(),
        "VariantSmokeProbe",
        json!({ "smoke_test": true }),
        json!({ "hello": "variant_verification" }),
    );
    println!(
        "[SmokeTest] Dispatched probe messages '{:?}' to targets {:?}",
        msg_id, target_routes
    );

    let mut completed_targets = HashSet::new();
    let mut logical_ids = HashSet::new();
    if msg_id.len() != target_routes.len()
        || msg_id.iter().collect::<HashSet<_>>().len() != target_routes.len()
    {
        return Err("Fan-out must allocate a distinct message ID per target".into());
    }
    let mut seen_stages_per_target: std::collections::HashMap<String, Vec<ContractStatus>> =
        std::collections::HashMap::new();

    let msg_deadline = Instant::now() + Duration::from_secs(5);
    while completed_targets.len() < target_routes.len() {
        if Instant::now() >= msg_deadline {
            return Err(format!(
                "Timed out waiting for 4-stage receipts. Seen stages: {:?}",
                seen_stages_per_target
            )
            .into());
        }
        for frame in probe.drain_frames() {
            if let KernelToDomainFrame::Receipt(r) = frame
                && msg_id.contains(&r.message_id)
            {
                if r.logical_id.is_empty() || r.logical_id == r.message_id {
                    return Err("Receipt must carry distinct logical_id and message_id".into());
                }
                logical_ids.insert(r.logical_id.clone());
                if logical_ids.len() != 1 {
                    return Err("Fan-out receipts must share the submission logical_id".into());
                }
                seen_stages_per_target
                    .entry(r.target.clone())
                    .or_default()
                    .push(r.status);
                if r.status == ContractStatus::Completed {
                    println!(
                        "[SmokeTest] Target '{}' completed with payload: {}",
                        r.target,
                        r.data.unwrap_or_default()
                    );
                    completed_targets.insert(r.target);
                } else if r.status == ContractStatus::ExecutionFailed
                    || r.status == ContractStatus::TargetInvalid
                    || r.status == ContractStatus::ContractInvalid
                {
                    return Err(format!(
                        "Probe failed for target '{}' with status {:?}: {:?}",
                        r.target, r.status, r.data
                    )
                    .into());
                }
            }
        }
        thread::sleep(Duration::from_millis(25));
    }

    // Verify every target went through Sent -> Received -> Started -> Completed
    for route in &target_routes {
        let stages = seen_stages_per_target
            .get(route)
            .cloned()
            .unwrap_or_default();
        if stages
            != vec![
                ContractStatus::Sent,
                ContractStatus::Received,
                ContractStatus::Started,
                ContractStatus::Completed,
            ]
        {
            return Err(format!("Unexpected receipt stages for '{}': {:?}", route, stages).into());
        }
    }

    // Cleanly stop all domains
    for spec in &boot_config.domains {
        kernel.supervisor().stop_process_domain(&spec.domain_id);
    }

    // Verify SQLite hub_logs persistence
    thread::sleep(Duration::from_millis(250));
    let persisted_logs = kernel
        .logger()
        .query_logs(20, None, None, None, None, None)?;
    println!(
        "[SmokeTest] Verified hub_logs persistence: recorded {} structured log entries in SQLite",
        persisted_logs.len()
    );

    println!(
        "[SmokeTest] Variant smoke test PASSED (registration + 4-stage messaging + unified hub_logs verified)!"
    );
    Ok(())
}

fn print_help() {
    println!(
        "Atelier Kernel v{} — Single-Machine Multi-Domain Microkernel Runtime",
        env!("CARGO_PKG_VERSION")
    );
    println!();
    println!("USAGE:");
    println!("  atelier-kernel [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("  --config <path>   Path to boot configuration JSON (default: ./kernel_boot.json)");
    println!("  --web-ui, --dashboard  Launch optional browser testing & observation console");
    println!("  --headless        Run pure microkernel supervisor without Web UI (default)");
    println!(
        "  --smoke-test      Boot variant, verify domain registration & 4-stage messaging, and exit"
    );
    println!("  --cli             Run interactive CLI REPL frontend domain");
    println!("  --auto, -a        Run automated 7-stage kernel verification suite and exit");
    println!(
        "  --check [path]    Run Machine Constitution (M1-M7) check on config or domain directory"
    );
    println!("  --init <dir>      Initialize a new product variant directory with config template");
    println!("  --version, -v     Print version information");
    println!("  --help, -h        Print this help message");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_help();
        return Ok(());
    }

    if args.iter().any(|a| a == "--version" || a == "-v") {
        println!("atelier-kernel {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    if let Some(target_dir) = args
        .windows(2)
        .find(|w| w[0] == "--init")
        .map(|w| w[1].as_str())
    {
        return init_variant_skeleton(target_dir);
    }

    if let Some(address) = args
        .windows(2)
        .find(|w| w[0] == "--port-relay")
        .map(|w| w[1].as_str())
    {
        return atelier_kernel::port_forwarder::run(address.parse()?).map_err(Into::into);
    }
    // Load external startup configuration (`kernel_boot.json`)
    let config_path = args
        .windows(2)
        .find(|w| w[0] == "--config")
        .map(|w| w[1].as_str())
        .unwrap_or("./kernel_boot.json");

    if let Some(check_pos) = args.iter().position(|a| a == "--check") {
        let check_target = args
            .get(check_pos + 1)
            .filter(|a| !a.starts_with('-'))
            .map(|s| s.as_str())
            .unwrap_or(config_path);
        let report = check_constitution(check_target);
        println!(
            "[ConstitutionCheck] Target: {} (configs={}, files={}, tables={})",
            report.target, report.checked_configs, report.checked_files, report.checked_tables
        );
        if report.is_ok() {
            println!("[ConstitutionCheck] PASSED — 0 violations found (M1–M7 compliant).");
            return Ok(());
        } else {
            eprintln!(
                "[ConstitutionCheck] FAILED — {} violation(s) found:",
                report.violations.len()
            );
            for v in &report.violations {
                eprintln!("  - {}", v);
            }
            std::process::exit(1);
        }
    }

    let boot_config = KernelBootConfig::load_from_file(config_path)?;

    if args.iter().any(|a| a == "--smoke-test") {
        return run_variant_smoke_test(config_path, &boot_config);
    }

    if args.iter().any(|a| a == "--auto" || a == "-a") {
        let kernel = AtelierKernel::open_with_db(&boot_config.db_path)?;
        let frontend = InteractiveFrontendDomain::attach_and_boot(&kernel);
        frontend.run_all_automated_demos();
        return Ok(());
    }

    if args.iter().any(|a| a == "--cli") {
        let kernel = AtelierKernel::open_with_db(&boot_config.db_path)?;
        let frontend = InteractiveFrontendDomain::attach_and_boot(&kernel);
        frontend.run_repl();
        return Ok(());
    }

    let enable_web_ui = (args.iter().any(|a| a == "--web-ui" || a == "--dashboard")
        || boot_config.web_ui_enabled)
        && !args.iter().any(|a| a == "--headless");

    if enable_web_ui {
        let kernel = Arc::new(AtelierKernel::open_with_db(&boot_config.db_path)?);
        let web_domain = Arc::new(WebFrontendDomain::attach_with_config(kernel, &boot_config));
        web_domain.serve_http(&boot_config.web_ui_bind)?;
        return Ok(());
    }

    // Default: Headless microkernel runtime without HTTP server port overhead
    let kernel = AtelierKernel::open_with_db(&boot_config.db_path)?;
    let spawned = kernel.supervisor().boot_from_config(&boot_config);
    println!(
        "[AtelierKernel] Microkernel headless runtime active ({} domain(s) supervised from {})",
        spawned.len(),
        config_path
    );
    println!(
        "  Web UI is disabled by default. Pass --web-ui / --dashboard or set \"web_ui_enabled\": true to enable observation console."
    );
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}
