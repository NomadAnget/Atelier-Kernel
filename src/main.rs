use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use atelier_kernel::{
    AtelierKernel, InteractiveFrontendDomain, KernelBootConfig, WebFrontendDomain,
};

const DEFAULT_TEMPLATE_JSON: &str = r#"{
  "db_path": "./data/atelier_kernel.db",
  "web_ui_bind": "127.0.0.1:8787",
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
def on_ping(msg):
    domain.storage_insert(record_id="last_ping", category="health", data=msg.data)
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

    println!("Initialized Atelier Kernel variant skeleton at: {}", root.display());
    println!("  - Config:  {}", cfg_path.display());
    println!("  - Domain:  {}", sample_py_path.display());
    println!("Run with: atelier-kernel --config {}", cfg_path.display());
    Ok(())
}

fn print_help() {
    println!("Atelier Kernel v{} — Single-Machine Multi-Domain Microkernel Runtime", env!("CARGO_PKG_VERSION"));
    println!();
    println!("USAGE:");
    println!("  atelier-kernel [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("  --config <path>   Path to boot configuration JSON (default: ./kernel_boot.json)");
    println!("  --headless        Run pure microkernel supervisor without Web UI dashboard");
    println!("  --cli             Run interactive CLI REPL frontend domain");
    println!("  --auto, -a        Run automated 7-stage kernel verification suite and exit");
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

    // Load external startup configuration (`kernel_boot.json`)
    let config_path = args
        .windows(2)
        .find(|w| w[0] == "--config")
        .map(|w| w[1].as_str())
        .unwrap_or("./kernel_boot.json");

    let boot_config = KernelBootConfig::load_from_file(config_path)?;

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

    if args.iter().any(|a| a == "--headless") {
        let kernel = AtelierKernel::open_with_db(&boot_config.db_path)?;
        let spawned = kernel.supervisor().boot_from_config(&boot_config);
        println!(
            "[AtelierKernel] Headless runtime started ({} domains supervised from {})",
            spawned.len(),
            config_path
        );
        loop {
            thread::sleep(Duration::from_secs(3600));
        }
    }

    // Default: Boot AtelierKernel from `kernel_boot.json` + Web Frontend Domain (`web_ui`)
    let kernel = Arc::new(AtelierKernel::open_with_db(&boot_config.db_path)?);
    let web_domain = Arc::new(WebFrontendDomain::attach_with_config(kernel, &boot_config));
    web_domain.serve_http(&boot_config.web_ui_bind)?;

    Ok(())
}
