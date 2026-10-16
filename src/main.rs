use std::sync::Arc;

use atelier_kernel::{
    AtelierKernel, InteractiveFrontendDomain, KernelBootConfig, WebFrontendDomain,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

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

    // Default: Boot AtelierKernel from `kernel_boot.json` + Web Frontend Domain (`web_ui`)
    let kernel = Arc::new(AtelierKernel::open_with_db(&boot_config.db_path)?);
    let web_domain = Arc::new(WebFrontendDomain::attach_with_config(kernel, &boot_config));
    web_domain.serve_http(&boot_config.web_ui_bind)?;

    Ok(())
}
