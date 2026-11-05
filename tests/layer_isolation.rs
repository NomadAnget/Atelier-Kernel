//! 分层隔离检查：`core` / `edge` 不得依赖 `services`（宪法「极简核心 + 核内服务」）。
//!
//! 直接扫描源码文本，防止后续重构把服务依赖悄悄带回核心/边缘层。

use std::fs;
use std::path::{Path, PathBuf};

fn source_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_rust_files(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
}

fn assert_no_services_dependency(layer: &str) {
    let dir = source_root().join("src").join(layer);
    let mut files = Vec::new();
    collect_rust_files(&dir, &mut files);
    assert!(!files.is_empty(), "{layer}: no source files found under {dir:?}");
    for file in files {
        let text = fs::read_to_string(&file).unwrap_or_else(|e| panic!("read {file:?}: {e}"));
        let rel = file.strip_prefix(source_root()).unwrap_or(&file);
        assert!(
            !text.contains("use crate::services"),
            "{layer}: {rel:?} must not import crate::services"
        );
        assert!(
            !text.contains("crate::services::"),
            "{layer}: {rel:?} must not reference crate::services::"
        );
    }
}

#[test]
fn core_layer_does_not_depend_on_services() {
    assert_no_services_dependency("core");
}

#[test]
fn edge_layer_is_a_service_free_leaf() {
    assert_no_services_dependency("edge");
}
