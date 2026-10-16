#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="${ROOT_DIR}/dist"
SDK_OUT="${DIST_DIR}/sdk"
RUNTIME_OUT="${DIST_DIR}/kernel-runtime"

if command -v cargo >/dev/null 2>&1; then
  CARGO_BIN="cargo"
elif [ -x "/root/.cargo/bin/cargo" ]; then
  CARGO_BIN="/root/.cargo/bin/cargo"
else
  echo "Error: cargo not found in PATH or /root/.cargo/bin/cargo" >&2
  exit 1
fi

echo "==> [1/4] Cleaning and preparing dist/ directories..."
rm -rf "${DIST_DIR}"
mkdir -p "${SDK_OUT}" "${RUNTIME_OUT}"

echo "==> [2/4] Building Python Domain SDK Wheel (.whl)..."
python3 "${ROOT_DIR}/sdk/python/build_wheel.py" "${SDK_OUT}"

echo "==> [3/4] Building Node.js Domain SDK Package (.tgz)..."
(
  cd "${ROOT_DIR}/sdk/nodejs"
  npm pack --pack-destination "${SDK_OUT}" >/dev/null
)
echo "Built Node.js npm package: $(ls "${SDK_OUT}"/*.tgz)"

echo "==> [4/4] Building Precompiled Rust Kernel Release Binary & Runtime Bundle..."
"${CARGO_BIN}" build --release --offline --manifest-path "${ROOT_DIR}/Cargo.toml"

cp "${ROOT_DIR}/target/release/atelier-kernel" "${RUNTIME_OUT}/atelier-kernel"
chmod +x "${RUNTIME_OUT}/atelier-kernel"
cp "${ROOT_DIR}/kernel_boot.json" "${RUNTIME_OUT}/kernel_boot.template.json"
cp "${ROOT_DIR}/VARIANT_GUIDE.md" "${RUNTIME_OUT}/VARIANT_GUIDE.md"

cat > "${RUNTIME_OUT}/README.md" <<'EOF'
# Atelier Kernel Runtime Bundle

## Contents
- `atelier-kernel`: Precompiled Rust microkernel executable (zero external runtime dependencies)
- `kernel_boot.template.json`: Default multi-domain startup configuration template

## Quick Start for a New Product Variant

1. Initialize a new variant skeleton directory:
   ```bash
   ./atelier-kernel --init ./my-variant
   ```
2. Install the Domain SDKs for your domain languages:
   ```bash
   pip install ../sdk/atelier_sdk-0.1.0-py3-none-any.whl
   npm install ../sdk/atelier-sdk-0.1.0.tgz
   ```
3. Boot your variant with Web Observer UI:
   ```bash
   ./atelier-kernel --config ./my-variant/kernel_boot.json
   ```
   Or boot in headless production mode:
   ```bash
   ./atelier-kernel --config ./my-variant/kernel_boot.json --headless
   ```
EOF

ARCH="$(uname -m)"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCHIVE_NAME="atelier-kernel-runtime-${OS}-${ARCH}.tar.gz"
tar -C "${DIST_DIR}" -czf "${DIST_DIR}/${ARCHIVE_NAME}" kernel-runtime sdk

echo ""
echo "================================================================"
echo "Atelier Kernel Automated Packaging Complete!"
echo "  - Python SDK Wheel:   ${SDK_OUT}/atelier_sdk-0.1.0-py3-none-any.whl"
echo "  - Node.js SDK NPM:    ${SDK_OUT}/atelier-sdk-0.1.0.tgz"
echo "  - Kernel Binary:      ${RUNTIME_OUT}/atelier-kernel"
echo "  - Config Template:    ${RUNTIME_OUT}/kernel_boot.template.json"
echo "  - Full Release Tar:   ${DIST_DIR}/${ARCHIVE_NAME}"
echo "================================================================"
