#!/usr/bin/env bash
set -euo pipefail

cargo fmt --all
cargo clippy --fix --allow-dirty --allow-staged
cargo clippy --all-targets --all-features -- -D warnings
cargo test --release --all-targets

# Ensure the musl target is installed so we can mimic CI's Alpine build.
if ! rustup target list --installed | grep -q 'x86_64-unknown-linux-musl'; then
  rustup target add x86_64-unknown-linux-musl
fi

# Ensure the system toolchain is available for linking.
if ! command -v x86_64-linux-musl-gcc >/dev/null 2>&1; then
  echo "warning: x86_64-linux-musl-gcc not found; skipping musl build. Install musl-tools to enable this check." >&2
else
  # Build the CLI in musl mode with only Bybit enabled to match the release matrix.
  cargo build --package tesser-cli --release \
    --target x86_64-unknown-linux-musl \
    --no-default-features --features bybit
fi
