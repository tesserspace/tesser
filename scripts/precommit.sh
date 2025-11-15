cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --release --all-targets
