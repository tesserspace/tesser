# Visualization

This directory contains the Tauri-based visualization app and its design docs.

- `docs/`: idea + PRD documents (source of truth for contracts)
- `app/`: Tauri desktop app (React/Vite + Tailwind + shadcn/ui + Rust host)

Automated checks:
- Host transport/protocol tests: `cd visualization/app/src-tauri && cargo test`
- Frontend build: `cd visualization/app && pnpm build`
