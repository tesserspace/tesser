# Tauri WebDriver E2E (Linux CI)

This directory contains end-to-end UI tests that run the **real Tauri app** and drive it via
the WebDriver protocol (PRD-18), using `tauri-driver` as the cross-platform wrapper.

Notes
- macOS is not supported by `tauri-driver` today (WKWebView has no official WebDriver).
- CI runs these tests on Linux only (WebKitWebDriver).

## Run locally (Linux)

Prereqs:
- `WebKitWebDriver` available in `PATH` (often `webkit2gtk-driver`)
- `tauri-driver` installed: `cargo install tauri-driver --locked`
- E2E deps installed: `pnpm -C visualization/app/e2e/tauri install`

Run:
- From `visualization/app`: `pnpm e2e:tauri`

Env:
- `TAURI_DRIVER_PORT` / `TAURI_NATIVE_DRIVER_PORT` to avoid local port collisions.
