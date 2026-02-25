//! WebAssembly wrapper for `tesser-backtest-core`.
//!
//! Exports a minimal JSON-in/JSON-out ABI so the docs playground can stay lightweight:
//! - `alloc(len) -> ptr`
//! - `dealloc(ptr, len)`
//! - `backtest(input_ptr, input_len, out_ptr)` where `out_ptr` points to `[u32; 2]` (ptr, len)

use serde_json::json;

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "tesser_playground")]
extern "C" {
    fn strategy_on_bar(
        i: u32,
        t: i64,
        o: f64,
        h: f64,
        l: f64,
        c: f64,
        v: f64,
        equity: f64,
        position_weight: f64,
        target_weight: f64,
    ) -> f64;
}

/// Allocate `len` bytes in WASM linear memory and return the pointer.
#[no_mangle]
pub extern "C" fn alloc(len: usize) -> *mut u8 {
    let mut buf = Vec::<u8>::with_capacity(len);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

/// Deallocate a buffer previously allocated via `alloc`.
#[no_mangle]
pub unsafe extern "C" fn dealloc(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    drop(Vec::from_raw_parts(ptr, len, len));
}

/// Run the backtest. Input is a UTF-8 JSON blob matching `tesser_backtest_core::BacktestInput`.
///
/// `out_ptr` must point to 8 bytes in linear memory. We write:
/// - `out_ptr[0]`: result pointer
/// - `out_ptr[1]`: result length
#[no_mangle]
pub unsafe extern "C" fn backtest(input_ptr: *const u8, input_len: usize, out_ptr: *mut u32) {
    // Defensive defaults.
    if !out_ptr.is_null() {
        *out_ptr = 0;
        *out_ptr.add(1) = 0;
    }

    if input_ptr.is_null() || input_len == 0 || out_ptr.is_null() {
        return;
    }

    let input_bytes = std::slice::from_raw_parts(input_ptr, input_len);
    let output_bytes = match run(input_bytes) {
        Ok(bytes) => bytes,
        Err(msg) => serde_json::to_vec(&json!({ "error": msg }))
            .unwrap_or_else(|_| br#"{"error":"unknown"}"#.to_vec()),
    };

    let out_len = output_bytes.len();
    let out_buf = alloc(out_len);
    if out_buf.is_null() {
        return;
    }
    std::ptr::copy_nonoverlapping(output_bytes.as_ptr(), out_buf, out_len);

    *out_ptr = out_buf as u32;
    *out_ptr.add(1) = out_len as u32;
}

fn run(input_bytes: &[u8]) -> Result<Vec<u8>, String> {
    let input: tesser_backtest_core::BacktestInput =
        serde_json::from_slice(input_bytes).map_err(|e| format!("invalid input JSON: {e}"))?;
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = input;
        return Err("tesser-backtest-wasm must be built for wasm32-unknown-unknown".into());
    }

    #[cfg(target_arch = "wasm32")]
    {
        let result = tesser_backtest_core::run_backtest(input, |ctx| unsafe {
            let bar = ctx.bar;
            let out = strategy_on_bar(
                ctx.i as u32,
                bar.t,
                bar.o,
                bar.h,
                bar.l,
                bar.c,
                bar.v,
                ctx.equity,
                ctx.position_weight,
                ctx.target_weight,
            );
            if out.is_nan() {
                None
            } else {
                Some(out)
            }
        })
        .map_err(|e| e.to_string())?;
        serde_json::to_vec(&result).map_err(|e| format!("failed to serialize result: {e}"))
    }
}
