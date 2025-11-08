# Tesser Coding Style Guide

This document outlines the coding style and conventions for the Tesser project. Adhering to these guidelines ensures that the codebase remains readable, consistent, and maintainable as it grows.

## General Principles

1.  **Clarity is King**: Write code that is easy to understand. Prefer clear, explicit logic over overly clever or "magic" code.
2.  **Idiomatic Rust**: Follow the conventions of the Rust community. When in doubt, refer to the official [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/).
3.  **Safety and Correctness**: Correctness is non-negotiable in a trading system. Avoid `unsafe` code unless absolutely necessary and heavily justified.

## Formatting

*   **`rustfmt` is mandatory.** All code must be formatted with the stable version of `rustfmt`. The project is configured to use the default settings. Before submitting a pull request, please run:
    ```sh
    cargo fmt --all
    ```
*   **Line Length**: Keep lines to a maximum of 100 characters. `rustfmt` will handle this automatically.

## Clippy

*   **`clippy` is mandatory.** Clippy is a powerful linter that catches common mistakes and helps improve code quality. Pull requests must be free of Clippy warnings. Run clippy with:
    ```sh
    cargo clippy --all-targets --all-features -- -D warnings
    ```

## Naming Conventions

Follow the standard Rust naming conventions ([RFC 430](https://rust-lang.github.io/rfcs/0430-finalizing-naming-conventions.html)):

## Error Handling

*   Use `Result<T, E>` for all fallible operations.
*   Define custom, specific error types for each crate using an `Error` enum. The [`thiserror`](https://crates.io/crates/thiserror) crate is highly recommended for this.
*   **Do not use `.unwrap()` or `.expect()` in library code.** These methods should only be used in tests or at the top level of a binary (`tesser-cli`) where a panic is an acceptable outcome.
*   Provide context with your errors. Use `map_err` to add context to error chains.

## Documentation

*   **All public items** (functions, structs, enums, traits, and modules) **must** have `rustdoc` comments (`///`).
*   The documentation should explain the *purpose* ("what" and "why") of the item, not just the implementation details ("how").
*   Include small, runnable examples in the documentation where it helps clarify usage.

```rust
/// Calculates the simple moving average (SMA) of a series.
///
/// # Examples
///
/// ```
/// let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
/// let sma = tesser::indicators::sma(&data, 3);
/// assert_eq!(sma, vec![2.0, 3.0, 4.0]);
/// ```
pub fn sma(data: &[f64], period: usize) -> Vec<f64> {
    // ... implementation ...
}
```

## Testing

*   **Unit Tests**: Should be placed in a `mod tests` block at the bottom of the file they are testing, annotated with `#[cfg(test)]`.
*   **Integration Tests**: Should be placed in the `tests/` directory at the root of a crate.
*   Strive for high test coverage on critical business logic (e.g., calculations, state machines, portfolio logic).

## Unsafe Code

*   The use of `unsafe` is strongly discouraged.
*   If `unsafe` is absolutely necessary (e.g., for FFI or a critical performance optimization), it must be:
    1.  Encapsulated within a safe abstraction.
    2.  Justified with a comment explaining why it is needed.
    3.  Accompanied by a `// SAFETY:` comment block that explains why the code is sound.

## Commit Messages

Please follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification. This helps automate changelog generation and makes the project history easier to navigate.

*   **Examples**:
    *   `feat: add support for Binance WebSocket streams`
    *   `fix(backtester): correct handling of market-on-open orders`
    *   `docs(readme): update getting started instructions`
    *   `refactor(core): move Position struct to its own module`

Do not use long descriptions in commit messages; keep them concise and to the point.
