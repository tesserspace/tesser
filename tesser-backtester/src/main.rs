use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    tracing::info!("Backtester binary is orchestrated via tesser-cli. Use `cargo run -p tesser-cli -- backtest`.");
    Ok(())
}
