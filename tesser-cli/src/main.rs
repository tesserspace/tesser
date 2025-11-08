use anyhow::Result;
use chrono::{Duration, Utc};
use clap::{Parser, Subcommand};
use tesser_backtester::{BacktestConfig, Backtester};
use tesser_bybit::BybitClient;
use tesser_core::{Candle, Interval, Symbol};
use tesser_execution::{ExecutionEngine, FixedOrderSizer};
use tesser_paper::PaperExecutionClient;
use tesser_strategy::{SmaCross, SmaCrossConfig};

#[derive(Parser)]
#[command(author, version, about = "Tesser CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a quick backtest using mock data
    Backtest {
        #[arg(long, default_value = "BTCUSDT")]
        symbol: String,
        #[arg(long, default_value_t = 200)]
        candles: usize,
        #[arg(long, default_value_t = 0.01)]
        quantity: f64,
    },
    /// Fetch Bybit server time via the REST API
    BybitTime,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,tesser_cli=info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Backtest {
            symbol,
            candles,
            quantity,
        } => run_backtest(symbol, candles, quantity).await?,
        Command::BybitTime => {
            let client = BybitClient::testnet(None);
            let server_time = client.server_time().await?;
            println!("Bybit server time (ns): {server_time}");
        }
    }
    Ok(())
}

async fn run_backtest(symbol: String, candles: usize, quantity: f64) -> Result<()> {
    let data = synth_candles(&symbol, candles);
    let strategy = Box::new(SmaCross::new(SmaCrossConfig {
        symbol: symbol.clone(),
        fast_period: 5,
        slow_period: 20,
        min_samples: 25,
    }));
    let execution = ExecutionEngine::new(
        PaperExecutionClient::default(),
        Box::new(FixedOrderSizer { quantity }),
    );
    let config = BacktestConfig::new(symbol.clone(), data);
    let report = Backtester::new(config, strategy, execution).run().await?;
    println!(
        "Backtest complete for {symbol}: signals={}, orders={}, ending_equity={:.2}",
        report.signals_emitted, report.orders_sent, report.ending_equity
    );
    Ok(())
}

fn synth_candles(symbol: &str, len: usize) -> Vec<Candle> {
    let mut candles = Vec::with_capacity(len);
    for i in 0..len {
        let base = 50_000.0 + (i as f64).sin() * 500.0;
        let open = base + (i as f64 % 3.0) * 10.0;
        let close = open + (i as f64 % 5.0) * 5.0 - 10.0;
        candles.push(Candle {
            symbol: Symbol::from(symbol),
            interval: Interval::OneMinute,
            open,
            high: open.max(close) + 20.0,
            low: open.min(close) - 20.0,
            close,
            volume: 1.0,
            timestamp: Utc::now() - Duration::minutes((len - i) as i64),
        });
    }
    candles
}
