use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use clap::{Args, Parser, Subcommand};
use csv::Writer;
use serde::{Deserialize, Serialize};
use tesser_backtester::{BacktestConfig, BacktestReport, Backtester};
use tesser_config::{load_config, AppConfig};
use tesser_core::{Candle, Interval, Symbol};
use tesser_data::download::{BybitDownloader, KlineRequest};
use tesser_execution::{ExecutionEngine, FixedOrderSizer};
use tesser_paper::PaperExecutionClient;
use tesser_strategy::{build_builtin_strategy, builtin_strategy_names};
use tracing::info;

#[derive(Parser)]
#[command(author, version, about = "Tesser CLI")]
struct Cli {
    /// Increases logging verbosity (-v debug, -vv trace)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    /// Selects which configuration environment to load (maps to config/{env}.toml)
    #[arg(long, default_value = "default")]
    env: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Data engineering tasks
    Data {
        #[command(subcommand)]
        action: DataCommand,
    },
    /// Backtesting workflows
    Backtest {
        #[command(subcommand)]
        action: BacktestCommand,
    },
    /// Live trading workflows
    Live {
        #[command(subcommand)]
        action: LiveCommand,
    },
    /// Strategy management helpers
    Strategies,
}

#[derive(Subcommand)]
enum DataCommand {
    /// Download historical market data (placeholder)
    Download(DataDownloadArgs),
    /// Validate a local data set (placeholder)
    Validate(DataValidateArgs),
    /// Resample existing data (placeholder)
    Resample(DataResampleArgs),
}

#[derive(Subcommand)]
enum BacktestCommand {
    /// Run a backtest from a strategy config file
    Run(BacktestRunArgs),
}

#[derive(Subcommand)]
enum LiveCommand {
    /// Start a live trading session (scaffolding)
    Run(LiveRunArgs),
}

#[derive(Args)]
struct DataDownloadArgs {
    #[arg(long, default_value = "bybit")]
    exchange: String,
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value = "linear")]
    category: String,
    #[arg(long, default_value = "1m")]
    interval: String,
    #[arg(long)]
    start: String,
    #[arg(long)]
    end: Option<String>,
    #[arg(long)]
    output: Option<PathBuf>,
}

impl DataDownloadArgs {
    async fn run(&self, config: &AppConfig) -> Result<()> {
        let exchange_cfg = config
            .exchange
            .get(&self.exchange)
            .ok_or_else(|| anyhow!("exchange profile '{}' not found in config", self.exchange))?;
        let interval: Interval = self.interval.parse().map_err(|err: String| anyhow!(err))?;
        let start = parse_datetime(&self.start)?;
        let end = match &self.end {
            Some(value) => parse_datetime(value)?,
            None => Utc::now(),
        };
        if start >= end {
            return Err(anyhow!("start time must be earlier than end time"));
        }

        let downloader = BybitDownloader::new(&exchange_cfg.rest_url);
        let request = KlineRequest::new(&self.category, &self.symbol, interval, start, end);
        info!(
            "Downloading {} candles for {} ({})",
            self.interval, self.symbol, self.exchange
        );
        let candles = downloader
            .download_klines(&request)
            .await
            .with_context(|| "failed to download candles from Bybit")?;

        if candles.is_empty() {
            info!("No candles returned for {}", self.symbol);
            return Ok(());
        }

        let output_path = self.output.clone().unwrap_or_else(|| {
            default_output_path(config, &self.exchange, &self.symbol, interval, start, end)
        });
        write_candles_csv(&output_path, &candles)?;
        info!(
            "Saved {} candles to {}",
            candles.len(),
            output_path.display()
        );
        Ok(())
    }
}

#[derive(Args)]
struct DataValidateArgs {
    #[arg(long)]
    path: PathBuf,
}

#[derive(Args)]
struct DataResampleArgs {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value = "1h")]
    interval: String,
}

#[derive(Args)]
struct BacktestRunArgs {
    #[arg(long)]
    strategy_config: PathBuf,
    /// One or more CSV files with historical candles (symbol,timestamp,...)
    #[arg(long = "data", value_name = "PATH", num_args = 0.., action = clap::ArgAction::Append)]
    data_paths: Vec<PathBuf>,
    #[arg(long, default_value_t = 500)]
    candles: usize,
    #[arg(long, default_value_t = 0.01)]
    quantity: f64,
}

#[derive(Args)]
struct LiveRunArgs {
    #[arg(long)]
    strategy_config: PathBuf,
    #[arg(long, default_value = "bybit_testnet")]
    exchange: String,
}

#[derive(Deserialize)]
struct StrategyConfigFile {
    #[serde(rename = "strategy_name")]
    name: String,
    #[serde(default = "empty_table")]
    params: toml::Value,
}

fn empty_table() -> toml::Value {
    toml::Value::Table(Default::default())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = load_config(Some(&cli.env)).context("failed to load configuration")?;

    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| match cli.verbose {
        0 => config.log_level.clone(),
        1 => "debug".to_string(),
        _ => "trace".to_string(),
    });

    tracing_subscriber::fmt().with_env_filter(filter).init();

    match cli.command {
        Commands::Data { action } => handle_data(action, &config).await?,
        Commands::Backtest {
            action: BacktestCommand::Run(args),
        } => args.run(&config).await?,
        Commands::Live {
            action: LiveCommand::Run(args),
        } => args.run(&config).await?,
        Commands::Strategies => list_strategies(),
    }

    Ok(())
}

async fn handle_data(cmd: DataCommand, config: &AppConfig) -> Result<()> {
    match cmd {
        DataCommand::Download(args) => {
            args.run(config).await?;
        }
        DataCommand::Validate(args) => {
            info!("stub: validating dataset at {}", args.path.display());
        }
        DataCommand::Resample(args) => {
            info!(
                "stub: resampling {} into {} at {}",
                args.input.display(),
                args.output.display(),
                args.interval
            );
        }
    }
    Ok(())
}

impl BacktestRunArgs {
    async fn run(&self, _config: &AppConfig) -> Result<()> {
        let contents = std::fs::read_to_string(&self.strategy_config)
            .with_context(|| format!("failed to read {}", self.strategy_config.display()))?;
        let def: StrategyConfigFile =
            toml::from_str(&contents).context("failed to parse strategy config file")?;
        let strategy = build_builtin_strategy(&def.name, def.params)
            .with_context(|| format!("failed to configure strategy {}", def.name))?;
        let symbols = strategy.subscriptions();
        if symbols.is_empty() {
            return Err(anyhow::anyhow!("strategy did not declare subscriptions"));
        }

        let mut candles = if self.data_paths.is_empty() {
            let mut generated = Vec::new();
            for (idx, symbol) in symbols.iter().enumerate() {
                let offset = idx as i64 * 10;
                generated.extend(synth_candles(symbol, self.candles, offset));
            }
            generated
        } else {
            load_candles_from_paths(&self.data_paths)?
        };

        if candles.is_empty() {
            return Err(anyhow!(
                "no candles loaded; provide --data or allow synthetic generation"
            ));
        }

        candles.sort_by_key(|c| c.timestamp);

        let execution = ExecutionEngine::new(
            PaperExecutionClient::default(),
            Box::new(FixedOrderSizer {
                quantity: self.quantity,
            }),
        );

        let cfg = BacktestConfig::new(symbols[0].clone(), candles);
        let report = Backtester::new(cfg, strategy, execution)
            .run()
            .await
            .context("backtest failed")?;
        print_report(report);
        Ok(())
    }
}

impl LiveRunArgs {
    async fn run(&self, config: &AppConfig) -> Result<()> {
        let exchange_cfg = config
            .exchange
            .get(&self.exchange)
            .ok_or_else(|| anyhow::anyhow!("exchange profile {} not found", self.exchange))?;
        info!(
            "stub: launching live session on {} (REST {}, WS {})",
            self.exchange, exchange_cfg.rest_url, exchange_cfg.ws_url
        );
        info!(
            "strategy config located at {} (not yet wired to execution)",
            self.strategy_config.display()
        );
        Ok(())
    }
}

fn list_strategies() {
    println!("Built-in strategies:");
    for name in builtin_strategy_names() {
        println!("- {name}");
    }
}

fn print_report(report: BacktestReport) {
    println!("Backtest completed:");
    println!("  Signals generated: {}", report.signals_emitted);
    println!("  Orders sent: {}", report.orders_sent);
    println!("  Ending equity: {:.2}", report.ending_equity);
}

fn synth_candles(symbol: &str, len: usize, offset_minutes: i64) -> Vec<Candle> {
    let mut candles = Vec::with_capacity(len);
    for i in 0..len {
        let base = 50_000.0 + ((i as f64) + offset_minutes as f64).sin() * 500.0;
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
            timestamp: Utc::now() - Duration::minutes((len - i) as i64)
                + Duration::minutes(offset_minutes),
        });
    }
    candles
}

fn parse_datetime(value: &str) -> Result<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.with_timezone(&Utc));
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow!("invalid date"))?;
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
    }
    Err(anyhow!("unable to parse datetime '{value}'"))
}

#[derive(Deserialize)]
struct CandleCsvRow {
    symbol: Option<String>,
    timestamp: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

fn load_candles_from_paths(paths: &[PathBuf]) -> Result<Vec<Candle>> {
    let mut candles = Vec::new();
    for path in paths {
        let mut reader = csv::Reader::from_path(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        for record in reader.deserialize::<CandleCsvRow>() {
            let row = record.with_context(|| format!("invalid row in {}", path.display()))?;
            let timestamp = parse_datetime(&row.timestamp)?;
            let symbol = row
                .symbol
                .clone()
                .or_else(|| infer_symbol_from_path(path))
                .ok_or_else(|| {
                    anyhow!(
                        "missing symbol column and unable to infer from path {}",
                        path.display()
                    )
                })?;
            let interval = infer_interval_from_path(path).unwrap_or(Interval::OneMinute);
            candles.push(Candle {
                symbol,
                interval,
                open: row.open,
                high: row.high,
                low: row.low,
                close: row.close,
                volume: row.volume,
                timestamp,
            });
        }
    }
    Ok(candles)
}

fn infer_symbol_from_path(path: &Path) -> Option<String> {
    path.parent()
        .and_then(|p| p.file_name())
        .map(|os| os.to_string_lossy().to_string())
}

fn infer_interval_from_path(path: &Path) -> Option<Interval> {
    path.file_stem()
        .and_then(|os| os.to_str())
        .and_then(|stem| stem.split('_').next())
        .and_then(|token| Interval::from_str(token).ok())
}

fn default_output_path(
    config: &AppConfig,
    exchange: &str,
    symbol: &str,
    interval: Interval,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> PathBuf {
    let interval_part = interval_label(interval);
    let start_part = start.format("%Y%m%d").to_string();
    let end_part = end.format("%Y%m%d").to_string();
    config
        .data_path
        .join(exchange)
        .join(symbol)
        .join(format!("{}_{}-{}.csv", interval_part, start_part, end_part))
}

fn interval_label(interval: Interval) -> &'static str {
    match interval {
        Interval::OneSecond => "1s",
        Interval::OneMinute => "1m",
        Interval::FiveMinutes => "5m",
        Interval::FifteenMinutes => "15m",
        Interval::OneHour => "1h",
        Interval::FourHours => "4h",
        Interval::OneDay => "1d",
    }
}

#[derive(Serialize)]
struct CandleRow<'a> {
    symbol: &'a str,
    timestamp: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

fn write_candles_csv(path: &Path, candles: &[Candle]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let mut writer =
        Writer::from_path(path).with_context(|| format!("failed to create {}", path.display()))?;
    for candle in candles {
        let row = CandleRow {
            symbol: &candle.symbol,
            timestamp: candle.timestamp.to_rfc3339(),
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume,
        };
        writer.serialize(row)?;
    }
    writer.flush()?;
    Ok(())
}
