use crate::alerts::sanitize_webhook;
use crate::analyze;
use crate::data_validation::{validate_dataset, ValidationConfig, ValidationOutcome};
use crate::live::{
    run_live, ExecutionBackend, LiveSessionSettings, PersistenceBackend, PersistenceSettings,
};
use crate::state;
use crate::telemetry::init_tracing;
use crate::PublicChannel;
use arrow::util::pretty::print_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use csv::Writer;
use futures::StreamExt;
use rust_decimal::{
    prelude::{FromPrimitive, ToPrimitive},
    Decimal,
};
use serde::{Deserialize, Serialize};
use tesser_backtester::reporting::PerformanceReport;
use tesser_backtester::{
    stream_from_events, BacktestConfig, BacktestMode, BacktestStream, Backtester, MarketEvent,
    MarketEventKind, MarketEventStream,
};
use tesser_broker::ExecutionClient;
use tesser_config::{load_config, AppConfig, PersistenceEngine, RiskManagementConfig};
use tesser_core::{Candle, DepthUpdate, Interval, OrderBook, OrderBookLevel, Side, Symbol, Tick};
use tesser_data::analytics::ExecutionAnalysisRequest;
use tesser_data::download::{BinanceDownloader, BybitDownloader, KlineRequest};
use tesser_data::merger::UnifiedEventStream;
use tesser_data::parquet::ParquetMarketStream;
use tesser_execution::{
    ExecutionEngine, FixedOrderSizer, NoopRiskChecker, OrderSizer, PortfolioPercentSizer,
    RiskAdjustedSizer,
};
use tesser_markets::MarketRegistry;
use tesser_paper::{MatchingEngine, PaperExecutionClient, PaperMarketStream};
use tesser_strategy::{builtin_strategy_names, load_strategy};
use tracing::{info, warn};

#[derive(Parser)]
#[command(author, version, about = "Tesser Trading Framework")]
pub struct Cli {
    /// Increases logging verbosity (-v debug, -vv trace)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    /// Selects which configuration environment to load (maps to config/{env}.toml)
    #[arg(long, default_value = "default")]
    env: String,
    #[command(subcommand)]
    command: Commands,
}

#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
pub enum Commands {
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
    /// Inspect or repair persisted runtime state
    State {
        #[command(subcommand)]
        action: StateCommand,
    },
    /// Strategy management helpers
    Strategies,
    /// Analyze previously recorded trading sessions
    Analyze {
        #[command(subcommand)]
        action: AnalyzeCommand,
    },
}

#[derive(Subcommand)]
pub enum DataCommand {
    /// Download historical market data
    Download(DataDownloadArgs),
    /// Validate and optionally repair a local data set
    Validate(DataValidateArgs),
    /// Resample existing data (placeholder)
    Resample(DataResampleArgs),
    /// Inspect a parquet file emitted by the flight recorder
    InspectParquet(DataInspectParquetArgs),
}

#[derive(Subcommand)]
pub enum BacktestCommand {
    /// Run a single backtest from a strategy config file
    Run(BacktestRunArgs),
    /// Run multiple strategy configs and aggregate the results
    Batch(BacktestBatchArgs),
}

#[derive(Subcommand)]
pub enum LiveCommand {
    /// Start a live trading session (scaffolding)
    Run(LiveRunArgs),
}

#[derive(Subcommand)]
pub enum StateCommand {
    /// Inspect the persisted live state snapshot
    Inspect(StateInspectArgs),
}

#[derive(Subcommand)]
pub enum AnalyzeCommand {
    /// Generate a TCA-lite execution report using flight recorder files
    Execution(AnalyzeExecutionArgs),
}

#[derive(Args)]
pub struct DataDownloadArgs {
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
    /// Skip automatic validation after download completes
    #[arg(long)]
    skip_validation: bool,
    /// Attempt to repair gaps detected during validation
    #[arg(long)]
    repair_missing: bool,
    /// Max allowed close-to-close jump when auto-validating (fractional)
    #[arg(long, default_value_t = 0.05)]
    validation_jump_threshold: f64,
    /// Allowed divergence between primary and reference closes (fractional)
    #[arg(long, default_value_t = 0.002)]
    validation_reference_tolerance: f64,
}

#[derive(Args)]
pub struct StateInspectArgs {
    /// Path to the persisted state (file for sqlite, directory for lmdb)
    #[arg(long)]
    path: Option<PathBuf>,
    /// Emit the raw JSON payload stored inside the database
    #[arg(long)]
    raw: bool,
}

#[derive(Args)]
pub struct AnalyzeExecutionArgs {
    /// Directory containing flight recorder parquet partitions
    #[arg(long, value_name = "PATH", default_value = "data/flight_recorder")]
    data_dir: PathBuf,
    /// Inclusive start of the analysis window (RFC3339, `YYYY-mm-dd`, etc.)
    #[arg(long)]
    start: Option<String>,
    /// Inclusive end of the analysis window
    #[arg(long)]
    end: Option<String>,
}

impl StateInspectArgs {
    fn resolved_path(&self, config: &AppConfig) -> PathBuf {
        self.path
            .clone()
            .unwrap_or_else(|| config.live.persistence_config().path.clone())
    }

    fn resolved_engine(&self, config: &AppConfig) -> PersistenceEngine {
        config.live.persistence_config().engine
    }
}

impl AnalyzeExecutionArgs {
    fn build_request(&self) -> Result<ExecutionAnalysisRequest> {
        let start = match &self.start {
            Some(value) => Some(parse_datetime(value)?),
            None => None,
        };
        let end = match &self.end {
            Some(value) => Some(parse_datetime(value)?),
            None => None,
        };
        Ok(ExecutionAnalysisRequest {
            data_dir: self.data_dir.clone(),
            start,
            end,
        })
    }
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

        info!(
            "Downloading {} candles for {} ({})",
            self.interval, self.symbol, self.exchange
        );
        let mut candles = match exchange_cfg.driver.as_str() {
            "bybit" | "" => {
                let downloader = BybitDownloader::new(&exchange_cfg.rest_url);
                let request = KlineRequest::new(&self.category, &self.symbol, interval, start, end);
                downloader
                    .download_klines(&request)
                    .await
                    .with_context(|| "failed to download candles from Bybit")?
            }
            "binance" => {
                let downloader = BinanceDownloader::new(&exchange_cfg.rest_url);
                let request = KlineRequest::new("", &self.symbol, interval, start, end);
                downloader
                    .download_klines(&request)
                    .await
                    .with_context(|| "failed to download candles from Binance")?
            }
            other => bail!("unknown exchange driver '{other}' for {}", self.exchange),
        };

        if candles.is_empty() {
            info!("No candles returned for {}", self.symbol);
            return Ok(());
        }

        if !self.skip_validation {
            let config = ValidationConfig {
                price_jump_threshold: self.validation_jump_threshold.max(f64::EPSILON),
                reference_tolerance: self.validation_reference_tolerance.max(f64::EPSILON),
                repair_missing: self.repair_missing,
            };
            let outcome =
                validate_dataset(candles.clone(), None, config).context("validation failed")?;
            print_validation_summary(&outcome);
            if self.repair_missing && outcome.summary.repaired_candles > 0 {
                candles = outcome.repaired;
                info!(
                    "Applied {} synthetic candle(s) to repair gaps",
                    outcome.summary.repaired_candles
                );
            }
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
pub struct DataValidateArgs {
    /// One or more CSV files to inspect
    #[arg(
        long = "path",
        value_name = "PATH",
        num_args = 1..,
        action = clap::ArgAction::Append
    )]
    paths: Vec<PathBuf>,
    /// Optional reference data set(s) used for cross validation
    #[arg(
        long = "reference",
        value_name = "PATH",
        num_args = 1..,
        action = clap::ArgAction::Append
    )]
    reference_paths: Vec<PathBuf>,
    /// Max allowed close-to-close jump before flagging (fractional, 0.05 = 5%)
    #[arg(long, default_value_t = 0.05)]
    jump_threshold: f64,
    /// Allowed divergence between primary and reference closes (fractional)
    #[arg(long, default_value_t = 0.002)]
    reference_tolerance: f64,
    /// Attempt to fill gaps by synthesizing candles
    #[arg(long)]
    repair_missing: bool,
    /// Location to write the repaired dataset
    #[arg(long)]
    output: Option<PathBuf>,
}

impl DataValidateArgs {
    fn run(&self) -> Result<()> {
        if self.paths.is_empty() {
            bail!("provide at least one --path for validation");
        }
        let candles =
            load_candles_from_paths(&self.paths).with_context(|| "failed to load dataset")?;
        if candles.is_empty() {
            bail!("loaded dataset is empty; nothing to validate");
        }
        let reference = if self.reference_paths.is_empty() {
            None
        } else {
            Some(
                load_candles_from_paths(&self.reference_paths)
                    .with_context(|| "failed to load reference dataset")?,
            )
        };

        let price_jump_threshold = if self.jump_threshold <= 0.0 {
            0.0001
        } else {
            self.jump_threshold
        };
        let reference_tolerance = if self.reference_tolerance <= 0.0 {
            0.0001
        } else {
            self.reference_tolerance
        };

        let config = ValidationConfig {
            price_jump_threshold,
            reference_tolerance,
            repair_missing: self.repair_missing,
        };

        let outcome = validate_dataset(candles, reference, config)?;
        print_validation_summary(&outcome);

        if let Some(output) = &self.output {
            write_candles_csv(output, &outcome.repaired)?;
            info!(
                "Wrote {} candles ({} new) to {}",
                outcome.repaired.len(),
                outcome.summary.repaired_candles,
                output.display()
            );
        } else if self.repair_missing && outcome.summary.repaired_candles > 0 {
            warn!(
                "Detected {} gap(s) filled with synthetic candles but --output was not provided",
                outcome.summary.repaired_candles
            );
        }

        Ok(())
    }
}

#[derive(Args)]
pub struct DataResampleArgs {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value = "1h")]
    interval: String,
}

#[derive(Args)]
pub struct DataInspectParquetArgs {
    /// Path to the parquet file produced by the flight recorder
    #[arg(value_name = "PATH")]
    path: PathBuf,
    /// Number of rows to display (0 prints the entire file)
    #[arg(long, default_value_t = 10)]
    rows: usize,
}

impl DataInspectParquetArgs {
    fn run(&self) -> Result<()> {
        let limit = if self.rows == 0 {
            usize::MAX
        } else {
            self.rows
        };
        let file = File::open(&self.path)
            .with_context(|| format!("failed to open {}", self.path.display()))?;
        let batch_size = limit.clamp(1, 8192);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(batch_size)
            .build()?;

        let mut printed = 0usize;
        while printed < limit {
            match reader.next() {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let remaining = limit.saturating_sub(printed);
                    let take = remaining.min(batch.num_rows());
                    let display_batch = if take == batch.num_rows() {
                        batch
                    } else {
                        batch.slice(0, take)
                    };
                    print_batches(std::slice::from_ref(&display_batch))?;
                    printed += take;
                }
                Some(Err(err)) => return Err(err.into()),
                None => break,
            }
        }

        if printed == 0 {
            println!("no rows available in {}", self.path.display());
        } else if limit != usize::MAX {
            println!("displayed {printed} row(s) from {}", self.path.display());
        }

        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum BacktestModeArg {
    Candle,
    Tick,
}

#[derive(Args)]
pub struct BacktestRunArgs {
    #[arg(long)]
    strategy_config: PathBuf,
    /// One or more CSV files with historical candles (symbol,timestamp,...)
    #[arg(long = "data", value_name = "PATH", num_args = 0.., action = clap::ArgAction::Append)]
    data_paths: Vec<PathBuf>,
    #[arg(long, default_value_t = 500)]
    candles: usize,
    #[arg(long, default_value = "0.01")]
    quantity: Decimal,
    /// Symmetric slippage in basis points (1 bp = 0.01%) applied to fills
    #[arg(long, default_value = "0")]
    slippage_bps: Decimal,
    /// Trading fees in basis points applied to notional
    #[arg(long, default_value = "0")]
    fee_bps: Decimal,
    /// Number of candles between signal and execution
    #[arg(long, default_value_t = 1)]
    latency_candles: usize,
    /// Order sizer (e.g. "fixed:0.01", "percent:0.02")
    #[arg(long, default_value = "fixed:0.01")]
    sizer: String,
    /// Selects the data source driving fills (`candle` or `tick`)
    #[arg(long, value_enum, default_value = "candle")]
    mode: BacktestModeArg,
    /// One or more JSONL files or a flight-recorder directory containing tick/order book events (required for `--mode tick`)
    #[arg(long = "lob-data", value_name = "PATH", num_args = 0.., action = clap::ArgAction::Append)]
    lob_paths: Vec<PathBuf>,
    #[arg(long)]
    markets_file: Option<PathBuf>,
}

enum LobSource {
    Json(Vec<PathBuf>),
    FlightRecorder(PathBuf),
}

#[derive(Args)]
pub struct BacktestBatchArgs {
    /// Glob or directory containing strategy config files
    #[arg(long = "config", value_name = "PATH", num_args = 1.., action = clap::ArgAction::Append)]
    config_paths: Vec<PathBuf>,
    /// Candle CSVs available to every strategy
    #[arg(long = "data", value_name = "PATH", num_args = 1.., action = clap::ArgAction::Append)]
    data_paths: Vec<PathBuf>,
    #[arg(long, default_value = "0.01")]
    quantity: Decimal,
    /// Optional output CSV summarizing results
    #[arg(long)]
    output: Option<PathBuf>,
    /// Symmetric slippage in basis points (1 bp = 0.01%) applied to fills
    #[arg(long, default_value = "0")]
    slippage_bps: Decimal,
    /// Trading fees in basis points applied to notional
    #[arg(long, default_value = "0")]
    fee_bps: Decimal,
    /// Number of candles between signal and execution
    #[arg(long, default_value_t = 1)]
    latency_candles: usize,
    /// Order sizer (e.g. "fixed:0.01", "percent:0.02")
    #[arg(long, default_value = "fixed:0.01")]
    sizer: String,
    #[arg(long)]
    markets_file: Option<PathBuf>,
}

#[derive(Args)]
pub struct LiveRunArgs {
    #[arg(long)]
    strategy_config: PathBuf,
    #[arg(long, default_value = "paper_sandbox")]
    exchange: String,
    #[arg(long, default_value = "linear")]
    category: String,
    #[arg(long, default_value = "1m")]
    interval: String,
    #[arg(long, default_value = "1")]
    quantity: Decimal,
    /// Selects which execution backend to use (`paper` or `bybit`)
    #[arg(
        long = "exec",
        default_value = "paper",
        value_enum,
        alias = "live-exec"
    )]
    exec: ExecutionBackend,
    /// Path to persisted state (file for sqlite, directory for lmdb)
    #[arg(long)]
    state_path: Option<PathBuf>,
    /// Persistence backend to use for runtime state (overrides config.live.persistence.engine)
    #[arg(long, value_enum)]
    persistence: Option<PersistenceBackend>,
    #[arg(long)]
    metrics_addr: Option<String>,
    #[arg(long)]
    log_path: Option<PathBuf>,
    /// Directory where parquet market/trade data is recorded
    #[arg(
        long = "record-data",
        value_name = "PATH",
        default_value = "data/flight_recorder"
    )]
    record_data: PathBuf,
    /// Control plane gRPC bind address (overrides config.live.control_addr)
    #[arg(long)]
    control_addr: Option<String>,
    #[arg(long)]
    initial_equity: Option<Decimal>,
    #[arg(long)]
    markets_file: Option<PathBuf>,
    #[arg(long, default_value = "0")]
    slippage_bps: Decimal,
    #[arg(long, default_value = "0")]
    fee_bps: Decimal,
    #[arg(long, default_value_t = 0)]
    latency_ms: u64,
    #[arg(long, default_value_t = 512)]
    history: usize,
    #[arg(long)]
    reconciliation_interval_secs: Option<u64>,
    #[arg(long)]
    reconciliation_threshold: Option<Decimal>,
    #[arg(long)]
    webhook_url: Option<String>,
    #[arg(long)]
    alert_max_data_gap_secs: Option<u64>,
    #[arg(long)]
    alert_max_order_failures: Option<u32>,
    #[arg(long)]
    alert_max_drawdown: Option<Decimal>,
    #[arg(long)]
    risk_max_order_qty: Option<Decimal>,
    #[arg(long)]
    risk_max_position_qty: Option<Decimal>,
    #[arg(long)]
    risk_max_drawdown: Option<Decimal>,
    /// Bybit orderbook depth to subscribe to (e.g., 1, 25, 50). Ignored for other connectors.
    #[arg(long)]
    orderbook_depth: Option<usize>,
    /// Order sizer (e.g. "fixed:0.01", "percent:0.02")
    #[arg(long, default_value = "fixed:1.0")]
    sizer: String,
}

impl LiveRunArgs {
    fn resolved_log_path(&self, config: &AppConfig) -> PathBuf {
        self.log_path
            .clone()
            .unwrap_or_else(|| config.live.log_path.clone())
    }

    fn resolved_metrics_addr(&self, config: &AppConfig) -> Result<SocketAddr> {
        let addr = self
            .metrics_addr
            .clone()
            .unwrap_or_else(|| config.live.metrics_addr.clone());
        addr.parse()
            .with_context(|| format!("invalid metrics address '{addr}'"))
    }

    fn resolved_control_addr(&self, config: &AppConfig) -> Result<SocketAddr> {
        let addr = self
            .control_addr
            .clone()
            .unwrap_or_else(|| config.live.control_addr.clone());
        addr.parse()
            .with_context(|| format!("invalid control address '{addr}'"))
    }

    fn reconciliation_interval(&self, config: &AppConfig) -> StdDuration {
        let secs = self
            .reconciliation_interval_secs
            .unwrap_or(config.live.reconciliation_interval_secs)
            .max(1);
        StdDuration::from_secs(secs)
    }

    fn reconciliation_threshold(&self, config: &AppConfig) -> Decimal {
        let configured = self
            .reconciliation_threshold
            .unwrap_or(config.live.reconciliation_threshold);
        if configured <= Decimal::ZERO {
            config.live.reconciliation_threshold.max(Decimal::new(1, 6))
        } else {
            configured
        }
    }

    fn resolved_initial_balances(&self, config: &AppConfig) -> HashMap<Symbol, Decimal> {
        let mut balances = clone_initial_balances(&config.backtest);
        if let Some(value) = self.initial_equity {
            balances.insert(
                config.backtest.reporting_currency.clone(),
                value.max(Decimal::ZERO),
            );
        }
        balances
    }

    fn build_alerting(&self, config: &AppConfig) -> tesser_config::AlertingConfig {
        let mut alerting = config.live.alerting.clone();
        let webhook = self
            .webhook_url
            .clone()
            .or_else(|| alerting.webhook_url.clone());
        alerting.webhook_url = sanitize_webhook(webhook);
        if let Some(sec) = self.alert_max_data_gap_secs {
            alerting.max_data_gap_secs = sec;
        }
        if let Some(limit) = self.alert_max_order_failures {
            alerting.max_order_failures = limit;
        }
        if let Some(limit) = self.alert_max_drawdown {
            alerting.max_drawdown = limit.max(Decimal::ZERO);
        }
        alerting
    }

    fn build_risk_config(&self, config: &AppConfig) -> RiskManagementConfig {
        let mut risk = config.risk_management.clone();
        if let Some(limit) = self.risk_max_order_qty {
            risk.max_order_quantity = limit.max(Decimal::ZERO);
        }
        if let Some(limit) = self.risk_max_position_qty {
            risk.max_position_quantity = limit.max(Decimal::ZERO);
        }
        if let Some(limit) = self.risk_max_drawdown {
            risk.max_drawdown = limit.max(Decimal::ZERO);
        }
        risk
    }
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

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let config = load_config(Some(&cli.env)).context("failed to load configuration")?;

    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| match cli.verbose {
        0 => config.log_level.clone(),
        1 => "debug".to_string(),
        _ => "trace".to_string(),
    });

    let log_override = match &cli.command {
        Commands::Live {
            action: LiveCommand::Run(args),
        } => Some(args.resolved_log_path(&config)),
        _ => None,
    };

    init_tracing(&filter, log_override.as_deref()).context("failed to initialize logging")?;

    match cli.command {
        Commands::Data { action } => handle_data(action, &config).await?,
        Commands::Backtest {
            action: BacktestCommand::Run(args),
        } => args.run(&config).await?,
        Commands::Backtest {
            action: BacktestCommand::Batch(args),
        } => args.run(&config).await?,
        Commands::Live {
            action: LiveCommand::Run(args),
        } => args.run(&config).await?,
        Commands::State { action } => handle_state(action, &config).await?,
        Commands::Analyze { action } => handle_analyze(action)?,
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
            args.run()?;
        }
        DataCommand::Resample(args) => {
            info!(
                "stub: resampling {} into {} at {}",
                args.input.display(),
                args.output.display(),
                args.interval
            );
        }
        DataCommand::InspectParquet(args) => {
            args.run()?;
        }
    }
    Ok(())
}

async fn handle_state(cmd: StateCommand, config: &AppConfig) -> Result<()> {
    match cmd {
        StateCommand::Inspect(args) => {
            state::inspect_state(
                args.resolved_path(config),
                args.resolved_engine(config),
                args.raw,
            )
            .await?;
        }
    }
    Ok(())
}

fn handle_analyze(cmd: AnalyzeCommand) -> Result<()> {
    match cmd {
        AnalyzeCommand::Execution(args) => analyze::run_execution(args.build_request()?),
    }
}

impl BacktestRunArgs {
    async fn run(&self, config: &AppConfig) -> Result<()> {
        let contents = std::fs::read_to_string(&self.strategy_config)
            .with_context(|| format!("failed to read {}", self.strategy_config.display()))?;
        let def: StrategyConfigFile =
            toml::from_str(&contents).context("failed to parse strategy config file")?;
        let strategy = load_strategy(&def.name, def.params)
            .with_context(|| format!("failed to configure strategy {}", def.name))?;
        let symbols = strategy.subscriptions();
        if symbols.is_empty() {
            return Err(anyhow::anyhow!("strategy did not declare subscriptions"));
        }

        let mode = match self.mode {
            BacktestModeArg::Candle => BacktestMode::Candle,
            BacktestModeArg::Tick => BacktestMode::Tick,
        };

        let markets_path = self
            .markets_file
            .clone()
            .or_else(|| config.backtest.markets_file.clone())
            .ok_or_else(|| anyhow!("backtest requires --markets-file or backtest.markets_file"))?;
        let market_registry = Arc::new(
            MarketRegistry::load_from_file(&markets_path).with_context(|| {
                format!("failed to load markets from {}", markets_path.display())
            })?,
        );

        let (market_stream, event_stream, execution_client, matching_engine) = match mode {
            BacktestMode::Candle => {
                let stream = self.build_candle_stream(&symbols)?;
                (
                    Some(stream),
                    None,
                    Arc::new(PaperExecutionClient::default()) as Arc<dyn ExecutionClient>,
                    None,
                )
            }
            BacktestMode::Tick => {
                if self.lob_paths.is_empty() {
                    bail!("--lob-data is required when --mode tick");
                }
                let source = self.detect_lob_source()?;
                let engine = Arc::new(MatchingEngine::new(
                    "matching-engine",
                    symbols.clone(),
                    reporting_balance(&config.backtest),
                ));
                let stream = match source {
                    LobSource::Json(paths) => {
                        let events = load_lob_events_from_paths(&paths)?;
                        if events.is_empty() {
                            bail!("no order book events loaded from --lob-data");
                        }
                        stream_from_events(events)
                    }
                    LobSource::FlightRecorder(root) => self
                        .build_flight_recorder_stream(&root, &symbols)
                        .context("failed to initialize flight recorder stream")?,
                };
                (
                    None,
                    Some(stream),
                    engine.clone() as Arc<dyn ExecutionClient>,
                    Some(engine),
                )
            }
        };

        let sizer = parse_sizer(&self.sizer, Some(self.quantity))?;
        let order_quantity = self.quantity;
        let execution = ExecutionEngine::new(execution_client, sizer, Arc::new(NoopRiskChecker));

        let mut cfg = BacktestConfig::new(symbols[0].clone());
        cfg.order_quantity = order_quantity;
        cfg.initial_balances = clone_initial_balances(&config.backtest);
        cfg.reporting_currency = config.backtest.reporting_currency.clone();
        cfg.execution.slippage_bps = self.slippage_bps.max(Decimal::ZERO);
        cfg.execution.fee_bps = self.fee_bps.max(Decimal::ZERO);
        cfg.execution.latency_candles = self.latency_candles.max(1);
        cfg.mode = mode;

        let report = Backtester::new(
            cfg,
            strategy,
            execution,
            matching_engine,
            market_registry,
            market_stream,
            event_stream,
        )
        .run()
        .await
        .context("backtest failed")?;
        print_report(&report);
        Ok(())
    }

    fn build_candle_stream(&self, symbols: &[Symbol]) -> Result<BacktestStream> {
        if symbols.is_empty() {
            bail!("strategy did not declare any subscriptions");
        }
        if self.data_paths.is_empty() {
            let mut generated = Vec::new();
            for (idx, symbol) in symbols.iter().enumerate() {
                let offset = idx as i64 * 10;
                generated.extend(synth_candles(symbol, self.candles, offset));
            }
            generated.sort_by_key(|c| c.timestamp);
            if generated.is_empty() {
                bail!("no synthetic candles generated; provide --data files instead");
            }
            return Ok(memory_market_stream(&symbols[0], generated));
        }

        match detect_data_format(&self.data_paths)? {
            DataFormat::Csv => {
                let mut candles = load_candles_from_paths(&self.data_paths)?;
                if candles.is_empty() {
                    bail!("no candles loaded from --data paths");
                }
                candles.sort_by_key(|c| c.timestamp);
                Ok(memory_market_stream(&symbols[0], candles))
            }
            DataFormat::Parquet => Ok(parquet_market_stream(symbols, self.data_paths.clone())),
        }
    }

    fn detect_lob_source(&self) -> Result<LobSource> {
        if self.lob_paths.len() == 1 {
            let path = &self.lob_paths[0];
            if path.is_dir() {
                return Ok(LobSource::FlightRecorder(path.clone()));
            }
        }
        if self.lob_paths.iter().all(|path| path.is_file()) {
            return Ok(LobSource::Json(self.lob_paths.clone()));
        }
        bail!(
            "tick mode expects either JSONL files or a single flight-recorder directory via --lob-data"
        )
    }

    fn build_flight_recorder_stream(
        &self,
        root: &Path,
        symbols: &[Symbol],
    ) -> Result<MarketEventStream> {
        let stream = UnifiedEventStream::from_flight_recorder(root, symbols)?
            .into_stream()
            .map(|event| event.map(MarketEvent::from));
        Ok(Box::pin(stream))
    }
}

impl BacktestBatchArgs {
    async fn run(&self, config: &AppConfig) -> Result<()> {
        if self.config_paths.is_empty() {
            return Err(anyhow!("provide at least one --config path"));
        }
        if self.data_paths.is_empty() {
            return Err(anyhow!("provide at least one --data path for batch mode"));
        }
        let markets_path = self
            .markets_file
            .clone()
            .or_else(|| config.backtest.markets_file.clone())
            .ok_or_else(|| {
                anyhow!("batch mode requires --markets-file or backtest.markets_file")
            })?;
        let market_registry = Arc::new(
            MarketRegistry::load_from_file(&markets_path).with_context(|| {
                format!("failed to load markets from {}", markets_path.display())
            })?,
        );
        let data_format = detect_data_format(&self.data_paths)?;
        let mut aggregated = Vec::new();
        for config_path in &self.config_paths {
            let contents = std::fs::read_to_string(config_path).with_context(|| {
                format!("failed to read strategy config {}", config_path.display())
            })?;
            let def: StrategyConfigFile =
                toml::from_str(&contents).context("failed to parse strategy config file")?;
            let strategy = load_strategy(&def.name, def.params)
                .with_context(|| format!("failed to configure strategy {}", def.name))?;
            let sizer = parse_sizer(&self.sizer, Some(self.quantity))?;
            let order_quantity = self.quantity;
            let symbols = strategy.subscriptions();
            if symbols.is_empty() {
                bail!("strategy {} did not declare subscriptions", strategy.name());
            }
            let stream = match data_format {
                DataFormat::Csv => {
                    let mut candles = load_candles_from_paths(&self.data_paths)?;
                    if candles.is_empty() {
                        bail!("no candles loaded from --data paths");
                    }
                    candles.sort_by_key(|c| c.timestamp);
                    memory_market_stream(&symbols[0], candles)
                }
                DataFormat::Parquet => parquet_market_stream(&symbols, self.data_paths.clone()),
            };
            let execution_client: Arc<dyn ExecutionClient> =
                Arc::new(PaperExecutionClient::default());
            let execution =
                ExecutionEngine::new(execution_client, sizer, Arc::new(NoopRiskChecker));
            let mut cfg = BacktestConfig::new(symbols[0].clone());
            cfg.order_quantity = order_quantity;
            cfg.initial_balances = clone_initial_balances(&config.backtest);
            cfg.reporting_currency = config.backtest.reporting_currency.clone();
            cfg.execution.slippage_bps = self.slippage_bps.max(Decimal::ZERO);
            cfg.execution.fee_bps = self.fee_bps.max(Decimal::ZERO);
            cfg.execution.latency_candles = self.latency_candles.max(1);

            let report = Backtester::new(
                cfg,
                strategy,
                execution,
                None,
                market_registry.clone(),
                Some(stream),
                None,
            )
            .run()
            .await
            .with_context(|| format!("backtest failed for {}", config_path.display()))?;
            aggregated.push(BatchRow {
                config: config_path.display().to_string(),
                signals: 0, // Legacy field, can be removed or calculated from report
                orders: 0,  // Legacy field, can be removed or calculated from report
                dropped_orders: 0, // Legacy field, can be removed or calculated from report
                ending_equity: report.ending_equity,
            });
        }

        if let Some(output) = &self.output {
            write_batch_report(output, &aggregated)?;
            println!("Batch report written to {}", output.display());
        }
        if aggregated.is_empty() {
            return Err(anyhow!("no batch jobs executed"));
        }
        Ok(())
    }
}

impl LiveRunArgs {
    async fn run(&self, config: &AppConfig) -> Result<()> {
        let exchange_cfg = config
            .exchange
            .get(&self.exchange)
            .cloned()
            .ok_or_else(|| anyhow!("exchange profile {} not found", self.exchange))?;

        let driver = exchange_cfg.driver.clone();

        let contents = fs::read_to_string(&self.strategy_config)
            .with_context(|| format!("failed to read {}", self.strategy_config.display()))?;
        let def: StrategyConfigFile =
            toml::from_str(&contents).context("failed to parse strategy config file")?;
        let strategy = load_strategy(&def.name, def.params)
            .with_context(|| format!("failed to configure strategy {}", def.name))?;
        let symbols = strategy.subscriptions();
        if symbols.is_empty() {
            bail!("strategy did not declare any subscriptions");
        }
        if self.quantity <= Decimal::ZERO {
            bail!("--quantity must be greater than zero");
        }
        let quantity = self.quantity;
        let initial_balances = self.resolved_initial_balances(config);
        let reporting_currency = config.backtest.reporting_currency.clone();
        let markets_file = self
            .markets_file
            .clone()
            .or_else(|| config.backtest.markets_file.clone());

        let interval: Interval = self.interval.parse().map_err(|err: String| anyhow!(err))?;
        let category =
            PublicChannel::from_str(&self.category).map_err(|err| anyhow!(err.to_string()))?;
        let metrics_addr = self.resolved_metrics_addr(config)?;
        let control_addr = self.resolved_control_addr(config)?;
        let persistence_cfg = config.live.persistence_config();
        let persistence_engine = self
            .persistence
            .map(PersistenceEngine::from)
            .unwrap_or(persistence_cfg.engine);
        let state_path = self
            .state_path
            .clone()
            .unwrap_or_else(|| persistence_cfg.path.clone());
        let persistence = PersistenceSettings::new(persistence_engine, state_path);
        let alerting = self.build_alerting(config);
        let history = self.history.max(32);
        let reconciliation_interval = self.reconciliation_interval(config);
        let reconciliation_threshold = self.reconciliation_threshold(config);
        let orderbook_depth = self
            .orderbook_depth
            .unwrap_or(super::live::default_order_book_depth());

        let settings = LiveSessionSettings {
            category,
            interval,
            quantity,
            slippage_bps: self.slippage_bps.max(Decimal::ZERO),
            fee_bps: self.fee_bps.max(Decimal::ZERO),
            history,
            metrics_addr,
            persistence,
            initial_balances,
            reporting_currency,
            markets_file,
            alerting,
            exec_backend: self.exec,
            risk: self.build_risk_config(config),
            reconciliation_interval,
            reconciliation_threshold,
            driver,
            orderbook_depth,
            record_path: Some(self.record_data.clone()),
            control_addr,
        };

        info!(
            strategy = %def.name,
            symbols = ?symbols,
            exchange = %self.exchange,
            interval = %self.interval,
            driver = ?settings.driver,
            exec = ?self.exec,
            persistence_engine = ?settings.persistence.engine,
            state_path = %settings.persistence.state_path.display(),
            control_addr = %settings.control_addr,
            "starting live session"
        );

        run_live(strategy, symbols, exchange_cfg, settings)
            .await
            .context("live session failed")
    }
}

fn list_strategies() {
    println!("Built-in strategies:");
    for name in builtin_strategy_names() {
        println!("- {name}");
    }
}

fn print_validation_summary(outcome: &ValidationOutcome) {
    const MAX_EXAMPLES: usize = 5;
    let summary = &outcome.summary;
    println!(
        "Validation summary for {} ({} candles)",
        summary.symbol, summary.rows
    );
    println!(
        "  Range: {} -> {}",
        summary.start.to_rfc3339(),
        summary.end.to_rfc3339()
    );
    println!("  Interval: {}", interval_label(summary.interval));
    println!("  Missing intervals: {}", summary.missing_candles);
    println!("  Duplicate intervals: {}", summary.duplicate_candles);
    println!("  Zero-volume candles: {}", summary.zero_volume_candles);
    println!("  Price spikes flagged: {}", summary.price_spike_count);
    println!(
        "  Cross-source mismatches: {}",
        summary.cross_mismatch_count
    );
    println!("  Repaired candles generated: {}", summary.repaired_candles);

    if !outcome.gaps.is_empty() {
        println!("  Gap examples:");
        for gap in outcome.gaps.iter().take(MAX_EXAMPLES) {
            println!(
                "    {} -> {} (missing {})",
                gap.start.to_rfc3339(),
                gap.end.to_rfc3339(),
                gap.missing
            );
        }
        if outcome.gaps.len() > MAX_EXAMPLES {
            println!(
                "    ... {} additional gap(s) omitted",
                outcome.gaps.len() - MAX_EXAMPLES
            );
        }
    }

    if !outcome.price_spikes.is_empty() {
        println!("  Price spike examples:");
        for spike in outcome.price_spikes.iter().take(MAX_EXAMPLES) {
            println!(
                "    {} (change {:.2}%)",
                spike.timestamp.to_rfc3339(),
                spike.change_fraction * 100.0
            );
        }
        if outcome.price_spikes.len() > MAX_EXAMPLES {
            println!(
                "    ... {} additional spike(s) omitted",
                outcome.price_spikes.len() - MAX_EXAMPLES
            );
        }
    }

    if !outcome.cross_mismatches.is_empty() {
        println!("  Cross-source mismatch examples:");
        for miss in outcome.cross_mismatches.iter().take(MAX_EXAMPLES) {
            println!(
                "    {} primary {:.4} vs ref {:.4} ({:.2}%)",
                miss.timestamp.to_rfc3339(),
                miss.primary_close,
                miss.reference_close,
                miss.delta_fraction * 100.0
            );
        }
        if outcome.cross_mismatches.len() > MAX_EXAMPLES {
            println!(
                "    ... {} additional mismatch(es) omitted",
                outcome.cross_mismatches.len() - MAX_EXAMPLES
            );
        }
    }
}

fn print_report(report: &PerformanceReport) {
    println!("\n{}", report);
}

fn synth_candles(symbol: &str, len: usize, offset_minutes: i64) -> Vec<Candle> {
    let mut candles = Vec::with_capacity(len);
    for i in 0..len {
        let base = 50_000.0 + ((i as f64) + offset_minutes as f64).sin() * 500.0;
        let open = base + (i as f64 % 3.0) * 10.0;
        let close = open + (i as f64 % 5.0) * 5.0 - 10.0;
        let open_dec =
            Decimal::from_f64(open).unwrap_or_else(|| Decimal::from_i64(base as i64).unwrap());
        let close_dec = Decimal::from_f64(close).unwrap_or(open_dec);
        let high = Decimal::from_f64(open.max(close) + 20.0).unwrap_or(open_dec);
        let low = Decimal::from_f64(open.min(close) - 20.0).unwrap_or(close_dec);
        candles.push(Candle {
            symbol: Symbol::from(symbol),
            interval: Interval::OneMinute,
            open: open_dec,
            high,
            low,
            close: close_dec,
            volume: Decimal::ONE,
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
            let open = Decimal::from_f64(row.open).ok_or_else(|| {
                anyhow!("invalid open value '{}' in {}", row.open, path.display())
            })?;
            let high = Decimal::from_f64(row.high).ok_or_else(|| {
                anyhow!("invalid high value '{}' in {}", row.high, path.display())
            })?;
            let low = Decimal::from_f64(row.low)
                .ok_or_else(|| anyhow!("invalid low value '{}' in {}", row.low, path.display()))?;
            let close = Decimal::from_f64(row.close).ok_or_else(|| {
                anyhow!("invalid close value '{}' in {}", row.close, path.display())
            })?;
            let volume = Decimal::from_f64(row.volume).ok_or_else(|| {
                anyhow!(
                    "invalid volume value '{}' in {}",
                    row.volume,
                    path.display()
                )
            })?;
            candles.push(Candle {
                symbol,
                interval,
                open,
                high,
                low,
                close,
                volume,
                timestamp,
            });
        }
    }
    Ok(candles)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DataFormat {
    Csv,
    Parquet,
}

fn detect_data_format(paths: &[PathBuf]) -> Result<DataFormat> {
    let mut detected: Option<DataFormat> = None;
    for path in paths {
        let ext = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase())
            .unwrap_or_else(|| String::from(""));
        let current = if ext == "parquet" {
            DataFormat::Parquet
        } else {
            DataFormat::Csv
        };
        if let Some(existing) = detected {
            if existing != current {
                bail!("cannot mix CSV and Parquet inputs in --data");
            }
        } else {
            detected = Some(current);
        }
    }
    detected.ok_or_else(|| anyhow!("no data paths provided"))
}

fn memory_market_stream(symbol: &str, candles: Vec<Candle>) -> BacktestStream {
    Box::new(PaperMarketStream::from_data(
        symbol.to_string(),
        Vec::new(),
        candles,
    ))
}

fn parquet_market_stream(symbols: &[Symbol], paths: Vec<PathBuf>) -> BacktestStream {
    Box::new(ParquetMarketStream::with_candles(symbols.to_vec(), paths))
}

#[derive(Deserialize)]
#[serde(tag = "event", rename_all = "lowercase")]
enum LobEventRow {
    Snapshot {
        timestamp: String,
        symbol: Option<String>,
        bids: Vec<[f64; 2]>,
        asks: Vec<[f64; 2]>,
    },
    Depth {
        timestamp: String,
        symbol: Option<String>,
        bids: Vec<[f64; 2]>,
        asks: Vec<[f64; 2]>,
    },
    Trade {
        timestamp: String,
        symbol: Option<String>,
        side: String,
        price: f64,
        size: f64,
    },
}

fn load_lob_events_from_paths(paths: &[PathBuf]) -> Result<Vec<MarketEvent>> {
    let mut events = Vec::new();
    for path in paths {
        let file = File::open(path)
            .with_context(|| format!("failed to open order book file {}", path.display()))?;
        let symbol_hint = infer_symbol_from_path(path);
        for line in BufReader::new(file).lines() {
            let line =
                line.with_context(|| format!("failed to read line from {}", path.display()))?;
            if line.trim().is_empty() {
                continue;
            }
            let row: LobEventRow = serde_json::from_str(&line)
                .with_context(|| format!("invalid order book event in {}", path.display()))?;
            match row {
                LobEventRow::Snapshot {
                    timestamp,
                    symbol,
                    bids,
                    asks,
                } => {
                    let ts = parse_datetime(&timestamp)?;
                    let symbol = symbol
                        .or_else(|| symbol_hint.clone())
                        .ok_or_else(|| anyhow!("missing symbol in snapshot {}", path.display()))?;
                    let bids = convert_levels(&bids)?;
                    let asks = convert_levels(&asks)?;
                    let book = OrderBook {
                        symbol: symbol.clone(),
                        bids,
                        asks,
                        timestamp: ts,
                        exchange_checksum: None,
                        local_checksum: None,
                    };
                    events.push(MarketEvent {
                        timestamp: ts,
                        kind: MarketEventKind::OrderBook(book),
                    });
                }
                LobEventRow::Depth {
                    timestamp,
                    symbol,
                    bids,
                    asks,
                } => {
                    let ts = parse_datetime(&timestamp)?;
                    let symbol = symbol.or_else(|| symbol_hint.clone()).ok_or_else(|| {
                        anyhow!("missing symbol in depth update {}", path.display())
                    })?;
                    let bids = convert_levels(&bids)?;
                    let asks = convert_levels(&asks)?;
                    let update = DepthUpdate {
                        symbol: symbol.clone(),
                        bids,
                        asks,
                        timestamp: ts,
                    };
                    events.push(MarketEvent {
                        timestamp: ts,
                        kind: MarketEventKind::Depth(update),
                    });
                }
                LobEventRow::Trade {
                    timestamp,
                    symbol,
                    side,
                    price,
                    size,
                } => {
                    let ts = parse_datetime(&timestamp)?;
                    let symbol = symbol
                        .or_else(|| symbol_hint.clone())
                        .ok_or_else(|| anyhow!("missing symbol in trade {}", path.display()))?;
                    let side = match side.to_lowercase().as_str() {
                        "buy" | "bid" | "b" => Side::Buy,
                        "sell" | "ask" | "s" => Side::Sell,
                        other => bail!("unsupported trade side '{other}' in {}", path.display()),
                    };
                    let price = Decimal::from_f64(price).ok_or_else(|| {
                        anyhow!("invalid trade price '{}' in {}", price, path.display())
                    })?;
                    let size = Decimal::from_f64(size).ok_or_else(|| {
                        anyhow!("invalid trade size '{}' in {}", size, path.display())
                    })?;
                    let tick = Tick {
                        symbol: symbol.clone(),
                        price,
                        size,
                        side,
                        exchange_timestamp: ts,
                        received_at: ts,
                    };
                    events.push(MarketEvent {
                        timestamp: ts,
                        kind: MarketEventKind::Trade(tick),
                    });
                }
            }
        }
    }
    events.sort_by_key(|event| event.timestamp);
    Ok(events)
}

fn convert_levels(levels: &[[f64; 2]]) -> Result<Vec<OrderBookLevel>> {
    levels
        .iter()
        .map(|pair| {
            let price = Decimal::from_f64(pair[0])
                .ok_or_else(|| anyhow!("invalid depth price {}", pair[0]))?;
            let size = Decimal::from_f64(pair[1])
                .ok_or_else(|| anyhow!("invalid depth size {}", pair[1]))?;
            Ok(OrderBookLevel { price, size })
        })
        .collect()
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
            open: candle.open.to_f64().unwrap_or(0.0),
            high: candle.high.to_f64().unwrap_or(0.0),
            low: candle.low.to_f64().unwrap_or(0.0),
            close: candle.close.to_f64().unwrap_or(0.0),
            volume: candle.volume.to_f64().unwrap_or(0.0),
        };
        writer.serialize(row)?;
    }
    writer.flush()?;
    Ok(())
}

#[derive(Serialize)]
struct BatchRow {
    config: String,
    signals: usize,
    orders: usize,
    dropped_orders: usize,
    ending_equity: f64,
}

fn write_batch_report(path: &Path, rows: &[BatchRow]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let mut writer =
        Writer::from_path(path).with_context(|| format!("failed to create {}", path.display()))?;
    for row in rows {
        writer.serialize(row)?;
    }
    writer.flush()?;
    Ok(())
}

fn clone_initial_balances(config: &tesser_config::BacktestConfig) -> HashMap<Symbol, Decimal> {
    config
        .initial_balances
        .iter()
        .map(|(currency, amount)| (currency.clone(), *amount))
        .collect()
}

fn reporting_balance(config: &tesser_config::BacktestConfig) -> Decimal {
    config
        .initial_balances
        .get(&config.reporting_currency)
        .copied()
        .unwrap_or_default()
}

fn parse_sizer(value: &str, cli_quantity: Option<Decimal>) -> Result<Box<dyn OrderSizer>> {
    let parts: Vec<_> = value.split(':').collect();
    match parts.as_slice() {
        ["fixed", val] => {
            let quantity =
                Decimal::from_str(val).context("invalid fixed sizer quantity (use decimals)")?;
            Ok(Box::new(FixedOrderSizer { quantity }))
        }
        ["fixed"] => {
            let quantity = cli_quantity.unwrap_or(Decimal::ONE);
            Ok(Box::new(FixedOrderSizer { quantity }))
        }
        ["percent", val] => {
            let percent =
                Decimal::from_str(val).context("invalid percent sizer value (use decimals)")?;
            Ok(Box::new(PortfolioPercentSizer {
                percent: percent.max(Decimal::ZERO),
            }))
        }
        ["risk-adjusted", val] => {
            let risk_fraction = Decimal::from_str(val)
                .context("invalid risk fraction value (use decimals)")?;
            Ok(Box::new(RiskAdjustedSizer {
                risk_fraction: risk_fraction.max(Decimal::ZERO),
            }))
        }
        _ => Err(anyhow!(
            "invalid sizer format, expected 'fixed:value', 'percent:value', or 'risk-adjusted:value'"
        )),
    }
}
