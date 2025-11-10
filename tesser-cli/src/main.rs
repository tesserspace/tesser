mod alerts;
mod data_validation;
mod live;
mod state;
mod telemetry;

use crate::alerts::sanitize_webhook;
use crate::data_validation::{validate_dataset, ValidationConfig, ValidationOutcome};
use crate::live::{run_live, ExecutionBackend, LiveSessionSettings};
use crate::telemetry::init_tracing;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use clap::{Args, Parser, Subcommand};
use csv::Writer;
use serde::{Deserialize, Serialize};
use tesser_backtester::{BacktestConfig, BacktestReport, Backtester};
use tesser_broker::ExecutionClient;
use tesser_bybit::PublicChannel;
use tesser_config::{load_config, AppConfig};
use tesser_core::{Candle, Interval, Symbol};
use tesser_data::download::{BybitDownloader, KlineRequest};
use tesser_execution::{ExecutionEngine, FixedOrderSizer, NoopRiskChecker};
use tesser_paper::PaperExecutionClient;
use tesser_strategy::{build_builtin_strategy, builtin_strategy_names};
use tracing::{info, warn};

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
    /// Inspect or repair persisted runtime state
    State {
        #[command(subcommand)]
        action: StateCommand,
    },
    /// Strategy management helpers
    Strategies,
}

#[derive(Subcommand)]
enum DataCommand {
    /// Download historical market data
    Download(DataDownloadArgs),
    /// Validate and optionally repair a local data set
    Validate(DataValidateArgs),
    /// Resample existing data (placeholder)
    Resample(DataResampleArgs),
}

#[derive(Subcommand)]
enum BacktestCommand {
    /// Run a single backtest from a strategy config file
    Run(BacktestRunArgs),
    /// Run multiple strategy configs and aggregate the results
    Batch(BacktestBatchArgs),
}

#[derive(Subcommand)]
enum LiveCommand {
    /// Start a live trading session (scaffolding)
    Run(LiveRunArgs),
}

#[derive(Subcommand)]
enum StateCommand {
    /// Inspect the SQLite state database
    Inspect(StateInspectArgs),
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
struct StateInspectArgs {
    /// Path to the SQLite state database (defaults to live.state_path)
    #[arg(long)]
    path: Option<PathBuf>,
    /// Emit the raw JSON payload stored inside the database
    #[arg(long)]
    raw: bool,
}

impl StateInspectArgs {
    fn resolved_path(&self, config: &AppConfig) -> PathBuf {
        self.path
            .clone()
            .unwrap_or_else(|| config.live.state_path.clone())
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

        let downloader = BybitDownloader::new(&exchange_cfg.rest_url);
        let request = KlineRequest::new(&self.category, &self.symbol, interval, start, end);
        info!(
            "Downloading {} candles for {} ({})",
            self.interval, self.symbol, self.exchange
        );
        let mut candles = downloader
            .download_klines(&request)
            .await
            .with_context(|| "failed to download candles from Bybit")?;

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
struct DataValidateArgs {
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
    /// Symmetric slippage in basis points (1 bp = 0.01%) applied to fills
    #[arg(long, default_value_t = 0.0)]
    slippage_bps: f64,
    /// Trading fees in basis points applied to notional
    #[arg(long, default_value_t = 0.0)]
    fee_bps: f64,
    /// Number of candles between signal and execution
    #[arg(long, default_value_t = 1)]
    latency_candles: usize,
}

#[derive(Args)]
struct BacktestBatchArgs {
    /// Glob or directory containing strategy config files
    #[arg(long = "config", value_name = "PATH", num_args = 1.., action = clap::ArgAction::Append)]
    config_paths: Vec<PathBuf>,
    /// Candle CSVs available to every strategy
    #[arg(long = "data", value_name = "PATH", num_args = 1.., action = clap::ArgAction::Append)]
    data_paths: Vec<PathBuf>,
    #[arg(long, default_value_t = 0.01)]
    quantity: f64,
    /// Optional output CSV summarizing results
    #[arg(long)]
    output: Option<PathBuf>,
    /// Symmetric slippage in basis points (1 bp = 0.01%) applied to fills
    #[arg(long, default_value_t = 0.0)]
    slippage_bps: f64,
    /// Trading fees in basis points applied to notional
    #[arg(long, default_value_t = 0.0)]
    fee_bps: f64,
    /// Number of candles between signal and execution
    #[arg(long, default_value_t = 1)]
    latency_candles: usize,
}

#[derive(Args)]
struct LiveRunArgs {
    #[arg(long)]
    strategy_config: PathBuf,
    #[arg(long, default_value = "bybit_testnet")]
    exchange: String,
    #[arg(long, default_value = "linear")]
    category: String,
    #[arg(long, default_value = "1m")]
    interval: String,
    #[arg(long, default_value_t = 1.0)]
    quantity: f64,
    /// Selects which execution backend to use (`paper` or `bybit`)
    #[arg(
        long = "exec",
        default_value = "paper",
        value_enum,
        alias = "live-exec"
    )]
    exec: ExecutionBackend,
    #[arg(long)]
    state_path: Option<PathBuf>,
    #[arg(long)]
    metrics_addr: Option<String>,
    #[arg(long)]
    log_path: Option<PathBuf>,
    #[arg(long, default_value_t = 0.0)]
    slippage_bps: f64,
    #[arg(long, default_value_t = 0.0)]
    fee_bps: f64,
    #[arg(long, default_value_t = 0)]
    latency_ms: u64,
    #[arg(long, default_value_t = 512)]
    history: usize,
    #[arg(long)]
    webhook_url: Option<String>,
}

impl LiveRunArgs {
    fn resolved_log_path(&self, config: &AppConfig) -> PathBuf {
        self.log_path
            .clone()
            .unwrap_or_else(|| config.live.log_path.clone())
    }

    fn resolved_state_path(&self, config: &AppConfig) -> PathBuf {
        self.state_path
            .clone()
            .unwrap_or_else(|| config.live.state_path.clone())
    }

    fn resolved_metrics_addr(&self, config: &AppConfig) -> Result<SocketAddr> {
        let addr = self
            .metrics_addr
            .clone()
            .unwrap_or_else(|| config.live.metrics_addr.clone());
        addr.parse()
            .with_context(|| format!("invalid metrics address '{addr}'"))
    }

    fn build_alerting(&self, config: &AppConfig) -> tesser_config::AlertingConfig {
        let mut alerting = config.live.alerting.clone();
        let webhook = self
            .webhook_url
            .clone()
            .or_else(|| alerting.webhook_url.clone());
        alerting.webhook_url = sanitize_webhook(webhook);
        alerting
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

#[tokio::main]
async fn main() -> Result<()> {
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
    }
    Ok(())
}

async fn handle_state(cmd: StateCommand, config: &AppConfig) -> Result<()> {
    match cmd {
        StateCommand::Inspect(args) => {
            state::inspect_state(args.resolved_path(config), args.raw).await?;
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

        let execution_client: Arc<dyn ExecutionClient> = Arc::new(PaperExecutionClient::default());
        let execution = ExecutionEngine::new(
            execution_client,
            Box::new(FixedOrderSizer {
                quantity: self.quantity,
            }),
            Arc::new(NoopRiskChecker),
        );

        let mut cfg = BacktestConfig::new(symbols[0].clone(), candles);
        cfg.order_quantity = self.quantity;
        cfg.execution.slippage_bps = self.slippage_bps.max(0.0);
        cfg.execution.fee_bps = self.fee_bps.max(0.0);
        cfg.execution.latency_candles = self.latency_candles.max(1);

        let report = Backtester::new(cfg, strategy, execution)
            .run()
            .await
            .context("backtest failed")?;
        print_report(report);
        Ok(())
    }
}

impl BacktestBatchArgs {
    async fn run(&self, _config: &AppConfig) -> Result<()> {
        if self.config_paths.is_empty() {
            return Err(anyhow!("provide at least one --config path"));
        }
        if self.data_paths.is_empty() {
            return Err(anyhow!("provide at least one --data path for batch mode"));
        }
        let mut aggregated = Vec::new();
        for config_path in &self.config_paths {
            let contents = std::fs::read_to_string(config_path).with_context(|| {
                format!("failed to read strategy config {}", config_path.display())
            })?;
            let def: StrategyConfigFile =
                toml::from_str(&contents).context("failed to parse strategy config file")?;
            let strategy = build_builtin_strategy(&def.name, def.params)
                .with_context(|| format!("failed to configure strategy {}", def.name))?;
            let mut candles = load_candles_from_paths(&self.data_paths)?;
            candles.sort_by_key(|c| c.timestamp);
            let execution_client: Arc<dyn ExecutionClient> =
                Arc::new(PaperExecutionClient::default());
            let execution = ExecutionEngine::new(
                execution_client,
                Box::new(FixedOrderSizer {
                    quantity: self.quantity,
                }),
                Arc::new(NoopRiskChecker),
            );
            let mut cfg = BacktestConfig::new(strategy.symbol().to_string(), candles);
            cfg.order_quantity = self.quantity;
            cfg.execution.slippage_bps = self.slippage_bps.max(0.0);
            cfg.execution.fee_bps = self.fee_bps.max(0.0);
            cfg.execution.latency_candles = self.latency_candles.max(1);

            let report = Backtester::new(cfg, strategy, execution)
                .run()
                .await
                .with_context(|| format!("backtest failed for {}", config_path.display()))?;
            println!(
                "[batch] {} -> signals {}, orders {}, dropped {}, ending equity {:.2}",
                config_path.display(),
                report.signals_emitted,
                report.orders_sent,
                report.dropped_orders,
                report.ending_equity
            );
            aggregated.push(BatchRow {
                config: config_path.display().to_string(),
                signals: report.signals_emitted,
                orders: report.orders_sent,
                dropped_orders: report.dropped_orders,
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

        let contents = fs::read_to_string(&self.strategy_config)
            .with_context(|| format!("failed to read {}", self.strategy_config.display()))?;
        let def: StrategyConfigFile =
            toml::from_str(&contents).context("failed to parse strategy config file")?;
        let strategy = build_builtin_strategy(&def.name, def.params)
            .with_context(|| format!("failed to configure strategy {}", def.name))?;
        let symbols = strategy.subscriptions();
        if symbols.is_empty() {
            bail!("strategy did not declare any subscriptions");
        }
        if self.quantity <= 0.0 {
            bail!("--quantity must be greater than zero");
        }

        let interval: Interval = self.interval.parse().map_err(|err: String| anyhow!(err))?;
        let category =
            PublicChannel::from_str(&self.category).map_err(|err| anyhow!(err.to_string()))?;
        let metrics_addr = self.resolved_metrics_addr(config)?;
        let state_path = self.resolved_state_path(config);
        let alerting = self.build_alerting(config);
        let history = self.history.max(32);

        let settings = LiveSessionSettings {
            category,
            interval,
            quantity: self.quantity,
            slippage_bps: self.slippage_bps,
            fee_bps: self.fee_bps,
            latency_ms: self.latency_ms,
            history,
            metrics_addr,
            state_path,
            initial_equity: config.backtest.initial_equity,
            alerting,
            exec_backend: self.exec,
            risk: config.risk_management.clone(),
        };

        info!(
            strategy = %def.name,
            symbols = ?symbols,
            exchange = %self.exchange,
            interval = %self.interval,
            exec = ?self.exec,
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

fn print_report(report: BacktestReport) {
    println!("Backtest completed:");
    println!("  Signals generated: {}", report.signals_emitted);
    println!("  Orders sent: {}", report.orders_sent);
    println!("  Orders dropped: {}", report.dropped_orders);
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
