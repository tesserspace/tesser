use crate::alerts::sanitize_webhook;
use crate::analyze;
use crate::data_validation::{validate_dataset, ValidationConfig, ValidationOutcome};
use crate::live::{
    run_live, ExecutionBackend, LiveSessionSettings, NamedExchange, PersistenceBackend,
    PersistenceSettings,
};
use crate::state;
use crate::telemetry::init_tracing;
use crate::tui;
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
use chrono::{DateTime, Days, Duration, NaiveDate, NaiveDateTime, Utc};
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
use tesser_broker::{ExecutionClient, RouterExecutionClient};
use tesser_config::{load_config, AppConfig, PersistenceEngine, RiskManagementConfig};
use tesser_core::{
    AssetId, Candle, DepthUpdate, ExchangeId, Interval, OrderBook, OrderBookLevel, Side, Symbol,
    Tick,
};
use tesser_data::analytics::ExecutionAnalysisRequest;
use tesser_data::download::{
    BinanceDownloader, BybitDownloader, KlineRequest, NormalizedTrade, TradeRequest, TradeSource,
};
use tesser_data::etl::{
    MappingConfig as EtlMappingConfig, Partitioning as EtlPartitioning, Pipeline as EtlPipeline,
};
use tesser_data::io::{self, DatasetFormat as IoDatasetFormat, TicksWriter};
use tesser_data::merger::UnifiedEventStream;
use tesser_data::parquet::ParquetMarketStream;
use tesser_data::transform::Resampler;
use tesser_execution::{
    ExecutionEngine, FixedOrderSizer, NoopRiskChecker, OrderSizer, PanicCloseConfig,
    PanicCloseMode, PortfolioPercentSizer, RiskAdjustedSizer,
};
use tesser_markets::MarketRegistry;
use tesser_paper::{
    FeeModel, FeeScheduleConfig, MatchingEngine, MatchingEngineConfig, PaperExecutionClient,
    PaperMarketStream, QueueModel,
};
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
    /// Launch the real-time TUI dashboard
    Monitor(MonitorArgs),
}

#[derive(Subcommand)]
pub enum DataCommand {
    /// Download historical market data
    Download(DataDownloadArgs),
    /// Download historical trade ticks
    DownloadTrades(DataDownloadTradesArgs),
    /// Validate and optionally repair a local data set
    Validate(DataValidateArgs),
    /// Resample existing data (placeholder)
    Resample(DataResampleArgs),
    /// Inspect a parquet file emitted by the flight recorder
    InspectParquet(DataInspectParquetArgs),
    /// Normalize raw data into the canonical schema
    Normalize(DataNormalizeArgs),
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
pub struct DataDownloadTradesArgs {
    #[arg(long, default_value = "bybit")]
    exchange: String,
    #[arg(long)]
    symbol: String,
    /// Bybit market category (e.g., linear, inverse, option, spot)
    #[arg(long, default_value = "linear")]
    category: String,
    #[arg(long)]
    start: String,
    #[arg(long)]
    end: Option<String>,
    #[arg(long)]
    output: Option<PathBuf>,
    /// Partition parquet output by trading day
    #[arg(long)]
    partition_by_day: bool,
    /// Per-request REST chunk size (max 1000)
    #[arg(long, default_value_t = 1000)]
    limit: usize,
    /// Trade data source (Bybit only)
    #[arg(long, value_enum, default_value_t = TradeSourceArg::Rest)]
    source: TradeSourceArg,
    /// Skip partitions that already exist
    #[arg(long)]
    resume: bool,
    /// Override Bybit public archive base URL
    #[arg(long)]
    bybit_public_url: Option<String>,
    /// Binance public archive market (spot/futures)
    #[arg(long, value_enum, default_value_t = BinanceMarketArg::FuturesUm)]
    binance_market: BinanceMarketArg,
}

#[derive(Args)]
pub struct DataNormalizeArgs {
    /// Glob pointing at the raw input files (e.g. ./raw/binance/*.csv)
    #[arg(long)]
    pub source: String,
    /// Output directory for canonical parquet partitions
    #[arg(long)]
    pub output: PathBuf,
    /// Path to the mapping configuration TOML file
    #[arg(long)]
    pub config: PathBuf,
    /// Symbol identifier recorded in the canonical set (e.g. binance:BTCUSDT)
    #[arg(long)]
    pub symbol: String,
    /// Partitioning strategy (daily or monthly)
    #[arg(long, value_enum, default_value = "daily")]
    pub partition: DataPartitionArg,
    /// Override the canonical interval label defined in the mapping config
    #[arg(long)]
    pub interval: Option<String>,
}

#[derive(Copy, Clone, Eq, PartialEq, ValueEnum)]
pub enum DataPartitionArg {
    Daily,
    Monthly,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum TradeSourceArg {
    Rest,
    #[value(name = "bybit-public")]
    BybitPublic,
    #[value(name = "binance-public")]
    BinancePublic,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum BinanceMarketArg {
    #[value(name = "spot")]
    Spot,
    #[value(name = "futures-um")]
    FuturesUm,
    #[value(name = "futures-cm")]
    FuturesCm,
}

impl BinanceMarketArg {
    fn base_path(self) -> &'static str {
        match self {
            Self::Spot => "https://data.binance.vision/data/spot/daily/aggTrades",
            Self::FuturesUm => "https://data.binance.vision/data/futures/um/daily/aggTrades",
            Self::FuturesCm => "https://data.binance.vision/data/futures/cm/daily/aggTrades",
        }
    }
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
    /// Optional CSV file path for exporting ExecutionStats
    #[arg(long, value_name = "PATH")]
    export_csv: Option<PathBuf>,
}

#[derive(Args)]
pub struct MonitorArgs {
    /// Control plane address (overrides config.live.control_addr)
    #[arg(long)]
    control_addr: Option<String>,
    /// UI refresh rate in milliseconds
    #[arg(long, default_value_t = 250)]
    tick_rate: u64,
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

impl MonitorArgs {
    async fn run(&self, config: &AppConfig) -> Result<()> {
        let addr = self
            .control_addr
            .clone()
            .unwrap_or_else(|| config.live.control_addr.clone());
        let refresh = self.tick_rate.max(50);
        let monitor_config = tui::MonitorConfig::new(addr, StdDuration::from_millis(refresh));
        tui::run_monitor(monitor_config).await
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

impl DataDownloadTradesArgs {
    async fn run(&self, config: &AppConfig) -> Result<()> {
        let exchange_cfg = config
            .exchange
            .get(&self.exchange)
            .ok_or_else(|| anyhow!("exchange profile '{}' not found in config", self.exchange))?;
        let start = parse_datetime(&self.start)?;
        let end = match &self.end {
            Some(value) => parse_datetime(value)?,
            None => Utc::now(),
        };
        if start >= end {
            return Err(anyhow!("start time must be earlier than end time"));
        }

        info!(
            "Downloading trades for {} on {} ({} -> {})",
            self.symbol, self.exchange, start, end
        );

        let cache_dir = config
            .data_path
            .join("ticks")
            .join("cache")
            .join(&self.exchange)
            .join(&self.symbol);
        let mut base_request = TradeRequest::new(&self.symbol, start, end)
            .with_limit(self.limit)
            .with_archive_cache_dir(cache_dir)
            .with_resume_archives(self.resume);
        let driver = exchange_cfg.driver.as_str();
        match driver {
            "bybit" | "" => {
                if self.source == TradeSourceArg::BinancePublic {
                    bail!("Binance public archives are not available for Bybit");
                }
                base_request = base_request.with_category(&self.category);
                if self.source == TradeSourceArg::BybitPublic {
                    base_request = base_request.with_source(TradeSource::BybitPublicArchive);
                    if let Some(url) = &self.bybit_public_url {
                        base_request = base_request.with_public_data_url(url);
                    }
                }
            }
            "binance" => match self.source {
                TradeSourceArg::Rest => {}
                TradeSourceArg::BinancePublic => {
                    base_request = base_request
                        .with_source(TradeSource::BinancePublicArchive)
                        .with_public_data_url(self.binance_market.base_path());
                }
                TradeSourceArg::BybitPublic => {
                    bail!("Bybit public archives are not available for Binance");
                }
            },
            other => bail!("unknown exchange driver '{other}' for {}", self.exchange),
        }

        if self.partition_by_day {
            self.download_partitioned(config, exchange_cfg, base_request, start, end)
                .await
        } else {
            let trades = self
                .fetch_trades(driver, &exchange_cfg.rest_url, base_request)
                .await?;
            if trades.is_empty() {
                info!("No trades returned for {}", self.symbol);
                return Ok(());
            }
            self.write_single(config, trades, start, end)
        }
    }

    async fn download_partitioned(
        &self,
        config: &AppConfig,
        exchange_cfg: &tesser_config::ExchangeConfig,
        base_request: TradeRequest<'_>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<()> {
        let base_dir = self
            .output
            .clone()
            .unwrap_or_else(|| default_tick_partition_dir(config, &self.exchange, &self.symbol));
        let mut current = start.date_naive();
        let final_date = end.date_naive();
        let mut total = 0usize;
        let mut files_written = 0usize;

        while current <= final_date {
            let next_date = current.checked_add_days(Days::new(1)).unwrap_or(current);
            let day_start = DateTime::<Utc>::from_naive_utc_and_offset(
                current
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow!("invalid date {}", current))?,
                Utc,
            )
            .max(start);
            let day_end = DateTime::<Utc>::from_naive_utc_and_offset(
                next_date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow!("invalid date {}", next_date))?,
                Utc,
            )
            .min(end);

            if day_start >= day_end {
                if next_date == current {
                    break;
                }
                current = next_date;
                continue;
            }

            let partition_path = partition_path(&base_dir, current);
            if self.resume && partition_path.exists() {
                info!(
                    "Skipping {} because {} already exists",
                    current,
                    partition_path.display()
                );
                if next_date == current {
                    break;
                }
                current = next_date;
                continue;
            }

            let mut day_request = base_request.clone();
            day_request.start = day_start;
            day_request.end = day_end;
            let trades = self
                .fetch_trades(
                    exchange_cfg.driver.as_str(),
                    &exchange_cfg.rest_url,
                    day_request,
                )
                .await?;

            if trades.is_empty() {
                info!("No trades returned for {}", current);
                if next_date == current {
                    break;
                }
                current = next_date;
                continue;
            }

            let mut writer = TicksWriter::new(&partition_path);
            let count = trades.len();
            writer.extend(trades.into_iter().map(|trade| (trade.trade_id, trade.tick)));
            writer.finish()?;
            total += count;
            files_written += 1;
            info!(
                "Saved {} raw trades for {} to {}",
                count,
                current,
                partition_path.display()
            );

            if next_date == current {
                break;
            }
            current = next_date;
        }

        info!(
            "Partitioned {} raw trades across {} file(s) under {}",
            total,
            files_written,
            base_dir.display()
        );
        Ok(())
    }

    async fn fetch_trades(
        &self,
        driver: &str,
        rest_url: &str,
        request: TradeRequest<'_>,
    ) -> Result<Vec<NormalizedTrade>> {
        match driver {
            "bybit" | "" => {
                let downloader = BybitDownloader::new(rest_url);
                downloader
                    .download_trades(&request)
                    .await
                    .with_context(|| "failed to download trades from Bybit")
            }
            "binance" => {
                let downloader = BinanceDownloader::new(rest_url);
                downloader
                    .download_trades(&request)
                    .await
                    .with_context(|| "failed to download trades from Binance")
            }
            other => bail!("unknown exchange driver '{other}' for {}", self.exchange),
        }
    }

    fn write_single(
        &self,
        config: &AppConfig,
        trades: Vec<NormalizedTrade>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<()> {
        let output_path = self.output.clone().unwrap_or_else(|| {
            default_tick_output_path(config, &self.exchange, &self.symbol, start, end)
        });
        let total = trades.len();
        let mut writer = TicksWriter::new(&output_path);
        writer.extend(trades.into_iter().map(|trade| (trade.trade_id, trade.tick)));
        writer.finish()?;
        info!("Saved {} raw trades to {}", total, output_path.display());
        Ok(())
    }
}

impl DataNormalizeArgs {
    fn run(&self) -> Result<()> {
        let raw = fs::read_to_string(&self.config)
            .with_context(|| format!("failed to read {}", self.config.display()))?;
        let mut mapping: EtlMappingConfig = toml::from_str(&raw)
            .with_context(|| format!("failed to parse mapping config {}", self.config.display()))?;
        if let Some(interval) = &self.interval {
            mapping.interval = interval.clone();
        }
        let pipeline = EtlPipeline::new(mapping);
        let rows = pipeline.run(
            &self.source,
            &self.output,
            &self.symbol,
            self.partition.into(),
        )?;
        info!(rows, output = %self.output.display(), "normalized data written");
        Ok(())
    }
}

impl From<DataPartitionArg> for EtlPartitioning {
    fn from(value: DataPartitionArg) -> Self {
        match value {
            DataPartitionArg::Daily => EtlPartitioning::Daily,
            DataPartitionArg::Monthly => EtlPartitioning::Monthly,
        }
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

impl DataResampleArgs {
    fn run(&self) -> Result<()> {
        if !self.input.exists() {
            bail!("input file {} not found", self.input.display());
        }
        let interval: Interval = self.interval.parse().map_err(|err: String| anyhow!(err))?;
        let dataset = io::read_dataset(&self.input)
            .with_context(|| format!("failed to read {}", self.input.display()))?;
        let io::CandleDataset {
            format: input_format,
            candles,
        } = dataset;
        if candles.is_empty() {
            bail!(
                "dataset {} does not contain any candles",
                self.input.display()
            );
        }
        let input_len = candles.len();
        let resampled = Resampler::resample(candles, interval);
        if resampled.is_empty() {
            bail!(
                "no candles produced after resampling {}",
                self.input.display()
            );
        }
        let output_format = self
            .format
            .map(IoDatasetFormat::from)
            .unwrap_or_else(|| IoDatasetFormat::from_path(&self.output));
        io::write_dataset(&self.output, output_format, &resampled)
            .with_context(|| format!("failed to write {}", self.output.display()))?;
        info!(
            "Resampled {} -> {} candles (input {:?} -> output {:?}) to {}",
            input_len,
            resampled.len(),
            input_format,
            output_format,
            self.output.display()
        );
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
    /// Force the output format (defaults to --output extension)
    #[arg(long, value_enum)]
    format: Option<DatasetFormatArg>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum DatasetFormatArg {
    Csv,
    Parquet,
}

impl From<DatasetFormatArg> for IoDatasetFormat {
    fn from(value: DatasetFormatArg) -> Self {
        match value {
            DatasetFormatArg::Csv => IoDatasetFormat::Csv,
            DatasetFormatArg::Parquet => IoDatasetFormat::Parquet,
        }
    }
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum QueueModelArg {
    Conserv,
    Optimistic,
}

impl From<QueueModelArg> for QueueModel {
    fn from(value: QueueModelArg) -> Self {
        match value {
            QueueModelArg::Conserv => QueueModel::Conservative,
            QueueModelArg::Optimistic => QueueModel::Optimistic,
        }
    }
}

#[derive(Args)]
pub struct BacktestRunArgs {
    #[arg(long)]
    strategy_config: PathBuf,
    /// One or more canonical parquet files produced via `tesser-cli data normalize`
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
    /// Optional fee schedule describing maker/taker rates
    #[arg(long = "fee-schedule")]
    fee_schedule: Option<PathBuf>,
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
    /// Round-trip latency, in milliseconds, applied to limit order placements/cancellations during tick-mode sims
    #[arg(long = "sim-latency-ms", default_value_t = 0)]
    sim_latency_ms: u64,
    /// Queue modeling assumption used when simulating passive fills
    #[arg(long = "sim-queue-model", value_enum, default_value = "conserv")]
    sim_queue_model: QueueModelArg,
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
    /// Canonical parquet paths available to every strategy
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
    /// Optional fee schedule describing maker/taker rates
    #[arg(long = "fee-schedule")]
    fee_schedule: Option<PathBuf>,
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
    /// Optional list of additional exchange profiles to load (comma separated or repeated)
    ///
    /// Each entry must match a `[exchange.NAME]` table or a `[[exchanges]]` block in the selected config.
    #[arg(long = "exchanges", value_name = "NAME", num_args = 0.., action = clap::ArgAction::Append)]
    exchanges: Vec<String>,
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
    risk_max_order_notional: Option<Decimal>,
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
    /// Panic-close mode used when multi-leg groups desync
    #[arg(long, value_enum, default_value = "market")]
    panic_mode: PanicModeArg,
    /// Basis points added/subtracted from the last price when using `panic_mode=aggressive-limit`
    #[arg(long, default_value = "50")]
    panic_limit_offset_bps: Decimal,
    /// Directory containing compiled WASM execution plugins.
    #[arg(long = "plugins-dir")]
    plugins_dir: Option<PathBuf>,
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

    fn resolved_initial_balances(&self, config: &AppConfig) -> HashMap<AssetId, Decimal> {
        let mut balances = clone_initial_balances(&config.backtest);
        if let Some(value) = self.initial_equity {
            let reporting = AssetId::from(config.backtest.reporting_currency.as_str());
            balances.insert(reporting, value.max(Decimal::ZERO));
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
        if let Some(limit) = self.risk_max_order_notional {
            risk.max_order_notional = (limit > Decimal::ZERO).then_some(limit);
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
        Commands::Monitor(args) => args.run(&config).await?,
    }

    Ok(())
}

async fn handle_data(cmd: DataCommand, config: &AppConfig) -> Result<()> {
    match cmd {
        DataCommand::Download(args) => {
            args.run(config).await?;
        }
        DataCommand::DownloadTrades(args) => {
            args.run(config).await?;
        }
        DataCommand::Validate(args) => {
            args.run()?;
        }
        DataCommand::Resample(args) => {
            args.run()?;
        }
        DataCommand::InspectParquet(args) => {
            args.run()?;
        }
        DataCommand::Normalize(args) => {
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
        AnalyzeCommand::Execution(args) => {
            analyze::run_execution(args.build_request()?, args.export_csv.as_deref())
        }
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
        let fee_schedule = self.resolve_fee_schedule()?;
        let fee_model_template = fee_schedule.build_model();
        let reporting_currency = AssetId::from(config.backtest.reporting_currency.as_str());
        let initial_balances = clone_initial_balances(&config.backtest);

        let (market_stream, event_stream, execution_client, matching_engine) = match mode {
            BacktestMode::Candle => {
                let stream = self.build_candle_stream(&symbols)?;
                let exec_client = build_sim_execution_client(
                    "paper-backtest",
                    &symbols,
                    self.slippage_bps,
                    fee_model_template.clone(),
                    &initial_balances,
                    reporting_currency,
                )
                .await;
                (Some(stream), None, exec_client, None)
            }
            BacktestMode::Tick => {
                if self.lob_paths.is_empty() {
                    bail!("--lob-data is required when --mode tick");
                }
                let source = self.detect_lob_source()?;
                let latency_ms = self.sim_latency_ms.min(i64::MAX as u64);
                let fee_model = fee_model_template.clone();
                let execution_client = build_sim_execution_client(
                    "paper-tick",
                    &symbols,
                    self.slippage_bps,
                    fee_model_template.clone(),
                    &initial_balances,
                    reporting_currency,
                )
                .await;
                let engine = Arc::new(MatchingEngine::with_config(
                    "matching-engine",
                    symbols.clone(),
                    reporting_balance(&config.backtest),
                    MatchingEngineConfig {
                        latency: Duration::milliseconds(latency_ms as i64),
                        queue_model: self.sim_queue_model.into(),
                        fee_model: fee_model.clone(),
                        cash_asset: Some(reporting_currency),
                    },
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
                (None, Some(stream), execution_client, Some(engine))
            }
        };

        let sizer = parse_sizer(&self.sizer, Some(self.quantity))?;
        let order_quantity = self.quantity;
        let execution = ExecutionEngine::new(execution_client, sizer, Arc::new(NoopRiskChecker));

        let mut cfg = BacktestConfig::new(symbols[0]);
        cfg.order_quantity = order_quantity;
        cfg.initial_balances = initial_balances.clone();
        cfg.reporting_currency = reporting_currency;
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

    fn resolve_fee_schedule(&self) -> Result<FeeScheduleConfig> {
        if let Some(path) = &self.fee_schedule {
            load_fee_schedule_file(path)
        } else {
            Ok(FeeScheduleConfig::with_defaults(
                self.fee_bps.max(Decimal::ZERO),
                self.fee_bps.max(Decimal::ZERO),
            ))
        }
    }

    fn build_candle_stream(&self, symbols: &[Symbol]) -> Result<BacktestStream> {
        if symbols.is_empty() {
            bail!("strategy did not declare any subscriptions");
        }
        if self.data_paths.is_empty() {
            let mut generated = Vec::new();
            for (idx, symbol) in symbols.iter().enumerate() {
                let offset = idx as i64 * 10;
                generated.extend(synth_candles(*symbol, self.candles, offset));
            }
            generated.sort_by_key(|c| c.timestamp);
            if generated.is_empty() {
                bail!("no synthetic candles generated; provide --data files instead");
            }
            return Ok(memory_market_stream(symbols[0], generated));
        }

        ensure_parquet_inputs(&self.data_paths)?;
        Ok(parquet_market_stream(symbols, self.data_paths.clone()))
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

fn load_fee_schedule_file(path: &Path) -> Result<FeeScheduleConfig> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read fee schedule {}", path.display()))?;
    let cfg = match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) if ext.eq_ignore_ascii_case("json") => serde_json::from_str(&contents)
            .with_context(|| format!("failed to parse JSON fee schedule {}", path.display()))?,
        _ => toml::from_str(&contents)
            .with_context(|| format!("failed to parse TOML fee schedule {}", path.display()))?,
    };
    Ok(cfg)
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
        let mut aggregated = Vec::new();
        let fee_schedule = if let Some(path) = &self.fee_schedule {
            load_fee_schedule_file(path)?
        } else {
            FeeScheduleConfig::with_defaults(
                self.fee_bps.max(Decimal::ZERO),
                self.fee_bps.max(Decimal::ZERO),
            )
        };
        let reporting_currency = AssetId::from(config.backtest.reporting_currency.as_str());
        let initial_balances = clone_initial_balances(&config.backtest);
        ensure_parquet_inputs(&self.data_paths)?;
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
            let stream = parquet_market_stream(&symbols, self.data_paths.clone());
            let execution_client = build_sim_execution_client(
                &format!("paper-batch-{}", def.name),
                &symbols,
                self.slippage_bps,
                fee_schedule.build_model(),
                &initial_balances,
                reporting_currency,
            )
            .await;
            let execution =
                ExecutionEngine::new(execution_client, sizer, Arc::new(NoopRiskChecker));
            let mut cfg = BacktestConfig::new(symbols[0]);
            cfg.order_quantity = order_quantity;
            cfg.initial_balances = initial_balances.clone();
            cfg.reporting_currency = reporting_currency;
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

async fn build_sim_execution_client(
    name_prefix: &str,
    symbols: &[Symbol],
    slippage_bps: Decimal,
    fee_model: Arc<dyn FeeModel>,
    initial_balances: &HashMap<AssetId, Decimal>,
    reporting_currency: AssetId,
) -> Arc<dyn ExecutionClient> {
    let mut grouped: HashMap<ExchangeId, Vec<Symbol>> = HashMap::new();
    for symbol in symbols {
        grouped.entry(symbol.exchange).or_default().push(*symbol);
    }
    if grouped.len() <= 1 {
        let (exchange, group) = grouped
            .into_iter()
            .next()
            .unwrap_or((ExchangeId::UNSPECIFIED, symbols.to_vec()));
        return build_paper_client_for_exchange(
            name_prefix,
            exchange,
            group,
            slippage_bps,
            fee_model,
            initial_balances,
            reporting_currency,
        )
        .await;
    }
    let mut routes = HashMap::new();
    for (exchange, group) in grouped {
        let client = build_paper_client_for_exchange(
            name_prefix,
            exchange,
            group,
            slippage_bps,
            fee_model.clone(),
            initial_balances,
            reporting_currency,
        )
        .await;
        routes.insert(exchange, client);
    }
    Arc::new(RouterExecutionClient::new(routes))
}

async fn build_paper_client_for_exchange(
    name_prefix: &str,
    exchange: ExchangeId,
    symbols: Vec<Symbol>,
    slippage_bps: Decimal,
    fee_model: Arc<dyn FeeModel>,
    initial_balances: &HashMap<AssetId, Decimal>,
    reporting_currency: AssetId,
) -> Arc<dyn ExecutionClient> {
    let cash_asset = cash_asset_for_exchange(reporting_currency, exchange);
    let client = Arc::new(PaperExecutionClient::with_cash_asset(
        format!("{name_prefix}-{}", exchange),
        symbols,
        slippage_bps,
        fee_model,
        cash_asset,
    ));
    let balance = initial_balance_for_asset(initial_balances, cash_asset);
    client.initialize_balance(cash_asset, balance).await;
    let exec: Arc<dyn ExecutionClient> = client;
    exec
}

fn cash_asset_for_exchange(reporting: AssetId, exchange: ExchangeId) -> AssetId {
    if reporting.exchange.is_specified() {
        if reporting.exchange == exchange {
            reporting
        } else {
            AssetId::from_code(exchange, reporting.code())
        }
    } else if exchange.is_specified() {
        AssetId::from_code(exchange, reporting.code())
    } else {
        reporting
    }
}

fn initial_balance_for_asset(balances: &HashMap<AssetId, Decimal>, target: AssetId) -> Decimal {
    if let Some(amount) = balances
        .iter()
        .find(|(asset, _)| asset.exchange == target.exchange && asset.code() == target.code())
        .map(|(_, amount)| *amount)
    {
        return amount;
    }
    if target.exchange.is_specified() {
        if let Some(amount) = balances
            .iter()
            .find(|(asset, _)| !asset.exchange.is_specified() && asset.code() == target.code())
            .map(|(_, amount)| *amount)
        {
            return amount;
        }
    }
    Decimal::from(10_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_balance_prefers_exchange_specific_entry() {
        let mut balances = HashMap::new();
        let exchange = ExchangeId::from("bybit_linear");
        let global = AssetId::from("USDT");
        let target = AssetId::from_code(exchange, "USDT");
        balances.insert(global, Decimal::new(5_000, 0));
        balances.insert(target, Decimal::new(2_500, 0));
        assert_eq!(
            initial_balance_for_asset(&balances, target),
            Decimal::new(2_500, 0)
        );
    }

    #[test]
    fn initial_balance_falls_back_to_unspecified_exchange() {
        let mut balances = HashMap::new();
        balances.insert(AssetId::from("USDT"), Decimal::new(7_500, 0));
        let exchange = ExchangeId::from("binance_perp");
        let target = AssetId::from_code(exchange, "USDT");
        assert_eq!(
            initial_balance_for_asset(&balances, target),
            Decimal::new(7_500, 0)
        );
    }
}

impl LiveRunArgs {
    async fn run(&self, config: &AppConfig) -> Result<()> {
        let exchange_names = if self.exchanges.is_empty() {
            vec![self.exchange.clone()]
        } else {
            let mut names = self.exchanges.clone();
            if !names.contains(&self.exchange) {
                names.insert(0, self.exchange.clone());
            }
            names
        };
        let profiles = config.exchange_profiles();
        let mut named_exchanges = Vec::new();
        for name in exchange_names {
            let config_entry = profiles
                .get(&name)
                .cloned()
                .ok_or_else(|| anyhow!("exchange profile {} not found", name))?;
            named_exchanges.push(NamedExchange {
                name,
                config: config_entry,
            });
        }

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
        let reporting_currency = AssetId::from(config.backtest.reporting_currency.as_str());
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
        let panic_close = PanicCloseConfig {
            mode: self.panic_mode.into(),
            limit_offset_bps: self.panic_limit_offset_bps.max(Decimal::ZERO),
        };
        let plugins_dir = self
            .plugins_dir
            .clone()
            .or_else(|| config.live.plugins_dir.clone());

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
            orderbook_depth,
            record_path: Some(self.record_data.clone()),
            control_addr,
            panic_close,
            plugins_dir,
        };

        let exchange_labels: Vec<String> = named_exchanges
            .iter()
            .map(|ex| format!("{} ({})", ex.name, ex.config.driver))
            .collect();

        info!(
            strategy = %def.name,
            symbols = ?symbols,
            exchanges = ?exchange_labels,
            interval = %self.interval,
            exec = ?self.exec,
            persistence_engine = ?settings.persistence.engine,
            state_path = %settings.persistence.state_path.display(),
            control_addr = %settings.control_addr,
            "starting live session"
        );

        run_live(strategy, symbols, named_exchanges, settings)
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

fn synth_candles(symbol: Symbol, len: usize, offset_minutes: i64) -> Vec<Candle> {
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
            symbol,
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
            let symbol_code = row
                .symbol
                .clone()
                .or_else(|| infer_symbol_from_path(path))
                .ok_or_else(|| {
                    anyhow!(
                        "missing symbol column and unable to infer from path {}",
                        path.display()
                    )
                })?;
            let symbol = Symbol::from(symbol_code.as_str());
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

fn ensure_parquet_inputs(paths: &[PathBuf]) -> Result<()> {
    if paths.is_empty() {
        bail!("provide at least one --data path (canonical parquet)");
    }
    for path in paths {
        let ext = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase());
        if ext.as_deref() != Some("parquet") {
            bail!(
                "{} is not a parquet file; run `tesser-cli data normalize` before backtesting",
                path.display()
            );
        }
    }
    Ok(())
}

fn memory_market_stream(symbol: Symbol, candles: Vec<Candle>) -> BacktestStream {
    Box::new(PaperMarketStream::from_data(symbol, Vec::new(), candles))
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
                    let symbol_code = symbol
                        .or_else(|| symbol_hint.clone())
                        .ok_or_else(|| anyhow!("missing symbol in snapshot {}", path.display()))?;
                    let symbol = Symbol::from(symbol_code.as_str());
                    let bids = convert_levels(&bids)?;
                    let asks = convert_levels(&asks)?;
                    let book = OrderBook {
                        symbol,
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
                    let symbol_code = symbol.or_else(|| symbol_hint.clone()).ok_or_else(|| {
                        anyhow!("missing symbol in depth update {}", path.display())
                    })?;
                    let symbol = Symbol::from(symbol_code.as_str());
                    let bids = convert_levels(&bids)?;
                    let asks = convert_levels(&asks)?;
                    let update = DepthUpdate {
                        symbol,
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
                    let symbol_code = symbol
                        .or_else(|| symbol_hint.clone())
                        .ok_or_else(|| anyhow!("missing symbol in trade {}", path.display()))?;
                    let symbol = Symbol::from(symbol_code.as_str());
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
                        symbol,
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

fn default_tick_output_path(
    config: &AppConfig,
    exchange: &str,
    symbol: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> PathBuf {
    let start_part = start.format("%Y%m%dT%H%M%S").to_string();
    let end_part = end.format("%Y%m%dT%H%M%S").to_string();
    config
        .data_path
        .join("ticks")
        .join(exchange)
        .join(format!("{symbol}_{start_part}-{end_part}.parquet"))
}

fn default_tick_partition_dir(config: &AppConfig, exchange: &str, symbol: &str) -> PathBuf {
    config.data_path.join("ticks").join(exchange).join(symbol)
}

fn partition_path(base_dir: &Path, date: NaiveDate) -> PathBuf {
    base_dir.join(format!("{}.parquet", date.format("%Y-%m-%d")))
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
struct CandleRow {
    symbol: String,
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
            symbol: candle.symbol.code().to_string(),
            timestamp: candle.timestamp.to_rfc3339(),
            open: candle.open.to_f64().unwrap_or(0.0),
            high: candle.high.to_f64().unwrap_or(0.0),
            low: candle.low.to_f64().unwrap_or(0.0),
            close: candle.close.to_f64().unwrap_or(0.0),
            volume: candle.volume.to_f64().unwrap_or(0.0),
        };
        writer.serialize(&row)?;
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

fn clone_initial_balances(config: &tesser_config::BacktestConfig) -> HashMap<AssetId, Decimal> {
    config
        .initial_balances
        .iter()
        .map(|(currency, amount)| (AssetId::from(currency.as_str()), *amount))
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

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum PanicModeArg {
    Market,
    AggressiveLimit,
}

impl From<PanicModeArg> for PanicCloseMode {
    fn from(arg: PanicModeArg) -> Self {
        match arg {
            PanicModeArg::Market => PanicCloseMode::Market,
            PanicModeArg::AggressiveLimit => PanicCloseMode::AggressiveLimit,
        }
    }
}
