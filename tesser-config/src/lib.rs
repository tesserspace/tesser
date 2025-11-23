//! Layered configuration loading utilities.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use config::{Config, ConfigError, Environment, File};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod deserializer;

/// Root application configuration deserialized from layered sources.
#[derive(Debug, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_data_path")]
    pub data_path: PathBuf,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub backtest: BacktestConfig,
    #[serde(default)]
    pub exchange: HashMap<String, ExchangeConfig>,
    #[serde(default)]
    pub live: LiveRuntimeConfig,
    #[serde(default)]
    pub risk_management: RiskManagementConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BacktestConfig {
    #[serde(
        default = "default_initial_balances",
        with = "deserializer::uppercase_key"
    )]
    pub initial_balances: HashMap<String, Decimal>,
    #[serde(default = "default_reporting_currency")]
    pub reporting_currency: String,
    #[serde(default)]
    pub markets_file: Option<PathBuf>,
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct ExchangeConfig {
    pub rest_url: String,
    pub ws_url: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub api_secret: String,
    #[serde(default = "default_exchange_driver_name")]
    pub driver: String,
    #[serde(default, flatten)]
    pub params: Value,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LiveRuntimeConfig {
    #[serde(default = "default_state_path")]
    pub state_path: PathBuf,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,
    #[serde(default = "default_control_addr")]
    pub control_addr: String,
    #[serde(default = "default_live_log_path")]
    pub log_path: PathBuf,
    #[serde(default = "default_reconciliation_interval_secs")]
    pub reconciliation_interval_secs: u64,
    #[serde(default = "default_reconciliation_threshold")]
    pub reconciliation_threshold: Decimal,
    #[serde(default)]
    pub alerting: AlertingConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AlertingConfig {
    #[serde(default)]
    pub webhook_url: Option<String>,
    #[serde(default = "default_data_gap_secs")]
    pub max_data_gap_secs: u64,
    #[serde(default = "default_order_failure_limit")]
    pub max_order_failures: u32,
    #[serde(default = "default_drawdown_limit")]
    pub max_drawdown: Decimal,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RiskManagementConfig {
    #[serde(default = "default_max_order_quantity")]
    pub max_order_quantity: Decimal,
    #[serde(default = "default_max_position_quantity")]
    pub max_position_quantity: Decimal,
    #[serde(default)]
    pub max_order_notional: Option<Decimal>,
    #[serde(default = "default_risk_drawdown_limit")]
    pub max_drawdown: Decimal,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            initial_balances: default_initial_balances(),
            reporting_currency: default_reporting_currency(),
            markets_file: None,
        }
    }
}

impl Default for LiveRuntimeConfig {
    fn default() -> Self {
        Self {
            state_path: default_state_path(),
            persistence: PersistenceConfig::default(),
            metrics_addr: default_metrics_addr(),
            control_addr: default_control_addr(),
            log_path: default_live_log_path(),
            reconciliation_interval_secs: default_reconciliation_interval_secs(),
            reconciliation_threshold: default_reconciliation_threshold(),
            alerting: AlertingConfig::default(),
        }
    }
}

impl Default for AlertingConfig {
    fn default() -> Self {
        Self {
            webhook_url: None,
            max_data_gap_secs: default_data_gap_secs(),
            max_order_failures: default_order_failure_limit(),
            max_drawdown: default_drawdown_limit(),
        }
    }
}

impl Default for RiskManagementConfig {
    fn default() -> Self {
        Self {
            max_order_quantity: default_max_order_quantity(),
            max_position_quantity: default_max_position_quantity(),
            max_order_notional: None,
            max_drawdown: default_risk_drawdown_limit(),
        }
    }
}

impl LiveRuntimeConfig {
    /// Return persistence settings, preserving legacy `state_path` overrides.
    pub fn persistence_config(&self) -> PersistenceConfig {
        self.persistence.with_fallback(&self.state_path)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct PersistenceConfig {
    #[serde(default = "default_persistence_engine")]
    pub engine: PersistenceEngine,
    #[serde(default = "default_state_path")]
    pub path: PathBuf,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            engine: default_persistence_engine(),
            path: default_state_path(),
        }
    }
}

impl PersistenceConfig {
    /// Resolve the effective path, preferring legacy `live.state_path` overrides when supplied.
    pub fn with_fallback(&self, fallback_path: &Path) -> Self {
        let mut cloned = self.clone();
        if cloned.path == default_state_path() && fallback_path != default_state_path() {
            cloned.path = fallback_path.to_path_buf();
        }
        cloned
    }
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PersistenceEngine {
    Sqlite,
    Lmdb,
}

fn default_data_path() -> PathBuf {
    PathBuf::from("./data")
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_equity() -> Decimal {
    Decimal::new(10_000, 0)
}

fn default_reporting_currency() -> String {
    "USDT".to_string()
}

fn default_initial_balances() -> HashMap<String, Decimal> {
    let mut balances = HashMap::new();
    balances.insert(default_reporting_currency(), default_equity());
    balances
}

fn default_exchange_driver_name() -> String {
    "bybit".to_string()
}

fn default_state_path() -> PathBuf {
    PathBuf::from("./reports/live_state.db")
}

fn default_persistence_engine() -> PersistenceEngine {
    PersistenceEngine::Sqlite
}

fn default_metrics_addr() -> String {
    "127.0.0.1:9100".into()
}

fn default_control_addr() -> String {
    "127.0.0.1:50052".into()
}

fn default_live_log_path() -> PathBuf {
    PathBuf::from("./logs/live.json")
}

fn default_reconciliation_interval_secs() -> u64 {
    60
}

fn default_reconciliation_threshold() -> Decimal {
    Decimal::new(1, 3) // 0.001 == 0.1%
}

fn default_data_gap_secs() -> u64 {
    300
}

fn default_order_failure_limit() -> u32 {
    3
}

fn default_drawdown_limit() -> Decimal {
    Decimal::new(3, 2)
}

fn default_max_order_quantity() -> Decimal {
    Decimal::ONE
}

fn default_max_position_quantity() -> Decimal {
    Decimal::from(2u8)
}

fn default_risk_drawdown_limit() -> Decimal {
    Decimal::new(5, 2)
}

/// Loads configuration by merging files and environment variables.
///
/// Sources (lowest to highest precedence):
/// 1. `config/default.toml`
/// 2. `config/{environment}.toml` (if `environment` is Some)
/// 3. `config/local.toml` (optional, ignored in git)
/// 4. Environment variables prefixed with `TESSER_`
pub fn load_config(env: Option<&str>) -> Result<AppConfig> {
    let base_path = Path::new("config");

    let mut builder =
        Config::builder().add_source(File::from(base_path.join("default.toml")).required(true));
    if let Some(env_name) = env {
        builder = builder
            .add_source(File::from(base_path.join(format!("{env_name}.toml"))).required(false));
    }

    builder = builder.add_source(File::from(base_path.join("local.toml")).required(false));

    builder = builder.add_source(
        Environment::with_prefix("TESSER")
            .separator("__")
            .ignore_empty(true),
    );

    let config = builder.build()?;
    config
        .try_deserialize()
        .map_err(|err: ConfigError| err.into())
}
