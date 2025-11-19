//! Strategy trait definitions, shared context, and a portfolio of reference strategies.

extern crate self as tesser_strategy;

pub use tesser_strategy_macros::register_strategy;
pub use toml::Value;

use chrono::Duration;
use once_cell::sync::Lazy;
use rust_decimal::MathematicalOps;
use rust_decimal::{
    prelude::{FromPrimitive, ToPrimitive},
    Decimal,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::sync::{Arc, RwLock};
use tesser_core::{
    Candle, ExecutionHint, Fill, OrderBook, Position, Signal, SignalKind, Symbol, Tick,
};
use tesser_indicators::{
    indicators::{Atr, BollingerBands, Ichimoku, IchimokuOutput, Macd, Rsi, Sma},
    Indicator,
};
use thiserror::Error;

/// Result alias used within strategy implementations.
pub type StrategyResult<T> = Result<T, StrategyError>;

/// Failure variants surfaced by strategies.
#[derive(Debug, Error)]
pub enum StrategyError {
    /// Raised when a strategy's configuration cannot be parsed or is invalid.
    #[error("configuration is invalid: {0}")]
    InvalidConfig(String),
    /// Raised when the strategy lacks sufficient historical data to proceed.
    #[error("not enough historical data to compute indicators")]
    NotEnoughData,
    /// Used for all other errors that should bubble up to the caller.
    #[error("an internal strategy error occurred: {0}")]
    Internal(String),
}

/// Immutable view of recent market data and portfolio state shared with strategies.
pub struct StrategyContext {
    recent_candles: VecDeque<Candle>,
    recent_ticks: VecDeque<Tick>,
    recent_order_books: VecDeque<OrderBook>,
    positions: Vec<Position>,
    max_history: usize,
}

impl StrategyContext {
    /// Create a new context keeping up to `max_history` events in memory.
    pub fn new(max_history: usize) -> Self {
        let capacity = max_history.max(1);
        Self {
            recent_candles: VecDeque::with_capacity(capacity),
            recent_ticks: VecDeque::with_capacity(capacity),
            recent_order_books: VecDeque::with_capacity(capacity),
            positions: Vec::new(),
            max_history: capacity,
        }
    }

    /// Push a candle while respecting the configured history size.
    pub fn push_candle(&mut self, candle: Candle) {
        if self.recent_candles.len() >= self.max_history {
            self.recent_candles.pop_front();
        }
        self.recent_candles.push_back(candle);
    }

    /// Push a tick while respecting the configured history size.
    pub fn push_tick(&mut self, tick: Tick) {
        if self.recent_ticks.len() >= self.max_history {
            self.recent_ticks.pop_front();
        }
        self.recent_ticks.push_back(tick);
    }

    /// Push an order book snapshot while respecting the configured history size.
    pub fn push_order_book(&mut self, book: OrderBook) {
        if self.recent_order_books.len() >= self.max_history {
            self.recent_order_books.pop_front();
        }
        self.recent_order_books.push_back(book);
    }

    /// Replace the in-memory position snapshot.
    pub fn update_positions(&mut self, positions: Vec<Position>) {
        self.positions = positions;
    }

    /// Access recently observed candles.
    #[must_use]
    pub fn candles(&self) -> &VecDeque<Candle> {
        &self.recent_candles
    }

    /// Access recently observed ticks.
    #[must_use]
    pub fn ticks(&self) -> &VecDeque<Tick> {
        &self.recent_ticks
    }

    /// Access recently observed order books.
    #[must_use]
    pub fn order_books(&self) -> &VecDeque<OrderBook> {
        &self.recent_order_books
    }

    /// Access all tracked positions.
    #[must_use]
    pub fn positions(&self) -> &Vec<Position> {
        &self.positions
    }

    /// Find the position for a specific symbol, if any.
    #[must_use]
    pub fn position(&self, symbol: &str) -> Option<&Position> {
        self.positions.iter().find(|p| p.symbol == symbol)
    }

    /// Returns the latest order book snapshot for the specified symbol.
    #[must_use]
    pub fn order_book(&self, symbol: &str) -> Option<&OrderBook> {
        self.recent_order_books
            .iter()
            .rev()
            .find(|book| book.symbol == symbol)
    }
}

impl Default for StrategyContext {
    fn default() -> Self {
        Self::new(512)
    }
}

/// Strategy lifecycle hooks used by engines that drive market data and fills.
pub trait Strategy: Send + Sync {
    /// Human-friendly identifier used in logs and telemetry.
    fn name(&self) -> &str;

    /// The primary symbol operated on by the strategy.
    fn symbol(&self) -> &str;

    /// Return the set of symbols that should be routed to this strategy (defaults to the primary).
    fn subscriptions(&self) -> Vec<Symbol> {
        vec![self.symbol().to_string()]
    }

    /// Called once before the strategy is registered, allowing it to parse parameters.
    fn configure(&mut self, params: toml::Value) -> StrategyResult<()>;

    /// Called whenever the data pipeline emits a new tick.
    fn on_tick(&mut self, ctx: &StrategyContext, tick: &Tick) -> StrategyResult<()>;

    /// Called whenever a candle is produced or replayed.
    fn on_candle(&mut self, ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()>;

    /// Called whenever one of the strategy's orders is filled.
    fn on_fill(&mut self, ctx: &StrategyContext, fill: &Fill) -> StrategyResult<()>;

    /// Called whenever an order book snapshot is received. Default implementation is a no-op.
    fn on_order_book(&mut self, _ctx: &StrategyContext, _book: &OrderBook) -> StrategyResult<()> {
        Ok(())
    }

    /// Allows the strategy to emit one or more signals after processing events.
    fn drain_signals(&mut self) -> Vec<Signal>;

    /// Returns a JSON snapshot of the strategy's internal state for persistence.
    fn snapshot(&self) -> StrategyResult<serde_json::Value> {
        Ok(serde_json::Value::Null)
    }

    /// Restores the strategy's internal state from a JSON snapshot.
    fn restore(&mut self, _state: serde_json::Value) -> StrategyResult<()> {
        Ok(())
    }
}

// -------------------------------------------------------------------------------------------------
// Strategy registry
// -------------------------------------------------------------------------------------------------

static STRATEGY_REGISTRY: Lazy<StrategyRegistry> = Lazy::new(StrategyRegistry::new);

/// Returns a handle to the global registry.
pub fn strategy_registry() -> &'static StrategyRegistry {
    &STRATEGY_REGISTRY
}

/// Registers a strategy factory with the global registry.
pub fn register_strategy_factory(factory: Arc<dyn StrategyFactory>) {
    strategy_registry().register(factory);
}

/// Builds a strategy by name using the registered factories.
pub fn load_strategy(name: &str, params: Value) -> StrategyResult<Box<dyn Strategy>> {
    strategy_registry().build(name, params)
}

/// Returns the list of built-in strategy identifiers in sorted order.
pub fn builtin_strategy_names() -> Vec<&'static str> {
    strategy_registry().names()
}

/// Factory contract used to construct strategies from configuration.
pub trait StrategyFactory: Send + Sync {
    /// Canonical, user-facing identifier for the strategy (e.g. "SmaCross").
    fn canonical_name(&self) -> &'static str;

    /// Additional aliases that should resolve to the same strategy.
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    /// Builds and configures a strategy instance with the provided parameters.
    fn build(&self, params: Value) -> StrategyResult<Box<dyn Strategy>>;
}

#[derive(Default)]
struct RegistryInner {
    by_canonical: HashMap<&'static str, Arc<dyn StrategyFactory>>,
    by_alias: HashMap<String, Arc<dyn StrategyFactory>>,
}

/// Thread-safe registry used to manage available strategies.
pub struct StrategyRegistry {
    inner: RwLock<RegistryInner>,
}

impl StrategyRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(RegistryInner::default()),
        }
    }

    fn register(&self, factory: Arc<dyn StrategyFactory>) {
        let mut inner = self.inner.write().expect("registry poisoned");
        let canonical = factory.canonical_name();
        if inner
            .by_canonical
            .insert(canonical, factory.clone())
            .is_some()
        {
            tracing::warn!(
                strategy = canonical,
                "duplicate strategy registration detected; overriding previous factory"
            );
        }
        inner
            .by_alias
            .insert(normalize_name(canonical), factory.clone());
        for alias in factory.aliases() {
            self.insert_alias(&mut inner.by_alias, alias, factory.clone(), canonical);
        }
    }

    fn insert_alias(
        &self,
        aliases: &mut HashMap<String, Arc<dyn StrategyFactory>>,
        alias: &str,
        factory: Arc<dyn StrategyFactory>,
        canonical: &'static str,
    ) {
        let normalized = normalize_name(alias);
        if let Some(existing) = aliases.get(&normalized) {
            if !Arc::ptr_eq(existing, &factory) {
                tracing::warn!(
                    alias = alias,
                    strategy = canonical,
                    "alias already registered for another strategy; overriding"
                );
            }
        }
        aliases.insert(normalized, factory);
    }

    fn build(&self, name: &str, params: Value) -> StrategyResult<Box<dyn Strategy>> {
        let factory = self
            .get(name)
            .ok_or_else(|| StrategyError::InvalidConfig(format!("unknown strategy: {name}")))?;
        factory.build(params)
    }

    fn get(&self, name: &str) -> Option<Arc<dyn StrategyFactory>> {
        let inner = self.inner.read().expect("registry poisoned");
        inner.by_alias.get(&normalize_name(name)).cloned()
    }

    fn names(&self) -> Vec<&'static str> {
        let inner = self.inner.read().expect("registry poisoned");
        let mut names: Vec<&'static str> = inner.by_canonical.keys().copied().collect();
        names.sort_unstable();
        names
    }
}

impl Default for StrategyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn normalize_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

// -------------------------------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------------------------------

fn collect_symbol_closes(candles: &VecDeque<Candle>, symbol: &str, limit: usize) -> Vec<Decimal> {
    let mut values: Vec<Decimal> = candles
        .iter()
        .rev()
        .filter(|c| c.symbol == symbol)
        .take(limit)
        .map(|c| c.close)
        .collect();
    values.reverse();
    values
}

fn z_score(values: &[Decimal]) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    let len = Decimal::from_usize(values.len())?;
    let mean = values.iter().copied().sum::<Decimal>() / len;
    let variance = values
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<Decimal>()
        / len;
    let std = variance.sqrt()?;
    if std.is_zero() {
        return None;
    }
    let last = *values.last()?;
    Some((last - mean) / std)
}

// -------------------------------------------------------------------------------------------------
// Baseline Strategies
// -------------------------------------------------------------------------------------------------

/// Double moving-average crossover strategy.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct SmaCrossConfig {
    pub symbol: Symbol,
    pub fast_period: usize,
    pub slow_period: usize,
    pub min_samples: usize,
    pub vwap_duration_secs: Option<i64>,
    pub vwap_participation: Option<Decimal>,
}

impl Default for SmaCrossConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            fast_period: 5,
            slow_period: 20,
            min_samples: 25,
            vwap_duration_secs: None,
            vwap_participation: None,
        }
    }
}

impl TryFrom<toml::Value> for SmaCrossConfig {
    type Error = StrategyError;

    fn try_from(value: toml::Value) -> Result<Self, Self::Error> {
        value.try_into().map_err(|err: toml::de::Error| {
            StrategyError::InvalidConfig(format!("failed to parse SmaCross config: {err}"))
        })
    }
}

/// Very small reference implementation that can be expanded later.
pub struct SmaCross {
    cfg: SmaCrossConfig,
    signals: Vec<Signal>,
    fast_ma: Sma,
    slow_ma: Sma,
    fast_prev: Option<Decimal>,
    fast_last: Option<Decimal>,
    slow_prev: Option<Decimal>,
    slow_last: Option<Decimal>,
    samples: usize,
}

impl Default for SmaCross {
    fn default() -> Self {
        Self::new(SmaCrossConfig::default())
    }
}

impl SmaCross {
    /// Instantiate the strategy with the provided configuration.
    pub fn new(cfg: SmaCrossConfig) -> Self {
        let fast_ma = Sma::new(cfg.fast_period).expect("fast period must be positive");
        let slow_ma = Sma::new(cfg.slow_period).expect("slow period must be positive");
        Self {
            cfg,
            signals: Vec::new(),
            fast_ma,
            slow_ma,
            fast_prev: None,
            fast_last: None,
            slow_prev: None,
            slow_last: None,
            samples: 0,
        }
    }

    fn rebuild_indicators(&mut self) -> StrategyResult<()> {
        self.fast_ma = Sma::new(self.cfg.fast_period)
            .map_err(|err| StrategyError::InvalidConfig(err.to_string()))?;
        self.slow_ma = Sma::new(self.cfg.slow_period)
            .map_err(|err| StrategyError::InvalidConfig(err.to_string()))?;
        self.fast_prev = None;
        self.fast_last = None;
        self.slow_prev = None;
        self.slow_last = None;
        self.samples = 0;
        Ok(())
    }

    fn maybe_emit_signal(&mut self, candle: &Candle) -> StrategyResult<()> {
        if let Some(value) = self.fast_ma.next(candle.close) {
            self.fast_prev = self.fast_last.replace(value);
        }
        if let Some(value) = self.slow_ma.next(candle.close) {
            self.slow_prev = self.slow_last.replace(value);
        }
        self.samples += 1;
        if self.samples < self.cfg.min_samples {
            return Ok(());
        }
        if let (Some(fast_prev), Some(fast_curr), Some(slow_prev), Some(slow_curr)) = (
            self.fast_prev,
            self.fast_last,
            self.slow_prev,
            self.slow_last,
        ) {
            if fast_prev <= slow_prev && fast_curr > slow_curr {
                let mut signal = Signal::new(self.cfg.symbol.clone(), SignalKind::EnterLong, 0.75);
                let stop_loss_factor = Decimal::new(98, 2); // 0.98
                signal.stop_loss = Some(candle.low * stop_loss_factor);
                if let Some(duration_secs) = self.cfg.vwap_duration_secs.filter(|v| *v > 0) {
                    let duration = Duration::seconds(duration_secs);
                    let participation = self
                        .cfg
                        .vwap_participation
                        .map(|value| value.max(Decimal::ZERO).min(Decimal::ONE));
                    signal.execution_hint = Some(ExecutionHint::Vwap {
                        duration,
                        participation_rate: participation,
                    });
                }
                self.signals.push(signal);
            } else if fast_prev >= slow_prev && fast_curr < slow_curr {
                self.signals.push(Signal::new(
                    self.cfg.symbol.clone(),
                    SignalKind::ExitLong,
                    0.75,
                ));
            }
        }

        Ok(())
    }
}

impl Strategy for SmaCross {
    fn name(&self) -> &str {
        "sma-cross"
    }

    fn symbol(&self) -> &str {
        &self.cfg.symbol
    }

    fn configure(&mut self, params: toml::Value) -> StrategyResult<()> {
        let cfg = SmaCrossConfig::try_from(params)?;
        if cfg.fast_period == 0 || cfg.slow_period == 0 {
            return Err(StrategyError::InvalidConfig(
                "period values must be greater than zero".into(),
            ));
        }
        self.cfg = cfg;
        self.rebuild_indicators()
    }

    fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, _ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if candle.symbol != self.cfg.symbol {
            return Ok(());
        }
        self.maybe_emit_signal(candle)
    }

    fn on_fill(&mut self, _ctx: &StrategyContext, _fill: &Fill) -> StrategyResult<()> {
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.signals)
    }
}

register_strategy!(SmaCross, "SmaCross");

/// Relative Strength Index mean-reversion strategy.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct RsiReversionConfig {
    pub symbol: Symbol,
    pub period: usize,
    pub oversold: Decimal,
    pub overbought: Decimal,
    pub lookback: usize,
}

impl Default for RsiReversionConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            period: 14,
            oversold: Decimal::from(30),
            overbought: Decimal::from(70),
            lookback: 200,
        }
    }
}

pub struct RsiReversion {
    cfg: RsiReversionConfig,
    signals: Vec<Signal>,
    rsi: Rsi,
    oversold_level: Decimal,
    overbought_level: Decimal,
    samples: usize,
}

impl Default for RsiReversion {
    fn default() -> Self {
        Self::new(RsiReversionConfig::default())
    }
}

impl RsiReversion {
    /// Instantiate the strategy with the provided configuration.
    pub fn new(cfg: RsiReversionConfig) -> Self {
        let rsi = Rsi::new(cfg.period).expect("period must be positive");
        let oversold_level = cfg.oversold;
        let overbought_level = cfg.overbought;
        Self {
            cfg,
            signals: Vec::new(),
            rsi,
            oversold_level,
            overbought_level,
            samples: 0,
        }
    }

    fn rebuild_indicator(&mut self) -> StrategyResult<()> {
        self.rsi = Rsi::new(self.cfg.period)
            .map_err(|err| StrategyError::InvalidConfig(err.to_string()))?;
        self.oversold_level = self.cfg.oversold;
        self.overbought_level = self.cfg.overbought;
        self.samples = 0;
        Ok(())
    }

    fn maybe_emit_signal(&mut self, candle: &Candle) -> StrategyResult<()> {
        let value = self.rsi.next(candle.close);
        self.samples += 1;
        if self.samples < self.cfg.lookback {
            return Ok(());
        }
        if let Some(rsi_value) = value {
            if rsi_value <= self.oversold_level {
                self.signals.push(Signal::new(
                    self.cfg.symbol.clone(),
                    SignalKind::EnterLong,
                    0.8,
                ));
            } else if rsi_value >= self.overbought_level {
                self.signals.push(Signal::new(
                    self.cfg.symbol.clone(),
                    SignalKind::ExitLong,
                    0.8,
                ));
            }
        }
        Ok(())
    }
}

impl Strategy for RsiReversion {
    fn name(&self) -> &str {
        "rsi-reversion"
    }

    fn symbol(&self) -> &str {
        &self.cfg.symbol
    }

    fn configure(&mut self, params: toml::Value) -> StrategyResult<()> {
        let cfg: RsiReversionConfig = params.try_into().map_err(|err: toml::de::Error| {
            StrategyError::InvalidConfig(format!("failed to parse RsiReversion config: {err}"))
        })?;
        if cfg.period == 0 {
            return Err(StrategyError::InvalidConfig(
                "period must be greater than zero".into(),
            ));
        }
        self.cfg = cfg;
        self.rebuild_indicator()
    }

    fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, _ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if candle.symbol != self.cfg.symbol {
            return Ok(());
        }
        self.maybe_emit_signal(candle)
    }

    fn on_fill(&mut self, _ctx: &StrategyContext, _fill: &Fill) -> StrategyResult<()> {
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.signals)
    }
}

register_strategy!(RsiReversion, "RsiReversion");

/// Bollinger band breakout strategy.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct BollingerBreakoutConfig {
    pub symbol: Symbol,
    pub period: usize,
    pub std_multiplier: Decimal,
    pub lookback: usize,
}

impl Default for BollingerBreakoutConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            period: 20,
            std_multiplier: Decimal::from(2),
            lookback: 200,
        }
    }
}

pub struct BollingerBreakout {
    cfg: BollingerBreakoutConfig,
    signals: Vec<Signal>,
    bands: BollingerBands,
    std_multiplier: Decimal,
    neutral_band: Decimal,
    samples: usize,
}

impl Default for BollingerBreakout {
    fn default() -> Self {
        Self::new(BollingerBreakoutConfig::default())
    }
}

impl BollingerBreakout {
    /// Instantiate the strategy with the provided configuration.
    pub fn new(cfg: BollingerBreakoutConfig) -> Self {
        let std_multiplier = cfg.std_multiplier;
        let neutral_band = std_multiplier * Decimal::new(25, 2); // 0.25
        let bands = BollingerBands::new(cfg.period, std_multiplier)
            .expect("period and multiplier must be valid");
        Self {
            cfg,
            signals: Vec::new(),
            bands,
            std_multiplier,
            neutral_band,
            samples: 0,
        }
    }

    fn rebuild_indicator(&mut self) -> StrategyResult<()> {
        self.std_multiplier = self.cfg.std_multiplier;
        self.neutral_band = self.std_multiplier * Decimal::new(25, 2);
        self.bands = BollingerBands::new(self.cfg.period, self.std_multiplier)
            .map_err(|err| StrategyError::InvalidConfig(err.to_string()))?;
        self.samples = 0;
        Ok(())
    }

    fn maybe_emit_signal(&mut self, candle: &Candle) -> StrategyResult<()> {
        let bands = match self.bands.next(candle.close) {
            Some(value) => value,
            None => {
                self.samples += 1;
                return Ok(());
            }
        };
        self.samples += 1;
        if self.samples < self.cfg.lookback {
            return Ok(());
        }
        let price = candle.close;
        if price > bands.upper {
            self.signals.push(Signal::new(
                self.cfg.symbol.clone(),
                SignalKind::EnterLong,
                0.7,
            ));
        } else if price < bands.lower {
            self.signals.push(Signal::new(
                self.cfg.symbol.clone(),
                SignalKind::EnterShort,
                0.7,
            ));
        } else if (price - bands.middle).abs() <= self.neutral_band {
            self.signals.push(Signal::new(
                self.cfg.symbol.clone(),
                SignalKind::Flatten,
                0.6,
            ));
        }
        Ok(())
    }
}

impl Strategy for BollingerBreakout {
    fn name(&self) -> &str {
        "bollinger-breakout"
    }

    fn symbol(&self) -> &str {
        &self.cfg.symbol
    }

    fn configure(&mut self, params: toml::Value) -> StrategyResult<()> {
        let cfg: BollingerBreakoutConfig = params.try_into().map_err(|err: toml::de::Error| {
            StrategyError::InvalidConfig(format!("failed to parse BollingerBreakout config: {err}"))
        })?;
        if cfg.period == 0 {
            return Err(StrategyError::InvalidConfig(
                "period must be greater than zero".into(),
            ));
        }
        self.cfg = cfg;
        self.rebuild_indicator()
    }

    fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, _ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if candle.symbol != self.cfg.symbol {
            return Ok(());
        }
        self.maybe_emit_signal(candle)
    }

    fn on_fill(&mut self, _ctx: &StrategyContext, _fill: &Fill) -> StrategyResult<()> {
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.signals)
    }
}

register_strategy!(BollingerBreakout, "BollingerBreakout");

// -------------------------------------------------------------------------------------------------
// Modern Strategies
// -------------------------------------------------------------------------------------------------

/// Simple linear model used by the ML classifier placeholder.
#[derive(Debug, Deserialize)]
struct LinearModelArtifact {
    bias: f64,
    weights: Vec<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct MlClassifierConfig {
    pub symbol: Symbol,
    pub model_path: String,
    pub lookback: usize,
    pub threshold_long: f64,
    pub threshold_short: f64,
}

impl Default for MlClassifierConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            model_path: "./model.toml".to_string(),
            lookback: 20,
            threshold_long: 0.25,
            threshold_short: -0.25,
        }
    }
}

#[derive(Default)]
pub struct MlClassifier {
    cfg: MlClassifierConfig,
    model: Option<LinearModelArtifact>,
    signals: Vec<Signal>,
}

impl MlClassifier {
    fn score(&self, ctx: &StrategyContext) -> Option<f64> {
        let model = self.model.as_ref()?;
        let raw_closes =
            collect_symbol_closes(ctx.candles(), &self.cfg.symbol, self.cfg.lookback + 1);
        if raw_closes.len() < self.cfg.lookback + 1 {
            return None;
        }
        let mut closes = Vec::with_capacity(raw_closes.len());
        for value in raw_closes {
            closes.push(value.to_f64()?);
        }
        let mut features = Vec::with_capacity(self.cfg.lookback);
        for window in closes.windows(2) {
            let prev = window[0];
            let curr = window[1];
            features.push(if prev.abs() < f64::EPSILON {
                0.0
            } else {
                (curr - prev) / prev
            });
        }
        let score = model
            .weights
            .iter()
            .zip(features.iter())
            .map(|(w, f)| w * f)
            .sum::<f64>()
            + model.bias;
        Some(score)
    }
}

impl Strategy for MlClassifier {
    fn name(&self) -> &str {
        "ml-classifier"
    }

    fn symbol(&self) -> &str {
        &self.cfg.symbol
    }

    fn configure(&mut self, params: toml::Value) -> StrategyResult<()> {
        let cfg: MlClassifierConfig = params.try_into().map_err(|err: toml::de::Error| {
            StrategyError::InvalidConfig(format!("failed to parse MlClassifier config: {err}"))
        })?;
        let contents = fs::read_to_string(&cfg.model_path).map_err(|err| {
            StrategyError::InvalidConfig(format!(
                "failed to read model at {}: {err}",
                cfg.model_path
            ))
        })?;
        let artifact: LinearModelArtifact = toml::from_str(&contents).map_err(|err| {
            StrategyError::InvalidConfig(format!(
                "failed to deserialize model artifact {}: {err}",
                cfg.model_path
            ))
        })?;
        if artifact.weights.is_empty() {
            return Err(StrategyError::InvalidConfig(
                "model artifact must contain at least one weight".into(),
            ));
        }
        self.model = Some(artifact);
        self.cfg = cfg;
        Ok(())
    }

    fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if candle.symbol != self.cfg.symbol {
            return Ok(());
        }
        if let Some(score) = self.score(ctx) {
            if score >= self.cfg.threshold_long {
                self.signals.push(Signal::new(
                    self.cfg.symbol.clone(),
                    SignalKind::EnterLong,
                    0.85,
                ));
            } else if score <= self.cfg.threshold_short {
                self.signals.push(Signal::new(
                    self.cfg.symbol.clone(),
                    SignalKind::EnterShort,
                    0.85,
                ));
            }
        }
        Ok(())
    }

    fn on_fill(&mut self, _ctx: &StrategyContext, _fill: &Fill) -> StrategyResult<()> {
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.signals)
    }
}

register_strategy!(MlClassifier, "MlClassifier");

/// Statistical arbitrage pairs-trading strategy.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct PairsTradingConfig {
    pub symbols: [Symbol; 2],
    pub lookback: usize,
    pub entry_z: Decimal,
    pub exit_z: Decimal,
}

impl Default for PairsTradingConfig {
    fn default() -> Self {
        Self {
            symbols: ["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            lookback: 200,
            entry_z: Decimal::from(2),
            exit_z: Decimal::new(5, 1),
        }
    }
}

pub struct PairsTradingArbitrage {
    cfg: PairsTradingConfig,
    signals: Vec<Signal>,
    entry_z_level: Decimal,
    exit_z_level: Decimal,
}

impl Default for PairsTradingArbitrage {
    fn default() -> Self {
        Self::from_config(PairsTradingConfig::default())
            .expect("default pairs trading config should be valid")
    }
}

impl PairsTradingArbitrage {
    fn from_config(cfg: PairsTradingConfig) -> StrategyResult<Self> {
        let mut strategy = Self {
            cfg,
            signals: Vec::new(),
            entry_z_level: Decimal::ZERO,
            exit_z_level: Decimal::ZERO,
        };
        strategy.rebuild_thresholds()?;
        Ok(strategy)
    }

    fn rebuild_thresholds(&mut self) -> StrategyResult<()> {
        self.entry_z_level = self.cfg.entry_z;
        self.exit_z_level = self.cfg.exit_z;
        if self.entry_z_level <= self.exit_z_level {
            return Err(StrategyError::InvalidConfig(
                "`entry_z` must be greater than `exit_z`".into(),
            ));
        }
        Ok(())
    }

    fn spreads(&self, ctx: &StrategyContext) -> Option<Vec<Decimal>> {
        let closes_a =
            collect_symbol_closes(ctx.candles(), &self.cfg.symbols[0], self.cfg.lookback);
        let closes_b =
            collect_symbol_closes(ctx.candles(), &self.cfg.symbols[1], self.cfg.lookback);
        if closes_a.len() < self.cfg.lookback || closes_b.len() < self.cfg.lookback {
            return None;
        }
        let mut spreads = Vec::with_capacity(self.cfg.lookback);
        for (a, b) in closes_a.iter().zip(closes_b.iter()) {
            if b.is_zero() {
                return None;
            }
            spreads.push((*a / *b).ln());
        }
        Some(spreads)
    }
}

impl Strategy for PairsTradingArbitrage {
    fn name(&self) -> &str {
        "pairs-trading"
    }

    fn symbol(&self) -> &str {
        &self.cfg.symbols[0]
    }

    fn subscriptions(&self) -> Vec<Symbol> {
        self.cfg.symbols.to_vec()
    }

    fn configure(&mut self, params: toml::Value) -> StrategyResult<()> {
        let cfg: PairsTradingConfig = params.try_into().map_err(|err: toml::de::Error| {
            StrategyError::InvalidConfig(format!(
                "failed to parse PairsTradingArbitrage config: {err}"
            ))
        })?;
        if cfg.lookback < 2 {
            return Err(StrategyError::InvalidConfig(
                "lookback must be at least 2".into(),
            ));
        }
        self.cfg = cfg;
        self.rebuild_thresholds()
    }

    fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if self.cfg.symbols.contains(&candle.symbol) {
            if let Some(spreads) = self.spreads(ctx) {
                if let Some(z) = z_score(&spreads) {
                    if z >= self.entry_z_level {
                        // Asset A rich: short A, long B.
                        self.signals.push(Signal::new(
                            self.cfg.symbols[0].clone(),
                            SignalKind::EnterShort,
                            0.8,
                        ));
                        self.signals.push(Signal::new(
                            self.cfg.symbols[1].clone(),
                            SignalKind::EnterLong,
                            0.8,
                        ));
                    } else if z <= -self.entry_z_level {
                        // Asset B rich: long A, short B.
                        self.signals.push(Signal::new(
                            self.cfg.symbols[0].clone(),
                            SignalKind::EnterLong,
                            0.8,
                        ));
                        self.signals.push(Signal::new(
                            self.cfg.symbols[1].clone(),
                            SignalKind::EnterShort,
                            0.8,
                        ));
                    } else if z.abs() <= self.exit_z_level {
                        for symbol in &self.cfg.symbols {
                            self.signals.push(Signal::new(
                                symbol.clone(),
                                SignalKind::Flatten,
                                0.6,
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn on_fill(&mut self, _ctx: &StrategyContext, _fill: &Fill) -> StrategyResult<()> {
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.signals)
    }
}

register_strategy!(
    PairsTradingArbitrage,
    "PairsTradingArbitrage",
    aliases = ["PairsTrading", "Pairs"]
);

/// Order book imbalance strategy operating on depth snapshots.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct OrderBookImbalanceConfig {
    pub symbol: Symbol,
    pub depth: usize,
    pub long_threshold: f64,
    pub short_threshold: f64,
    pub neutral_zone: f64,
}

impl Default for OrderBookImbalanceConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            depth: 5,
            long_threshold: 0.2,
            short_threshold: -0.2,
            neutral_zone: 0.05,
        }
    }
}

#[derive(Default)]
pub struct OrderBookImbalance {
    cfg: OrderBookImbalanceConfig,
    signals: Vec<Signal>,
}

impl Strategy for OrderBookImbalance {
    fn name(&self) -> &str {
        "orderbook-imbalance"
    }

    fn symbol(&self) -> &str {
        &self.cfg.symbol
    }

    fn configure(&mut self, params: toml::Value) -> StrategyResult<()> {
        let cfg: OrderBookImbalanceConfig = params.try_into().map_err(|err: toml::de::Error| {
            StrategyError::InvalidConfig(format!(
                "failed to parse OrderBookImbalance config: {err}"
            ))
        })?;
        if cfg.depth == 0 {
            return Err(StrategyError::InvalidConfig(
                "depth must be greater than zero".into(),
            ));
        }
        self.cfg = cfg;
        Ok(())
    }

    fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, _ctx: &StrategyContext, _candle: &Candle) -> StrategyResult<()> {
        Ok(())
    }

    fn on_fill(&mut self, _ctx: &StrategyContext, _fill: &Fill) -> StrategyResult<()> {
        Ok(())
    }

    fn on_order_book(&mut self, _ctx: &StrategyContext, book: &OrderBook) -> StrategyResult<()> {
        if book.symbol != self.cfg.symbol {
            return Ok(());
        }
        if let Some(imbalance) = book.imbalance(self.cfg.depth) {
            if let Some(imbalance_f64) = imbalance.to_f64() {
                if imbalance_f64 >= self.cfg.long_threshold {
                    self.signals.push(Signal::new(
                        self.cfg.symbol.clone(),
                        SignalKind::EnterLong,
                        0.9,
                    ));
                } else if imbalance_f64 <= self.cfg.short_threshold {
                    self.signals.push(Signal::new(
                        self.cfg.symbol.clone(),
                        SignalKind::EnterShort,
                        0.9,
                    ));
                } else if imbalance_f64.abs() <= self.cfg.neutral_zone {
                    self.signals.push(Signal::new(
                        self.cfg.symbol.clone(),
                        SignalKind::Flatten,
                        0.6,
                    ));
                }
            }
        }
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.signals)
    }
}

register_strategy!(OrderBookImbalance, "OrderBookImbalance", aliases = ["OBI"]);

// -------------------------------------------------------------------------------------------------
// Advanced Strategies
// -------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct OrderBookScalperConfig {
    pub symbol: Symbol,
    pub depth: usize,
    pub imbalance_threshold: f64,
    pub neutral_zone: f64,
    pub peg_offset_bps: Decimal,
    pub clip_size: Decimal,
    pub refresh_secs: u64,
    pub macd_fast: usize,
    pub macd_slow: usize,
    pub macd_signal: usize,
    pub min_tick_size: Decimal,
}

impl Default for OrderBookScalperConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            depth: 10,
            imbalance_threshold: 0.25,
            neutral_zone: 0.05,
            peg_offset_bps: Decimal::new(5, 1),
            clip_size: Decimal::from(5),
            refresh_secs: 1,
            macd_fast: 12,
            macd_slow: 26,
            macd_signal: 9,
            min_tick_size: Decimal::ONE,
        }
    }
}

pub struct OrderBookScalper {
    cfg: OrderBookScalperConfig,
    signals: Vec<Signal>,
    macd: Macd,
    last_tick_size: Decimal,
    last_histogram: Option<Decimal>,
}

impl Default for OrderBookScalper {
    fn default() -> Self {
        Self::new(OrderBookScalperConfig::default())
    }
}

impl OrderBookScalper {
    pub fn new(cfg: OrderBookScalperConfig) -> Self {
        let macd =
            Macd::new(cfg.macd_fast, cfg.macd_slow, cfg.macd_signal).expect("valid MACD periods");
        Self {
            cfg,
            signals: Vec::new(),
            macd,
            last_tick_size: Decimal::ZERO,
            last_histogram: None,
        }
    }

    fn imbalance_supports_entry(&self, imbalance: f64, histogram: Decimal) -> SignalKind {
        if imbalance >= self.cfg.imbalance_threshold && histogram > Decimal::ZERO {
            SignalKind::EnterLong
        } else if imbalance <= -self.cfg.imbalance_threshold && histogram < Decimal::ZERO {
            SignalKind::EnterShort
        } else {
            SignalKind::Flatten
        }
    }
}

impl Strategy for OrderBookScalper {
    fn name(&self) -> &str {
        "orderbook-scalper"
    }

    fn symbol(&self) -> &str {
        &self.cfg.symbol
    }

    fn configure(&mut self, params: toml::Value) -> StrategyResult<()> {
        let cfg: OrderBookScalperConfig = params.try_into().map_err(|err: toml::de::Error| {
            StrategyError::InvalidConfig(format!("failed to parse OrderBookScalper config: {err}"))
        })?;
        if cfg.depth == 0 {
            return Err(StrategyError::InvalidConfig(
                "depth must be greater than zero".into(),
            ));
        }
        self.macd = Macd::new(cfg.macd_fast, cfg.macd_slow, cfg.macd_signal)
            .map_err(|err| StrategyError::InvalidConfig(err.to_string()))?;
        self.cfg = cfg;
        self.last_histogram = None;
        Ok(())
    }

    fn on_tick(&mut self, _ctx: &StrategyContext, tick: &Tick) -> StrategyResult<()> {
        if tick.symbol == self.cfg.symbol {
            self.last_tick_size = tick.size.abs();
        }
        Ok(())
    }

    fn on_candle(&mut self, _ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if candle.symbol == self.cfg.symbol {
            if let Some(output) = self.macd.next(candle.close) {
                self.last_histogram = Some(output.histogram);
            }
        }
        Ok(())
    }

    fn on_order_book(&mut self, _ctx: &StrategyContext, book: &OrderBook) -> StrategyResult<()> {
        if book.symbol != self.cfg.symbol {
            return Ok(());
        }
        if self.last_tick_size < self.cfg.min_tick_size {
            return Ok(());
        }
        let macd = match self.last_histogram {
            Some(value) => value,
            None => return Ok(()),
        };
        if let Some(balance) = book.imbalance(self.cfg.depth) {
            if let Some(im) = balance.to_f64() {
                let kind = self.imbalance_supports_entry(im, macd);
                if !matches!(kind, SignalKind::Flatten) || im.abs() <= self.cfg.neutral_zone {
                    let mut signal = Signal::new(self.cfg.symbol.clone(), kind, 0.9);
                    signal.execution_hint = Some(ExecutionHint::PeggedBest {
                        offset_bps: self.cfg.peg_offset_bps,
                        clip_size: Some(self.cfg.clip_size.max(Decimal::ONE)),
                        refresh_secs: Some(self.cfg.refresh_secs),
                    });
                    self.signals.push(signal);
                }
            }
        }
        Ok(())
    }

    fn on_fill(&mut self, _ctx: &StrategyContext, _fill: &Fill) -> StrategyResult<()> {
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.signals)
    }
}

register_strategy!(OrderBookScalper, "OrderBookScalper");

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct CrossExchangeArbConfig {
    pub symbol_a: Symbol,
    pub symbol_b: Symbol,
    pub spread_bps: Decimal,
    pub exit_bps: Decimal,
    pub ichimoku_conversion: usize,
    pub ichimoku_base: usize,
    pub ichimoku_span_b: usize,
}

impl Default for CrossExchangeArbConfig {
    fn default() -> Self {
        Self {
            symbol_a: "BTCUSDT".into(),
            symbol_b: "BTCUSDC".into(),
            spread_bps: Decimal::new(50, 2),
            exit_bps: Decimal::new(10, 2),
            ichimoku_conversion: 9,
            ichimoku_base: 26,
            ichimoku_span_b: 52,
        }
    }
}

pub struct CrossExchangeArb {
    cfg: CrossExchangeArbConfig,
    signals: Vec<Signal>,
    last_price_a: Option<Decimal>,
    last_price_b: Option<Decimal>,
    ichimoku: Ichimoku,
    last_cloud: Option<IchimokuOutput>,
}

impl Default for CrossExchangeArb {
    fn default() -> Self {
        Self::new(CrossExchangeArbConfig::default())
    }
}

impl CrossExchangeArb {
    pub fn new(cfg: CrossExchangeArbConfig) -> Self {
        let ichimoku = Ichimoku::new(
            cfg.ichimoku_conversion,
            cfg.ichimoku_base,
            cfg.ichimoku_span_b,
        )
        .expect("valid ichimoku periods");
        Self {
            cfg,
            signals: Vec::new(),
            last_price_a: None,
            last_price_b: None,
            ichimoku,
            last_cloud: None,
        }
    }

    fn emit_pair_trade(&mut self, long_a: bool) {
        let duration = Duration::seconds(30);
        let mut signal_a = Signal::new(
            self.cfg.symbol_a.clone(),
            if long_a {
                SignalKind::EnterLong
            } else {
                SignalKind::EnterShort
            },
            0.85,
        );
        signal_a.execution_hint = Some(ExecutionHint::Twap { duration });
        let mut signal_b = Signal::new(
            self.cfg.symbol_b.clone(),
            if long_a {
                SignalKind::EnterShort
            } else {
                SignalKind::EnterLong
            },
            0.85,
        );
        signal_b.execution_hint = Some(ExecutionHint::Twap { duration });
        self.signals.push(signal_a);
        self.signals.push(signal_b);
    }
}

impl Strategy for CrossExchangeArb {
    fn name(&self) -> &str {
        "cross-exchange-arb"
    }

    fn symbol(&self) -> &str {
        &self.cfg.symbol_a
    }

    fn subscriptions(&self) -> Vec<Symbol> {
        vec![self.cfg.symbol_a.clone(), self.cfg.symbol_b.clone()]
    }

    fn configure(&mut self, params: toml::Value) -> StrategyResult<()> {
        let cfg: CrossExchangeArbConfig = params.try_into().map_err(|err: toml::de::Error| {
            StrategyError::InvalidConfig(format!("failed to parse CrossExchangeArb config: {err}"))
        })?;
        self.ichimoku = Ichimoku::new(
            cfg.ichimoku_conversion,
            cfg.ichimoku_base,
            cfg.ichimoku_span_b,
        )
        .map_err(|err| StrategyError::InvalidConfig(err.to_string()))?;
        self.cfg = cfg;
        Ok(())
    }

    fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, _ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if candle.symbol == self.cfg.symbol_a {
            self.last_price_a = Some(candle.close);
            if let Some(cloud) = self.ichimoku.next(candle.clone()) {
                self.last_cloud = Some(cloud);
            }
        } else if candle.symbol == self.cfg.symbol_b {
            self.last_price_b = Some(candle.close);
        }
        if let (Some(a), Some(b)) = (self.last_price_a, self.last_price_b) {
            if b.is_zero() {
                return Ok(());
            }
            let spread = (a - b) / b;
            let is_trending = self
                .last_cloud
                .map(|cloud| (cloud.conversion_line - cloud.base_line).abs() > Decimal::new(5, 2))
                .unwrap_or(false);
            if is_trending {
                return Ok(());
            }
            if spread >= self.cfg.spread_bps / Decimal::from(10_000) {
                self.emit_pair_trade(false);
            } else if spread <= -(self.cfg.spread_bps / Decimal::from(10_000)) {
                self.emit_pair_trade(true);
            } else if spread.abs() <= self.cfg.exit_bps / Decimal::from(10_000) {
                self.signals.push(Signal::new(
                    self.cfg.symbol_a.clone(),
                    SignalKind::Flatten,
                    0.6,
                ));
                self.signals.push(Signal::new(
                    self.cfg.symbol_b.clone(),
                    SignalKind::Flatten,
                    0.6,
                ));
            }
        }
        Ok(())
    }

    fn on_order_book(&mut self, _ctx: &StrategyContext, _book: &OrderBook) -> StrategyResult<()> {
        Ok(())
    }

    fn on_fill(&mut self, _ctx: &StrategyContext, _fill: &Fill) -> StrategyResult<()> {
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.signals)
    }
}

register_strategy!(CrossExchangeArb, "CrossExchangeArb");

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct VolatilitySkewConfig {
    pub underlying: Symbol,
    pub vol_symbol: Symbol,
    pub atr_period: usize,
    pub implied_premium: Decimal,
    pub realized_multiplier: Decimal,
    pub sniper_timeout_secs: u64,
}

impl Default for VolatilitySkewConfig {
    fn default() -> Self {
        Self {
            underlying: "BTCUSDT".into(),
            vol_symbol: "BVOLUSDT".into(),
            atr_period: 14,
            implied_premium: Decimal::new(15, 2),
            realized_multiplier: Decimal::ONE,
            sniper_timeout_secs: 300,
        }
    }
}

pub struct VolatilitySkew {
    cfg: VolatilitySkewConfig,
    signals: Vec<Signal>,
    atr: Atr,
    last_implied_vol: Option<Decimal>,
}

impl Default for VolatilitySkew {
    fn default() -> Self {
        Self::new(VolatilitySkewConfig::default())
    }
}

impl VolatilitySkew {
    pub fn new(cfg: VolatilitySkewConfig) -> Self {
        let atr = Atr::new(cfg.atr_period).expect("valid ATR");
        Self {
            cfg,
            signals: Vec::new(),
            atr,
            last_implied_vol: None,
        }
    }
}

impl Strategy for VolatilitySkew {
    fn name(&self) -> &str {
        "volatility-skew"
    }

    fn symbol(&self) -> &str {
        &self.cfg.underlying
    }

    fn subscriptions(&self) -> Vec<Symbol> {
        vec![self.cfg.underlying.clone(), self.cfg.vol_symbol.clone()]
    }

    fn configure(&mut self, params: toml::Value) -> StrategyResult<()> {
        let cfg: VolatilitySkewConfig = params.try_into().map_err(|err: toml::de::Error| {
            StrategyError::InvalidConfig(format!("failed to parse VolatilitySkew config: {err}"))
        })?;
        self.atr = Atr::new(cfg.atr_period)
            .map_err(|err| StrategyError::InvalidConfig(err.to_string()))?;
        self.cfg = cfg;
        Ok(())
    }

    fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, _ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if candle.symbol == self.cfg.vol_symbol {
            self.last_implied_vol = Some(candle.close.max(Decimal::ZERO));
            return Ok(());
        }
        if candle.symbol != self.cfg.underlying {
            return Ok(());
        }
        if let Some(realized) = self.atr.next(candle.clone()) {
            if let Some(implied) = self.last_implied_vol {
                if candle.close.is_zero() {
                    return Ok(());
                }
                let realized_vol = realized / candle.close.max(Decimal::ONE);
                let threshold = realized_vol * self.cfg.realized_multiplier;
                let premium =
                    threshold * (Decimal::ONE + self.cfg.implied_premium / Decimal::from(100));
                let discount =
                    threshold * (Decimal::ONE - self.cfg.implied_premium / Decimal::from(100));
                let timeout = Some(Duration::seconds(self.cfg.sniper_timeout_secs as i64));
                if implied >= premium {
                    let mut signal =
                        Signal::new(self.cfg.underlying.clone(), SignalKind::EnterShort, 0.8);
                    signal.execution_hint = Some(ExecutionHint::Sniper {
                        trigger_price: candle.close,
                        timeout,
                    });
                    self.signals.push(signal);
                } else if implied <= discount {
                    let mut signal =
                        Signal::new(self.cfg.underlying.clone(), SignalKind::EnterLong, 0.8);
                    signal.execution_hint = Some(ExecutionHint::Sniper {
                        trigger_price: candle.close,
                        timeout,
                    });
                    self.signals.push(signal);
                }
            }
        }
        Ok(())
    }

    fn on_order_book(&mut self, _ctx: &StrategyContext, _book: &OrderBook) -> StrategyResult<()> {
        Ok(())
    }

    fn on_fill(&mut self, _ctx: &StrategyContext, _fill: &Fill) -> StrategyResult<()> {
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.signals)
    }
}

register_strategy!(VolatilitySkew, "VolatilitySkew");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rsi_handles_constant_input() {
        let mut rsi = Rsi::new(14).unwrap();
        for _ in 0..=14 {
            rsi.next(1.0);
        }
        assert_eq!(rsi.next(1.0), Some(Decimal::from(100)));
    }

    #[test]
    fn bollinger_band_width_decreases_with_low_vol() {
        let mut high_vol = BollingerBands::new(20, Decimal::from(2)).unwrap();
        let mut high_band = None;
        for price in (0..30).map(|i| 100.0 + (i as f64 % 3.0)) {
            high_band = high_vol.next(price);
        }
        let (lower_high, upper_high) = {
            let bands = high_band.expect("high volatility bands present");
            (bands.lower, bands.upper)
        };

        let mut low_vol = BollingerBands::new(20, Decimal::from(2)).unwrap();
        let mut low_band = None;
        for _ in 0..30 {
            low_band = low_vol.next(100.0);
        }
        let (lower_low, upper_low) = {
            let bands = low_band.expect("low volatility bands present");
            (bands.lower, bands.upper)
        };

        assert!((upper_low - lower_low) < (upper_high - lower_high));
    }
}
