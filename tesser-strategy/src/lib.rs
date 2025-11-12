//! Strategy trait definitions, shared context, and a portfolio of reference strategies.

extern crate self as tesser_strategy;

pub use tesser_strategy_macros::register_strategy;
pub use toml::Value;

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::sync::{Arc, RwLock};
use tesser_core::{Candle, Fill, OrderBook, Position, Signal, SignalKind, Symbol, Tick};
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

fn collect_symbol_closes(candles: &VecDeque<Candle>, symbol: &str, limit: usize) -> Vec<f64> {
    let mut values: Vec<f64> = candles
        .iter()
        .rev()
        .filter(|c| c.symbol == symbol)
        .take(limit)
        .map(|c| c.close)
        .collect();
    values.reverse();
    values
}

fn sma_series(values: &[f64], period: usize) -> StrategyResult<Vec<f64>> {
    if period == 0 {
        return Err(StrategyError::InvalidConfig(
            "period must be greater than zero".into(),
        ));
    }
    if values.len() < period {
        return Err(StrategyError::NotEnoughData);
    }
    let mut series = Vec::with_capacity(values.len() - period + 1);
    for window in values.windows(period) {
        let mean = window.iter().sum::<f64>() / period as f64;
        series.push(mean);
    }
    Ok(series)
}

fn bollinger_bands(values: &[f64], period: usize, std_mult: f64) -> Option<(f64, f64, f64)> {
    if period == 0 || values.len() < period {
        return None;
    }
    let window = &values[values.len() - period..];
    let mean = window.iter().sum::<f64>() / period as f64;
    let variance = window
        .iter()
        .map(|price| {
            let diff = price - mean;
            diff * diff
        })
        .sum::<f64>()
        / period as f64;
    let std = variance.sqrt();
    Some((mean - std_mult * std, mean + std_mult * std, mean))
}

fn calculate_rsi(values: &[f64], period: usize) -> Option<f64> {
    if period == 0 || values.len() < period + 1 {
        return None;
    }
    let slice = &values[values.len() - (period + 1)..];
    let mut gains = 0.0;
    let mut losses = 0.0;
    for window in slice.windows(2) {
        let change = window[1] - window[0];
        if change >= 0.0 {
            gains += change;
        } else {
            losses -= change;
        }
    }
    let avg_gain = gains / period as f64;
    let avg_loss = losses / period as f64;
    if avg_loss.abs() < f64::EPSILON {
        Some(100.0)
    } else {
        let rs = avg_gain / avg_loss;
        Some(100.0 - 100.0 / (1.0 + rs))
    }
}

fn z_score(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / values.len() as f64;
    let std = variance.sqrt();
    if std.abs() < f64::EPSILON {
        None
    } else {
        Some((values.last().copied().unwrap_or(mean) - mean) / std)
    }
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
}

impl Default for SmaCrossConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            fast_period: 5,
            slow_period: 20,
            min_samples: 25,
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
#[derive(Default)]
pub struct SmaCross {
    cfg: SmaCrossConfig,
    signals: Vec<Signal>,
}

impl SmaCross {
    /// Instantiate the strategy with the provided configuration.
    pub fn new(cfg: SmaCrossConfig) -> Self {
        Self {
            cfg,
            signals: Vec::new(),
        }
    }

    fn maybe_emit_signal(&mut self, ctx: &StrategyContext) -> StrategyResult<()> {
        let closes = collect_symbol_closes(
            ctx.candles(),
            &self.cfg.symbol,
            self.cfg.min_samples.max(self.cfg.slow_period + 1),
        );
        if closes.len() < self.cfg.slow_period + 1 {
            return Ok(());
        }
        let fast = sma_series(&closes, self.cfg.fast_period)?;
        let slow = sma_series(&closes, self.cfg.slow_period)?;
        if let (Some(fast_prev), Some(slow_prev)) = (fast.first(), slow.first()) {
            let fast_last = *fast.last().unwrap_or(fast_prev);
            let slow_last = *slow.last().unwrap_or(slow_prev);
            if fast_prev <= slow_prev && fast_last > slow_last {
                let mut signal = Signal::new(self.cfg.symbol.clone(), SignalKind::EnterLong, 0.75);
                if let Some(last_candle) = ctx.candles().back() {
                    signal.stop_loss = Some(last_candle.low * 0.98);
                }
                self.signals.push(signal);
            } else if fast_prev >= slow_prev && fast_last < slow_last {
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
        Ok(())
    }

    fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if candle.symbol != self.cfg.symbol {
            return Ok(());
        }
        self.maybe_emit_signal(ctx)
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
    pub oversold: f64,
    pub overbought: f64,
    pub lookback: usize,
}

impl Default for RsiReversionConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            period: 14,
            oversold: 30.0,
            overbought: 70.0,
            lookback: 200,
        }
    }
}

#[derive(Default)]
pub struct RsiReversion {
    cfg: RsiReversionConfig,
    signals: Vec<Signal>,
}

impl RsiReversion {
    fn maybe_emit_signal(&mut self, ctx: &StrategyContext) -> StrategyResult<()> {
        let closes = collect_symbol_closes(ctx.candles(), &self.cfg.symbol, self.cfg.lookback);
        let rsi = match calculate_rsi(&closes, self.cfg.period) {
            Some(value) => value,
            None => return Ok(()),
        };
        if rsi <= self.cfg.oversold {
            self.signals.push(Signal::new(
                self.cfg.symbol.clone(),
                SignalKind::EnterLong,
                0.8,
            ));
        } else if rsi >= self.cfg.overbought {
            self.signals.push(Signal::new(
                self.cfg.symbol.clone(),
                SignalKind::ExitLong,
                0.8,
            ));
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
        Ok(())
    }

    fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if candle.symbol != self.cfg.symbol {
            return Ok(());
        }
        self.maybe_emit_signal(ctx)
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
    pub std_multiplier: f64,
    pub lookback: usize,
}

impl Default for BollingerBreakoutConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            period: 20,
            std_multiplier: 2.0,
            lookback: 200,
        }
    }
}

#[derive(Default)]
pub struct BollingerBreakout {
    cfg: BollingerBreakoutConfig,
    signals: Vec<Signal>,
}

impl BollingerBreakout {
    fn maybe_emit_signal(&mut self, ctx: &StrategyContext) -> StrategyResult<()> {
        let closes = collect_symbol_closes(ctx.candles(), &self.cfg.symbol, self.cfg.lookback);
        let (lower, upper, mid) =
            match bollinger_bands(&closes, self.cfg.period, self.cfg.std_multiplier) {
                Some(bands) => bands,
                None => return Ok(()),
            };
        let last_close = closes.last().copied().unwrap_or(mid);
        if last_close > upper {
            self.signals.push(Signal::new(
                self.cfg.symbol.clone(),
                SignalKind::EnterLong,
                0.7,
            ));
        } else if last_close < lower {
            self.signals.push(Signal::new(
                self.cfg.symbol.clone(),
                SignalKind::EnterShort,
                0.7,
            ));
        } else if (last_close - mid).abs() <= (self.cfg.std_multiplier * 0.25) {
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
        Ok(())
    }

    fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if candle.symbol != self.cfg.symbol {
            return Ok(());
        }
        self.maybe_emit_signal(ctx)
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
        let closes = collect_symbol_closes(ctx.candles(), &self.cfg.symbol, self.cfg.lookback + 1);
        if closes.len() < self.cfg.lookback + 1 {
            return None;
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
    pub entry_z: f64,
    pub exit_z: f64,
}

impl Default for PairsTradingConfig {
    fn default() -> Self {
        Self {
            symbols: ["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            lookback: 200,
            entry_z: 2.0,
            exit_z: 0.5,
        }
    }
}

#[derive(Default)]
pub struct PairsTradingArbitrage {
    cfg: PairsTradingConfig,
    signals: Vec<Signal>,
}

impl PairsTradingArbitrage {
    fn spreads(&self, ctx: &StrategyContext) -> Option<Vec<f64>> {
        let closes_a =
            collect_symbol_closes(ctx.candles(), &self.cfg.symbols[0], self.cfg.lookback);
        let closes_b =
            collect_symbol_closes(ctx.candles(), &self.cfg.symbols[1], self.cfg.lookback);
        if closes_a.len() < self.cfg.lookback || closes_b.len() < self.cfg.lookback {
            return None;
        }
        Some(
            closes_a
                .iter()
                .zip(closes_b.iter())
                .map(|(a, b)| (a / b).ln())
                .collect(),
        )
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
        Ok(())
    }

    fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if self.cfg.symbols.contains(&candle.symbol) {
            if let Some(spreads) = self.spreads(ctx) {
                if let Some(z) = z_score(&spreads) {
                    if z >= self.cfg.entry_z {
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
                    } else if z <= -self.cfg.entry_z {
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
                    } else if z.abs() <= self.cfg.exit_z {
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
            if imbalance >= self.cfg.long_threshold {
                self.signals.push(Signal::new(
                    self.cfg.symbol.clone(),
                    SignalKind::EnterLong,
                    0.9,
                ));
            } else if imbalance <= self.cfg.short_threshold {
                self.signals.push(Signal::new(
                    self.cfg.symbol.clone(),
                    SignalKind::EnterShort,
                    0.9,
                ));
            } else if imbalance.abs() <= self.cfg.neutral_zone {
                self.signals.push(Signal::new(
                    self.cfg.symbol.clone(),
                    SignalKind::Flatten,
                    0.6,
                ));
            }
        }
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.signals)
    }
}

register_strategy!(OrderBookImbalance, "OrderBookImbalance", aliases = ["OBI"]);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rsi_handles_constant_input() {
        let data = vec![1.0; 20];
        assert_eq!(calculate_rsi(&data, 14), Some(100.0));
    }

    #[test]
    fn bollinger_band_width_decreases_with_low_vol() {
        let mut data: Vec<f64> = (0..30).map(|i| 100.0 + (i as f64 % 3.0)).collect();
        let (lower_high, upper_high, _) = bollinger_bands(&data, 20, 2.0).expect("bands present");
        data.extend((0..30).map(|_| 100.0));
        let (lower_low, upper_low, _) = bollinger_bands(&data, 20, 2.0).expect("bands present");
        assert!((upper_low - lower_low) < (upper_high - lower_high));
    }
}
