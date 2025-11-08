//! Strategy trait definitions and reusable helpers.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};
use tesser_core::{Candle, Fill, Signal, Symbol, Tick};
use thiserror::Error;

/// Result alias used within strategy implementations.
pub type StrategyResult<T> = Result<T, StrategyError>;

/// Failure variants that strategies are expected to surface.
#[derive(Debug, Error)]
pub enum StrategyError {
    /// Raised when required inputs are missing (e.g., insufficient data history).
    #[error("strategy not ready: {0}")]
    NotReady(String),
    /// Covers bookkeeping issues.
    #[error("state error: {0}")]
    State(String),
    /// Raised when user-provided configuration is invalid.
    #[error("invalid parameter: {0}")]
    InvalidParameter(String),
    /// Generic wrapper for other error types.
    #[error("unhandled error: {0}")]
    Other(String),
}

/// Strategy lifecycle hooks.
pub trait Strategy: Send + Sync {
    /// Human-friendly identifier used in logs and telemetry.
    fn name(&self) -> &str;

    /// The market symbol this strategy operates on.
    fn symbol(&self) -> &str;

    /// Called whenever the data pipeline emits a new tick.
    fn on_tick(&mut self, ctx: &mut StrategyContext, tick: &Tick) -> StrategyResult<()>;

    /// Called whenever a candle is produced or replayed.
    fn on_candle(&mut self, ctx: &mut StrategyContext, candle: &Candle) -> StrategyResult<()>;

    /// Called whenever one of the strategy's orders is filled.
    fn on_fill(&mut self, ctx: &mut StrategyContext, fill: &Fill) -> StrategyResult<()>;

    /// Allows the strategy to emit one or more signals after processing events.
    fn drain_signals(&mut self) -> Vec<Signal>;
}

/// Minimal context shared with strategies so they can inspect recent data.
#[derive(Default)]
pub struct StrategyContext {
    recent_ticks: VecDeque<Tick>,
    recent_candles: VecDeque<Candle>,
    max_history: usize,
}

impl StrategyContext {
    /// Create a new context with history capacity.
    pub fn new(max_history: usize) -> Self {
        Self {
            recent_ticks: VecDeque::with_capacity(max_history),
            recent_candles: VecDeque::with_capacity(max_history),
            max_history,
        }
    }

    /// Push a tick while respecting history capacity.
    pub fn push_tick(&mut self, tick: Tick) {
        if self.recent_ticks.len() >= self.max_history {
            self.recent_ticks.pop_front();
        }
        self.recent_ticks.push_back(tick);
    }

    /// Push a candle while respecting history capacity.
    pub fn push_candle(&mut self, candle: Candle) {
        if self.recent_candles.len() >= self.max_history {
            self.recent_candles.pop_front();
        }
        self.recent_candles.push_back(candle);
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
}

/// Example configuration for a moving-average crossover strategy.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SmaCrossConfig {
    pub symbol: Symbol,
    pub fast_period: usize,
    pub slow_period: usize,
    pub min_samples: usize,
}

/// Very small reference implementation that can be expanded later.
pub struct SmaCross {
    cfg: SmaCrossConfig,
    signals: Vec<Signal>,
}

impl SmaCross {
    /// Instantiate a new SMA cross strategy.
    pub fn new(cfg: SmaCrossConfig) -> Self {
        Self {
            cfg,
            signals: Vec::new(),
        }
    }

    fn maybe_emit_signal(&mut self, candles: &VecDeque<Candle>) -> StrategyResult<()> {
        if candles.len() < self.cfg.min_samples {
            return Err(StrategyError::NotReady(format!(
                "waiting for {} candles (have {})",
                self.cfg.min_samples,
                candles.len()
            )));
        }

        let fast = Self::sma(candles, self.cfg.fast_period)?;
        let slow = Self::sma(candles, self.cfg.slow_period)?;

        if let (Some(fast_prev), Some(slow_prev)) = (fast.first().copied(), slow.first().copied()) {
            let fast_last = *fast.last().unwrap_or(&fast_prev);
            let slow_last = *slow.last().unwrap_or(&slow_prev);
            if fast_prev <= slow_prev && fast_last > slow_last {
                self.signals.push(Signal::new(
                    self.cfg.symbol.clone(),
                    tesser_core::SignalKind::EnterLong,
                    0.75,
                ));
            } else if fast_prev >= slow_prev && fast_last < slow_last {
                self.signals.push(Signal::new(
                    self.cfg.symbol.clone(),
                    tesser_core::SignalKind::ExitLong,
                    0.75,
                ));
            }
        }
        Ok(())
    }

    fn sma(candles: &VecDeque<Candle>, period: usize) -> StrategyResult<Vec<f64>> {
        if period == 0 {
            return Err(StrategyError::InvalidParameter(
                "period must be greater than zero".into(),
            ));
        }
        let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
        if closes.len() < period {
            return Err(StrategyError::NotReady(format!(
                "need {period} closes, have {}",
                closes.len()
            )));
        }
        let mut values = Vec::with_capacity(closes.len() - period + 1);
        for window in closes.windows(period) {
            values.push(window.iter().sum::<f64>() / period as f64);
        }
        Ok(values)
    }
}

impl Strategy for SmaCross {
    fn name(&self) -> &str {
        "sma-cross"
    }

    fn symbol(&self) -> &str {
        &self.cfg.symbol
    }

    fn on_tick(&mut self, _ctx: &mut StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, ctx: &mut StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if candle.symbol != self.cfg.symbol {
            return Ok(());
        }
        ctx.push_candle(candle.clone());
        self.maybe_emit_signal(ctx.candles())
    }

    fn on_fill(&mut self, _ctx: &mut StrategyContext, _fill: &Fill) -> StrategyResult<()> {
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.signals)
    }
}
