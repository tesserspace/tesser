//! Basic backtesting harness that ties strategies to the paper connector.

pub mod reporting;

use std::{collections::VecDeque, sync::Arc};

use anyhow::{anyhow, Context};
use chrono::{DateTime, Utc};
use reporting::{PerformanceReport, Reporter};
use tesser_core::{
    Candle, DepthUpdate, Fill, Order, OrderBook, Price, Quantity, Side, Symbol, Tick,
};
use tesser_execution::{ExecutionEngine, RiskContext};
use tesser_paper::{MatchingEngine, PaperExecutionClient};
use tesser_portfolio::{Portfolio, PortfolioConfig};
use tesser_strategy::{Strategy, StrategyContext};
use tracing::{info, warn};

/// Configuration used by the backtest harness.
pub struct BacktestConfig {
    pub symbol: Symbol,
    pub candles: Vec<Candle>,
    pub lob_events: Vec<MarketEvent>,
    pub order_quantity: Quantity,
    pub history: usize,
    pub initial_equity: f64,
    pub execution: ExecutionModel,
    pub mode: BacktestMode,
}

impl BacktestConfig {
    /// Convenience constructor for a single symbol.
    pub fn new(symbol: Symbol, candles: Vec<Candle>) -> Self {
        Self {
            symbol,
            candles,
            lob_events: Vec::new(),
            order_quantity: 1.0,
            history: 512,
            initial_equity: 10_000.0,
            execution: ExecutionModel::default(),
            mode: BacktestMode::Candle,
        }
    }
}

/// Controls how fills are simulated during the backtest.
#[derive(Clone, Copy, Debug)]
pub struct ExecutionModel {
    /// Number of candles to wait before an order is eligible for execution (minimum 1).
    pub latency_candles: usize,
    /// Symmetric slippage applied in basis points (1 bp = 0.01%).
    pub slippage_bps: f64,
    /// Trading fee in basis points applied to notional.
    pub fee_bps: f64,
    /// Pessimism factor within the OHLC range when simulating fills (0.0-1.0).
    /// For buys, simulate closer to the high; for sells, closer to the low.
    pub pessimism_factor: f64,
}

impl Default for ExecutionModel {
    fn default() -> Self {
        Self {
            latency_candles: 1,
            slippage_bps: 0.0,
            fee_bps: 0.0,
            pessimism_factor: 0.25,
        }
    }
}

/// Determines how the backtester consumes historical data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum BacktestMode {
    #[default]
    Candle,
    Tick,
}

/// Event emitted by the high-fidelity data source.
#[derive(Clone, Debug)]
pub struct MarketEvent {
    pub timestamp: DateTime<Utc>,
    pub kind: MarketEventKind,
}

/// Individual event types used by the matching-engine replay.
#[derive(Clone, Debug)]
pub enum MarketEventKind {
    OrderBook(OrderBook),
    Depth(DepthUpdate),
    Trade(Tick),
}

/// Summary metrics returned after a backtest completes.
pub struct BacktestReport {
    pub signals_emitted: usize,
    pub orders_sent: usize,
    pub ending_equity: f64,
    pub dropped_orders: usize,
}

/// The engine wiring strategies to execution and portfolio components.
pub struct Backtester {
    config: BacktestConfig,
    strategy: Box<dyn Strategy>,
    strategy_ctx: StrategyContext,
    execution: ExecutionEngine,
    portfolio: Portfolio,
    pending: VecDeque<PendingFill>,
    matching_engine: Option<Arc<MatchingEngine>>,
}

struct PendingFill {
    order: Order,
    due_index: usize,
}

impl Backtester {
    /// Construct a new backtester.
    pub fn new(
        config: BacktestConfig,
        strategy: Box<dyn Strategy>,
        execution: ExecutionEngine,
        matching_engine: Option<Arc<MatchingEngine>>,
    ) -> Self {
        let portfolio_config = PortfolioConfig {
            initial_equity: config.initial_equity,
            max_drawdown: None, // Disable liquidate-only for backtests for now
        };
        Self {
            strategy_ctx: StrategyContext::new(config.history),
            portfolio: Portfolio::new(portfolio_config),
            config,
            strategy,
            execution,
            pending: VecDeque::new(),
            matching_engine,
        }
    }

    /// Execute the backtest using the provided candles.
    pub async fn run(mut self) -> anyhow::Result<PerformanceReport> {
        match self.config.mode {
            BacktestMode::Candle => self.run_candle().await,
            BacktestMode::Tick => self.run_tick().await,
        }
    }

    async fn run_candle(&mut self) -> anyhow::Result<PerformanceReport> {
        let mut equity_curve = Vec::new();
        let mut all_fills = Vec::new();

        for idx in 0..self.config.candles.len() {
            let candle = self.config.candles[idx].clone();

            if let Some(paper_client) = self
                .execution
                .client()
                .as_any()
                .downcast_ref::<PaperExecutionClient>()
            {
                let triggered_fills = paper_client
                    .check_triggers(&candle)
                    .await
                    .context("failed to check paper triggers")?;
                for fill in triggered_fills {
                    info!(
                        order_id = %fill.order_id,
                        price = fill.fill_price,
                        "triggered paper conditional order"
                    );
                    self.record_fill(&fill, &mut all_fills)?;
                }
            }

            self.process_pending_fills(idx, &candle, &mut all_fills)
                .context("failed to settle pending fills")?;

            self.strategy_ctx.push_candle(candle.clone());
            self.strategy
                .on_candle(&self.strategy_ctx, &candle)
                .context("strategy failed on candle")?;
            let signals = self.strategy.drain_signals();
            for signal in signals {
                let ctx = RiskContext {
                    signed_position_qty: self.portfolio.signed_position_qty(&signal.symbol),
                    portfolio_equity: self.portfolio.equity(),
                    last_price: candle.close,
                    liquidate_only: false,
                };
                if let Some(order) = self.execution.handle_signal(signal, ctx).await? {
                    let latency = self.config.execution.latency_candles.max(1);
                    let due_index = idx + latency;
                    if due_index >= self.config.candles.len() {
                        warn!(
                            order_id = %order.id,
                            due_index,
                            "dropping order; not enough candles remain to honor latency"
                        );
                        continue;
                    }
                    self.pending.push_back(PendingFill { order, due_index });
                }
            }

            equity_curve.push((candle.timestamp, self.portfolio.equity()));
        }

        let reporter = Reporter::new(self.config.initial_equity, equity_curve, all_fills);
        reporter.calculate()
    }

    fn process_pending_fills(
        &mut self,
        candle_index: usize,
        candle: &Candle,
        all_fills: &mut Vec<Fill>,
    ) -> anyhow::Result<()> {
        let mut remaining = VecDeque::new();
        while let Some(pending) = self.pending.pop_front() {
            if pending.due_index == candle_index {
                let fill = self.build_fill(&pending.order, candle);
                self.record_fill(&fill, all_fills)?;
            } else {
                remaining.push_back(pending);
            }
        }
        self.pending = remaining;
        Ok(())
    }

    fn build_fill(&self, order: &Order, candle: &Candle) -> Fill {
        // Price within the candle's OHLC band, biased pessimistically
        let factor = self.config.execution.pessimism_factor.clamp(0.0, 1.0);
        let mut price = match order.request.side {
            Side::Buy => {
                let band = (candle.high - candle.open).max(0.0);
                candle.open + band * factor
            }
            Side::Sell => {
                let band = (candle.open - candle.low).max(0.0);
                candle.open - band * factor
            }
        };
        let slippage_rate = self.config.execution.slippage_bps / 10_000.0;
        if slippage_rate > 0.0 {
            price *= match order.request.side {
                Side::Buy => 1.0 + slippage_rate,
                Side::Sell => 1.0 - slippage_rate,
            };
        }
        let fee_rate = self.config.execution.fee_bps / 10_000.0;
        let notional = price * order.request.quantity.abs();
        let fee = if fee_rate > 0.0 {
            Some(notional * fee_rate)
        } else {
            None
        };
        Fill {
            order_id: order.id.clone(),
            symbol: order.request.symbol.clone(),
            side: order.request.side,
            fill_price: price,
            fill_quantity: order.request.quantity,
            fee,
            timestamp: candle.timestamp,
        }
    }

    async fn run_tick(&mut self) -> anyhow::Result<PerformanceReport> {
        let matching = self
            .matching_engine
            .clone()
            .ok_or_else(|| anyhow!("tick mode requires a matching engine"))?;
        let mut equity_curve = Vec::new();
        let mut all_fills = Vec::new();
        let mut last_trade_price: Option<Price> = None;

        let events = self.config.lob_events.clone();
        for event in events {
            match &event.kind {
                MarketEventKind::OrderBook(book) => {
                    matching.load_market_snapshot(book);
                    self.strategy_ctx.push_order_book(book.clone());
                    self.strategy
                        .on_order_book(&self.strategy_ctx, book)
                        .context("strategy failed on order book event")?;
                }
                MarketEventKind::Depth(update) => {
                    matching.apply_depth_update(update);
                }
                MarketEventKind::Trade(tick) => {
                    matching
                        .process_trade(tick.side, tick.price, tick.size)
                        .await;
                    last_trade_price = Some(tick.price);
                    self.portfolio.mark_price(&tick.symbol, tick.price);
                    self.strategy_ctx.push_tick(tick.clone());
                    self.strategy
                        .on_tick(&self.strategy_ctx, tick)
                        .context("strategy failed on tick event")?;
                }
            }

            self.consume_matching_fills(&mut all_fills).await?;
            self.process_signals_tick(last_trade_price.or_else(|| matching.mid_price()))
                .await?;
            self.consume_matching_fills(&mut all_fills).await?;
            equity_curve.push((event.timestamp, self.portfolio.equity()));
        }

        let reporter = Reporter::new(self.config.initial_equity, equity_curve, all_fills);
        reporter.calculate()
    }

    async fn process_signals_tick(&mut self, fallback_price: Option<Price>) -> anyhow::Result<()> {
        let signals = self.strategy.drain_signals();
        for signal in signals {
            let reference_price = self
                .last_tick_price(&signal.symbol)
                .or(fallback_price)
                .unwrap_or(0.0);
            let ctx = RiskContext {
                signed_position_qty: self.portfolio.signed_position_qty(&signal.symbol),
                portfolio_equity: self.portfolio.equity(),
                last_price: reference_price,
                liquidate_only: false,
            };
            let _ = self.execution.handle_signal(signal, ctx).await?;
        }
        Ok(())
    }

    fn last_tick_price(&self, symbol: &str) -> Option<Price> {
        self.strategy_ctx
            .ticks()
            .iter()
            .rev()
            .find(|tick| tick.symbol == symbol)
            .map(|tick| tick.price)
    }

    async fn consume_matching_fills(&mut self, all_fills: &mut Vec<Fill>) -> anyhow::Result<()> {
        if let Some(engine) = &self.matching_engine {
            let fills = engine.drain_fills().await;
            for fill in fills {
                self.record_fill(&fill, all_fills)?;
            }
        }
        Ok(())
    }

    fn record_fill(&mut self, fill: &Fill, all_fills: &mut Vec<Fill>) -> anyhow::Result<()> {
        self.portfolio
            .apply_fill(fill)
            .context("failed to update portfolio with fill")?;
        all_fills.push(fill.clone());
        self.strategy_ctx
            .update_positions(self.portfolio.positions());
        self.strategy
            .on_fill(&self.strategy_ctx, fill)
            .context("strategy failed on fill event")?;
        Ok(())
    }
}
