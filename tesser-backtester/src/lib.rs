//! Basic backtesting harness that ties strategies to the paper connector.

pub mod reporting;

use std::collections::VecDeque;

use anyhow::Context;
use reporting::{PerformanceReport, Reporter};
use tesser_core::{Candle, Fill, Order, Quantity, Side, Symbol};
use tesser_execution::{ExecutionEngine, RiskContext};
use tesser_paper::PaperExecutionClient;
use tesser_portfolio::{Portfolio, PortfolioConfig};
use tesser_strategy::{Strategy, StrategyContext};
use tracing::{info, warn};

/// Configuration used by the backtest harness.
pub struct BacktestConfig {
    pub symbol: Symbol,
    pub candles: Vec<Candle>,
    pub order_quantity: Quantity,
    pub history: usize,
    pub initial_equity: f64,
    pub execution: ExecutionModel,
}

impl BacktestConfig {
    /// Convenience constructor for a single symbol.
    pub fn new(symbol: Symbol, candles: Vec<Candle>) -> Self {
        Self {
            symbol,
            candles,
            order_quantity: 1.0,
            history: 512,
            initial_equity: 10_000.0,
            execution: ExecutionModel::default(),
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
}

impl Default for ExecutionModel {
    fn default() -> Self {
        Self {
            latency_candles: 1,
            slippage_bps: 0.0,
            fee_bps: 0.0,
        }
    }
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
        }
    }

    /// Execute the backtest using the provided candles.
    pub async fn run(mut self) -> anyhow::Result<PerformanceReport> {
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
                    self.portfolio
                        .apply_fill(&fill)
                        .context("failed to update portfolio with triggered fill")?;
                    all_fills.push(fill.clone());
                    self.strategy_ctx
                        .update_positions(self.portfolio.positions());
                    self.strategy
                        .on_fill(&self.strategy_ctx, &fill)
                        .context("strategy failed on triggered fill event")?;
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
                self.portfolio
                    .apply_fill(&fill)
                    .context("failed to update portfolio with fill")?;
                all_fills.push(fill.clone());
                self.strategy_ctx
                    .update_positions(self.portfolio.positions());
                self.strategy
                    .on_fill(&self.strategy_ctx, &fill)
                    .context("strategy failed on fill event")?;
            } else {
                remaining.push_back(pending);
            }
        }
        self.pending = remaining;
        Ok(())
    }

    fn build_fill(&self, order: &Order, candle: &Candle) -> Fill {
        let mut price = candle.open;
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
}
