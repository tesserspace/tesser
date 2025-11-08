//! Basic backtesting harness that ties strategies to the paper connector.

use anyhow::Context;
use chrono::Utc;
use tesser_broker::ExecutionClient;
use tesser_core::{Candle, Fill, Quantity, Symbol};
use tesser_execution::ExecutionEngine;
use tesser_portfolio::{Portfolio, PortfolioConfig};
use tesser_strategy::{Strategy, StrategyContext};

/// Configuration used by the backtest harness.
pub struct BacktestConfig {
    pub symbol: Symbol,
    pub candles: Vec<Candle>,
    pub order_quantity: Quantity,
    pub history: usize,
}

impl BacktestConfig {
    /// Convenience constructor for a single symbol.
    pub fn new(symbol: Symbol, candles: Vec<Candle>) -> Self {
        Self {
            symbol,
            candles,
            order_quantity: 1.0,
            history: 512,
        }
    }
}

/// Summary metrics returned after a backtest completes.
pub struct BacktestReport {
    pub signals_emitted: usize,
    pub orders_sent: usize,
    pub ending_equity: f64,
}

/// The engine wiring strategies to execution and portfolio components.
pub struct Backtester<C>
where
    C: ExecutionClient,
{
    config: BacktestConfig,
    strategy: Box<dyn Strategy>,
    strategy_ctx: StrategyContext,
    execution: ExecutionEngine<C>,
    portfolio: Portfolio,
}

impl<C> Backtester<C>
where
    C: ExecutionClient,
{
    /// Construct a new backtester.
    pub fn new(
        config: BacktestConfig,
        strategy: Box<dyn Strategy>,
        execution: ExecutionEngine<C>,
    ) -> Self {
        Self {
            strategy_ctx: StrategyContext::new(config.history),
            portfolio: Portfolio::new(PortfolioConfig::default()),
            config,
            strategy,
            execution,
        }
    }

    /// Execute the backtest using the provided candles.
    pub async fn run(mut self) -> anyhow::Result<BacktestReport> {
        let mut signals_emitted = 0;
        let mut orders_sent = 0;

        for candle in &self.config.candles {
            self.strategy
                .on_candle(&mut self.strategy_ctx, candle)
                .context("strategy failed on candle")?;
            let signals = self.strategy.drain_signals();
            for signal in signals {
                signals_emitted += 1;
                if let Some(order) = self.execution.handle_signal(signal).await? {
                    orders_sent += 1;
                    let fill = Fill {
                        order_id: order.id.clone(),
                        symbol: order.request.symbol.clone(),
                        side: order.request.side,
                        fill_price: candle.close,
                        fill_quantity: order.request.quantity,
                        fee: None,
                        timestamp: Utc::now(),
                    };
                    self.portfolio
                        .apply_fill(&fill)
                        .context("failed to update portfolio")?;
                    self.strategy
                        .on_fill(&mut self.strategy_ctx, &fill)
                        .context("strategy failed on fill")?;
                }
            }
        }

        Ok(BacktestReport {
            signals_emitted,
            orders_sent,
            ending_equity: self.portfolio.equity(),
        })
    }
}
