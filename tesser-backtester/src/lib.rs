//! Basic backtesting harness that ties strategies to the paper connector.

pub mod reporting;

use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::Arc,
};

use anyhow::{anyhow, Context};
use chrono::{DateTime, Utc};
use futures::{stream, Stream, StreamExt};
use reporting::{PerformanceReport, Reporter};
use rust_decimal::Decimal;
use tesser_broker::MarketStream;
use tesser_core::{
    AssetId, Candle, DepthUpdate, Fill, InstrumentKind, Order, OrderBook, Price, Quantity, Side,
    Symbol, Tick,
};
use tesser_data::merger::{UnifiedEvent, UnifiedEventKind};
use tesser_execution::{ExecutionEngine, RiskContext};
use tesser_markets::MarketRegistry;
use tesser_paper::{MatchingEngine, PaperExecutionClient};
use tesser_portfolio::{Portfolio, PortfolioConfig};
use tesser_strategy::{Strategy, StrategyContext};
use tracing::{info, warn};

/// Configuration used by the backtest harness.
pub struct BacktestConfig {
    pub symbol: Symbol,
    pub order_quantity: Quantity,
    pub history: usize,
    pub initial_balances: HashMap<AssetId, Decimal>,
    pub reporting_currency: AssetId,
    pub execution: ExecutionModel,
    pub mode: BacktestMode,
}

impl BacktestConfig {
    /// Convenience constructor for a single symbol.
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            order_quantity: Decimal::ONE,
            history: 512,
            initial_balances: HashMap::from([(AssetId::from("USDT"), Decimal::new(10_000, 0))]),
            reporting_currency: AssetId::from("USDT"),
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
    pub slippage_bps: Decimal,
    /// Trading fee in basis points applied to notional.
    pub fee_bps: Decimal,
    /// Pessimism factor within the OHLC range when simulating fills (0.0-1.0).
    /// For buys, simulate closer to the high; for sells, closer to the low.
    pub pessimism_factor: Decimal,
}

impl Default for ExecutionModel {
    fn default() -> Self {
        Self {
            latency_candles: 1,
            slippage_bps: Decimal::ZERO,
            fee_bps: Decimal::ZERO,
            pessimism_factor: Decimal::new(25, 2), // 0.25
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

impl From<UnifiedEvent> for MarketEvent {
    fn from(event: UnifiedEvent) -> Self {
        let kind = match event.kind {
            UnifiedEventKind::OrderBook(book) => MarketEventKind::OrderBook(book),
            UnifiedEventKind::Depth(update) => MarketEventKind::Depth(update),
            UnifiedEventKind::Trade(trade) => MarketEventKind::Trade(trade),
        };
        Self {
            timestamp: event.timestamp,
            kind,
        }
    }
}

/// Stream type alias used for LOB-driven backtests.
pub type MarketEventStream = Pin<Box<dyn Stream<Item = anyhow::Result<MarketEvent>> + Send>>;

/// Convenience helper to wrap iterators of market events.
pub fn stream_from_events(events: Vec<MarketEvent>) -> MarketEventStream {
    Box::pin(stream::iter(events.into_iter().map(Ok)))
}

/// Summary metrics returned after a backtest completes.
pub struct BacktestReport {
    pub signals_emitted: usize,
    pub orders_sent: usize,
    pub ending_equity: f64,
    pub dropped_orders: usize,
}

/// Trait object used by the streaming backtester to read recorded market data.
pub type BacktestStream = Box<dyn MarketStream<Subscription = ()> + Send + Sync>;

/// The engine wiring strategies to execution and portfolio components.
pub struct Backtester {
    config: BacktestConfig,
    strategy: Box<dyn Strategy>,
    strategy_ctx: StrategyContext,
    execution: ExecutionEngine,
    portfolio: Portfolio,
    pending: VecDeque<PendingFill>,
    matching_engine: Option<Arc<MatchingEngine>>,
    market_stream: Option<BacktestStream>,
    lob_stream: Option<MarketEventStream>,
    candle_index: usize,
    market_registry: Arc<MarketRegistry>,
}

struct PendingFill {
    order: Order,
    due_after: usize,
}

impl Backtester {
    /// Construct a new backtester.
    pub fn new(
        config: BacktestConfig,
        strategy: Box<dyn Strategy>,
        execution: ExecutionEngine,
        matching_engine: Option<Arc<MatchingEngine>>,
        market_registry: Arc<MarketRegistry>,
        market_stream: Option<BacktestStream>,
        lob_stream: Option<MarketEventStream>,
    ) -> Self {
        let portfolio_config = PortfolioConfig {
            initial_balances: config.initial_balances.clone(),
            reporting_currency: config.reporting_currency,
            max_drawdown: None, // Disable liquidate-only for backtests for now
        };
        let mut strategy_ctx = StrategyContext::new(config.history);
        strategy_ctx.attach_market_registry(market_registry.clone());
        Self {
            strategy_ctx,
            portfolio: Portfolio::new(portfolio_config, market_registry.clone()),
            config,
            strategy,
            execution,
            pending: VecDeque::new(),
            matching_engine,
            market_stream,
            lob_stream,
            candle_index: 0,
            market_registry,
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
        let mut equity_curve: Vec<(DateTime<Utc>, Decimal)> = Vec::new();
        let mut all_fills = Vec::new();
        if self.market_stream.is_none() {
            return Err(anyhow!("candle mode requires a market stream"));
        }

        loop {
            let mut progressed = false;

            let tick = {
                let stream = self
                    .market_stream
                    .as_mut()
                    .ok_or_else(|| anyhow!("market stream missing"))?;
                stream.next_tick().await?
            };
            if let Some(tick) = tick {
                self.handle_tick_event(tick)
                    .await
                    .context("failed to process tick event")?;
                progressed = true;
            }

            let candle = {
                let stream = self
                    .market_stream
                    .as_mut()
                    .ok_or_else(|| anyhow!("market stream missing"))?;
                stream.next_candle().await?
            };
            if let Some(candle) = candle {
                self.handle_candle_event(candle, &mut equity_curve, &mut all_fills)
                    .await
                    .context("failed to process candle event")?;
                progressed = true;
            }

            if !progressed {
                break;
            }
        }

        if !self.pending.is_empty() {
            warn!(
                pending = self.pending.len(),
                "pending fills remained when market stream ended"
            );
        }

        let reporter = Reporter::new(self.portfolio.initial_equity(), equity_curve, all_fills);
        reporter.calculate()
    }

    async fn handle_tick_event(&mut self, tick: Tick) -> anyhow::Result<()> {
        self.strategy_ctx.push_tick(tick.clone());
        self.strategy
            .on_tick(&self.strategy_ctx, &tick)
            .await
            .context("strategy failed on tick")?;
        if let Err(err) = self.portfolio.update_market_data(tick.symbol, tick.price) {
            warn!(symbol = %tick.symbol, error = %err, "failed to refresh market data");
        }
        Ok(())
    }

    async fn handle_candle_event(
        &mut self,
        candle: Candle,
        equity_curve: &mut Vec<(DateTime<Utc>, Decimal)>,
        all_fills: &mut Vec<Fill>,
    ) -> anyhow::Result<()> {
        let idx = self.candle_index;
        self.candle_index += 1;

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
                    price = %fill.fill_price,
                    "triggered paper conditional order"
                );
                self.record_fill(&fill, all_fills)
                    .await
                    .context("failed to record triggered fill")?;
            }
        }

        self.process_pending_fills(idx, &candle, all_fills)
            .await
            .context("failed to settle pending fills")?;

        self.strategy_ctx.push_candle(candle.clone());
        self.strategy
            .on_candle(&self.strategy_ctx, &candle)
            .await
            .context("strategy failed on candle")?;
        let signals = self.strategy.drain_signals();
        for signal in signals {
            let Some(instrument) = self.market_registry.get(signal.symbol) else {
                warn!(symbol = %signal.symbol, "instrument metadata missing; skipping signal");
                continue;
            };
            let base_available = self
                .portfolio
                .balance(instrument.base)
                .map(|cash| cash.quantity)
                .unwrap_or_default();
            let quote_available = self
                .portfolio
                .balance(instrument.quote)
                .map(|cash| cash.quantity)
                .unwrap_or_default();
            let settlement_available = self
                .portfolio
                .balance(instrument.settlement_currency)
                .map(|cash| cash.quantity)
                .unwrap_or_default();
            let ctx = RiskContext {
                symbol: signal.symbol,
                exchange: signal.symbol.exchange,
                signed_position_qty: self.portfolio.signed_position_qty(signal.symbol),
                portfolio_equity: self.portfolio.equity(),
                exchange_equity: self.portfolio.exchange_equity(signal.symbol.exchange),
                last_price: candle.close,
                liquidate_only: false,
                instrument_kind: Some(instrument.kind),
                base_asset: instrument.base,
                quote_asset: instrument.quote,
                settlement_asset: instrument.settlement_currency,
                base_available,
                quote_available,
                settlement_available,
            };
            if let Some(order) = self.execution.handle_signal(signal, ctx).await? {
                let latency = self.config.execution.latency_candles.max(1);
                let due_after = idx.saturating_add(latency);
                self.pending.push_back(PendingFill { order, due_after });
            }
        }

        if let Err(err) = self
            .portfolio
            .update_market_data(candle.symbol, candle.close)
        {
            warn!(
                symbol = %candle.symbol,
                error = %err,
                "failed to refresh market data"
            );
        }

        let equity = self.portfolio.equity();
        equity_curve.push((candle.timestamp, equity));
        Ok(())
    }

    async fn process_pending_fills(
        &mut self,
        candle_index: usize,
        candle: &Candle,
        all_fills: &mut Vec<Fill>,
    ) -> anyhow::Result<()> {
        let mut remaining = VecDeque::new();
        while let Some(pending) = self.pending.pop_front() {
            if pending.due_after <= candle_index {
                let fill = self.build_fill(&pending.order, candle);
                self.record_fill(&fill, all_fills)
                    .await
                    .context("failed to record pending fill")?;
            } else {
                remaining.push_back(pending);
            }
        }
        self.pending = remaining;
        Ok(())
    }

    fn build_fill(&self, order: &Order, candle: &Candle) -> Fill {
        // Price within the candle's OHLC band, biased pessimistically
        let factor = self
            .config
            .execution
            .pessimism_factor
            .max(Decimal::ZERO)
            .min(Decimal::ONE);
        let mut price = match order.request.side {
            Side::Buy => {
                let band = (candle.high - candle.open).max(Decimal::ZERO);
                candle.open + band * factor
            }
            Side::Sell => {
                let band = (candle.open - candle.low).max(Decimal::ZERO);
                candle.open - band * factor
            }
        };
        let slippage_rate =
            self.config.execution.slippage_bps.max(Decimal::ZERO) / Decimal::from(10_000);
        if slippage_rate > Decimal::ZERO {
            let multiplier = match order.request.side {
                Side::Buy => Decimal::ONE + slippage_rate,
                Side::Sell => Decimal::ONE - slippage_rate,
            };
            price *= multiplier;
        }
        let fee_rate = self.config.execution.fee_bps.max(Decimal::ZERO) / Decimal::from(10_000);
        let notional = price * order.request.quantity.abs();
        let fee = if fee_rate > Decimal::ZERO {
            Some(notional * fee_rate)
        } else {
            None
        };
        let fee_asset = self
            .market_registry
            .get(order.request.symbol)
            .map(|instrument| match instrument.kind {
                InstrumentKind::Spot => instrument.quote,
                _ => instrument.settlement_currency,
            });
        Fill {
            order_id: order.id.clone(),
            symbol: order.request.symbol,
            side: order.request.side,
            fill_price: price,
            fill_quantity: order.request.quantity,
            fee,
            fee_asset,
            timestamp: candle.timestamp,
        }
    }

    async fn run_tick(&mut self) -> anyhow::Result<PerformanceReport> {
        let matching = self
            .matching_engine
            .clone()
            .ok_or_else(|| anyhow!("tick mode requires a matching engine"))?;
        let mut equity_curve: Vec<(DateTime<Utc>, Decimal)> = Vec::new();
        let mut all_fills = Vec::new();
        let mut last_trade_price: Option<Price> = None;

        loop {
            let next_event = {
                let stream = self
                    .lob_stream
                    .as_mut()
                    .ok_or_else(|| anyhow!("tick mode requires a market event stream"))?;
                stream.next().await
            };
            let Some(event) = next_event else {
                break;
            };
            let event = event?;
            matching.advance_time(event.timestamp).await;
            match &event.kind {
                MarketEventKind::OrderBook(book) => {
                    matching.load_market_snapshot(book);
                    self.strategy_ctx.push_order_book(book.clone());
                    self.strategy
                        .on_order_book(&self.strategy_ctx, book)
                        .await
                        .context("strategy failed on order book event")?;
                }
                MarketEventKind::Depth(update) => {
                    matching.apply_depth_update(update);
                }
                MarketEventKind::Trade(tick) => {
                    matching
                        .process_trade(tick.side, tick.price, tick.size, tick.exchange_timestamp)
                        .await;
                    last_trade_price = Some(tick.price);
                    if let Err(err) = self.portfolio.update_market_data(tick.symbol, tick.price) {
                        warn!(symbol = %tick.symbol, error = %err, "failed to refresh market data");
                    }
                    self.strategy_ctx.push_tick(tick.clone());
                    self.strategy
                        .on_tick(&self.strategy_ctx, tick)
                        .await
                        .context("strategy failed on tick event")?;
                }
            }

            self.consume_matching_fills(&mut all_fills).await?;
            self.process_signals_tick(last_trade_price.or_else(|| matching.mid_price()))
                .await?;
            self.consume_matching_fills(&mut all_fills).await?;
            let equity = self.portfolio.equity();
            equity_curve.push((event.timestamp, equity));
        }

        let reporter = Reporter::new(self.portfolio.initial_equity(), equity_curve, all_fills);
        reporter.calculate()
    }

    async fn process_signals_tick(&mut self, fallback_price: Option<Price>) -> anyhow::Result<()> {
        let signals = self.strategy.drain_signals();
        for signal in signals {
            let reference_price = self
                .last_tick_price(&signal.symbol)
                .or(fallback_price)
                .unwrap_or(Decimal::ZERO);
            let Some(instrument) = self.market_registry.get(signal.symbol) else {
                warn!(symbol = %signal.symbol, "instrument metadata missing; skipping signal");
                continue;
            };
            let base_available = self
                .portfolio
                .balance(instrument.base)
                .map(|cash| cash.quantity)
                .unwrap_or_default();
            let quote_available = self
                .portfolio
                .balance(instrument.quote)
                .map(|cash| cash.quantity)
                .unwrap_or_default();
            let settlement_available = self
                .portfolio
                .balance(instrument.settlement_currency)
                .map(|cash| cash.quantity)
                .unwrap_or_default();
            let ctx = RiskContext {
                symbol: signal.symbol,
                exchange: signal.symbol.exchange,
                signed_position_qty: self.portfolio.signed_position_qty(signal.symbol),
                portfolio_equity: self.portfolio.equity(),
                exchange_equity: self.portfolio.exchange_equity(signal.symbol.exchange),
                last_price: reference_price,
                liquidate_only: false,
                instrument_kind: Some(instrument.kind),
                base_asset: instrument.base,
                quote_asset: instrument.quote,
                settlement_asset: instrument.settlement_currency,
                base_available,
                quote_available,
                settlement_available,
            };
            let _ = self.execution.handle_signal(signal, ctx).await?;
        }
        Ok(())
    }

    fn last_tick_price(&self, symbol: &Symbol) -> Option<Price> {
        self.strategy_ctx
            .ticks()
            .iter()
            .rev()
            .find(|tick| tick.symbol == *symbol)
            .map(|tick| tick.price)
    }

    async fn consume_matching_fills(&mut self, all_fills: &mut Vec<Fill>) -> anyhow::Result<()> {
        if let Some(engine) = &self.matching_engine {
            let fills = engine.drain_fills().await;
            for fill in fills {
                self.record_fill(&fill, all_fills)
                    .await
                    .context("failed to record matching fill")?;
            }
        }
        Ok(())
    }

    async fn record_fill(&mut self, fill: &Fill, all_fills: &mut Vec<Fill>) -> anyhow::Result<()> {
        self.portfolio
            .apply_fill(fill)
            .context("failed to update portfolio with fill")?;
        all_fills.push(fill.clone());
        self.strategy_ctx
            .update_positions(self.portfolio.positions());
        self.strategy
            .on_fill(&self.strategy_ctx, fill)
            .await
            .context("strategy failed on fill event")?;
        Ok(())
    }
}
