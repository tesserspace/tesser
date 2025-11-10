use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::ValueEnum;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use tesser_broker::{ExecutionClient, MarketStream};
use tesser_bybit::{
    BybitClient, BybitConfig, BybitCredentials, BybitMarketStream, BybitSubscription, PublicChannel,
};
use tesser_config::{AlertingConfig, ExchangeConfig};
use tesser_core::{Candle, Fill, Interval, Order, Price, Side, Signal};
use tesser_execution::{ExecutionEngine, FixedOrderSizer};
use tesser_paper::PaperExecutionClient;
use tesser_portfolio::{Portfolio, PortfolioConfig};
use tesser_strategy::{Strategy, StrategyContext};

use crate::alerts::{AlertDispatcher, AlertManager};
use crate::state::{LiveState, LiveStateStore};
use crate::telemetry::{spawn_metrics_server, LiveMetrics};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum ExecutionBackend {
    Paper,
    Live,
}

impl ExecutionBackend {
    fn is_paper(self) -> bool {
        matches!(self, Self::Paper)
    }
}

pub struct LiveSessionSettings {
    pub category: PublicChannel,
    pub interval: Interval,
    pub quantity: f64,
    pub slippage_bps: f64,
    pub fee_bps: f64,
    pub latency_ms: u64,
    pub history: usize,
    pub metrics_addr: SocketAddr,
    pub state_path: PathBuf,
    pub initial_equity: f64,
    pub alerting: AlertingConfig,
    pub exec_backend: ExecutionBackend,
}

pub async fn run_live(
    strategy: Box<dyn Strategy>,
    symbols: Vec<String>,
    exchange: ExchangeConfig,
    settings: LiveSessionSettings,
) -> Result<()> {
    if symbols.is_empty() {
        return Err(anyhow!("strategy did not declare any subscriptions"));
    }
    if settings.quantity <= 0.0 {
        return Err(anyhow!("--quantity must be positive"));
    }

    let mut stream = BybitMarketStream::connect_public(&exchange.ws_url, settings.category)
        .await
        .context("failed to connect to Bybit WebSocket")?;
    for symbol in &symbols {
        stream
            .subscribe(BybitSubscription::Trades {
                symbol: symbol.clone(),
            })
            .await
            .with_context(|| format!("failed to subscribe to trades for {symbol}"))?;
        stream
            .subscribe(BybitSubscription::Kline {
                symbol: symbol.clone(),
                interval: settings.interval,
            })
            .await
            .with_context(|| format!("failed to subscribe to klines for {symbol}"))?;
    }

    let execution_client = build_execution_client(&exchange, &settings)?;
    if matches!(settings.exec_backend, ExecutionBackend::Live) {
        info!(
            rest = %exchange.rest_url,
            category = ?settings.category,
            "live execution enabled via Bybit REST"
        );
    }
    let execution = ExecutionEngine::new(
        execution_client,
        Box::new(FixedOrderSizer {
            quantity: settings.quantity,
        }),
    );

    let runtime = LiveRuntime::new(stream, strategy, symbols, execution, settings).await?;
    runtime.run().await
}

fn build_execution_client(
    exchange: &ExchangeConfig,
    settings: &LiveSessionSettings,
) -> Result<Arc<dyn ExecutionClient>> {
    match settings.exec_backend {
        ExecutionBackend::Paper => Ok(Arc::new(PaperExecutionClient::default())),
        ExecutionBackend::Live => {
            let api_key = exchange.api_key.trim();
            let api_secret = exchange.api_secret.trim();
            if api_key.is_empty() || api_secret.is_empty() {
                bail!("exchange profile is missing api_key/api_secret required for live execution");
            }
            let client = BybitClient::new(
                BybitConfig {
                    base_url: exchange.rest_url.clone(),
                    category: settings.category.as_path().to_string(),
                    recv_window: 5_000,
                },
                Some(BybitCredentials {
                    api_key: api_key.to_string(),
                    api_secret: api_secret.to_string(),
                }),
            );
            Ok(Arc::new(client))
        }
    }
}

struct LiveRuntime {
    stream: BybitMarketStream,
    strategy: Box<dyn Strategy>,
    strategy_ctx: StrategyContext,
    execution: ExecutionEngine,
    portfolio: Portfolio,
    metrics: LiveMetrics,
    alerts: AlertManager,
    market: HashMap<String, MarketSnapshot>,
    fill_model: FillModel,
    latency: Duration,
    state_store: LiveStateStore,
    persisted: LiveState,
    shutdown: ShutdownSignal,
    metrics_task: JoinHandle<()>,
    alert_task: Option<JoinHandle<()>>,
    exec_backend: ExecutionBackend,
}

impl LiveRuntime {
    async fn new(
        stream: BybitMarketStream,
        strategy: Box<dyn Strategy>,
        symbols: Vec<String>,
        execution: ExecutionEngine,
        settings: LiveSessionSettings,
    ) -> Result<Self> {
        let mut strategy_ctx = StrategyContext::new(settings.history);
        let state_store = LiveStateStore::new(settings.state_path.clone());
        let mut persisted = match state_store.load().await {
            Ok(state) => state,
            Err(err) => {
                warn!(error = %err, "failed to load live state; starting from defaults");
                LiveState::default()
            }
        };
        let portfolio = if let Some(snapshot) = persisted.portfolio.take() {
            Portfolio::from_state(snapshot)
        } else {
            Portfolio::new(PortfolioConfig {
                initial_equity: settings.initial_equity,
            })
        };
        strategy_ctx.update_positions(portfolio.positions());

        let mut market = HashMap::new();
        for symbol in &symbols {
            let mut snapshot = MarketSnapshot::default();
            if let Some(price) = persisted.last_prices.get(symbol).copied() {
                snapshot.last_trade = Some(price);
            }
            market.insert(symbol.clone(), snapshot);
        }

        let metrics = LiveMetrics::new();
        let metrics_task = spawn_metrics_server(metrics.registry(), settings.metrics_addr);
        let dispatcher = AlertDispatcher::new(settings.alerting.webhook_url.clone());
        let alerts = AlertManager::new(settings.alerting, dispatcher);
        let alert_task = alerts.spawn_watchdog();
        let shutdown = ShutdownSignal::new();

        info!(symbols = ?symbols, category = ?settings.category, "market stream ready");

        Ok(Self {
            stream,
            strategy,
            strategy_ctx,
            execution,
            portfolio,
            metrics,
            alerts,
            market,
            fill_model: FillModel::new(settings.slippage_bps, settings.fee_bps),
            latency: Duration::from_millis(settings.latency_ms),
            state_store,
            persisted,
            shutdown,
            metrics_task,
            alert_task,
            exec_backend: settings.exec_backend,
        })
    }

    async fn run(mut self) -> Result<()> {
        info!("live session started");
        let backoff = Duration::from_millis(200);
        while !self.shutdown.triggered() {
            let mut progressed = false;

            if let Some(tick) = self.stream.next_tick().await? {
                progressed = true;
                self.handle_tick(tick).await?;
            }

            if let Some(candle) = self.stream.next_candle().await? {
                progressed = true;
                self.handle_candle(candle).await?;
            }

            if !progressed && !self.shutdown.sleep(backoff).await {
                break;
            }
        }
        info!("live session stopping");
        self.metrics_task.abort();
        if let Some(handle) = self.alert_task.take() {
            handle.abort();
        }
        if let Err(err) = self.save_state().await {
            warn!(error = %err, "failed to persist shutdown state");
        }
        Ok(())
    }

    async fn handle_tick(&mut self, tick: tesser_core::Tick) -> Result<()> {
        self.metrics.inc_tick();
        self.metrics.update_staleness(0.0);
        self.alerts.heartbeat().await;
        if let Some(snapshot) = self.market.get_mut(&tick.symbol) {
            snapshot.last_trade = Some(tick.price);
            snapshot.last_trade_ts = Some(tick.exchange_timestamp);
        }
        self.persisted
            .last_prices
            .insert(tick.symbol.clone(), tick.price);
        self.portfolio.mark_price(&tick.symbol, tick.price);
        self.metrics.update_price(&tick.symbol, tick.price);
        self.strategy_ctx.push_tick(tick.clone());
        self.strategy
            .on_tick(&self.strategy_ctx, &tick)
            .context("strategy failure on tick")?;
        self.process_signals().await?;
        Ok(())
    }

    async fn handle_candle(&mut self, candle: Candle) -> Result<()> {
        if let Some(snapshot) = self.market.get_mut(&candle.symbol) {
            snapshot.last_candle = Some(candle.clone());
            snapshot.last_trade = Some(candle.close);
        }
        self.metrics.inc_candle();
        self.metrics.update_price(&candle.symbol, candle.close);
        self.metrics.update_staleness(0.0);
        self.alerts.heartbeat().await;
        self.strategy_ctx.push_candle(candle.clone());
        self.strategy
            .on_candle(&self.strategy_ctx, &candle)
            .context("strategy failure on candle")?;
        self.persisted.last_candle_ts = Some(candle.timestamp);
        self.persisted
            .last_prices
            .insert(candle.symbol.clone(), candle.close);
        self.save_state().await?;
        self.process_signals().await?;
        Ok(())
    }

    async fn process_signals(&mut self) -> Result<()> {
        let signals = self.strategy.drain_signals();
        if signals.is_empty() {
            return Ok(());
        }
        self.metrics.inc_signals(signals.len());
        for signal in signals {
            match self.execution.handle_signal(signal.clone()).await {
                Ok(Some(order)) => {
                    self.metrics.inc_order();
                    let result = if self.exec_backend.is_paper() {
                        self.settle_order(order, signal.clone()).await
                    } else {
                        self.handle_live_submission(order).await
                    };
                    match result {
                        Ok(()) => {
                            self.alerts.reset_order_failures().await;
                        }
                        Err(err) => {
                            warn!(error = %err, symbol = %signal.symbol, "failed to settle order");
                            self.metrics.inc_order_failure();
                            self.alerts
                                .order_failure(&format!("settlement error: {err}"))
                                .await;
                        }
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(error = %err, "execution rejected order");
                    self.metrics.inc_order_failure();
                    self.alerts
                        .order_failure(&format!("execution error: {err}"))
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn handle_live_submission(&mut self, order: Order) -> Result<()> {
        info!(
            order_id = %order.id,
            symbol = %order.request.symbol,
            status = ?order.status,
            "sent live order to external broker"
        );
        self.persisted
            .open_orders
            .retain(|existing| existing.id != order.id);
        self.persisted.open_orders.push(order);
        self.save_state().await
    }

    async fn settle_order(&mut self, order: Order, signal: Signal) -> Result<()> {
        if self.latency.as_millis() > 0 {
            tokio::time::sleep(self.latency).await;
        }
        let price = self
            .market
            .get(&order.request.symbol)
            .and_then(|snap| snap.price())
            .ok_or_else(|| anyhow!("no market price available for {}", order.request.symbol))?;
        let (fill_price, fee) =
            self.fill_model
                .apply(order.request.side, price, order.request.quantity);
        let fill = Fill {
            order_id: order.id.clone(),
            symbol: order.request.symbol.clone(),
            side: order.request.side,
            fill_price,
            fill_quantity: order.request.quantity,
            fee,
            timestamp: Utc::now(),
        };
        self.portfolio
            .apply_fill(&fill)
            .context("failed to update portfolio with fill")?;
        self.strategy_ctx
            .update_positions(self.portfolio.positions());
        self.strategy
            .on_fill(&self.strategy_ctx, &fill)
            .context("strategy failure on fill")?;
        self.metrics.update_equity(self.portfolio.equity());
        self.alerts.update_equity(self.portfolio.equity()).await;
        self.persisted.portfolio = Some(self.portfolio.snapshot());
        self.persisted
            .last_prices
            .insert(order.request.symbol.clone(), fill_price);
        self.save_state().await?;

        debug!(order_id = %order.id, symbol = %signal.symbol, "order settled in paper engine");
        Ok(())
    }

    async fn save_state(&self) -> Result<()> {
        self.state_store.save(&self.persisted).await
    }
}

#[derive(Default)]
struct MarketSnapshot {
    last_trade: Option<Price>,
    last_trade_ts: Option<DateTime<Utc>>,
    last_candle: Option<Candle>,
}

impl MarketSnapshot {
    fn price(&self) -> Option<Price> {
        self.last_trade
            .or_else(|| self.last_candle.as_ref().map(|c| c.close))
    }
}

struct FillModel {
    slippage: f64,
    fee: f64,
}

impl FillModel {
    fn new(slippage_bps: f64, fee_bps: f64) -> Self {
        Self {
            slippage: slippage_bps.max(0.0) / 10_000.0,
            fee: fee_bps.max(0.0) / 10_000.0,
        }
    }

    fn apply(&self, side: Side, price: Price, qty: f64) -> (Price, Option<Price>) {
        let mut filled = price;
        if self.slippage > 0.0 {
            filled *= match side {
                Side::Buy => 1.0 + self.slippage,
                Side::Sell => 1.0 - self.slippage,
            };
        }
        let fee = if self.fee > 0.0 {
            Some(filled.abs() * qty.abs() * self.fee)
        } else {
            None
        };
        (filled, fee)
    }
}

struct ShutdownSignal {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl ShutdownSignal {
    fn new() -> Self {
        let flag = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());
        let flag_clone = flag.clone();
        let notify_clone = notify.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                flag_clone.store(true, Ordering::SeqCst);
                notify_clone.notify_waiters();
            }
        });
        Self { flag, notify }
    }

    fn triggered(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    async fn sleep(&self, duration: Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(duration) => true,
            _ = self.notify.notified() => false,
        }
    }
}
