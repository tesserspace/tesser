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
use futures::StreamExt;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use tesser_broker::{ExecutionClient, MarketStream};
use tesser_bybit::ws::{BybitWsExecution, BybitWsOrder, PrivateMessage};
use tesser_bybit::{
    BybitClient, BybitConfig, BybitCredentials, BybitMarketStream, BybitSubscription, PublicChannel,
};
use tesser_config::{AlertingConfig, ExchangeConfig, RiskManagementConfig};
use tesser_core::{Candle, Fill, Interval, Order, OrderStatus, Price, Side};
use tesser_execution::{
    BasicRiskChecker, ExecutionEngine, FixedOrderSizer, OrderOrchestrator, PreTradeRiskChecker,
    RiskContext, RiskLimits, SqliteAlgoStateRepository,
};
use tesser_paper::PaperExecutionClient;
use tesser_portfolio::{
    LiveState, Portfolio, PortfolioConfig, SqliteStateRepository, StateRepository,
};
use tesser_strategy::{Strategy, StrategyContext};

use crate::alerts::{AlertDispatcher, AlertManager};
use crate::telemetry::{spawn_metrics_server, LiveMetrics};

/// Unified event type for asynchronous updates from the broker.
#[derive(Debug)]
pub enum BrokerEvent {
    OrderUpdate(Order),
    Fill(Fill),
}

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
    pub history: usize,
    pub metrics_addr: SocketAddr,
    pub state_path: PathBuf,
    pub initial_equity: f64,
    pub alerting: AlertingConfig,
    pub exec_backend: ExecutionBackend,
    pub risk: RiskManagementConfig,
}

impl LiveSessionSettings {
    fn risk_limits(&self) -> RiskLimits {
        RiskLimits {
            max_order_quantity: self.risk.max_order_quantity.max(0.0),
            max_position_quantity: self.risk.max_position_quantity.max(0.0),
        }
    }
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
    let risk_checker: Arc<dyn PreTradeRiskChecker> =
        Arc::new(BasicRiskChecker::new(settings.risk_limits()));
    let execution = ExecutionEngine::new(
        execution_client,
        Box::new(FixedOrderSizer {
            quantity: settings.quantity,
        }),
        risk_checker,
    );

    // Create algorithm state repository
    let algo_repo_path = settings.state_path.with_extension("algos.db");
    let algo_state_repo = Arc::new(SqliteAlgoStateRepository::new(&algo_repo_path)?);

    // Create orchestrator with execution engine
    let orchestrator = OrderOrchestrator::new(Arc::new(execution), algo_state_repo).await?;

    let runtime = LiveRuntime::new(stream, strategy, symbols, orchestrator, settings).await?;
    runtime.run().await
}

fn build_execution_client(
    exchange: &ExchangeConfig,
    settings: &LiveSessionSettings,
) -> Result<Arc<dyn ExecutionClient>> {
    match settings.exec_backend {
        ExecutionBackend::Paper => Ok(Arc::new(PaperExecutionClient::new(
            "paper".to_string(),
            vec!["BTCUSDT".to_string()],
            settings.slippage_bps,
            settings.fee_bps,
        ))),
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
    orchestrator: OrderOrchestrator,
    portfolio: Portfolio,
    metrics: LiveMetrics,
    alerts: AlertManager,
    market: HashMap<String, MarketSnapshot>,
    state_repo: Arc<dyn StateRepository>,
    persisted: LiveState,
    shutdown: ShutdownSignal,
    metrics_task: JoinHandle<()>,
    alert_task: Option<JoinHandle<()>>,
    private_event_rx: mpsc::Receiver<BrokerEvent>,
    exec_backend: ExecutionBackend,
}

impl LiveRuntime {
    async fn new(
        stream: BybitMarketStream,
        strategy: Box<dyn Strategy>,
        symbols: Vec<String>,
        orchestrator: OrderOrchestrator,
        settings: LiveSessionSettings,
    ) -> Result<Self> {
        let mut strategy_ctx = StrategyContext::new(settings.history);
        let state_repo: Arc<dyn StateRepository> =
            Arc::new(SqliteStateRepository::new(settings.state_path.clone()));
        let mut persisted = match tokio::task::spawn_blocking({
            let repo = state_repo.clone();
            move || repo.load()
        })
        .await
        {
            Ok(Ok(state)) => state,
            Ok(Err(err)) => {
                warn!(error = %err, "failed to load live state; starting from defaults");
                LiveState::default()
            }
            Err(err) => {
                warn!(error = %err, "state load task failed; starting from defaults");
                LiveState::default()
            }
        };
        let portfolio_cfg = PortfolioConfig {
            initial_equity: settings.initial_equity,
            max_drawdown: Some(settings.risk.max_drawdown),
        };
        let portfolio = if let Some(snapshot) = persisted.portfolio.take() {
            Portfolio::from_state(snapshot, portfolio_cfg.clone())
        } else {
            Portfolio::new(portfolio_cfg.clone())
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
        let (private_event_tx, private_event_rx) = mpsc::channel(1024);

        if !settings.exec_backend.is_paper() {
            let execution_engine = orchestrator.execution_engine();
            let bybit_creds = match execution_engine.credentials() {
                Some(creds) => creds,
                None => bail!("live execution requires Bybit credentials"),
            };
            let ws_url = execution_engine.ws_url();
            let private_tx = private_event_tx.clone();
            tokio::spawn(async move {
                let creds = bybit_creds;
                let endpoint = ws_url;
                loop {
                    match tesser_bybit::ws::connect_private(&endpoint, &creds).await {
                        Ok(mut socket) => {
                            info!("Connected to Bybit private WebSocket stream");
                            while let Some(msg) = socket.next().await {
                                if let Ok(Message::Text(text)) = msg {
                                    if let Ok(value) =
                                        serde_json::from_str::<serde_json::Value>(&text)
                                    {
                                        if let Some(topic) =
                                            value.get("topic").and_then(|v| v.as_str())
                                        {
                                            match topic {
                                                "order" => {
                                                    if let Ok(msg) = serde_json::from_value::<
                                                        PrivateMessage<BybitWsOrder>,
                                                    >(
                                                        value.clone()
                                                    ) {
                                                        for update in msg.data {
                                                            match update.to_tesser_order(None) {
                                                                Ok(order) => {
                                                                    if let Err(err) = private_tx.send(BrokerEvent::OrderUpdate(order)).await {
                                                                        error!("failed to send private order update: {err}");
                                                                    }
                                                                }
                                                                Err(err) => error!("failed to convert order update: {err}"),
                                                            }
                                                        }
                                                    }
                                                }
                                                "execution" => {
                                                    if let Ok(msg) = serde_json::from_value::<
                                                        PrivateMessage<BybitWsExecution>,
                                                    >(
                                                        value.clone()
                                                    ) {
                                                        for exec in msg.data {
                                                            match exec.to_tesser_fill() {
                                                                Ok(fill) => {
                                                                    if let Err(err) = private_tx.send(BrokerEvent::Fill(fill)).await {
                                                                        error!("failed to send private fill event: {err}");
                                                                    }
                                                                }
                                                                Err(err) => error!("failed to parse execution: {err}"),
                                                            }
                                                        }
                                                    }
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => error!("Private WebSocket connection failed: {e}. Retrying..."),
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            });
        }

        info!(
            symbols = ?symbols,
            category = ?settings.category,
            metrics_addr = %settings.metrics_addr,
            state_path = %settings.state_path.display(),
            history = settings.history,
            "market stream ready"
        );

        Ok(Self {
            stream,
            strategy,
            strategy_ctx,
            orchestrator,
            portfolio,
            metrics,
            alerts,
            market,
            state_repo,
            persisted,
            shutdown,
            metrics_task,
            alert_task,
            private_event_rx,
            exec_backend: settings.exec_backend,
        })
    }

    async fn run(mut self) -> Result<()> {
        info!("live session started");
        self.reconcile_state()
            .await
            .context("initial state reconciliation failed")?;
        let backoff = Duration::from_millis(200);
        let mut reconciliation_timer = tokio::time::interval(Duration::from_secs(60));
        let mut orchestrator_timer = tokio::time::interval(Duration::from_secs(1));

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

            tokio::select! {
                biased;
                Some(event) = self.private_event_rx.recv() => {
                    progressed = true;
                    match event {
                        BrokerEvent::OrderUpdate(order) => {
                            self.handle_order_update(order).await?;
                        }
                        BrokerEvent::Fill(fill) => {
                            self.handle_real_fill(fill).await?;
                        }
                    }
                }
                _ = reconciliation_timer.tick(), if !self.exec_backend.is_paper() => {
                    if let Err(e) = self.reconcile_state().await {
                        error!("Periodic state reconciliation failed: {}", e);
                    }
                }
                _ = orchestrator_timer.tick() => {
                    // Drive TWAP and other time-based algorithms
                    if let Err(e) = self.orchestrator.on_timer_tick().await {
                        error!("Orchestrator timer tick failed: {}", e);
                    }
                }
                else => {}
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

    async fn reconcile_state(&mut self) -> Result<()> {
        if self.exec_backend.is_paper() {
            return Ok(());
        }

        info!("Running state reconciliation...");
        let client = self.orchestrator.execution_engine().client();
        match client.positions().await {
            Ok(remote_positions) => {
                let local_positions = self.portfolio.positions();
                if remote_positions.len() != local_positions.len() {
                    warn!(
                        remote = remote_positions.len(),
                        local = local_positions.len(),
                        "Position count mismatch"
                    );
                    self.alerts
                        .notify("Reconciliation Error", "Position count mismatch detected")
                        .await;
                }
            }
            Err(e) => error!("Failed to fetch remote positions: {e}"),
        }

        match client.account_balances().await {
            Ok(remote_balances) => {
                if let Some(usdt) = remote_balances.iter().find(|b| b.currency == "USDT") {
                    let local_cash = self.portfolio.cash();
                    let diff = (usdt.available - local_cash).abs();
                    if diff > 1.0 {
                        warn!(
                            remote = usdt.available,
                            local = local_cash,
                            "Cash balance mismatch"
                        );
                        self.alerts
                            .notify(
                                "Reconciliation Error",
                                &format!("Cash balance deviates by {:.2}", diff),
                            )
                            .await;
                    }
                }
            }
            Err(e) => error!("Failed to fetch remote balances: {e}"),
        }

        info!("State reconciliation complete.");
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

        // Update price in paper trading client if using paper mode
        if let Some(paper_client) = self
            .orchestrator
            .execution_engine()
            .client()
            .as_any()
            .downcast_ref::<PaperExecutionClient>()
        {
            paper_client.update_price(&tick.symbol, tick.price);
        }

        // Route tick to orchestrator for algorithmic orders
        if let Err(e) = self.orchestrator.on_tick(&tick).await {
            tracing::warn!(error = %e, "Orchestrator failed to process tick");
        }

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

        // Update price in paper trading client if using paper mode
        if let Some(paper_client) = self
            .orchestrator
            .execution_engine()
            .client()
            .as_any()
            .downcast_ref::<PaperExecutionClient>()
        {
            paper_client.update_price(&candle.symbol, candle.close);
        }

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
            let last_price = self
                .market
                .get(&signal.symbol)
                .and_then(|s| s.price())
                .unwrap_or(0.0);

            let ctx = RiskContext {
                signed_position_qty: self.portfolio.signed_position_qty(&signal.symbol),
                portfolio_equity: self.portfolio.equity(),
                last_price,
                liquidate_only: self.portfolio.liquidate_only(),
            };
            match self.orchestrator.on_signal(&signal, &ctx).await {
                Ok(()) => {
                    // The orchestrator handles order submission internally
                    // Metrics and alerts are handled when fills are received
                    self.alerts.reset_order_failures().await;
                }
                Err(err) => {
                    warn!(error = %err, "orchestrator rejected signal");
                    self.metrics.inc_order_failure();
                    self.alerts
                        .order_failure(&format!("orchestrator error: {err}"))
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn handle_order_update(&mut self, order: Order) -> Result<()> {
        info!(
            order_id = %order.id,
            status = ?order.status,
            "Received real-time order update"
        );
        let mut found = false;
        for existing in &mut self.persisted.open_orders {
            if existing.id == order.id {
                existing.status = order.status;
                existing.updated_at = order.updated_at;
                found = true;
                break;
            }
        }
        if !found {
            self.persisted.open_orders.push(order.clone());
        }

        if matches!(
            order.status,
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
        ) {
            self.persisted.open_orders.retain(|o| o.id != order.id);
        }

        self.save_state().await?;
        Ok(())
    }

    async fn handle_real_fill(&mut self, fill: Fill) -> Result<()> {
        info!(
            order_id = %fill.order_id,
            symbol = %fill.symbol,
            price = fill.fill_price,
            qty = fill.fill_quantity,
            "Processing real fill"
        );

        // Route fill to orchestrator first (for algorithmic orders)
        if let Err(e) = self.orchestrator.on_fill(&fill).await {
            warn!(error = %e, "Orchestrator failed to process fill");
        }

        self.portfolio
            .apply_fill(&fill)
            .context("Failed to apply real fill to portfolio")?;
        self.strategy_ctx
            .update_positions(self.portfolio.positions());
        self.strategy
            .on_fill(&self.strategy_ctx, &fill)
            .context("Strategy failed on real fill event")?;
        self.metrics.update_equity(self.portfolio.equity());
        self.metrics.inc_order(); // Count the fill as a completed order
        self.alerts.update_equity(self.portfolio.equity()).await;
        self.alerts
            .notify(
                "Order Filled",
                &format!(
                    "order filled: {}@{:.2} ({})",
                    fill.fill_quantity,
                    fill.fill_price,
                    match fill.side {
                        Side::Buy => "buy",
                        Side::Sell => "sell",
                    }
                ),
            )
            .await;
        self.persisted.portfolio = Some(self.portfolio.snapshot());
        self.save_state().await
    }

    async fn save_state(&self) -> Result<()> {
        let repo = self.state_repo.clone();
        let snapshot = self.persisted.clone();
        tokio::task::spawn_blocking(move || repo.save(&snapshot))
            .await
            .map_err(|err| anyhow!("state persistence task failed: {err}"))?
            .map_err(|err| anyhow!(err.to_string()))
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
