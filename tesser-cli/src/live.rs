use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::ValueEnum;
use futures::StreamExt;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, trace, warn};

use tesser_broker::{ExecutionClient, MarketStream};
use tesser_bybit::ws::{BybitWsExecution, BybitWsOrder, PrivateMessage};
use tesser_bybit::{
    BybitClient, BybitConfig, BybitCredentials, BybitMarketStream, BybitSubscription, PublicChannel,
};
use tesser_config::{AlertingConfig, ExchangeConfig, RiskManagementConfig};
use tesser_core::{
    Candle, Fill, Interval, Order, OrderBook, OrderStatus, Position, Price, Quantity, Side, Signal,
    Symbol, Tick,
};
use tesser_events::{
    CandleEvent, Event, EventBus, FillEvent, OrderBookEvent, OrderUpdateEvent, SignalEvent,
    TickEvent,
};
use tesser_execution::{
    BasicRiskChecker, ExecutionEngine, FixedOrderSizer, OrderOrchestrator, PreTradeRiskChecker,
    RiskContext, RiskLimits, SqliteAlgoStateRepository,
};
use tesser_markets::MarketRegistry;
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

const DEFAULT_ORDER_BOOK_DEPTH: usize = 50;
const STRATEGY_LOCK_WARN_THRESHOLD: Duration = Duration::from_millis(25);
const STRATEGY_CALL_WARN_THRESHOLD: Duration = Duration::from_millis(250);

pub struct LiveSessionSettings {
    pub category: PublicChannel,
    pub interval: Interval,
    pub quantity: Quantity,
    pub slippage_bps: Decimal,
    pub fee_bps: Decimal,
    pub history: usize,
    pub metrics_addr: SocketAddr,
    pub state_path: PathBuf,
    pub initial_balances: HashMap<Symbol, Decimal>,
    pub reporting_currency: Symbol,
    pub markets_file: Option<PathBuf>,
    pub alerting: AlertingConfig,
    pub exec_backend: ExecutionBackend,
    pub risk: RiskManagementConfig,
    pub reconciliation_interval: Duration,
    pub reconciliation_threshold: Decimal,
}

impl LiveSessionSettings {
    fn risk_limits(&self) -> RiskLimits {
        RiskLimits {
            max_order_quantity: self.risk.max_order_quantity.max(Decimal::ZERO),
            max_position_quantity: self.risk.max_position_quantity.max(Decimal::ZERO),
        }
    }
}

pub async fn run_live(
    strategy: Box<dyn Strategy>,
    symbols: Vec<String>,
    exchange: ExchangeConfig,
    settings: LiveSessionSettings,
) -> Result<()> {
    run_live_with_shutdown(strategy, symbols, exchange, settings, ShutdownSignal::new()).await
}

/// Variant of [`run_live`] that accepts a manually controlled shutdown signal.
pub async fn run_live_with_shutdown(
    strategy: Box<dyn Strategy>,
    symbols: Vec<String>,
    exchange: ExchangeConfig,
    settings: LiveSessionSettings,
    shutdown: ShutdownSignal,
) -> Result<()> {
    if symbols.is_empty() {
        return Err(anyhow!("strategy did not declare any subscriptions"));
    }
    if settings.quantity <= Decimal::ZERO {
        return Err(anyhow!("--quantity must be positive"));
    }

    let public_connection = Arc::new(AtomicBool::new(false));
    let private_connection = if matches!(settings.exec_backend, ExecutionBackend::Live) {
        Some(Arc::new(AtomicBool::new(false)))
    } else {
        None
    };
    let mut stream = BybitMarketStream::connect_public(
        &exchange.ws_url,
        settings.category,
        Some(public_connection.clone()),
    )
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
        stream
            .subscribe(BybitSubscription::OrderBook {
                symbol: symbol.clone(),
                depth: DEFAULT_ORDER_BOOK_DEPTH,
            })
            .await
            .with_context(|| format!("failed to subscribe to order books for {symbol}"))?;
    }

    let execution_client = build_execution_client(&exchange, &settings)?;
    let market_registry = load_market_registry(execution_client.clone(), &settings).await?;
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
        execution_client.clone(),
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

    let runtime = LiveRuntime::new(
        stream,
        strategy,
        symbols,
        orchestrator,
        settings,
        market_registry,
        shutdown,
        public_connection,
        private_connection,
    )
    .await?;
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
                    ws_url: Some(exchange.ws_url.clone()),
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
    orchestrator: Arc<OrderOrchestrator>,
    state_repo: Arc<dyn StateRepository>,
    persisted: Arc<Mutex<LiveState>>,
    event_bus: Arc<EventBus>,
    shutdown: ShutdownSignal,
    metrics_task: JoinHandle<()>,
    alert_task: Option<JoinHandle<()>>,
    reconciliation_task: Option<JoinHandle<()>>,
    reconciliation_ctx: Option<Arc<ReconciliationContext>>,
    private_event_rx: mpsc::Receiver<BrokerEvent>,
    #[allow(dead_code)]
    last_private_sync: Arc<tokio::sync::Mutex<Option<DateTime<Utc>>>>,
    subscriber_handles: Vec<JoinHandle<()>>,
    connection_monitors: Vec<JoinHandle<()>>,
    order_timeout_task: JoinHandle<()>,
    strategy: Arc<Mutex<Box<dyn Strategy>>>,
    _public_connection: Arc<AtomicBool>,
    _private_connection: Option<Arc<AtomicBool>>,
}

impl LiveRuntime {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        stream: BybitMarketStream,
        mut strategy: Box<dyn Strategy>,
        symbols: Vec<String>,
        orchestrator: OrderOrchestrator,
        settings: LiveSessionSettings,
        market_registry: Arc<MarketRegistry>,
        shutdown: ShutdownSignal,
        public_connection: Arc<AtomicBool>,
        private_connection: Option<Arc<AtomicBool>>,
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
        let mut live_bootstrap = None;
        if matches!(settings.exec_backend, ExecutionBackend::Live) {
            info!("Synchronizing portfolio state from exchange");
            let execution_client = orchestrator.execution_engine().client();
            let positions = execution_client
                .positions()
                .await
                .context("failed to fetch remote positions")?;
            let balances = execution_client
                .account_balances()
                .await
                .context("failed to fetch remote account balances")?;
            let mut open_orders = Vec::new();
            for symbol in &symbols {
                let mut symbol_orders = execution_client
                    .list_open_orders(symbol)
                    .await
                    .with_context(|| format!("failed to fetch open orders for {symbol}"))?;
                open_orders.append(&mut symbol_orders);
            }
            persisted.open_orders = open_orders;
            live_bootstrap = Some((positions, balances));
        }

        let portfolio_cfg = PortfolioConfig {
            initial_balances: settings.initial_balances.clone(),
            reporting_currency: settings.reporting_currency.clone(),
            max_drawdown: Some(settings.risk.max_drawdown),
        };
        let portfolio = if let Some((positions, balances)) = live_bootstrap {
            Portfolio::from_exchange_state(
                positions,
                balances,
                portfolio_cfg.clone(),
                market_registry.clone(),
            )
        } else if let Some(snapshot) = persisted.portfolio.take() {
            Portfolio::from_state(snapshot, portfolio_cfg.clone(), market_registry.clone())
        } else {
            Portfolio::new(portfolio_cfg.clone(), market_registry.clone())
        };
        strategy_ctx.update_positions(portfolio.positions());
        // Restore strategy state if found in persistence
        if let Some(state) = persisted.strategy_state.take() {
            info!("restoring strategy state from persistence");
            strategy
                .restore(state)
                .context("failed to restore strategy state")?;
        }
        persisted.portfolio = Some(portfolio.snapshot());

        let mut market = HashMap::new();
        for symbol in &symbols {
            let mut snapshot = MarketSnapshot::default();
            if let Some(price) = persisted.last_prices.get(symbol).copied() {
                snapshot.last_trade = Some(price);
            }
            market.insert(symbol.clone(), snapshot);
        }

        let metrics = LiveMetrics::new();
        metrics.update_connection_status("public", public_connection.load(Ordering::SeqCst));
        if let Some(flag) = &private_connection {
            metrics.update_connection_status("private", flag.load(Ordering::SeqCst));
        }
        let metrics_task = spawn_metrics_server(metrics.registry(), settings.metrics_addr);
        let dispatcher = AlertDispatcher::new(settings.alerting.webhook_url.clone());
        let alerts = AlertManager::new(
            settings.alerting,
            dispatcher,
            Some(public_connection.clone()),
            private_connection.clone(),
        );
        let (private_event_tx, private_event_rx) = mpsc::channel(1024);
        let last_private_sync = Arc::new(tokio::sync::Mutex::new(persisted.last_candle_ts));
        let alerts = Arc::new(alerts);
        let alert_task = alerts.spawn_watchdog();
        let metrics = Arc::new(metrics);
        let mut connection_monitors = Vec::new();
        connection_monitors.push(spawn_connection_monitor(
            shutdown.clone(),
            public_connection.clone(),
            metrics.clone(),
            "public",
        ));
        if let Some(flag) = private_connection.clone() {
            connection_monitors.push(spawn_connection_monitor(
                shutdown.clone(),
                flag,
                metrics.clone(),
                "private",
            ));
        }

        if !settings.exec_backend.is_paper() {
            let execution_engine = orchestrator.execution_engine();
            let bybit_creds = match execution_engine.credentials() {
                Some(creds) => creds,
                None => bail!("live execution requires Bybit credentials"),
            };
            let ws_url = execution_engine.ws_url();
            let private_tx = private_event_tx.clone();
            let exec_client = execution_engine.client();
            let symbols_for_private = symbols.clone();
            let last_sync_handle = last_private_sync.clone();
            let private_connection_flag = private_connection.clone();
            let metrics_for_private = metrics.clone();
            tokio::spawn(async move {
                let creds = bybit_creds;
                let endpoint = ws_url;
                let client = exec_client;
                let symbols = symbols_for_private;
                let last_sync = last_sync_handle;
                loop {
                    match tesser_bybit::ws::connect_private(
                        &endpoint,
                        &creds,
                        private_connection_flag.clone(),
                    )
                    .await
                    {
                        Ok(mut socket) => {
                            if let Some(flag) = &private_connection_flag {
                                flag.store(true, Ordering::SeqCst);
                            }
                            metrics_for_private.update_connection_status("private", true);
                            info!("Connected to Bybit private WebSocket stream");
                            // Incremental reconciliation right after a successful reconnect
                            // 1) Sync open orders
                            for symbol in &symbols {
                                match client.list_open_orders(symbol).await {
                                    Ok(orders) => {
                                        for order in orders {
                                            if let Err(err) = private_tx
                                                .send(BrokerEvent::OrderUpdate(order))
                                                .await
                                            {
                                                error!(
                                                    "failed to send reconciled order update: {err}"
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("failed to reconcile open orders for {symbol}: {e}");
                                    }
                                }
                            }

                            // 2) Fetch any executions since the last sync timestamp
                            if let Some(bybit) =
                                client.as_any().downcast_ref::<tesser_bybit::BybitClient>()
                            {
                                let since = {
                                    let guard = last_sync.lock().await;
                                    // Default to 30 minutes ago if missing
                                    guard.unwrap_or_else(|| {
                                        Utc::now() - chrono::Duration::minutes(30)
                                    })
                                };
                                match bybit.list_executions_since(since).await {
                                    Ok(fills) => {
                                        for fill in fills {
                                            if let Err(err) =
                                                private_tx.send(BrokerEvent::Fill(fill)).await
                                            {
                                                error!("failed to send reconciled fill: {err}");
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "failed to reconcile executions since {:?}: {}",
                                            since, e
                                        );
                                    }
                                }
                                // Update last sync time to now regardless to avoid tight loops
                                let mut guard = last_sync.lock().await;
                                *guard = Some(Utc::now());
                            }

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
                        Err(e) => {
                            if let Some(flag) = &private_connection_flag {
                                flag.store(false, Ordering::SeqCst);
                            }
                            metrics_for_private.update_connection_status("private", false);
                            error!("Private WebSocket connection failed: {e}. Retrying...");
                        }
                    }
                    if let Some(flag) = &private_connection_flag {
                        flag.store(false, Ordering::SeqCst);
                    }
                    metrics_for_private.update_connection_status("private", false);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            });
        }

        let strategy = Arc::new(Mutex::new(strategy));
        let strategy_ctx = Arc::new(Mutex::new(strategy_ctx));
        let portfolio = Arc::new(Mutex::new(portfolio));
        let market = Arc::new(Mutex::new(market));
        let persisted = Arc::new(Mutex::new(persisted));
        let orchestrator = Arc::new(orchestrator);
        let event_bus = Arc::new(EventBus::new(2048));
        let reconciliation_ctx = (!settings.exec_backend.is_paper()).then(|| {
            Arc::new(ReconciliationContext::new(ReconciliationContextConfig {
                client: orchestrator.execution_engine().client(),
                portfolio: portfolio.clone(),
                persisted: persisted.clone(),
                state_repo: state_repo.clone(),
                alerts: alerts.clone(),
                metrics: metrics.clone(),
                reporting_currency: settings.reporting_currency.clone(),
                threshold: settings.reconciliation_threshold,
            }))
        });
        let reconciliation_task = reconciliation_ctx.as_ref().map(|ctx| {
            spawn_reconciliation_loop(
                ctx.clone(),
                shutdown.clone(),
                settings.reconciliation_interval,
            )
        });
        let subscriber_handles = spawn_event_subscribers(
            event_bus.clone(),
            strategy.clone(),
            strategy_ctx.clone(),
            orchestrator.clone(),
            portfolio.clone(),
            metrics.clone(),
            alerts.clone(),
            market.clone(),
            state_repo.clone(),
            persisted.clone(),
            settings.exec_backend,
        );
        let order_timeout_task = spawn_order_timeout_monitor(
            orchestrator.clone(),
            event_bus.clone(),
            alerts.clone(),
            shutdown.clone(),
        );

        info!(
            symbols = ?symbols,
            category = ?settings.category,
            metrics_addr = %settings.metrics_addr,
            state_path = %settings.state_path.display(),
            history = settings.history,
            "market stream ready"
        );

        for symbol in &symbols {
            let ctx = shared_risk_context(symbol, &portfolio, &market, &persisted).await;
            orchestrator.update_risk_context(symbol.clone(), ctx);
        }

        Ok(Self {
            stream,
            orchestrator,
            state_repo,
            persisted,
            event_bus,
            shutdown,
            metrics_task,
            alert_task,
            reconciliation_task,
            reconciliation_ctx,
            private_event_rx,
            last_private_sync,
            subscriber_handles,
            connection_monitors,
            order_timeout_task,
            strategy,
            _public_connection: public_connection,
            _private_connection: private_connection,
        })
    }

    async fn run(mut self) -> Result<()> {
        info!("live session started");
        if let Some(ctx) = self.reconciliation_ctx.as_ref() {
            perform_state_reconciliation(ctx.as_ref())
                .await
                .context("initial state reconciliation failed")?;
        }
        let backoff = Duration::from_millis(200);
        let mut orchestrator_timer = tokio::time::interval(Duration::from_secs(1));

        while !self.shutdown.triggered() {
            let mut progressed = false;

            if let Some(tick) = self.stream.next_tick().await? {
                progressed = true;
                self.event_bus.publish(Event::Tick(TickEvent { tick }));
            }

            if let Some(candle) = self.stream.next_candle().await? {
                progressed = true;
                self.event_bus
                    .publish(Event::Candle(CandleEvent { candle }));
            }

            if let Some(book) = self.stream.next_order_book().await? {
                progressed = true;
                self.event_bus
                    .publish(Event::OrderBook(OrderBookEvent { order_book: book }));
            }

            tokio::select! {
                biased;
                Some(event) = self.private_event_rx.recv() => {
                    progressed = true;
                    match event {
                        BrokerEvent::OrderUpdate(order) => {
                            info!(
                                order_id = %order.id,
                                status = ?order.status,
                                symbol = %order.request.symbol,
                                "received private order update"
                            );
                            self.event_bus
                                .publish(Event::OrderUpdate(OrderUpdateEvent { order }));
                        }
                        BrokerEvent::Fill(fill) => {
                            info!(
                                order_id = %fill.order_id,
                                symbol = %fill.symbol,
                                qty = %fill.fill_quantity,
                                price = %fill.fill_price,
                                "received private fill"
                            );
                            self.event_bus.publish(Event::Fill(FillEvent { fill }));
                        }
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
        if let Some(handle) = self.reconciliation_task.take() {
            handle.abort();
        }
        self.order_timeout_task.abort();
        for handle in self.subscriber_handles.drain(..) {
            handle.abort();
        }
        for handle in self.connection_monitors.drain(..) {
            handle.abort();
        }
        if let Err(err) = self.save_state().await {
            warn!(error = %err, "failed to persist shutdown state");
        }
        Ok(())
    }

    async fn save_state(&self) -> Result<()> {
        persist_state(
            self.state_repo.clone(),
            self.persisted.clone(),
            Some(self.strategy.clone()),
        )
        .await
    }
}

struct ReconciliationContext {
    client: Arc<dyn ExecutionClient>,
    portfolio: Arc<Mutex<Portfolio>>,
    persisted: Arc<Mutex<LiveState>>,
    state_repo: Arc<dyn StateRepository>,
    alerts: Arc<AlertManager>,
    metrics: Arc<LiveMetrics>,
    reporting_currency: Symbol,
    threshold: Decimal,
}

struct ReconciliationContextConfig {
    client: Arc<dyn ExecutionClient>,
    portfolio: Arc<Mutex<Portfolio>>,
    persisted: Arc<Mutex<LiveState>>,
    state_repo: Arc<dyn StateRepository>,
    alerts: Arc<AlertManager>,
    metrics: Arc<LiveMetrics>,
    reporting_currency: Symbol,
    threshold: Decimal,
}

impl ReconciliationContext {
    fn new(config: ReconciliationContextConfig) -> Self {
        let ReconciliationContextConfig {
            client,
            portfolio,
            persisted,
            state_repo,
            alerts,
            metrics,
            reporting_currency,
            threshold,
        } = config;
        let min_threshold = Decimal::new(1, 6); // 0.000001 as a practical floor
        let threshold = if threshold <= Decimal::ZERO {
            min_threshold
        } else {
            threshold
        };
        Self {
            client,
            portfolio,
            persisted,
            state_repo,
            alerts,
            metrics,
            reporting_currency,
            threshold,
        }
    }
}

fn spawn_reconciliation_loop(
    ctx: Arc<ReconciliationContext>,
    shutdown: ShutdownSignal,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while shutdown.sleep(interval).await {
            if let Err(err) = perform_state_reconciliation(ctx.as_ref()).await {
                error!(error = %err, "periodic state reconciliation failed");
            }
        }
    })
}

async fn perform_state_reconciliation(ctx: &ReconciliationContext) -> Result<()> {
    info!("running state reconciliation");
    let remote_positions = ctx
        .client
        .positions()
        .await
        .context("failed to fetch remote positions")?;
    let remote_balances = ctx
        .client
        .account_balances()
        .await
        .context("failed to fetch remote balances")?;
    let (local_positions, local_cash) = {
        let guard = ctx.portfolio.lock().await;
        (guard.positions(), guard.cash())
    };

    let remote_map = positions_to_map(remote_positions);
    let local_map = positions_to_map(local_positions);
    let mut tracked_symbols: HashSet<String> = HashSet::new();
    tracked_symbols.extend(remote_map.keys().cloned());
    tracked_symbols.extend(local_map.keys().cloned());

    let mut severe_findings = Vec::new();
    for symbol in tracked_symbols {
        let local_qty = local_map.get(&symbol).copied().unwrap_or(Decimal::ZERO);
        let remote_qty = remote_map.get(&symbol).copied().unwrap_or(Decimal::ZERO);
        let diff = (local_qty - remote_qty).abs();
        let diff_value = diff.to_f64().unwrap_or(0.0);
        ctx.metrics.update_position_diff(&symbol, diff_value);
        if diff > Decimal::ZERO {
            warn!(
                symbol = %symbol,
                local = %local_qty,
                remote = %remote_qty,
                diff = %diff,
                "position mismatch detected during reconciliation"
            );
            let pct = normalize_diff(diff, remote_qty);
            if pct >= ctx.threshold {
                error!(
                    symbol = %symbol,
                    local = %local_qty,
                    remote = %remote_qty,
                    diff = %diff,
                    pct = %pct,
                    "position mismatch exceeds threshold"
                );
                severe_findings.push(format!(
                    "{symbol} local={local_qty} remote={remote_qty} diff={diff}"
                ));
            }
        }
    }

    let reporting = ctx.reporting_currency.as_str();
    let remote_cash = remote_balances
        .iter()
        .find(|balance| balance.currency == reporting)
        .map(|balance| balance.available)
        .unwrap_or_else(|| Decimal::ZERO);
    let cash_diff = (remote_cash - local_cash).abs();
    ctx.metrics
        .update_balance_diff(reporting, cash_diff.to_f64().unwrap_or(0.0));
    if cash_diff > Decimal::ZERO {
        warn!(
            currency = %reporting,
            local = %local_cash,
            remote = %remote_cash,
            diff = %cash_diff,
            "balance mismatch detected during reconciliation"
        );
        let pct = normalize_diff(cash_diff, remote_cash);
        if pct >= ctx.threshold {
            error!(
                currency = %reporting,
                local = %local_cash,
                remote = %remote_cash,
                diff = %cash_diff,
                pct = %pct,
                "balance mismatch exceeds threshold"
            );
            severe_findings.push(format!(
                "{reporting} balance local={local_cash} remote={remote_cash} diff={cash_diff}"
            ));
        }
    }

    if severe_findings.is_empty() {
        info!("state reconciliation complete with no critical divergence");
        return Ok(());
    }

    let alert_body = severe_findings.join("; ");
    ctx.alerts
        .notify("State reconciliation divergence", &alert_body)
        .await;
    enforce_liquidate_only(ctx).await;
    Ok(())
}

async fn enforce_liquidate_only(ctx: &ReconciliationContext) {
    let snapshot = {
        let mut guard = ctx.portfolio.lock().await;
        if !guard.set_liquidate_only(true) {
            return;
        }
        info!("entering liquidate-only mode due to reconciliation divergence");
        guard.snapshot()
    };
    {
        let mut state = ctx.persisted.lock().await;
        state.portfolio = Some(snapshot);
    }
    if let Err(err) = persist_state(ctx.state_repo.clone(), ctx.persisted.clone(), None).await {
        warn!(error = %err, "failed to persist liquidate-only transition");
    }
}

fn positions_to_map(positions: Vec<Position>) -> HashMap<String, Decimal> {
    let mut map = HashMap::new();
    for position in positions {
        map.insert(position.symbol.clone(), position_signed_qty(&position));
    }
    map
}

fn position_signed_qty(position: &Position) -> Decimal {
    match position.side {
        Some(Side::Buy) => position.quantity,
        Some(Side::Sell) => -position.quantity,
        None => Decimal::ZERO,
    }
}

fn normalize_diff(diff: Decimal, reference: Decimal) -> Decimal {
    if diff <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        let denominator = std::cmp::max(reference.abs(), Decimal::ONE);
        diff / denominator
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

#[derive(Clone)]
pub struct ShutdownSignal {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl ShutdownSignal {
    pub fn new() -> Self {
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

    pub fn trigger(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
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

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_event_subscribers(
    bus: Arc<EventBus>,
    strategy: Arc<Mutex<Box<dyn Strategy>>>,
    strategy_ctx: Arc<Mutex<StrategyContext>>,
    orchestrator: Arc<OrderOrchestrator>,
    portfolio: Arc<Mutex<Portfolio>>,
    metrics: Arc<LiveMetrics>,
    alerts: Arc<AlertManager>,
    market: Arc<Mutex<HashMap<String, MarketSnapshot>>>,
    state_repo: Arc<dyn StateRepository>,
    persisted: Arc<Mutex<LiveState>>,
    exec_backend: ExecutionBackend,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();

    let market_bus = bus.clone();
    let market_strategy = strategy.clone();
    let market_ctx = strategy_ctx.clone();
    let market_metrics = metrics.clone();
    let market_alerts = alerts.clone();
    let market_state = state_repo.clone();
    let market_persisted = persisted.clone();
    let market_portfolio = portfolio.clone();
    let market_snapshot = market.clone();
    let orchestrator_clone = orchestrator.clone();
    handles.push(tokio::spawn(async move {
        let mut stream = market_bus.subscribe();
        loop {
            match stream.recv().await {
                Ok(Event::Tick(evt)) => {
                    if let Err(err) = process_tick_event(
                        evt.tick,
                        market_strategy.clone(),
                        market_ctx.clone(),
                        market_metrics.clone(),
                        market_alerts.clone(),
                        market_snapshot.clone(),
                        market_portfolio.clone(),
                        market_state.clone(),
                        market_persisted.clone(),
                        market_bus.clone(),
                    )
                    .await
                    {
                        warn!(error = %err, "tick handler failed");
                    }
                }
                Ok(Event::Candle(evt)) => {
                    if let Err(err) = process_candle_event(
                        evt.candle,
                        market_strategy.clone(),
                        market_ctx.clone(),
                        market_metrics.clone(),
                        market_alerts.clone(),
                        market_snapshot.clone(),
                        market_portfolio.clone(),
                        orchestrator_clone.clone(),
                        exec_backend,
                        market_state.clone(),
                        market_persisted.clone(),
                        market_bus.clone(),
                    )
                    .await
                    {
                        warn!(error = %err, "candle handler failed");
                    }
                }
                Ok(Event::OrderBook(evt)) => {
                    if let Err(err) = process_order_book_event(
                        evt.order_book,
                        market_strategy.clone(),
                        market_ctx.clone(),
                        market_metrics.clone(),
                        market_alerts.clone(),
                        market_snapshot.clone(),
                        market_bus.clone(),
                    )
                    .await
                    {
                        warn!(error = %err, "order book handler failed");
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(lag)) => {
                    warn!(lag = lag, "market subscriber lagged");
                    continue;
                }
            }
        }
    }));

    let exec_bus = bus.clone();
    let exec_portfolio = portfolio.clone();
    let exec_market = market.clone();
    let exec_persisted = persisted.clone();
    let exec_alerts = alerts.clone();
    let exec_metrics = metrics.clone();
    let exec_orchestrator = orchestrator.clone();
    handles.push(tokio::spawn(async move {
        let orchestrator = exec_orchestrator.clone();
        let mut stream = exec_bus.subscribe();
        loop {
            match stream.recv().await {
                Ok(Event::Signal(evt)) => {
                    if let Err(err) = process_signal_event(
                        evt.signal,
                        orchestrator.clone(),
                        exec_portfolio.clone(),
                        exec_market.clone(),
                        exec_persisted.clone(),
                        exec_alerts.clone(),
                        exec_metrics.clone(),
                    )
                    .await
                    {
                        warn!(error = %err, "signal handler failed");
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(lag)) => {
                    warn!(lag = lag, "signal subscriber lagged");
                    continue;
                }
            }
        }
    }));

    let fill_bus = bus.clone();
    let fill_state = state_repo.clone();
    let fill_orchestrator = orchestrator.clone();
    let fill_persisted = persisted.clone();
    let fill_alerts = alerts.clone();
    handles.push(tokio::spawn(async move {
        let orchestrator = fill_orchestrator.clone();
        let persisted = fill_persisted.clone();
        let mut stream = fill_bus.subscribe();
        loop {
            match stream.recv().await {
                Ok(Event::Fill(evt)) => {
                    if let Err(err) = process_fill_event(
                        evt.fill,
                        portfolio.clone(),
                        strategy.clone(),
                        strategy_ctx.clone(),
                        orchestrator.clone(),
                        metrics.clone(),
                        fill_alerts.clone(),
                        fill_state.clone(),
                        persisted.clone(),
                    )
                    .await
                    {
                        warn!(error = %err, "fill handler failed");
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(lag)) => {
                    warn!(lag = lag, "fill subscriber lagged");
                    continue;
                }
            }
        }
    }));

    let order_bus = bus.clone();
    let order_persisted = persisted.clone();
    let order_alerts = alerts.clone();
    let order_orchestrator = orchestrator.clone();
    // Note: We don't pass strategy to order update handler to avoid lock contention
    // on high-frequency updates. Strategy state is snapshotted on candles/fills.
    handles.push(tokio::spawn(async move {
        let orchestrator = order_orchestrator.clone();
        let persisted = order_persisted.clone();
        let mut stream = order_bus.subscribe();
        loop {
            match stream.recv().await {
                Ok(Event::OrderUpdate(evt)) => {
                    if let Err(err) = process_order_update_event(
                        evt.order,
                        orchestrator.clone(),
                        order_alerts.clone(),
                        state_repo.clone(),
                        persisted.clone(),
                    )
                    .await
                    {
                        warn!(error = %err, "order update handler failed");
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(lag)) => {
                    warn!(lag = lag, "order subscriber lagged");
                    continue;
                }
            }
        }
    }));

    handles
}

#[allow(clippy::too_many_arguments)]
async fn process_tick_event(
    tick: Tick,
    strategy: Arc<Mutex<Box<dyn Strategy>>>,
    strategy_ctx: Arc<Mutex<StrategyContext>>,
    metrics: Arc<LiveMetrics>,
    alerts: Arc<AlertManager>,
    market: Arc<Mutex<HashMap<String, MarketSnapshot>>>,
    portfolio: Arc<Mutex<Portfolio>>,
    state_repo: Arc<dyn StateRepository>,
    persisted: Arc<Mutex<LiveState>>,
    bus: Arc<EventBus>,
) -> Result<()> {
    metrics.inc_tick();
    metrics.update_staleness(0.0);
    metrics.update_last_data_timestamp(Utc::now().timestamp() as f64);
    alerts.heartbeat().await;
    {
        let mut guard = market.lock().await;
        if let Some(snapshot) = guard.get_mut(&tick.symbol) {
            snapshot.last_trade = Some(tick.price);
            snapshot.last_trade_ts = Some(tick.exchange_timestamp);
        }
    }
    let mut drawdown_triggered = false;
    let mut snapshot_on_trigger = None;
    {
        let mut guard = portfolio.lock().await;
        let was_liquidate_only = guard.liquidate_only();
        match guard.update_market_data(&tick.symbol, tick.price) {
            Ok(_) => {
                if !was_liquidate_only && guard.liquidate_only() {
                    drawdown_triggered = true;
                    snapshot_on_trigger = Some(guard.snapshot());
                }
            }
            Err(err) => {
                warn!(
                    symbol = %tick.symbol,
                    error = %err,
                    "failed to refresh market data"
                );
            }
        }
    }
    {
        let mut state = persisted.lock().await;
        state.last_prices.insert(tick.symbol.clone(), tick.price);
        if drawdown_triggered {
            if let Some(snapshot) = snapshot_on_trigger.take() {
                state.portfolio = Some(snapshot);
            }
        }
    }
    if drawdown_triggered {
        persist_state(
            state_repo.clone(),
            persisted.clone(),
            Some(strategy.clone()),
        )
        .await?;
        alert_liquidate_only(alerts.clone()).await;
    }
    {
        let mut ctx = strategy_ctx.lock().await;
        ctx.push_tick(tick.clone());
        let lock_start = Instant::now();
        let mut strat = strategy.lock().await;
        log_strategy_lock("tick", lock_start.elapsed());
        let call_start = Instant::now();
        strat
            .on_tick(&ctx, &tick)
            .await
            .context("strategy failure on tick event")?;
        log_strategy_call("tick", call_start.elapsed());
    }
    emit_signals(strategy.clone(), bus.clone(), metrics.clone()).await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_candle_event(
    candle: Candle,
    strategy: Arc<Mutex<Box<dyn Strategy>>>,
    strategy_ctx: Arc<Mutex<StrategyContext>>,
    metrics: Arc<LiveMetrics>,
    alerts: Arc<AlertManager>,
    market: Arc<Mutex<HashMap<String, MarketSnapshot>>>,
    portfolio: Arc<Mutex<Portfolio>>,
    orchestrator: Arc<OrderOrchestrator>,
    exec_backend: ExecutionBackend,
    state_repo: Arc<dyn StateRepository>,
    persisted: Arc<Mutex<LiveState>>,
    bus: Arc<EventBus>,
) -> Result<()> {
    metrics.inc_candle();
    metrics.update_staleness(0.0);
    metrics.update_last_data_timestamp(Utc::now().timestamp() as f64);
    alerts.heartbeat().await;
    metrics.update_price(&candle.symbol, candle.close.to_f64().unwrap_or(0.0));
    {
        let mut guard = market.lock().await;
        if let Some(snapshot) = guard.get_mut(&candle.symbol) {
            snapshot.last_candle = Some(candle.clone());
            snapshot.last_trade = Some(candle.close);
        }
    }
    if exec_backend.is_paper() {
        let client = orchestrator.execution_engine().client();
        if let Some(paper) = client.as_any().downcast_ref::<PaperExecutionClient>() {
            paper.update_price(&candle.symbol, candle.close);
        }
    }
    let mut candle_drawdown_triggered = false;
    let mut candle_snapshot = None;
    {
        let mut guard = portfolio.lock().await;
        let was_liquidate_only = guard.liquidate_only();
        match guard.update_market_data(&candle.symbol, candle.close) {
            Ok(_) => {
                if !was_liquidate_only && guard.liquidate_only() {
                    candle_drawdown_triggered = true;
                    candle_snapshot = Some(guard.snapshot());
                }
            }
            Err(err) => {
                warn!(
                    symbol = %candle.symbol,
                    error = %err,
                    "failed to refresh market data"
                );
            }
        }
    }
    if candle_drawdown_triggered {
        if let Some(snapshot) = candle_snapshot.take() {
            let mut persisted_guard = persisted.lock().await;
            persisted_guard.portfolio = Some(snapshot);
        }
        alert_liquidate_only(alerts.clone()).await;
    }
    {
        let mut ctx = strategy_ctx.lock().await;
        ctx.push_candle(candle.clone());
        let lock_start = Instant::now();
        let mut strat = strategy.lock().await;
        log_strategy_lock("candle", lock_start.elapsed());
        let call_start = Instant::now();
        strat
            .on_candle(&ctx, &candle)
            .await
            .context("strategy failure on candle event")?;
        log_strategy_call("candle", call_start.elapsed());
    }
    {
        let mut snapshot = persisted.lock().await;
        snapshot.last_candle_ts = Some(candle.timestamp);
        snapshot
            .last_prices
            .insert(candle.symbol.clone(), candle.close);
    }
    persist_state(
        state_repo.clone(),
        persisted.clone(),
        Some(strategy.clone()),
    )
    .await?;
    let ctx = shared_risk_context(&candle.symbol, &portfolio, &market, &persisted).await;
    orchestrator.update_risk_context(candle.symbol.clone(), ctx);
    emit_signals(strategy.clone(), bus.clone(), metrics.clone()).await;
    Ok(())
}

async fn process_order_book_event(
    book: OrderBook,
    strategy: Arc<Mutex<Box<dyn Strategy>>>,
    strategy_ctx: Arc<Mutex<StrategyContext>>,
    metrics: Arc<LiveMetrics>,
    alerts: Arc<AlertManager>,
    _market: Arc<Mutex<HashMap<String, MarketSnapshot>>>,
    bus: Arc<EventBus>,
) -> Result<()> {
    metrics.update_staleness(0.0);
    alerts.heartbeat().await;
    {
        let mut ctx = strategy_ctx.lock().await;
        ctx.push_order_book(book.clone());
        let lock_start = Instant::now();
        let mut strat = strategy.lock().await;
        log_strategy_lock("order_book", lock_start.elapsed());
        let call_start = Instant::now();
        strat
            .on_order_book(&ctx, &book)
            .await
            .context("strategy failure on order book")?;
        log_strategy_call("order_book", call_start.elapsed());
    }
    emit_signals(strategy.clone(), bus.clone(), metrics.clone()).await;
    Ok(())
}

async fn process_signal_event(
    signal: Signal,
    orchestrator: Arc<OrderOrchestrator>,
    portfolio: Arc<Mutex<Portfolio>>,
    market: Arc<Mutex<HashMap<String, MarketSnapshot>>>,
    persisted: Arc<Mutex<LiveState>>,
    alerts: Arc<AlertManager>,
    metrics: Arc<LiveMetrics>,
) -> Result<()> {
    let ctx = shared_risk_context(&signal.symbol, &portfolio, &market, &persisted).await;
    orchestrator.update_risk_context(signal.symbol.clone(), ctx);
    match orchestrator.on_signal(&signal, &ctx).await {
        Ok(()) => {
            alerts.reset_order_failures().await;
        }
        Err(err) => {
            metrics.inc_order_failure();
            alerts
                .order_failure(&format!("orchestrator error: {err}"))
                .await;
        }
    }
    Ok(())
}

fn log_strategy_lock(event: &str, wait: Duration) {
    let wait_ms = wait.as_secs_f64() * 1000.0;
    if wait >= STRATEGY_LOCK_WARN_THRESHOLD {
        warn!(target: "strategy", event, wait_ms, "strategy lock wait exceeded threshold");
    } else {
        trace!(target: "strategy", event, wait_ms, "strategy lock acquired");
    }
}

fn log_strategy_call(event: &str, elapsed: Duration) {
    let duration_ms = elapsed.as_secs_f64() * 1000.0;
    if elapsed >= STRATEGY_CALL_WARN_THRESHOLD {
        warn!(target: "strategy", event, duration_ms, "strategy call latency above threshold");
    } else {
        trace!(target: "strategy", event, duration_ms, "strategy call completed");
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_fill_event(
    fill: Fill,
    portfolio: Arc<Mutex<Portfolio>>,
    strategy: Arc<Mutex<Box<dyn Strategy>>>,
    strategy_ctx: Arc<Mutex<StrategyContext>>,
    orchestrator: Arc<OrderOrchestrator>,
    metrics: Arc<LiveMetrics>,
    alerts: Arc<AlertManager>,
    state_repo: Arc<dyn StateRepository>,
    persisted: Arc<Mutex<LiveState>>,
) -> Result<()> {
    let mut drawdown_triggered = false;
    {
        let mut guard = portfolio.lock().await;
        let was_liquidate_only = guard.liquidate_only();
        guard
            .apply_fill(&fill)
            .context("Failed to apply fill to portfolio")?;
        if !was_liquidate_only && guard.liquidate_only() {
            drawdown_triggered = true;
        }
        let snapshot = guard.snapshot();
        let mut persisted_guard = persisted.lock().await;
        persisted_guard.portfolio = Some(snapshot);
    }
    {
        let positions = {
            let guard = portfolio.lock().await;
            guard.positions()
        };
        let mut ctx = strategy_ctx.lock().await;
        ctx.update_positions(positions);
    }
    orchestrator.on_fill(&fill).await.ok();
    {
        let ctx = strategy_ctx.lock().await;
        let lock_start = Instant::now();
        let mut strat = strategy.lock().await;
        log_strategy_lock("fill", lock_start.elapsed());
        let call_start = Instant::now();
        strat
            .on_fill(&ctx, &fill)
            .await
            .context("Strategy failed on fill event")?;
        log_strategy_call("fill", call_start.elapsed());
    }
    let equity = {
        let guard = portfolio.lock().await;
        guard.equity()
    };
    if let Some(value) = equity.to_f64() {
        metrics.update_equity(value);
    }
    alerts.update_equity(equity).await;
    metrics.inc_order();
    alerts
        .notify(
            "Order Filled",
            &format!(
                "order filled: {}@{} ({})",
                fill.fill_quantity,
                fill.fill_price,
                match fill.side {
                    Side::Buy => "buy",
                    Side::Sell => "sell",
                }
            ),
        )
        .await;
    if drawdown_triggered {
        alert_liquidate_only(alerts.clone()).await;
    }
    persist_state(
        state_repo.clone(),
        persisted.clone(),
        Some(strategy.clone()),
    )
    .await?;
    Ok(())
}

async fn process_order_update_event(
    order: Order,
    orchestrator: Arc<OrderOrchestrator>,
    alerts: Arc<AlertManager>,
    state_repo: Arc<dyn StateRepository>,
    persisted: Arc<Mutex<LiveState>>,
) -> Result<()> {
    orchestrator.on_order_update(&order);
    if matches!(order.status, OrderStatus::Rejected) {
        error!(
            order_id = %order.id,
            symbol = %order.request.symbol,
            "order rejected by exchange"
        );
        alerts.order_failure("order rejected by exchange").await;
        alerts
            .notify(
                "Order rejected",
                &format!(
                    "Order {} for {} was rejected",
                    order.id, order.request.symbol
                ),
            )
            .await;
    }
    {
        let mut snapshot = persisted.lock().await;
        let mut found = false;
        for existing in &mut snapshot.open_orders {
            if existing.id == order.id {
                *existing = order.clone();
                found = true;
                break;
            }
        }
        if !found {
            snapshot.open_orders.push(order.clone());
        }
        if matches!(
            order.status,
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
        ) {
            snapshot.open_orders.retain(|o| o.id != order.id);
        }
    }
    persist_state(state_repo, persisted, None).await?;
    Ok(())
}

async fn emit_signals(
    strategy: Arc<Mutex<Box<dyn Strategy>>>,
    bus: Arc<EventBus>,
    metrics: Arc<LiveMetrics>,
) {
    let signals = {
        let mut strat = strategy.lock().await;
        strat.drain_signals()
    };
    if signals.is_empty() {
        return;
    }
    metrics.inc_signals(signals.len());
    for signal in signals {
        bus.publish(Event::Signal(SignalEvent { signal }));
    }
}

async fn persist_state(
    repo: Arc<dyn StateRepository>,
    persisted: Arc<Mutex<LiveState>>,
    strategy: Option<Arc<Mutex<Box<dyn Strategy>>>>,
) -> Result<()> {
    if let Some(strat_lock) = strategy {
        // Snapshot strategy state before cloning the full state for persistence
        let strat = strat_lock.lock().await;
        if let Ok(json_state) = strat.snapshot() {
            let mut guard = persisted.lock().await;
            guard.strategy_state = Some(json_state);
        } else {
            warn!("failed to snapshot strategy state");
        }
    }

    let snapshot = {
        let guard = persisted.lock().await;
        guard.clone()
    };
    tokio::task::spawn_blocking(move || repo.save(&snapshot))
        .await
        .map_err(|err| anyhow!("state persistence task failed: {err}"))?
        .map_err(|err| anyhow!(err.to_string()))
}

async fn shared_risk_context(
    symbol: &str,
    portfolio: &Arc<Mutex<Portfolio>>,
    market: &Arc<Mutex<HashMap<String, MarketSnapshot>>>,
    persisted: &Arc<Mutex<LiveState>>,
) -> RiskContext {
    let (signed_qty, equity, liquidate_only) = {
        let guard = portfolio.lock().await;
        (
            guard.signed_position_qty(symbol),
            guard.equity(),
            guard.liquidate_only(),
        )
    };
    let observed_price = {
        let guard = market.lock().await;
        guard.get(symbol).and_then(|snapshot| snapshot.price())
    };
    let last_price = if let Some(price) = observed_price {
        price
    } else {
        let guard = persisted.lock().await;
        guard
            .last_prices
            .get(symbol)
            .copied()
            .unwrap_or(Decimal::ZERO)
    };
    RiskContext {
        signed_position_qty: signed_qty,
        portfolio_equity: equity,
        last_price,
        liquidate_only,
    }
}

async fn alert_liquidate_only(alerts: Arc<AlertManager>) {
    alerts
        .notify(
            "Max drawdown triggered",
            "Portfolio entered liquidate-only mode; new exposure blocked until review",
        )
        .await;
}

fn spawn_connection_monitor(
    shutdown: ShutdownSignal,
    flag: Arc<AtomicBool>,
    metrics: Arc<LiveMetrics>,
    stream: &'static str,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            metrics.update_connection_status(stream, flag.load(Ordering::SeqCst));
            if !shutdown.sleep(Duration::from_secs(5)).await {
                break;
            }
        }
    })
}

fn spawn_order_timeout_monitor(
    orchestrator: Arc<OrderOrchestrator>,
    bus: Arc<EventBus>,
    alerts: Arc<AlertManager>,
    shutdown: ShutdownSignal,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tesser_execution::orchestrator::ORDER_POLL_INTERVAL);
        loop {
            ticker.tick().await;
            if shutdown.triggered() {
                break;
            }
            match orchestrator.poll_stale_orders().await {
                Ok(updates) => {
                    for order in updates {
                        if matches!(order.status, OrderStatus::Rejected | OrderStatus::Canceled) {
                            let message = format!(
                                "Order {} for {} timed out after {}s",
                                order.id,
                                order.request.symbol,
                                tesser_execution::orchestrator::ORDER_TIMEOUT.as_secs()
                            );
                            error!(%message);
                            alerts.order_failure(&message).await;
                            alerts.notify("Order timeout", &message).await;
                        }
                        bus.publish(Event::OrderUpdate(OrderUpdateEvent { order }));
                    }
                }
                Err(err) => {
                    warn!(error = %err, "order timeout monitor failed");
                }
            }
        }
    })
}

async fn load_market_registry(
    client: Arc<dyn ExecutionClient>,
    settings: &LiveSessionSettings,
) -> Result<Arc<MarketRegistry>> {
    if let Some(path) = &settings.markets_file {
        let registry = MarketRegistry::load_from_file(path)
            .with_context(|| format!("failed to load markets from {}", path.display()))?;
        return Ok(Arc::new(registry));
    }

    if settings.exec_backend.is_paper() {
        return Err(anyhow!(
            "paper execution requires --markets-file when exchange metadata is unavailable"
        ));
    }

    let instruments = client
        .list_instruments(settings.category.as_path())
        .await
        .context("failed to fetch instruments from execution client")?;
    let registry =
        MarketRegistry::from_instruments(instruments).map_err(|err| anyhow!(err.to_string()))?;
    Ok(Arc::new(registry))
}
