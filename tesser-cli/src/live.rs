use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, Ordering},
    Arc, Once,
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
use tracing::{debug, error, info, trace, warn};

use serde_json::{json, Value};

fn ensure_builtin_connectors_registered() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        register_connector_factory(Arc::new(PaperFactory::default()));
        #[cfg(feature = "bybit")]
        register_bybit_factory();
        #[cfg(feature = "binance")]
        register_binance_factory();
    });
}

#[cfg(feature = "binance")]
use tesser_binance::{
    fill_from_update, order_from_update, register_factory as register_binance_factory,
    ws::{extract_order_update, BinanceUserDataStream, UserDataStreamEventsResponse},
    BinanceClient,
};
use tesser_broker::{
    get_connector_factory, register_connector_factory, BrokerResult, ConnectorFactory,
    ConnectorStream, ConnectorStreamConfig, ExecutionClient,
};
#[cfg(feature = "bybit")]
use tesser_bybit::ws::{BybitWsExecution, BybitWsOrder, PrivateMessage};
#[cfg(feature = "bybit")]
use tesser_bybit::{register_factory as register_bybit_factory, BybitClient, BybitCredentials};
use tesser_config::{AlertingConfig, ExchangeConfig, PersistenceEngine, RiskManagementConfig};
use tesser_core::{
    AccountBalance, Candle, ExchangeId, Fill, Interval, Order, OrderBook, OrderStatus, Position,
    Price, Quantity, Side, Signal, Symbol, Tick,
};
use tesser_data::recorder::{ParquetRecorder, RecorderConfig, RecorderHandle};
use tesser_events::{
    CandleEvent, Event, EventBus, FillEvent, OrderBookEvent, OrderUpdateEvent, SignalEvent,
    TickEvent,
};
use tesser_execution::{
    AlgoStateRepository, BasicRiskChecker, ExecutionEngine, FixedOrderSizer, OrderOrchestrator,
    PreTradeRiskChecker, RiskContext, RiskLimits, SqliteAlgoStateRepository, StoredAlgoState,
};
use tesser_journal::LmdbJournal;
use tesser_markets::{InstrumentCatalog, MarketRegistry};
use tesser_paper::{FeeScheduleConfig, PaperExecutionClient, PaperFactory};
use tesser_portfolio::{
    LiveState, Portfolio, PortfolioConfig, SqliteStateRepository, StateRepository,
};
use tesser_strategy::{Strategy, StrategyContext};

use crate::alerts::{AlertDispatcher, AlertManager};
use crate::control;
use crate::telemetry::{spawn_metrics_server, LiveMetrics};
use crate::PublicChannel;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum PersistenceBackend {
    Sqlite,
    Lmdb,
}

impl From<PersistenceBackend> for PersistenceEngine {
    fn from(value: PersistenceBackend) -> Self {
        match value {
            PersistenceBackend::Sqlite => PersistenceEngine::Sqlite,
            PersistenceBackend::Lmdb => PersistenceEngine::Lmdb,
        }
    }
}

const DEFAULT_ORDER_BOOK_DEPTH: usize = 50;

pub const fn default_order_book_depth() -> usize {
    DEFAULT_ORDER_BOOK_DEPTH
}
const STRATEGY_LOCK_WARN_THRESHOLD: Duration = Duration::from_millis(25);
const STRATEGY_CALL_WARN_THRESHOLD: Duration = Duration::from_millis(250);

#[async_trait::async_trait]
trait LiveMarketStream: Send {
    async fn next_tick(&mut self) -> BrokerResult<Option<Tick>>;
    async fn next_candle(&mut self) -> BrokerResult<Option<Candle>>;
    async fn next_order_book(&mut self) -> BrokerResult<Option<OrderBook>>;
}

struct FactoryStreamAdapter {
    inner: Box<dyn ConnectorStream>,
}

impl FactoryStreamAdapter {
    fn new(inner: Box<dyn ConnectorStream>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl LiveMarketStream for FactoryStreamAdapter {
    async fn next_tick(&mut self) -> BrokerResult<Option<Tick>> {
        self.inner.next_tick().await
    }

    async fn next_candle(&mut self) -> BrokerResult<Option<Candle>> {
        self.inner.next_candle().await
    }

    async fn next_order_book(&mut self) -> BrokerResult<Option<OrderBook>> {
        self.inner.next_order_book().await
    }
}

#[derive(Clone)]
pub struct PersistenceSettings {
    pub engine: PersistenceEngine,
    pub state_path: PathBuf,
    pub algo_path: PathBuf,
}

impl PersistenceSettings {
    pub fn new(engine: PersistenceEngine, state_path: PathBuf) -> Self {
        let algo_path = match engine {
            PersistenceEngine::Sqlite => state_path.with_extension("algos.db"),
            PersistenceEngine::Lmdb => state_path.clone(),
        };
        Self {
            engine,
            state_path,
            algo_path,
        }
    }

    fn algo_repo_path(&self) -> &PathBuf {
        &self.algo_path
    }
}

struct PersistenceHandles {
    state: Arc<dyn StateRepository<Snapshot = LiveState>>,
    algo: Arc<dyn AlgoStateRepository<State = StoredAlgoState>>,
}

pub struct LiveSessionSettings {
    pub category: PublicChannel,
    pub interval: Interval,
    pub quantity: Quantity,
    pub slippage_bps: Decimal,
    pub fee_bps: Decimal,
    pub history: usize,
    pub metrics_addr: SocketAddr,
    pub persistence: PersistenceSettings,
    pub initial_balances: HashMap<Symbol, Decimal>,
    pub reporting_currency: Symbol,
    pub markets_file: Option<PathBuf>,
    pub alerting: AlertingConfig,
    pub exec_backend: ExecutionBackend,
    pub risk: RiskManagementConfig,
    pub reconciliation_interval: Duration,
    pub reconciliation_threshold: Decimal,
    pub driver: String,
    pub orderbook_depth: usize,
    pub record_path: Option<PathBuf>,
    pub control_addr: SocketAddr,
}

impl LiveSessionSettings {
    fn risk_limits(&self) -> RiskLimits {
        RiskLimits {
            max_order_quantity: self.risk.max_order_quantity.max(Decimal::ZERO),
            max_position_quantity: self.risk.max_position_quantity.max(Decimal::ZERO),
            max_order_notional: self
                .risk
                .max_order_notional
                .and_then(|limit| (limit > Decimal::ZERO).then_some(limit)),
        }
    }
}

fn build_persistence_handles(settings: &LiveSessionSettings) -> Result<PersistenceHandles> {
    match settings.persistence.engine {
        PersistenceEngine::Sqlite => {
            let state_repo: Arc<dyn StateRepository<Snapshot = LiveState>> = Arc::new(
                SqliteStateRepository::new(settings.persistence.state_path.clone()),
            );
            let algo_repo: Arc<dyn AlgoStateRepository<State = StoredAlgoState>> = Arc::new(
                SqliteAlgoStateRepository::new(settings.persistence.algo_repo_path())?,
            );
            Ok(PersistenceHandles {
                state: state_repo,
                algo: algo_repo,
            })
        }
        PersistenceEngine::Lmdb => {
            let journal = Arc::new(LmdbJournal::open(&settings.persistence.state_path)?);
            let state_repo: Arc<dyn StateRepository<Snapshot = LiveState>> =
                Arc::new(journal.state_repo());
            let algo_repo: Arc<dyn AlgoStateRepository<State = StoredAlgoState>> =
                Arc::new(journal.algo_repo());
            Ok(PersistenceHandles {
                state: state_repo,
                algo: algo_repo,
            })
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
    ensure_builtin_connectors_registered();
    let connector_payload = build_exchange_payload(&exchange, &settings);
    let connector_factory = get_connector_factory(&settings.driver)
        .ok_or_else(|| anyhow!("driver {} is not registered", settings.driver))?;
    let stream_config = ConnectorStreamConfig {
        ws_url: Some(exchange.ws_url.clone()),
        metadata: json!({
            "category": settings.category.as_path(),
            "symbols": symbols.clone(),
            "orderbook_depth": settings.orderbook_depth,
        }),
        connection_status: Some(public_connection.clone()),
    };
    let mut connector_stream = connector_factory
        .create_market_stream(&connector_payload, stream_config)
        .await
        .map_err(|err| anyhow!("failed to create market stream: {err}"))?;
    connector_stream
        .subscribe(&symbols, settings.interval)
        .await
        .map_err(|err| anyhow!("failed to subscribe via connector: {err}"))?;
    let market_stream: Box<dyn LiveMarketStream> =
        Box::new(FactoryStreamAdapter::new(connector_stream));

    let execution_client =
        build_execution_client(&settings, connector_factory.clone(), &connector_payload).await?;
    let market_registry = load_market_registry(execution_client.clone(), &settings).await?;
    if matches!(settings.exec_backend, ExecutionBackend::Live) {
        info!(
            rest = %exchange.rest_url,
            driver = ?settings.driver,
            "live execution enabled via {:?} REST",
            settings.driver
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

    let mut bootstrap = None;
    if matches!(settings.exec_backend, ExecutionBackend::Live) {
        info!("synchronizing portfolio snapshot from exchange");
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
        bootstrap = Some(LiveBootstrap {
            positions,
            balances,
            open_orders,
        });
    }

    let persistence = build_persistence_handles(&settings)?;

    // Create orchestrator with execution engine
    let initial_open_orders = bootstrap
        .as_ref()
        .map(|data| data.open_orders.clone())
        .unwrap_or_default();
    let orchestrator = OrderOrchestrator::new(
        Arc::new(execution),
        persistence.algo.clone(),
        initial_open_orders,
    )
    .await?;

    let runtime = LiveRuntime::new(
        market_stream,
        strategy,
        symbols,
        orchestrator,
        persistence.state,
        settings,
        exchange.ws_url.clone(),
        market_registry,
        shutdown,
        public_connection,
        private_connection,
        bootstrap,
    )
    .await?;
    runtime.run().await
}

async fn build_execution_client(
    settings: &LiveSessionSettings,
    connector_factory: Arc<dyn ConnectorFactory>,
    connector_payload: &Value,
) -> Result<Arc<dyn ExecutionClient>> {
    match settings.exec_backend {
        ExecutionBackend::Paper => {
            if settings.driver == "paper" {
                return connector_factory
                    .create_execution_client(connector_payload)
                    .await
                    .map_err(|err| anyhow!("failed to create execution client: {err}"));
            }
            Ok(Arc::new(PaperExecutionClient::new(
                "paper".to_string(),
                vec!["BTCUSDT".to_string()],
                settings.slippage_bps,
                FeeScheduleConfig::with_defaults(
                    settings.fee_bps.max(Decimal::ZERO),
                    settings.fee_bps.max(Decimal::ZERO),
                )
                .build_model(),
            )))
        }
        ExecutionBackend::Live => connector_factory
            .create_execution_client(connector_payload)
            .await
            .map_err(|err| anyhow!("failed to create execution client: {err}")),
    }
}

struct LiveRuntime {
    market: Box<dyn LiveMarketStream>,
    orchestrator: Arc<OrderOrchestrator>,
    state_repo: Arc<dyn StateRepository<Snapshot = LiveState>>,
    persisted: Arc<Mutex<LiveState>>,
    event_bus: Arc<EventBus>,
    recorder: Option<ParquetRecorder>,
    control_task: Option<JoinHandle<()>>,
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

struct LiveBootstrap {
    positions: Vec<Position>,
    balances: Vec<AccountBalance>,
    open_orders: Vec<Order>,
}

impl LiveRuntime {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        market: Box<dyn LiveMarketStream>,
        mut strategy: Box<dyn Strategy>,
        symbols: Vec<String>,
        orchestrator: OrderOrchestrator,
        state_repo: Arc<dyn StateRepository<Snapshot = LiveState>>,
        settings: LiveSessionSettings,
        #[cfg_attr(not(feature = "binance"), allow(unused_variables))] exchange_ws_url: String,
        market_registry: Arc<MarketRegistry>,
        shutdown: ShutdownSignal,
        public_connection: Arc<AtomicBool>,
        private_connection: Option<Arc<AtomicBool>>,
        bootstrap: Option<LiveBootstrap>,
    ) -> Result<Self> {
        let mut strategy_ctx = StrategyContext::new(settings.history);
        let driver = Arc::new(settings.driver.clone());
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
        if let Some(data) = bootstrap {
            persisted.open_orders = data.open_orders;
            live_bootstrap = Some((data.positions, data.balances));
        } else if matches!(settings.exec_backend, ExecutionBackend::Live) {
            warn!("live session missing bootstrap data; continuing without remote snapshot");
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

        let mut market_snapshots = HashMap::new();
        for symbol in &symbols {
            let mut snapshot = MarketSnapshot::default();
            if let Some(price) = persisted.last_prices.get(symbol).copied() {
                snapshot.last_trade = Some(price);
            }
            market_snapshots.insert(symbol.clone(), snapshot);
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
            let exec_client = execution_engine.client();
            match settings.driver.as_str() {
                "bybit" | "" => {
                    #[cfg(feature = "bybit")]
                    {
                        let bybit = exec_client
                            .as_ref()
                            .as_any()
                            .downcast_ref::<BybitClient>()
                            .ok_or_else(|| anyhow!("execution client is not Bybit"))?;
                        let creds = bybit
                            .get_credentials()
                            .ok_or_else(|| anyhow!("live execution requires Bybit credentials"))?;
                        spawn_bybit_private_stream(
                            creds,
                            bybit.get_ws_url(),
                            private_event_tx.clone(),
                            exec_client.clone(),
                            symbols.clone(),
                            last_private_sync.clone(),
                            private_connection.clone(),
                            metrics.clone(),
                            shutdown.clone(),
                        );
                    }
                    #[cfg(not(feature = "bybit"))]
                    {
                        bail!("driver 'bybit' is unavailable without the 'bybit' feature");
                    }
                }
                "binance" => {
                    #[cfg(feature = "binance")]
                    {
                        spawn_binance_private_stream(
                            exec_client.clone(),
                            exchange_ws_url.clone(),
                            private_event_tx.clone(),
                            private_connection.clone(),
                            metrics.clone(),
                            shutdown.clone(),
                        );
                    }
                    #[cfg(not(feature = "binance"))]
                    {
                        bail!("driver 'binance' is unavailable without the 'binance' feature");
                    }
                }
                "paper" => {}
                other => {
                    bail!("private stream unsupported for driver '{other}'");
                }
            }
        }

        let recorder = if let Some(record_path) = settings.record_path.clone() {
            let config = RecorderConfig {
                root: record_path.clone(),
                ..RecorderConfig::default()
            };
            match ParquetRecorder::spawn(config).await {
                Ok(recorder) => {
                    info!(path = %record_path.display(), "flight recorder enabled");
                    Some(recorder)
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        path = %record_path.display(),
                        "failed to start flight recorder"
                    );
                    None
                }
            }
        } else {
            None
        };
        let recorder_handle = recorder.as_ref().map(|rec| rec.handle());

        let strategy = Arc::new(Mutex::new(strategy));
        let strategy_ctx = Arc::new(Mutex::new(strategy_ctx));
        let portfolio = Arc::new(Mutex::new(portfolio));
        let market_cache = Arc::new(Mutex::new(market_snapshots));
        let persisted = Arc::new(Mutex::new(persisted));
        let orchestrator = Arc::new(orchestrator);
        let event_bus = Arc::new(EventBus::new(2048));
        let last_data_timestamp = Arc::new(AtomicI64::new(0));
        let control_task = control::spawn_control_plane(
            settings.control_addr,
            portfolio.clone(),
            orchestrator.clone(),
            persisted.clone(),
            last_data_timestamp.clone(),
            event_bus.clone(),
            shutdown.clone(),
        );
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
            market_cache.clone(),
            state_repo.clone(),
            persisted.clone(),
            settings.exec_backend,
            recorder_handle.clone(),
            last_data_timestamp.clone(),
            driver.clone(),
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
            state_path = %settings.persistence.state_path.display(),
            persistence_engine = ?settings.persistence.engine,
            history = settings.history,
            "market stream ready"
        );

        for symbol in &symbols {
            let ctx = shared_risk_context(symbol, &portfolio, &market_cache, &persisted).await;
            orchestrator.update_risk_context(symbol.clone(), ctx);
        }

        Ok(Self {
            market,
            orchestrator,
            state_repo,
            persisted,
            event_bus,
            recorder,
            control_task: Some(control_task),
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

        'run: while !self.shutdown.triggered() {
            let mut progressed = false;

            let tick = tokio::select! {
                res = self.market.next_tick() => Some(res),
                _ = self.shutdown.wait() => None,
            };
            match tick {
                Some(res) => {
                    if let Some(tick) = res? {
                        progressed = true;
                        self.event_bus.publish(Event::Tick(TickEvent { tick }));
                    }
                }
                None => break 'run,
            }

            let candle = tokio::select! {
                res = self.market.next_candle() => Some(res),
                _ = self.shutdown.wait() => None,
            };
            match candle {
                Some(res) => {
                    if let Some(candle) = res? {
                        progressed = true;
                        self.event_bus
                            .publish(Event::Candle(CandleEvent { candle }));
                    }
                }
                None => break 'run,
            }

            let book = tokio::select! {
                res = self.market.next_order_book() => Some(res),
                _ = self.shutdown.wait() => None,
            };
            match book {
                Some(res) => {
                    if let Some(book) = res? {
                        progressed = true;
                        self.event_bus
                            .publish(Event::OrderBook(OrderBookEvent { order_book: book }));
                    }
                }
                None => break 'run,
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
                _ = self.shutdown.wait() => break 'run,
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
        if let Err(err) = persist_state(
            self.state_repo.clone(),
            self.persisted.clone(),
            Some(self.strategy.clone()),
        )
        .await
        {
            warn!(error = %err, "failed to persist shutdown state");
        }
        if let Some(task) = self.control_task.take() {
            if let Err(err) = task.await {
                warn!(error = %err, "control plane server task aborted");
            }
        }
        if let Some(recorder) = self.recorder.take() {
            if let Err(err) = recorder.shutdown().await {
                warn!(error = %err, "failed to flush flight recorder");
            }
        }
        Ok(())
    }
}

struct ReconciliationContext {
    client: Arc<dyn ExecutionClient>,
    portfolio: Arc<Mutex<Portfolio>>,
    persisted: Arc<Mutex<LiveState>>,
    state_repo: Arc<dyn StateRepository<Snapshot = LiveState>>,
    alerts: Arc<AlertManager>,
    metrics: Arc<LiveMetrics>,
    reporting_currency: Symbol,
    threshold: Decimal,
}

struct ReconciliationContextConfig {
    client: Arc<dyn ExecutionClient>,
    portfolio: Arc<Mutex<Portfolio>>,
    persisted: Arc<Mutex<LiveState>>,
    state_repo: Arc<dyn StateRepository<Snapshot = LiveState>>,
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

fn build_exchange_payload(exchange: &ExchangeConfig, settings: &LiveSessionSettings) -> Value {
    let mut payload = serde_json::Map::new();
    payload.insert("rest_url".into(), Value::String(exchange.rest_url.clone()));
    payload.insert("ws_url".into(), Value::String(exchange.ws_url.clone()));
    payload.insert("api_key".into(), Value::String(exchange.api_key.clone()));
    payload.insert(
        "api_secret".into(),
        Value::String(exchange.api_secret.clone()),
    );
    payload.insert(
        "category".into(),
        Value::String(settings.category.as_path().to_string()),
    );
    payload.insert(
        "orderbook_depth".into(),
        Value::Number(serde_json::Number::from(settings.orderbook_depth as u64)),
    );
    if let Value::Object(extra) = exchange.params.clone() {
        for (key, value) in extra {
            payload.insert(key, value);
        }
    }
    Value::Object(payload)
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

    pub fn triggered(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    pub async fn wait(&self) {
        if self.triggered() {
            return;
        }
        self.notify.notified().await;
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

impl Clone for ShutdownSignal {
    fn clone(&self) -> Self {
        Self {
            flag: self.flag.clone(),
            notify: self.notify.clone(),
        }
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
    state_repo: Arc<dyn StateRepository<Snapshot = LiveState>>,
    persisted: Arc<Mutex<LiveState>>,
    exec_backend: ExecutionBackend,
    recorder: Option<RecorderHandle>,
    last_data_timestamp: Arc<AtomicI64>,
    driver: Arc<String>,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();
    let market_recorder = recorder.clone();

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
    let market_data_tracker = last_data_timestamp.clone();
    let driver_clone = driver.clone();
    handles.push(tokio::spawn(async move {
        let recorder = market_recorder;
        let mut stream = market_bus.subscribe();
        loop {
            match stream.recv().await {
                Ok(Event::Tick(evt)) => {
                    if let Some(handle) = recorder.as_ref() {
                        handle.record_tick(evt.tick.clone());
                    }
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
                        market_data_tracker.clone(),
                    )
                    .await
                    {
                        warn!(error = %err, "tick handler failed");
                    }
                }
                Ok(Event::Candle(evt)) => {
                    if let Some(handle) = recorder.as_ref() {
                        handle.record_candle(evt.candle.clone());
                    }
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
                        market_data_tracker.clone(),
                    )
                    .await
                    {
                        warn!(error = %err, "candle handler failed");
                    }
                }
                Ok(Event::OrderBook(evt)) => {
                    if let Some(handle) = recorder.as_ref() {
                        handle.record_order_book(evt.order_book.clone());
                    }
                    if let Err(err) = process_order_book_event(
                        evt.order_book,
                        market_strategy.clone(),
                        market_ctx.clone(),
                        market_metrics.clone(),
                        market_alerts.clone(),
                        market_snapshot.clone(),
                        market_bus.clone(),
                        market_data_tracker.clone(),
                        driver_clone.clone(),
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
    let exec_recorder = recorder.clone();
    handles.push(tokio::spawn(async move {
        let orchestrator = exec_orchestrator.clone();
        let recorder = exec_recorder;
        let mut stream = exec_bus.subscribe();
        loop {
            match stream.recv().await {
                Ok(Event::Signal(evt)) => {
                    if let Some(handle) = recorder.as_ref() {
                        handle.record_signal(evt.signal.clone());
                    }
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
    let fill_recorder = recorder.clone();
    handles.push(tokio::spawn(async move {
        let orchestrator = fill_orchestrator.clone();
        let persisted = fill_persisted.clone();
        let recorder = fill_recorder;
        let mut stream = fill_bus.subscribe();
        loop {
            match stream.recv().await {
                Ok(Event::Fill(evt)) => {
                    if let Some(handle) = recorder.as_ref() {
                        handle.record_fill(evt.fill.clone());
                    }
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
    let order_recorder = recorder;
    handles.push(tokio::spawn(async move {
        let orchestrator = order_orchestrator.clone();
        let persisted = order_persisted.clone();
        let recorder = order_recorder;
        let mut stream = order_bus.subscribe();
        loop {
            match stream.recv().await {
                Ok(Event::OrderUpdate(evt)) => {
                    if let Some(handle) = recorder.as_ref() {
                        handle.record_order(evt.order.clone());
                    }
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
    state_repo: Arc<dyn StateRepository<Snapshot = LiveState>>,
    persisted: Arc<Mutex<LiveState>>,
    bus: Arc<EventBus>,
    last_data_timestamp: Arc<AtomicI64>,
) -> Result<()> {
    metrics.inc_tick();
    metrics.update_staleness(0.0);
    metrics.update_last_data_timestamp(Utc::now().timestamp() as f64);
    last_data_timestamp.store(tick.exchange_timestamp.timestamp(), Ordering::SeqCst);
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
    debug!(symbol = %tick.symbol, price = %tick.price, "completed tick processing");
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
    state_repo: Arc<dyn StateRepository<Snapshot = LiveState>>,
    persisted: Arc<Mutex<LiveState>>,
    bus: Arc<EventBus>,
    last_data_timestamp: Arc<AtomicI64>,
) -> Result<()> {
    metrics.inc_candle();
    metrics.update_staleness(0.0);
    metrics.update_last_data_timestamp(Utc::now().timestamp() as f64);
    last_data_timestamp.store(candle.timestamp.timestamp(), Ordering::SeqCst);
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
    debug!(symbol = %candle.symbol, close = %candle.close, "completed candle processing");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_order_book_event(
    mut book: OrderBook,
    strategy: Arc<Mutex<Box<dyn Strategy>>>,
    strategy_ctx: Arc<Mutex<StrategyContext>>,
    metrics: Arc<LiveMetrics>,
    alerts: Arc<AlertManager>,
    _market: Arc<Mutex<HashMap<String, MarketSnapshot>>>,
    bus: Arc<EventBus>,
    last_data_timestamp: Arc<AtomicI64>,
    driver: Arc<String>,
) -> Result<()> {
    metrics.update_staleness(0.0);
    alerts.heartbeat().await;
    last_data_timestamp.store(book.timestamp.timestamp(), Ordering::SeqCst);
    let driver_name = driver.as_str();
    let local_checksum = if let Some(cs) = book.local_checksum {
        cs
    } else {
        let computed = book.computed_checksum(None);
        book.local_checksum = Some(computed);
        computed
    };
    if let Some(expected) = book.exchange_checksum {
        if expected != local_checksum {
            metrics.inc_checksum_mismatch(driver_name, &book.symbol);
            alerts
                .order_book_checksum_mismatch(driver_name, &book.symbol, expected, local_checksum)
                .await;
        }
    }
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
    state_repo: Arc<dyn StateRepository<Snapshot = LiveState>>,
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
    state_repo: Arc<dyn StateRepository<Snapshot = LiveState>>,
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
        let drained = strat.drain_signals();
        debug!(count = drained.len(), "strategy drained signals");
        drained
    };
    if signals.is_empty() {
        return;
    }
    metrics.inc_signals(signals.len());
    for signal in signals {
        debug!(id = %signal.id, symbol = %signal.symbol, kind = ?signal.kind, "publishing signal event");
        bus.publish(Event::Signal(SignalEvent { signal }));
    }
}

async fn persist_state(
    repo: Arc<dyn StateRepository<Snapshot = LiveState>>,
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
    let mut catalog = InstrumentCatalog::new();
    if let Some(path) = &settings.markets_file {
        catalog
            .add_file(path)
            .with_context(|| format!("failed to load markets from {}", path.display()))?;
    }

    if !settings.exec_backend.is_paper() {
        let instruments = client
            .list_instruments(settings.category.as_path())
            .await
            .context("failed to fetch instruments from execution client")?;
        catalog
            .add_instruments(instruments)
            .map_err(|err| anyhow!(err.to_string()))?;
    } else if catalog.is_empty() {
        return Err(anyhow!(
            "paper execution requires --markets-file when exchange metadata is unavailable"
        ));
    }

    if catalog.is_empty() {
        return Err(anyhow!(
            "no market metadata available; supply --markets-file or use a live exchange"
        ));
    }

    let registry = catalog.build().map_err(|err| anyhow!(err.to_string()))?;
    Ok(Arc::new(registry))
}

#[cfg(feature = "bybit")]
#[allow(clippy::too_many_arguments)]
fn spawn_bybit_private_stream(
    creds: BybitCredentials,
    ws_url: String,
    private_tx: mpsc::Sender<BrokerEvent>,
    exec_client: Arc<dyn ExecutionClient>,
    symbols: Vec<String>,
    last_sync: Arc<tokio::sync::Mutex<Option<DateTime<Utc>>>>,
    private_connection_flag: Option<Arc<AtomicBool>>,
    metrics: Arc<LiveMetrics>,
    shutdown: ShutdownSignal,
) {
    let exchange_id = exec_client
        .as_any()
        .downcast_ref::<BybitClient>()
        .map(|client| client.exchange())
        .unwrap_or(ExchangeId::UNSPECIFIED);
    tokio::spawn(async move {
        loop {
            match tesser_bybit::ws::connect_private(
                &ws_url,
                &creds,
                private_connection_flag.clone(),
            )
            .await
            {
                Ok(mut socket) => {
                    if let Some(flag) = &private_connection_flag {
                        flag.store(true, Ordering::SeqCst);
                    }
                    metrics.update_connection_status("private", true);
                    info!("Connected to Bybit private WebSocket stream");
                    for symbol in &symbols {
                        match exec_client.list_open_orders(symbol).await {
                            Ok(orders) => {
                                for order in orders {
                                    if let Err(err) =
                                        private_tx.send(BrokerEvent::OrderUpdate(order)).await
                                    {
                                        error!("failed to send reconciled order update: {err}");
                                    }
                                }
                            }
                            Err(e) => {
                                error!("failed to reconcile open orders for {symbol}: {e}");
                            }
                        }
                    }
                    if let Some(bybit) = exec_client.as_any().downcast_ref::<BybitClient>() {
                        let since = {
                            let guard = last_sync.lock().await;
                            guard.unwrap_or_else(|| Utc::now() - chrono::Duration::minutes(30))
                        };
                        match bybit.list_executions_since(since).await {
                            Ok(fills) => {
                                for fill in fills {
                                    if let Err(err) = private_tx.send(BrokerEvent::Fill(fill)).await
                                    {
                                        error!("failed to send reconciled fill: {err}");
                                    }
                                }
                            }
                            Err(e) => {
                                error!("failed to reconcile executions since {:?}: {}", since, e);
                            }
                        }
                        let mut guard = last_sync.lock().await;
                        *guard = Some(Utc::now());
                    }

                    while let Some(msg) = socket.next().await {
                        if shutdown.triggered() {
                            break;
                        }
                        if let Ok(Message::Text(text)) = msg {
                            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                                if let Some(topic) = value.get("topic").and_then(|v| v.as_str()) {
                                    match topic {
                                        "order" => {
                                            if let Ok(msg) = serde_json::from_value::<
                                                PrivateMessage<BybitWsOrder>,
                                            >(
                                                value.clone()
                                            ) {
                                                for update in msg.data {
                                                    if let Ok(order) =
                                                        update.to_tesser_order(exchange_id, None)
                                                    {
                                                        if let Err(err) = private_tx
                                                            .send(BrokerEvent::OrderUpdate(order))
                                                            .await
                                                        {
                                                            error!(
                                                                "failed to send private order update: {err}"
                                                            );
                                                        }
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
                                                    if let Ok(fill) =
                                                        exec.to_tesser_fill(exchange_id)
                                                    {
                                                        if let Err(err) = private_tx
                                                            .send(BrokerEvent::Fill(fill))
                                                            .await
                                                        {
                                                            error!(
                                                                "failed to send private fill event: {err}"
                                                            );
                                                        }
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
                    metrics.update_connection_status("private", false);
                    error!("Bybit private WebSocket connection failed: {e}. Retrying...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
            if shutdown.triggered() {
                break;
            }
        }
    });
}

#[cfg(feature = "binance")]
#[allow(clippy::too_many_arguments)]
fn spawn_binance_private_stream(
    exec_client: Arc<dyn ExecutionClient>,
    ws_url: String,
    private_tx: mpsc::Sender<BrokerEvent>,
    private_connection_flag: Option<Arc<AtomicBool>>,
    metrics: Arc<LiveMetrics>,
    shutdown: ShutdownSignal,
) {
    tokio::spawn(async move {
        loop {
            let Some(binance) = exec_client
                .as_ref()
                .as_any()
                .downcast_ref::<BinanceClient>()
            else {
                warn!("execution client is not Binance");
                return;
            };
            let exchange = binance.exchange();
            let listen_key = match binance.start_user_stream().await {
                Ok(key) => key,
                Err(err) => {
                    error!("failed to start Binance user stream: {err}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            match BinanceUserDataStream::connect(&ws_url, &listen_key).await {
                Ok(user_stream) => {
                    if let Some(flag) = &private_connection_flag {
                        flag.store(true, Ordering::SeqCst);
                    }
                    metrics.update_connection_status("private", true);
                    let (reconnect_tx, mut reconnect_rx) = mpsc::channel(1);
                    let tx_orders = private_tx.clone();
                    let exchange_id = exchange;
                    user_stream.on_event(move |event| {
                        if let Some(update) = extract_order_update(&event) {
                            if let Some(order) = order_from_update(exchange_id, update) {
                                let _ = tx_orders.blocking_send(BrokerEvent::OrderUpdate(order));
                            }
                            if let Some(fill) = fill_from_update(exchange_id, update) {
                                let _ = tx_orders.blocking_send(BrokerEvent::Fill(fill));
                            }
                        }
                        if matches!(event, UserDataStreamEventsResponse::ListenKeyExpired(_)) {
                            let _ = reconnect_tx.try_send(());
                        }
                    });
                    let keepalive_client = exec_client.clone();
                    let keepalive_handle = tokio::spawn(async move {
                        let mut interval = tokio::time::interval(Duration::from_secs(30 * 60));
                        loop {
                            interval.tick().await;
                            let Some(client) = keepalive_client
                                .as_ref()
                                .as_any()
                                .downcast_ref::<BinanceClient>()
                            else {
                                break;
                            };
                            if client.keepalive_user_stream().await.is_err() {
                                break;
                            }
                        }
                    });
                    tokio::select! {
                        _ = reconnect_rx.recv() => {
                            warn!("binance listen key expired; reconnecting");
                        }
                        _ = shutdown.wait() => {
                            keepalive_handle.abort();
                            let _ = user_stream.unsubscribe().await;
                            return;
                        }
                    }
                    keepalive_handle.abort();
                    let _ = user_stream.unsubscribe().await;
                }
                Err(err) => {
                    error!("failed to connect to Binance user stream: {err}");
                }
            }
            if let Some(flag) = &private_connection_flag {
                flag.store(false, Ordering::SeqCst);
            }
            metrics.update_connection_status("private", false);
            if shutdown.triggered() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}
