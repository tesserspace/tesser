use std::any::Any;
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
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

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
    ConnectorStream, ConnectorStreamConfig, ExecutionClient, RouterExecutionClient,
};
#[cfg(feature = "bybit")]
use tesser_bybit::ws::{BybitWsExecution, BybitWsOrder, PrivateMessage};
#[cfg(feature = "bybit")]
use tesser_bybit::{register_factory as register_bybit_factory, BybitClient, BybitCredentials};
use tesser_config::{AlertingConfig, ExchangeConfig, PersistenceEngine, RiskManagementConfig};
use tesser_core::{
    AccountBalance, AssetId, Candle, ExchangeId, ExitStrategy, Fill, Interval, Order, OrderBook,
    OrderStatus, Position, Price, Quantity, Side, Signal, SignalKind, Symbol, Tick,
};
use tesser_data::recorder::{ParquetRecorder, RecorderConfig, RecorderHandle};
use tesser_events::{
    CandleEvent, Event, EventBus, FillEvent, OrderBookEvent, OrderUpdateEvent, SignalEvent,
    TickEvent,
};
use tesser_execution::{
    AlgoStateRepository, BasicRiskChecker, ExecutionEngine, FixedOrderSizer, OrderOrchestrator,
    PanicCloseConfig, PanicObserver, PreTradeRiskChecker, RiskContext, RiskLimits,
    SqliteAlgoStateRepository, StoredAlgoState,
};
use tesser_journal::LmdbJournal;
use tesser_markets::{InstrumentCatalog, MarketRegistry};
use tesser_paper::{FeeScheduleConfig, PaperExecutionClient, PaperFactory};
use tesser_portfolio::{
    LiveState, Portfolio, PortfolioConfig, PortfolioState, SqliteStateRepository, StateRepository,
};
use tesser_strategy::{
    PairTradeSnapshot, PairsTradingArbitrage, Strategy, StrategyContext, StrategyError,
    StrategyResult,
};

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

#[derive(Clone)]
enum MarketEvent {
    Tick(Tick),
    Candle(Candle),
    OrderBook(OrderBook),
}

#[derive(Clone)]
pub struct StrategyHandle {
    tx: mpsc::Sender<StrategyCommand>,
}

impl StrategyHandle {
    async fn send_account_event(&self, event: StrategyAccountEvent) {
        let _ = self.tx.send(StrategyCommand::Account(event)).await;
    }

    async fn snapshot(&self) -> Option<serde_json::Value> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(StrategyCommand::Request(StrategyRequest::Snapshot {
                respond_to: tx,
            }))
            .await;
        rx.await.ok().flatten()
    }

    pub async fn list_managed_trades(&self) -> anyhow::Result<Vec<PairTradeSnapshot>> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(StrategyCommand::Request(
                StrategyRequest::ListManagedTrades { respond_to: tx },
            ))
            .await;
        rx.await
            .unwrap_or_else(|_| Err(anyhow!("strategy channel closed")))
    }

    pub async fn update_exit_strategy(
        &self,
        trade_id: Uuid,
        exit: ExitStrategy,
    ) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(StrategyCommand::Request(
                StrategyRequest::UpdateExitStrategy {
                    trade_id,
                    exit,
                    respond_to: tx,
                },
            ))
            .await;
        rx.await
            .unwrap_or_else(|_| Err(anyhow!("strategy channel closed")))
    }
}

enum StrategyCommand {
    Account(StrategyAccountEvent),
    Request(StrategyRequest),
}

enum StrategyAccountEvent {
    Fill {
        fill: Fill,
        positions: Vec<Position>,
    },
}

enum StrategyRequest {
    Snapshot {
        respond_to: oneshot::Sender<Option<serde_json::Value>>,
    },
    ListManagedTrades {
        respond_to: oneshot::Sender<anyhow::Result<Vec<PairTradeSnapshot>>>,
    },
    UpdateExitStrategy {
        trade_id: Uuid,
        exit: ExitStrategy,
        respond_to: oneshot::Sender<anyhow::Result<()>>,
    },
}

#[derive(Clone)]
pub struct OmsHandle {
    tx: mpsc::Sender<OmsRequest>,
}

impl OmsHandle {
    pub async fn portfolio_state(&self) -> Option<PortfolioState> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(OmsRequest::PortfolioState { respond_to: tx })
            .await;
        rx.await.ok().flatten()
    }

    pub async fn open_orders(&self) -> Vec<Order> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(OmsRequest::OpenOrders { respond_to: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    pub async fn status(&self) -> OmsStatus {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(OmsRequest::Status { respond_to: tx }).await;
        rx.await.unwrap_or_default()
    }

    pub(crate) async fn portfolio_summary(&self) -> PortfolioSummary {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(OmsRequest::PortfolioSummary { respond_to: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    pub async fn enter_liquidate_only(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(OmsRequest::EnterLiquidateOnly { respond_to: tx })
            .await;
        rx.await.unwrap_or(false)
    }
}

#[derive(Default, Clone)]
pub struct OmsStatus {
    pub equity: Price,
    pub liquidate_only: bool,
}

#[derive(Default, Clone)]
pub(crate) struct PortfolioSummary {
    positions: Vec<Position>,
    cash: Price,
}

enum OmsRequest {
    PortfolioState {
        respond_to: oneshot::Sender<Option<PortfolioState>>,
    },
    OpenOrders {
        respond_to: oneshot::Sender<Vec<Order>>,
    },
    Status {
        respond_to: oneshot::Sender<OmsStatus>,
    },
    PortfolioSummary {
        respond_to: oneshot::Sender<PortfolioSummary>,
    },
    EnterLiquidateOnly {
        respond_to: oneshot::Sender<bool>,
    },
}

#[derive(Clone)]
struct PersistenceHandle {
    tx: mpsc::Sender<PersistenceCommand>,
}

impl PersistenceHandle {
    async fn save(&self, snapshot: LiveState) {
        let _ = self
            .tx
            .send(PersistenceCommand::Save(Box::new(snapshot)))
            .await;
    }

    async fn shutdown(&self) {
        let _ = self.tx.send(PersistenceCommand::Shutdown).await;
    }
}

enum PersistenceCommand {
    Save(Box<LiveState>),
    Shutdown,
}

struct PanicAlertHook {
    metrics: Arc<LiveMetrics>,
    alerts: Arc<AlertManager>,
}

impl PanicAlertHook {
    fn new(metrics: Arc<LiveMetrics>, alerts: Arc<AlertManager>) -> Self {
        Self { metrics, alerts }
    }
}

impl PanicObserver for PanicAlertHook {
    fn on_group_event(&self, group_id: Uuid, symbol: Symbol, quantity: Quantity, reason: &str) {
        self.metrics.inc_panic_close();
        let alerts = self.alerts.clone();
        let title = "Execution group panic close";
        let message = format!("Group {group_id} panic-closed {symbol} qty={quantity}: {reason}");
        tokio::spawn(async move {
            alerts.notify(title, &message).await;
        });
    }
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
const STRATEGY_CALL_WARN_THRESHOLD: Duration = Duration::from_millis(250);
const MARKET_EVENT_TIMEOUT: Duration = Duration::from_millis(10);

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

struct RouterMarketStream {
    tick_rx: mpsc::Receiver<Tick>,
    candle_rx: mpsc::Receiver<Candle>,
    book_rx: mpsc::Receiver<OrderBook>,
    tasks: Vec<JoinHandle<()>>,
}

impl RouterMarketStream {
    fn new(streams: Vec<(String, Box<dyn LiveMarketStream>)>, shutdown: ShutdownSignal) -> Self {
        let (tick_tx, tick_rx) = mpsc::channel(512);
        let (candle_tx, candle_rx) = mpsc::channel(512);
        let (book_tx, book_rx) = mpsc::channel(512);
        let mut tasks = Vec::new();
        for (name, mut stream) in streams {
            let tick_tx = tick_tx.clone();
            let candle_tx = candle_tx.clone();
            let book_tx = book_tx.clone();
            let shutdown = shutdown.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    if shutdown.triggered() {
                        break;
                    }
                    let mut emitted = false;

                    let tick = tokio::select! {
                        res = stream.next_tick() => res,
                        _ = shutdown.wait() => break,
                    };
                    match tick {
                        Ok(Some(event)) => {
                            emitted = true;
                            if tick_tx.send(event).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => {}
                        Err(err) => {
                            warn!(exchange = %name, error = %err, "market stream tick failed");
                            break;
                        }
                    }

                    let candle = tokio::select! {
                        res = stream.next_candle() => res,
                        _ = shutdown.wait() => break,
                    };
                    match candle {
                        Ok(Some(event)) => {
                            emitted = true;
                            if candle_tx.send(event).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => {}
                        Err(err) => {
                            warn!(exchange = %name, error = %err, "market stream candle failed");
                            break;
                        }
                    }

                    let book = tokio::select! {
                        res = stream.next_order_book() => res,
                        _ = shutdown.wait() => break,
                    };
                    match book {
                        Ok(Some(event)) => {
                            emitted = true;
                            if book_tx.send(event).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => {}
                        Err(err) => {
                            warn!(exchange = %name, error = %err, "market stream order book failed");
                            break;
                        }
                    }

                    if !emitted && !shutdown.sleep(Duration::from_millis(5)).await {
                        break;
                    }
                }
            }));
        }
        Self {
            tick_rx,
            candle_rx,
            book_rx,
            tasks,
        }
    }
}

#[async_trait::async_trait]
impl LiveMarketStream for RouterMarketStream {
    async fn next_tick(&mut self) -> BrokerResult<Option<Tick>> {
        Ok(self.tick_rx.recv().await)
    }

    async fn next_candle(&mut self) -> BrokerResult<Option<Candle>> {
        Ok(self.candle_rx.recv().await)
    }

    async fn next_order_book(&mut self) -> BrokerResult<Option<OrderBook>> {
        Ok(self.book_rx.recv().await)
    }
}

impl Drop for RouterMarketStream {
    fn drop(&mut self) {
        for handle in &self.tasks {
            handle.abort();
        }
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

#[derive(Clone)]
pub struct NamedExchange {
    pub name: String,
    pub config: ExchangeConfig,
}

struct ExchangeRoute {
    name: String,
    driver: String,
    #[cfg(feature = "binance")]
    ws_url: String,
    execution: Arc<dyn ExecutionClient>,
}

struct ExchangeBuildResult {
    execution_client: Arc<dyn ExecutionClient>,
    router: Option<Arc<RouterExecutionClient>>,
    market_stream: Box<dyn LiveMarketStream>,
    routes: Vec<ExchangeRoute>,
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
    pub initial_balances: HashMap<AssetId, Decimal>,
    pub reporting_currency: AssetId,
    pub markets_file: Option<PathBuf>,
    pub alerting: AlertingConfig,
    pub exec_backend: ExecutionBackend,
    pub risk: RiskManagementConfig,
    pub reconciliation_interval: Duration,
    pub reconciliation_threshold: Decimal,
    pub orderbook_depth: usize,
    pub record_path: Option<PathBuf>,
    pub control_addr: SocketAddr,
    pub panic_close: PanicCloseConfig,
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
    symbols: Vec<Symbol>,
    exchanges: Vec<NamedExchange>,
    settings: LiveSessionSettings,
) -> Result<()> {
    run_live_with_shutdown(
        strategy,
        symbols,
        exchanges,
        settings,
        ShutdownSignal::new(),
    )
    .await
}

/// Variant of [`run_live`] that accepts a manually controlled shutdown signal.
pub async fn run_live_with_shutdown(
    strategy: Box<dyn Strategy>,
    symbols: Vec<Symbol>,
    exchanges: Vec<NamedExchange>,
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
    if exchanges.is_empty() {
        return Err(anyhow!("no exchange profiles supplied"));
    }
    let symbol_codes: Vec<String> = symbols
        .iter()
        .map(|symbol| symbol.code().to_string())
        .collect();
    let driver_label = exchanges
        .iter()
        .map(|ex| ex.config.driver.clone())
        .collect::<Vec<_>>()
        .join(",");

    let ExchangeBuildResult {
        execution_client,
        router,
        market_stream,
        routes,
    } = build_exchange_routes(
        &settings,
        &exchanges,
        &symbols,
        &symbol_codes,
        public_connection.clone(),
        shutdown.clone(),
    )
    .await?;
    let market_registry = load_market_registry(execution_client.clone(), &settings).await?;
    if matches!(settings.exec_backend, ExecutionBackend::Live) {
        info!(drivers = %driver_label, "live execution enabled");
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
                .list_open_orders(*symbol)
                .await
                .with_context(|| format!("failed to fetch open orders for {}", symbol.code()))?;
            open_orders.append(&mut symbol_orders);
        }
        bootstrap = Some(LiveBootstrap {
            positions,
            balances,
            open_orders,
        });
    }

    let persistence = build_persistence_handles(&settings)?;

    let metrics = Arc::new(LiveMetrics::new());
    let alerting_cfg = settings.alerting.clone();
    let dispatcher = AlertDispatcher::new(alerting_cfg.webhook_url.clone());
    let alerts = Arc::new(AlertManager::new(
        alerting_cfg,
        dispatcher,
        Some(public_connection.clone()),
        private_connection.clone(),
    ));
    let panic_hook: Arc<dyn PanicObserver> =
        Arc::new(PanicAlertHook::new(metrics.clone(), alerts.clone()));

    // Create orchestrator with execution engine
    let initial_open_orders = bootstrap
        .as_ref()
        .map(|data| data.open_orders.clone())
        .unwrap_or_default();
    let orchestrator = OrderOrchestrator::new(
        Arc::new(execution),
        persistence.algo.clone(),
        initial_open_orders,
        settings.panic_close,
        Some(panic_hook.clone()),
    )
    .await?;

    let runtime = LiveRuntime::new(
        market_stream,
        strategy,
        symbols,
        routes,
        router.clone(),
        orchestrator,
        persistence.state,
        settings,
        metrics,
        alerts,
        market_registry,
        shutdown,
        public_connection,
        private_connection,
        bootstrap,
    )
    .await?;
    runtime.run().await
}

async fn build_exchange_routes(
    settings: &LiveSessionSettings,
    exchanges: &[NamedExchange],
    symbols: &[Symbol],
    symbol_codes: &[String],
    connection_flag: Arc<AtomicBool>,
    shutdown: ShutdownSignal,
) -> Result<ExchangeBuildResult> {
    let mut stream_sources: Vec<(String, Box<dyn LiveMarketStream>)> = Vec::new();
    let mut router_inputs: HashMap<ExchangeId, Arc<dyn ExecutionClient>> = HashMap::new();
    let mut routes = Vec::new();

    for exchange in exchanges {
        let payload = build_exchange_payload(&exchange.config, settings, &exchange.name);
        let driver = exchange.config.driver.clone();
        let factory = get_connector_factory(&driver)
            .ok_or_else(|| anyhow!("driver {} is not registered", driver))?;
        let stream_config = ConnectorStreamConfig {
            ws_url: Some(exchange.config.ws_url.clone()),
            metadata: json!({
                "category": settings.category.as_path(),
                "symbols": symbol_codes,
                "orderbook_depth": settings.orderbook_depth,
            }),
            connection_status: Some(connection_flag.clone()),
        };
        let mut connector_stream = factory
            .create_market_stream(&payload, stream_config)
            .await
            .map_err(|err| {
                anyhow!(
                    "failed to create market stream for {}: {err}",
                    exchange.name
                )
            })?;
        connector_stream
            .subscribe(symbol_codes, settings.interval)
            .await
            .map_err(|err| anyhow!("failed to subscribe {}: {err}", exchange.name))?;
        stream_sources.push((
            exchange.name.clone(),
            Box::new(FactoryStreamAdapter::new(connector_stream)),
        ));

        let execution_client =
            build_single_execution_client(settings, &driver, factory, &payload, symbols).await?;
        let exchange_id = ExchangeId::from(exchange.name.as_str());
        router_inputs.insert(exchange_id, execution_client.clone());
        routes.push(ExchangeRoute {
            name: exchange.name.clone(),
            driver,
            #[cfg(feature = "binance")]
            ws_url: exchange.config.ws_url.clone(),
            execution: execution_client.clone(),
        });
    }

    let (execution_client, router_handle): (
        Arc<dyn ExecutionClient>,
        Option<Arc<RouterExecutionClient>>,
    ) = if router_inputs.len() == 1 {
        (router_inputs.into_values().next().unwrap(), None)
    } else {
        let router = Arc::new(RouterExecutionClient::new(router_inputs));
        (router.clone(), Some(router))
    };

    let market_stream: Box<dyn LiveMarketStream> = if stream_sources.len() == 1 {
        stream_sources.into_iter().next().unwrap().1
    } else {
        Box::new(RouterMarketStream::new(stream_sources, shutdown))
    };

    Ok(ExchangeBuildResult {
        execution_client,
        router: router_handle,
        market_stream,
        routes,
    })
}

async fn build_single_execution_client(
    settings: &LiveSessionSettings,
    driver: &str,
    connector_factory: Arc<dyn ConnectorFactory>,
    connector_payload: &Value,
    symbols: &[Symbol],
) -> Result<Arc<dyn ExecutionClient>> {
    match settings.exec_backend {
        ExecutionBackend::Paper => {
            if driver == "paper" {
                return connector_factory
                    .create_execution_client(connector_payload)
                    .await
                    .map_err(|err| anyhow!("failed to create execution client: {err}"));
            }
            Ok(Arc::new(PaperExecutionClient::new(
                format!("paper-{driver}"),
                symbols.to_vec(),
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
    _event_bus: Arc<EventBus>,
    recorder: Option<ParquetRecorder>,
    control_task: Option<JoinHandle<()>>,
    shutdown: ShutdownSignal,
    metrics_task: JoinHandle<()>,
    alert_task: Option<JoinHandle<()>>,
    reconciliation_task: Option<JoinHandle<()>>,
    reconciliation_ctx: Option<Arc<ReconciliationContext>>,
    connection_monitors: Vec<JoinHandle<()>>,
    order_timeout_task: JoinHandle<()>,
    persistence_handle: PersistenceHandle,
    persistence_task: JoinHandle<()>,
    market_task: JoinHandle<Result<()>>,
    strategy_task: JoinHandle<()>,
    oms_task: JoinHandle<()>,
    _strategy_handle: StrategyHandle,
    _oms_handle: OmsHandle,
    _orchestrator: Arc<OrderOrchestrator>,
    #[allow(dead_code)]
    last_private_sync: Arc<tokio::sync::Mutex<Option<DateTime<Utc>>>>,
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
        symbols: Vec<Symbol>,
        exchanges: Vec<ExchangeRoute>,
        router: Option<Arc<RouterExecutionClient>>,
        orchestrator: OrderOrchestrator,
        state_repo: Arc<dyn StateRepository<Snapshot = LiveState>>,
        settings: LiveSessionSettings,
        metrics: Arc<LiveMetrics>,
        alerts: Arc<AlertManager>,
        market_registry: Arc<MarketRegistry>,
        shutdown: ShutdownSignal,
        public_connection: Arc<AtomicBool>,
        private_connection: Option<Arc<AtomicBool>>,
        bootstrap: Option<LiveBootstrap>,
    ) -> Result<Self> {
        let mut strategy_ctx = StrategyContext::new(settings.history);
        strategy_ctx.attach_market_registry(market_registry.clone());
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
            reporting_currency: settings.reporting_currency,
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
        for (asset, expected) in &settings.initial_balances {
            if let Some(cash) = portfolio.balance(*asset) {
                debug!(
                    asset = %asset,
                    expected = %expected,
                    actual = %cash.quantity,
                    "initial portfolio balance"
                );
            }
        }
        strategy_ctx.update_positions(portfolio.positions());
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
            market_snapshots.insert(*symbol, snapshot);
        }

        metrics.update_connection_status("public", public_connection.load(Ordering::SeqCst));
        if let Some(flag) = &private_connection {
            metrics.update_connection_status("private", flag.load(Ordering::SeqCst));
        }
        let metrics_task = spawn_metrics_server(metrics.registry(), settings.metrics_addr);
        let (private_event_tx, private_event_rx) = mpsc::channel(1024);
        let last_private_sync = Arc::new(tokio::sync::Mutex::new(persisted.last_candle_ts));
        let alert_task = alerts.spawn_watchdog();
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
            let router_handle = router.clone();
            for route in &exchanges {
                match route.driver.as_str() {
                    "bybit" | "" => {
                        #[cfg(feature = "bybit")]
                        {
                            let bybit = route
                                .execution
                                .as_ref()
                                .as_any()
                                .downcast_ref::<BybitClient>()
                                .ok_or_else(|| {
                                    anyhow!("execution client for {} is not Bybit", route.name)
                                })?;
                            let creds = bybit.get_credentials().ok_or_else(|| {
                                anyhow!("live execution requires Bybit credentials")
                            })?;
                            spawn_bybit_private_stream(
                                creds,
                                bybit.get_ws_url(),
                                private_event_tx.clone(),
                                route.execution.clone(),
                                symbols.clone(),
                                last_private_sync.clone(),
                                private_connection.clone(),
                                metrics.clone(),
                                router_handle.clone(),
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
                                route.execution.clone(),
                                route.ws_url.clone(),
                                private_event_tx.clone(),
                                private_connection.clone(),
                                metrics.clone(),
                                router_handle.clone(),
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

        let event_bus = Arc::new(EventBus::new(2048));
        let last_data_timestamp = Arc::new(AtomicI64::new(0));
        let (persistence_handle, persistence_task) = spawn_persistence_actor(state_repo.clone());
        let orchestrator = Arc::new(orchestrator);

        let (strategy_market_tx, strategy_market_rx) = mpsc::channel(2048);
        let (oms_market_tx, oms_market_rx) = mpsc::channel(2048);
        let (signal_tx, signal_rx) = mpsc::channel(512);
        let (strategy_cmd_tx, strategy_cmd_rx) = mpsc::channel(128);
        let (oms_req_tx, oms_req_rx) = mpsc::channel(64);

        let strategy_handle = StrategyHandle {
            tx: strategy_cmd_tx.clone(),
        };
        let oms_handle = OmsHandle {
            tx: oms_req_tx.clone(),
        };

        let control_task = control::spawn_control_plane(
            settings.control_addr,
            control::ControlPlaneComponents {
                oms: oms_handle.clone(),
                orchestrator: orchestrator.clone(),
                last_data_timestamp: last_data_timestamp.clone(),
                event_bus: event_bus.clone(),
                strategy: strategy_handle.clone(),
                shutdown: shutdown.clone(),
            },
        );
        let reconciliation_ctx = (!settings.exec_backend.is_paper()).then(|| {
            Arc::new(ReconciliationContext::new(ReconciliationContextConfig {
                client: orchestrator.execution_engine().client(),
                oms: oms_handle.clone(),
                alerts: alerts.clone(),
                metrics: metrics.clone(),
                reporting_currency: settings.reporting_currency,
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

        let driver_summary = Arc::new(if exchanges.is_empty() {
            "unknown".to_string()
        } else {
            exchanges
                .iter()
                .map(|route| route.driver.clone())
                .collect::<Vec<_>>()
                .join(",")
        });

        let order_timeout_task = spawn_order_timeout_monitor(
            orchestrator.clone(),
            private_event_tx.clone(),
            alerts.clone(),
            shutdown.clone(),
        );

        let strategy_actor = StrategyActor::new(
            strategy,
            strategy_ctx,
            strategy_market_rx,
            strategy_cmd_rx,
            signal_tx,
            event_bus.clone(),
            metrics.clone(),
            alerts.clone(),
            recorder_handle.clone(),
            market_registry.clone(),
            driver_summary.clone(),
            shutdown.clone(),
        );
        let strategy_task = tokio::spawn(async move { strategy_actor.run().await });

        let oms_actor = OmsActor::new(
            oms_market_rx,
            signal_rx,
            private_event_rx,
            oms_req_rx,
            orchestrator.clone(),
            portfolio,
            persisted,
            market_snapshots,
            metrics.clone(),
            alerts.clone(),
            recorder_handle.clone(),
            event_bus.clone(),
            persistence_handle.clone(),
            strategy_handle.clone(),
            market_registry.clone(),
            settings.exec_backend,
            shutdown.clone(),
        );
        for symbol in &symbols {
            let ctx = shared_risk_context(
                *symbol,
                &oms_actor.portfolio,
                &oms_actor.market_snapshots,
                &oms_actor.live_state,
                &oms_actor.market_registry,
            );
            oms_actor.orchestrator.update_risk_context(*symbol, ctx);
        }
        let oms_task = tokio::spawn(async move { oms_actor.run().await });

        let market_actor = MarketActor::new(
            market,
            event_bus.clone(),
            recorder_handle,
            metrics.clone(),
            alerts.clone(),
            last_data_timestamp.clone(),
            strategy_market_tx,
            oms_market_tx,
            shutdown.clone(),
        );
        let market_task = tokio::spawn(async move { market_actor.run().await });

        info!(
            symbols = ?symbols,
            category = ?settings.category,
            metrics_addr = %settings.metrics_addr,
            state_path = %settings.persistence.state_path.display(),
            persistence_engine = ?settings.persistence.engine,
            history = settings.history,
            "market stream ready"
        );

        Ok(Self {
            _event_bus: event_bus,
            recorder,
            control_task: Some(control_task),
            shutdown,
            metrics_task,
            alert_task,
            reconciliation_task,
            reconciliation_ctx,
            connection_monitors,
            order_timeout_task,
            persistence_handle,
            persistence_task,
            market_task,
            strategy_task,
            oms_task,
            _strategy_handle: strategy_handle,
            _oms_handle: oms_handle,
            _orchestrator: orchestrator,
            last_private_sync,
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

        let mut market_task = self.market_task;
        let strategy_task = self.strategy_task;
        let oms_task = self.oms_task;

        let market_result = tokio::select! {
            res = &mut market_task => Some(res),
            _ = self.shutdown.wait() => None,
        };
        if let Some(ref res) = market_result {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => warn!(error = %err, "market actor failed"),
                Err(err) => warn!(error = %err, "market actor task panicked"),
            }
        }
        self.shutdown.trigger();

        self.metrics_task.abort();
        if let Some(handle) = self.alert_task.take() {
            handle.abort();
        }
        if let Some(handle) = self.reconciliation_task.take() {
            handle.abort();
        }
        self.order_timeout_task.abort();
        for handle in self.connection_monitors.drain(..) {
            handle.abort();
        }

        self.persistence_handle.shutdown().await;

        if market_result.is_none() {
            if let Err(err) = market_task.await {
                warn!(error = %err, "market actor join failed");
            }
        }
        if let Err(err) = strategy_task.await {
            warn!(error = %err, "strategy actor join failed");
        }
        if let Err(err) = oms_task.await {
            warn!(error = %err, "oms actor join failed");
        }
        if let Err(err) = self.persistence_task.await {
            warn!(error = %err, "persistence actor join failed");
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
    oms: OmsHandle,
    alerts: Arc<AlertManager>,
    metrics: Arc<LiveMetrics>,
    reporting_currency: AssetId,
    threshold: Decimal,
}

struct ReconciliationContextConfig {
    client: Arc<dyn ExecutionClient>,
    oms: OmsHandle,
    alerts: Arc<AlertManager>,
    metrics: Arc<LiveMetrics>,
    reporting_currency: AssetId,
    threshold: Decimal,
}

impl ReconciliationContext {
    fn new(config: ReconciliationContextConfig) -> Self {
        let ReconciliationContextConfig {
            client,
            oms,
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
            oms,
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
    let local_summary = ctx.oms.portfolio_summary().await;
    let local_positions = local_summary.positions.clone();
    let local_cash = local_summary.cash;

    let remote_map = positions_to_map(remote_positions);
    let local_map = positions_to_map(local_positions);
    let mut tracked_symbols: HashSet<Symbol> = HashSet::new();
    tracked_symbols.extend(remote_map.keys().cloned());
    tracked_symbols.extend(local_map.keys().cloned());

    let mut severe_findings = Vec::new();
    for symbol in tracked_symbols {
        let local_qty = local_map.get(&symbol).copied().unwrap_or(Decimal::ZERO);
        let remote_qty = remote_map.get(&symbol).copied().unwrap_or(Decimal::ZERO);
        let diff = (local_qty - remote_qty).abs();
        let diff_value = diff.to_f64().unwrap_or(0.0);
        let symbol_name = symbol.code().to_string();
        ctx.metrics.update_position_diff(&symbol_name, diff_value);
        if diff > Decimal::ZERO {
            warn!(
                symbol = %symbol_name,
                local = %local_qty,
                remote = %remote_qty,
                diff = %diff,
                "position mismatch detected during reconciliation"
            );
            let pct = normalize_diff(diff, remote_qty);
            if pct >= ctx.threshold {
                error!(
                    symbol = %symbol_name,
                    local = %local_qty,
                    remote = %remote_qty,
                    diff = %diff,
                    pct = %pct,
                    "position mismatch exceeds threshold"
                );
                severe_findings.push(format!(
                    "{symbol_name} local={local_qty} remote={remote_qty} diff={diff}"
                ));
            }
        }
    }

    let reporting = ctx.reporting_currency;
    let reporting_label = reporting.to_string();
    let remote_cash = remote_balances
        .iter()
        .find(|balance| balance.asset == reporting)
        .map(|balance| balance.available)
        .unwrap_or_else(|| Decimal::ZERO);
    let cash_diff = (remote_cash - local_cash).abs();
    ctx.metrics
        .update_balance_diff(&reporting_label, cash_diff.to_f64().unwrap_or(0.0));
    if cash_diff > Decimal::ZERO {
        warn!(
            currency = %reporting_label,
            local = %local_cash,
            remote = %remote_cash,
            diff = %cash_diff,
            "balance mismatch detected during reconciliation"
        );
        let pct = normalize_diff(cash_diff, remote_cash);
        if pct >= ctx.threshold {
            error!(
                currency = %reporting_label,
                local = %local_cash,
                remote = %remote_cash,
                diff = %cash_diff,
                pct = %pct,
                "balance mismatch exceeds threshold"
            );
            severe_findings.push(format!(
                "{reporting_label} balance local={local_cash} remote={remote_cash} diff={cash_diff}"
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
    ctx.oms.enter_liquidate_only().await;
    Ok(())
}

fn positions_to_map(positions: Vec<Position>) -> HashMap<Symbol, Decimal> {
    let mut map = HashMap::new();
    for position in positions {
        map.insert(position.symbol, position_signed_qty(&position));
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

fn build_exchange_payload(
    exchange: &ExchangeConfig,
    settings: &LiveSessionSettings,
    name: &str,
) -> Value {
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
    payload.insert("exchange".into(), Value::String(name.to_string()));
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

fn log_strategy_call(event: &str, elapsed: Duration) {
    let duration_ms = elapsed.as_secs_f64() * 1000.0;
    if elapsed >= STRATEGY_CALL_WARN_THRESHOLD {
        warn!(target: "strategy", event, duration_ms, "strategy call latency above threshold");
    } else {
        trace!(target: "strategy", event, duration_ms, "strategy call completed");
    }
}

fn normalize_group_quantities(signals: &mut [Signal], registry: &MarketRegistry) {
    use std::collections::HashMap;

    assign_implicit_group_ids(signals);

    let mut groups: HashMap<Uuid, Vec<usize>> = HashMap::new();
    for (idx, signal) in signals.iter().enumerate() {
        if let Some(group_id) = signal.group_id {
            groups.entry(group_id).or_default().push(idx);
        }
    }
    for indices in groups.values() {
        if indices.len() < 2 {
            continue;
        }
        let mut quantity = indices
            .iter()
            .filter_map(|idx| signals[*idx].quantity)
            .find(|qty| *qty > Decimal::ZERO);
        let mut step = Decimal::ZERO;
        for idx in indices {
            let symbol = signals[*idx].symbol;
            let Some(instr) = registry.get(symbol) else {
                quantity = None;
                break;
            };
            if instr.lot_size > step {
                step = instr.lot_size;
            }
        }
        let Some(mut qty) = quantity else {
            continue;
        };
        if step > Decimal::ZERO {
            qty = (qty / step).floor() * step;
        }
        if qty <= Decimal::ZERO {
            continue;
        }
        for idx in indices {
            signals[*idx].quantity = Some(qty);
        }
    }
}

fn assign_implicit_group_ids(signals: &mut [Signal]) {
    use std::collections::HashMap;

    let mut note_groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, signal) in signals.iter().enumerate() {
        if signal.group_id.is_some() {
            continue;
        }
        if let Some(note) = signal.note.as_deref() {
            if !note.is_empty() {
                note_groups.entry(note.to_string()).or_default().push(idx);
            }
        }
    }
    for indices in note_groups.values() {
        if indices.len() < 2 {
            continue;
        }
        let group = Uuid::new_v4();
        for idx in indices {
            signals[*idx].group_id = Some(group);
        }
    }

    let mut untagged: Vec<usize> = signals
        .iter()
        .enumerate()
        .filter(|(_, signal)| signal.group_id.is_none())
        .map(|(idx, _)| idx)
        .collect();
    if untagged.len() == 2 {
        let a = signals[untagged[0]].kind;
        let b = signals[untagged[1]].kind;
        if signal_kind_family(a) == signal_kind_family(b) && signal_kind_family(a).is_some() {
            let group = Uuid::new_v4();
            for idx in untagged.drain(..) {
                signals[idx].group_id = Some(group);
            }
        }
    }
}

fn signal_kind_family(kind: SignalKind) -> Option<u8> {
    match kind {
        SignalKind::EnterLong | SignalKind::EnterShort => Some(0),
        SignalKind::ExitLong | SignalKind::ExitShort | SignalKind::Flatten => Some(1),
    }
}

struct PersistenceActor {
    repo: Arc<dyn StateRepository<Snapshot = LiveState>>,
    rx: mpsc::Receiver<PersistenceCommand>,
}

impl PersistenceActor {
    async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                PersistenceCommand::Save(snapshot) => {
                    let repo = self.repo.clone();
                    if let Err(err) = tokio::task::spawn_blocking(move || repo.save(&snapshot))
                        .await
                        .map_err(|err| anyhow!("state persistence task failed: {err}"))
                        .and_then(|result| result.map_err(|e| anyhow!(e.to_string())))
                    {
                        warn!(error = %err, "failed to persist live state snapshot");
                    }
                }
                PersistenceCommand::Shutdown => break,
            }
        }
    }
}

fn spawn_persistence_actor(
    repo: Arc<dyn StateRepository<Snapshot = LiveState>>,
) -> (PersistenceHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(64);
    let actor = PersistenceActor { repo, rx };
    let handle = tokio::spawn(async move { actor.run().await });
    (PersistenceHandle { tx }, handle)
}

struct StrategyActor {
    strategy: Box<dyn Strategy>,
    ctx: StrategyContext,
    market_rx: mpsc::Receiver<MarketEvent>,
    cmd_rx: mpsc::Receiver<StrategyCommand>,
    signal_tx: mpsc::Sender<Signal>,
    bus: Arc<EventBus>,
    metrics: Arc<LiveMetrics>,
    alerts: Arc<AlertManager>,
    recorder: Option<RecorderHandle>,
    market_registry: Arc<MarketRegistry>,
    driver_label: Arc<String>,
    shutdown: ShutdownSignal,
}

struct MarketActor {
    stream: Box<dyn LiveMarketStream>,
    event_bus: Arc<EventBus>,
    recorder: Option<RecorderHandle>,
    metrics: Arc<LiveMetrics>,
    alerts: Arc<AlertManager>,
    last_data_timestamp: Arc<AtomicI64>,
    strategy_tx: mpsc::Sender<MarketEvent>,
    oms_tx: mpsc::Sender<MarketEvent>,
    shutdown: ShutdownSignal,
}

impl MarketActor {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream: Box<dyn LiveMarketStream>,
        event_bus: Arc<EventBus>,
        recorder: Option<RecorderHandle>,
        metrics: Arc<LiveMetrics>,
        alerts: Arc<AlertManager>,
        last_data_timestamp: Arc<AtomicI64>,
        strategy_tx: mpsc::Sender<MarketEvent>,
        oms_tx: mpsc::Sender<MarketEvent>,
        shutdown: ShutdownSignal,
    ) -> Self {
        Self {
            stream,
            event_bus,
            recorder,
            metrics,
            alerts,
            last_data_timestamp,
            strategy_tx,
            oms_tx,
            shutdown,
        }
    }

    async fn run(mut self) -> Result<()> {
        let backoff = Duration::from_millis(200);
        'outer: while !self.shutdown.triggered() {
            let mut progressed = false;
            if let Some(tick) = self.next_tick().await? {
                progressed = true;
                self.dispatch_market_event(MarketEvent::Tick(tick)).await;
            }
            if let Some(candle) = self.next_candle().await? {
                progressed = true;
                self.dispatch_market_event(MarketEvent::Candle(candle))
                    .await;
            }
            if let Some(book) = self.next_order_book().await? {
                progressed = true;
                self.dispatch_market_event(MarketEvent::OrderBook(book))
                    .await;
            }
            if !progressed && !self.shutdown.sleep(backoff).await {
                break 'outer;
            }
        }
        Ok(())
    }

    async fn next_tick(&mut self) -> Result<Option<Tick>> {
        let tick = tokio::select! {
            res = tokio::time::timeout(MARKET_EVENT_TIMEOUT, self.stream.next_tick()) => Some(res),
            _ = self.shutdown.wait() => None,
        };
        match tick {
            Some(Ok(Ok(Some(tick)))) => {
                self.metrics.inc_tick();
                self.metrics.update_staleness(0.0);
                self.metrics
                    .update_last_data_timestamp(Utc::now().timestamp() as f64);
                self.last_data_timestamp
                    .store(tick.exchange_timestamp.timestamp(), Ordering::SeqCst);
                self.alerts.heartbeat().await;
                if let Some(handle) = &self.recorder {
                    handle.record_tick(tick.clone());
                }
                self.event_bus
                    .publish(Event::Tick(TickEvent { tick: tick.clone() }));
                Ok(Some(tick))
            }
            Some(Ok(Ok(None))) => Ok(None),
            Some(Ok(Err(err))) => Err(err.into()),
            Some(Err(_)) => Ok(None),
            None => Ok(None),
        }
    }

    async fn next_candle(&mut self) -> Result<Option<Candle>> {
        let candle = tokio::select! {
            res = tokio::time::timeout(MARKET_EVENT_TIMEOUT, self.stream.next_candle()) => Some(res),
            _ = self.shutdown.wait() => None,
        };
        match candle {
            Some(Ok(Ok(Some(candle)))) => {
                self.metrics
                    .update_last_data_timestamp(Utc::now().timestamp() as f64);
                self.last_data_timestamp
                    .store(candle.timestamp.timestamp(), Ordering::SeqCst);
                self.alerts.heartbeat().await;
                if let Some(handle) = &self.recorder {
                    handle.record_candle(candle.clone());
                }
                self.event_bus.publish(Event::Candle(CandleEvent {
                    candle: candle.clone(),
                }));
                Ok(Some(candle))
            }
            Some(Ok(Ok(None))) => Ok(None),
            Some(Ok(Err(err))) => Err(err.into()),
            Some(Err(_)) => Ok(None),
            None => Ok(None),
        }
    }

    async fn next_order_book(&mut self) -> Result<Option<OrderBook>> {
        let book = tokio::select! {
            res = tokio::time::timeout(MARKET_EVENT_TIMEOUT, self.stream.next_order_book()) => Some(res),
            _ = self.shutdown.wait() => None,
        };
        match book {
            Some(Ok(Ok(Some(book)))) => {
                self.metrics
                    .update_last_data_timestamp(Utc::now().timestamp() as f64);
                self.last_data_timestamp
                    .store(book.timestamp.timestamp(), Ordering::SeqCst);
                self.alerts.heartbeat().await;
                if let Some(handle) = &self.recorder {
                    handle.record_order_book(book.clone());
                }
                self.event_bus.publish(Event::OrderBook(OrderBookEvent {
                    order_book: book.clone(),
                }));
                Ok(Some(book))
            }
            Some(Ok(Ok(None))) => Ok(None),
            Some(Ok(Err(err))) => Err(err.into()),
            Some(Err(_)) => Ok(None),
            None => Ok(None),
        }
    }

    fn dispatch_market_event(&self, event: MarketEvent) -> impl std::future::Future<Output = ()> {
        let strategy_tx = self.strategy_tx.clone();
        let oms_tx = self.oms_tx.clone();
        async move {
            let _ = strategy_tx.send(event.clone()).await;
            let _ = oms_tx.send(event).await;
        }
    }
}

struct OmsActor {
    market_rx: mpsc::Receiver<MarketEvent>,
    signal_rx: mpsc::Receiver<Signal>,
    broker_rx: mpsc::Receiver<BrokerEvent>,
    request_rx: mpsc::Receiver<OmsRequest>,
    orchestrator: Arc<OrderOrchestrator>,
    portfolio: Portfolio,
    live_state: LiveState,
    market_snapshots: HashMap<Symbol, MarketSnapshot>,
    metrics: Arc<LiveMetrics>,
    alerts: Arc<AlertManager>,
    recorder: Option<RecorderHandle>,
    bus: Arc<EventBus>,
    persistence: PersistenceHandle,
    strategy: StrategyHandle,
    market_registry: Arc<MarketRegistry>,
    exec_backend: ExecutionBackend,
    shutdown: ShutdownSignal,
}

impl OmsActor {
    #[allow(clippy::too_many_arguments)]
    fn new(
        market_rx: mpsc::Receiver<MarketEvent>,
        signal_rx: mpsc::Receiver<Signal>,
        broker_rx: mpsc::Receiver<BrokerEvent>,
        request_rx: mpsc::Receiver<OmsRequest>,
        orchestrator: Arc<OrderOrchestrator>,
        portfolio: Portfolio,
        live_state: LiveState,
        market_snapshots: HashMap<Symbol, MarketSnapshot>,
        metrics: Arc<LiveMetrics>,
        alerts: Arc<AlertManager>,
        recorder: Option<RecorderHandle>,
        bus: Arc<EventBus>,
        persistence: PersistenceHandle,
        strategy: StrategyHandle,
        market_registry: Arc<MarketRegistry>,
        exec_backend: ExecutionBackend,
        shutdown: ShutdownSignal,
    ) -> Self {
        Self {
            market_rx,
            signal_rx,
            broker_rx,
            request_rx,
            orchestrator,
            portfolio,
            live_state,
            market_snapshots,
            metrics,
            alerts,
            recorder,
            bus,
            persistence,
            strategy,
            market_registry,
            exec_backend,
            shutdown,
        }
    }

    async fn run(mut self) {
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                Some(event) = self.market_rx.recv() => {
                    if let Err(err) = self.handle_market_event(event).await {
                        warn!(error = %err, "oms market handler failed");
                    }
                }
                Some(signal) = self.signal_rx.recv() => {
                    if let Err(err) = self.handle_signal(signal).await {
                        warn!(error = %err, "oms signal handler failed");
                    }
                }
                Some(evt) = self.broker_rx.recv() => {
                    if let Err(err) = self.handle_broker_event(evt).await {
                        warn!(error = %err, "oms broker handler failed");
                    }
                }
                Some(req) = self.request_rx.recv() => self.handle_request(req).await,
                _ = timer.tick() => {
                    if let Err(err) = self.orchestrator.on_timer_tick().await {
                        error!(error = %err, "orchestrator timer tick failed");
                    }
                }
                _ = self.shutdown.wait() => break,
                else => break,
            }
        }
        debug!("oms actor stopped");
    }

    async fn handle_market_event(&mut self, event: MarketEvent) -> Result<()> {
        match event {
            MarketEvent::Tick(tick) => self.handle_tick(tick).await?,
            MarketEvent::Candle(candle) => self.handle_candle(candle).await?,
            MarketEvent::OrderBook(_) => {}
        }
        Ok(())
    }

    async fn handle_tick(&mut self, tick: Tick) -> Result<()> {
        if let Some(snapshot) = self.market_snapshots.get_mut(&tick.symbol) {
            snapshot.last_trade = Some(tick.price);
            snapshot.last_trade_ts = Some(tick.exchange_timestamp);
        }
        let was_liquidate_only = self.portfolio.liquidate_only();
        if let Err(err) = self.portfolio.update_market_data(tick.symbol, tick.price) {
            warn!(symbol = %tick.symbol, error = %err, "failed to refresh market data");
        }
        self.live_state.last_prices.insert(tick.symbol, tick.price);
        if !was_liquidate_only && self.portfolio.liquidate_only() {
            self.snapshot_portfolio();
            alert_liquidate_only(self.alerts.clone()).await;
            self.persist_state(true).await;
        }
        Ok(())
    }

    async fn handle_candle(&mut self, candle: Candle) -> Result<()> {
        if let Some(snapshot) = self.market_snapshots.get_mut(&candle.symbol) {
            snapshot.last_candle = Some(candle.clone());
            snapshot.last_trade = Some(candle.close);
        }
        if self.exec_backend.is_paper() {
            let client = self.orchestrator.execution_engine().client();
            if let Some(paper) = client.as_any().downcast_ref::<PaperExecutionClient>() {
                paper.update_price(&candle.symbol, candle.close);
            }
        }
        let was_liquidate_only = self.portfolio.liquidate_only();
        if let Err(err) = self
            .portfolio
            .update_market_data(candle.symbol, candle.close)
        {
            warn!(symbol = %candle.symbol, error = %err, "failed to refresh market data");
        }
        if !was_liquidate_only && self.portfolio.liquidate_only() {
            alert_liquidate_only(self.alerts.clone()).await;
        }
        self.live_state.last_candle_ts = Some(candle.timestamp);
        self.live_state
            .last_prices
            .insert(candle.symbol, candle.close);
        self.snapshot_portfolio();
        let ctx = shared_risk_context(
            candle.symbol,
            &self.portfolio,
            &self.market_snapshots,
            &self.live_state,
            &self.market_registry,
        );
        self.orchestrator.update_risk_context(candle.symbol, ctx);
        self.persist_state(true).await;
        Ok(())
    }

    async fn handle_signal(&mut self, signal: Signal) -> Result<()> {
        let ctx = shared_risk_context(
            signal.symbol,
            &self.portfolio,
            &self.market_snapshots,
            &self.live_state,
            &self.market_registry,
        );
        self.orchestrator.update_risk_context(signal.symbol, ctx);
        match self.orchestrator.on_signal(&signal, &ctx).await {
            Ok(_) => {
                self.alerts.reset_order_failures().await;
            }
            Err(err) => {
                warn!(
                    symbol = %signal.symbol,
                    error = %err,
                    quote_available = %ctx.quote_available,
                    base_available = %ctx.base_available,
                    settlement_available = %ctx.settlement_available,
                    "orchestrator failed on signal"
                );
                self.metrics.inc_order_failure();
                self.metrics.inc_router_failure("orchestrator");
                self.alerts
                    .order_failure(&format!("orchestrator error: {err}"))
                    .await;
            }
        }
        Ok(())
    }

    async fn handle_fill(&mut self, fill: Fill) -> Result<()> {
        let was_liquidate_only = self.portfolio.liquidate_only();
        self.portfolio
            .apply_fill(&fill)
            .with_context(|| format!("Failed to apply fill to portfolio for {}", fill.symbol))?;
        self.snapshot_portfolio();
        if let Some(instr) = self.market_registry.get(fill.symbol) {
            let base_balance = self
                .portfolio
                .balance(instr.base)
                .map(|cash| cash.quantity)
                .unwrap_or_default();
            let quote_balance = self
                .portfolio
                .balance(instr.quote)
                .map(|cash| cash.quantity)
                .unwrap_or_default();
            let settlement_balance = self
                .portfolio
                .balance(instr.settlement_currency)
                .map(|cash| cash.quantity)
                .unwrap_or_default();
            debug!(
                symbol = %fill.symbol,
                base_balance = %base_balance,
                quote_balance = %quote_balance,
                settlement_balance = %settlement_balance,
                "portfolio balances after fill"
            );
        }
        self.orchestrator.on_fill(&fill).await.ok();
        let positions = self.portfolio.positions();
        self.strategy
            .send_account_event(StrategyAccountEvent::Fill {
                fill: fill.clone(),
                positions,
            })
            .await;
        let equity = self.portfolio.equity();
        if let Some(value) = equity.to_f64() {
            self.metrics.update_equity(value);
        }
        self.alerts.update_equity(equity).await;
        self.metrics.inc_order();
        if let Some(handle) = &self.recorder {
            handle.record_fill(fill.clone());
        }
        self.bus
            .publish(Event::Fill(FillEvent { fill: fill.clone() }));
        self.alerts
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
        if !was_liquidate_only && self.portfolio.liquidate_only() {
            alert_liquidate_only(self.alerts.clone()).await;
        }
        self.persist_state(true).await;
        Ok(())
    }

    async fn handle_order_update(&mut self, order: Order) -> Result<()> {
        self.orchestrator.on_order_update(&order).await;
        if let Some(handle) = &self.recorder {
            handle.record_order(order.clone());
        }
        if matches!(order.status, OrderStatus::Rejected) {
            error!(
                order_id = %order.id,
                symbol = %order.request.symbol,
                "order rejected by exchange"
            );
            self.alerts
                .order_failure("order rejected by exchange")
                .await;
            self.alerts
                .notify(
                    "Order rejected",
                    &format!(
                        "Order {} for {} was rejected",
                        order.id, order.request.symbol
                    ),
                )
                .await;
        }
        let mut found = false;
        for existing in &mut self.live_state.open_orders {
            if existing.id == order.id {
                *existing = order.clone();
                found = true;
                break;
            }
        }
        if !found {
            self.live_state.open_orders.push(order.clone());
        }
        if matches!(
            order.status,
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
        ) {
            self.live_state.open_orders.retain(|o| o.id != order.id);
        }
        self.bus
            .publish(Event::OrderUpdate(OrderUpdateEvent { order }));
        self.persist_state(false).await;
        Ok(())
    }

    async fn handle_broker_event(&mut self, event: BrokerEvent) -> Result<()> {
        match event {
            BrokerEvent::OrderUpdate(order) => self.handle_order_update(order).await?,
            BrokerEvent::Fill(fill) => self.handle_fill(fill).await?,
        }
        Ok(())
    }

    async fn persist_state(&self, include_strategy: bool) {
        let mut snapshot = self.live_state.clone();
        if include_strategy {
            if let Some(state) = self.strategy.snapshot().await {
                snapshot.strategy_state = Some(state);
            }
        }
        self.persistence.save(snapshot).await;
    }

    fn snapshot_portfolio(&mut self) {
        self.live_state.portfolio = Some(self.portfolio.snapshot());
    }

    async fn handle_request(&mut self, request: OmsRequest) {
        match request {
            OmsRequest::PortfolioState { respond_to } => {
                let snapshot = self.live_state.portfolio.clone();
                let _ = respond_to.send(snapshot);
            }
            OmsRequest::OpenOrders { respond_to } => {
                let _ = respond_to.send(self.live_state.open_orders.clone());
            }
            OmsRequest::Status { respond_to } => {
                let status = OmsStatus {
                    equity: self.portfolio.equity(),
                    liquidate_only: self.portfolio.liquidate_only(),
                };
                let _ = respond_to.send(status);
            }
            OmsRequest::PortfolioSummary { respond_to } => {
                let summary = PortfolioSummary {
                    positions: self.portfolio.positions(),
                    cash: self.portfolio.cash(),
                };
                let _ = respond_to.send(summary);
            }
            OmsRequest::EnterLiquidateOnly { respond_to } => {
                let changed = self.portfolio.set_liquidate_only(true);
                if changed {
                    self.snapshot_portfolio();
                    self.persist_state(true).await;
                }
                let _ = respond_to.send(changed);
            }
        }
    }
}

impl StrategyActor {
    #[allow(clippy::too_many_arguments)]
    fn new(
        strategy: Box<dyn Strategy>,
        ctx: StrategyContext,
        market_rx: mpsc::Receiver<MarketEvent>,
        cmd_rx: mpsc::Receiver<StrategyCommand>,
        signal_tx: mpsc::Sender<Signal>,
        bus: Arc<EventBus>,
        metrics: Arc<LiveMetrics>,
        alerts: Arc<AlertManager>,
        recorder: Option<RecorderHandle>,
        market_registry: Arc<MarketRegistry>,
        driver_label: Arc<String>,
        shutdown: ShutdownSignal,
    ) -> Self {
        Self {
            strategy,
            ctx,
            market_rx,
            cmd_rx,
            signal_tx,
            bus,
            metrics,
            alerts,
            recorder,
            market_registry,
            driver_label,
            shutdown,
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                Some(event) = self.market_rx.recv() => {
                    if let Err(err) = self.handle_market_event(event).await {
                        warn!(error = %err, "strategy market handler failed");
                    }
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    if let Err(err) = self.handle_command(cmd).await {
                        warn!(error = %err, "strategy command failed");
                    }
                }
                _ = self.shutdown.wait() => break,
                else => break,
            }
        }
        debug!("strategy actor stopped");
    }

    async fn handle_market_event(&mut self, event: MarketEvent) -> Result<()> {
        match event {
            MarketEvent::Tick(tick) => self.handle_tick(tick).await?,
            MarketEvent::Candle(candle) => self.handle_candle(candle).await?,
            MarketEvent::OrderBook(book) => self.handle_order_book(book).await?,
        }
        Ok(())
    }

    async fn handle_tick(&mut self, tick: Tick) -> Result<()> {
        self.ctx.push_tick(tick.clone());
        let call_start = Instant::now();
        self.strategy
            .on_tick(&self.ctx, &tick)
            .await
            .context("strategy failure on tick event")?;
        log_strategy_call("tick", call_start.elapsed());
        self.emit_signals().await;
        Ok(())
    }

    async fn handle_candle(&mut self, candle: Candle) -> Result<()> {
        self.metrics
            .update_price(candle.symbol.code(), candle.close.to_f64().unwrap_or(0.0));
        self.ctx.push_candle(candle.clone());
        let call_start = Instant::now();
        self.strategy
            .on_candle(&self.ctx, &candle)
            .await
            .context("strategy failure on candle event")?;
        log_strategy_call("candle", call_start.elapsed());
        self.emit_signals().await;
        Ok(())
    }

    async fn handle_order_book(&mut self, mut book: OrderBook) -> Result<()> {
        let driver_name = self.driver_label.as_str();
        let local_checksum = if let Some(cs) = book.local_checksum {
            cs
        } else {
            let computed = book.computed_checksum(None);
            book.local_checksum = Some(computed);
            computed
        };
        if let Some(expected) = book.exchange_checksum {
            if expected != local_checksum {
                let symbol_label = book.symbol.code().to_string();
                self.metrics
                    .inc_checksum_mismatch(driver_name, &symbol_label);
                self.alerts
                    .order_book_checksum_mismatch(
                        driver_name,
                        &symbol_label,
                        expected,
                        local_checksum,
                    )
                    .await;
            }
        }
        self.ctx.push_order_book(book.clone());
        let call_start = Instant::now();
        self.strategy
            .on_order_book(&self.ctx, &book)
            .await
            .context("strategy failure on order book")?;
        log_strategy_call("order_book", call_start.elapsed());
        self.emit_signals().await;
        Ok(())
    }

    async fn handle_command(&mut self, cmd: StrategyCommand) -> Result<()> {
        match cmd {
            StrategyCommand::Account(StrategyAccountEvent::Fill { fill, positions }) => {
                self.ctx.update_positions(positions);
                let call_start = Instant::now();
                self.strategy
                    .on_fill(&self.ctx, &fill)
                    .await
                    .context("strategy failure on fill event")?;
                log_strategy_call("fill", call_start.elapsed());
            }
            StrategyCommand::Request(request) => match request {
                StrategyRequest::Snapshot { respond_to } => {
                    let snapshot = self.strategy.snapshot().ok();
                    let _ = respond_to.send(snapshot);
                }
                StrategyRequest::ListManagedTrades { respond_to } => {
                    let result = self
                        .with_pairs_strategy(|pairs| Ok(pairs.managed_trades()))
                        .map_err(|err| anyhow!(err.to_string()));
                    let _ = respond_to.send(result);
                }
                StrategyRequest::UpdateExitStrategy {
                    trade_id,
                    exit,
                    respond_to,
                } => {
                    let result = self
                        .with_pairs_strategy(|pairs| {
                            pairs.update_trade_exit_strategy(trade_id, exit.clone())
                        })
                        .map_err(|err| anyhow!(err.to_string()));
                    let _ = respond_to.send(result);
                }
            },
        }
        Ok(())
    }

    fn with_pairs_strategy<R>(
        &mut self,
        f: impl FnOnce(&mut PairsTradingArbitrage) -> StrategyResult<R>,
    ) -> StrategyResult<R> {
        let any = (&mut *self.strategy) as &mut dyn Any;
        let Some(pairs) = any.downcast_mut::<PairsTradingArbitrage>() else {
            return Err(StrategyError::InvalidConfig(
                "active strategy does not expose managed trades".to_string(),
            ));
        };
        f(pairs)
    }

    async fn emit_signals(&mut self) {
        let mut signals = self.strategy.drain_signals();
        if signals.is_empty() {
            return;
        }
        self.metrics.inc_signals(signals.len());
        normalize_group_quantities(&mut signals, &self.market_registry);
        for signal in signals {
            let event_signal = signal.clone();
            debug!(id = %event_signal.id, symbol = %event_signal.symbol, kind = ?event_signal.kind, "publishing signal event");
            if let Some(handle) = &self.recorder {
                handle.record_signal(event_signal.clone());
            }
            self.bus.publish(Event::Signal(SignalEvent {
                signal: event_signal.clone(),
            }));
            if self.signal_tx.send(event_signal).await.is_err() {
                warn!("signal channel dropped; shutting down strategy actor");
                self.shutdown.trigger();
                break;
            }
        }
    }
}

fn shared_risk_context(
    symbol: Symbol,
    portfolio: &Portfolio,
    market: &HashMap<Symbol, MarketSnapshot>,
    persisted: &LiveState,
    registry: &Arc<MarketRegistry>,
) -> RiskContext {
    let instrument = registry.get(symbol);
    let (instrument_kind, base_asset, quote_asset, settlement_asset) = instrument
        .map(|instrument| {
            (
                Some(instrument.kind),
                instrument.base,
                instrument.quote,
                instrument.settlement_currency,
            )
        })
        .unwrap_or((
            None,
            AssetId::unspecified(),
            AssetId::unspecified(),
            AssetId::unspecified(),
        ));
    let (
        signed_qty,
        equity,
        venue_equity,
        liquidate_only,
        base_available,
        quote_available,
        settlement_available,
    ) = (
        portfolio.signed_position_qty(symbol),
        portfolio.equity(),
        portfolio.exchange_equity(symbol.exchange),
        portfolio.liquidate_only(),
        portfolio
            .balance(base_asset)
            .map(|cash| cash.quantity)
            .unwrap_or_default(),
        portfolio
            .balance(quote_asset)
            .map(|cash| cash.quantity)
            .unwrap_or_default(),
        portfolio
            .balance(settlement_asset)
            .map(|cash| cash.quantity)
            .unwrap_or_default(),
    );
    let observed_price = market.get(&symbol).and_then(|snapshot| snapshot.price());
    let last_price = observed_price.unwrap_or_else(|| {
        persisted
            .last_prices
            .get(&symbol)
            .copied()
            .unwrap_or(Decimal::ZERO)
    });
    RiskContext {
        symbol,
        exchange: symbol.exchange,
        signed_position_qty: signed_qty,
        portfolio_equity: equity,
        exchange_equity: venue_equity,
        last_price,
        liquidate_only,
        instrument_kind,
        base_asset,
        quote_asset,
        settlement_asset,
        base_available,
        quote_available,
        settlement_available,
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
    broker_tx: mpsc::Sender<BrokerEvent>,
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
                        if broker_tx
                            .send(BrokerEvent::OrderUpdate(order))
                            .await
                            .is_err()
                        {
                            break;
                        }
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
    let mut loaded_local = false;
    if let Some(path) = &settings.markets_file {
        catalog
            .add_file(path)
            .with_context(|| format!("failed to load markets from {}", path.display()))?;
        loaded_local = true;
    }

    if !loaded_local && !settings.exec_backend.is_paper() {
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
    symbols: Vec<Symbol>,
    last_sync: Arc<tokio::sync::Mutex<Option<DateTime<Utc>>>>,
    private_connection_flag: Option<Arc<AtomicBool>>,
    metrics: Arc<LiveMetrics>,
    router: Option<Arc<RouterExecutionClient>>,
    shutdown: ShutdownSignal,
) {
    let exchange_id = exec_client
        .as_any()
        .downcast_ref::<BybitClient>()
        .map(|client| client.exchange())
        .unwrap_or(ExchangeId::UNSPECIFIED);
    let venue_symbols: Vec<Symbol> = symbols
        .iter()
        .copied()
        .filter(|symbol| symbol.exchange == exchange_id)
        .collect();
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
                    for symbol in &venue_symbols {
                        match exec_client.list_open_orders(*symbol).await {
                            Ok(orders) => {
                                for mut order in orders {
                                    if let Some(router) = &router {
                                        order = router.normalize_order_event(exchange_id, order);
                                    }
                                    if let Err(err) =
                                        private_tx.send(BrokerEvent::OrderUpdate(order)).await
                                    {
                                        error!("failed to send reconciled order update: {err}");
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "failed to reconcile open orders for {}: {e}",
                                    symbol.code()
                                );
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
                                for mut fill in fills {
                                    if let Some(router) = &router {
                                        match router.normalize_fill_event(exchange_id, fill) {
                                            Some(normalized) => fill = normalized,
                                            None => {
                                                metrics.inc_router_failure("orphan_fill");
                                                continue;
                                            }
                                        }
                                    }
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
                                                    if let Ok(mut order) =
                                                        update.to_tesser_order(exchange_id, None)
                                                    {
                                                        if let Some(router) = &router {
                                                            order = router.normalize_order_event(
                                                                exchange_id,
                                                                order,
                                                            );
                                                        }
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
                                                    if let Ok(mut fill) =
                                                        exec.to_tesser_fill(exchange_id)
                                                    {
                                                        if let Some(router) = &router {
                                                            match router.normalize_fill_event(
                                                                exchange_id,
                                                                fill,
                                                            ) {
                                                                Some(normalized) => {
                                                                    fill = normalized
                                                                }
                                                                None => {
                                                                    metrics.inc_router_failure(
                                                                        "orphan_fill",
                                                                    );
                                                                    continue;
                                                                }
                                                            }
                                                        }
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
    router: Option<Arc<RouterExecutionClient>>,
    shutdown: ShutdownSignal,
) {
    let router_handle = router.clone();
    tokio::spawn(async move {
        let router = router_handle;
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
                    let router_for_event = router.clone();
                    let metrics_for_event = metrics.clone();
                    user_stream.on_event(move |event| {
                        if let Some(update) = extract_order_update(&event) {
                            if let Some(mut order) = order_from_update(exchange_id, update) {
                                if let Some(router) = &router_for_event {
                                    order = router.normalize_order_event(exchange_id, order);
                                }
                                let _ = tx_orders.blocking_send(BrokerEvent::OrderUpdate(order));
                            }
                            if let Some(mut fill) = fill_from_update(exchange_id, update) {
                                if let Some(router) = &router_for_event {
                                    match router.normalize_fill_event(exchange_id, fill) {
                                        Some(normalized) => fill = normalized,
                                        None => {
                                            metrics_for_event.inc_router_failure("orphan_fill");
                                            return;
                                        }
                                    }
                                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use tesser_core::OrderBookLevel;

    struct StaticStream {
        ticks: VecDeque<Tick>,
        candles: VecDeque<Candle>,
        books: VecDeque<OrderBook>,
    }

    impl StaticStream {
        fn new(ticks: Vec<Tick>, candles: Vec<Candle>, books: Vec<OrderBook>) -> Self {
            Self {
                ticks: ticks.into(),
                candles: candles.into(),
                books: books.into(),
            }
        }
    }

    #[async_trait::async_trait]
    impl LiveMarketStream for StaticStream {
        async fn next_tick(&mut self) -> BrokerResult<Option<Tick>> {
            Ok(self.ticks.pop_front())
        }

        async fn next_candle(&mut self) -> BrokerResult<Option<Candle>> {
            Ok(self.candles.pop_front())
        }

        async fn next_order_book(&mut self) -> BrokerResult<Option<OrderBook>> {
            Ok(self.books.pop_front())
        }
    }

    fn build_tick(exchange: &str, price: i64) -> Tick {
        Tick {
            symbol: Symbol::from(exchange),
            price: Decimal::from(price),
            size: Decimal::ONE,
            side: Side::Buy,
            exchange_timestamp: Utc::now(),
            received_at: Utc::now(),
        }
    }

    fn build_candle(exchange: &str, close: i64) -> Candle {
        Candle {
            symbol: Symbol::from(exchange),
            interval: Interval::OneMinute,
            open: Decimal::from(close),
            high: Decimal::from(close),
            low: Decimal::from(close),
            close: Decimal::from(close),
            volume: Decimal::ONE,
            timestamp: Utc::now(),
        }
    }

    fn build_book(exchange: &str, price: i64) -> OrderBook {
        OrderBook {
            symbol: Symbol::from(exchange),
            bids: vec![OrderBookLevel {
                price: Decimal::from(price),
                size: Decimal::ONE,
            }],
            asks: vec![OrderBookLevel {
                price: Decimal::from(price + 1),
                size: Decimal::ONE,
            }],
            timestamp: Utc::now(),
            exchange_checksum: None,
            local_checksum: None,
        }
    }

    #[tokio::test]
    async fn router_market_stream_fans_in_events() {
        let shutdown = ShutdownSignal::new();
        let stream_a = Box::new(StaticStream::new(
            vec![build_tick("A", 1), build_tick("A", 2)],
            vec![build_candle("A", 10)],
            vec![build_book("A", 5)],
        ));
        let stream_b = Box::new(StaticStream::new(
            vec![build_tick("B", 3)],
            vec![build_candle("B", 20)],
            vec![build_book("B", 15)],
        ));
        let mut router = RouterMarketStream::new(
            vec![("A".into(), stream_a), ("B".into(), stream_b)],
            shutdown.clone(),
        );

        let first = router.next_tick().await.unwrap().unwrap();
        let second = router.next_tick().await.unwrap().unwrap();
        let third = router.next_tick().await.unwrap().unwrap();
        assert_eq!(first.symbol, Symbol::from("A"));
        assert_eq!(second.symbol, Symbol::from("A"));
        assert_eq!(third.symbol, Symbol::from("B"));

        let candle_a = router.next_candle().await.unwrap().unwrap();
        let candle_b = router.next_candle().await.unwrap().unwrap();
        assert_eq!(candle_a.symbol, Symbol::from("A"));
        assert_eq!(candle_b.symbol, Symbol::from("B"));

        let book_a = router.next_order_book().await.unwrap().unwrap();
        let book_b = router.next_order_book().await.unwrap().unwrap();
        assert_eq!(book_a.bids[0].price, Decimal::from(5));
        assert_eq!(book_b.asks[0].price, Decimal::from(16));

        shutdown.trigger();
    }
}
