use async_trait::async_trait;
use chrono::Duration;
use rust_decimal::Decimal;
use std::sync::{Arc, Mutex};
use tempfile::NamedTempFile;
use tesser_broker::{BrokerError, BrokerInfo, ExecutionClient};
use tesser_core::{ExecutionHint, Signal, SignalKind, SignalPanicBehavior, Symbol};
use tesser_execution::{
    algorithm::{ChildOrderAction, TwapAlgorithm},
    AlgoStatus, ExecutionAlgorithm, ExecutionEngine, FixedOrderSizer, NoopRiskChecker,
    OrderOrchestrator, PanicCloseConfig, PanicObserver, RiskContext, SqliteAlgoStateRepository,
};
use tesser_paper::PaperExecutionClient;
use uuid::Uuid;

#[tokio::test]
async fn test_twap_algorithm_basic() {
    // Create a simple TWAP signal
    let signal = Signal::new("BTCUSDT", SignalKind::EnterLong, 0.8);

    // Create TWAP algorithm directly with very short duration for testing
    let mut twap = TwapAlgorithm::new(
        signal.clone(),
        Decimal::ONE,         // total quantity
        Duration::seconds(2), // Very short duration
        2,                    // Only 2 slices for simplicity
    )
    .unwrap();

    // Test initial state
    assert_eq!(twap.status(), AlgoStatus::Working);

    // Start the algorithm
    let initial_orders = twap.start().unwrap();
    assert!(initial_orders.is_empty()); // TWAP starts with no initial orders

    // Add a small delay to ensure timer condition is met
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Trigger timer event to get first slice
    let orders = twap.on_timer().unwrap();
    assert_eq!(orders.len(), 1);
    match &orders[0].action {
        ChildOrderAction::Place(request) => {
            assert_eq!(request.quantity, Decimal::new(5, 1));
        }
        other => panic!("unexpected order action: {other:?}"),
    }

    // Wait for the next slice interval (1 second for 2 slices over 2 seconds)
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    // Trigger more timer events - since no fills have been processed,
    // the second slice gets all remaining quantity
    let orders = twap.on_timer().unwrap();
    assert_eq!(orders.len(), 1);
    match &orders[0].action {
        ChildOrderAction::Place(request) => assert_eq!(request.quantity, Decimal::ONE),
        other => panic!("unexpected order action: {other:?}"),
    }

    // After all slices are executed, no more orders should be generated
    let orders = twap.on_timer().unwrap();
    assert_eq!(orders.len(), 0);
}

#[tokio::test]
async fn test_orchestrator_integration() {
    // Create temporary database for state persistence
    let temp_file = NamedTempFile::new().unwrap();
    let temp_path = temp_file.path();

    // Create execution engine with paper client
    let client = Arc::new(PaperExecutionClient::default());
    let sizer = Box::new(FixedOrderSizer {
        quantity: Decimal::ONE,
    });
    let risk_checker = Arc::new(NoopRiskChecker);
    let execution_engine = Arc::new(ExecutionEngine::new(client, sizer, risk_checker));

    // Create state repository
    let algo_state_repo = Arc::new(SqliteAlgoStateRepository::new(temp_path).unwrap());

    // Create orchestrator
    let orchestrator = OrderOrchestrator::new(
        execution_engine,
        algo_state_repo,
        Vec::new(),
        PanicCloseConfig::default(),
        None,
    )
    .await
    .unwrap();

    // Create TWAP signal
    let signal =
        Signal::new("BTCUSDT", SignalKind::EnterLong, 0.8).with_hint(ExecutionHint::Twap {
            duration: Duration::minutes(2),
        });
    let symbol: Symbol = "BTCUSDT".into();
    let ctx = RiskContext {
        symbol,
        exchange: symbol.exchange,
        signed_position_qty: Decimal::ZERO,
        portfolio_equity: Decimal::from(10_000),
        exchange_equity: Decimal::from(10_000),
        last_price: Decimal::from(50_000),
        liquidate_only: false,
        ..RiskContext::default()
    };

    // Submit signal to orchestrator
    orchestrator.on_signal(&signal, &ctx).await.unwrap();

    // Check that algorithm was created
    assert_eq!(orchestrator.active_algorithms_count(), 1);

    // Trigger timer to execute slices
    orchestrator.on_timer_tick().await.unwrap();

    // Algorithm should still be active
    assert_eq!(orchestrator.active_algorithms_count(), 1);
}

#[test]
fn test_twap_state_persistence() {
    let signal = Signal::new("BTCUSDT", SignalKind::EnterLong, 0.8);
    let twap = TwapAlgorithm::new(signal, Decimal::ONE, Duration::minutes(5), 5).unwrap();

    // Serialize state
    let state = twap.state();

    // Deserialize state
    let restored_twap = TwapAlgorithm::from_state(state).unwrap();

    // Verify restored algorithm has same properties
    assert_eq!(restored_twap.status(), AlgoStatus::Working);
}

#[tokio::test]
async fn orchestrator_restores_from_sqlite() {
    let temp_file = NamedTempFile::new().unwrap();
    let temp_path = temp_file.path().to_path_buf();
    let repo = Arc::new(SqliteAlgoStateRepository::new(&temp_path).unwrap());

    let client = Arc::new(PaperExecutionClient::default());
    let sizer = Box::new(FixedOrderSizer {
        quantity: Decimal::ONE,
    });
    let risk_checker = Arc::new(NoopRiskChecker);
    let engine = Arc::new(ExecutionEngine::new(client, sizer, risk_checker));

    let orchestrator = OrderOrchestrator::new(
        engine.clone(),
        repo.clone(),
        Vec::new(),
        PanicCloseConfig::default(),
        None,
    )
    .await
    .unwrap();
    let signal =
        Signal::new("BTCUSDT", SignalKind::EnterLong, 0.5).with_hint(ExecutionHint::Twap {
            duration: Duration::minutes(1),
        });
    let symbol: Symbol = "BTCUSDT".into();
    let ctx = RiskContext {
        symbol,
        exchange: symbol.exchange,
        signed_position_qty: Decimal::ZERO,
        portfolio_equity: Decimal::from(10_000),
        exchange_equity: Decimal::from(10_000),
        last_price: Decimal::from(25_000),
        liquidate_only: false,
        ..RiskContext::default()
    };
    orchestrator.on_signal(&signal, &ctx).await.unwrap();
    assert_eq!(orchestrator.active_algorithms_count(), 1);

    drop(orchestrator);

    let restored =
        OrderOrchestrator::new(engine, repo, Vec::new(), PanicCloseConfig::default(), None)
            .await
            .unwrap();
    assert_eq!(restored.active_algorithms_count(), 1);
}

fn new_sqlite_repo() -> SqliteAlgoStateRepository {
    let temp_file = NamedTempFile::new().unwrap();
    SqliteAlgoStateRepository::new(temp_file.path()).unwrap()
}

type PanicEventLog = Arc<Mutex<Vec<(Uuid, Symbol, Decimal, String)>>>;

struct RecordingObserver {
    events: PanicEventLog,
}

impl RecordingObserver {
    fn new() -> (Self, PanicEventLog) {
        let store = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                events: store.clone(),
            },
            store,
        )
    }
}

impl PanicObserver for RecordingObserver {
    fn on_group_event(&self, group_id: Uuid, symbol: Symbol, qty: Decimal, reason: &str) {
        self.events
            .lock()
            .unwrap()
            .push((group_id, symbol, qty, reason.to_string()));
    }
}

struct FailingClient;

#[async_trait]
impl ExecutionClient for FailingClient {
    fn info(&self) -> BrokerInfo {
        BrokerInfo {
            name: "fail".into(),
            markets: vec![],
            supports_testnet: true,
        }
    }

    async fn place_order(
        &self,
        _request: tesser_core::OrderRequest,
    ) -> Result<tesser_core::Order, BrokerError> {
        Err(BrokerError::InvalidRequest("synthetic failure".into()))
    }

    async fn cancel_order(&self, _order_id: String, _symbol: Symbol) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn amend_order(
        &self,
        _request: tesser_core::OrderUpdateRequest,
    ) -> Result<tesser_core::Order, BrokerError> {
        Err(BrokerError::InvalidRequest("unsupported".into()))
    }

    async fn list_open_orders(
        &self,
        _symbol: Symbol,
    ) -> Result<Vec<tesser_core::Order>, BrokerError> {
        Ok(Vec::new())
    }

    async fn account_balances(&self) -> Result<Vec<tesser_core::AccountBalance>, BrokerError> {
        Ok(Vec::new())
    }

    async fn positions(&self) -> Result<Vec<tesser_core::Position>, BrokerError> {
        Ok(Vec::new())
    }

    async fn list_instruments(
        &self,
        _category: &str,
    ) -> Result<Vec<tesser_core::Instrument>, BrokerError> {
        Ok(Vec::new())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[tokio::test]
async fn router_failure_triggers_panic_observer() {
    let repo = Arc::new(new_sqlite_repo());
    let client = Arc::new(FailingClient);
    let sizer = Box::new(FixedOrderSizer {
        quantity: Decimal::from(2),
    });
    let engine = Arc::new(ExecutionEngine::new(
        client,
        sizer,
        Arc::new(NoopRiskChecker),
    ));
    let (observer, log) = RecordingObserver::new();

    let orchestrator = OrderOrchestrator::new(
        engine,
        repo,
        Vec::new(),
        PanicCloseConfig::default(),
        Some(Arc::new(observer)),
    )
    .await
    .unwrap();
    let group = Uuid::new_v4();
    let signal = Signal::new("BINANCE:BTCUSDT", SignalKind::EnterLong, 0.9)
        .with_group(group)
        .with_panic_behavior(SignalPanicBehavior::AggressiveLimit {
            offset_bps: Decimal::from(15),
        });
    let ctx = RiskContext {
        symbol: signal.symbol,
        exchange: signal.symbol.exchange,
        last_price: Decimal::from(30_000),
        ..RiskContext::default()
    };
    assert!(orchestrator.on_signal(&signal, &ctx).await.is_err());
    let events = log.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, group);
    assert_eq!(events[0].1, signal.symbol);
}
