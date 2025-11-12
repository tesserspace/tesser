use chrono::Duration;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tesser_core::{ExecutionHint, Signal, SignalKind};
use tesser_execution::{
    algorithm::TwapAlgorithm, AlgoStatus, ExecutionAlgorithm, ExecutionEngine, FixedOrderSizer,
    NoopRiskChecker, OrderOrchestrator, RiskContext, SqliteAlgoStateRepository,
};
use tesser_paper::PaperExecutionClient;

#[tokio::test]
async fn test_twap_algorithm_basic() {
    // Create a simple TWAP signal
    let signal = Signal::new("BTCUSDT", SignalKind::EnterLong, 0.8);

    // Create TWAP algorithm directly with very short duration for testing
    let mut twap = TwapAlgorithm::new(
        signal.clone(),
        1.0,                  // total quantity
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
    assert_eq!(orders[0].order_request.quantity, 0.5); // 1.0 / 2 slices

    // Wait for the next slice interval (1 second for 2 slices over 2 seconds)
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    // Trigger more timer events - since no fills have been processed,
    // the second slice gets all remaining quantity
    let orders = twap.on_timer().unwrap();
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].order_request.quantity, 1.0); // All remaining quantity

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
    let sizer = Box::new(FixedOrderSizer { quantity: 1.0 });
    let risk_checker = Arc::new(NoopRiskChecker);
    let execution_engine = Arc::new(ExecutionEngine::new(client, sizer, risk_checker));

    // Create state repository
    let algo_state_repo = Arc::new(SqliteAlgoStateRepository::new(temp_path).unwrap());

    // Create orchestrator
    let orchestrator = OrderOrchestrator::new(execution_engine, algo_state_repo)
        .await
        .unwrap();

    // Create TWAP signal
    let signal =
        Signal::new("BTCUSDT", SignalKind::EnterLong, 0.8).with_hint(ExecutionHint::Twap {
            duration: Duration::minutes(2),
        });

    let ctx = RiskContext {
        signed_position_qty: 0.0,
        portfolio_equity: 10000.0,
        last_price: 50000.0,
        liquidate_only: false,
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
    let twap = TwapAlgorithm::new(signal, 1.0, Duration::minutes(5), 5).unwrap();

    // Serialize state
    let state = twap.state();

    // Deserialize state
    let restored_twap = TwapAlgorithm::from_state(state).unwrap();

    // Verify restored algorithm has same properties
    assert_eq!(restored_twap.status(), AlgoStatus::Working);
}
