#![cfg(feature = "bybit")]

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{Duration as ChronoDuration, Utc};
use rust_decimal::Decimal;
use tempfile::tempdir;
use tokio::time::sleep;

use tesser_broker::ExecutionClient;
use tesser_bybit::{BybitClient, BybitConfig, BybitCredentials};
use tesser_core::{
    AccountBalance, Candle, ExecutionHint, Interval, Order, OrderStatus, Side, Signal, SignalKind,
    Tick,
};
use tesser_execution::{
    ExecutionEngine, FixedOrderSizer, NoopRiskChecker, OrderOrchestrator, RiskContext,
    SqliteAlgoStateRepository,
};
use tesser_test_utils::{AccountConfig, MockExchange, MockExchangeConfig};

const SYMBOL: &str = "BTCUSDT";

async fn assert_single_open_order(client: &BybitClient) -> Result<Order> {
    let mut orders = client.list_open_orders(SYMBOL).await?;
    assert_eq!(orders.len(), 1, "expected exactly one open order");
    Ok(orders.remove(0))
}

fn slice_number(client_id: &str) -> Option<u32> {
    client_id
        .split("-slice-")
        .last()
        .and_then(|suffix| suffix.parse::<u32>().ok())
}

#[tokio::test(flavor = "multi_thread")]
async fn twap_orders_adopt_after_restart() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let account = AccountConfig::new("test-key", "test-secret").with_balance(AccountBalance {
        currency: "USDT".into(),
        total: Decimal::new(10_000, 0),
        available: Decimal::new(10_000, 0),
        updated_at: Utc::now(),
    });
    let ticks = vec![Tick {
        symbol: SYMBOL.into(),
        price: Decimal::new(20_000, 0),
        size: Decimal::ONE,
        side: Side::Buy,
        exchange_timestamp: Utc::now(),
        received_at: Utc::now(),
    }];
    let candles = vec![Candle {
        symbol: SYMBOL.into(),
        interval: Interval::OneMinute,
        open: Decimal::new(20_000, 0),
        high: Decimal::new(20_010, 0),
        low: Decimal::new(19_990, 0),
        close: Decimal::new(20_000, 0),
        volume: Decimal::ONE,
        timestamp: Utc::now(),
    }];
    let config = MockExchangeConfig::new()
        .with_account(account)
        .with_ticks(ticks)
        .with_candles(candles);
    let mut exchange = MockExchange::start(config).await?;

    let bybit_cfg = BybitConfig {
        base_url: exchange.rest_url(),
        ws_url: Some(exchange.ws_url()),
        ..BybitConfig::default()
    };
    let raw_client = Arc::new(BybitClient::new(
        bybit_cfg,
        Some(BybitCredentials {
            api_key: "test-key".into(),
            api_secret: "test-secret".into(),
        }),
    ));
    let client: Arc<dyn ExecutionClient> = raw_client.clone();

    let sizer = Box::new(FixedOrderSizer {
        quantity: Decimal::new(2, 0),
    });
    let engine = Arc::new(ExecutionEngine::new(
        client.clone(),
        sizer,
        Arc::new(NoopRiskChecker),
    ));

    let temp = tempdir()?;
    let algo_path = temp.path().join("algos.db");
    let repo = Arc::new(SqliteAlgoStateRepository::new(&algo_path)?);

    let orchestrator = OrderOrchestrator::new(engine.clone(), repo.clone(), Vec::new()).await?;

    let signal = Signal::new(SYMBOL, SignalKind::EnterLong, 0.8).with_hint(ExecutionHint::Twap {
        duration: ChronoDuration::seconds(4),
    });
    let ctx = RiskContext {
        signed_position_qty: Decimal::ZERO,
        portfolio_equity: Decimal::from(10_000),
        last_price: Decimal::new(20_000, 0),
        liquidate_only: false,
    };
    orchestrator.on_signal(&signal, &ctx).await?;
    sleep(Duration::from_millis(25)).await;
    orchestrator.on_timer_tick().await?;

    let first_order = assert_single_open_order(raw_client.as_ref()).await?;
    assert!(
        matches!(
            first_order.status,
            OrderStatus::PendingNew | OrderStatus::Accepted
        ),
        "first slice should be working"
    );
    let client_id = first_order
        .request
        .client_order_id
        .clone()
        .expect("client order id missing");
    let first_slice = slice_number(&client_id).expect("invalid slice id");

    drop(orchestrator);
    sleep(Duration::from_millis(10)).await;

    let open_orders = raw_client.list_open_orders(SYMBOL).await?;
    assert_eq!(open_orders.len(), 1);
    let adopted = open_orders[0].clone();

    let restarted_engine = Arc::new(ExecutionEngine::new(
        client.clone(),
        Box::new(FixedOrderSizer {
            quantity: Decimal::new(2, 0),
        }),
        Arc::new(NoopRiskChecker),
    ));
    let restored = OrderOrchestrator::new(restarted_engine, repo.clone(), open_orders).await?;
    restored.update_risk_context(SYMBOL, ctx);
    assert_eq!(restored.active_algorithms_count(), 1);

    let state = exchange.state();
    let (_order_snapshot, fill) = state
        .fill_order(
            "test-key",
            &adopted.id,
            adopted.request.quantity,
            Decimal::new(20_001, 0),
        )
        .await?;
    restored.on_fill(&fill).await?;

    sleep(Duration::from_secs(2)).await;
    restored.on_timer_tick().await?;

    let next_order = assert_single_open_order(raw_client.as_ref()).await?;
    let next_client_id = next_order
        .request
        .client_order_id
        .clone()
        .expect("client id missing after restart");
    let next_slice = slice_number(&next_client_id).expect("invalid slice id after restart");
    assert!(
        next_slice > first_slice,
        "expected a later slice after recovery (prev {first_slice}, got {next_slice})"
    );

    exchange.shutdown().await;
    Ok(())
}
