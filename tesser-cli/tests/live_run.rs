#![cfg(feature = "bybit")]
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use hyper::{
    body::to_bytes,
    service::{make_service_fn, service_fn},
    Body, Request, Response,
};
use rust_decimal::Decimal;
use serde_json::{json, Value as JsonValue};
use tempfile::tempdir;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};
use tokio::{sync::mpsc, task::JoinHandle};
use tonic::transport::Channel;

use async_trait::async_trait;
use tesser_cli::live::{
    run_live_with_shutdown, ExecutionBackend, LiveSessionSettings, PersistenceSettings,
    ShutdownSignal,
};
use tesser_cli::PublicChannel;
use tesser_config::{AlertingConfig, ExchangeConfig, PersistenceEngine, RiskManagementConfig};
use tesser_core::{AccountBalance, Candle, Interval, Position, Side, Signal, SignalKind, Tick};
use tesser_portfolio::{SqliteStateRepository, StateRepository};
use tesser_rpc::proto::control_service_client::ControlServiceClient;
use tesser_rpc::proto::{
    CancelAllRequest, GetOpenOrdersRequest, GetPortfolioRequest, GetStatusRequest,
};
use tesser_strategy::{Strategy, StrategyContext, StrategyResult};
use tesser_test_utils::{
    AccountConfig, MockExchange, MockExchangeConfig, OrderFillStep, Scenario, ScenarioAction,
    ScenarioManager, ScenarioTrigger,
};

const SYMBOL: &str = "BTCUSDT";

fn next_control_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind port");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr
}

async fn connect_control_client(addr: SocketAddr) -> Result<ControlServiceClient<Channel>> {
    let endpoint = format!("http://{addr}");
    const MAX_ATTEMPTS: usize = 50;
    for attempt in 0..MAX_ATTEMPTS {
        match ControlServiceClient::connect(endpoint.clone()).await {
            Ok(client) => return Ok(client),
            Err(err) => {
                if attempt + 1 == MAX_ATTEMPTS {
                    return Err(anyhow!("failed to connect to control plane: {err}"));
                }
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
    unreachable!()
}

#[tokio::test(flavor = "multi_thread")]
async fn mock_exchange_starts() -> Result<()> {
    let account = AccountConfig::new("test-key", "test-secret").with_balance(AccountBalance {
        currency: "USDT".into(),
        total: Decimal::new(10000, 0),
        available: Decimal::new(10000, 0),
        updated_at: Utc::now(),
    });

    let candles = vec![Candle {
        symbol: SYMBOL.into(),
        interval: Interval::OneMinute,
        open: Decimal::new(1000, 0),
        high: Decimal::new(1010, 0),
        low: Decimal::new(995, 0),
        close: Decimal::new(1005, 0),
        volume: Decimal::ONE,
        timestamp: Utc::now(),
    }];

    let ticks = vec![Tick {
        symbol: SYMBOL.into(),
        price: Decimal::new(1005, 0),
        size: Decimal::ONE,
        side: Side::Buy,
        exchange_timestamp: Utc::now(),
        received_at: Utc::now(),
    }];

    let config = MockExchangeConfig::new()
        .with_account(account)
        .with_candles(candles)
        .with_ticks(ticks);
    let exchange = MockExchange::start(config).await?;
    assert!(exchange.rest_url().starts_with("http://127.0.0.1"));
    assert!(exchange.ws_url().starts_with("ws://127.0.0.1"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn live_run_executes_round_trip() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let account = AccountConfig::new("test-key", "test-secret").with_balance(AccountBalance {
        currency: "USDT".into(),
        total: Decimal::new(10_000, 0),
        available: Decimal::new(10_000, 0),
        updated_at: Utc::now(),
    });
    let base_time = Utc::now();
    let candles = (0..6)
        .map(|i| Candle {
            symbol: SYMBOL.into(),
            interval: Interval::OneMinute,
            open: Decimal::new(1_000 + i as i64, 0),
            high: Decimal::new(1_010 + i as i64, 0),
            low: Decimal::new(995 + i as i64, 0),
            close: Decimal::new(1_005 + i as i64, 0),
            volume: Decimal::ONE,
            timestamp: base_time + ChronoDuration::minutes(i as i64),
        })
        .collect::<Vec<_>>();
    let ticks = (0..6)
        .map(|i| Tick {
            symbol: SYMBOL.into(),
            price: Decimal::new(1_005 + i as i64, 0),
            size: Decimal::ONE,
            side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
            exchange_timestamp: base_time + ChronoDuration::seconds(i as i64),
            received_at: Utc::now(),
        })
        .collect::<Vec<_>>();

    let config = MockExchangeConfig::new()
        .with_account(account)
        .with_candles(candles)
        .with_ticks(ticks);
    let mut exchange = MockExchange::start(config).await?;
    let scenarios = exchange.state().scenarios();
    scenarios
        .push(Scenario {
            name: "entry-fill".into(),
            trigger: ScenarioTrigger::OrderCreate,
            action: ScenarioAction::FillPlan {
                steps: vec![OrderFillStep {
                    after: Duration::from_millis(25),
                    quantity: Decimal::ONE,
                    price: Some(Decimal::new(1_001, 0)),
                }],
            },
        })
        .await;
    scenarios
        .push(Scenario {
            name: "exit-fill".into(),
            trigger: ScenarioTrigger::OrderCreate,
            action: ScenarioAction::FillPlan {
                steps: vec![OrderFillStep {
                    after: Duration::from_millis(25),
                    quantity: Decimal::ONE,
                    price: Some(Decimal::new(1_002, 0)),
                }],
            },
        })
        .await;

    let temp = tempdir()?;
    let state_path = temp.path().join("live_state.db");
    let markets_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../config/markets.toml");
    let settings = LiveSessionSettings {
        category: PublicChannel::Linear,
        interval: Interval::OneMinute,
        quantity: Decimal::ONE,
        slippage_bps: Decimal::ZERO,
        fee_bps: Decimal::ZERO,
        history: 8,
        metrics_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        persistence: PersistenceSettings::new(PersistenceEngine::Sqlite, state_path.clone()),
        initial_balances: HashMap::from([(String::from("USDT"), Decimal::new(10_000, 0))]),
        reporting_currency: "USDT".into(),
        markets_file: Some(markets_file),
        alerting: AlertingConfig::default(),
        exec_backend: ExecutionBackend::Live,
        risk: RiskManagementConfig::default(),
        reconciliation_interval: Duration::from_secs(1),
        reconciliation_threshold: Decimal::new(1, 3),
        driver: "bybit".into(),
        orderbook_depth: 50,
        record_path: None,
        control_addr: "127.0.0.1:0".parse().unwrap(),
    };
    let exchange_cfg = ExchangeConfig {
        rest_url: exchange.rest_url(),
        ws_url: exchange.ws_url(),
        api_key: "test-key".into(),
        api_secret: "test-secret".into(),
        driver: "bybit".into(),
        params: JsonValue::Null,
    };
    let (strategy, monitor) = ScriptedStrategy::new(SYMBOL);
    let shutdown = ShutdownSignal::new();
    let run_handle = tokio::spawn(run_live_with_shutdown(
        Box::new(strategy),
        vec![SYMBOL.to_string()],
        exchange_cfg,
        settings,
        shutdown.clone(),
    ));

    match timeout(Duration::from_secs(10), monitor.wait_for_fills(2)).await {
        Ok(result) => result?,
        Err(_) => {
            let state = exchange.state();
            let open_orders = state
                .open_orders("test-key", None)
                .await
                .unwrap_or_default();
            let positions = state
                .account_positions("test-key")
                .await
                .unwrap_or_default();
            panic!(
                "timed out waiting for fills; open_orders = {:?}, positions = {:?}",
                open_orders, positions
            );
        }
    };
    shutdown.trigger();
    run_handle.await??;

    let positions = exchange
        .state()
        .account_positions("test-key")
        .await
        .expect("positions");
    assert!(positions
        .into_iter()
        .all(|position| position.quantity.is_zero()));
    let balances = exchange
        .state()
        .account_balances("test-key")
        .await
        .expect("balances");
    let usdt = balances.iter().find(|b| b.currency == "USDT").unwrap();
    assert_eq!(usdt.available, Decimal::new(10_001, 0));

    exchange.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn control_plane_reports_status() -> Result<()> {
    let account = AccountConfig::new("test-key", "test-secret").with_balance(AccountBalance {
        currency: "USDT".into(),
        total: Decimal::new(10_000, 0),
        available: Decimal::new(10_000, 0),
        updated_at: Utc::now(),
    });
    let candles = vec![Candle {
        symbol: SYMBOL.into(),
        interval: Interval::OneMinute,
        open: Decimal::new(1_000, 0),
        high: Decimal::new(1_010, 0),
        low: Decimal::new(995, 0),
        close: Decimal::new(1_005, 0),
        volume: Decimal::ONE,
        timestamp: Utc::now(),
    }];
    let ticks = vec![Tick {
        symbol: SYMBOL.into(),
        price: Decimal::new(1_005, 0),
        size: Decimal::ONE,
        side: Side::Buy,
        exchange_timestamp: Utc::now(),
        received_at: Utc::now(),
    }];
    let config = MockExchangeConfig::new()
        .with_account(account)
        .with_candles(candles)
        .with_ticks(ticks);
    let mut exchange = MockExchange::start(config).await?;

    let control_addr = next_control_addr();
    let temp = tempdir()?;
    let state_path = temp.path().join("live_state.db");
    let markets_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../config/markets.toml");
    let settings = LiveSessionSettings {
        category: PublicChannel::Linear,
        interval: Interval::OneMinute,
        quantity: Decimal::ONE,
        slippage_bps: Decimal::ZERO,
        fee_bps: Decimal::ZERO,
        history: 8,
        metrics_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        persistence: PersistenceSettings::new(PersistenceEngine::Sqlite, state_path.clone()),
        initial_balances: HashMap::from([(String::from("USDT"), Decimal::new(10_000, 0))]),
        reporting_currency: "USDT".into(),
        markets_file: Some(markets_file),
        alerting: AlertingConfig::default(),
        exec_backend: ExecutionBackend::Live,
        risk: RiskManagementConfig::default(),
        reconciliation_interval: Duration::from_secs(1),
        reconciliation_threshold: Decimal::new(1, 3),
        driver: "bybit".into(),
        orderbook_depth: 50,
        record_path: None,
        control_addr,
    };
    let exchange_cfg = ExchangeConfig {
        rest_url: exchange.rest_url(),
        ws_url: exchange.ws_url(),
        api_key: "test-key".into(),
        api_secret: "test-secret".into(),
        driver: "bybit".into(),
        params: JsonValue::Null,
    };
    let strategy: Box<dyn Strategy> = Box::new(PassiveStrategy::new(SYMBOL));
    let shutdown = ShutdownSignal::new();
    let run_handle = tokio::spawn(run_live_with_shutdown(
        strategy,
        vec![SYMBOL.to_string()],
        exchange_cfg,
        settings,
        shutdown.clone(),
    ));

    let mut client = connect_control_client(control_addr).await?;
    let status = client.get_status(GetStatusRequest {}).await?.into_inner();
    assert!(!status.shutdown);
    assert_eq!(status.active_algorithms, 0);

    let portfolio = client
        .get_portfolio(GetPortfolioRequest {})
        .await?
        .into_inner();
    let snapshot = portfolio.portfolio.expect("snapshot");
    assert_eq!(snapshot.reporting_currency, "USDT");
    assert!(!snapshot.liquidate_only);

    let open_orders = client
        .get_open_orders(GetOpenOrdersRequest {})
        .await?
        .into_inner();
    assert!(open_orders.orders.is_empty());

    let cancel_resp = client.cancel_all(CancelAllRequest {}).await?.into_inner();
    assert_eq!(cancel_resp.cancelled_orders, 0);

    shutdown.trigger();
    run_handle.await??;
    exchange.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn reconciliation_enters_liquidate_only_on_divergence() -> Result<()> {
    let account = AccountConfig::new("test-key", "test-secret").with_balance(AccountBalance {
        currency: "USDT".into(),
        total: Decimal::new(10_000, 0),
        available: Decimal::new(10_000, 0),
        updated_at: Utc::now(),
    });
    let base_time = Utc::now();
    let candles = (0..3)
        .map(|i| Candle {
            symbol: SYMBOL.into(),
            interval: Interval::OneMinute,
            open: Decimal::new(1_000 + i as i64, 0),
            high: Decimal::new(1_010 + i as i64, 0),
            low: Decimal::new(995 + i as i64, 0),
            close: Decimal::new(1_005 + i as i64, 0),
            volume: Decimal::ONE,
            timestamp: base_time + ChronoDuration::minutes(i as i64),
        })
        .collect::<Vec<_>>();
    let ticks = (0..3)
        .map(|i| Tick {
            symbol: SYMBOL.into(),
            price: Decimal::new(1_005 + i as i64, 0),
            size: Decimal::ONE,
            side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
            exchange_timestamp: base_time + ChronoDuration::seconds(i as i64),
            received_at: Utc::now(),
        })
        .collect::<Vec<_>>();
    let config = MockExchangeConfig::new()
        .with_account(account)
        .with_candles(candles)
        .with_ticks(ticks);
    let mut exchange = MockExchange::start(config).await?;
    let (alert_url, mut alert_rx, alert_handle) = start_alert_listener().await?;

    let alerting = AlertingConfig {
        webhook_url: Some(alert_url),
        ..AlertingConfig::default()
    };

    let temp = tempdir()?;
    let state_path = temp.path().join("live_state.db");
    let markets_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../config/markets.toml");
    let settings = LiveSessionSettings {
        category: PublicChannel::Linear,
        interval: Interval::OneMinute,
        quantity: Decimal::ONE,
        slippage_bps: Decimal::ZERO,
        fee_bps: Decimal::ZERO,
        history: 4,
        metrics_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        persistence: PersistenceSettings::new(PersistenceEngine::Sqlite, state_path.clone()),
        initial_balances: HashMap::from([(String::from("USDT"), Decimal::new(10_000, 0))]),
        reporting_currency: "USDT".into(),
        markets_file: Some(markets_file),
        alerting,
        exec_backend: ExecutionBackend::Live,
        risk: RiskManagementConfig::default(),
        reconciliation_interval: Duration::from_millis(200),
        reconciliation_threshold: Decimal::new(1, 4),
        driver: "bybit".into(),
        orderbook_depth: 50,
        record_path: None,
        control_addr: "127.0.0.1:0".parse().unwrap(),
    };
    let exchange_cfg = ExchangeConfig {
        rest_url: exchange.rest_url(),
        ws_url: exchange.ws_url(),
        api_key: "test-key".into(),
        api_secret: "test-secret".into(),
        driver: "bybit".into(),
        params: JsonValue::Null,
    };
    let strategy: Box<dyn Strategy> = Box::new(PassiveStrategy::new(SYMBOL));
    let shutdown = ShutdownSignal::new();
    let run_handle = tokio::spawn(run_live_with_shutdown(
        strategy,
        vec![SYMBOL.to_string()],
        exchange_cfg,
        settings,
        shutdown.clone(),
    ));

    sleep(Duration::from_millis(300)).await;
    exchange
        .state()
        .with_account_mut("test-key", |account| {
            account.positions.insert(
                SYMBOL.into(),
                Position {
                    symbol: SYMBOL.into(),
                    side: Some(Side::Buy),
                    quantity: Decimal::new(5, 0),
                    entry_price: Some(Decimal::new(1_000, 0)),
                    unrealized_pnl: Decimal::ZERO,
                    updated_at: Utc::now(),
                },
            );
            if let Some(balance) = account.balances.get_mut("USDT") {
                balance.available += Decimal::new(500, 0);
                balance.total = balance.available;
                balance.updated_at = Utc::now();
            }
            Ok(())
        })
        .await?;

    let alert = timeout(Duration::from_secs(5), alert_rx.recv())
        .await
        .context("alert listener timed out")?
        .context("alert channel closed unexpectedly")?;
    assert_eq!(
        alert.get("title").and_then(|value| value.as_str()),
        Some("State reconciliation divergence")
    );
    wait_for_liquidate_only(&state_path, Duration::from_secs(5)).await?;

    shutdown.trigger();
    run_handle.await??;
    alert_handle.abort();
    exchange.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn alerts_on_rejected_order() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let account = AccountConfig::new("test-key", "test-secret").with_balance(AccountBalance {
        currency: "USDT".into(),
        total: Decimal::new(10_000, 0),
        available: Decimal::new(10_000, 0),
        updated_at: Utc::now(),
    });
    let base_time = Utc::now();
    let candles = (0..2)
        .map(|i| Candle {
            symbol: SYMBOL.into(),
            interval: Interval::OneMinute,
            open: Decimal::new(1_000 + i as i64, 0),
            high: Decimal::new(1_010 + i as i64, 0),
            low: Decimal::new(995 + i as i64, 0),
            close: Decimal::new(1_005 + i as i64, 0),
            volume: Decimal::ONE,
            timestamp: base_time + ChronoDuration::minutes(i as i64),
        })
        .collect::<Vec<_>>();
    let ticks = (0..2)
        .map(|i| Tick {
            symbol: SYMBOL.into(),
            price: Decimal::new(1_005 + i as i64, 0),
            size: Decimal::ONE,
            side: Side::Buy,
            exchange_timestamp: base_time + ChronoDuration::seconds(i as i64),
            received_at: Utc::now(),
        })
        .collect::<Vec<_>>();
    let rejection_payload = json!({
        "topic": "order",
        "data": [{
            "orderId": "MOCK-ORDER-1",
            "symbol": SYMBOL,
            "side": "Buy",
            "orderStatus": "Rejected"
        }]
    });
    let scenarios = ScenarioManager::new();
    scenarios
        .push(Scenario {
            name: "reject-order".into(),
            trigger: ScenarioTrigger::OrderCreate,
            action: ScenarioAction::InjectPrivateEvent(rejection_payload),
        })
        .await;
    let config = MockExchangeConfig::new()
        .with_account(account)
        .with_candles(candles)
        .with_ticks(ticks)
        .with_scenarios(scenarios);
    let mut exchange = MockExchange::start(config).await?;
    let (alert_url, mut alert_rx, alert_handle) = start_alert_listener().await?;

    let alerting = AlertingConfig {
        webhook_url: Some(alert_url),
        max_order_failures: 1,
        ..AlertingConfig::default()
    };

    let temp = tempdir()?;
    let state_path = temp.path().join("live_state.db");
    let markets_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../config/markets.toml");
    let settings = LiveSessionSettings {
        category: PublicChannel::Linear,
        interval: Interval::OneMinute,
        quantity: Decimal::ONE,
        slippage_bps: Decimal::ZERO,
        fee_bps: Decimal::ZERO,
        history: 4,
        metrics_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        persistence: PersistenceSettings::new(PersistenceEngine::Sqlite, state_path),
        initial_balances: HashMap::from([(String::from("USDT"), Decimal::new(10_000, 0))]),
        reporting_currency: "USDT".into(),
        markets_file: Some(markets_file),
        alerting,
        exec_backend: ExecutionBackend::Live,
        risk: RiskManagementConfig::default(),
        reconciliation_interval: Duration::from_secs(1),
        reconciliation_threshold: Decimal::new(1, 3),
        driver: "bybit".into(),
        orderbook_depth: 50,
        record_path: None,
        control_addr: "127.0.0.1:0".parse().unwrap(),
    };
    let exchange_cfg = ExchangeConfig {
        rest_url: exchange.rest_url(),
        ws_url: exchange.ws_url(),
        api_key: "test-key".into(),
        api_secret: "test-secret".into(),
        driver: "bybit".into(),
        params: JsonValue::Null,
    };
    let (strategy, monitor) = ScriptedStrategy::new(SYMBOL);
    let shutdown = ShutdownSignal::new();
    let run_handle = tokio::spawn(run_live_with_shutdown(
        Box::new(strategy),
        vec![SYMBOL.to_string()],
        exchange_cfg,
        settings,
        shutdown.clone(),
    ));

    // Wait for a rejection alert
    let alert = timeout(Duration::from_secs(10), async {
        loop {
            match alert_rx.recv().await {
                Some(alert)
                    if alert.get("title").and_then(|value| value.as_str())
                        == Some("Order rejected") =>
                {
                    tracing::info!(?alert, "received order rejected alert payload");
                    break Some(alert);
                }
                Some(other) => {
                    tracing::info!(?other, "received non-rejection alert while waiting");
                    continue;
                }
                None => break None,
            }
        }
    })
    .await
    .context("alert listener timed out")?
    .context("alert channel closed unexpectedly")?;
    assert_eq!(
        alert.get("title").and_then(|value| value.as_str()),
        Some("Order rejected")
    );

    // Prevent unused warnings for strategy monitor
    drop(monitor);
    shutdown.trigger();
    run_handle.await??;
    alert_handle.abort();
    exchange.shutdown().await;
    Ok(())
}

async fn start_alert_listener() -> Result<(String, mpsc::Receiver<JsonValue>, JoinHandle<()>)> {
    let (tx, rx) = mpsc::channel(8);
    let make_svc = make_service_fn(move |_| {
        let tx = tx.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                let tx = tx.clone();
                async move {
                    let bytes = to_bytes(req.into_body()).await?;
                    if let Ok(json) = serde_json::from_slice::<JsonValue>(&bytes) {
                        let _ = tx.send(json).await;
                    }
                    Ok::<_, hyper::Error>(Response::new(Body::from("ok")))
                }
            }))
        }
    });
    let server = hyper::Server::bind(&"127.0.0.1:0".parse().unwrap()).serve(make_svc);
    let addr = server.local_addr();
    let handle = tokio::spawn(async move {
        if let Err(err) = server.await {
            eprintln!("alert listener exited: {err}");
        }
    });
    Ok((format!("http://{}", addr), rx, handle))
}

async fn wait_for_liquidate_only(path: &Path, timeout: Duration) -> Result<()> {
    let repo = SqliteStateRepository::new(path.to_path_buf());
    let deadline = Instant::now() + timeout;
    loop {
        let state = repo
            .load()
            .map_err(|err| anyhow!("failed to load state: {err}"))?;
        if state
            .portfolio
            .as_ref()
            .map(|snapshot| snapshot.liquidate_only)
            .unwrap_or(false)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(anyhow!(
        "timed out waiting for liquidate-only flag in persisted state"
    ))
}

struct ScriptedStrategy {
    symbol: String,
    stage: usize,
    pending: Vec<Signal>,
    state: Arc<StrategyState>,
}

struct PassiveStrategy {
    symbol: String,
}

impl PassiveStrategy {
    fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
        }
    }
}

#[async_trait]
impl Strategy for PassiveStrategy {
    fn name(&self) -> &str {
        "passive-test"
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn configure(&mut self, _params: toml::Value) -> StrategyResult<()> {
        Ok(())
    }

    async fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    async fn on_candle(&mut self, _ctx: &StrategyContext, _candle: &Candle) -> StrategyResult<()> {
        Ok(())
    }

    async fn on_fill(
        &mut self,
        _ctx: &StrategyContext,
        _fill: &tesser_core::Fill,
    ) -> StrategyResult<()> {
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        Vec::new()
    }
}

impl ScriptedStrategy {
    fn new(symbol: &str) -> (Self, StrategyMonitor) {
        let state = Arc::new(StrategyState {
            fills: AtomicUsize::new(0),
            notify: Notify::new(),
        });
        (
            Self {
                symbol: symbol.to_string(),
                stage: 0,
                pending: Vec::new(),
                state: state.clone(),
            },
            StrategyMonitor { inner: state },
        )
    }
}

#[async_trait]
impl Strategy for ScriptedStrategy {
    fn name(&self) -> &str {
        "scripted-test"
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn configure(&mut self, _params: toml::Value) -> StrategyResult<()> {
        Ok(())
    }

    async fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    async fn on_candle(&mut self, _ctx: &StrategyContext, _candle: &Candle) -> StrategyResult<()> {
        if self.stage == 0 {
            self.pending
                .push(Signal::new(self.symbol.clone(), SignalKind::EnterLong, 1.0));
            self.stage = 1;
        } else if self.stage == 1 {
            self.pending
                .push(Signal::new(self.symbol.clone(), SignalKind::ExitLong, 1.0));
            self.stage = 2;
        }
        Ok(())
    }

    async fn on_fill(
        &mut self,
        _ctx: &StrategyContext,
        _fill: &tesser_core::Fill,
    ) -> StrategyResult<()> {
        self.state.fills.fetch_add(1, Ordering::SeqCst);
        self.state.notify.notify_waiters();
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.pending)
    }
}

struct StrategyState {
    fills: AtomicUsize,
    notify: Notify,
}

struct StrategyMonitor {
    inner: Arc<StrategyState>,
}

impl StrategyMonitor {
    async fn wait_for_fills(&self, expected: usize) -> Result<()> {
        while self.inner.fills.load(Ordering::SeqCst) < expected {
            self.inner.notify.notified().await;
        }
        Ok(())
    }
}
