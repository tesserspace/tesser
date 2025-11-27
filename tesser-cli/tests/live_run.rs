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
use chrono::{DateTime, Duration as ChronoDuration, Utc};
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
    run_live_with_shutdown, ExecutionBackend, LiveSessionSettings, NamedExchange,
    PersistenceSettings, ShutdownSignal,
};
use tesser_cli::PublicChannel;
use tesser_config::{AlertingConfig, ExchangeConfig, PersistenceEngine, RiskManagementConfig};
use tesser_core::{
    AccountBalance, AssetId, Candle, ExchangeId, Interval, Position, Side, Signal, SignalKind,
    Symbol, Tick,
};
use tesser_execution::PanicCloseConfig;
use tesser_portfolio::{SqliteStateRepository, StateRepository};
use tesser_rpc::proto::control_service_client::ControlServiceClient;
use tesser_rpc::proto::{
    CancelAllRequest, GetOpenOrdersRequest, GetPortfolioRequest, GetStatusRequest,
};
use tesser_strategy::{PairsTradingArbitrage, Strategy, StrategyContext, StrategyResult};
use tesser_test_utils::{
    AccountConfig, AutoFillConfig, MockExchange, MockExchangeConfig, OrderFillStep, Scenario,
    ScenarioAction, ScenarioManager, ScenarioTrigger,
};

const SYMBOL: &str = "BTCUSDT";

fn next_control_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind port");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr
}

fn bybit_exchange() -> ExchangeId {
    ExchangeId::from("bybit_linear")
}

fn test_symbol() -> Symbol {
    Symbol::from_code(bybit_exchange(), SYMBOL)
}

fn usdt_asset() -> AssetId {
    AssetId::from_code(bybit_exchange(), "USDT")
}

fn binance_exchange() -> ExchangeId {
    ExchangeId::from("binance_perp")
}

fn binance_symbol() -> Symbol {
    Symbol::from_code(binance_exchange(), SYMBOL)
}

fn binance_asset() -> AssetId {
    AssetId::from_code(binance_exchange(), "USDT")
}

fn build_price_series(
    symbol: Symbol,
    base_time: DateTime<Utc>,
    closes: &[i64],
) -> (Vec<Candle>, Vec<Tick>) {
    let mut candles = Vec::with_capacity(closes.len());
    let mut ticks = Vec::with_capacity(closes.len());
    for (index, close) in closes.iter().enumerate() {
        let timestamp = base_time + ChronoDuration::minutes(index as i64);
        let price = Decimal::new(*close, 0);
        candles.push(Candle {
            symbol,
            interval: Interval::OneMinute,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: Decimal::ONE,
            timestamp,
        });
        ticks.push(Tick {
            symbol,
            price,
            size: Decimal::ONE,
            side: Side::Buy,
            exchange_timestamp: timestamp,
            received_at: timestamp,
        });
    }
    (candles, ticks)
}

async fn wait_for_fill_count(
    exchange: &MockExchange,
    expected: usize,
    timeout_dur: Duration,
) -> Result<()> {
    let state = exchange.state();
    let since = Utc::now() - ChronoDuration::minutes(1);
    timeout(timeout_dur, async move {
        loop {
            let fills = state.executions_between("test-key", since, None).await?;
            if fills.len() >= expected {
                return Ok::<(), anyhow::Error>(());
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {expected} fills"))??;
    Ok(())
}

async fn wait_for_positions_flat(exchange: &MockExchange, timeout_dur: Duration) -> Result<()> {
    let state = exchange.state();
    let venue = state.exchange().await;
    timeout(timeout_dur, async move {
        let mut iterations = 0u32;
        loop {
            let positions = state.account_positions("test-key").await?;
            if positions.iter().all(|position| position.quantity.is_zero()) {
                return Ok::<(), anyhow::Error>(());
            }
            if iterations.is_multiple_of(20) {
                println!(
                    "[pairs-test] waiting for {venue} positions to flatten, snapshot={positions:?}"
                );
            }
            iterations = iterations.wrapping_add(1);
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {venue} positions to flatten"))??;
    Ok(())
}

fn account_balance(total: Decimal) -> AccountBalance {
    let asset = usdt_asset();
    AccountBalance {
        exchange: asset.exchange,
        asset,
        total,
        available: total,
        updated_at: Utc::now(),
    }
}

fn default_initial_balances() -> HashMap<AssetId, Decimal> {
    HashMap::from([(usdt_asset(), Decimal::new(10_000, 0))])
}

fn dual_initial_balances() -> HashMap<AssetId, Decimal> {
    HashMap::from([
        (usdt_asset(), Decimal::new(10_000, 0)),
        (binance_asset(), Decimal::new(10_000, 0)),
    ])
}

fn binance_account_balance(total: Decimal) -> AccountBalance {
    let asset = binance_asset();
    AccountBalance {
        exchange: asset.exchange,
        asset,
        total,
        available: total,
        updated_at: Utc::now(),
    }
}

fn spawn_live_runtime(
    strategy: Box<dyn Strategy>,
    symbols: Vec<Symbol>,
    exchanges: Vec<NamedExchange>,
    settings: LiveSessionSettings,
    shutdown: ShutdownSignal,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let result = run_live_with_shutdown(strategy, symbols, exchanges, settings, shutdown).await;
        if let Err(err) = &result {
            eprintln!("live runtime exited with error: {err:?}");
        }
        result
    })
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
    let account = AccountConfig::new("test-key", "test-secret")
        .with_balance(account_balance(Decimal::new(10_000, 0)));

    let candles = vec![Candle {
        symbol: test_symbol(),
        interval: Interval::OneMinute,
        open: Decimal::new(1000, 0),
        high: Decimal::new(1010, 0),
        low: Decimal::new(995, 0),
        close: Decimal::new(1005, 0),
        volume: Decimal::ONE,
        timestamp: Utc::now(),
    }];

    let ticks = vec![Tick {
        symbol: test_symbol(),
        price: Decimal::new(1005, 0),
        size: Decimal::ONE,
        side: Side::Buy,
        exchange_timestamp: Utc::now(),
        received_at: Utc::now(),
    }];

    let config = MockExchangeConfig::new()
        .with_exchange(bybit_exchange())
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
    let account = AccountConfig::new("test-key", "test-secret")
        .with_balance(account_balance(Decimal::new(10_000, 0)));
    let base_time = Utc::now();
    let candles = (0..6)
        .map(|i| Candle {
            symbol: test_symbol(),
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
            symbol: test_symbol(),
            price: Decimal::new(1_005 + i as i64, 0),
            size: Decimal::ONE,
            side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
            exchange_timestamp: base_time + ChronoDuration::seconds(i as i64),
            received_at: Utc::now(),
        })
        .collect::<Vec<_>>();

    let config = MockExchangeConfig::new()
        .with_exchange(bybit_exchange())
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
    let risk = RiskManagementConfig {
        max_drawdown: Decimal::ZERO,
        ..RiskManagementConfig::default()
    };
    let settings = LiveSessionSettings {
        category: PublicChannel::Linear,
        interval: Interval::OneMinute,
        quantity: Decimal::ONE,
        slippage_bps: Decimal::ZERO,
        fee_bps: Decimal::ZERO,
        history: 8,
        metrics_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        persistence: PersistenceSettings::new(PersistenceEngine::Sqlite, state_path.clone()),
        initial_balances: default_initial_balances(),
        reporting_currency: usdt_asset(),
        markets_file: Some(markets_file),
        alerting: AlertingConfig::default(),
        exec_backend: ExecutionBackend::Live,
        risk,
        reconciliation_interval: Duration::from_secs(60),
        reconciliation_threshold: Decimal::ONE,
        orderbook_depth: 50,
        record_path: None,
        control_addr: "127.0.0.1:0".parse().unwrap(),
        panic_close: PanicCloseConfig::default(),
    };
    let exchange_cfg = ExchangeConfig {
        rest_url: exchange.rest_url(),
        ws_url: exchange.ws_url(),
        api_key: "test-key".into(),
        api_secret: "test-secret".into(),
        driver: "bybit".into(),
        params: JsonValue::Null,
    };
    let exchanges = vec![NamedExchange {
        name: "bybit_linear".into(),
        config: exchange_cfg,
    }];
    let (strategy, monitor) = ScriptedStrategy::new(test_symbol());
    let shutdown = ShutdownSignal::new();
    let run_handle = spawn_live_runtime(
        Box::new(strategy),
        vec![test_symbol()],
        exchanges,
        settings,
        shutdown.clone(),
    );

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
    let usdt = balances.iter().find(|b| b.asset == usdt_asset()).unwrap();
    assert_eq!(usdt.available, Decimal::new(10_001, 0));

    exchange.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn live_run_executes_round_trip_multi_exchange() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let account = AccountConfig::new("test-key", "test-secret")
        .with_balance(account_balance(Decimal::new(10_000, 0)));
    let account_b = AccountConfig::new("test-key", "test-secret")
        .with_balance(binance_account_balance(Decimal::new(10_000, 0)));
    let base_time = Utc::now();
    let candles = (0..6)
        .map(|i| Candle {
            symbol: test_symbol(),
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
            symbol: test_symbol(),
            price: Decimal::new(1_005 + i as i64, 0),
            size: Decimal::ONE,
            side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
            exchange_timestamp: base_time + ChronoDuration::seconds(i as i64),
            received_at: Utc::now(),
        })
        .collect::<Vec<_>>();
    let config_a = MockExchangeConfig::new()
        .with_exchange(bybit_exchange())
        .with_account(account)
        .with_candles(candles.clone())
        .with_ticks(ticks.clone());
    let mut exchange_a = MockExchange::start(config_a).await?;
    let candles_b = candles
        .iter()
        .map(|c| Candle {
            symbol: binance_symbol(),
            interval: c.interval,
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close,
            volume: c.volume,
            timestamp: c.timestamp,
        })
        .collect::<Vec<_>>();
    let ticks_b = ticks
        .iter()
        .map(|t| Tick {
            symbol: binance_symbol(),
            price: t.price,
            size: t.size,
            side: t.side,
            exchange_timestamp: t.exchange_timestamp,
            received_at: t.received_at,
        })
        .collect::<Vec<_>>();
    let config_b = MockExchangeConfig::new()
        .with_exchange(binance_exchange())
        .with_account(account_b)
        .with_candles(candles_b)
        .with_ticks(ticks_b);
    let mut exchange_b = MockExchange::start(config_b).await?;
    for ex in [&exchange_a, &exchange_b] {
        let scenarios = ex.state().scenarios();
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
    }
    let temp = tempdir()?;
    let state_path = temp.path().join("live_state.db");
    let markets_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../config/markets.toml");
    let risk = RiskManagementConfig {
        max_drawdown: Decimal::ZERO,
        ..RiskManagementConfig::default()
    };
    let settings = LiveSessionSettings {
        category: PublicChannel::Linear,
        interval: Interval::OneMinute,
        quantity: Decimal::ONE,
        slippage_bps: Decimal::ZERO,
        fee_bps: Decimal::ZERO,
        history: 8,
        metrics_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        persistence: PersistenceSettings::new(PersistenceEngine::Sqlite, state_path.clone()),
        initial_balances: dual_initial_balances(),
        reporting_currency: usdt_asset(),
        markets_file: Some(markets_file),
        alerting: AlertingConfig::default(),
        exec_backend: ExecutionBackend::Live,
        risk,
        reconciliation_interval: Duration::from_secs(60),
        reconciliation_threshold: Decimal::ONE,
        orderbook_depth: 50,
        record_path: None,
        control_addr: "127.0.0.1:0".parse().unwrap(),
        panic_close: PanicCloseConfig::default(),
    };
    let exchanges = vec![
        NamedExchange {
            name: "bybit_linear".into(),
            config: ExchangeConfig {
                rest_url: exchange_a.rest_url(),
                ws_url: exchange_a.ws_url(),
                api_key: "test-key".into(),
                api_secret: "test-secret".into(),
                driver: "bybit".into(),
                params: JsonValue::Null,
            },
        },
        NamedExchange {
            name: "binance_perp".into(),
            config: ExchangeConfig {
                rest_url: exchange_b.rest_url(),
                ws_url: exchange_b.ws_url(),
                api_key: "test-key".into(),
                api_secret: "test-secret".into(),
                // Reuse the Bybit connector stack for the mock Binance venue to avoid
                // implementing the full Binance API surface in tests.
                driver: "bybit".into(),
                params: JsonValue::Null,
            },
        },
    ];
    let symbols = vec![test_symbol(), binance_symbol()];
    let (strategy, monitor) = MultiScriptedStrategy::new(symbols.clone());
    let shutdown = ShutdownSignal::new();
    let run_handle = spawn_live_runtime(
        Box::new(strategy),
        symbols,
        exchanges,
        settings,
        shutdown.clone(),
    );
    match timeout(Duration::from_secs(10), monitor.wait_for_fills(4)).await {
        Ok(result) => result?,
        Err(_) => {
            return Err(anyhow!(
                "strategy only observed {} fills before timing out",
                monitor.fills()
            ));
        }
    }
    shutdown.trigger();
    run_handle.await??;
    for ex in [&exchange_a, &exchange_b] {
        let positions = ex
            .state()
            .account_positions("test-key")
            .await
            .expect("positions");
        assert!(positions
            .into_iter()
            .all(|position| position.quantity.is_zero()));
    }
    let balances_a = exchange_a
        .state()
        .account_balances("test-key")
        .await
        .expect("balances");
    let usdt = balances_a.iter().find(|b| b.asset == usdt_asset()).unwrap();
    assert_eq!(usdt.available, Decimal::new(10_001, 0));
    let balances_b = exchange_b
        .state()
        .account_balances("test-key")
        .await
        .expect("balances");
    let busdt = balances_b
        .iter()
        .find(|b| b.asset == binance_asset())
        .unwrap();
    assert_eq!(busdt.available, Decimal::new(10_001, 0));
    exchange_a.shutdown().await;
    exchange_b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn pairs_trading_executes_cross_exchange_round_trip() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let account_a = AccountConfig::new("test-key", "test-secret")
        .with_balance(account_balance(Decimal::new(10_000, 0)));
    let account_b = AccountConfig::new("test-key", "test-secret")
        .with_balance(binance_account_balance(Decimal::new(10_000, 0)));
    let base_time = Utc::now();
    let series_a = [100, 100, 100, 300, 110, 95, 101, 98];
    let series_b = [100, 100, 100, 100, 100, 100, 100, 100];
    let (candles_a, ticks_a) = build_price_series(test_symbol(), base_time, &series_a);
    let (candles_b, ticks_b) = build_price_series(binance_symbol(), base_time, &series_b);
    let config_a = MockExchangeConfig::new()
        .with_exchange(bybit_exchange())
        .with_account(account_a)
        .with_candles(candles_a)
        .with_ticks(ticks_a)
        .with_auto_fill(AutoFillConfig {
            delay: Duration::from_millis(25),
            price: Some(Decimal::new(10_000, 0)),
        });
    let config_b = MockExchangeConfig::new()
        .with_exchange(binance_exchange())
        .with_account(account_b)
        .with_candles(candles_b)
        .with_ticks(ticks_b)
        .with_auto_fill(AutoFillConfig {
            delay: Duration::from_millis(25),
            price: Some(Decimal::new(9_950, 0)),
        });
    let mut exchange_a = MockExchange::start(config_a).await?;
    let mut exchange_b = MockExchange::start(config_b).await?;
    let temp = tempdir()?;
    let state_path = temp.path().join("live_state.db");
    let markets_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../config/markets.toml");
    let alerting = AlertingConfig {
        max_drawdown: Decimal::new(2, 0),
        ..AlertingConfig::default()
    };
    let risk = RiskManagementConfig {
        max_drawdown: Decimal::new(2, 0),
        ..RiskManagementConfig::default()
    };
    let settings = LiveSessionSettings {
        category: PublicChannel::Linear,
        interval: Interval::OneMinute,
        quantity: Decimal::ONE,
        slippage_bps: Decimal::ZERO,
        fee_bps: Decimal::ZERO,
        history: 16,
        metrics_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        persistence: PersistenceSettings::new(PersistenceEngine::Sqlite, state_path.clone()),
        initial_balances: dual_initial_balances(),
        reporting_currency: usdt_asset(),
        markets_file: Some(markets_file),
        alerting,
        exec_backend: ExecutionBackend::Live,
        risk,
        reconciliation_interval: Duration::from_secs(30),
        reconciliation_threshold: Decimal::new(1, 1),
        orderbook_depth: 50,
        record_path: None,
        control_addr: "127.0.0.1:0".parse().unwrap(),
        panic_close: PanicCloseConfig::default(),
    };
    let exchanges = vec![
        NamedExchange {
            name: "bybit_linear".into(),
            config: ExchangeConfig {
                rest_url: exchange_a.rest_url(),
                ws_url: exchange_a.ws_url(),
                api_key: "test-key".into(),
                api_secret: "test-secret".into(),
                driver: "bybit".into(),
                params: JsonValue::Null,
            },
        },
        NamedExchange {
            name: "binance_perp".into(),
            config: ExchangeConfig {
                rest_url: exchange_b.rest_url(),
                ws_url: exchange_b.ws_url(),
                api_key: "test-key".into(),
                api_secret: "test-secret".into(),
                driver: "bybit".into(),
                params: JsonValue::Null,
            },
        },
    ];
    let mut strategy = PairsTradingArbitrage::default();
    let config_value: toml::Value = toml::from_str(
        r#"
        lookback = 4
        entry_z = 1.5
        exit_z = 0.8
        symbols = ["bybit_linear:BTCUSDT", "binance_perp:BTCUSDT"]
        "#,
    )?;
    strategy.configure(config_value)?;
    let symbols = strategy.subscriptions();
    let shutdown = ShutdownSignal::new();
    let run_handle = spawn_live_runtime(
        Box::new(strategy),
        symbols,
        exchanges,
        settings,
        shutdown.clone(),
    );
    wait_for_fill_count(&exchange_a, 2, Duration::from_secs(10)).await?;
    wait_for_fill_count(&exchange_b, 2, Duration::from_secs(10)).await?;
    tokio::try_join!(
        wait_for_positions_flat(&exchange_a, Duration::from_secs(10)),
        wait_for_positions_flat(&exchange_b, Duration::from_secs(10))
    )?;
    shutdown.trigger();
    run_handle.await??;
    exchange_a.shutdown().await;
    exchange_b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn control_plane_reports_status() -> Result<()> {
    let account = AccountConfig::new("test-key", "test-secret")
        .with_balance(account_balance(Decimal::new(10_000, 0)));
    let candles = vec![Candle {
        symbol: test_symbol(),
        interval: Interval::OneMinute,
        open: Decimal::new(1_000, 0),
        high: Decimal::new(1_010, 0),
        low: Decimal::new(995, 0),
        close: Decimal::new(1_005, 0),
        volume: Decimal::ONE,
        timestamp: Utc::now(),
    }];
    let ticks = vec![Tick {
        symbol: test_symbol(),
        price: Decimal::new(1_005, 0),
        size: Decimal::ONE,
        side: Side::Buy,
        exchange_timestamp: Utc::now(),
        received_at: Utc::now(),
    }];
    let config = MockExchangeConfig::new()
        .with_exchange(bybit_exchange())
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
        initial_balances: default_initial_balances(),
        reporting_currency: usdt_asset(),
        markets_file: Some(markets_file),
        alerting: AlertingConfig::default(),
        exec_backend: ExecutionBackend::Live,
        risk: RiskManagementConfig::default(),
        reconciliation_interval: Duration::from_secs(1),
        reconciliation_threshold: Decimal::new(1, 3),
        orderbook_depth: 50,
        record_path: None,
        control_addr,
        panic_close: PanicCloseConfig::default(),
    };
    let exchange_cfg = ExchangeConfig {
        rest_url: exchange.rest_url(),
        ws_url: exchange.ws_url(),
        api_key: "test-key".into(),
        api_secret: "test-secret".into(),
        driver: "bybit".into(),
        params: JsonValue::Null,
    };
    let exchanges = vec![NamedExchange {
        name: "bybit_linear".into(),
        config: exchange_cfg,
    }];
    let strategy: Box<dyn Strategy> = Box::new(PassiveStrategy::new(test_symbol()));
    let shutdown = ShutdownSignal::new();
    let run_handle = spawn_live_runtime(
        strategy,
        vec![test_symbol()],
        exchanges,
        settings,
        shutdown.clone(),
    );

    let mut client = connect_control_client(control_addr).await?;
    let status = client.get_status(GetStatusRequest {}).await?.into_inner();
    assert!(!status.shutdown);
    assert_eq!(status.active_algorithms, 0);

    let portfolio = client
        .get_portfolio(GetPortfolioRequest {})
        .await?
        .into_inner();
    let snapshot = portfolio.portfolio.expect("snapshot");
    assert_eq!(snapshot.reporting_currency, usdt_asset().to_string());
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
    let account = AccountConfig::new("test-key", "test-secret")
        .with_balance(account_balance(Decimal::new(10_000, 0)));
    let base_time = Utc::now();
    let candles = (0..3)
        .map(|i| Candle {
            symbol: test_symbol(),
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
            symbol: test_symbol(),
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
        initial_balances: default_initial_balances(),
        reporting_currency: usdt_asset(),
        markets_file: Some(markets_file),
        alerting,
        exec_backend: ExecutionBackend::Live,
        risk: RiskManagementConfig::default(),
        reconciliation_interval: Duration::from_millis(200),
        reconciliation_threshold: Decimal::new(1, 4),
        orderbook_depth: 50,
        record_path: None,
        control_addr: "127.0.0.1:0".parse().unwrap(),
        panic_close: PanicCloseConfig::default(),
    };
    let exchange_cfg = ExchangeConfig {
        rest_url: exchange.rest_url(),
        ws_url: exchange.ws_url(),
        api_key: "test-key".into(),
        api_secret: "test-secret".into(),
        driver: "bybit".into(),
        params: JsonValue::Null,
    };
    let exchanges = vec![NamedExchange {
        name: "bybit_linear".into(),
        config: exchange_cfg,
    }];
    let strategy: Box<dyn Strategy> = Box::new(PassiveStrategy::new(test_symbol()));
    let shutdown = ShutdownSignal::new();
    let run_handle = spawn_live_runtime(
        strategy,
        vec![test_symbol()],
        exchanges,
        settings,
        shutdown.clone(),
    );

    sleep(Duration::from_millis(300)).await;
    exchange
        .state()
        .with_account_mut("test-key", |account| {
            account.positions.insert(
                test_symbol(),
                Position {
                    symbol: test_symbol(),
                    side: Some(Side::Buy),
                    quantity: Decimal::new(5, 0),
                    entry_price: Some(Decimal::new(1_000, 0)),
                    unrealized_pnl: Decimal::ZERO,
                    updated_at: Utc::now(),
                },
            );
            if let Some(balance) = account.balances.get_mut(&usdt_asset()) {
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
    let account = AccountConfig::new("test-key", "test-secret")
        .with_balance(account_balance(Decimal::new(10_000, 0)));
    let base_time = Utc::now();
    let candles = (0..2)
        .map(|i| Candle {
            symbol: test_symbol(),
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
            symbol: test_symbol(),
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
        .with_exchange(bybit_exchange())
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
        initial_balances: default_initial_balances(),
        reporting_currency: usdt_asset(),
        markets_file: Some(markets_file),
        alerting,
        exec_backend: ExecutionBackend::Live,
        risk: RiskManagementConfig::default(),
        reconciliation_interval: Duration::from_secs(1),
        reconciliation_threshold: Decimal::new(1, 3),
        orderbook_depth: 50,
        record_path: None,
        control_addr: "127.0.0.1:0".parse().unwrap(),
        panic_close: PanicCloseConfig::default(),
    };
    let exchange_cfg = ExchangeConfig {
        rest_url: exchange.rest_url(),
        ws_url: exchange.ws_url(),
        api_key: "test-key".into(),
        api_secret: "test-secret".into(),
        driver: "bybit".into(),
        params: JsonValue::Null,
    };
    let exchanges = vec![NamedExchange {
        name: "bybit_linear".into(),
        config: exchange_cfg,
    }];
    let (strategy, monitor) = ScriptedStrategy::new(test_symbol());
    let shutdown = ShutdownSignal::new();
    let run_handle = spawn_live_runtime(
        Box::new(strategy),
        vec![test_symbol()],
        exchanges,
        settings,
        shutdown.clone(),
    );

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
    symbol: Symbol,
    stage: usize,
    pending: Vec<Signal>,
    state: Arc<StrategyState>,
}

struct MultiScriptedStrategy {
    symbols: Vec<Symbol>,
    stages: HashMap<Symbol, usize>,
    pending: Vec<Signal>,
    state: Arc<StrategyState>,
}

struct PassiveStrategy {
    symbol: Symbol,
}

impl PassiveStrategy {
    fn new(symbol: Symbol) -> Self {
        Self { symbol }
    }
}

#[async_trait]
impl Strategy for PassiveStrategy {
    fn name(&self) -> &str {
        "passive-test"
    }

    fn symbol(&self) -> Symbol {
        self.symbol
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
    fn new(symbol: Symbol) -> (Self, StrategyMonitor) {
        let state = Arc::new(StrategyState {
            fills: AtomicUsize::new(0),
            notify: Notify::new(),
        });
        (
            Self {
                symbol,
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

    fn symbol(&self) -> Symbol {
        self.symbol
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
                .push(Signal::new(self.symbol, SignalKind::EnterLong, 1.0));
            self.stage = 1;
        } else if self.stage == 1 {
            self.pending
                .push(Signal::new(self.symbol, SignalKind::ExitLong, 1.0));
            self.stage = 2;
        }
        Ok(())
    }

    async fn on_fill(
        &mut self,
        _ctx: &StrategyContext,
        fill: &tesser_core::Fill,
    ) -> StrategyResult<()> {
        eprintln!("multi-scripted fill received for {}", fill.symbol);
        self.state.fills.fetch_add(1, Ordering::SeqCst);
        self.state.notify.notify_waiters();
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.pending)
    }
}

impl MultiScriptedStrategy {
    fn new(symbols: Vec<Symbol>) -> (Self, StrategyMonitor) {
        let state = Arc::new(StrategyState {
            fills: AtomicUsize::new(0),
            notify: Notify::new(),
        });
        let mut stages = HashMap::new();
        for symbol in &symbols {
            stages.insert(*symbol, 0);
        }
        (
            Self {
                symbols,
                stages,
                pending: Vec::new(),
                state: state.clone(),
            },
            StrategyMonitor { inner: state },
        )
    }
}

#[async_trait]
impl Strategy for MultiScriptedStrategy {
    fn name(&self) -> &str {
        "multi-scripted-test"
    }

    fn symbol(&self) -> Symbol {
        self.symbols[0]
    }

    fn subscriptions(&self) -> Vec<Symbol> {
        self.symbols.clone()
    }

    fn configure(&mut self, _params: toml::Value) -> StrategyResult<()> {
        Ok(())
    }

    async fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    async fn on_candle(&mut self, _ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if let Some(stage) = self.stages.get_mut(&candle.symbol) {
            match *stage {
                0 => {
                    self.pending
                        .push(Signal::new(candle.symbol, SignalKind::EnterLong, 1.0));
                    *stage = 1;
                }
                1 => {
                    self.pending
                        .push(Signal::new(candle.symbol, SignalKind::ExitLong, 1.0));
                    *stage = 2;
                }
                _ => {}
            }
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

    fn fills(&self) -> usize {
        self.inner.fills.load(Ordering::SeqCst)
    }
}
