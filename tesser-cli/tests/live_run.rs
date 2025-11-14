use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use anyhow::Result;
use chrono::{Duration as ChronoDuration, Utc};
use rust_decimal::Decimal;
use tempfile::tempdir;
use tokio::sync::Notify;
use tokio::time::timeout;

use tesser_bybit::PublicChannel;
use tesser_cli::live::{
    run_live_with_shutdown, ExecutionBackend, LiveSessionSettings, ShutdownSignal,
};
use tesser_config::{AlertingConfig, ExchangeConfig, RiskManagementConfig};
use tesser_core::{AccountBalance, Candle, Interval, Side, Signal, SignalKind, Tick};
use tesser_strategy::{Strategy, StrategyContext, StrategyResult};
use tesser_test_utils::{
    AccountConfig, MockExchange, MockExchangeConfig, OrderFillStep, Scenario, ScenarioAction,
    ScenarioTrigger,
};

const SYMBOL: &str = "BTCUSDT";

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
    let settings = LiveSessionSettings {
        category: PublicChannel::Linear,
        interval: Interval::OneMinute,
        quantity: Decimal::ONE,
        slippage_bps: Decimal::ZERO,
        fee_bps: Decimal::ZERO,
        history: 8,
        metrics_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        state_path: temp.path().join("live_state.db"),
        initial_equity: Decimal::new(10_000, 0),
        alerting: AlertingConfig::default(),
        exec_backend: ExecutionBackend::Live,
        risk: RiskManagementConfig::default(),
    };
    let exchange_cfg = ExchangeConfig {
        rest_url: exchange.rest_url(),
        ws_url: exchange.ws_url(),
        api_key: "test-key".into(),
        api_secret: "test-secret".into(),
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

struct ScriptedStrategy {
    symbol: String,
    stage: usize,
    pending: Vec<Signal>,
    state: Arc<StrategyState>,
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

    fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        Ok(())
    }

    fn on_candle(&mut self, _ctx: &StrategyContext, _candle: &Candle) -> StrategyResult<()> {
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

    fn on_fill(&mut self, _ctx: &StrategyContext, _fill: &tesser_core::Fill) -> StrategyResult<()> {
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
