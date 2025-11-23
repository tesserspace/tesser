#![cfg(feature = "bybit")]

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use chrono::{Duration as ChronoDuration, Utc};
use rust_decimal::Decimal;
use tempfile::tempdir;
use tokio::sync::{oneshot, Notify};
use tokio::time::{sleep, timeout};
use tonic::transport::Channel;

use tesser_cli::live::{
    run_live_with_shutdown, ExecutionBackend, LiveSessionSettings, PersistenceSettings,
    ShutdownSignal,
};
use tesser_cli::PublicChannel;
use tesser_config::{AlertingConfig, ExchangeConfig, PersistenceEngine, RiskManagementConfig};
use tesser_core::{
    AccountBalance, Candle, ExecutionHint, Interval, Side, Signal, SignalKind, Tick,
};
use tesser_rpc::proto::control_service_client::ControlServiceClient;
use tesser_rpc::proto::{event::Payload, MonitorRequest};
use tesser_strategy::{Strategy, StrategyContext, StrategyResult};
use tesser_test_utils::{AccountConfig, MockExchange, MockExchangeConfig};

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

struct SignalStrategy {
    symbol: String,
    emitted: bool,
    pending: Vec<Signal>,
    gate_passed: bool,
    ready: Arc<Notify>,
}

impl SignalStrategy {
    fn new(symbol: &str, ready: Arc<Notify>) -> Self {
        Self {
            symbol: symbol.to_string(),
            emitted: false,
            pending: Vec::new(),
            gate_passed: false,
            ready,
        }
    }
}

#[async_trait::async_trait]
impl Strategy for SignalStrategy {
    fn name(&self) -> &str {
        "signal-test"
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn configure(&mut self, _params: toml::Value) -> StrategyResult<()> {
        Ok(())
    }

    async fn on_tick(&mut self, _ctx: &StrategyContext, _tick: &Tick) -> StrategyResult<()> {
        if !self.gate_passed {
            self.ready.notified().await;
            self.gate_passed = true;
        }
        if !self.emitted {
            self.emitted = true;
            let mut signal = Signal::new(&self.symbol, SignalKind::EnterLong, 0.73);
            signal.stop_loss = Some(Decimal::new(99_000, 0));
            signal.take_profit = Some(Decimal::new(101_500, 0));
            signal.note = Some("integration-monitor".into());
            signal.execution_hint = Some(ExecutionHint::Twap {
                duration: ChronoDuration::seconds(30),
            });
            println!("strategy queued signal id={}", signal.id);
            self.pending.push(signal);
        }
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
        std::mem::take(&mut self.pending)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn monitor_streams_signal_events_and_records() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
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
        low: Decimal::new(990, 0),
        close: Decimal::new(1_005, 0),
        volume: Decimal::ONE,
        timestamp: Utc::now(),
    }];

    let ticks = (0..5)
        .map(|i| Tick {
            symbol: SYMBOL.into(),
            price: Decimal::new(1_005 + i, 0),
            size: Decimal::ONE,
            side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
            exchange_timestamp: Utc::now(),
            received_at: Utc::now(),
        })
        .collect::<Vec<_>>();

    let config = MockExchangeConfig::new()
        .with_account(account)
        .with_candles(candles)
        .with_ticks(ticks);
    let mut exchange = MockExchange::start(config).await?;

    let exchange_cfg = ExchangeConfig {
        rest_url: exchange.rest_url(),
        ws_url: exchange.ws_url(),
        api_key: "test-key".into(),
        api_secret: "test-secret".into(),
        driver: "bybit".into(),
        params: serde_json::Value::Null,
    };

    let temp = tempdir()?;
    let state_path = temp.path().join("live_state.db");
    let record_root = temp.path().join("flight_recorder");
    let markets_file = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../config/markets.toml");
    let control_addr = next_control_addr();
    let settings = LiveSessionSettings {
        category: PublicChannel::Linear,
        interval: Interval::OneMinute,
        quantity: Decimal::ONE,
        slippage_bps: Decimal::ZERO,
        fee_bps: Decimal::ZERO,
        history: 4,
        metrics_addr: "127.0.0.1:0".parse().unwrap(),
        persistence: PersistenceSettings::new(PersistenceEngine::Sqlite, state_path.clone()),
        initial_balances: HashMap::from([(String::from("USDT"), Decimal::new(10_000, 0))]),
        reporting_currency: "USDT".into(),
        markets_file: Some(markets_file),
        alerting: AlertingConfig::default(),
        exec_backend: ExecutionBackend::Live,
        risk: RiskManagementConfig::default(),
        reconciliation_interval: Duration::from_secs(5),
        reconciliation_threshold: Decimal::new(1, 3),
        driver: "bybit".into(),
        orderbook_depth: 50,
        record_path: Some(record_root.clone()),
        control_addr,
    };

    let ready = Arc::new(Notify::new());
    let strategy: Box<dyn Strategy> = Box::new(SignalStrategy::new(SYMBOL, ready.clone()));
    let shutdown = ShutdownSignal::new();
    let run_handle = tokio::spawn(run_live_with_shutdown(
        strategy,
        vec![SYMBOL.to_string()],
        exchange_cfg,
        settings,
        shutdown.clone(),
    ));

    let mut client = connect_control_client(control_addr).await?;
    let mut stream = client.monitor(MonitorRequest {}).await?.into_inner();
    ready.notify_waiters();

    let (signal_tx, signal_rx) = oneshot::channel();
    let monitor_handle = tokio::spawn(async move {
        loop {
            match stream.message().await {
                Ok(Some(event)) => match event.payload {
                    Some(Payload::Signal(sig)) => {
                        println!(
                            "monitor received signal id={} symbol={} confidence={}",
                            sig.id, sig.symbol, sig.confidence
                        );
                        let _ = signal_tx.send(Ok(sig));
                        break;
                    }
                    Some(other) => println!("received monitor event: {:?}", other),
                    None => println!("received monitor event with empty payload"),
                },
                Ok(None) => {
                    let _ = signal_tx.send(Err(tonic::Status::unknown("monitor stream closed")));
                    break;
                }
                Err(status) => {
                    let _ = signal_tx.send(Err(status));
                    break;
                }
            }
        }
    });

    let signal = timeout(Duration::from_secs(10), async {
        signal_rx
            .await
            .map_err(|_| anyhow!("monitor task dropped"))
            .and_then(|res| res.map_err(|status| anyhow!(status.to_string())))
    })
    .await
    .map_err(|_| anyhow!("timed out waiting for monitor signal"))??;

    assert_eq!(signal.symbol, SYMBOL);
    assert_eq!(signal.note, "integration-monitor");

    shutdown.trigger();
    let _ = monitor_handle.await;
    let _ = timeout(Duration::from_secs(10), run_handle)
        .await
        .map_err(|_| anyhow!("live run did not shut down in time"))??;
    exchange.shutdown().await;

    let mut found_signal_file = false;
    let signal_dir = record_root.join("signals");
    if signal_dir.exists() {
        for entry in std::fs::read_dir(&signal_dir)? {
            let entry = entry?;
            if entry.path().is_dir() {
                for file in std::fs::read_dir(entry.path())? {
                    let file = file?;
                    if file
                        .path()
                        .extension()
                        .map(|ext| ext == "parquet")
                        .unwrap_or(false)
                    {
                        found_signal_file = true;
                        break;
                    }
                }
            }
        }
    }
    assert!(
        found_signal_file,
        "flight recorder did not persist signal parquet file"
    );

    Ok(())
}
