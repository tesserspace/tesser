#![cfg(feature = "bybit")]

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{Duration as ChronoDuration, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use tempfile::{tempdir, TempDir};
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
    AccountBalance, Candle, ExecutionHint, Interval, OrderBook, OrderBookLevel, Side, Signal,
    SignalKind, Tick,
};
use tesser_data::recorder::{ParquetRecorder, RecorderConfig};
use tesser_rpc::proto::control_service_client::ControlServiceClient;
use tesser_rpc::proto::{event::Payload, Event, MonitorRequest};
use tesser_strategy::{Strategy, StrategyContext, StrategyResult};
use tesser_test_utils::{AccountConfig, MockExchange, MockExchangeConfig};

const SYMBOL: &str = "BTCUSDT";

fn next_control_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind port");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr
}

fn find_parquet_file(dir: &Path) -> Option<PathBuf> {
    if !dir.exists() {
        return None;
    }
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        if let Ok(entries) = std::fs::read_dir(&path) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_dir() {
                        stack.push(entry.path());
                    } else if entry
                        .path()
                        .extension()
                        .map(|ext| ext == "parquet")
                        .unwrap_or(false)
                    {
                        return Some(entry.path());
                    }
                }
            }
        }
    }
    None
}

fn assert_list_of_levels(field: &arrow::datatypes::Field) {
    match field.data_type() {
        DataType::List(inner) => match inner.data_type() {
            DataType::Struct(children) => {
                assert_decimal_child(children, "price");
                assert_decimal_child(children, "size");
            }
            other => panic!(
                "expected struct items for {}, got {:?}",
                field.name(),
                other
            ),
        },
        other => panic!(
            "expected list of structs for column {}, got {:?}",
            field.name(),
            other
        ),
    }
}

fn assert_decimal_child(children: &arrow::datatypes::Fields, name: &str) {
    let (_, child) = children
        .find(name)
        .unwrap_or_else(|| panic!("missing '{name}' field"));
    match child.data_type() {
        DataType::Decimal128(_, _) => {}
        other => panic!("expected decimal for {name}, got {:?}", other),
    }
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
    emit_signal: bool,
}

impl SignalStrategy {
    fn new(symbol: &str, ready: Arc<Notify>, emit_signal: bool) -> Self {
        Self {
            symbol: symbol.to_string(),
            emitted: false,
            pending: Vec::new(),
            gate_passed: false,
            ready,
            emit_signal,
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
        if self.emit_signal && !self.emitted {
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
    let harness = LiveTestHarness::start(true, true).await?;

    let mut client = connect_control_client(harness.control_addr).await?;
    let mut stream = client.monitor(MonitorRequest {}).await?.into_inner();
    harness.ready.notify_waiters();

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

    harness.shutdown.trigger();
    let _ = monitor_handle.await;
    let record_root = harness.record_root.clone();
    let tempdir = harness.stop().await?;

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
    drop(tempdir);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn flight_recorder_writes_order_books() -> Result<()> {
    let temp = tempdir()?;
    let config = RecorderConfig {
        root: temp.path().to_path_buf(),
        max_buffered_rows: 1,
        flush_interval: Duration::from_millis(10),
        max_rows_per_file: 16,
        channel_capacity: 4,
    };
    let recorder = ParquetRecorder::spawn(config).await?;
    let handle = recorder.handle();
    handle.record_order_book(OrderBook {
        symbol: SYMBOL.into(),
        bids: vec![OrderBookLevel {
            price: Decimal::new(20_000, 0),
            size: Decimal::new(1, 0),
        }],
        asks: vec![OrderBookLevel {
            price: Decimal::new(20_010, 0),
            size: Decimal::new(2, 0),
        }],
        timestamp: Utc::now(),
        exchange_checksum: Some(123),
        local_checksum: Some(123),
    });
    sleep(Duration::from_millis(50)).await;
    drop(handle);
    recorder.shutdown().await?;

    let book_dir = temp.path().join("order_books");
    let file_path = find_parquet_file(&book_dir)
        .ok_or_else(|| anyhow!("flight recorder did not persist order book parquet file"))?;

    let file = std::fs::File::open(&file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema();
    let ts_field = schema
        .field_with_name("timestamp")
        .context("missing timestamp column")?;
    assert!(
        matches!(
            ts_field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        ),
        "unexpected timestamp type: {:?}",
        ts_field.data_type()
    );
    let symbol_field = schema
        .field_with_name("symbol")
        .context("missing symbol column")?;
    assert!(
        matches!(symbol_field.data_type(), DataType::Utf8),
        "unexpected symbol type: {:?}",
        symbol_field.data_type()
    );
    let bids_field = schema
        .field_with_name("bids")
        .context("missing bids column")?;
    let asks_field = schema
        .field_with_name("asks")
        .context("missing asks column")?;
    assert_list_of_levels(bids_field);
    assert_list_of_levels(asks_field);

    let mut reader = builder.build()?;
    let batch = reader
        .next()
        .transpose()?
        .expect("order book batch missing");
    assert!(batch.num_rows() > 0, "order book batch empty");
    drop(temp);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn monitor_streams_tick_events() -> Result<()> {
    let harness = LiveTestHarness::start(false, false).await?;
    let mut client = connect_control_client(harness.control_addr).await?;
    let mut stream = client.monitor(MonitorRequest {}).await?.into_inner();
    harness.ready.notify_waiters();

    let tick = timeout(Duration::from_secs(10), async {
        loop {
            match stream.message().await? {
                Some(event) => match event.payload {
                    Some(Payload::Tick(tick)) => return Ok::<_, tonic::Status>(tick),
                    Some(other) => {
                        println!("received monitor event: {:?}", other);
                    }
                    None => println!("received monitor event with empty payload"),
                },
                None => return Err(tonic::Status::unknown("monitor stream closed")),
            }
        }
    })
    .await
    .map_err(|_| anyhow!("timed out waiting for monitor tick"))??;

    assert_eq!(tick.symbol, SYMBOL);
    drop(stream);
    drop(client);
    let _ = harness.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn monitor_supports_multiple_subscribers() -> Result<()> {
    let harness = LiveTestHarness::start(false, false).await?;
    let mut client_a = connect_control_client(harness.control_addr).await?;
    let mut client_b = connect_control_client(harness.control_addr).await?;
    let stream_a = client_a.monitor(MonitorRequest {}).await?.into_inner();
    let stream_b = client_b.monitor(MonitorRequest {}).await?.into_inner();
    harness.ready.notify_waiters();

    async fn first_tick(mut stream: tonic::Streaming<Event>) -> Result<(), anyhow::Error> {
        loop {
            match stream.message().await {
                Ok(Some(event)) => match event.payload {
                    Some(Payload::Tick(tick)) => {
                        assert_eq!(tick.symbol, SYMBOL);
                        break Ok(());
                    }
                    _ => continue,
                },
                Ok(None) => break Err(anyhow!("monitor stream closed")),
                Err(status) => break Err(anyhow!(status.to_string())),
            }
        }
    }

    timeout(Duration::from_secs(10), first_tick(stream_a))
        .await
        .map_err(|_| anyhow!("subscriber A timed out"))??;
    timeout(Duration::from_secs(10), first_tick(stream_b))
        .await
        .map_err(|_| anyhow!("subscriber B timed out"))??;

    drop(client_a);
    drop(client_b);
    let _ = harness.stop().await?;
    Ok(())
}

struct LiveTestHarness {
    shutdown: ShutdownSignal,
    run_handle: tokio::task::JoinHandle<Result<()>>,
    control_addr: SocketAddr,
    record_root: PathBuf,
    exchange: MockExchange,
    ready: Arc<Notify>,
    tempdir: TempDir,
}

impl LiveTestHarness {
    async fn start(record_data: bool, emit_signal: bool) -> Result<Self> {
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
        let exchange = MockExchange::start(config).await?;

        let control_addr = next_control_addr();
        let temp = tempdir()?;
        let state_path = temp.path().join("live_state.db");
        let record_root = temp.path().join("flight_recorder");
        let ready = Arc::new(Notify::new());
        let strategy: Box<dyn Strategy> =
            Box::new(SignalStrategy::new(SYMBOL, ready.clone(), emit_signal));

        let exchange_cfg = ExchangeConfig {
            rest_url: exchange.rest_url(),
            ws_url: exchange.ws_url(),
            api_key: "test-key".into(),
            api_secret: "test-secret".into(),
            driver: "bybit".into(),
            params: JsonValue::Null,
        };

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
            markets_file: Some(
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../config/markets.toml"),
            ),
            alerting: AlertingConfig::default(),
            exec_backend: ExecutionBackend::Live,
            risk: RiskManagementConfig::default(),
            reconciliation_interval: Duration::from_secs(5),
            reconciliation_threshold: Decimal::new(1, 3),
            driver: "bybit".into(),
            orderbook_depth: 50,
            record_path: record_data.then(|| record_root.clone()),
            control_addr,
        };

        let shutdown = ShutdownSignal::new();
        let run_handle = tokio::spawn(run_live_with_shutdown(
            strategy,
            vec![SYMBOL.to_string()],
            exchange_cfg,
            settings,
            shutdown.clone(),
        ));

        Ok(Self {
            shutdown,
            run_handle,
            control_addr,
            record_root,
            exchange,
            ready,
            tempdir: temp,
        })
    }

    async fn stop(mut self) -> Result<TempDir> {
        self.shutdown.trigger();
        let run_handle = self.run_handle;
        let _ = timeout(Duration::from_secs(10), run_handle)
            .await
            .map_err(|_| anyhow!("live run did not shut down in time"))??;
        self.exchange.shutdown().await;
        Ok(self.tempdir)
    }
}
