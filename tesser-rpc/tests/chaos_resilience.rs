use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use rust_decimal::Decimal;
use tesser_core::{Side, Tick};
use tesser_rpc::proto::strategy_service_server::{StrategyService, StrategyServiceServer};
use tesser_rpc::proto::{
    self, CandleRequest, FillRequest, HeartbeatRequest, HeartbeatResponse, InitRequest,
    InitResponse, OrderBookRequest, Signal, SignalList, TickRequest,
};
use tesser_rpc::RpcStrategy;
use tesser_strategy::{Strategy, StrategyContext};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Notify};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};

#[derive(Clone)]
struct ResilientMockStrategy {
    init_count: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl StrategyService for ResilientMockStrategy {
    async fn initialize(
        &self,
        _request: Request<InitRequest>,
    ) -> Result<Response<InitResponse>, Status> {
        self.init_count.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(InitResponse {
            symbols: vec!["BTC-USD".to_string()],
            success: true,
            error_message: String::new(),
        }))
    }

    async fn on_tick(
        &self,
        _request: Request<TickRequest>,
    ) -> Result<Response<SignalList>, Status> {
        let generation = self.init_count.load(Ordering::SeqCst);
        let note = format!("gen_{}", generation);
        let signal = Signal {
            symbol: "BTC-USD".to_string(),
            kind: proto::signal::Kind::EnterLong as i32,
            confidence: 1.0,
            stop_loss: None,
            take_profit: None,
            execution_hint: None,
            note,
            id: format!("mock-signal-{}", generation),
            generated_at: Some(prost_types::Timestamp {
                seconds: 0,
                nanos: 0,
            }),
            metadata: String::new(),
        };

        Ok(Response::new(SignalList {
            signals: vec![signal],
        }))
    }

    async fn on_candle(
        &self,
        _request: Request<CandleRequest>,
    ) -> Result<Response<SignalList>, Status> {
        Ok(Response::new(SignalList::default()))
    }

    async fn on_order_book(
        &self,
        _request: Request<OrderBookRequest>,
    ) -> Result<Response<SignalList>, Status> {
        Ok(Response::new(SignalList::default()))
    }

    async fn on_fill(
        &self,
        _request: Request<FillRequest>,
    ) -> Result<Response<SignalList>, Status> {
        Ok(Response::new(SignalList::default()))
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        Ok(Response::new(HeartbeatResponse {
            healthy: true,
            status_msg: "ok".to_string(),
        }))
    }
}

struct TestServerController {
    addr: SocketAddr,
    shutdown_tx: broadcast::Sender<()>,
    init_counter: Arc<AtomicUsize>,
    ready: Arc<Notify>,
    generation: Arc<AtomicUsize>,
}

impl TestServerController {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, _) = broadcast::channel(4);
        let controller = Self {
            addr,
            shutdown_tx,
            init_counter: Arc::new(AtomicUsize::new(0)),
            ready: Arc::new(Notify::new()),
            generation: Arc::new(AtomicUsize::new(0)),
        };
        controller.spawn_with_listener(listener);
        controller.wait_for_generation(1).await;
        controller
    }

    fn spawn_with_listener(&self, listener: TcpListener) {
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let service = ResilientMockStrategy {
            init_count: self.init_counter.clone(),
        };
        let ready = self.ready.clone();
        let generation = self.generation.clone();

        tokio::spawn(async move {
            generation.fetch_add(1, Ordering::SeqCst);
            ready.notify_waiters();
            if let Err(err) = Server::builder()
                .add_service(StrategyServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_rx.recv().await;
                })
                .await
            {
                eprintln!("mock server terminated: {err}");
            }
        });
    }

    async fn wait_for_generation(&self, target: usize) {
        loop {
            if self.generation.load(Ordering::SeqCst) >= target {
                break;
            }
            self.ready.notified().await;
        }
    }

    async fn kill(&self) {
        let _ = self.shutdown_tx.send(());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    async fn restart(&self) {
        let listener = TcpListener::bind(self.addr).await.unwrap();
        let target = self.generation.load(Ordering::SeqCst) + 1;
        self.spawn_with_listener(listener);
        self.wait_for_generation(target).await;
    }
}

fn build_tick() -> Tick {
    Tick {
        symbol: "BTC-USD".to_string(),
        price: Decimal::from(100),
        size: Decimal::from(1),
        side: Side::Buy,
        exchange_timestamp: Utc::now(),
        received_at: Utc::now(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chaos_test_reconnection_lifecycle() {
    let controller = TestServerController::new().await;
    let mut strategy = RpcStrategy::default();
    let config_toml = format!(
        "transport = \"grpc\"\nendpoint = \"http://{}\"\nheartbeat_interval_ms = 200\n",
        controller.addr
    );
    let config: toml::Value = config_toml.parse().unwrap();
    strategy.configure(config).unwrap();

    let ctx = StrategyContext::default();

    strategy.on_tick(&ctx, &build_tick()).await.unwrap();
    let mut signals = strategy.drain_signals();
    assert_eq!(signals.len(), 1);
    assert_eq!(signals[0].note.as_deref(), Some("gen_1"));
    assert_eq!(controller.init_counter.load(Ordering::SeqCst), 1);

    controller.kill().await;

    strategy.on_tick(&ctx, &build_tick()).await.unwrap();
    signals = strategy.drain_signals();
    assert!(signals.is_empty());

    controller.restart().await;

    strategy.on_tick(&ctx, &build_tick()).await.unwrap();
    signals = strategy.drain_signals();
    assert_eq!(signals.len(), 1);
    assert_eq!(signals[0].note.as_deref(), Some("gen_2"));
    assert_eq!(controller.init_counter.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn config_validation_rejects_bad_urls() {
    let mut strategy = RpcStrategy::default();
    let config: toml::Value = r#"
        transport = "grpc"
        endpoint = "not_a_url"
    "#
    .parse()
    .unwrap();

    let result = strategy.configure(config);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("invalid gRPC endpoint URL"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filters_symbols_locally() {
    let controller = TestServerController::new().await;
    let mut strategy = RpcStrategy::default();
    let config_toml = format!(
        "transport = \"grpc\"\nendpoint = \"http://{}\"\nsymbols = [\"ETH-USD\"]\n",
        controller.addr
    );
    let config: toml::Value = config_toml.parse().unwrap();
    strategy.configure(config).unwrap();

    let ctx = StrategyContext::default();

    let mut btc_tick = build_tick();
    btc_tick.symbol = "BTC-USD".to_string();
    strategy.on_tick(&ctx, &btc_tick).await.unwrap();
    assert!(strategy.drain_signals().is_empty());
    assert_eq!(controller.init_counter.load(Ordering::SeqCst), 0);

    let mut eth_tick = build_tick();
    eth_tick.symbol = "ETH-USD".to_string();
    strategy.on_tick(&ctx, &eth_tick).await.unwrap();
    let signals = strategy.drain_signals();
    assert_eq!(signals.len(), 1);
    assert_eq!(controller.init_counter.load(Ordering::SeqCst), 1);
}
