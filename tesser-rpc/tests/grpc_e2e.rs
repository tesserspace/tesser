use std::net::SocketAddr;

use chrono::Utc;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use tesser_core::{Candle, Fill, Interval, OrderBook, OrderBookLevel, Side, SignalKind, Tick};
use tesser_rpc::proto::strategy_service_server::{StrategyService, StrategyServiceServer};
use tesser_rpc::proto::{
    self, CandleRequest, FillRequest, InitRequest, InitResponse, OrderBookRequest, Signal,
    SignalList, TickRequest,
};
use tesser_rpc::RpcStrategy;
use tesser_strategy::{Strategy, StrategyContext};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};

#[derive(Default)]
struct MockStrategyService;

#[tonic::async_trait]
impl StrategyService for MockStrategyService {
    async fn initialize(
        &self,
        request: Request<InitRequest>,
    ) -> Result<Response<InitResponse>, Status> {
        let payload = request.into_inner();
        assert!(!payload.config_json.is_empty());
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
        Ok(Response::new(SignalList {
            signals: vec![build_signal(proto::signal::Kind::EnterLong, "tick")],
        }))
    }

    async fn on_candle(
        &self,
        _request: Request<CandleRequest>,
    ) -> Result<Response<SignalList>, Status> {
        Ok(Response::new(SignalList {
            signals: vec![build_signal(proto::signal::Kind::ExitLong, "candle")],
        }))
    }

    async fn on_order_book(
        &self,
        _request: Request<OrderBookRequest>,
    ) -> Result<Response<SignalList>, Status> {
        Ok(Response::new(SignalList {
            signals: vec![build_signal(proto::signal::Kind::EnterShort, "book")],
        }))
    }

    async fn on_fill(
        &self,
        _request: Request<FillRequest>,
    ) -> Result<Response<SignalList>, Status> {
        Ok(Response::new(SignalList {
            signals: vec![build_signal(proto::signal::Kind::Flatten, "fill")],
        }))
    }

    async fn heartbeat(
        &self,
        _request: Request<proto::HeartbeatRequest>,
    ) -> Result<Response<proto::HeartbeatResponse>, Status> {
        Ok(Response::new(proto::HeartbeatResponse {
            healthy: true,
            status_msg: "ok".to_string(),
        }))
    }
}

fn build_signal(kind: proto::signal::Kind, note: &str) -> Signal {
    Signal {
        symbol: "BTC-USD".to_string(),
        kind: kind as i32,
        confidence: 0.9,
        stop_loss: None,
        take_profit: None,
        execution_hint: None,
        note: note.to_string(),
    }
}

async fn spawn_server() -> (SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel();

    tokio::spawn(async move {
        Server::builder()
            .add_service(StrategyServiceServer::new(MockStrategyService))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = rx.await;
            })
            .await
            .unwrap();
    });

    (addr, tx)
}

fn build_tick() -> Tick {
    Tick {
        symbol: "BTC-USD".to_string(),
        price: Decimal::from(50_000),
        size: Decimal::from_f64(0.1).unwrap(),
        side: Side::Buy,
        exchange_timestamp: Utc::now(),
        received_at: Utc::now(),
    }
}

fn build_candle() -> Candle {
    Candle {
        symbol: "BTC-USD".to_string(),
        interval: Interval::OneMinute,
        open: Decimal::from(49_900),
        high: Decimal::from(50_100),
        low: Decimal::from(49_800),
        close: Decimal::from(50_050),
        volume: Decimal::from(1000),
        timestamp: Utc::now(),
    }
}

fn build_book() -> OrderBook {
    OrderBook {
        symbol: "BTC-USD".to_string(),
        bids: vec![OrderBookLevel {
            price: Decimal::from(50_000),
            size: Decimal::from_f64(1.2).unwrap(),
        }],
        asks: vec![OrderBookLevel {
            price: Decimal::from(50_010),
            size: Decimal::from_f64(1.1).unwrap(),
        }],
        timestamp: Utc::now(),
        exchange_checksum: None,
        local_checksum: None,
    }
}

fn build_fill() -> Fill {
    Fill {
        order_id: "order-123".to_string(),
        symbol: "BTC-USD".to_string(),
        side: Side::Sell,
        fill_price: Decimal::from(50_020),
        fill_quantity: Decimal::from_f64(0.2).unwrap(),
        fee: None,
        timestamp: Utc::now(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_strategy_handles_grpc_events() {
    let (addr, shutdown_tx) = spawn_server().await;
    let mut strategy = RpcStrategy::default();
    let config: toml::Value = format!(
        "transport = \"grpc\"\nendpoint = \"http://{}\"\ntimeout_ms = 500\n",
        addr
    )
    .parse()
    .unwrap();

    strategy.configure(config).unwrap();

    let ctx = StrategyContext::default();
    strategy.on_tick(&ctx, &build_tick()).await.unwrap();
    let mut signals = strategy.drain_signals();
    assert_eq!(signals.len(), 1);
    assert_eq!(signals[0].kind, SignalKind::EnterLong);
    assert_eq!(signals[0].note.as_deref(), Some("tick"));

    strategy.on_candle(&ctx, &build_candle()).await.unwrap();
    signals = strategy.drain_signals();
    assert_eq!(signals[0].kind, SignalKind::ExitLong);

    strategy.on_order_book(&ctx, &build_book()).await.unwrap();
    signals = strategy.drain_signals();
    assert_eq!(signals[0].kind, SignalKind::EnterShort);

    strategy.on_fill(&ctx, &build_fill()).await.unwrap();
    signals = strategy.drain_signals();
    assert_eq!(signals[0].kind, SignalKind::Flatten);
    assert_eq!(strategy.symbol(), "BTC-USD");
    assert_eq!(strategy.subscriptions(), vec!["BTC-USD".to_string()]);

    let _ = shutdown_tx.send(());
}
