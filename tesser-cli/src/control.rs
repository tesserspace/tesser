use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use tesser_core::ExitStrategy;
use tesser_events::{Event as RuntimeEvent, EventBus};
use tesser_execution::OrderOrchestrator;
use tesser_rpc::conversions::to_decimal_proto;
use tesser_rpc::proto::control_service_server::{ControlService, ControlServiceServer};
use tesser_rpc::proto::{
    self, CancelAllRequest, CancelAllResponse, Event, GetOpenOrdersRequest, GetOpenOrdersResponse,
    GetPortfolioRequest, GetPortfolioResponse, GetStatusRequest, GetStatusResponse,
    ListManagedTradesRequest, ListManagedTradesResponse, ManagedTradeInfo, MonitorRequest,
    OrderSnapshot, PortfolioSnapshot, UpdateTradeExitStrategyRequest,
    UpdateTradeExitStrategyResponse,
};
use tesser_strategy::PairTradeSnapshot;
use uuid::Uuid;

use crate::live::{OmsHandle, ShutdownSignal, StrategyHandle};

pub struct ControlPlaneComponents {
    pub oms: OmsHandle,
    pub orchestrator: Arc<OrderOrchestrator>,
    pub last_data_timestamp: Arc<AtomicI64>,
    pub event_bus: Arc<EventBus>,
    pub strategy: StrategyHandle,
    pub shutdown: ShutdownSignal,
}

/// Launch the Control Plane gRPC server alongside the live runtime.
pub fn spawn_control_plane(addr: SocketAddr, components: ControlPlaneComponents) -> JoinHandle<()> {
    let ControlPlaneComponents {
        oms,
        orchestrator,
        last_data_timestamp,
        event_bus,
        strategy,
        shutdown,
    } = components;
    let service = ControlGrpcService::new(
        oms,
        orchestrator,
        last_data_timestamp,
        event_bus,
        strategy,
        shutdown.clone(),
    );
    info!(%addr, "starting control plane gRPC server");
    tokio::spawn(async move {
        if let Err(err) = Server::builder()
            .add_service(ControlServiceServer::new(service))
            .serve_with_shutdown(addr, async move { shutdown.wait().await })
            .await
        {
            warn!(error = %err, "control plane server exited with error");
        }
    })
}

struct ControlGrpcService {
    oms: OmsHandle,
    orchestrator: Arc<OrderOrchestrator>,
    last_data_timestamp: Arc<AtomicI64>,
    event_bus: Arc<EventBus>,
    strategy: StrategyHandle,
    shutdown: ShutdownSignal,
}

impl ControlGrpcService {
    fn new(
        oms: OmsHandle,
        orchestrator: Arc<OrderOrchestrator>,
        last_data_timestamp: Arc<AtomicI64>,
        event_bus: Arc<EventBus>,
        strategy: StrategyHandle,
        shutdown: ShutdownSignal,
    ) -> Self {
        Self {
            oms,
            orchestrator,
            last_data_timestamp,
            event_bus,
            strategy,
            shutdown,
        }
    }

    fn last_data_timestamp(&self) -> Option<prost_types::Timestamp> {
        let secs = self.last_data_timestamp.load(Ordering::SeqCst);
        if secs <= 0 {
            return None;
        }
        Some(prost_types::Timestamp {
            seconds: secs,
            nanos: 0,
        })
    }

    async fn cancel_all_impl(&self) -> Result<(u32, u32)> {
        let algo_ids: Vec<_> = self
            .orchestrator
            .algorithm_statuses()
            .keys()
            .copied()
            .collect();
        let mut cancelled_algorithms = 0u32;
        for algo_id in algo_ids {
            match self.orchestrator.cancel_algo(&algo_id).await {
                Ok(_) => cancelled_algorithms += 1,
                Err(err) => warn!(algo = %algo_id, error = %err, "failed to cancel algorithm"),
            }
        }

        let open_orders = self.oms.open_orders().await;
        let client = self.orchestrator.execution_engine().client();
        let mut cancelled_orders = 0u32;
        for order in open_orders {
            let symbol = order.request.symbol;
            match client.cancel_order(order.id.clone(), symbol).await {
                Ok(_) => cancelled_orders += 1,
                Err(err) => warn!(order_id = %order.id, error = %err, "failed to cancel order"),
            }
        }
        Ok((cancelled_orders, cancelled_algorithms))
    }

    #[allow(clippy::result_large_err)]
    fn snapshot_to_proto(snapshot: PairTradeSnapshot) -> Result<ManagedTradeInfo, Status> {
        let exit_strategy_json = serde_json::to_string(&snapshot.exit_strategy)
            .map_err(|err| Status::internal(format!("failed to encode exit strategy: {err}")))?;
        Ok(ManagedTradeInfo {
            trade_id: snapshot.trade_id.to_string(),
            symbol_a: snapshot.symbols[0].to_string(),
            symbol_b: snapshot.symbols[1].to_string(),
            direction: format!("{:?}", snapshot.direction),
            entry_timestamp: Some(timestamp_from_datetime(snapshot.entry_timestamp)),
            entry_z: Some(to_decimal_proto(snapshot.entry_z_score)),
            candles_held: snapshot.candles_held,
            exit_strategy_json,
        })
    }
}

#[tonic::async_trait]
impl ControlService for ControlGrpcService {
    type MonitorStream = ReceiverStream<Result<Event, Status>>;

    async fn get_portfolio(
        &self,
        _request: Request<GetPortfolioRequest>,
    ) -> Result<Response<GetPortfolioResponse>, Status> {
        let snapshot: PortfolioSnapshot = self
            .oms
            .portfolio_state()
            .await
            .map(|state| state.into())
            .unwrap_or_default();
        Ok(Response::new(GetPortfolioResponse {
            portfolio: Some(snapshot),
        }))
    }

    async fn get_open_orders(
        &self,
        _request: Request<GetOpenOrdersRequest>,
    ) -> Result<Response<GetOpenOrdersResponse>, Status> {
        let orders = self.oms.open_orders().await;
        let proto_orders: Vec<OrderSnapshot> =
            orders.into_iter().map(OrderSnapshot::from).collect();
        Ok(Response::new(GetOpenOrdersResponse {
            orders: proto_orders,
        }))
    }

    async fn get_status(
        &self,
        _request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let summary = self.oms.status().await;
        let response = GetStatusResponse {
            shutdown: self.shutdown.triggered(),
            liquidate_only: summary.liquidate_only,
            active_algorithms: self.orchestrator.active_algorithms_count() as u32,
            last_data_timestamp: self.last_data_timestamp(),
            equity: Some(to_decimal_proto(summary.equity)),
        };
        Ok(Response::new(response))
    }

    async fn cancel_all(
        &self,
        _request: Request<CancelAllRequest>,
    ) -> Result<Response<CancelAllResponse>, Status> {
        match self.cancel_all_impl().await {
            Ok((orders, algos)) => Ok(Response::new(CancelAllResponse {
                cancelled_orders: orders,
                cancelled_algorithms: algos,
            })),
            Err(err) => Err(Status::internal(err.to_string())),
        }
    }

    async fn list_managed_trades(
        &self,
        _request: Request<ListManagedTradesRequest>,
    ) -> Result<Response<ListManagedTradesResponse>, Status> {
        let snapshots = self
            .strategy
            .list_managed_trades()
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let mut trades = Vec::with_capacity(snapshots.len());
        for snapshot in snapshots {
            trades.push(Self::snapshot_to_proto(snapshot)?);
        }
        Ok(Response::new(ListManagedTradesResponse { trades }))
    }

    async fn update_trade_exit_strategy(
        &self,
        request: Request<UpdateTradeExitStrategyRequest>,
    ) -> Result<Response<UpdateTradeExitStrategyResponse>, Status> {
        let payload = request.into_inner();
        let trade_id = Uuid::parse_str(&payload.trade_id)
            .map_err(|err| Status::invalid_argument(format!("invalid trade_id: {err}")))?;
        let new_strategy: ExitStrategy =
            serde_json::from_str(&payload.new_strategy_json).map_err(|err| {
                Status::invalid_argument(format!("invalid exit strategy json: {err}"))
            })?;
        self.strategy
            .update_exit_strategy(trade_id, new_strategy.clone())
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(UpdateTradeExitStrategyResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn monitor(
        &self,
        _request: Request<MonitorRequest>,
    ) -> Result<Response<Self::MonitorStream>, Status> {
        let mut stream = self.event_bus.subscribe();
        info!("monitor subscriber connected");
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            loop {
                match stream.recv().await {
                    Ok(event) => {
                        let label = event_label(&event);
                        debug!(kind = label, "monitor captured event");
                        if let Some(proto) = event_to_proto(event) {
                            if tx.send(Ok(proto)).await.is_err() {
                                warn!(kind = label, "monitor stream receiver dropped during send");
                                break;
                            } else {
                                debug!(kind = label, "monitor event forwarded to client");
                            }
                        } else {
                            debug!(kind = label, "monitor event skipped (no proto mapping)");
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(lag)) => {
                        warn!(lag, "monitor stream lagged; dropping events");
                        continue;
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn event_to_proto(event: RuntimeEvent) -> Option<proto::Event> {
    use tesser_rpc::proto::event::Payload;

    match event {
        RuntimeEvent::Tick(evt) => Some(proto::Event {
            payload: Some(Payload::Tick(evt.tick.into())),
        }),
        RuntimeEvent::Candle(evt) => Some(proto::Event {
            payload: Some(Payload::Candle(evt.candle.into())),
        }),
        RuntimeEvent::Signal(evt) => Some(proto::Event {
            payload: Some(Payload::Signal(evt.signal.into())),
        }),
        RuntimeEvent::Fill(evt) => Some(proto::Event {
            payload: Some(Payload::Fill(evt.fill.into())),
        }),
        RuntimeEvent::OrderUpdate(evt) => Some(proto::Event {
            payload: Some(Payload::Order(evt.order.into())),
        }),
        RuntimeEvent::OrderBook(book) => {
            debug!(symbol = %book.order_book.symbol, "monitor dropping order book event");
            None
        }
    }
}

fn event_label(event: &RuntimeEvent) -> &'static str {
    match event {
        RuntimeEvent::Tick(_) => "tick",
        RuntimeEvent::Candle(_) => "candle",
        RuntimeEvent::Signal(_) => "signal",
        RuntimeEvent::Fill(_) => "fill",
        RuntimeEvent::OrderUpdate(_) => "order",
        RuntimeEvent::OrderBook(_) => "order_book",
    }
}

fn timestamp_from_datetime(ts: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: ts.timestamp(),
        nanos: ts.timestamp_subsec_nanos() as i32,
    }
}
