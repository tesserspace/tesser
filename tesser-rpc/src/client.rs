use async_trait::async_trait;
use anyhow::Result;
use crate::proto::{
    InitRequest, InitResponse, TickRequest, CandleRequest, 
    SignalList, OrderBookRequest, FillRequest
};

/// Transport-agnostic interface for communicating with external strategies.
/// 
/// This allows swapping gRPC for Shared Memory, ZeroMQ, or other transports
/// without changing the core RpcStrategy logic.
#[async_trait]
pub trait RemoteStrategyClient: Send + Sync {
    /// Establishes the connection to the remote strategy.
    async fn connect(&mut self) -> Result<()>;
    
    /// Performs the initial handshake and configuration.
    async fn initialize(&mut self, req: InitRequest) -> Result<InitResponse>;

    /// Pushes a tick event.
    async fn on_tick(&self, req: TickRequest) -> Result<SignalList>;
    
    /// Pushes a candle event.
    async fn on_candle(&self, req: CandleRequest) -> Result<SignalList>;
    
    /// Pushes an order book snapshot.
    async fn on_order_book(&self, req: OrderBookRequest) -> Result<SignalList>;
    
    /// Pushes an execution fill.
    async fn on_fill(&self, req: FillRequest) -> Result<SignalList>;
}
