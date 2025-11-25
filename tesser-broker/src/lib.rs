//! Exchange-agnostic traits used by the rest of the framework.

use std::any::Any;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tesser_core::{
    AccountBalance, Candle, Instrument, Order, OrderBook, OrderId, OrderRequest,
    OrderUpdateRequest, Position, Signal, Symbol, Tick,
};
use thiserror::Error;

pub mod limiter;

pub use governor::Quota;
pub use limiter::{RateLimiter, RateLimiterError};

/// Convenience alias for broker results.
pub type BrokerResult<T> = Result<T, BrokerError>;

/// Common error type returned by broker implementations.
#[derive(Debug, Error)]
pub enum BrokerError {
    /// Represents transport-level failures (network, timeouts, etc.).
    #[error("transport error: {0}")]
    Transport(String),
    /// Returned when authentication fails or credentials are missing.
    #[error("authentication failed: {0}")]
    Authentication(String),
    /// Returned when the request parameters are invalid for the target exchange.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// Wraps serialization or parsing errors.
    #[error("serialization error: {0}")]
    Serialization(String),
    /// Exchange responded with a business error (e.g., insufficient margin).
    #[error("exchange error: {0}")]
    Exchange(String),
    /// A catch-all branch for other issues.
    #[error("unexpected error: {0}")]
    Other(String),
}

impl BrokerError {
    /// Helper used by connectors when mapping any error type into a broker error.
    pub fn from_display(err: impl std::fmt::Display, kind: BrokerErrorKind) -> Self {
        match kind {
            BrokerErrorKind::Transport => Self::Transport(err.to_string()),
            BrokerErrorKind::Authentication => Self::Authentication(err.to_string()),
            BrokerErrorKind::InvalidRequest => Self::InvalidRequest(err.to_string()),
            BrokerErrorKind::Serialization => Self::Serialization(err.to_string()),
            BrokerErrorKind::Exchange => Self::Exchange(err.to_string()),
            BrokerErrorKind::Other => Self::Other(err.to_string()),
        }
    }
}

/// Enumerates the broad families of broker errors.
#[derive(Debug, Clone, Copy)]
pub enum BrokerErrorKind {
    Transport,
    Authentication,
    InvalidRequest,
    Serialization,
    Exchange,
    Other,
}

/// Represents metadata describing the capabilities of a connector.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub name: String,
    pub markets: Vec<String>,
    pub supports_testnet: bool,
}

/// Trait describing data subscriptions (live WebSocket or replayed feeds).
#[async_trait]
pub trait MarketStream: Send + Sync {
    /// Exchange specific subscription descriptor.
    type Subscription: Send + Sync + Serialize;

    /// Human-friendly name of the connector used for logging purposes.
    fn name(&self) -> &str;

    /// Returns optional metadata describing the connector.
    fn info(&self) -> Option<&BrokerInfo> {
        None
    }

    /// Subscribe to a new stream (e.g., order book or trades).
    async fn subscribe(&mut self, subscription: Self::Subscription) -> BrokerResult<()>;

    /// Fetch the next tick in FIFO order.
    async fn next_tick(&mut self) -> BrokerResult<Option<Tick>>;

    /// Fetch the next candle when available.
    async fn next_candle(&mut self) -> BrokerResult<Option<Candle>>;

    /// Fetch the next order book snapshot.
    async fn next_order_book(&mut self) -> BrokerResult<Option<OrderBook>>;
}

/// Trait describing the execution interface (REST or WebSocket).
#[async_trait]
pub trait ExecutionClient: Send + Sync {
    /// Return metadata about the connector for telemetry.
    fn info(&self) -> BrokerInfo;

    /// Place a new order on the exchange.
    async fn place_order(&self, request: OrderRequest) -> BrokerResult<Order>;

    /// Cancel an existing order by identifier.
    async fn cancel_order(&self, order_id: OrderId, symbol: Symbol) -> BrokerResult<()>;

    /// Amend an existing order in-place when the exchange supports it.
    async fn amend_order(&self, request: OrderUpdateRequest) -> BrokerResult<Order>;

    /// Get all open orders for a symbol.
    async fn list_open_orders(&self, symbol: Symbol) -> BrokerResult<Vec<Order>>;

    /// Retrieve the latest known account balances.
    async fn account_balances(&self) -> BrokerResult<Vec<AccountBalance>>;

    /// Retrieve the current open positions.
    async fn positions(&self) -> BrokerResult<Vec<Position>>;

    /// Retrieve instrument metadata for the provided market category.
    async fn list_instruments(&self, category: &str) -> BrokerResult<Vec<Instrument>>;

    /// Helper for downcasting to a concrete type.
    fn as_any(&self) -> &dyn Any;
}

/// Strategy and execution share this event channel API during live trading and backtests.
#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// Publish an event of arbitrary payload to downstream consumers (e.g., CLI UI).
    async fn publish<T>(&self, topic: &str, payload: &T) -> BrokerResult<()>
    where
        T: Serialize + Send + Sync;
}

/// Provides access to historical market data snapshots.
#[async_trait]
pub trait HistoricalData {
    /// Fetch a chunk of historical candles.
    async fn candles(
        &self,
        symbol: &str,
        interval: tesser_core::Interval,
        limit: usize,
    ) -> BrokerResult<Vec<Candle>>;

    /// Fetch raw ticks, when supported by the exchange.
    async fn ticks(&self, symbol: &str, limit: usize) -> BrokerResult<Vec<Tick>>;
}

/// Allows connectors to emit framework-native signals (e.g., risk alerts).
#[async_trait]
pub trait SignalBus: Send + Sync {
    /// Send a signal downstream.
    async fn dispatch(&self, signal: Signal) -> BrokerResult<()>;
}

/// Helper trait used by connectors to (de)serialize exchange payloads.
pub trait PayloadExt: Sized {
    /// Deserialize JSON bytes into a strongly typed payload.
    fn from_json_bytes(bytes: &[u8]) -> BrokerResult<Self>
    where
        Self: DeserializeOwned,
    {
        serde_json::from_slice(bytes).map_err(|err| {
            BrokerError::Serialization(format!("failed to deserialize payload: {err}"))
        })
    }
}

impl<T> PayloadExt for T where T: DeserializeOwned {}

mod connector;
pub use connector::{
    get_connector_factory, register_connector_factory, registered_connectors, ConnectorFactory,
    ConnectorStream, ConnectorStreamConfig,
};
pub mod router;
pub use router::RouterExecutionClient;
