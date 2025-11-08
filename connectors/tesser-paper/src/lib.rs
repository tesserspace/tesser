//! Simple paper-trading connector used by the backtester.

use std::collections::VecDeque;

use async_trait::async_trait;
use chrono::Utc;
use tesser_broker::{BrokerInfo, BrokerResult, ExecutionClient, MarketStream};
use tesser_core::{
    AccountBalance, Candle, Order, OrderId, OrderRequest, OrderStatus, Position, Symbol, Tick,
};
use tokio::sync::Mutex as AsyncMutex;
use tracing::info;
use uuid::Uuid;

/// In-memory execution client that fills orders immediately at the provided limit (or last) price.
pub struct PaperExecutionClient {
    info: BrokerInfo,
    orders: AsyncMutex<Vec<Order>>,
    balances: AsyncMutex<Vec<AccountBalance>>,
    positions: AsyncMutex<Vec<Position>>,
}

impl Default for PaperExecutionClient {
    fn default() -> Self {
        Self::new("paper".into(), vec!["BTCUSDT".into()])
    }
}

impl PaperExecutionClient {
    /// Create a new paper execution client configurable with instrument metadata.
    pub fn new(name: String, markets: Vec<String>) -> Self {
        Self {
            info: BrokerInfo {
                name,
                markets,
                supports_testnet: true,
            },
            orders: AsyncMutex::new(Vec::new()),
            balances: AsyncMutex::new(vec![AccountBalance {
                currency: "USDT".into(),
                total: 10_000.0,
                available: 10_000.0,
                updated_at: Utc::now(),
            }]),
            positions: AsyncMutex::new(Vec::new()),
        }
    }

    fn fill_order(request: &OrderRequest) -> Order {
        let now = Utc::now();
        Order {
            id: Uuid::new_v4().to_string(),
            request: request.clone(),
            status: OrderStatus::Filled,
            filled_quantity: request.quantity,
            avg_fill_price: request.price,
            created_at: now,
            updated_at: now,
        }
    }
}

#[async_trait]
impl ExecutionClient for PaperExecutionClient {
    fn info(&self) -> BrokerInfo {
        self.info.clone()
    }

    async fn place_order(&self, request: OrderRequest) -> BrokerResult<Order> {
        let order = Self::fill_order(&request);
        self.orders.lock().await.push(order.clone());
        info!(
            symbol = %order.request.symbol,
            qty = order.request.quantity,
            side = ?order.request.side,
            "paper order filled"
        );
        Ok(order)
    }

    async fn cancel_order(&self, _order_id: OrderId) -> BrokerResult<()> {
        Ok(())
    }

    async fn list_open_orders(&self, _symbol: &str) -> BrokerResult<Vec<Order>> {
        Ok(Vec::new())
    }

    async fn account_balances(&self) -> BrokerResult<Vec<AccountBalance>> {
        Ok(self.balances.lock().await.clone())
    }

    async fn positions(&self) -> BrokerResult<Vec<Position>> {
        Ok(self.positions.lock().await.clone())
    }
}

/// Deterministic data source backed by vectors of ticks/candles.
pub struct PaperMarketStream {
    ticks: VecDeque<Tick>,
    candles: VecDeque<Candle>,
    info: BrokerInfo,
}

impl PaperMarketStream {
    /// Build a stream from pre-loaded data.
    pub fn from_data(symbol: Symbol, ticks: Vec<Tick>, candles: Vec<Candle>) -> Self {
        Self {
            ticks: ticks.into(),
            candles: candles.into(),
            info: BrokerInfo {
                name: "paper-market".into(),
                markets: vec![symbol],
                supports_testnet: true,
            },
        }
    }
}

#[async_trait]
impl MarketStream for PaperMarketStream {
    type Subscription = ();

    fn name(&self) -> &str {
        &self.info.name
    }

    fn info(&self) -> Option<&BrokerInfo> {
        Some(&self.info)
    }

    async fn subscribe(&mut self, _subscription: Self::Subscription) -> BrokerResult<()> {
        Ok(())
    }

    async fn next_tick(&mut self) -> BrokerResult<Option<Tick>> {
        Ok(self.ticks.pop_front())
    }

    async fn next_candle(&mut self) -> BrokerResult<Option<Candle>> {
        Ok(self.candles.pop_front())
    }
}
