//! Simple paper-trading connector used by the backtester.

use std::{any::Any, collections::VecDeque};

use async_trait::async_trait;
use chrono::Utc;
use tesser_broker::{BrokerError, BrokerInfo, BrokerResult, ExecutionClient, MarketStream};
use tesser_core::{
    AccountBalance, Candle, Fill, Order, OrderId, OrderRequest, OrderStatus, Position, Side,
    Symbol, Tick,
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
    pending_orders: AsyncMutex<Vec<Order>>,
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
            pending_orders: AsyncMutex::new(Vec::new()),
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

    /// Inspect conditional orders and emit fills for any whose trigger price was reached.
    pub async fn check_triggers(&self, candle: &Candle) -> BrokerResult<Vec<Fill>> {
        let mut triggered_fills = Vec::new();
        let mut pending = self.pending_orders.lock().await;
        let mut still_pending = Vec::new();

        for order in pending.drain(..) {
            let triggered = match order.request.side {
                Side::Buy => order
                    .request
                    .trigger_price
                    .is_some_and(|tp| candle.high >= tp),
                Side::Sell => order
                    .request
                    .trigger_price
                    .is_some_and(|tp| candle.low <= tp),
            };

            if triggered {
                let fill_price = order.request.trigger_price.unwrap_or(candle.open);
                triggered_fills.push(Fill {
                    order_id: order.id.clone(),
                    symbol: order.request.symbol.clone(),
                    side: order.request.side,
                    fill_price,
                    fill_quantity: order.request.quantity,
                    fee: None,
                    timestamp: candle.timestamp,
                });
            } else {
                still_pending.push(order);
            }
        }

        *pending = still_pending;
        Ok(triggered_fills)
    }
}

#[async_trait]
impl ExecutionClient for PaperExecutionClient {
    fn info(&self) -> BrokerInfo {
        self.info.clone()
    }

    async fn place_order(&self, request: OrderRequest) -> BrokerResult<Order> {
        match request.order_type {
            tesser_core::OrderType::Market | tesser_core::OrderType::Limit => {
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
            tesser_core::OrderType::StopMarket => {
                let trigger_price = request.trigger_price.ok_or_else(|| {
                    BrokerError::InvalidRequest("StopMarket order requires a trigger_price".into())
                })?;
                let mut order = Self::fill_order(&request);
                order.status = OrderStatus::PendingNew;
                order.filled_quantity = 0.0;
                order.avg_fill_price = None;
                self.pending_orders.lock().await.push(order.clone());
                info!(
                    symbol = %order.request.symbol,
                    qty = order.request.quantity,
                    trigger = trigger_price,
                    "paper conditional order placed"
                );
                Ok(order)
            }
        }
    }

    async fn cancel_order(&self, _order_id: OrderId, _symbol: &str) -> BrokerResult<()> {
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

    fn as_any(&self) -> &dyn Any {
        self
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
