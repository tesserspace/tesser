//! Simple paper-trading connector used by the backtester.

use std::{
    any::Any,
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tesser_broker::{BrokerError, BrokerInfo, BrokerResult, ExecutionClient, MarketStream};
use tesser_core::{
    AccountBalance, Candle, Fill, Order, OrderBook, OrderId, OrderRequest, OrderStatus, Position,
    Price, Side, Symbol, Tick,
};
use tokio::sync::Mutex as AsyncMutex;
use tracing::info;
use uuid::Uuid;

/// In-memory execution client that fills orders immediately at the provided limit (or last) price.
#[derive(Clone)]
pub struct PaperExecutionClient {
    info: BrokerInfo,
    orders: Arc<AsyncMutex<Vec<Order>>>,
    balances: Arc<AsyncMutex<Vec<AccountBalance>>>,
    positions: Arc<AsyncMutex<Vec<Position>>>,
    pending_orders: Arc<AsyncMutex<Vec<Order>>>,
    /// Latest market prices for each symbol
    last_prices: Arc<Mutex<HashMap<Symbol, Price>>>,
    /// Simulation parameters
    slippage_bps: f64,
    fee_bps: f64,
}

impl Default for PaperExecutionClient {
    fn default() -> Self {
        Self::new("paper".into(), vec!["BTCUSDT".into()], 0.0, 0.0)
    }
}

impl PaperExecutionClient {
    /// Create a new paper execution client configurable with instrument metadata.
    pub fn new(name: String, markets: Vec<String>, slippage_bps: f64, fee_bps: f64) -> Self {
        Self {
            info: BrokerInfo {
                name,
                markets,
                supports_testnet: true,
            },
            orders: Arc::new(AsyncMutex::new(Vec::new())),
            balances: Arc::new(AsyncMutex::new(vec![AccountBalance {
                currency: "USDT".into(),
                total: 10_000.0,
                available: 10_000.0,
                updated_at: Utc::now(),
            }])),
            positions: Arc::new(AsyncMutex::new(Vec::new())),
            pending_orders: Arc::new(AsyncMutex::new(Vec::new())),
            last_prices: Arc::new(Mutex::new(HashMap::new())),
            slippage_bps,
            fee_bps,
        }
    }

    /// Update the latest market price for a symbol.
    pub fn update_price(&self, symbol: &Symbol, price: Price) {
        let mut prices = self.last_prices.lock().unwrap();
        prices.insert(symbol.clone(), price);
    }

    /// Create a Fill object from an order with proper fee calculation.
    fn create_fill_from_order(
        &self,
        order: &Order,
        fill_price: Price,
        timestamp: DateTime<Utc>,
    ) -> Fill {
        let fee = if self.fee_bps > 0.0 {
            let fee_rate = self.fee_bps / 10_000.0;
            Some(fill_price.abs() * order.request.quantity.abs() * fee_rate)
        } else {
            None
        };

        Fill {
            order_id: order.id.clone(),
            symbol: order.request.symbol.clone(),
            side: order.request.side,
            fill_price,
            fill_quantity: order.request.quantity,
            fee,
            timestamp,
        }
    }

    fn fill_order(&self, request: &OrderRequest) -> Order {
        let now = Utc::now();

        // Get the last known price for this symbol
        let last_price = {
            let prices = self.last_prices.lock().unwrap();
            prices.get(&request.symbol).copied()
        };

        // Determine the base fill price
        let base_price = match request.order_type {
            tesser_core::OrderType::Market => {
                // For market orders, use the last known market price or fallback to request price
                last_price.or(request.price).unwrap_or_else(|| {
                    tracing::warn!(
                        symbol = %request.symbol,
                        "No market price available for market order, using fallback price of 1.0"
                    );
                    1.0 // Emergency fallback
                })
            }
            tesser_core::OrderType::Limit | tesser_core::OrderType::StopMarket => {
                // For limit and stop orders, use the specified price
                request.price.unwrap_or_else(|| {
                    tracing::warn!(
                        symbol = %request.symbol,
                        "No price specified for limit/stop order, using fallback price of 1.0"
                    );
                    1.0 // Emergency fallback
                })
            }
        };

        // Apply slippage
        let slippage_rate = self.slippage_bps / 10_000.0;
        let fill_price = if slippage_rate > 0.0 {
            match request.side {
                Side::Buy => base_price * (1.0 + slippage_rate), // Buy at higher price
                Side::Sell => base_price * (1.0 - slippage_rate), // Sell at lower price
            }
        } else {
            base_price
        };

        Order {
            id: Uuid::new_v4().to_string(),
            request: request.clone(),
            status: OrderStatus::Filled,
            filled_quantity: request.quantity,
            avg_fill_price: Some(fill_price),
            created_at: now,
            updated_at: now,
        }
    }

    /// Inspect conditional orders and emit fills for any whose trigger price was reached.
    pub async fn check_triggers(&self, candle: &Candle) -> BrokerResult<Vec<Fill>> {
        use std::collections::{HashMap, HashSet};
        let mut triggered_fills = Vec::new();
        let mut pending = self.pending_orders.lock().await;
        let mut not_triggered: Vec<Order> = Vec::new();

        // Track pairs of (-sl, -tp) by shared base id
        #[derive(Default)]
        struct Pair {
            sl: Option<Order>,
            tp: Option<Order>,
        }
        let mut pairs: HashMap<String, Pair> = HashMap::new();
        let mut standalone_triggered: Vec<Order> = Vec::new();

        let suffix_base = |cid: &str| -> Option<(String, &'static str)> {
            if let Some(stripped) = cid.strip_suffix("-sl") {
                return Some((stripped.to_string(), "sl"));
            }
            if let Some(stripped) = cid.strip_suffix("-tp") {
                return Some((stripped.to_string(), "tp"));
            }
            None
        };

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
                if let Some(cid) = order.request.client_order_id.as_ref() {
                    if let Some((base, kind)) = suffix_base(cid) {
                        let entry = pairs.entry(base).or_default();
                        match kind {
                            "sl" => entry.sl = Some(order),
                            "tp" => entry.tp = Some(order),
                            _ => {}
                        }
                        continue;
                    }
                }
                // No recognizable suffix; treat as a standalone triggered conditional
                standalone_triggered.push(order);
            } else {
                not_triggered.push(order);
            }
        }

        // Resolve conflicts: if both sl and tp trigger within same candle, choose the more conservative SL
        let mut remove_bases: HashSet<String> = HashSet::new();
        for (base, pair) in pairs.into_iter() {
            match (pair.sl, pair.tp) {
                (Some(sl), Some(_tp)) => {
                    let fill_price = sl.request.trigger_price.unwrap_or(candle.open);
                    let fill = self.create_fill_from_order(&sl, fill_price, candle.timestamp);
                    triggered_fills.push(fill);
                    remove_bases.insert(base);
                }
                (Some(sl), None) => {
                    let fill_price = sl.request.trigger_price.unwrap_or(candle.open);
                    let fill = self.create_fill_from_order(&sl, fill_price, candle.timestamp);
                    triggered_fills.push(fill);
                }
                (None, Some(tp)) => {
                    let fill_price = tp.request.trigger_price.unwrap_or(candle.open);
                    let fill = self.create_fill_from_order(&tp, fill_price, candle.timestamp);
                    triggered_fills.push(fill);
                }
                (None, None) => {}
            }
        }

        // Standalone triggered orders
        for order in standalone_triggered.into_iter() {
            let fill_price = order.request.trigger_price.unwrap_or(candle.open);
            let fill = self.create_fill_from_order(&order, fill_price, candle.timestamp);
            triggered_fills.push(fill);
        }

        // Rebuild pending set, dropping any orders that share the base id of a conflict pair
        let mut still_pending = Vec::new();
        for order in not_triggered.into_iter() {
            let drop_for_conflict = order
                .request
                .client_order_id
                .as_ref()
                .and_then(|cid| suffix_base(cid))
                .map(|(base, _)| remove_bases.contains(&base))
                .unwrap_or(false);
            if !drop_for_conflict {
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
                let order = self.fill_order(&request);
                self.orders.lock().await.push(order.clone());

                // Calculate fee if applicable
                let fee = if self.fee_bps > 0.0 && order.avg_fill_price.is_some() {
                    let fill_price = order.avg_fill_price.unwrap();
                    let fee_rate = self.fee_bps / 10_000.0;
                    Some(fill_price * order.request.quantity * fee_rate)
                } else {
                    None
                };

                info!(
                    symbol = %order.request.symbol,
                    qty = order.request.quantity,
                    price = ?order.avg_fill_price,
                    fee = ?fee,
                    side = ?order.request.side,
                    "paper order filled"
                );
                Ok(order)
            }
            tesser_core::OrderType::StopMarket => {
                let trigger_price = request.trigger_price.ok_or_else(|| {
                    BrokerError::InvalidRequest("StopMarket order requires a trigger_price".into())
                })?;
                let mut order = self.fill_order(&request);
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

    async fn next_order_book(&mut self) -> BrokerResult<Option<OrderBook>> {
        Ok(None)
    }
}
