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
    AccountBalance, Candle, DepthUpdate, Fill, LocalOrderBook, Order, OrderBook, OrderId,
    OrderRequest, OrderStatus, OrderType, Position, Price, Quantity, Side, Symbol, Tick,
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
impl ExecutionClient for MatchingEngine {
    fn info(&self) -> BrokerInfo {
        self.info.clone()
    }

    async fn place_order(&self, request: OrderRequest) -> BrokerResult<Order> {
        let now = Utc::now();
        let mut order = Order {
            id: Uuid::new_v4().to_string(),
            request: request.clone(),
            status: OrderStatus::PendingNew,
            filled_quantity: 0.0,
            avg_fill_price: None,
            created_at: now,
            updated_at: now,
        };

        match request.order_type {
            OrderType::Market => {
                let (slices, _) = self.match_market(&request).await?;
                self.handle_immediate_fills(&mut order, &slices).await?;
                Ok(order)
            }
            OrderType::Limit => {
                let limit_price = request.price.ok_or_else(|| {
                    BrokerError::InvalidRequest("limit order requires price".into())
                })?;
                if self.limit_crosses(request.side, limit_price) {
                    let (slices, _) = self.match_market(&request).await?;
                    self.handle_immediate_fills(&mut order, &slices).await?;
                    Ok(order)
                } else {
                    order.status = OrderStatus::Accepted;
                    order.updated_at = Utc::now();
                    {
                        let mut resting = self.resting_depth.lock().unwrap();
                        resting.add_order(request.side, limit_price, request.quantity);
                    }
                    self.record_resting_order(order.clone()).await;
                    Ok(order)
                }
            }
            OrderType::StopMarket => Err(BrokerError::InvalidRequest(
                "MatchingEngine does not yet support stop orders".into(),
            )),
        }
    }

    async fn cancel_order(&self, order_id: OrderId, _symbol: &str) -> BrokerResult<()> {
        let mut open = self.open_orders.lock().await;
        if let Some(resting) = open.remove(&order_id) {
            let mut book = self.resting_depth.lock().unwrap();
            book.remove_order(
                resting.order.request.side,
                resting.price,
                resting.remaining.max(f64::EPSILON),
            );
            Ok(())
        } else {
            Err(BrokerError::InvalidRequest(format!(
                "order {} not found",
                order_id
            )))
        }
    }

    async fn list_open_orders(&self, _symbol: &str) -> BrokerResult<Vec<Order>> {
        let open = self.open_orders.lock().await;
        Ok(open.values().map(|resting| resting.order.clone()).collect())
    }

    async fn account_balances(&self) -> BrokerResult<Vec<AccountBalance>> {
        Ok(self.balances.lock().await.clone())
    }

    async fn positions(&self) -> BrokerResult<Vec<Position>> {
        let positions = self.positions.lock().await;
        Ok(positions.values().cloned().collect())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Order-level metadata stored for maker orders managed by the matching engine.
#[derive(Clone)]
struct RestingOrder {
    order: Order,
    remaining: Quantity,
    price: Price,
}

/// Matching engine that consumes tick/L2 data for high-fidelity backtests.
///
/// Backtester integration guide:
/// - hydrate depth via [`load_market_snapshot`] and [`upsert_market_level`].
/// - feed tick prints into [`process_trade`] so maker orders can fill.
/// - drain generated fills through [`drain_fills`] and forward them to strategies.
///
///  CLI/backtester wiring for this engine will land in a follow-up change.
#[derive(Clone)]
pub struct MatchingEngine {
    info: BrokerInfo,
    market_depth: Arc<Mutex<LocalOrderBook>>,
    resting_depth: Arc<Mutex<LocalOrderBook>>,
    balances: Arc<AsyncMutex<Vec<AccountBalance>>>,
    positions: Arc<AsyncMutex<HashMap<Symbol, Position>>>,
    open_orders: Arc<AsyncMutex<HashMap<OrderId, RestingOrder>>>,
    fills: Arc<AsyncMutex<Vec<Fill>>>,
}

impl MatchingEngine {
    /// Build a new matching engine with the provided book metadata.
    pub fn new(name: impl Into<String>, markets: Vec<String>, initial_cash: Price) -> Self {
        let now = Utc::now();
        Self {
            info: BrokerInfo {
                name: name.into(),
                markets,
                supports_testnet: true,
            },
            market_depth: Arc::new(Mutex::new(LocalOrderBook::new())),
            resting_depth: Arc::new(Mutex::new(LocalOrderBook::new())),
            balances: Arc::new(AsyncMutex::new(vec![AccountBalance {
                currency: "USDT".into(),
                total: initial_cash,
                available: initial_cash,
                updated_at: now,
            }])),
            positions: Arc::new(AsyncMutex::new(HashMap::new())),
            open_orders: Arc::new(AsyncMutex::new(HashMap::new())),
            fills: Arc::new(AsyncMutex::new(Vec::new())),
        }
    }

    /// Replace the market depth snapshot with a fresh order book.
    pub fn load_market_snapshot(&self, snapshot: &OrderBook) {
        let mut depth = self.market_depth.lock().unwrap();
        let bids: Vec<(Price, Quantity)> = snapshot
            .bids
            .iter()
            .map(|level| (level.price, level.size))
            .collect();
        let asks: Vec<(Price, Quantity)> = snapshot
            .asks
            .iter()
            .map(|level| (level.price, level.size))
            .collect();
        depth.load_snapshot(&bids, &asks);
    }

    /// Apply an incremental depth update.
    pub fn upsert_market_level(&self, side: Side, price: Price, quantity: Quantity) {
        let mut depth = self.market_depth.lock().unwrap();
        depth.clear_level(side, price);
        if quantity > 0.0 {
            depth.add_order(side, price, quantity);
        }
    }

    /// Update the resting book maintained for our own limit orders (used when backfills occur).
    pub fn force_resting_level(&self, side: Side, price: Price, quantity: Quantity) {
        let mut book = self.resting_depth.lock().unwrap();
        if quantity <= 0.0 {
            book.remove_order(side, price, quantity.abs());
        } else {
            book.add_order(side, price, quantity);
        }
    }

    /// Apply a depth delta emitted by the historical dataset.
    pub fn apply_depth_update(&self, update: &DepthUpdate) {
        for level in &update.bids {
            self.upsert_market_level(Side::Buy, level.price, level.size);
        }
        for level in &update.asks {
            self.upsert_market_level(Side::Sell, level.price, level.size);
        }
    }

    /// Mid-price derived from the current best bid/ask.
    #[must_use]
    pub fn mid_price(&self) -> Option<Price> {
        let depth = self.market_depth.lock().unwrap();
        match (depth.best_bid(), depth.best_ask()) {
            (Some((bid, _)), Some((ask, _))) => Some((bid + ask) / 2.0),
            (Some((bid, _)), None) => Some(bid),
            (None, Some((ask, _))) => Some(ask),
            _ => None,
        }
    }

    /// Process a real market trade and attempt to fill maker orders that should have crossed.
    pub async fn process_trade(
        &self,
        aggressor_side: Side,
        price: Price,
        mut quantity: Quantity,
    ) -> Vec<Fill> {
        let mut generated = Vec::new();
        let eps = f64::EPSILON;
        if quantity <= eps {
            return generated;
        }

        let mut open = self.open_orders.lock().await;
        let mut finished = Vec::new();
        for (order_id, resting) in open.iter_mut() {
            if quantity <= eps {
                break;
            }
            if !Self::trade_crosses_order(aggressor_side, price, resting) {
                continue;
            }
            let trade_qty: Quantity = quantity.min(resting.remaining);
            resting.remaining -= trade_qty;
            quantity -= trade_qty;
            {
                let mut resting_book = self.resting_depth.lock().unwrap();
                resting_book.remove_order(resting.order.request.side, resting.price, trade_qty);
            }
            let fill = Self::build_fill(
                order_id,
                &resting.order.request.symbol,
                resting.order.request.side,
                price,
                trade_qty,
            );
            generated.push(fill.clone());
            let prev_qty = resting.order.filled_quantity;
            resting.order.filled_quantity += trade_qty;
            let prev_notional = resting.order.avg_fill_price.unwrap_or(0.0) * prev_qty;
            let new_notional = prev_notional + price * trade_qty;
            resting.order.avg_fill_price =
                Some(new_notional / resting.order.filled_quantity.max(f64::EPSILON));
            resting.order.updated_at = fill.timestamp;
            resting.order.status = if resting.remaining <= eps {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            };
            self.apply_fill_accounting(&fill).await;
            if resting.remaining <= eps {
                finished.push(order_id.clone());
            }
        }
        drop(open);

        if !finished.is_empty() {
            let mut open = self.open_orders.lock().await;
            for order_id in finished {
                open.remove(&order_id);
            }
        }

        let mut store = self.fills.lock().await;
        store.extend(generated.clone());
        generated
    }

    fn trade_crosses_order(side: Side, trade_price: Price, order: &RestingOrder) -> bool {
        match side {
            Side::Buy => order.order.request.side == Side::Sell && order.price <= trade_price,
            Side::Sell => order.order.request.side == Side::Buy && order.price >= trade_price,
        }
    }

    fn build_fill(order_id: &OrderId, symbol: &str, side: Side, price: Price, qty: Price) -> Fill {
        Fill {
            order_id: order_id.clone(),
            symbol: symbol.to_string(),
            side,
            fill_price: price,
            fill_quantity: qty,
            fee: None,
            timestamp: Utc::now(),
        }
    }

    async fn record_resting_order(&self, order: Order) {
        let mut open = self.open_orders.lock().await;
        let price = order.request.price.unwrap_or_default();
        open.insert(
            order.id.clone(),
            RestingOrder {
                price,
                remaining: order.request.quantity,
                order,
            },
        );
    }

    async fn apply_fill_accounting(&self, fill: &Fill) {
        let mut balances = self.balances.lock().await;
        if let Some(balance) = balances.iter_mut().find(|b| b.currency == "USDT") {
            let notional = fill.fill_price * fill.fill_quantity;
            match fill.side {
                Side::Buy => balance.available -= notional,
                Side::Sell => balance.available += notional,
            }
            balance.total = balance.available;
            balance.updated_at = fill.timestamp;
        }
        drop(balances);

        let mut positions = self.positions.lock().await;
        let position = positions.entry(fill.symbol.clone()).or_insert(Position {
            symbol: fill.symbol.clone(),
            side: Some(fill.side),
            quantity: 0.0,
            entry_price: Some(fill.fill_price),
            unrealized_pnl: 0.0,
            updated_at: fill.timestamp,
        });
        match position.side {
            Some(side) if side == fill.side => {
                let total_qty = position.quantity + fill.fill_quantity;
                let prev_cost = position
                    .entry_price
                    .map(|price| price * position.quantity)
                    .unwrap_or_default();
                let new_cost = fill.fill_price * fill.fill_quantity;
                position.entry_price = Some((prev_cost + new_cost) / total_qty.max(f64::EPSILON));
                position.quantity = total_qty;
            }
            Some(_) => {
                position.quantity -= fill.fill_quantity;
                if position.quantity <= f64::EPSILON {
                    position.side = None;
                    position.entry_price = None;
                    position.quantity = 0.0;
                }
            }
            None => {
                position.side = Some(fill.side);
                position.quantity = fill.fill_quantity;
                position.entry_price = Some(fill.fill_price);
            }
        }
        position.updated_at = fill.timestamp;
    }

    async fn match_market(
        &self,
        request: &OrderRequest,
    ) -> BrokerResult<(Vec<(Price, Quantity)>, Quantity)> {
        let mut depth = self.market_depth.lock().unwrap();
        let slices = depth.take_liquidity(request.side, request.quantity);
        drop(depth);
        if slices.is_empty() {
            return Err(BrokerError::Other(
                "insufficient market depth for matching engine".into(),
            ));
        }
        let total: Quantity = slices.iter().map(|(_, qty)| qty).sum();
        Ok((slices, total))
    }

    async fn handle_immediate_fills(
        &self,
        order: &mut Order,
        slices: &[(Price, Quantity)],
    ) -> BrokerResult<()> {
        let total_qty: Quantity = slices.iter().map(|(_, qty)| qty).sum();
        let notional: Price = slices.iter().map(|(price, qty)| price * qty).sum();
        order.filled_quantity = total_qty;
        order.avg_fill_price = Some(notional / total_qty.max(f64::EPSILON));
        order.status = if (order.request.quantity - total_qty).abs() <= f64::EPSILON {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };

        let mut realized_fills = Vec::new();
        for (price, qty) in slices {
            let fill = Self::build_fill(
                &order.id,
                &order.request.symbol,
                order.request.side,
                *price,
                *qty,
            );
            self.apply_fill_accounting(&fill).await;
            realized_fills.push(fill);
        }
        let mut store = self.fills.lock().await;
        store.extend(realized_fills);
        Ok(())
    }

    /// Drain fills generated by the matching engine since the previous call.
    pub async fn drain_fills(&self) -> Vec<Fill> {
        let mut store = self.fills.lock().await;
        let mut drained = Vec::new();
        std::mem::swap(&mut *store, &mut drained);
        drained
    }

    fn limit_crosses(&self, side: Side, price: Price) -> bool {
        let depth = self.market_depth.lock().unwrap();
        match side {
            Side::Buy => depth.best_ask().is_some_and(|(ask, _)| price >= ask),
            Side::Sell => depth.best_bid().is_some_and(|(bid, _)| price <= bid),
        }
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
