//! Simple paper-trading connector used by the backtester.

mod conditional;

use std::{
    any::Any,
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, TimeZone, Utc};
use conditional::ConditionalOrderManager;
use csv::StringRecord;
use rand::{rngs::StdRng, Rng, SeedableRng};
use rand_distr::StandardNormal;
use rust_decimal::{
    prelude::{FromPrimitive, ToPrimitive},
    Decimal,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tesser_broker::{
    register_connector_factory, BrokerError, BrokerInfo, BrokerResult, ConnectorFactory,
    ConnectorStream, ConnectorStreamConfig, ExecutionClient, MarketStream,
};
use tesser_core::{
    AccountBalance, Candle, DepthUpdate, Fill, Instrument, Interval, LocalOrderBook, Order,
    OrderBook, OrderId, OrderRequest, OrderStatus, OrderType, OrderUpdateRequest, Position, Price,
    Quantity, Side, Symbol, Tick, TimeInForce,
};
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex as AsyncMutex},
    time::{interval, sleep, Duration as TokioDuration},
};
use tracing::{error, info};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum PaperMarketConfig {
    RandomWalk {
        #[serde(default = "default_start_price")]
        start_price: Decimal,
        #[serde(default = "default_volatility")]
        volatility: f64,
        #[serde(default = "default_tick_interval")]
        interval_ms: u64,
    },
    Replay {
        path: PathBuf,
        #[serde(default = "default_replay_speed")]
        speed: f64,
    },
}

impl Default for PaperMarketConfig {
    fn default() -> Self {
        Self::RandomWalk {
            start_price: default_start_price(),
            volatility: default_volatility(),
            interval_ms: default_tick_interval(),
        }
    }
}

impl PaperMarketConfig {
    fn initial_price(&self) -> Decimal {
        match self {
            Self::RandomWalk { start_price, .. } => *start_price,
            Self::Replay { .. } => Decimal::from(25_000),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PaperConnectorConfig {
    #[serde(default = "default_symbol")]
    pub symbol: Symbol,
    #[serde(default = "default_balance_currency")]
    pub balance_currency: String,
    #[serde(default = "default_initial_balance")]
    pub initial_balance: Decimal,
    #[serde(default)]
    pub slippage_bps: Decimal,
    #[serde(default)]
    pub fee_bps: Decimal,
    #[serde(default)]
    pub market: PaperMarketConfig,
}

impl Default for PaperConnectorConfig {
    fn default() -> Self {
        Self {
            symbol: default_symbol(),
            balance_currency: default_balance_currency(),
            initial_balance: default_initial_balance(),
            slippage_bps: Decimal::ZERO,
            fee_bps: Decimal::ZERO,
            market: PaperMarketConfig::default(),
        }
    }
}

impl PaperConnectorConfig {
    fn cache_key(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| self.symbol.clone())
    }
}

struct PaperRuntimeState {
    client: Arc<PaperExecutionClient>,
    config: PaperConnectorConfig,
    initialized: AsyncMutex<bool>,
}

impl PaperRuntimeState {
    fn new(config: PaperConnectorConfig) -> Self {
        let stream_name = format!("paper-{}", config.symbol.to_lowercase());
        let client = Arc::new(PaperExecutionClient::new(
            stream_name,
            vec![config.symbol.clone()],
            config.slippage_bps,
            config.fee_bps,
        ));
        Self {
            client,
            config,
            initialized: AsyncMutex::new(false),
        }
    }

    async fn ensure_initialized(&self) {
        let mut guard = self.initialized.lock().await;
        if *guard {
            return;
        }
        self.client
            .initialize_balance(&self.config.balance_currency, self.config.initial_balance)
            .await;
        self.client
            .update_price(&self.config.symbol, self.config.market.initial_price());
        *guard = true;
    }
}

#[derive(Default)]
pub struct PaperFactory {
    runtimes: RwLock<HashMap<String, Arc<PaperRuntimeState>>>,
}

impl PaperFactory {
    fn parse_config(&self, value: &Value) -> BrokerResult<PaperConnectorConfig> {
        serde_json::from_value(value.clone()).map_err(|err| {
            BrokerError::InvalidRequest(format!("invalid paper connector config: {err}"))
        })
    }

    fn runtime_for(&self, cfg: PaperConnectorConfig) -> Arc<PaperRuntimeState> {
        let key = cfg.cache_key();
        if let Some(runtime) = self.runtimes.read().unwrap().get(&key) {
            return runtime.clone();
        }
        let runtime = Arc::new(PaperRuntimeState::new(cfg));
        self.runtimes.write().unwrap().insert(key, runtime.clone());
        runtime
    }
}

#[async_trait]
impl ConnectorFactory for PaperFactory {
    fn name(&self) -> &str {
        "paper"
    }

    async fn create_execution_client(
        &self,
        config: &Value,
    ) -> BrokerResult<Arc<dyn ExecutionClient>> {
        let cfg = self.parse_config(config)?;
        let runtime = self.runtime_for(cfg);
        runtime.ensure_initialized().await;
        Ok(runtime.client.clone())
    }

    async fn create_market_stream(
        &self,
        config: &Value,
        stream_config: ConnectorStreamConfig,
    ) -> BrokerResult<Box<dyn ConnectorStream>> {
        let cfg = self.parse_config(config)?;
        let runtime = self.runtime_for(cfg.clone());
        runtime.ensure_initialized().await;
        let stream = LivePaperStream::new(
            cfg.symbol.clone(),
            cfg.market.clone(),
            runtime.client.clone(),
            stream_config.connection_status,
        )?;
        Ok(Box::new(stream))
    }
}

pub fn register_factory() {
    register_connector_factory(Arc::new(PaperFactory::default()));
}

fn default_symbol() -> Symbol {
    "BTCUSDT".to_string()
}

fn default_balance_currency() -> String {
    "USDT".into()
}

fn default_initial_balance() -> Decimal {
    Decimal::from(10_000)
}

fn default_start_price() -> Decimal {
    Decimal::from(25_000)
}

fn default_volatility() -> f64 {
    0.003
}

fn default_tick_interval() -> u64 {
    500
}

fn default_replay_speed() -> f64 {
    1.0
}

/// In-memory execution client that fills orders immediately at the provided limit (or last) price.
#[derive(Clone)]
pub struct PaperExecutionClient {
    info: BrokerInfo,
    orders: Arc<AsyncMutex<Vec<Order>>>,
    balances: Arc<AsyncMutex<Vec<AccountBalance>>>,
    positions: Arc<AsyncMutex<Vec<Position>>>,
    conditional_orders: Arc<AsyncMutex<ConditionalOrderManager>>,
    /// Latest market prices for each symbol
    last_prices: Arc<Mutex<HashMap<Symbol, Price>>>,
    /// Simulation parameters
    slippage_bps: Decimal,
    fee_bps: Decimal,
}

impl Default for PaperExecutionClient {
    fn default() -> Self {
        Self::new(
            "paper".into(),
            vec!["BTCUSDT".into()],
            Decimal::ZERO,
            Decimal::ZERO,
        )
    }
}

impl PaperExecutionClient {
    /// Create a new paper execution client configurable with instrument metadata.
    pub fn new(
        name: String,
        markets: Vec<String>,
        slippage_bps: Decimal,
        fee_bps: Decimal,
    ) -> Self {
        Self {
            info: BrokerInfo {
                name,
                markets,
                supports_testnet: true,
            },
            orders: Arc::new(AsyncMutex::new(Vec::new())),
            balances: Arc::new(AsyncMutex::new(vec![AccountBalance {
                currency: "USDT".into(),
                total: Decimal::from(10_000),
                available: Decimal::from(10_000),
                updated_at: Utc::now(),
            }])),
            positions: Arc::new(AsyncMutex::new(Vec::new())),
            conditional_orders: Arc::new(AsyncMutex::new(ConditionalOrderManager::new())),
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

    /// Reset the available balance to a configured amount.
    pub async fn initialize_balance(&self, currency: &str, amount: Decimal) {
        let mut balances = self.balances.lock().await;
        if let Some(entry) = balances.iter_mut().find(|b| b.currency == currency) {
            entry.total = amount;
            entry.available = amount;
            entry.updated_at = Utc::now();
        } else {
            balances.push(AccountBalance {
                currency: currency.into(),
                total: amount,
                available: amount,
                updated_at: Utc::now(),
            });
        }
    }

    /// Create a Fill object from an order with proper fee calculation.
    fn create_fill_from_order(
        &self,
        order: &Order,
        fill_price: Price,
        timestamp: DateTime<Utc>,
    ) -> Fill {
        let fee = if self.fee_bps > Decimal::ZERO {
            let fee_rate = (self.fee_bps.max(Decimal::ZERO)) / Decimal::from(10_000);
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
        let fallback_price = Decimal::ONE;
        let base_price = match request.order_type {
            tesser_core::OrderType::Market => {
                // For market orders, use the last known market price or fallback to request price
                last_price.or(request.price).unwrap_or_else(|| {
                    tracing::warn!(
                        symbol = %request.symbol,
                        "No market price available for market order, using fallback price of 1.0"
                    );
                    fallback_price
                })
            }
            tesser_core::OrderType::Limit | tesser_core::OrderType::StopMarket => {
                // For limit and stop orders, use the specified price
                request.price.unwrap_or_else(|| {
                    tracing::warn!(
                        symbol = %request.symbol,
                        "No price specified for limit/stop order, using fallback price of 1.0"
                    );
                    fallback_price
                })
            }
        };

        // Apply slippage
        let slippage_rate = if self.slippage_bps > Decimal::ZERO {
            Some((self.slippage_bps.max(Decimal::ZERO)) / Decimal::from(10_000))
        } else {
            None
        };
        let fill_price = if let Some(rate) = slippage_rate {
            match request.side {
                Side::Buy => base_price * (Decimal::ONE + rate), // Buy at higher price
                Side::Sell => base_price * (Decimal::ONE - rate), // Sell at lower price
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

    fn build_pending_order(request: OrderRequest) -> Order {
        let now = Utc::now();
        Order {
            id: Uuid::new_v4().to_string(),
            request,
            status: OrderStatus::PendingNew,
            filled_quantity: Decimal::ZERO,
            avg_fill_price: None,
            created_at: now,
            updated_at: now,
        }
    }

    async fn enqueue_conditional(&self, order: Order) {
        let mut book = self.conditional_orders.lock().await;
        book.push(order);
    }

    async fn spawn_attached_orders(&self, order: &Order) {
        if order.request.take_profit.is_none() && order.request.stop_loss.is_none() {
            return;
        }
        let qty = order.request.quantity.abs();
        if qty <= Decimal::ZERO {
            return;
        }
        let exit_side = order.request.side.inverse();
        let base_cid = order
            .request
            .client_order_id
            .clone()
            .unwrap_or_else(|| order.id.clone());

        if let Some(price) = order.request.take_profit {
            let request = OrderRequest {
                symbol: order.request.symbol.clone(),
                side: exit_side,
                order_type: OrderType::StopMarket,
                quantity: qty,
                price: None,
                trigger_price: Some(price),
                time_in_force: Some(TimeInForce::GoodTilCanceled),
                client_order_id: Some(format!("{base_cid}-tp")),
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            };
            self.enqueue_conditional(Self::build_pending_order(request))
                .await;
        }

        if let Some(price) = order.request.stop_loss {
            let request = OrderRequest {
                symbol: order.request.symbol.clone(),
                side: exit_side,
                order_type: OrderType::StopMarket,
                quantity: qty,
                price: None,
                trigger_price: Some(price),
                time_in_force: Some(TimeInForce::GoodTilCanceled),
                client_order_id: Some(format!("{base_cid}-sl")),
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            };
            self.enqueue_conditional(Self::build_pending_order(request))
                .await;
        }
    }

    /// Inspect conditional orders and emit fills for any whose trigger price was reached.
    pub async fn check_triggers(&self, candle: &Candle) -> BrokerResult<Vec<Fill>> {
        let triggered = {
            let mut book = self.conditional_orders.lock().await;
            book.trigger_with_candle(candle)
        };
        let fills = triggered
            .into_iter()
            .map(|event| {
                self.create_fill_from_order(&event.order, event.fill_price, event.timestamp)
            })
            .collect();
        Ok(fills)
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
            filled_quantity: Decimal::ZERO,
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
            OrderType::StopMarket => {
                let trigger_price = request.trigger_price.ok_or_else(|| {
                    BrokerError::InvalidRequest("stop-market order missing trigger price".into())
                })?;
                let mut request = request.clone();
                request.price = None;
                request.trigger_price = Some(trigger_price);
                request.time_in_force = Some(TimeInForce::GoodTilCanceled);
                let pending = Self::build_pending_order(request);
                self.enqueue_conditional(pending.clone()).await;
                Ok(pending)
            }
        }
    }

    async fn cancel_order(&self, order_id: OrderId, _symbol: &str) -> BrokerResult<()> {
        let mut open = self.open_orders.lock().await;
        if let Some(resting) = open.remove(&order_id) {
            let mut book = self.resting_depth.lock().unwrap();
            book.remove_order(resting.order.request.side, resting.price, resting.remaining);
            Ok(())
        } else {
            Err(BrokerError::InvalidRequest(format!(
                "order {} not found",
                order_id
            )))
        }
    }

    async fn amend_order(&self, request: OrderUpdateRequest) -> BrokerResult<Order> {
        let mut open = self.open_orders.lock().await;
        let resting = open.get_mut(&request.order_id).ok_or_else(|| {
            BrokerError::InvalidRequest(format!("order {} not found", request.order_id))
        })?;
        if resting.order.request.symbol != request.symbol {
            return Err(BrokerError::InvalidRequest(
                "symbol mismatch for amend".into(),
            ));
        }
        if resting.order.request.side != request.side {
            return Err(BrokerError::InvalidRequest(
                "side mismatch for amend".into(),
            ));
        }
        {
            let mut book = self.resting_depth.lock().unwrap();
            book.remove_order(resting.order.request.side, resting.price, resting.remaining);
        }
        if let Some(price) = request.new_price {
            resting.price = price;
            resting.order.request.price = Some(price);
        }
        if let Some(quantity) = request.new_quantity {
            if quantity < resting.order.filled_quantity {
                return Err(BrokerError::InvalidRequest(
                    "new quantity below filled amount".into(),
                ));
            }
            resting.order.request.quantity = quantity;
            resting.remaining = (quantity - resting.order.filled_quantity).max(Decimal::ZERO);
        } else {
            resting.remaining =
                (resting.order.request.quantity - resting.order.filled_quantity).max(Decimal::ZERO);
        }
        if resting.remaining <= Decimal::ZERO {
            resting.order.status = OrderStatus::Filled;
        } else if resting.order.filled_quantity > Decimal::ZERO {
            resting.order.status = OrderStatus::PartiallyFilled;
        } else {
            resting.order.status = OrderStatus::Accepted;
        }
        resting.order.updated_at = Utc::now();
        {
            let mut book = self.resting_depth.lock().unwrap();
            if resting.remaining > Decimal::ZERO {
                book.add_order(resting.order.request.side, resting.price, resting.remaining);
            }
        }
        Ok(resting.order.clone())
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

    async fn list_instruments(&self, _category: &str) -> BrokerResult<Vec<Instrument>> {
        Ok(Vec::new())
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
    conditional_orders: Arc<AsyncMutex<ConditionalOrderManager>>,
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
            conditional_orders: Arc::new(AsyncMutex::new(ConditionalOrderManager::new())),
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
        if quantity > Decimal::ZERO {
            depth.add_order(side, price, quantity);
        }
    }

    /// Update the resting book maintained for our own limit orders (used when backfills occur).
    pub fn force_resting_level(&self, side: Side, price: Price, quantity: Quantity) {
        let mut book = self.resting_depth.lock().unwrap();
        if quantity <= Decimal::ZERO {
            book.remove_order(side, price, quantity.abs());
        } else {
            book.add_order(side, price, quantity);
        }
    }

    fn build_pending_order(request: OrderRequest) -> Order {
        let now = Utc::now();
        Order {
            id: Uuid::new_v4().to_string(),
            request,
            status: OrderStatus::PendingNew,
            filled_quantity: Decimal::ZERO,
            avg_fill_price: None,
            created_at: now,
            updated_at: now,
        }
    }

    async fn enqueue_conditional(&self, order: Order) {
        let mut book = self.conditional_orders.lock().await;
        book.push(order);
    }

    async fn spawn_attached_orders(&self, order: &Order) {
        if order.request.take_profit.is_none() && order.request.stop_loss.is_none() {
            return;
        }
        let qty = order.request.quantity.abs();
        if qty <= Decimal::ZERO {
            return;
        }
        let exit_side = order.request.side.inverse();
        let base_cid = order
            .request
            .client_order_id
            .clone()
            .unwrap_or_else(|| order.id.clone());

        if let Some(price) = order.request.take_profit {
            let request = OrderRequest {
                symbol: order.request.symbol.clone(),
                side: exit_side,
                order_type: OrderType::StopMarket,
                quantity: qty,
                price: None,
                trigger_price: Some(price),
                time_in_force: Some(TimeInForce::GoodTilCanceled),
                client_order_id: Some(format!("{base_cid}-tp")),
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            };
            self.enqueue_conditional(Self::build_pending_order(request))
                .await;
        }

        if let Some(price) = order.request.stop_loss {
            let request = OrderRequest {
                symbol: order.request.symbol.clone(),
                side: exit_side,
                order_type: OrderType::StopMarket,
                quantity: qty,
                price: None,
                trigger_price: Some(price),
                time_in_force: Some(TimeInForce::GoodTilCanceled),
                client_order_id: Some(format!("{base_cid}-sl")),
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            };
            self.enqueue_conditional(Self::build_pending_order(request))
                .await;
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
            (Some((bid, _)), Some((ask, _))) => Some((bid + ask) / Decimal::from(2)),
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
        timestamp: DateTime<Utc>,
    ) -> Vec<Fill> {
        let mut generated = Vec::new();
        if quantity <= Decimal::ZERO {
            return generated;
        }

        let mut open = self.open_orders.lock().await;
        let mut finished = Vec::new();
        for (order_id, resting) in open.iter_mut() {
            if quantity <= Decimal::ZERO {
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
            let prev_notional = resting.order.avg_fill_price.unwrap_or(Decimal::ZERO) * prev_qty;
            let new_notional = prev_notional + price * trade_qty;
            resting.order.avg_fill_price = if resting.order.filled_quantity.is_zero() {
                Some(price)
            } else {
                Some(new_notional / resting.order.filled_quantity)
            };
            resting.order.updated_at = fill.timestamp;
            resting.order.status = if resting.remaining <= Decimal::ZERO {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            };
            self.apply_fill_accounting(&fill).await;
            if resting.remaining <= Decimal::ZERO {
                finished.push(order_id.clone());
            }
        }
        drop(open);

        if !finished.is_empty() {
            let mut open = self.open_orders.lock().await;
            for order_id in finished {
                if let Some(order) = open.remove(&order_id) {
                    self.spawn_attached_orders(&order.order).await;
                }
            }
        }

        let mut store = self.fills.lock().await;
        store.extend(generated.clone());
        drop(store);

        self.trigger_conditionals(price, timestamp).await;
        generated
    }

    async fn trigger_conditionals(&self, price: Price, timestamp: DateTime<Utc>) {
        let triggered = {
            let mut book = self.conditional_orders.lock().await;
            book.trigger_with_price(price, timestamp)
        };
        if triggered.is_empty() {
            return;
        }
        let mut store = self.fills.lock().await;
        for event in triggered {
            let fill = Fill {
                order_id: event.order.id.clone(),
                symbol: event.order.request.symbol.clone(),
                side: event.order.request.side,
                fill_price: event.fill_price,
                fill_quantity: event.order.request.quantity,
                fee: None,
                timestamp: event.timestamp,
            };
            self.apply_fill_accounting(&fill).await;
            store.push(fill);
        }
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
            quantity: Decimal::ZERO,
            entry_price: Some(fill.fill_price),
            unrealized_pnl: Decimal::ZERO,
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
                position.entry_price = if total_qty.is_zero() {
                    Some(fill.fill_price)
                } else {
                    Some((prev_cost + new_cost) / total_qty)
                };
                position.quantity = total_qty;
            }
            Some(_) => {
                position.quantity -= fill.fill_quantity;
                if position.quantity <= Decimal::ZERO {
                    position.side = None;
                    position.entry_price = None;
                    position.quantity = Decimal::ZERO;
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
        order.avg_fill_price = if total_qty.is_zero() {
            None
        } else {
            Some(notional / total_qty)
        };
        order.status = if order.request.quantity == total_qty {
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
        drop(store);

        if matches!(order.status, OrderStatus::Filled) {
            self.spawn_attached_orders(order).await;
        }
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
                let fee = if self.fee_bps > Decimal::ZERO {
                    let rate = self.fee_bps.max(Decimal::ZERO) / Decimal::from(10_000);
                    order
                        .avg_fill_price
                        .map(|fill_price| fill_price * order.request.quantity.abs() * rate)
                } else {
                    None
                };

                info!(
                    symbol = %order.request.symbol,
                    qty = %order.request.quantity,
                    price = ?order.avg_fill_price,
                    fee = ?fee,
                    side = ?order.request.side,
                    "paper order filled"
                );
                self.spawn_attached_orders(&order).await;
                Ok(order)
            }
            tesser_core::OrderType::StopMarket => {
                let trigger_price = request.trigger_price.ok_or_else(|| {
                    BrokerError::InvalidRequest("StopMarket order requires a trigger_price".into())
                })?;
                let mut request = request.clone();
                request.price = None;
                request.time_in_force = Some(TimeInForce::GoodTilCanceled);
                request.trigger_price = Some(trigger_price);
                let order = Self::build_pending_order(request);
                self.enqueue_conditional(order.clone()).await;
                info!(
                    symbol = %order.request.symbol,
                    qty = %order.request.quantity,
                    trigger = %trigger_price,
                    "paper conditional order placed"
                );
                Ok(order)
            }
        }
    }

    async fn cancel_order(&self, _order_id: OrderId, _symbol: &str) -> BrokerResult<()> {
        Ok(())
    }

    async fn amend_order(&self, request: OrderUpdateRequest) -> BrokerResult<Order> {
        let mut orders = self.orders.lock().await;
        if let Some(order) = orders.iter_mut().find(|o| o.id == request.order_id) {
            if order.request.symbol != request.symbol {
                return Err(BrokerError::InvalidRequest(
                    "symbol mismatch for amend".into(),
                ));
            }
            if order.request.side != request.side {
                return Err(BrokerError::InvalidRequest(
                    "side mismatch for amend".into(),
                ));
            }
            if let Some(price) = request.new_price {
                order.request.price = Some(price);
            }
            if let Some(quantity) = request.new_quantity {
                order.request.quantity = quantity;
            }
            order.updated_at = Utc::now();
            return Ok(order.clone());
        }
        Err(BrokerError::InvalidRequest(format!(
            "order {} not found",
            request.order_id
        )))
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

    async fn list_instruments(&self, _category: &str) -> BrokerResult<Vec<Instrument>> {
        Ok(Vec::new())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct CandleBuilder {
    symbol: Symbol,
    open: Option<Decimal>,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    start: Option<DateTime<Utc>>,
}

impl CandleBuilder {
    fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            open: None,
            high: Decimal::ZERO,
            low: Decimal::ZERO,
            close: Decimal::ZERO,
            volume: Decimal::ZERO,
            start: None,
        }
    }

    fn update(
        &mut self,
        price: Decimal,
        timestamp: DateTime<Utc>,
        qty: Decimal,
        interval: ChronoDuration,
    ) -> Option<Candle> {
        if self.open.is_none() {
            self.open = Some(price);
            self.high = price;
            self.low = price;
            self.close = price;
            self.volume = qty;
            self.start = Some(timestamp);
            return None;
        }

        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        self.volume += qty;

        let start = self.start.unwrap_or(timestamp);
        if timestamp - start >= interval {
            let candle = Candle {
                symbol: self.symbol.clone(),
                interval: Interval::OneMinute, // placeholder, updated by caller if needed
                open: self.open.unwrap_or(price),
                high: self.high,
                low: self.low,
                close: self.close,
                volume: self.volume,
                timestamp: start,
            };
            self.reset(price, timestamp, qty);
            Some(candle)
        } else {
            None
        }
    }

    fn reset(&mut self, price: Decimal, timestamp: DateTime<Utc>, volume: Decimal) {
        self.open = Some(price);
        self.high = price;
        self.low = price;
        self.close = price;
        self.volume = volume;
        self.start = Some(timestamp);
    }
}

struct ReplaySample {
    timestamp: DateTime<Utc>,
    price: Decimal,
    size: Decimal,
    side: Side,
}

pub struct LivePaperStream {
    symbol: Symbol,
    tick_rx: AsyncMutex<mpsc::Receiver<Tick>>,
    candle_rx: AsyncMutex<mpsc::Receiver<Candle>>,
    candle_interval: Arc<AsyncMutex<ChronoDuration>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl LivePaperStream {
    fn new(
        symbol: Symbol,
        market: PaperMarketConfig,
        exec_client: Arc<PaperExecutionClient>,
        connection_status: Option<Arc<AtomicBool>>,
    ) -> BrokerResult<Self> {
        let (tick_tx, tick_rx) = mpsc::channel(2048);
        let (candle_tx, candle_rx) = mpsc::channel(512);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let candle_interval = Arc::new(AsyncMutex::new(ChronoDuration::minutes(1)));
        spawn_market_generator(
            symbol.clone(),
            market,
            exec_client,
            tick_tx,
            candle_tx,
            candle_interval.clone(),
            shutdown_rx,
            connection_status,
        );
        Ok(Self {
            symbol,
            tick_rx: AsyncMutex::new(tick_rx),
            candle_rx: AsyncMutex::new(candle_rx),
            candle_interval,
            shutdown_tx: Some(shutdown_tx),
        })
    }
}

impl Drop for LivePaperStream {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[async_trait]
impl ConnectorStream for LivePaperStream {
    async fn subscribe(&mut self, symbols: &[String], interval: Interval) -> BrokerResult<()> {
        if !symbols.is_empty() && !symbols.iter().any(|s| s == &self.symbol) {
            return Err(BrokerError::InvalidRequest(format!(
                "paper stream only supports symbol {}",
                self.symbol
            )));
        }
        let mut guard = self.candle_interval.lock().await;
        *guard = interval.as_duration();
        Ok(())
    }

    async fn next_tick(&mut self) -> BrokerResult<Option<Tick>> {
        let mut rx = self.tick_rx.lock().await;
        Ok(rx.recv().await)
    }

    async fn next_candle(&mut self) -> BrokerResult<Option<Candle>> {
        let mut rx = self.candle_rx.lock().await;
        Ok(rx.recv().await)
    }

    async fn next_order_book(&mut self) -> BrokerResult<Option<OrderBook>> {
        Ok(None)
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_market_generator(
    symbol: Symbol,
    market: PaperMarketConfig,
    exec_client: Arc<PaperExecutionClient>,
    tick_tx: mpsc::Sender<Tick>,
    candle_tx: mpsc::Sender<Candle>,
    candle_interval: Arc<AsyncMutex<ChronoDuration>>,
    shutdown_rx: oneshot::Receiver<()>,
    connection_status: Option<Arc<AtomicBool>>,
) {
    tokio::spawn(async move {
        if let Some(flag) = &connection_status {
            flag.store(true, Ordering::SeqCst);
        }
        let result = match market {
            PaperMarketConfig::RandomWalk {
                start_price,
                volatility,
                interval_ms,
            } => {
                run_random_walk(
                    symbol.clone(),
                    start_price,
                    volatility,
                    interval_ms,
                    exec_client,
                    tick_tx,
                    candle_tx,
                    candle_interval,
                    shutdown_rx,
                )
                .await
            }
            PaperMarketConfig::Replay { path, speed } => {
                run_replay(
                    symbol.clone(),
                    path,
                    speed,
                    exec_client,
                    tick_tx,
                    candle_tx,
                    candle_interval,
                    shutdown_rx,
                )
                .await
            }
        };
        if let Err(err) = result {
            error!(symbol = %symbol, error = %err, "paper generator exited");
        }
        if let Some(flag) = connection_status {
            flag.store(false, Ordering::SeqCst);
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_random_walk(
    symbol: Symbol,
    start_price: Decimal,
    volatility: f64,
    interval_ms: u64,
    exec_client: Arc<PaperExecutionClient>,
    tick_tx: mpsc::Sender<Tick>,
    candle_tx: mpsc::Sender<Candle>,
    candle_interval: Arc<AsyncMutex<ChronoDuration>>,
    mut shutdown: oneshot::Receiver<()>,
) -> BrokerResult<()> {
    let mut rng = StdRng::from_entropy();
    let mut price = start_price.to_f64().unwrap_or(25_000.0).max(1.0);
    let mut ticker = interval(TokioDuration::from_millis(interval_ms.max(10)));
    let mut candle_builder = CandleBuilder::new(symbol.clone());
    loop {
        select! {
            _ = ticker.tick() => {},
            _ = &mut shutdown => break,
        }
        let noise: f64 = rng.sample(StandardNormal);
        let delta = 1.0 + noise * volatility.clamp(0.0001, 0.1);
        price = (price * delta).max(1.0);
        let price_decimal =
            Decimal::from_f64(price).unwrap_or_else(|| Decimal::from_f64(25_000.0).unwrap());
        exec_client.update_price(&symbol, price_decimal);
        let now = Utc::now();
        let size = Decimal::ONE;
        let tick = Tick {
            symbol: symbol.clone(),
            price: price_decimal,
            size,
            side: if delta >= 1.0 { Side::Buy } else { Side::Sell },
            exchange_timestamp: now,
            received_at: now,
        };
        if tick_tx.send(tick).await.is_err() {
            break;
        }
        let interval = *candle_interval.lock().await;
        if let Some(mut candle) = candle_builder.update(
            price_decimal,
            now,
            size,
            interval.max(ChronoDuration::seconds(1)),
        ) {
            candle.interval = interval_to_core(interval);
            if candle_tx.send(candle).await.is_err() {
                break;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_replay(
    symbol: Symbol,
    path: PathBuf,
    speed: f64,
    exec_client: Arc<PaperExecutionClient>,
    tick_tx: mpsc::Sender<Tick>,
    candle_tx: mpsc::Sender<Candle>,
    candle_interval: Arc<AsyncMutex<ChronoDuration>>,
    mut shutdown: oneshot::Receiver<()>,
) -> BrokerResult<()> {
    let samples = load_replay_samples(&path)?;
    if samples.is_empty() {
        return Err(BrokerError::InvalidRequest(format!(
            "replay dataset '{}' is empty",
            path.display()
        )));
    }
    let mut candle_builder = CandleBuilder::new(symbol.clone());
    let mut idx = 0usize;
    loop {
        let sample = &samples[idx];
        let delay = if idx == 0 {
            TokioDuration::from_millis(10)
        } else {
            let prev = &samples[(idx + samples.len() - 1) % samples.len()];
            let delta = (sample.timestamp - prev.timestamp).max(ChronoDuration::milliseconds(1));
            scale_duration(chrono_to_std(delta), speed)
        };
        select! {
            _ = sleep(delay) => {},
            _ = &mut shutdown => break,
        }
        exec_client.update_price(&symbol, sample.price);
        let tick = Tick {
            symbol: symbol.clone(),
            price: sample.price,
            size: sample.size,
            side: sample.side,
            exchange_timestamp: sample.timestamp,
            received_at: Utc::now(),
        };
        if tick_tx.send(tick).await.is_err() {
            break;
        }
        let interval = *candle_interval.lock().await;
        if let Some(mut candle) = candle_builder.update(
            sample.price,
            sample.timestamp,
            sample.size,
            interval.max(ChronoDuration::seconds(1)),
        ) {
            candle.interval = interval_to_core(interval);
            if candle_tx.send(candle).await.is_err() {
                break;
            }
        }
        idx = (idx + 1) % samples.len();
    }
    Ok(())
}

fn scale_duration(duration: TokioDuration, speed: f64) -> TokioDuration {
    if speed <= 0.0 {
        return duration;
    }
    let scaled = duration.as_secs_f64() / speed.max(0.0001);
    TokioDuration::from_secs_f64(scaled.max(0.000_001))
}

fn chrono_to_std(duration: ChronoDuration) -> TokioDuration {
    if duration.num_nanoseconds().unwrap_or(0) <= 0 {
        return TokioDuration::from_millis(1);
    }
    TokioDuration::from_nanos(duration.num_nanoseconds().unwrap_or(1) as u64)
}

fn interval_to_core(duration: ChronoDuration) -> Interval {
    let seconds = duration.num_seconds();
    match seconds {
        0..=1 => Interval::OneSecond,
        2..=90 => Interval::OneMinute,
        91..=600 => Interval::FiveMinutes,
        601..=1800 => Interval::FifteenMinutes,
        1801..=7200 => Interval::OneHour,
        7201..=28800 => Interval::FourHours,
        _ => Interval::OneDay,
    }
}

fn load_replay_samples(path: &PathBuf) -> BrokerResult<Vec<ReplaySample>> {
    let mut reader =
        csv::Reader::from_path(path).map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
    let headers = reader
        .headers()
        .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?
        .clone();
    let mut records = Vec::new();
    for result in reader.records() {
        let record = result.map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let timestamp = parse_timestamp(&record, &headers)?;
        let price = parse_decimal_field(&record, &headers, "price")?;
        let size = parse_decimal_field(&record, &headers, "size").unwrap_or(Decimal::ONE);
        let side = parse_side_field(&record, &headers);
        records.push(ReplaySample {
            timestamp,
            price,
            size,
            side,
        });
    }
    Ok(records)
}

fn parse_timestamp(record: &StringRecord, headers: &StringRecord) -> BrokerResult<DateTime<Utc>> {
    let idx = headers
        .iter()
        .position(|h| h.eq_ignore_ascii_case("timestamp"))
        .unwrap_or(0);
    let raw = record
        .get(idx)
        .ok_or_else(|| BrokerError::InvalidRequest("missing timestamp column".into()))?;
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
                .map(|dt| Utc.from_utc_datetime(&dt))
        })
        .map_err(|err| BrokerError::InvalidRequest(format!("invalid timestamp '{raw}': {err}")))
}

fn parse_decimal_field(
    record: &StringRecord,
    headers: &StringRecord,
    field: &str,
) -> BrokerResult<Decimal> {
    let idx = headers
        .iter()
        .position(|h| h.eq_ignore_ascii_case(field))
        .ok_or_else(|| BrokerError::InvalidRequest(format!("missing column '{field}'")))?;
    record
        .get(idx)
        .ok_or_else(|| BrokerError::InvalidRequest(format!("missing value for column '{field}'")))?
        .parse::<Decimal>()
        .map_err(|err| BrokerError::InvalidRequest(format!("invalid {field}: {err}")))
}

fn parse_side_field(record: &StringRecord, headers: &StringRecord) -> Side {
    headers
        .iter()
        .position(|h| h.eq_ignore_ascii_case("side"))
        .and_then(|idx| record.get(idx))
        .map(|raw| match raw.to_lowercase().as_str() {
            "sell" => Side::Sell,
            _ => Side::Buy,
        })
        .unwrap_or(Side::Buy)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn matching_engine_amend_updates_resting_state() {
        let engine = MatchingEngine::new("paper", vec!["BTCUSDT".into()], Decimal::from(10_000));
        let request = OrderRequest {
            symbol: "BTCUSDT".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: Decimal::from(2),
            price: Some(Decimal::from(25_000)),
            trigger_price: None,
            time_in_force: Some(TimeInForce::GoodTilCanceled),
            client_order_id: None,
            take_profit: None,
            stop_loss: None,
            display_quantity: None,
        };

        let order = engine.place_order(request).await.unwrap();
        assert_eq!(order.status, OrderStatus::Accepted);
        let update = OrderUpdateRequest {
            order_id: order.id.clone(),
            symbol: order.request.symbol.clone(),
            side: order.request.side,
            new_price: Some(Decimal::from(25_500)),
            new_quantity: Some(Decimal::from(3)),
        };

        let amended = engine.amend_order(update).await.unwrap();
        assert_eq!(amended.request.price, Some(Decimal::from(25_500)));
        assert_eq!(amended.request.quantity, Decimal::from(3));

        let book = engine.resting_depth.lock().unwrap();
        let (price, qty) = book.best_bid().expect("resting bid exists");
        assert_eq!(price, Decimal::from(25_500));
        assert_eq!(qty, Decimal::from(3));
    }
}
