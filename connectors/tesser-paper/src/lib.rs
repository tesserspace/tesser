//! Simple paper-trading connector used by the backtester.

mod conditional;
mod fees;

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
    AccountBalance, AssetId, Candle, DepthUpdate, Fill, Instrument, Interval, LocalOrderBook,
    Order, OrderBook, OrderId, OrderRequest, OrderStatus, OrderType, OrderUpdateRequest, Position,
    Price, Quantity, Side, Symbol, Tick, TimeInForce,
};
use tokio::task::JoinHandle;
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex as AsyncMutex},
    time::{interval, sleep, Duration as TokioDuration},
};
use tracing::{error, info, warn};
use uuid::Uuid;

pub use fees::{FeeContext, FeeModel, FeeScheduleConfig, LiquidityRole, MarketFeeConfig};

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
    #[serde(default)]
    pub symbols: Vec<Symbol>,
    #[serde(default = "default_balance_currency")]
    pub balance_currency: String,
    #[serde(default = "default_initial_balance")]
    pub initial_balance: Decimal,
    #[serde(default)]
    pub slippage_bps: Decimal,
    #[serde(default)]
    pub fee_bps: Decimal,
    #[serde(default)]
    pub fee_schedule: Option<FeeScheduleConfig>,
    #[serde(default)]
    pub market: PaperMarketConfig,
}

impl Default for PaperConnectorConfig {
    fn default() -> Self {
        Self {
            symbol: default_symbol(),
            symbols: Vec::new(),
            balance_currency: default_balance_currency(),
            initial_balance: default_initial_balance(),
            slippage_bps: Decimal::ZERO,
            fee_bps: Decimal::ZERO,
            fee_schedule: None,
            market: PaperMarketConfig::default(),
        }
    }
}

impl PaperConnectorConfig {
    fn cache_key(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            self.symbols()
                .first()
                .copied()
                .unwrap_or_else(default_symbol)
                .code()
                .to_string()
        })
    }

    fn balance_asset(&self) -> AssetId {
        if let Ok(parsed) = self.balance_currency.parse::<AssetId>() {
            parsed
        } else {
            let primary = self
                .symbols()
                .first()
                .copied()
                .unwrap_or_else(default_symbol);
            AssetId::from_code(primary.exchange, &self.balance_currency)
        }
    }

    fn symbols(&self) -> Vec<Symbol> {
        if self.symbols.is_empty() {
            vec![self.symbol]
        } else {
            self.symbols.clone()
        }
    }
}

struct PaperRuntimeState {
    client: Arc<PaperExecutionClient>,
    config: PaperConnectorConfig,
    symbols: Vec<Symbol>,
    initialized: AsyncMutex<bool>,
}

impl PaperRuntimeState {
    fn new(config: PaperConnectorConfig) -> Self {
        let symbols = config.symbols();
        let primary = symbols.first().copied().unwrap_or_else(default_symbol);
        let stream_name = format!("paper-{}", primary.code().to_lowercase());
        let fee_schedule = config
            .fee_schedule
            .clone()
            .unwrap_or_else(|| FeeScheduleConfig::flat(config.fee_bps.max(Decimal::ZERO)));
        let fee_model = fee_schedule.build_model();
        let cash_asset = config.balance_asset();
        let client = Arc::new(PaperExecutionClient::with_cash_asset(
            stream_name,
            symbols.clone(),
            config.slippage_bps,
            fee_model,
            cash_asset,
        ));
        Self {
            client,
            config,
            symbols,
            initialized: AsyncMutex::new(false),
        }
    }

    async fn ensure_initialized(&self) {
        let mut guard = self.initialized.lock().await;
        if *guard {
            return;
        }
        let asset = self.config.balance_asset();
        self.client
            .initialize_balance(asset, self.config.initial_balance)
            .await;
        for symbol in &self.symbols {
            self.client
                .update_price(symbol, self.config.market.initial_price());
        }
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
        let symbols = cfg.symbols();
        if symbols.len() == 1 {
            let stream = LivePaperStream::new(
                symbols[0],
                cfg.market.clone(),
                runtime.client.clone(),
                stream_config.connection_status,
            )?;
            Ok(Box::new(stream))
        } else {
            let mut streams = Vec::new();
            for symbol in symbols {
                streams.push(LivePaperStream::new(
                    symbol,
                    cfg.market.clone(),
                    runtime.client.clone(),
                    stream_config.connection_status.clone(),
                )?);
            }
            Ok(Box::new(FanInPaperStream::new(streams)))
        }
    }
}

pub fn register_factory() {
    register_connector_factory(Arc::new(PaperFactory::default()));
}

fn default_symbol() -> Symbol {
    Symbol::from("BTCUSDT")
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
    positions: Arc<AsyncMutex<HashMap<Symbol, Position>>>,
    conditional_orders: Arc<AsyncMutex<ConditionalOrderManager>>,
    /// Latest market prices for each symbol
    last_prices: Arc<Mutex<HashMap<Symbol, Price>>>,
    /// Simulation parameters
    slippage_bps: Decimal,
    fee_model: Arc<dyn FeeModel>,
    cash_asset: Arc<Mutex<AssetId>>,
}

impl Default for PaperExecutionClient {
    fn default() -> Self {
        let fee_model = FeeScheduleConfig::default().build_model();
        Self::new(
            "paper".into(),
            vec![Symbol::from("BTCUSDT")],
            Decimal::ZERO,
            fee_model,
        )
    }
}

impl PaperExecutionClient {
    /// Create a new paper execution client configurable with instrument metadata.
    pub fn new(
        name: String,
        markets: Vec<Symbol>,
        slippage_bps: Decimal,
        fee_model: Arc<dyn FeeModel>,
    ) -> Self {
        Self::with_cash_asset(
            name,
            markets,
            slippage_bps,
            fee_model,
            AssetId::from("USDT"),
        )
    }

    pub fn with_cash_asset(
        name: String,
        markets: Vec<Symbol>,
        slippage_bps: Decimal,
        fee_model: Arc<dyn FeeModel>,
        cash_asset: AssetId,
    ) -> Self {
        let broker_markets: Vec<String> = markets
            .iter()
            .map(|symbol| symbol.code().to_string())
            .collect();
        let initial_balance = AccountBalance {
            exchange: cash_asset.exchange,
            asset: cash_asset,
            total: Decimal::from(10_000),
            available: Decimal::from(10_000),
            updated_at: Utc::now(),
        };
        Self {
            info: BrokerInfo {
                name,
                markets: broker_markets,
                supports_testnet: true,
            },
            orders: Arc::new(AsyncMutex::new(Vec::new())),
            balances: Arc::new(AsyncMutex::new(vec![initial_balance])),
            positions: Arc::new(AsyncMutex::new(HashMap::new())),
            conditional_orders: Arc::new(AsyncMutex::new(ConditionalOrderManager::new())),
            last_prices: Arc::new(Mutex::new(HashMap::new())),
            slippage_bps,
            fee_model,
            cash_asset: Arc::new(Mutex::new(cash_asset)),
        }
    }

    /// Update the latest market price for a symbol.
    pub fn update_price(&self, symbol: &Symbol, price: Price) {
        let mut prices = self.last_prices.lock().unwrap();
        prices.insert(*symbol, price);
    }

    /// Reset the available balance to a configured amount.
    pub async fn initialize_balance(&self, asset: AssetId, amount: Decimal) {
        *self.cash_asset.lock().unwrap() = asset;
        let mut balances = self.balances.lock().await;
        if let Some(entry) = balances.iter_mut().find(|b| b.asset == asset) {
            entry.total = amount;
            entry.available = amount;
            entry.updated_at = Utc::now();
        } else {
            balances.push(AccountBalance {
                exchange: asset.exchange,
                asset,
                total: amount,
                available: amount,
                updated_at: Utc::now(),
            });
        }
    }

    fn compute_fee(
        &self,
        symbol: Symbol,
        side: Side,
        role: LiquidityRole,
        price: Price,
        qty: Quantity,
    ) -> Decimal {
        self.fee_model
            .fee(
                FeeContext {
                    symbol: symbol.code(),
                    side,
                    role,
                },
                price,
                qty,
            )
            .max(Decimal::ZERO)
    }

    /// Create a Fill object from an order with proper fee calculation.
    fn create_fill_from_order(
        &self,
        order: &Order,
        fill_price: Price,
        timestamp: DateTime<Utc>,
    ) -> Fill {
        let fee_amount = self.compute_fee(
            order.request.symbol,
            order.request.side,
            LiquidityRole::Taker,
            fill_price,
            order.request.quantity.abs(),
        );
        let fee = if fee_amount.is_zero() {
            None
        } else {
            Some(fee_amount)
        };

        Fill {
            order_id: order.id.clone(),
            symbol: order.request.symbol,
            side: order.request.side,
            fill_price,
            fill_quantity: order.request.quantity,
            fee,
            fee_asset: fee.map(|_| *self.cash_asset.lock().unwrap()),
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
                symbol: order.request.symbol,
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
                symbol: order.request.symbol,
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

    #[allow(dead_code)]
    async fn apply_fill_accounting(&self, fill: &Fill) {
        let cash_asset = *self.cash_asset.lock().unwrap();
        let mut balances = self.balances.lock().await;
        if let Some(balance) = balances.iter_mut().find(|b| b.asset == cash_asset) {
            let notional = fill.fill_price * fill.fill_quantity;
            let fee = fill.fee.unwrap_or(Decimal::ZERO);
            match fill.side {
                Side::Buy => balance.available -= notional + fee,
                Side::Sell => balance.available += notional - fee,
            }
            balance.total = balance.available;
            balance.updated_at = fill.timestamp;
        }
        drop(balances);

        let mut positions = self.positions.lock().await;
        let position = positions.entry(fill.symbol).or_insert(Position {
            symbol: fill.symbol,
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
                    let now = self.simulated_now();
                    let activation_time = self.activation_deadline(now);
                    if self.latency <= ChronoDuration::zero() {
                        order.status = OrderStatus::Accepted;
                        order.updated_at = now;
                    }
                    self.record_resting_order(order.clone(), limit_price, activation_time)
                        .await;
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

    async fn cancel_order(&self, order_id: OrderId, _symbol: Symbol) -> BrokerResult<()> {
        let mut open = self.open_orders.lock().await;
        if let Some(resting) = open.get_mut(&order_id) {
            if self.latency <= ChronoDuration::zero() {
                let resting = open.remove(&order_id).unwrap();
                drop(open);
                if resting.active && resting.remaining > Decimal::ZERO {
                    let mut book = self.resting_depth.lock().unwrap();
                    book.remove_order(resting.order.request.side, resting.price, resting.remaining);
                }
                return Ok(());
            }
            let now = self.simulated_now();
            resting.cancel_after = Some(self.activation_deadline(now));
            return Ok(());
        }
        Err(BrokerError::InvalidRequest(format!(
            "order {} not found",
            order_id
        )))
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
        if resting.active && resting.remaining > Decimal::ZERO {
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
            resting.original_quantity = quantity;
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
        if resting.active && resting.remaining > Decimal::ZERO {
            resting.initial_queue_position =
                self.queue_ahead_for(resting.order.request.side, resting.price, Decimal::ZERO);
            resting.processed_volume = Decimal::ZERO;
            let mut book = self.resting_depth.lock().unwrap();
            book.add_order(resting.order.request.side, resting.price, resting.remaining);
        }
        Ok(resting.order.clone())
    }

    async fn list_open_orders(&self, _symbol: Symbol) -> BrokerResult<Vec<Order>> {
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
    initial_queue_position: Quantity,
    processed_volume: Quantity,
    activation_time: DateTime<Utc>,
    cancel_after: Option<DateTime<Utc>>,
    active: bool,
    original_quantity: Quantity,
}

/// Controls how aggressively the matching engine assumes we improve queue position.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum QueueModel {
    #[default]
    Conservative,
    Optimistic,
}

/// Configuration block wiring latency and queue modeling assumptions.
#[derive(Clone)]
pub struct MatchingEngineConfig {
    pub latency: ChronoDuration,
    pub queue_model: QueueModel,
    pub fee_model: Arc<dyn FeeModel>,
    pub cash_asset: Option<AssetId>,
}

impl Default for MatchingEngineConfig {
    fn default() -> Self {
        Self {
            latency: ChronoDuration::zero(),
            queue_model: QueueModel::default(),
            fee_model: FeeScheduleConfig::default().build_model(),
            cash_asset: None,
        }
    }
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
    latency: ChronoDuration,
    queue_model: QueueModel,
    clock: Arc<Mutex<Option<DateTime<Utc>>>>,
    queue_reset: Arc<AtomicBool>,
    fee_model: Arc<dyn FeeModel>,
    cash_asset: Arc<Mutex<AssetId>>,
}

impl MatchingEngine {
    /// Build a new matching engine with the provided book metadata.
    pub fn new(name: impl Into<String>, markets: Vec<Symbol>, initial_cash: Price) -> Self {
        Self::with_config(name, markets, initial_cash, MatchingEngineConfig::default())
    }

    /// Build a matching engine configured with explicit latency/queue parameters.
    pub fn with_config(
        name: impl Into<String>,
        markets: Vec<Symbol>,
        initial_cash: Price,
        config: MatchingEngineConfig,
    ) -> Self {
        let now = Utc::now();
        let latency = if config.latency < ChronoDuration::zero() {
            ChronoDuration::zero()
        } else {
            config.latency
        };
        let cash_asset = config.cash_asset.unwrap_or_else(|| AssetId::from("USDT"));
        let broker_markets: Vec<String> = markets
            .iter()
            .map(|symbol| symbol.code().to_string())
            .collect();
        Self {
            info: BrokerInfo {
                name: name.into(),
                markets: broker_markets,
                supports_testnet: true,
            },
            market_depth: Arc::new(Mutex::new(LocalOrderBook::new())),
            resting_depth: Arc::new(Mutex::new(LocalOrderBook::new())),
            balances: Arc::new(AsyncMutex::new(vec![AccountBalance {
                exchange: cash_asset.exchange,
                asset: cash_asset,
                total: initial_cash,
                available: initial_cash,
                updated_at: now,
            }])),
            positions: Arc::new(AsyncMutex::new(HashMap::new())),
            open_orders: Arc::new(AsyncMutex::new(HashMap::new())),
            fills: Arc::new(AsyncMutex::new(Vec::new())),
            conditional_orders: Arc::new(AsyncMutex::new(ConditionalOrderManager::new())),
            latency,
            queue_model: config.queue_model,
            clock: Arc::new(Mutex::new(None)),
            queue_reset: Arc::new(AtomicBool::new(false)),
            fee_model: config.fee_model.clone(),
            cash_asset: Arc::new(Mutex::new(cash_asset)),
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
        self.queue_reset.store(true, Ordering::Relaxed);
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
        self.queue_reset.store(true, Ordering::Relaxed);
    }

    fn compute_fee(
        &self,
        symbol: Symbol,
        side: Side,
        role: LiquidityRole,
        price: Price,
        qty: Quantity,
    ) -> Decimal {
        self.fee_model
            .fee(
                FeeContext {
                    symbol: symbol.code(),
                    side,
                    role,
                },
                price,
                qty,
            )
            .max(Decimal::ZERO)
    }

    fn simulated_now(&self) -> DateTime<Utc> {
        let guard = self.clock.lock().unwrap();
        guard.unwrap_or_else(Utc::now)
    }

    fn activation_deadline(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        now + self.latency
    }

    fn queue_ahead_for(&self, side: Side, price: Price, exclude_self: Quantity) -> Quantity {
        if self.queue_model == QueueModel::Optimistic {
            return Decimal::ZERO;
        }
        let market_qty = {
            let depth = self.market_depth.lock().unwrap();
            depth.volume_at_level(side, price)
        };
        let own_qty = {
            let book = self.resting_depth.lock().unwrap();
            let total = book.volume_at_level(side, price);
            (total - exclude_self).max(Decimal::ZERO)
        };
        market_qty + own_qty
    }

    pub async fn advance_time(&self, now: DateTime<Utc>) {
        {
            let mut guard = self.clock.lock().unwrap();
            if guard.is_none_or(|prev| now > prev) {
                *guard = Some(now);
            }
        }
        if self.queue_reset.swap(false, Ordering::Relaxed) {
            self.reseed_queue_positions().await;
        }
        self.activate_due(now).await;
        self.finalize_cancels(now).await;
    }

    async fn activate_due(&self, now: DateTime<Utc>) {
        let mut additions = Vec::new();
        {
            let mut open = self.open_orders.lock().await;
            for resting in open.values_mut() {
                if resting.active || now < resting.activation_time {
                    continue;
                }
                resting.active = true;
                resting.order.status = OrderStatus::Accepted;
                resting.order.updated_at = now;
                resting.initial_queue_position =
                    self.queue_ahead_for(resting.order.request.side, resting.price, Decimal::ZERO);
                resting.processed_volume = Decimal::ZERO;
                if resting.remaining > Decimal::ZERO {
                    additions.push((resting.order.request.side, resting.price, resting.remaining));
                }
            }
        }
        if additions.is_empty() {
            return;
        }
        let mut book = self.resting_depth.lock().unwrap();
        for (side, price, qty) in additions {
            book.add_order(side, price, qty);
        }
    }

    async fn finalize_cancels(&self, now: DateTime<Utc>) {
        let mut canceled = Vec::new();
        {
            let mut open = self.open_orders.lock().await;
            let ids: Vec<_> = open
                .iter()
                .filter_map(|(id, resting)| {
                    if resting.cancel_after.is_some_and(|deadline| now >= deadline) {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect();
            for id in ids {
                if let Some(resting) = open.remove(&id) {
                    canceled.push(resting);
                }
            }
        }
        if canceled.is_empty() {
            return;
        }
        let mut book = self.resting_depth.lock().unwrap();
        for mut resting in canceled {
            if resting.active && resting.remaining > Decimal::ZERO {
                book.remove_order(resting.order.request.side, resting.price, resting.remaining);
            }
            resting.order.status = OrderStatus::Canceled;
            resting.order.updated_at = now;
        }
    }

    async fn reseed_queue_positions(&self) {
        let mut open = self.open_orders.lock().await;
        for resting in open.values_mut() {
            if !resting.active || resting.remaining <= Decimal::ZERO {
                continue;
            }
            let ahead =
                self.queue_ahead_for(resting.order.request.side, resting.price, resting.remaining);
            resting.initial_queue_position = ahead;
            resting.processed_volume = Decimal::ZERO;
        }
    }

    fn passive_fillable_volume(
        &self,
        resting: &mut RestingOrder,
        trade_size: Quantity,
    ) -> Quantity {
        if resting.remaining <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let prev_processed = resting.processed_volume;
        let cap = resting.initial_queue_position + resting.original_quantity;
        let new_processed = (resting.processed_volume + trade_size).min(cap);
        resting.processed_volume = new_processed;
        let prev_fillable = (prev_processed - resting.initial_queue_position).max(Decimal::ZERO);
        let new_fillable = (new_processed - resting.initial_queue_position).max(Decimal::ZERO);
        (new_fillable - prev_fillable).max(Decimal::ZERO)
    }

    async fn apply_resting_fill(
        &self,
        order_id: &OrderId,
        resting: &mut RestingOrder,
        price: Price,
        quantity: Quantity,
        timestamp: DateTime<Utc>,
        fills: &mut Vec<Fill>,
    ) {
        if quantity <= Decimal::ZERO {
            return;
        }
        {
            let mut resting_book = self.resting_depth.lock().unwrap();
            resting_book.remove_order(resting.order.request.side, resting.price, quantity);
        }
        let fee_amount = self.compute_fee(
            resting.order.request.symbol,
            resting.order.request.side,
            LiquidityRole::Maker,
            price,
            quantity,
        );
        let fill = self.build_fill(
            order_id,
            resting.order.request.symbol,
            resting.order.request.side,
            price,
            quantity,
            fee_amount,
            timestamp,
        );
        fills.push(fill.clone());
        let prev_qty = resting.order.filled_quantity;
        resting.order.filled_quantity += quantity;
        let prev_notional = resting.order.avg_fill_price.unwrap_or(Decimal::ZERO) * prev_qty;
        let new_notional = prev_notional + price * quantity;
        resting.order.avg_fill_price = if resting.order.filled_quantity.is_zero() {
            Some(price)
        } else {
            Some(new_notional / resting.order.filled_quantity)
        };
        resting.remaining = (resting.remaining - quantity).max(Decimal::ZERO);
        resting.order.updated_at = timestamp;
        resting.order.status = if resting.remaining <= Decimal::ZERO {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };
        self.apply_fill_accounting(&fill).await;
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
                symbol: order.request.symbol,
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
                symbol: order.request.symbol,
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
        self.advance_time(timestamp).await;
        let mut generated = Vec::new();
        if quantity <= Decimal::ZERO {
            return generated;
        }

        let trade_size = quantity;
        let mut open = self.open_orders.lock().await;
        let mut finished = Vec::new();
        for (order_id, resting) in open.iter_mut() {
            if resting.remaining <= Decimal::ZERO || !resting.active {
                continue;
            }
            if quantity <= Decimal::ZERO && resting.price != price {
                continue;
            }
            if !Self::trade_crosses_order(aggressor_side, price, resting) {
                if resting.price == price && resting.order.request.side != aggressor_side {
                    let incremental = self.passive_fillable_volume(resting, trade_size);
                    if incremental > Decimal::ZERO && quantity > Decimal::ZERO {
                        let fill_qty = incremental.min(resting.remaining).min(quantity);
                        self.apply_resting_fill(
                            order_id,
                            resting,
                            price,
                            fill_qty,
                            timestamp,
                            &mut generated,
                        )
                        .await;
                        quantity -= fill_qty;
                        if resting.remaining <= Decimal::ZERO {
                            finished.push(order_id.clone());
                        }
                    }
                }
                continue;
            }
            let trade_qty: Quantity = quantity.min(resting.remaining);
            quantity -= trade_qty;
            self.apply_resting_fill(
                order_id,
                resting,
                price,
                trade_qty,
                timestamp,
                &mut generated,
            )
            .await;
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
            let qty = event.order.request.quantity;
            let fee_amount = self.compute_fee(
                event.order.request.symbol,
                event.order.request.side,
                LiquidityRole::Taker,
                event.fill_price,
                qty,
            );
            let fill = self.build_fill(
                &event.order.id,
                event.order.request.symbol,
                event.order.request.side,
                event.fill_price,
                qty,
                fee_amount,
                event.timestamp,
            );
            self.apply_fill_accounting(&fill).await;
            store.push(fill);
        }
    }

    fn trade_crosses_order(side: Side, trade_price: Price, order: &RestingOrder) -> bool {
        match side {
            Side::Buy => order.order.request.side == Side::Sell && order.price < trade_price,
            Side::Sell => order.order.request.side == Side::Buy && order.price > trade_price,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_fill(
        &self,
        order_id: &OrderId,
        symbol: Symbol,
        side: Side,
        price: Price,
        qty: Quantity,
        fee: Decimal,
        timestamp: DateTime<Utc>,
    ) -> Fill {
        let fee_asset = if fee.is_zero() {
            None
        } else {
            Some(*self.cash_asset.lock().unwrap())
        };
        Fill {
            order_id: order_id.clone(),
            symbol,
            side,
            fill_price: price,
            fill_quantity: qty,
            fee: if fee.is_zero() { None } else { Some(fee) },
            fee_asset,
            timestamp,
        }
    }

    async fn record_resting_order(
        &self,
        order: Order,
        price: Price,
        activation_time: DateTime<Utc>,
    ) {
        let original_quantity = order.request.quantity;
        let mut entry = RestingOrder {
            price,
            remaining: (order.request.quantity - order.filled_quantity).max(Decimal::ZERO),
            order,
            initial_queue_position: Decimal::ZERO,
            processed_volume: Decimal::ZERO,
            activation_time,
            cancel_after: None,
            active: self.latency <= ChronoDuration::zero(),
            original_quantity,
        };
        if entry.active && entry.remaining > Decimal::ZERO {
            entry.initial_queue_position =
                self.queue_ahead_for(entry.order.request.side, entry.price, Decimal::ZERO);
            let mut book = self.resting_depth.lock().unwrap();
            book.add_order(entry.order.request.side, entry.price, entry.remaining);
        }
        if entry.active {
            entry.order.status = OrderStatus::Accepted;
            entry.order.updated_at = activation_time;
        }
        let mut open = self.open_orders.lock().await;
        open.insert(entry.order.id.clone(), entry);
    }

    async fn apply_fill_accounting(&self, fill: &Fill) {
        let cash_asset = *self.cash_asset.lock().unwrap();
        let mut balances = self.balances.lock().await;
        if let Some(balance) = balances.iter_mut().find(|b| b.asset == cash_asset) {
            let notional = fill.fill_price * fill.fill_quantity;
            let fee = fill.fee.unwrap_or(Decimal::ZERO);
            match fill.side {
                Side::Buy => balance.available -= notional + fee,
                Side::Sell => balance.available += notional - fee,
            }
            balance.total = balance.available;
            balance.updated_at = fill.timestamp;
        }
        drop(balances);

        let mut positions = self.positions.lock().await;
        let position = positions.entry(fill.symbol).or_insert(Position {
            symbol: fill.symbol,
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
            let fee_amount = self.compute_fee(
                order.request.symbol,
                order.request.side,
                LiquidityRole::Taker,
                *price,
                *qty,
            );
            let fill = self.build_fill(
                &order.id,
                order.request.symbol,
                order.request.side,
                *price,
                *qty,
                fee_amount,
                Utc::now(),
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

                info!(
                    symbol = %order.request.symbol,
                    qty = %order.request.quantity,
                    price = ?order.avg_fill_price,
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

    async fn cancel_order(&self, _order_id: OrderId, _symbol: Symbol) -> BrokerResult<()> {
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

    async fn list_open_orders(&self, _symbol: Symbol) -> BrokerResult<Vec<Order>> {
        Ok(Vec::new())
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
                symbol: self.symbol,
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
            symbol,
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
        let with_exchange = self.symbol.to_string();
        let without_exchange = self.symbol.code();
        if !symbols.is_empty()
            && !symbols
                .iter()
                .any(|s| s == &with_exchange || s == without_exchange)
        {
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

struct FanInPaperStream {
    pending: Vec<LivePaperStream>,
    tick_rx: Option<mpsc::Receiver<Tick>>,
    candle_rx: Option<mpsc::Receiver<Candle>>,
    book_rx: Option<mpsc::Receiver<OrderBook>>,
    tasks: Vec<JoinHandle<()>>,
}

impl FanInPaperStream {
    fn new(streams: Vec<LivePaperStream>) -> Self {
        Self {
            pending: streams,
            tick_rx: None,
            candle_rx: None,
            book_rx: None,
            tasks: Vec::new(),
        }
    }

    fn ensure_started(&mut self) {
        if self.tick_rx.is_some() {
            return;
        }
        let (tick_tx, tick_rx) = mpsc::channel(2048);
        let (candle_tx, candle_rx) = mpsc::channel(512);
        let (book_tx, book_rx) = mpsc::channel(256);
        let mut handles = Vec::new();
        for mut stream in self.pending.drain(..) {
            let tick_tx = tick_tx.clone();
            let candle_tx = candle_tx.clone();
            let book_tx = book_tx.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    match stream.next_tick().await {
                        Ok(Some(event)) => {
                            if tick_tx.send(event).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(err) => {
                            warn!(error = %err, "paper stream tick failed");
                            break;
                        }
                    }

                    match stream.next_candle().await {
                        Ok(Some(event)) => {
                            if candle_tx.send(event).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(err) => {
                            warn!(error = %err, "paper stream candle failed");
                            break;
                        }
                    }

                    match stream.next_order_book().await {
                        Ok(Some(event)) => {
                            if book_tx.send(event).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(err) => {
                            warn!(error = %err, "paper stream book failed");
                            break;
                        }
                    }
                }
            }));
        }
        self.tick_rx = Some(tick_rx);
        self.candle_rx = Some(candle_rx);
        self.book_rx = Some(book_rx);
        self.tasks = handles;
    }
}

#[async_trait]
impl ConnectorStream for FanInPaperStream {
    async fn subscribe(&mut self, symbols: &[String], interval: Interval) -> BrokerResult<()> {
        for stream in &mut self.pending {
            stream.subscribe(symbols, interval).await?;
        }
        self.ensure_started();
        Ok(())
    }

    async fn next_tick(&mut self) -> BrokerResult<Option<Tick>> {
        if let Some(rx) = &mut self.tick_rx {
            Ok(rx.recv().await)
        } else {
            Ok(None)
        }
    }

    async fn next_candle(&mut self) -> BrokerResult<Option<Candle>> {
        if let Some(rx) = &mut self.candle_rx {
            Ok(rx.recv().await)
        } else {
            Ok(None)
        }
    }

    async fn next_order_book(&mut self) -> BrokerResult<Option<OrderBook>> {
        if let Some(rx) = &mut self.book_rx {
            Ok(rx.recv().await)
        } else {
            Ok(None)
        }
    }
}

impl Drop for FanInPaperStream {
    fn drop(&mut self) {
        for handle in self.tasks.drain(..) {
            handle.abort();
        }
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
                    symbol,
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
                    symbol,
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
    let mut candle_builder = CandleBuilder::new(symbol);
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
            symbol,
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
    let mut candle_builder = CandleBuilder::new(symbol);
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
            symbol,
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
                markets: vec![symbol.code().to_string()],
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
    use serde_json::json;
    use std::collections::HashSet;
    use tesser_core::OrderBookLevel;

    #[tokio::test(flavor = "current_thread")]
    async fn paper_factory_supports_multi_symbol_streams() -> BrokerResult<()> {
        let factory = PaperFactory::default();
        let config = json!({
            "symbols": ["bybit_linear:BTCUSDT", "bybit_linear:ETHUSDT"],
            "market": {
                "mode": "random_walk",
                "start_price": "25000",
                "volatility": 0.001,
                "interval_ms": 10
            }
        });
        let mut stream = factory
            .create_market_stream(&config, ConnectorStreamConfig::default())
            .await?;
        stream
            .subscribe(
                &["bybit_linear:BTCUSDT".into(), "bybit_linear:ETHUSDT".into()],
                Interval::OneMinute,
            )
            .await?;
        let mut seen = HashSet::new();
        while seen.len() < 2 {
            let tick = tokio::time::timeout(TokioDuration::from_millis(500), stream.next_tick())
                .await
                .map_err(|_| BrokerError::Other("timed out waiting for paper tick".into()))??;
            if let Some(event) = tick {
                seen.insert(event.symbol);
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn matching_engine_amend_updates_resting_state() {
        let engine = MatchingEngine::new(
            "paper",
            vec![Symbol::from("BTCUSDT")],
            Decimal::from(10_000),
        );
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
            symbol: order.request.symbol,
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

    #[tokio::test(flavor = "current_thread")]
    async fn queue_model_waits_for_depth_before_fill() {
        let engine = MatchingEngine::with_config(
            "paper",
            vec![Symbol::from("BTCUSDT")],
            Decimal::from(10_000),
            MatchingEngineConfig {
                latency: ChronoDuration::zero(),
                queue_model: QueueModel::Conservative,
                fee_model: FeeScheduleConfig::default().build_model(),
                cash_asset: None,
            },
        );
        let book_time = Utc::now();
        let snapshot = OrderBook {
            symbol: "BTCUSDT".into(),
            bids: vec![OrderBookLevel {
                price: Decimal::from(9_950),
                size: Decimal::from(2),
            }],
            asks: vec![OrderBookLevel {
                price: Decimal::from(10_000),
                size: Decimal::from(1),
            }],
            timestamp: book_time,
            exchange_checksum: None,
            local_checksum: None,
        };
        engine.load_market_snapshot(&snapshot);
        engine.advance_time(book_time).await;

        let request = OrderRequest {
            symbol: "BTCUSDT".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: Decimal::ONE,
            price: Some(Decimal::from(9_950)),
            trigger_price: None,
            time_in_force: Some(TimeInForce::GoodTilCanceled),
            client_order_id: None,
            take_profit: None,
            stop_loss: None,
            display_quantity: None,
        };

        let order = engine.place_order(request).await.unwrap();
        assert_eq!(order.status, OrderStatus::Accepted);

        let ts = book_time;
        engine
            .process_trade(Side::Sell, Decimal::from(9_950), Decimal::ONE, ts)
            .await;
        assert!(engine.drain_fills().await.is_empty());

        engine
            .process_trade(
                Side::Sell,
                Decimal::from(9_950),
                Decimal::ONE,
                ts + ChronoDuration::milliseconds(1),
            )
            .await;
        assert!(engine.drain_fills().await.is_empty());

        engine
            .process_trade(
                Side::Sell,
                Decimal::from(9_950),
                Decimal::ONE,
                ts + ChronoDuration::milliseconds(2),
            )
            .await;
        let fills = engine.drain_fills().await;
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_quantity, Decimal::ONE);
        assert_eq!(fills[0].fill_price, Decimal::from(9_950));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn latency_delays_limit_order_activation() {
        let engine = MatchingEngine::with_config(
            "paper",
            vec![Symbol::from("BTCUSDT")],
            Decimal::from(10_000),
            MatchingEngineConfig {
                latency: ChronoDuration::milliseconds(50),
                queue_model: QueueModel::Optimistic,
                fee_model: FeeScheduleConfig::default().build_model(),
                cash_asset: None,
            },
        );
        let book_time = Utc::now();
        let snapshot = OrderBook {
            symbol: "BTCUSDT".into(),
            bids: vec![OrderBookLevel {
                price: Decimal::from(9_900),
                size: Decimal::from(1),
            }],
            asks: vec![OrderBookLevel {
                price: Decimal::from(10_000),
                size: Decimal::from(1),
            }],
            timestamp: book_time,
            exchange_checksum: None,
            local_checksum: None,
        };
        engine.load_market_snapshot(&snapshot);
        engine.advance_time(book_time).await;

        let request = OrderRequest {
            symbol: "BTCUSDT".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: Decimal::ONE,
            price: Some(Decimal::from(9_900)),
            trigger_price: None,
            time_in_force: Some(TimeInForce::GoodTilCanceled),
            client_order_id: None,
            take_profit: None,
            stop_loss: None,
            display_quantity: None,
        };

        let order = engine.place_order(request).await.unwrap();
        assert_eq!(order.status, OrderStatus::PendingNew);
        let mut open = engine
            .list_open_orders(Symbol::from("BTCUSDT"))
            .await
            .unwrap();
        assert_eq!(open[0].status, OrderStatus::PendingNew);

        let ts = book_time;
        engine
            .process_trade(Side::Sell, Decimal::from(9_900), Decimal::ONE, ts)
            .await;
        assert!(engine.drain_fills().await.is_empty());
        open = engine
            .list_open_orders(Symbol::from("BTCUSDT"))
            .await
            .unwrap();
        assert_eq!(open[0].status, OrderStatus::PendingNew);

        engine
            .process_trade(
                Side::Sell,
                Decimal::from(9_900),
                Decimal::ONE,
                ts + ChronoDuration::milliseconds(75),
            )
            .await;
        let fills = engine.drain_fills().await;
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_quantity, Decimal::ONE);
        let open = engine
            .list_open_orders(Symbol::from("BTCUSDT"))
            .await
            .unwrap();
        assert!(open.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn optimistic_queue_fills_even_with_depth_ahead() {
        let engine = MatchingEngine::with_config(
            "paper",
            vec![Symbol::from("BTCUSDT")],
            Decimal::from(10_000),
            MatchingEngineConfig {
                latency: ChronoDuration::zero(),
                queue_model: QueueModel::Optimistic,
                fee_model: FeeScheduleConfig::default().build_model(),
                cash_asset: None,
            },
        );
        let book_time = Utc::now();
        let snapshot = OrderBook {
            symbol: "BTCUSDT".into(),
            bids: vec![OrderBookLevel {
                price: Decimal::from(9_900),
                size: Decimal::from(10),
            }],
            asks: vec![OrderBookLevel {
                price: Decimal::from(10_000),
                size: Decimal::from(10),
            }],
            timestamp: book_time,
            exchange_checksum: None,
            local_checksum: None,
        };
        engine.load_market_snapshot(&snapshot);
        engine.advance_time(book_time).await;

        let request = OrderRequest {
            symbol: "BTCUSDT".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: Decimal::new(5, 0),
            price: Some(Decimal::from(9_900)),
            trigger_price: None,
            time_in_force: Some(TimeInForce::GoodTilCanceled),
            client_order_id: None,
            take_profit: None,
            stop_loss: None,
            display_quantity: None,
        };
        engine.place_order(request).await.unwrap();

        let ts = book_time;
        engine
            .process_trade(Side::Sell, Decimal::from(9_900), Decimal::new(2, 0), ts)
            .await;
        let fills = engine.drain_fills().await;
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].fill_quantity, Decimal::new(2, 0));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fee_schedule_distinguishes_maker_and_taker() {
        let fee_cfg =
            FeeScheduleConfig::with_defaults(Decimal::ZERO, Decimal::from_f64(0.5).unwrap());
        let engine = MatchingEngine::with_config(
            "paper",
            vec![Symbol::from("BTCUSDT")],
            Decimal::from(10_000),
            MatchingEngineConfig {
                latency: ChronoDuration::zero(),
                queue_model: QueueModel::Optimistic,
                fee_model: fee_cfg.build_model(),
                cash_asset: None,
            },
        );
        let book_time = Utc::now();
        let snapshot = OrderBook {
            symbol: "BTCUSDT".into(),
            bids: vec![OrderBookLevel {
                price: Decimal::from(9_900),
                size: Decimal::from(5),
            }],
            asks: vec![OrderBookLevel {
                price: Decimal::from(10_000),
                size: Decimal::from(5),
            }],
            timestamp: book_time,
            exchange_checksum: None,
            local_checksum: None,
        };
        engine.load_market_snapshot(&snapshot);
        engine.advance_time(book_time).await;

        let taker_request = OrderRequest {
            symbol: "BTCUSDT".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: Decimal::ONE,
            price: Some(Decimal::from(10_100)),
            trigger_price: None,
            time_in_force: Some(TimeInForce::GoodTilCanceled),
            client_order_id: None,
            take_profit: None,
            stop_loss: None,
            display_quantity: None,
        };
        let _ = engine.place_order(taker_request).await.unwrap();
        let mut fills = engine.drain_fills().await;
        assert_eq!(fills.len(), 1);
        let taker_fee = fills.pop().and_then(|fill| fill.fee).unwrap();
        assert!(taker_fee > Decimal::ZERO);

        let maker_request = OrderRequest {
            symbol: "BTCUSDT".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: Decimal::ONE,
            price: Some(Decimal::from(9_900)),
            trigger_price: None,
            time_in_force: Some(TimeInForce::GoodTilCanceled),
            client_order_id: None,
            take_profit: None,
            stop_loss: None,
            display_quantity: None,
        };
        let order = engine.place_order(maker_request).await.unwrap();
        assert_eq!(order.status, OrderStatus::Accepted);
        engine
            .process_trade(
                Side::Sell,
                Decimal::from(9_900),
                Decimal::ONE,
                book_time + ChronoDuration::milliseconds(1),
            )
            .await;
        let fills = engine.drain_fills().await;
        let maker_fee = fills
            .last()
            .and_then(|fill| fill.fee)
            .unwrap_or(Decimal::ZERO);
        assert!(maker_fee.is_zero());
    }
}
