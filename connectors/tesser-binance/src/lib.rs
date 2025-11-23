use async_trait::async_trait;
use binance_sdk::{
    common::{config::ConfigurationRestApi, errors::ConnectorError, models::RestApiResponse},
    derivatives_trading_usds_futures::{
        self as binance_futures,
        rest_api::{
            self, NewOrderParams, NewOrderPriceMatchEnum, NewOrderSideEnum, NewOrderTimeInForceEnum,
        },
        websocket_streams,
    },
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use std::{any::Any, collections::HashMap, num::NonZeroU32, sync::Arc, time::Duration};
use tesser_broker::{
    register_connector_factory, BrokerError, BrokerInfo, BrokerResult, ConnectorFactory,
    ConnectorStream, ConnectorStreamConfig, ExecutionClient, MarketStream, Quota, RateLimiter,
    RateLimiterError,
};
use tesser_core::{
    AccountBalance, AssetId, Candle, ExchangeId, Fill, Instrument, InstrumentKind, Order,
    OrderBook, OrderRequest, OrderStatus, OrderType, OrderUpdateRequest, Position, Side, Symbol,
    TimeInForce,
};
use tokio::time::sleep;
use uuid::Uuid;

pub mod ws;

pub use ws::{
    extract_order_update, BinanceMarketStream, BinanceSubscription, BinanceUserDataStream,
    UserDataStreamEventsResponse,
};

const BINANCE_DEFAULT_WEIGHT_LIMIT: u32 = 1_200;
const WEIGHT_BACKOFF_RATIO: f32 = 0.9;
const ORDER_WEIGHT: u32 = 1;
const CANCEL_WEIGHT: u32 = 1;
const QUERY_WEIGHT: u32 = 5;

#[derive(Clone)]
pub struct BinanceCredentials {
    pub api_key: String,
    pub api_secret: String,
}

pub struct BinanceConfig {
    pub rest_url: String,
    pub ws_url: String,
    pub recv_window: u64,
    pub weight_limit_per_minute: u32,
}

impl Default for BinanceConfig {
    fn default() -> Self {
        Self {
            rest_url: "https://fapi.binance.com".to_string(),
            ws_url: "wss://fstream.binance.com/stream".to_string(),
            recv_window: 5_000,
            weight_limit_per_minute: BINANCE_DEFAULT_WEIGHT_LIMIT,
        }
    }
}

impl BinanceConfig {
    pub fn testnet() -> Self {
        Self {
            rest_url: "https://testnet.binancefuture.com".to_string(),
            ws_url: "wss://stream.binancefuture.com/stream".to_string(),
            ..Self::default()
        }
    }
}

pub struct BinanceClient {
    rest: Arc<rest_api::RestApi>,
    info: BrokerInfo,
    credentials: Option<BinanceCredentials>,
    config: BinanceConfig,
    weight_limiter: Option<RateLimiter>,
    exchange: ExchangeId,
}

impl BinanceClient {
    pub fn new(
        config: BinanceConfig,
        credentials: Option<BinanceCredentials>,
        exchange: ExchangeId,
    ) -> Self {
        let mut builder = ConfigurationRestApi::builder();
        builder = builder.base_path(config.rest_url.clone());
        if let Some(creds) = credentials.as_ref() {
            builder = builder
                .api_key(creds.api_key.clone())
                .api_secret(creds.api_secret.clone());
        }
        let rest_cfg = builder
            .build()
            .expect("failed to build Binance REST configuration");
        let rest = binance_futures::DerivativesTradingUsdsFuturesRestApi::from_config(rest_cfg);
        let supports_testnet = config.rest_url.contains("testnet");
        let weight_limiter = NonZeroU32::new(config.weight_limit_per_minute)
            .map(Quota::per_minute)
            .map(RateLimiter::direct);
        Self {
            rest: Arc::new(rest),
            info: BrokerInfo {
                name: "binance".into(),
                markets: vec!["usd_perp".into()],
                supports_testnet,
            },
            credentials,
            config,
            weight_limiter,
            exchange,
        }
    }

    pub fn credentials(&self) -> Option<BinanceCredentials> {
        self.credentials.clone()
    }

    pub fn ws_url(&self) -> &str {
        &self.config.ws_url
    }

    pub fn rest_url(&self) -> &str {
        &self.config.rest_url
    }

    pub fn recv_window(&self) -> u64 {
        self.config.recv_window
    }

    pub fn rest(&self) -> Arc<rest_api::RestApi> {
        Arc::clone(&self.rest)
    }

    pub fn exchange(&self) -> ExchangeId {
        self.exchange
    }

    async fn throttle_weight(&self, units: u32) -> BrokerResult<()> {
        if units == 0 {
            return Ok(());
        }
        if let Some(limiter) = &self.weight_limiter {
            if let Some(nonzero) = NonZeroU32::new(units) {
                limiter
                    .until_units_ready(nonzero)
                    .await
                    .map_err(Self::rate_limiter_error)?;
            }
        }
        Ok(())
    }

    async fn handle_weight_headers(&self, headers: &HashMap<String, String>) {
        if self.config.weight_limit_per_minute == 0 {
            return;
        }
        if let Some(value) = headers.get("x-mbx-used-weight-1m") {
            if let Ok(used) = value.parse::<u32>() {
                self.backoff_if_needed(used).await;
            }
        }
    }

    async fn backoff_if_needed(&self, used: u32) {
        let limit = self.config.weight_limit_per_minute;
        if limit == 0 {
            return;
        }
        if used >= limit {
            sleep(Duration::from_secs(1)).await;
        } else if (used as f32) / (limit as f32) >= WEIGHT_BACKOFF_RATIO {
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn parse_response<T>(&self, result: anyhow::Result<RestApiResponse<T>>) -> BrokerResult<T>
    where
        T: Send + 'static,
    {
        let response = result.map_err(|err| BrokerError::Transport(err.to_string()))?;
        self.handle_weight_headers(&response.headers).await;
        response.data().await.map_err(map_connector_error)
    }

    fn rate_limiter_error(err: RateLimiterError) -> BrokerError {
        BrokerError::Other(format!("rate limited: {err}"))
    }

    pub async fn start_user_stream(&self) -> BrokerResult<String> {
        self.throttle_weight(QUERY_WEIGHT).await?;
        let response = self
            .parse_response(self.rest.start_user_data_stream().await)
            .await?;
        response
            .listen_key
            .ok_or_else(|| BrokerError::Other("missing listenKey".into()))
    }

    pub async fn keepalive_user_stream(&self) -> BrokerResult<String> {
        self.throttle_weight(QUERY_WEIGHT).await?;
        let response = self
            .parse_response(self.rest.keepalive_user_data_stream().await)
            .await?;
        response
            .listen_key
            .ok_or_else(|| BrokerError::Other("missing listenKey".into()))
    }
}

#[async_trait]
impl ExecutionClient for BinanceClient {
    fn info(&self) -> BrokerInfo {
        self.info.clone()
    }

    async fn place_order(&self, request: OrderRequest) -> BrokerResult<Order> {
        self.throttle_weight(ORDER_WEIGHT).await?;
        let side = map_side(request.side);
        let order_type = map_order_type(request.order_type);
        let symbol_code = request.symbol.code().to_string();
        let mut builder = NewOrderParams::builder(symbol_code, side, order_type);
        let mut client_request = request.clone();
        if let Some(price) = request.price {
            builder = builder.price(Some(price));
        }
        builder = builder.quantity(Some(request.quantity));
        if let Some(trigger) = request.trigger_price {
            builder = builder.stop_price(Some(trigger));
        }
        let tif = match request
            .time_in_force
            .or_else(|| default_time_in_force(request.order_type))
        {
            Some(TimeInForce::GoodTilCanceled) => Some(NewOrderTimeInForceEnum::Gtc),
            Some(TimeInForce::ImmediateOrCancel) => Some(NewOrderTimeInForceEnum::Ioc),
            Some(TimeInForce::FillOrKill) => Some(NewOrderTimeInForceEnum::Fok),
            None => None,
        };
        if let Some(value) = tif {
            builder = builder.time_in_force(Some(value));
        }
        let client_id = client_request
            .client_order_id
            .clone()
            .unwrap_or_else(|| format!("tesser-{}", Uuid::new_v4()));
        client_request.client_order_id = Some(client_id.clone());
        builder = builder
            .new_client_order_id(Some(client_id))
            .price_match(Some(NewOrderPriceMatchEnum::None))
            .recv_window(Some(self.config.recv_window as i64));

        let params = builder
            .build()
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let response = self
            .parse_response(self.rest.new_order(params).await)
            .await?;
        Ok(build_order_from_response(response, client_request))
    }

    async fn cancel_order(&self, order_id: tesser_core::OrderId, symbol: &str) -> BrokerResult<()> {
        self.throttle_weight(CANCEL_WEIGHT).await?;
        let mut builder = rest_api::CancelOrderParams::builder(symbol.to_string())
            .recv_window(Some(self.config.recv_window as i64));
        if let Ok(id) = order_id.parse::<i64>() {
            builder = builder.order_id(Some(id));
        } else {
            builder = builder.orig_client_order_id(Some(order_id));
        }
        let params = builder
            .build()
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        self.parse_response(self.rest.cancel_order(params).await)
            .await?;
        Ok(())
    }

    async fn amend_order(&self, request: OrderUpdateRequest) -> BrokerResult<Order> {
        self.throttle_weight(ORDER_WEIGHT).await?;
        let new_price = request.new_price.ok_or_else(|| {
            BrokerError::InvalidRequest("amend requires new price for Binance".into())
        })?;
        let new_qty = request.new_quantity.ok_or_else(|| {
            BrokerError::InvalidRequest("amend requires new quantity for Binance".into())
        })?;
        let mut builder = rest_api::ModifyOrderParams::builder(
            request.symbol.code().to_string(),
            map_modify_side(request.side),
            new_qty,
            new_price,
        )
        .recv_window(Some(self.config.recv_window as i64));
        if let Ok(id) = request.order_id.parse::<i64>() {
            builder = builder.order_id(Some(id));
        } else {
            builder = builder.orig_client_order_id(Some(request.order_id.clone()));
        }
        let params = builder
            .build()
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let response = self
            .parse_response(self.rest.modify_order(params).await)
            .await?;
        Ok(build_order_from_modify_response(response, &request))
    }

    async fn list_open_orders(&self, symbol: &str) -> BrokerResult<Vec<Order>> {
        self.throttle_weight(QUERY_WEIGHT).await?;
        let params = rest_api::CurrentAllOpenOrdersParams::builder()
            .symbol(Some(symbol.to_string()))
            .recv_window(Some(self.config.recv_window as i64))
            .build()
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let raw = self
            .parse_response(self.rest.current_all_open_orders(params).await)
            .await?;
        let mut orders = Vec::new();
        for entry in raw {
            if let Some(order) = order_from_open_order(self.exchange, &entry) {
                orders.push(order);
            }
        }
        Ok(orders)
    }

    async fn account_balances(&self) -> BrokerResult<Vec<AccountBalance>> {
        self.throttle_weight(QUERY_WEIGHT).await?;
        let params = rest_api::FuturesAccountBalanceV3Params::builder()
            .recv_window(Some(self.config.recv_window as i64))
            .build()
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let balances = self
            .parse_response(self.rest.futures_account_balance_v3(params).await)
            .await?;
        Ok(balances
            .into_iter()
            .filter_map(|entry| balance_from_entry(self.exchange, &entry))
            .collect())
    }

    async fn positions(&self) -> BrokerResult<Vec<Position>> {
        self.throttle_weight(QUERY_WEIGHT).await?;
        let params = rest_api::PositionInformationV2Params::builder()
            .recv_window(Some(self.config.recv_window as i64))
            .build()
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let positions = self
            .parse_response(self.rest.position_information_v2(params).await)
            .await?;
        Ok(positions
            .into_iter()
            .filter_map(|entry| position_from_entry(self.exchange, &entry))
            .collect())
    }

    async fn list_instruments(&self, _category: &str) -> BrokerResult<Vec<Instrument>> {
        self.throttle_weight(QUERY_WEIGHT).await?;
        let info = self
            .parse_response(self.rest.exchange_information().await)
            .await?;
        let mut instruments = Vec::new();
        for symbol in info.symbols.unwrap_or_default() {
            if let Some(instr) = instrument_from_symbol(self.exchange, &symbol) {
                instruments.push(instr);
            }
        }
        Ok(instruments)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn build_order_from_response(response: rest_api::NewOrderResponse, request: OrderRequest) -> Order {
    let status = response
        .status
        .as_deref()
        .map(map_order_status)
        .unwrap_or(OrderStatus::PendingNew);
    Order {
        id: response
            .order_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| Utc::now().timestamp_millis().to_string()),
        request,
        status,
        filled_quantity: parse_decimal_opt(response.executed_qty.as_deref())
            .unwrap_or(Decimal::ZERO),
        avg_fill_price: parse_decimal_opt(response.avg_price.as_deref()),
        created_at: timestamp_from_ms(response.update_time),
        updated_at: timestamp_from_ms(response.update_time),
    }
}

fn build_order_from_modify_response(
    response: rest_api::ModifyOrderResponse,
    update: &OrderUpdateRequest,
) -> Order {
    let symbol = response
        .symbol
        .clone()
        .map(|code| Symbol::from_code(update.symbol.exchange, code))
        .unwrap_or(update.symbol);
    let order_type = response
        .r#type
        .as_deref()
        .map(map_order_type_from_str)
        .unwrap_or(OrderType::Limit);
    let quantity = parse_decimal_opt(response.orig_qty.as_deref())
        .or(update.new_quantity)
        .unwrap_or(Decimal::ZERO);
    let price = parse_decimal_opt(response.price.as_deref()).or(update.new_price);
    let request = OrderRequest {
        symbol,
        side: response
            .side
            .as_deref()
            .map(map_order_side)
            .unwrap_or(update.side),
        order_type,
        quantity,
        price,
        trigger_price: parse_decimal_opt(response.stop_price.as_deref()),
        time_in_force: response.time_in_force.as_deref().and_then(map_tif_from_str),
        client_order_id: response.client_order_id.clone(),
        take_profit: None,
        stop_loss: None,
        display_quantity: None,
    };
    Order {
        id: response
            .order_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| update.order_id.clone()),
        request,
        status: response
            .status
            .as_deref()
            .map(map_order_status)
            .unwrap_or(OrderStatus::PendingNew),
        filled_quantity: parse_decimal_opt(response.executed_qty.as_deref())
            .unwrap_or(Decimal::ZERO),
        avg_fill_price: parse_decimal_opt(response.avg_price.as_deref()),
        created_at: timestamp_from_ms(response.update_time),
        updated_at: timestamp_from_ms(response.update_time),
    }
}

fn order_from_open_order(
    exchange: ExchangeId,
    entry: &rest_api::AllOrdersResponseInner,
) -> Option<Order> {
    let symbol = entry
        .symbol
        .as_deref()
        .map(|code| Symbol::from_code(exchange, code))?;
    let request = OrderRequest {
        symbol,
        side: entry
            .side
            .as_deref()
            .map(map_order_side)
            .unwrap_or(Side::Buy),
        order_type: entry
            .r#type
            .as_deref()
            .map(map_order_type_from_str)
            .unwrap_or(OrderType::Limit),
        quantity: parse_decimal_opt(entry.orig_qty.as_deref())?,
        price: parse_decimal_opt(entry.price.as_deref()),
        trigger_price: parse_decimal_opt(entry.stop_price.as_deref()),
        time_in_force: entry.time_in_force.as_deref().and_then(map_tif_from_str),
        client_order_id: entry.client_order_id.clone(),
        take_profit: None,
        stop_loss: None,
        display_quantity: None,
    };
    Some(Order {
        id: entry
            .order_id
            .map(|id| id.to_string())
            .or_else(|| entry.client_order_id.clone())?,
        request,
        status: entry
            .status
            .as_deref()
            .map(map_order_status)
            .unwrap_or(OrderStatus::PendingNew),
        filled_quantity: parse_decimal_opt(entry.executed_qty.as_deref()).unwrap_or(Decimal::ZERO),
        avg_fill_price: parse_decimal_opt(entry.avg_price.as_deref()),
        created_at: timestamp_from_ms(entry.time),
        updated_at: timestamp_from_ms(entry.update_time),
    })
}

fn balance_from_entry(
    exchange: ExchangeId,
    entry: &rest_api::FuturesAccountBalanceV2ResponseInner,
) -> Option<AccountBalance> {
    let asset = entry
        .asset
        .as_deref()
        .map(|code| AssetId::from_code(exchange, code))?;
    let total = parse_decimal_opt(entry.balance.as_deref())?;
    let available = parse_decimal_opt(entry.available_balance.as_deref()).unwrap_or(total);
    Some(AccountBalance {
        exchange,
        asset,
        total,
        available,
        updated_at: timestamp_from_ms(entry.update_time),
    })
}

fn position_from_entry(
    exchange: ExchangeId,
    entry: &rest_api::PositionInformationV2ResponseInner,
) -> Option<Position> {
    let qty = parse_decimal_opt(entry.position_amt.as_deref())?;
    if qty.is_zero() {
        return None;
    }
    let side = if qty.is_sign_positive() {
        Some(Side::Buy)
    } else {
        Some(Side::Sell)
    };
    let symbol = entry
        .symbol
        .as_deref()
        .map(|code| Symbol::from_code(exchange, code))?;
    Some(Position {
        symbol,
        side,
        quantity: qty.abs(),
        entry_price: parse_decimal_opt(entry.entry_price.as_deref()),
        unrealized_pnl: parse_decimal_opt(entry.un_realized_profit.as_deref())
            .unwrap_or(Decimal::ZERO),
        updated_at: timestamp_from_ms(entry.update_time),
    })
}

fn instrument_from_symbol(
    exchange: ExchangeId,
    symbol: &rest_api::ExchangeInformationResponseSymbolsInner,
) -> Option<Instrument> {
    let symbol_name = symbol
        .symbol
        .as_deref()
        .map(|code| Symbol::from_code(exchange, code))?;
    let base = symbol
        .base_asset
        .as_deref()
        .map(|code| AssetId::from_code(exchange, code))?;
    let quote = symbol
        .quote_asset
        .as_deref()
        .map(|code| AssetId::from_code(exchange, code))?;
    let settlement = symbol
        .margin_asset
        .as_deref()
        .map(|code| AssetId::from_code(exchange, code))
        .unwrap_or(quote);
    let mut tick_size = Decimal::ONE;
    let mut lot_size = Decimal::ONE;
    if let Some(filters) = &symbol.filters {
        for filter in filters {
            match filter.filter_type.as_deref() {
                Some("PRICE_FILTER") => {
                    if let Some(tick) = parse_decimal_opt(filter.tick_size.as_deref()) {
                        tick_size = tick;
                    }
                }
                Some("LOT_SIZE") => {
                    if let Some(step) = parse_decimal_opt(filter.step_size.as_deref()) {
                        lot_size = step;
                    }
                }
                _ => {}
            }
        }
    }

    Some(Instrument {
        symbol: symbol_name,
        base,
        quote,
        kind: InstrumentKind::LinearPerpetual,
        settlement_currency: settlement,
        tick_size,
        lot_size,
    })
}

fn map_order_status(value: &str) -> OrderStatus {
    match value {
        "NEW" => OrderStatus::Accepted,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "EXPIRED" => OrderStatus::Canceled,
        "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::PendingNew,
    }
}

fn map_order_side(value: &str) -> Side {
    match value {
        "SELL" => Side::Sell,
        _ => Side::Buy,
    }
}

fn map_side(value: Side) -> NewOrderSideEnum {
    match value {
        Side::Buy => NewOrderSideEnum::Buy,
        Side::Sell => NewOrderSideEnum::Sell,
    }
}

fn map_modify_side(value: Side) -> rest_api::ModifyOrderSideEnum {
    match value {
        Side::Buy => rest_api::ModifyOrderSideEnum::Buy,
        Side::Sell => rest_api::ModifyOrderSideEnum::Sell,
    }
}

fn map_order_type(order_type: OrderType) -> String {
    match order_type {
        OrderType::Market => "MARKET".into(),
        OrderType::Limit => "LIMIT".into(),
        OrderType::StopMarket => "STOP_MARKET".into(),
    }
}

fn map_order_type_from_str(value: &str) -> OrderType {
    match value {
        "MARKET" => OrderType::Market,
        "STOP_MARKET" | "TAKE_PROFIT_MARKET" => OrderType::StopMarket,
        _ => OrderType::Limit,
    }
}

fn map_tif_from_str(value: &str) -> Option<TimeInForce> {
    match value {
        "GTC" => Some(TimeInForce::GoodTilCanceled),
        "IOC" => Some(TimeInForce::ImmediateOrCancel),
        "FOK" => Some(TimeInForce::FillOrKill),
        _ => None,
    }
}

fn default_time_in_force(order_type: OrderType) -> Option<TimeInForce> {
    match order_type {
        OrderType::Limit => Some(TimeInForce::GoodTilCanceled),
        _ => None,
    }
}

pub fn parse_decimal_opt(value: Option<&str>) -> Option<Decimal> {
    value.and_then(|v| v.parse::<Decimal>().ok())
}

pub fn timestamp_from_ms(value: Option<i64>) -> DateTime<Utc> {
    value
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or_else(Utc::now)
}

pub fn fill_from_update(
    exchange: ExchangeId,
    order: &websocket_streams::OrderTradeUpdateO,
) -> Option<Fill> {
    let last_qty = parse_decimal_opt(order.l.as_deref())?;
    if last_qty.is_zero() {
        return None;
    }
    let price = parse_decimal_opt(order.l_uppercase.as_deref())
        .or_else(|| parse_decimal_opt(order.p.as_deref()))?;
    let side = order
        .s_uppercase
        .as_deref()
        .map(map_order_side)
        .unwrap_or(Side::Buy);
    let symbol = order
        .s
        .as_deref()
        .map(|code| Symbol::from_code(exchange, code))
        .unwrap_or(Symbol::unspecified());
    Some(Fill {
        order_id: order.i.map(|id| id.to_string()).unwrap_or_default(),
        symbol,
        side,
        fill_price: price,
        fill_quantity: last_qty,
        fee: parse_decimal_opt(order.n.as_deref()),
        timestamp: timestamp_from_ms(order.t_uppercase),
    })
}

pub fn order_from_update(
    exchange: ExchangeId,
    order: &websocket_streams::OrderTradeUpdateO,
) -> Option<Order> {
    let symbol = order
        .s
        .as_deref()
        .map(|code| Symbol::from_code(exchange, code))?;
    let request = OrderRequest {
        symbol,
        side: order
            .s_uppercase
            .as_deref()
            .map(map_order_side)
            .unwrap_or(Side::Buy),
        order_type: order
            .o
            .as_deref()
            .map(map_order_type_from_str)
            .unwrap_or(OrderType::Limit),
        quantity: parse_decimal_opt(order.q.as_deref()).unwrap_or(Decimal::ZERO),
        price: parse_decimal_opt(order.p.as_deref()),
        trigger_price: parse_decimal_opt(order.sp.as_deref()),
        time_in_force: order.f.as_deref().and_then(map_tif_from_str),
        client_order_id: order.c.clone(),
        take_profit: None,
        stop_loss: None,
        display_quantity: None,
    };
    Some(Order {
        id: order.i.map(|v| v.to_string()).unwrap_or_default(),
        request,
        status: order
            .x_uppercase
            .as_deref()
            .map(map_order_status)
            .unwrap_or(OrderStatus::PendingNew),
        filled_quantity: parse_decimal_opt(order.z.as_deref()).unwrap_or(Decimal::ZERO),
        avg_fill_price: parse_decimal_opt(order.ap.as_deref()),
        created_at: timestamp_from_ms(order.t_uppercase),
        updated_at: timestamp_from_ms(order.t_uppercase),
    })
}

#[derive(Clone, Debug, Deserialize)]
struct BinanceConnectorConfig {
    #[serde(default = "default_rest_url")]
    rest_url: String,
    #[serde(default = "default_ws_url")]
    ws_url: String,
    #[serde(default = "default_exchange_name")]
    exchange: String,
    #[serde(default)]
    api_key: String,
    #[serde(default)]
    api_secret: String,
    #[serde(default = "default_recv_window")]
    recv_window: u64,
    #[serde(default = "default_weight_limit_per_minute")]
    weight_limit_per_minute: u32,
}

fn default_rest_url() -> String {
    "https://fapi.binance.com".into()
}

fn default_ws_url() -> String {
    "wss://fstream.binance.com/stream".into()
}

fn default_exchange_name() -> String {
    "binance_perp".into()
}

fn default_recv_window() -> u64 {
    5_000
}

fn default_weight_limit_per_minute() -> u32 {
    BINANCE_DEFAULT_WEIGHT_LIMIT
}

#[derive(Default)]
pub struct BinanceFactory;

impl BinanceFactory {
    fn parse_config(&self, value: &Value) -> BrokerResult<BinanceConnectorConfig> {
        serde_json::from_value(value.clone()).map_err(|err| {
            BrokerError::InvalidRequest(format!("invalid binance connector config: {err}"))
        })
    }

    fn credentials(cfg: &BinanceConnectorConfig) -> Option<BinanceCredentials> {
        if cfg.api_key.trim().is_empty() || cfg.api_secret.trim().is_empty() {
            None
        } else {
            Some(BinanceCredentials {
                api_key: cfg.api_key.clone(),
                api_secret: cfg.api_secret.clone(),
            })
        }
    }
}

const BINANCE_DEFAULT_DEPTH: usize = 50;

pub fn register_factory() {
    register_connector_factory(Arc::new(BinanceFactory));
}

#[async_trait]
impl ConnectorFactory for BinanceFactory {
    fn name(&self) -> &str {
        "binance"
    }

    async fn create_execution_client(
        &self,
        config: &Value,
    ) -> BrokerResult<Arc<dyn ExecutionClient>> {
        let cfg = self.parse_config(config)?;
        let exchange = ExchangeId::from(cfg.exchange.as_str());
        let binance_cfg = BinanceConfig {
            rest_url: cfg.rest_url.clone(),
            ws_url: cfg.ws_url.clone(),
            recv_window: cfg.recv_window,
            weight_limit_per_minute: cfg.weight_limit_per_minute,
        };
        Ok(Arc::new(BinanceClient::new(
            binance_cfg,
            Self::credentials(&cfg),
            exchange,
        )))
    }

    async fn create_market_stream(
        &self,
        config: &Value,
        stream_config: ConnectorStreamConfig,
    ) -> BrokerResult<Box<dyn ConnectorStream>> {
        let cfg = self.parse_config(config)?;
        let exchange = ExchangeId::from(cfg.exchange.as_str());
        let stream = BinanceMarketStream::connect(
            &cfg.ws_url,
            &cfg.rest_url,
            stream_config.connection_status,
            exchange,
        )
        .await?;
        Ok(Box::new(BinanceConnectorStream::new(
            stream,
            stream_config
                .metadata
                .get("orderbook_depth")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize)
                .unwrap_or(BINANCE_DEFAULT_DEPTH),
        )))
    }
}

struct BinanceConnectorStream {
    inner: BinanceMarketStream,
    depth: usize,
}

impl BinanceConnectorStream {
    fn new(inner: BinanceMarketStream, depth: usize) -> Self {
        Self { inner, depth }
    }
}

#[async_trait]
impl ConnectorStream for BinanceConnectorStream {
    async fn subscribe(
        &mut self,
        symbols: &[String],
        interval: tesser_core::Interval,
    ) -> BrokerResult<()> {
        for symbol in symbols {
            self.inner
                .subscribe(BinanceSubscription::Trades {
                    symbol: symbol.clone(),
                })
                .await?;
            self.inner
                .subscribe(BinanceSubscription::Kline {
                    symbol: symbol.clone(),
                    interval,
                })
                .await?;
            self.inner
                .subscribe(BinanceSubscription::OrderBook {
                    symbol: symbol.clone(),
                    depth: self.depth,
                })
                .await?;
        }
        Ok(())
    }

    async fn next_tick(&mut self) -> BrokerResult<Option<tesser_core::Tick>> {
        self.inner.next_tick().await
    }

    async fn next_candle(&mut self) -> BrokerResult<Option<Candle>> {
        self.inner.next_candle().await
    }

    async fn next_order_book(&mut self) -> BrokerResult<Option<OrderBook>> {
        self.inner.next_order_book().await
    }
}

fn map_connector_error(err: ConnectorError) -> BrokerError {
    match err {
        ConnectorError::UnauthorizedError(msg) => BrokerError::Authentication(msg),
        ConnectorError::ForbiddenError(msg)
        | ConnectorError::TooManyRequestsError(msg)
        | ConnectorError::RateLimitBanError(msg)
        | ConnectorError::ServerError { msg, .. } => BrokerError::Exchange(msg),
        ConnectorError::NetworkError(msg) => BrokerError::Transport(msg),
        ConnectorError::NotFoundError(msg) | ConnectorError::BadRequestError(msg) => {
            BrokerError::InvalidRequest(msg)
        }
        ConnectorError::ConnectorClientError(msg) => BrokerError::Other(msg),
    }
}
