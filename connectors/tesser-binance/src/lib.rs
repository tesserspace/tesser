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
use std::{any::Any, sync::Arc};
use tesser_broker::{BrokerError, BrokerInfo, BrokerResult, ExecutionClient};
use tesser_core::{
    AccountBalance, Fill, Instrument, InstrumentKind, Order, OrderRequest, OrderStatus, OrderType,
    Position, Side, TimeInForce,
};
use uuid::Uuid;

pub mod ws;

pub use ws::{
    extract_order_update, BinanceMarketStream, BinanceSubscription, BinanceUserDataStream,
    UserDataStreamEventsResponse,
};

#[derive(Clone)]
pub struct BinanceCredentials {
    pub api_key: String,
    pub api_secret: String,
}

pub struct BinanceConfig {
    pub rest_url: String,
    pub ws_url: String,
    pub recv_window: u64,
}

impl Default for BinanceConfig {
    fn default() -> Self {
        Self {
            rest_url: "https://fapi.binance.com".to_string(),
            ws_url: "wss://fstream.binance.com/stream".to_string(),
            recv_window: 5_000,
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
}

impl BinanceClient {
    pub fn new(config: BinanceConfig, credentials: Option<BinanceCredentials>) -> Self {
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
        Self {
            rest: Arc::new(rest),
            info: BrokerInfo {
                name: "binance".into(),
                markets: vec!["usd_perp".into()],
                supports_testnet,
            },
            credentials,
            config,
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

    pub async fn start_user_stream(&self) -> BrokerResult<String> {
        let response = handle_response(self.rest.start_user_data_stream().await).await?;
        response
            .listen_key
            .ok_or_else(|| BrokerError::Other("missing listenKey".into()))
    }

    pub async fn keepalive_user_stream(&self) -> BrokerResult<String> {
        let response = handle_response(self.rest.keepalive_user_data_stream().await).await?;
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
        let side = map_side(request.side);
        let order_type = map_order_type(request.order_type);
        let mut builder = NewOrderParams::builder(request.symbol.clone(), side, order_type);
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
        let response = handle_response(self.rest.new_order(params).await).await?;
        Ok(build_order_from_response(response, client_request))
    }

    async fn cancel_order(&self, order_id: tesser_core::OrderId, symbol: &str) -> BrokerResult<()> {
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
        handle_response(self.rest.cancel_order(params).await).await?;
        Ok(())
    }

    async fn list_open_orders(&self, symbol: &str) -> BrokerResult<Vec<Order>> {
        let params = rest_api::CurrentAllOpenOrdersParams::builder()
            .symbol(Some(symbol.to_string()))
            .recv_window(Some(self.config.recv_window as i64))
            .build()
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let raw = handle_response(self.rest.current_all_open_orders(params).await).await?;
        let mut orders = Vec::new();
        for entry in raw {
            if let Some(order) = order_from_open_order(&entry) {
                orders.push(order);
            }
        }
        Ok(orders)
    }

    async fn account_balances(&self) -> BrokerResult<Vec<AccountBalance>> {
        let params = rest_api::FuturesAccountBalanceV3Params::builder()
            .recv_window(Some(self.config.recv_window as i64))
            .build()
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let balances = handle_response(self.rest.futures_account_balance_v3(params).await).await?;
        Ok(balances
            .into_iter()
            .filter_map(|entry| balance_from_entry(&entry))
            .collect())
    }

    async fn positions(&self) -> BrokerResult<Vec<Position>> {
        let params = rest_api::PositionInformationV2Params::builder()
            .recv_window(Some(self.config.recv_window as i64))
            .build()
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let positions = handle_response(self.rest.position_information_v2(params).await).await?;
        Ok(positions
            .into_iter()
            .filter_map(|entry| position_from_entry(&entry))
            .collect())
    }

    async fn list_instruments(&self, _category: &str) -> BrokerResult<Vec<Instrument>> {
        let info = handle_response(self.rest.exchange_information().await).await?;
        let mut instruments = Vec::new();
        for symbol in info.symbols.unwrap_or_default() {
            if let Some(instr) = instrument_from_symbol(&symbol) {
                instruments.push(instr);
            }
        }
        Ok(instruments)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

async fn handle_response<T>(result: anyhow::Result<RestApiResponse<T>>) -> BrokerResult<T>
where
    T: Send + 'static,
{
    let response = result.map_err(|err| BrokerError::Transport(err.to_string()))?;
    response
        .data()
        .await
        .map_err(|err| map_connector_error(err))
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
            .unwrap_or_else(|| Decimal::ZERO),
        avg_fill_price: parse_decimal_opt(response.avg_price.as_deref()),
        created_at: timestamp_from_ms(response.update_time),
        updated_at: timestamp_from_ms(response.update_time),
    }
}

fn order_from_open_order(entry: &rest_api::AllOrdersResponseInner) -> Option<Order> {
    let request = OrderRequest {
        symbol: entry.symbol.clone()?,
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
        filled_quantity: parse_decimal_opt(entry.executed_qty.as_deref())
            .unwrap_or_else(|| Decimal::ZERO),
        avg_fill_price: parse_decimal_opt(entry.avg_price.as_deref()),
        created_at: timestamp_from_ms(entry.time),
        updated_at: timestamp_from_ms(entry.update_time),
    })
}

fn balance_from_entry(
    entry: &rest_api::FuturesAccountBalanceV2ResponseInner,
) -> Option<AccountBalance> {
    let currency = entry.asset.clone()?;
    let total = parse_decimal_opt(entry.balance.as_deref())?;
    let available = parse_decimal_opt(entry.available_balance.as_deref()).unwrap_or(total);
    Some(AccountBalance {
        currency,
        total,
        available,
        updated_at: timestamp_from_ms(entry.update_time),
    })
}

fn position_from_entry(entry: &rest_api::PositionInformationV2ResponseInner) -> Option<Position> {
    let qty = parse_decimal_opt(entry.position_amt.as_deref())?;
    if qty.is_zero() {
        return None;
    }
    let side = if qty.is_sign_positive() {
        Some(Side::Buy)
    } else {
        Some(Side::Sell)
    };
    Some(Position {
        symbol: entry.symbol.clone()?,
        side,
        quantity: qty.abs(),
        entry_price: parse_decimal_opt(entry.entry_price.as_deref()),
        unrealized_pnl: parse_decimal_opt(entry.un_realized_profit.as_deref())
            .unwrap_or_else(|| Decimal::ZERO),
        updated_at: timestamp_from_ms(entry.update_time),
    })
}

fn instrument_from_symbol(
    symbol: &rest_api::ExchangeInformationResponseSymbolsInner,
) -> Option<Instrument> {
    let symbol_name = symbol.symbol.clone()?;
    let base = symbol.base_asset.clone()?;
    let quote = symbol.quote_asset.clone()?;
    let settlement = symbol.margin_asset.clone().unwrap_or_else(|| quote.clone());
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
        .and_then(|ms| DateTime::<Utc>::from_timestamp_millis(ms))
        .unwrap_or_else(Utc::now)
}

pub fn fill_from_update(order: &websocket_streams::OrderTradeUpdateO) -> Option<Fill> {
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
    Some(Fill {
        order_id: order.i.map(|id| id.to_string()).unwrap_or_default(),
        symbol: order.s.clone().unwrap_or_default(),
        side,
        fill_price: price,
        fill_quantity: last_qty,
        fee: parse_decimal_opt(order.n.as_deref()),
        timestamp: timestamp_from_ms(order.t_uppercase),
    })
}

pub fn order_from_update(order: &websocket_streams::OrderTradeUpdateO) -> Option<Order> {
    let request = OrderRequest {
        symbol: order.s.clone()?,
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
        quantity: parse_decimal_opt(order.q.as_deref()).unwrap_or_else(|| Decimal::ZERO),
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
        filled_quantity: parse_decimal_opt(order.z.as_deref()).unwrap_or_else(|| Decimal::ZERO),
        avg_fill_price: parse_decimal_opt(order.ap.as_deref()),
        created_at: timestamp_from_ms(order.t_uppercase),
        updated_at: timestamp_from_ms(order.t_uppercase),
    })
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
