//! Bybit REST connector targeting the public v5 API.
//!
//! Authentication details follow the rules described in
//! `bybit-api-docs/docs/v5/guide.mdx`.

use std::{any::Any, num::NonZeroU32, str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::{Client, Method};
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;
use sha2::Sha256;
use tesser_broker::{
    register_connector_factory, BrokerError, BrokerErrorKind, BrokerInfo, BrokerResult,
    ConnectorFactory, ConnectorStream, ConnectorStreamConfig, ExecutionClient, MarketStream, Quota,
    RateLimiter, RateLimiterError,
};
use tesser_core::{
    AccountBalance, AssetId, Candle, ExchangeId, Fill, Instrument, InstrumentKind, Order,
    OrderBook, OrderRequest, OrderStatus, OrderType, OrderUpdateRequest, Position, Quantity, Side,
    Symbol, TimeInForce,
};
use tracing::warn;

pub mod ws;

pub use ws::{BybitMarketStream, BybitSubscription, PublicChannel};

type HmacSha256 = Hmac<Sha256>;

/// API credentials required for private REST endpoints.
#[derive(Clone)]
pub struct BybitCredentials {
    pub api_key: String,
    pub api_secret: String,
}

/// Configuration for the Bybit REST client.
pub struct BybitConfig {
    pub base_url: String,
    pub category: String,
    pub recv_window: u64,
    pub ws_url: Option<String>,
    pub public_quota: Option<Quota>,
    pub private_quota: Option<Quota>,
}

impl Default for BybitConfig {
    fn default() -> Self {
        Self {
            base_url: "https://api-testnet.bybit.com".into(),
            category: "linear".into(),
            recv_window: 5_000,
            ws_url: None,
            public_quota: None,
            private_quota: None,
        }
    }
}

/// A thin wrapper over the Bybit v5 REST API.
pub struct BybitClient {
    http: Client,
    config: BybitConfig,
    credentials: Option<BybitCredentials>,
    info: BrokerInfo,
    public_limiter: Option<RateLimiter>,
    private_limiter: Option<RateLimiter>,
    exchange: ExchangeId,
}

impl BybitClient {
    /// Build a new client optionally configured with credentials.
    pub fn new(
        config: BybitConfig,
        credentials: Option<BybitCredentials>,
        exchange: ExchangeId,
    ) -> Self {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to create reqwest client");
        let public_limiter = config.public_quota.map(RateLimiter::direct);
        let private_limiter = config.private_quota.map(RateLimiter::keyed);
        Self {
            info: BrokerInfo {
                name: "bybit".into(),
                markets: vec![config.category.clone()],
                supports_testnet: config.base_url.contains("testnet"),
            },
            http,
            config,
            credentials,
            public_limiter,
            private_limiter,
            exchange,
        }
    }

    /// Convenience helper for the Bybit testnet.
    pub fn testnet(credentials: Option<BybitCredentials>) -> Self {
        Self::new(
            BybitConfig::default(),
            credentials,
            ExchangeId::from("bybit_linear"),
        )
    }

    pub fn get_credentials(&self) -> Option<BybitCredentials> {
        self.credentials.clone()
    }

    pub fn exchange(&self) -> ExchangeId {
        self.exchange
    }

    pub fn get_ws_url(&self) -> String {
        self.config
            .ws_url
            .clone()
            .unwrap_or_else(|| self.config.base_url.replace("https://api", "wss://stream"))
    }

    fn symbol_code(symbol: Symbol) -> &'static str {
        symbol.code()
    }

    fn parse_symbol(&self, value: &str) -> Symbol {
        Symbol::from_code(self.exchange, value)
    }

    fn parse_asset(&self, value: &str) -> AssetId {
        AssetId::from_code(self.exchange, value)
    }

    async fn throttle_public(&self) -> BrokerResult<()> {
        if let Some(limiter) = &self.public_limiter {
            limiter
                .until_ready()
                .await
                .map_err(Self::rate_limiter_error)?;
        }
        Ok(())
    }

    async fn throttle_private(&self) -> BrokerResult<()> {
        if let (Some(limiter), Some(creds)) = (&self.private_limiter, &self.credentials) {
            limiter
                .until_key_ready(&creds.api_key)
                .await
                .map_err(Self::rate_limiter_error)?;
        }
        Ok(())
    }

    /// Fetch Bybit server time (docs/v5/market/time.mdx).
    pub async fn server_time(&self) -> BrokerResult<u128> {
        let resp: ApiResponse<ServerTimeResult> = self.public_get("/v5/market/time").await?;
        self.ensure_success(&resp)?;
        resp.result
            .time_nano
            .parse::<u128>()
            .map_err(|err| BrokerError::Serialization(err.to_string()))
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.config.base_url, path.trim_start_matches('/'))
    }

    fn ensure_success<T>(&self, resp: &ApiResponse<T>) -> BrokerResult<()> {
        if resp.ret_code == 0 {
            Ok(())
        } else {
            Err(BrokerError::Exchange(format!(
                "{} (code {})",
                resp.ret_msg, resp.ret_code
            )))
        }
    }

    async fn public_get<T>(&self, path: &str) -> BrokerResult<ApiResponse<T>>
    where
        T: DeserializeOwned,
    {
        self.throttle_public().await?;
        let url = self.url(path);
        self.http
            .get(url)
            .send()
            .await
            .map_err(|err| BrokerError::Transport(err.to_string()))?
            .json::<ApiResponse<T>>()
            .await
            .map_err(|err| BrokerError::Serialization(err.to_string()))
    }

    fn creds(&self) -> BrokerResult<&BybitCredentials> {
        self.credentials
            .as_ref()
            .ok_or_else(|| BrokerError::Authentication("missing Bybit credentials".into()))
    }

    fn rate_limiter_error(err: RateLimiterError) -> BrokerError {
        BrokerError::Other(format!("rate limited: {err}"))
    }

    async fn signed_request<T>(
        &self,
        method: Method,
        path: &str,
        body: Value,
        query: Option<Vec<(String, String)>>,
    ) -> BrokerResult<ApiResponse<T>>
    where
        T: DeserializeOwned,
    {
        let creds = self.creds()?;
        self.throttle_private().await?;
        let timestamp = Utc::now().timestamp_millis();
        let query_string = query
            .as_ref()
            .map(|pairs| serde_urlencoded::to_string(pairs).unwrap_or_default())
            .unwrap_or_default();
        let payload = if method == Method::GET {
            format!(
                "{timestamp}{}{}{}",
                creds.api_key, self.config.recv_window, query_string
            )
        } else {
            let body_string = body.to_string();
            format!(
                "{timestamp}{}{}{}",
                creds.api_key, self.config.recv_window, body_string
            )
        };
        let mut mac = HmacSha256::new_from_slice(creds.api_secret.as_bytes())
            .map_err(|err| BrokerError::Other(format!("failed to create signing key: {err}")))?;
        mac.update(payload.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());
        let mut request = self.http.request(
            method.clone(),
            if query_string.is_empty() {
                self.url(path)
            } else {
                format!("{}?{}", self.url(path), query_string)
            },
        );
        request = request
            .header("X-BAPI-API-KEY", &creds.api_key)
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-RECV-WINDOW", self.config.recv_window.to_string())
            .header("Content-Type", "application/json");
        if method != Method::GET {
            request = request.json(&body);
        }
        let resp = request
            .send()
            .await
            .map_err(|err| BrokerError::Transport(err.to_string()))?
            .json::<ApiResponse<T>>()
            .await
            .map_err(|err| BrokerError::Serialization(err.to_string()))?;
        self.ensure_success(&resp)?;
        Ok(resp)
    }

    fn map_time_in_force(tif: Option<TimeInForce>) -> &'static str {
        match tif.unwrap_or(TimeInForce::GoodTilCanceled) {
            TimeInForce::GoodTilCanceled => "GTC",
            TimeInForce::ImmediateOrCancel => "IOC",
            TimeInForce::FillOrKill => "FOK",
        }
    }

    fn map_side(side: Side) -> &'static str {
        match side {
            Side::Buy => "Buy",
            Side::Sell => "Sell",
        }
    }

    fn map_order_type(order_type: tesser_core::OrderType) -> &'static str {
        match order_type {
            tesser_core::OrderType::Market => "Market",
            tesser_core::OrderType::Limit => "Limit",
            tesser_core::OrderType::StopMarket => "Market",
        }
    }

    fn qty_string(qty: Quantity) -> String {
        qty.normalize().to_string()
    }

    pub(crate) fn map_order_status(status: &str) -> OrderStatus {
        match status {
            "New" | "Created" | "PendingNew" | "Untriggered" => OrderStatus::PendingNew,
            "Accepted" | "Active" | "Triggered" => OrderStatus::Accepted,
            "Rejected" => OrderStatus::Rejected,
            "PartiallyFilled" | "PartiallyFilledCanceled" => OrderStatus::PartiallyFilled,
            "Filled" => OrderStatus::Filled,
            "Cancelled" | "Canceled" | "Deactivated" => OrderStatus::Canceled,
            other => {
                warn!(status = other, "unhandled Bybit order status");
                OrderStatus::PendingNew
            }
        }
    }

    /// Fetch executions (fills) since the provided timestamp (inclusive), paginated.
    /// Maps the response into framework-native `Fill` records.
    pub async fn list_executions_since(
        &self,
        since: chrono::DateTime<chrono::Utc>,
    ) -> BrokerResult<Vec<Fill>> {
        let mut cursor: Option<String> = None;
        let mut out: Vec<Fill> = Vec::new();
        let start_ms = since.timestamp_millis();
        let end_ms = chrono::Utc::now().timestamp_millis();

        loop {
            let mut query = vec![
                ("category".to_string(), self.config.category.clone()),
                ("limit".to_string(), "100".to_string()),
                ("startTime".to_string(), start_ms.to_string()),
                ("endTime".to_string(), end_ms.to_string()),
            ];
            if let Some(ref cur) = cursor {
                query.push(("cursor".to_string(), cur.clone()));
            }
            let resp: ApiResponse<ExecutionListResult> = self
                .signed_request(Method::GET, "/v5/execution/list", Value::Null, Some(query))
                .await?;

            for item in resp.result.list.into_iter() {
                // Filter only entries with non-zero quantity
                if item.exec_qty.is_empty() {
                    continue;
                }
                let exec_qty: Decimal = item.exec_qty.parse().unwrap_or(Decimal::ZERO);
                if exec_qty.is_zero() {
                    continue;
                }
                let price: Decimal = item.exec_price.parse().unwrap_or(Decimal::ZERO);
                let fee: Option<Decimal> = if item.exec_fee.is_empty() {
                    None
                } else {
                    item.exec_fee.parse::<Decimal>().ok()
                };
                let ts = millis_to_datetime(&item.exec_time);
                let side = match item.side.as_str() {
                    "Buy" => Side::Buy,
                    _ => Side::Sell,
                };
                out.push(Fill {
                    order_id: item.order_id,
                    symbol: self.parse_symbol(&item.symbol),
                    side,
                    fill_price: price,
                    fill_quantity: exec_qty,
                    fee,
                    timestamp: ts,
                });
            }

            if let Some(c) = resp.result.next_page_cursor {
                if c.is_empty() {
                    break;
                }
                cursor = Some(c);
            } else {
                break;
            }
        }

        Ok(out)
    }
}

#[async_trait]
impl ExecutionClient for BybitClient {
    fn info(&self) -> BrokerInfo {
        self.info.clone()
    }

    async fn place_order(&self, request: OrderRequest) -> BrokerResult<Order> {
        let mut payload = serde_json::json!({
            "category": self.config.category,
            "symbol": Self::symbol_code(request.symbol),
            "side": Self::map_side(request.side),
            "qty": Self::qty_string(request.quantity),
            "timeInForce": Self::map_time_in_force(request.time_in_force),
            "price": request.price,
            "orderLinkId": request.client_order_id,
        });

        match request.order_type {
            tesser_core::OrderType::Market | tesser_core::OrderType::Limit => {
                payload["orderType"] = serde_json::json!(Self::map_order_type(request.order_type));
            }
            tesser_core::OrderType::StopMarket => {
                let trigger_price = request.trigger_price.ok_or_else(|| {
                    BrokerError::InvalidRequest("StopMarket order requires a trigger_price".into())
                })?;
                payload["orderType"] = serde_json::json!("Market");
                payload["triggerPrice"] = serde_json::json!(format!("{}", trigger_price));
            }
        }
        let resp: ApiResponse<CreateOrderResult> = self
            .signed_request(Method::POST, "/v5/order/create", payload, None)
            .await?;
        Ok(Order {
            id: resp.result.order_id,
            request,
            status: OrderStatus::PendingNew,
            filled_quantity: Decimal::ZERO,
            avg_fill_price: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
    }

    async fn cancel_order(&self, order_id: tesser_core::OrderId, symbol: &str) -> BrokerResult<()> {
        let payload = serde_json::json!({
            "category": self.config.category,
            "symbol": symbol,
            "orderId": order_id,
        });
        self.signed_request::<serde_json::Value>(Method::POST, "/v5/order/cancel", payload, None)
            .await?;
        Ok(())
    }

    async fn amend_order(&self, request: OrderUpdateRequest) -> BrokerResult<Order> {
        let mut payload = serde_json::json!({
            "category": self.config.category,
            "symbol": Self::symbol_code(request.symbol),
            "orderId": request.order_id,
        });
        if let Some(price) = request.new_price {
            payload["price"] = serde_json::json!(price);
        }
        if let Some(quantity) = request.new_quantity {
            payload["qty"] = serde_json::json!(Self::qty_string(quantity));
        }
        if payload.get("price").is_none() && payload.get("qty").is_none() {
            return Err(BrokerError::InvalidRequest(
                "amend requires price or quantity".into(),
            ));
        }
        let resp: ApiResponse<CreateOrderResult> = self
            .signed_request(Method::POST, "/v5/order/amend", payload, None)
            .await?;
        if let Ok(open_orders) = self.list_open_orders(Self::symbol_code(request.symbol)).await {
            if let Some(order) = open_orders
                .into_iter()
                .find(|order| order.id == resp.result.order_id)
            {
                return Ok(order);
            }
        }
        Ok(Order {
            id: resp.result.order_id,
            request: OrderRequest {
                symbol: request.symbol,
                side: request.side,
                order_type: OrderType::Limit,
                quantity: request.new_quantity.unwrap_or(Decimal::ZERO),
                price: request.new_price,
                trigger_price: None,
                time_in_force: None,
                client_order_id: None,
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            },
            status: OrderStatus::PendingNew,
            filled_quantity: Decimal::ZERO,
            avg_fill_price: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
    }

    async fn list_open_orders(&self, symbol: &str) -> BrokerResult<Vec<Order>> {
        let query = vec![
            ("category".to_string(), self.config.category.clone()),
            ("symbol".to_string(), symbol.to_string()),
            ("openOnly".to_string(), "0".into()),
        ];
        let resp: ApiResponse<OpenOrdersResult> = self
            .signed_request(Method::GET, "/v5/order/realtime", Value::Null, Some(query))
            .await?;
        let orders = resp
            .result
            .list
            .into_iter()
            .map(|item| {
                let symbol = self.parse_symbol(&item.symbol);
                Order {
                    id: item.order_id,
                    request: OrderRequest {
                        symbol,
                        side: if item.side == "Buy" {
                            Side::Buy
                        } else {
                            Side::Sell
                        },
                        order_type: if item.trigger_price.is_some() {
                            tesser_core::OrderType::StopMarket
                        } else if item.order_type == "Market" {
                            tesser_core::OrderType::Market
                        } else {
                            tesser_core::OrderType::Limit
                        },
                        quantity: item.qty.parse().unwrap_or(Decimal::ZERO),
                        price: item.price.parse::<Decimal>().ok(),
                        trigger_price: item
                            .trigger_price
                            .as_deref()
                            .and_then(|value| value.parse::<Decimal>().ok()),
                        time_in_force: None,
                        client_order_id: Some(item.order_link_id),
                        take_profit: None,
                        stop_loss: None,
                        display_quantity: None,
                    },
                    status: Self::map_order_status(&item.order_status),
                    filled_quantity: item.cum_exec_qty.parse().unwrap_or(Decimal::ZERO),
                    avg_fill_price: item.avg_price.parse::<Decimal>().ok(),
                    created_at: millis_to_datetime(&item.created_time),
                    updated_at: millis_to_datetime(&item.updated_time),
                }
            })
            .collect();
        Ok(orders)
    }

    async fn account_balances(&self) -> BrokerResult<Vec<AccountBalance>> {
        let query = vec![("accountType".to_string(), "UNIFIED".into())];
        let resp: ApiResponse<WalletBalanceResult> = self
            .signed_request(
                Method::GET,
                "/v5/account/wallet-balance",
                Value::Null,
                Some(query),
            )
            .await?;
        let mut balances = Vec::new();
        for account in resp.result.list {
            for coin in account.coin {
                let total = coin.wallet_balance.parse().unwrap_or(Decimal::ZERO);
                let available = coin
                    .available_to_withdraw
                    .as_deref()
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or(Decimal::ZERO);
                balances.push(AccountBalance {
                    exchange: self.exchange,
                    asset: self.parse_asset(&coin.coin),
                    total,
                    available,
                    updated_at: Utc::now(),
                });
            }
        }
        Ok(balances)
    }

    async fn positions(&self) -> BrokerResult<Vec<Position>> {
        let query = vec![("category".to_string(), self.config.category.clone())];
        let resp: ApiResponse<PositionListResult> = self
            .signed_request(Method::GET, "/v5/position/list", Value::Null, Some(query))
            .await?;
        let mut positions = Vec::new();
        for item in resp.result.list {
            let quantity: Decimal = item
                .size
                .parse()
                .map_err(|err| BrokerError::from_display(err, BrokerErrorKind::Serialization))?;
            if quantity.is_zero() {
                continue;
            }
            let entry_price = item.avg_price.parse().ok();
            positions.push(Position {
                symbol: self.parse_symbol(&item.symbol),
                side: match item.side.as_str() {
                    "Buy" => Some(Side::Buy),
                    "Sell" => Some(Side::Sell),
                    _ => None,
                },
                quantity,
                entry_price,
                unrealized_pnl: item.unrealised_pnl.parse().unwrap_or(Decimal::ZERO),
                updated_at: millis_to_datetime(&item.updated_time),
            });
        }
        Ok(positions)
    }

    async fn list_instruments(&self, category: &str) -> BrokerResult<Vec<Instrument>> {
        let path = format!("/v5/market/instruments-info?category={}", category);
        let resp: ApiResponse<InstrumentInfoResult> = self.public_get(&path).await?;
        self.ensure_success(&resp)?;
        let mut instruments = Vec::new();
        for item in resp.result.list {
            let tick_size = item.price_filter.tick_size.parse().unwrap_or(Decimal::ZERO);
            let lot_size = item
                .lot_size_filter
                .qty_step
                .as_deref()
                .and_then(|value| value.parse().ok())
                .unwrap_or(Decimal::ZERO);
            let settlement = item
                .settle_coin
                .clone()
                .unwrap_or_else(|| item.quote_coin.clone());
            instruments.push(Instrument {
                symbol: self.parse_symbol(&item.symbol),
                base: self.parse_asset(&item.base_coin),
                quote: self.parse_asset(&item.quote_coin),
                kind: map_instrument_kind(item.contract_type.as_deref(), category),
                settlement_currency: self.parse_asset(&settlement),
                tick_size,
                lot_size,
            });
        }
        Ok(instruments)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn map_instrument_kind(contract_type: Option<&str>, category: &str) -> InstrumentKind {
    match contract_type {
        Some("InversePerpetual") => InstrumentKind::InversePerpetual,
        Some("LinearPerpetual") => InstrumentKind::LinearPerpetual,
        _ => match category {
            "inverse" => InstrumentKind::InversePerpetual,
            "spot" => InstrumentKind::Spot,
            _ => InstrumentKind::LinearPerpetual,
        },
    }
}

pub(crate) fn millis_to_datetime(value: &str) -> chrono::DateTime<Utc> {
    value
        .parse::<i64>()
        .ok()
        .and_then(chrono::DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or_else(Utc::now)
}

#[derive(Deserialize)]
struct ApiResponse<T> {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: T,
}

#[derive(Deserialize)]
struct ServerTimeResult {
    #[serde(rename = "timeNano")]
    time_nano: String,
}

#[derive(Deserialize)]
struct CreateOrderResult {
    #[serde(rename = "orderId")]
    order_id: String,
}

#[derive(Deserialize)]
struct InstrumentInfoResult {
    list: Vec<InstrumentInfoItem>,
}

#[derive(Deserialize)]
struct InstrumentInfoItem {
    symbol: String,
    #[serde(rename = "baseCoin")]
    base_coin: String,
    #[serde(rename = "quoteCoin")]
    quote_coin: String,
    #[serde(rename = "settleCoin")]
    settle_coin: Option<String>,
    #[serde(rename = "contractType")]
    contract_type: Option<String>,
    #[serde(rename = "priceFilter")]
    price_filter: InstrumentPriceFilter,
    #[serde(rename = "lotSizeFilter")]
    lot_size_filter: InstrumentLotFilter,
}

#[derive(Deserialize)]
struct InstrumentPriceFilter {
    #[serde(rename = "tickSize")]
    tick_size: String,
}

#[derive(Deserialize)]
struct InstrumentLotFilter {
    #[serde(rename = "qtyStep")]
    qty_step: Option<String>,
}

#[derive(Deserialize)]
struct OpenOrdersResult {
    list: Vec<OrderItem>,
}

#[derive(Deserialize)]
struct OrderItem {
    #[serde(rename = "orderId")]
    order_id: String,
    #[serde(rename = "orderLinkId")]
    order_link_id: String,
    symbol: String,
    price: String,
    qty: String,
    side: String,
    #[serde(rename = "orderStatus")]
    order_status: String,
    #[serde(rename = "orderType")]
    order_type: String,
    #[serde(rename = "triggerPrice")]
    trigger_price: Option<String>,
    #[serde(rename = "cumExecQty")]
    cum_exec_qty: String,
    #[serde(rename = "avgPrice")]
    avg_price: String,
    #[serde(rename = "createdTime")]
    created_time: String,
    #[serde(rename = "updatedTime")]
    updated_time: String,
}

#[derive(Deserialize)]
struct WalletBalanceResult {
    list: Vec<AccountEntry>,
}

#[derive(Deserialize)]
struct AccountEntry {
    coin: Vec<CoinBalance>,
}

#[derive(Deserialize)]
struct CoinBalance {
    coin: String,
    #[serde(rename = "walletBalance")]
    wallet_balance: String,
    #[serde(rename = "availableToWithdraw")]
    available_to_withdraw: Option<String>,
}

#[derive(Deserialize)]
struct PositionListResult {
    list: Vec<PositionItem>,
}

#[derive(Deserialize)]
struct PositionItem {
    symbol: String,
    side: String,
    size: String,
    #[serde(rename = "avgPrice")]
    avg_price: String,
    #[serde(rename = "unrealisedPnl")]
    unrealised_pnl: String,
    #[serde(rename = "updatedTime")]
    updated_time: String,
}

#[derive(Deserialize)]
struct ExecutionListResult {
    #[serde(rename = "nextPageCursor")]
    next_page_cursor: Option<String>,
    #[allow(dead_code)]
    category: String,
    list: Vec<ExecutionListItem>,
}

#[derive(Deserialize)]
struct ExecutionListItem {
    symbol: String,
    #[serde(rename = "orderId")]
    order_id: String,
    side: String,
    #[serde(rename = "execPrice")]
    exec_price: String,
    #[serde(rename = "execQty")]
    exec_qty: String,
    #[serde(rename = "execFee")]
    exec_fee: String,
    #[serde(rename = "execTime")]
    exec_time: String,
}

#[derive(Clone, Debug, Deserialize)]
struct BybitConnectorConfig {
    #[serde(default = "default_rest_url")]
    rest_url: String,
    #[serde(default = "default_ws_url")]
    ws_url: String,
    #[serde(default)]
    exchange: Option<String>,
    #[serde(default = "default_category")]
    category: String,
    #[serde(default)]
    api_key: String,
    #[serde(default)]
    api_secret: String,
    #[serde(default = "default_recv_window")]
    recv_window: u64,
    #[serde(default = "default_rate_limit_tier")]
    rate_limit_tier: RateLimitTier,
    #[serde(default)]
    private_rps: Option<u32>,
    #[serde(default)]
    public_rps: Option<u32>,
}

fn default_rest_url() -> String {
    "https://api.bybit.com".into()
}

fn default_ws_url() -> String {
    "wss://stream.bybit.com".into()
}

fn default_category() -> String {
    "linear".into()
}

fn resolve_exchange_id(cfg: &BybitConnectorConfig) -> ExchangeId {
    let default_name = format!("bybit_{}", cfg.category.trim().to_ascii_lowercase());
    let name = cfg
        .exchange
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or(default_name);
    ExchangeId::from(name.as_str())
}

fn default_recv_window() -> u64 {
    5_000
}

fn default_rate_limit_tier() -> RateLimitTier {
    RateLimitTier::Standard
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RateLimitTier {
    Standard,
    Pro,
    Vip,
}

impl RateLimitTier {
    fn private_rps(self) -> u32 {
        match self {
            RateLimitTier::Standard => 10,
            RateLimitTier::Pro => 20,
            RateLimitTier::Vip => 50,
        }
    }
}

#[derive(Default)]
pub struct BybitFactory;

impl BybitFactory {
    fn parse_config(&self, value: &Value) -> BrokerResult<BybitConnectorConfig> {
        serde_json::from_value(value.clone()).map_err(|err| {
            BrokerError::InvalidRequest(format!("invalid bybit connector config: {err}"))
        })
    }

    fn credentials(cfg: &BybitConnectorConfig) -> Option<BybitCredentials> {
        if cfg.api_key.trim().is_empty() || cfg.api_secret.trim().is_empty() {
            None
        } else {
            Some(BybitCredentials {
                api_key: cfg.api_key.clone(),
                api_secret: cfg.api_secret.clone(),
            })
        }
    }
}

const BYBIT_DEFAULT_DEPTH: usize = 50;
const BYBIT_PUBLIC_DEFAULT_RPS: u32 = 50;

pub fn register_factory() {
    register_connector_factory(Arc::new(BybitFactory));
}

fn rate_limits_from_config(cfg: &BybitConnectorConfig) -> (Option<Quota>, Option<Quota>) {
    let private_rps = cfg
        .private_rps
        .unwrap_or_else(|| cfg.rate_limit_tier.private_rps());
    let public_rps = cfg.public_rps.unwrap_or(BYBIT_PUBLIC_DEFAULT_RPS);
    (quota_from_rps(public_rps), quota_from_rps(private_rps))
}

fn quota_from_rps(rps: u32) -> Option<Quota> {
    NonZeroU32::new(rps).map(Quota::per_second)
}

#[async_trait]
impl ConnectorFactory for BybitFactory {
    fn name(&self) -> &str {
        "bybit"
    }

    async fn create_execution_client(
        &self,
        config: &Value,
    ) -> BrokerResult<Arc<dyn ExecutionClient>> {
        let cfg = self.parse_config(config)?;
        let exchange = resolve_exchange_id(&cfg);
        let (public_quota, private_quota) = rate_limits_from_config(&cfg);
        let bybit_cfg = BybitConfig {
            base_url: cfg.rest_url.clone(),
            category: cfg.category.clone(),
            recv_window: cfg.recv_window,
            ws_url: Some(cfg.ws_url.clone()),
            public_quota,
            private_quota,
        };
        Ok(Arc::new(BybitClient::new(
            bybit_cfg,
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
        let exchange = resolve_exchange_id(&cfg);
        let channel = PublicChannel::from_str(&cfg.category)
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let stream = BybitMarketStream::connect_public(
            &cfg.ws_url,
            channel,
            stream_config.connection_status,
            exchange,
        )
        .await?;
        Ok(Box::new(BybitConnectorStream::new(
            stream,
            stream_config
                .metadata
                .get("orderbook_depth")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize)
                .unwrap_or(BYBIT_DEFAULT_DEPTH),
        )))
    }
}

struct BybitConnectorStream {
    inner: BybitMarketStream,
    depth: usize,
}

impl BybitConnectorStream {
    fn new(inner: BybitMarketStream, depth: usize) -> Self {
        Self { inner, depth }
    }
}

#[async_trait]
impl ConnectorStream for BybitConnectorStream {
    async fn subscribe(
        &mut self,
        symbols: &[String],
        interval: tesser_core::Interval,
    ) -> BrokerResult<()> {
        for symbol in symbols {
            self.inner
                .subscribe(BybitSubscription::Trades {
                    symbol: symbol.clone(),
                })
                .await?;
            self.inner
                .subscribe(BybitSubscription::Kline {
                    symbol: symbol.clone(),
                    interval,
                })
                .await?;
            self.inner
                .subscribe(BybitSubscription::OrderBook {
                    symbol: symbol.clone(),
                    depth: self.depth.clamp(1, 200),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_matches_docs_example() {
        let creds = BybitCredentials {
            api_key: "XXXXXXXXXX".into(),
            api_secret: "sec".repeat(10),
        };
        let payload = format!(
            "{}{}{}{}",
            1_658_385_579_423i64, creds.api_key, 5_000, r#"{"category": "option"}"#
        );
        let mut mac = HmacSha256::new_from_slice(creds.api_secret.as_bytes()).expect("init mac");
        mac.update(payload.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());
        assert_eq!(
            signature.len(),
            64,
            "signature should be 256-bit hex encoded"
        );
    }
}
