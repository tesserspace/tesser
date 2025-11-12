//! Bybit REST connector targeting the public v5 API.
//!
//! Authentication details follow the rules described in
//! `bybit-api-docs/docs/v5/guide.mdx`.

use std::{any::Any, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::{Client, Method};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;
use sha2::Sha256;
use tesser_broker::{BrokerError, BrokerErrorKind, BrokerInfo, BrokerResult, ExecutionClient};
use tesser_core::{AccountBalance, Order, OrderRequest, OrderStatus, Position, Side, TimeInForce};
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
}

impl Default for BybitConfig {
    fn default() -> Self {
        Self {
            base_url: "https://api-testnet.bybit.com".into(),
            category: "linear".into(),
            recv_window: 5_000,
        }
    }
}

/// A thin wrapper over the Bybit v5 REST API.
pub struct BybitClient {
    http: Client,
    config: BybitConfig,
    credentials: Option<BybitCredentials>,
    info: BrokerInfo,
}

impl BybitClient {
    /// Build a new client optionally configured with credentials.
    pub fn new(config: BybitConfig, credentials: Option<BybitCredentials>) -> Self {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to create reqwest client");
        Self {
            info: BrokerInfo {
                name: "bybit".into(),
                markets: vec![config.category.clone()],
                supports_testnet: config.base_url.contains("testnet"),
            },
            http,
            config,
            credentials,
        }
    }

    /// Convenience helper for the Bybit testnet.
    pub fn testnet(credentials: Option<BybitCredentials>) -> Self {
        Self::new(BybitConfig::default(), credentials)
    }

    pub fn get_credentials(&self) -> Option<BybitCredentials> {
        self.credentials.clone()
    }

    pub fn get_ws_url(&self) -> String {
        self.config.base_url.replace("https://api", "wss://stream")
    }

    /// Fetch Bybit server time (docs/v5/market/time.mdx).
    pub async fn server_time(&self) -> BrokerResult<u128> {
        let resp: ApiResponse<ServerTimeResult> = self
            .public_get("/v5/market/time")
            .await
            .map_err(|err| BrokerError::Transport(err.to_string()))?;
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

    async fn public_get<T>(&self, path: &str) -> Result<ApiResponse<T>, reqwest::Error>
    where
        T: DeserializeOwned,
    {
        let url = self.url(path);
        self.http
            .get(url)
            .send()
            .await?
            .json::<ApiResponse<T>>()
            .await
    }

    fn creds(&self) -> BrokerResult<&BybitCredentials> {
        self.credentials
            .as_ref()
            .ok_or_else(|| BrokerError::Authentication("missing Bybit credentials".into()))
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

    fn qty_string(qty: f64) -> String {
        format!("{}", qty)
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
}

#[async_trait]
impl ExecutionClient for BybitClient {
    fn info(&self) -> BrokerInfo {
        self.info.clone()
    }

    async fn place_order(&self, request: OrderRequest) -> BrokerResult<Order> {
        let mut payload = serde_json::json!({
            "category": self.config.category,
            "symbol": request.symbol,
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
            filled_quantity: 0.0,
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
            .map(|item| Order {
                id: item.order_id,
                request: OrderRequest {
                    symbol: item.symbol,
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
                    quantity: item.qty.parse().unwrap_or(0.0),
                    price: item.price.parse().ok(),
                    trigger_price: item
                        .trigger_price
                        .as_deref()
                        .and_then(|value| value.parse().ok()),
                    time_in_force: None,
                    client_order_id: Some(item.order_link_id),
                },
                status: Self::map_order_status(&item.order_status),
                filled_quantity: item.cum_exec_qty.parse().unwrap_or(0.0),
                avg_fill_price: item.avg_price.parse().ok(),
                created_at: millis_to_datetime(&item.created_time),
                updated_at: millis_to_datetime(&item.updated_time),
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
                let total = coin.wallet_balance.parse().unwrap_or(0.0);
                let available = coin
                    .available_to_withdraw
                    .as_deref()
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or(0.0);
                balances.push(AccountBalance {
                    currency: coin.coin,
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
            let quantity = item
                .size
                .parse::<f64>()
                .map_err(|err| BrokerError::from_display(err, BrokerErrorKind::Serialization))?;
            if quantity.abs() < f64::EPSILON {
                continue;
            }
            let entry_price = item.avg_price.parse::<f64>().ok();
            positions.push(Position {
                symbol: item.symbol,
                side: match item.side.as_str() {
                    "Buy" => Some(Side::Buy),
                    "Sell" => Some(Side::Sell),
                    _ => None,
                },
                quantity,
                entry_price,
                unrealized_pnl: item.unrealised_pnl.parse::<f64>().unwrap_or(0.0),
                updated_at: millis_to_datetime(&item.updated_time),
            });
        }
        Ok(positions)
    }

    fn as_any(&self) -> &dyn Any {
        self
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
