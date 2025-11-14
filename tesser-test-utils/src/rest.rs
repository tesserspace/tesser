use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, Mac};
use hyper::body::{to_bytes, Bytes};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};
use serde_urlencoded;
use sha2::Sha256;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::warn;
use uuid::Uuid;

use tesser_core::{Fill, Order, OrderRequest, OrderStatus, OrderType, Side, TimeInForce};

use crate::scenario::{OrderFillStep, ScenarioAction, ScenarioTrigger};
use crate::state::{MockExchangeState, PrivateMessage};

type HmacSha256 = Hmac<Sha256>;

const ACCOUNT_TYPE: &str = "UNIFIED";
const DEFAULT_CATEGORY: &str = "linear";

pub struct MockRestApi {
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
}

impl MockRestApi {
    pub async fn spawn(state: MockExchangeState) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let std_listener = listener.into_std()?;
        std_listener.set_nonblocking(true)?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let make_svc = make_service_fn(move |_| {
            let state = state.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let state = state.clone();
                    async move { Ok::<_, Infallible>(route(req, state).await) }
                }))
            }
        });
        let server = Server::from_tcp(std_listener)?.serve(make_svc);
        let handle = tokio::spawn(async move {
            if let Err(err) = server
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
            {
                tracing::error!(error = %err, "mock REST server exited with error");
            }
        });
        Ok(Self {
            addr,
            shutdown_tx: Some(shutdown_tx),
            handle,
        })
    }

    #[must_use]
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub async fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.handle.abort();
    }
}

impl Drop for MockRestApi {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.handle.abort();
    }
}

async fn route(req: Request<Body>, state: MockExchangeState) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let method = parts.method.clone();
    let path = parts.uri.path().to_string();
    let body_bytes = match to_bytes(body).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                10001,
                format!("failed to read request body: {err}"),
            )
        }
    };

    match (method, path.as_str()) {
        (Method::POST, "/v5/order/create") => handle_order_create(parts, body_bytes, state).await,
        (Method::POST, "/v5/order/cancel") => handle_order_cancel(parts, body_bytes, state).await,
        (Method::GET, "/v5/position/list") => handle_positions(parts, state).await,
        (Method::GET, "/v5/account/wallet-balance") => handle_wallet_balance(parts, state).await,
        (Method::GET, "/v5/execution/list") => handle_execution_list(parts, state).await,
        (Method::GET, "/v5/order/realtime") => handle_open_orders(parts, state).await,
        _ => not_found(),
    }
}

async fn handle_order_create(
    parts: http::request::Parts,
    body: Bytes,
    state: MockExchangeState,
) -> Response<Body> {
    let api_key = match authenticate(&parts, &body, Method::POST, &state).await {
        Ok(api_key) => api_key,
        Err(resp) => return resp,
    };
    let payload: CreateOrderPayload = match serde_json::from_slice(&body) {
        Ok(payload) => payload,
        Err(err) => return bad_request(format!("invalid JSON payload: {err}")),
    };

    let mut fill_plan: Option<Vec<OrderFillStep>> = None;
    if let Some(action) = state
        .scenarios()
        .take_for(ScenarioTrigger::OrderCreate)
        .await
    {
        match action {
            ScenarioAction::Delay(duration) => sleep(duration).await,
            ScenarioAction::Fail { status, reason } => {
                return error_response(status, 10002, reason);
            }
            ScenarioAction::InjectPrivateEvent(payload) => {
                let _ = state.emit_private_message(payload).await;
            }
            ScenarioAction::FillPlan { steps } => {
                fill_plan = Some(steps);
            }
        }
    }

    let order = match create_order(&state, &api_key, payload).await {
        Ok(order) => order,
        Err(err) => return bad_request(err.to_string()),
    };

    if let Err(err) = broadcast_order_update(&state, &order).await {
        warn!(error = %err, "failed to deliver order acceptance");
    }

    if let Some(steps) = fill_plan {
        spawn_fill_plan(state.clone(), api_key.clone(), order.clone(), steps);
    }

    ok_response(json!({
        "orderId": order.id,
        "orderLinkId": order.request.client_order_id.clone().unwrap_or_default(),
    }))
}

async fn handle_order_cancel(
    parts: http::request::Parts,
    body: Bytes,
    state: MockExchangeState,
) -> Response<Body> {
    let api_key = match authenticate(&parts, &body, Method::POST, &state).await {
        Ok(api_key) => api_key,
        Err(resp) => return resp,
    };
    let payload: CancelOrderPayload = match serde_json::from_slice(&body) {
        Ok(payload) => payload,
        Err(err) => return bad_request(format!("invalid JSON payload: {err}")),
    };

    if let Some(action) = state
        .scenarios()
        .take_for(ScenarioTrigger::OrderCancel)
        .await
    {
        match action {
            ScenarioAction::Delay(duration) => sleep(duration).await,
            ScenarioAction::Fail { status, reason } => {
                return error_response(status, 10003, reason);
            }
            ScenarioAction::InjectPrivateEvent(payload) => {
                let _ = state.emit_private_message(payload).await;
            }
            ScenarioAction::FillPlan { .. } => {}
        }
    }

    let order_id = match state
        .find_order_id(
            &api_key,
            payload.order_id.as_deref(),
            payload.order_link_id.as_deref(),
        )
        .await
    {
        Ok(id) => id,
        Err(err) => return bad_request(err.to_string()),
    };

    let order = match state.cancel_order(&api_key, &order_id).await {
        Ok(order) => order,
        Err(err) => return bad_request(err.to_string()),
    };

    if let Err(err) = broadcast_order_update(&state, &order).await {
        warn!(error = %err, "failed to deliver order cancel event");
    }

    ok_response(json!({ "orderId": order_id }))
}

async fn handle_positions(parts: http::request::Parts, state: MockExchangeState) -> Response<Body> {
    let api_key = match authenticate(&parts, &Bytes::new(), Method::GET, &state).await {
        Ok(api_key) => api_key,
        Err(resp) => return resp,
    };

    match state.account_positions(&api_key).await {
        Ok(positions) => {
            let list: Vec<Value> = positions
                .into_iter()
                .map(|position| {
                    json!({
                        "symbol": position.symbol,
                        "side": side_label(position.side),
                        "size": decimal_to_string(position.quantity),
                        "avgPrice": position.entry_price.map(decimal_to_string).unwrap_or_else(|| "0".into()),
                        "unrealisedPnl": decimal_to_string(position.unrealized_pnl),
                        "updatedTime": position.updated_at.timestamp_millis().to_string(),
                    })
                })
                .collect();
            ok_response(json!({ "list": list }))
        }
        Err(err) => bad_request(err.to_string()),
    }
}

async fn handle_wallet_balance(
    parts: http::request::Parts,
    state: MockExchangeState,
) -> Response<Body> {
    let api_key = match authenticate(&parts, &Bytes::new(), Method::GET, &state).await {
        Ok(api_key) => api_key,
        Err(resp) => return resp,
    };

    match state.account_balances(&api_key).await {
        Ok(balances) => {
            let coins: Vec<Value> = balances
                .into_iter()
                .map(|balance| {
                    json!({
                        "coin": balance.currency,
                        "walletBalance": decimal_to_string(balance.total),
                        "availableToWithdraw": decimal_to_string(balance.available),
                    })
                })
                .collect();
            ok_response(json!({
                "list": [{
                    "accountType": ACCOUNT_TYPE,
                    "coin": coins,
                }]
            }))
        }
        Err(err) => bad_request(err.to_string()),
    }
}

async fn handle_execution_list(
    parts: http::request::Parts,
    state: MockExchangeState,
) -> Response<Body> {
    let api_key = match authenticate(&parts, &Bytes::new(), Method::GET, &state).await {
        Ok(api_key) => api_key,
        Err(resp) => return resp,
    };
    let params = parse_query(parts.uri.query());
    let start = params
        .get("startTime")
        .and_then(|value| parse_timestamp(value))
        .unwrap_or_else(|| Utc::now() - Duration::hours(1));
    let end = params
        .get("endTime")
        .and_then(|value| parse_timestamp(value));

    match state.executions_between(&api_key, start, end).await {
        Ok(fills) => {
            let list: Vec<Value> = fills
                .into_iter()
                .map(|fill| {
                    json!({
                        "symbol": fill.symbol,
                        "orderId": fill.order_id,
                        "side": map_side(fill.side),
                        "execPrice": decimal_to_string(fill.fill_price),
                        "execQty": decimal_to_string(fill.fill_quantity),
                        "execFee": fill.fee.map(decimal_to_string).unwrap_or_else(|| "0".into()),
                        "execTime": fill.timestamp.timestamp_millis().to_string(),
                    })
                })
                .collect();
            ok_response(json!({
                "category": DEFAULT_CATEGORY,
                "list": list,
                "nextPageCursor": Value::Null,
            }))
        }
        Err(err) => bad_request(err.to_string()),
    }
}

async fn handle_open_orders(
    parts: http::request::Parts,
    state: MockExchangeState,
) -> Response<Body> {
    let api_key = match authenticate(&parts, &Bytes::new(), Method::GET, &state).await {
        Ok(api_key) => api_key,
        Err(resp) => return resp,
    };
    let params = parse_query(parts.uri.query());
    let symbol = params.get("symbol").cloned();

    match state.open_orders(&api_key, symbol.as_deref()).await {
        Ok(orders) => {
            let list: Vec<Value> = orders
                .into_iter()
                .map(|order| {
                    json!({
                        "orderId": order.id,
                        "orderLinkId": order.request.client_order_id.clone().unwrap_or_default(),
                        "symbol": order.request.symbol,
                        "price": order.request.price.map(decimal_to_string).unwrap_or_else(|| "0".into()),
                        "qty": decimal_to_string(order.request.quantity),
                        "side": map_side(order.request.side),
                        "orderStatus": map_order_status(order.status),
                        "orderType": map_order_type(order.request.order_type),
                        "triggerPrice": order.request.trigger_price.map(decimal_to_string),
                        "cumExecQty": decimal_to_string(order.filled_quantity),
                        "avgPrice": order
                            .avg_fill_price
                            .map(decimal_to_string)
                            .unwrap_or_else(|| "0".into()),
                        "createdTime": order.created_at.timestamp_millis().to_string(),
                        "updatedTime": order.updated_at.timestamp_millis().to_string(),
                    })
                })
                .collect();
            ok_response(json!({ "list": list }))
        }
        Err(err) => bad_request(err.to_string()),
    }
}

async fn create_order(
    state: &MockExchangeState,
    api_key: &str,
    payload: CreateOrderPayload,
) -> Result<Order> {
    let quantity = parse_decimal(&payload.qty, "qty")?;
    if quantity <= Decimal::ZERO {
        return Err(anyhow!("qty must be positive"));
    }
    let side = parse_side(&payload.side)?;
    let order_type = parse_order_type(&payload.order_type)?;
    let price = match payload.price {
        Some(ref value) => Some(parse_decimal(value, "price")?),
        None => None,
    };
    let trigger_price = match payload.trigger_price {
        Some(ref value) => Some(parse_decimal(value, "triggerPrice")?),
        None => None,
    };
    let time_in_force = match payload.time_in_force {
        Some(ref value) => Some(parse_time_in_force(value)?),
        None => None,
    };

    let request = OrderRequest {
        symbol: payload.symbol.clone(),
        side,
        order_type,
        quantity,
        price,
        trigger_price,
        time_in_force,
        client_order_id: payload.order_link_id.clone(),
        take_profit: None,
        stop_loss: None,
        display_quantity: None,
    };
    let now = Utc::now();
    let order = Order {
        id: state.next_order_id().await,
        request,
        status: OrderStatus::Accepted,
        filled_quantity: Decimal::ZERO,
        avg_fill_price: None,
        created_at: now,
        updated_at: now,
    };
    state.register_order(api_key, order.clone()).await
}

fn spawn_fill_plan(
    state: MockExchangeState,
    api_key: String,
    template: Order,
    steps: Vec<OrderFillStep>,
) {
    let order_id = template.id.clone();
    tokio::spawn(async move {
        let default_price = template
            .request
            .price
            .or(template.request.trigger_price)
            .unwrap_or(Decimal::ONE);
        for step in steps {
            sleep(step.after).await;
            let price = step.price.unwrap_or(default_price);
            match state
                .fill_order(&api_key, &order_id, step.quantity, price)
                .await
            {
                Ok((order, fill)) => {
                    if let Err(err) = broadcast_order_update(&state, &order).await {
                        warn!(error = %err, "failed to broadcast scripted order update");
                    }
                    if let Err(err) = broadcast_execution(&state, &fill).await {
                        warn!(error = %err, "failed to broadcast scripted execution");
                    }
                }
                Err(err) => {
                    warn!(order_id = %order_id, error = %err, "scripted fill failed");
                    break;
                }
            }
        }
    });
}

async fn broadcast_order_update(state: &MockExchangeState, order: &Order) -> Result<()> {
    state
        .emit_private_message(order_message(order))
        .await
        .map(|_| ())
}

async fn broadcast_execution(state: &MockExchangeState, fill: &Fill) -> Result<()> {
    state
        .emit_private_message(execution_message(fill))
        .await
        .map(|_| ())
}

fn parse_query(query: Option<&str>) -> HashMap<String, String> {
    query
        .and_then(|raw| serde_urlencoded::from_str(raw).ok())
        .unwrap_or_default()
}

fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    value
        .parse::<i64>()
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
}

async fn authenticate(
    parts: &http::request::Parts,
    body: &Bytes,
    method: Method,
    state: &MockExchangeState,
) -> Result<String, Response<Body>> {
    let headers = &parts.headers;
    let api_key = header_str(headers, "X-BAPI-API-KEY")
        .ok_or_else(|| bad_request("missing X-BAPI-API-KEY header"))?;
    let signature = header_str(headers, "X-BAPI-SIGN")
        .ok_or_else(|| bad_request("missing X-BAPI-SIGN header"))?;
    let timestamp: i64 = header_str(headers, "X-BAPI-TIMESTAMP")
        .ok_or_else(|| bad_request("missing X-BAPI-TIMESTAMP header"))?
        .parse()
        .map_err(|_| bad_request("invalid X-BAPI-TIMESTAMP header"))?;
    let recv_window: u64 = header_str(headers, "X-BAPI-RECV-WINDOW")
        .ok_or_else(|| bad_request("missing X-BAPI-RECV-WINDOW header"))?
        .parse()
        .map_err(|_| bad_request("invalid X-BAPI-RECV-WINDOW header"))?;
    let payload = if method == Method::GET {
        format!(
            "{timestamp}{api_key}{recv_window}{}",
            parts.uri.query().unwrap_or("")
        )
    } else {
        let body_str = std::str::from_utf8(body)
            .map_err(|_| bad_request("body must be valid UTF-8 for signed request"))?;
        format!("{timestamp}{api_key}{recv_window}{body_str}")
    };
    let secret = match state.account_secret(api_key).await {
        Some(secret) => secret,
        None => return Err(unauthorized("unknown API key")),
    };
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|err| error_response(StatusCode::INTERNAL_SERVER_ERROR, -1, err.to_string()))?;
    mac.update(payload.as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());
    if expected != signature {
        return Err(unauthorized("signature mismatch"));
    }
    Ok(api_key.to_string())
}

fn header_str<'a>(headers: &'a hyper::HeaderMap, name: &'static str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn create_envelope(result: Value, ret_code: i64, ret_msg: &str) -> Value {
    json!({
        "retCode": ret_code,
        "retMsg": ret_msg,
        "result": result,
        "retExtInfo": Value::Null,
        "time": Utc::now().timestamp_millis(),
    })
}

fn ok_response(result: Value) -> Response<Body> {
    json_response(StatusCode::OK, create_envelope(result, 0, "OK"))
}

fn bad_request(msg: impl Into<String>) -> Response<Body> {
    error_response(StatusCode::BAD_REQUEST, 10001, msg)
}

fn unauthorized(msg: impl Into<String>) -> Response<Body> {
    error_response(StatusCode::UNAUTHORIZED, 10005, msg)
}

fn not_found() -> Response<Body> {
    error_response(StatusCode::NOT_FOUND, 404, "endpoint not found")
}

fn error_response(status: StatusCode, code: i64, msg: impl Into<String>) -> Response<Body> {
    json_response(status, create_envelope(Value::Null, code, &msg.into()))
}

fn json_response(status: StatusCode, body: Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn decimal_to_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn parse_decimal(value: &str, field: &str) -> Result<Decimal> {
    Decimal::from_str(value).with_context(|| format!("invalid decimal for {field}"))
}

fn parse_side(value: &str) -> Result<Side> {
    match value {
        "Buy" | "buy" => Ok(Side::Buy),
        "Sell" | "sell" => Ok(Side::Sell),
        other => Err(anyhow!("unsupported side '{other}'")),
    }
}

fn parse_order_type(value: &str) -> Result<OrderType> {
    match value {
        "Limit" | "limit" => Ok(OrderType::Limit),
        "Market" | "market" => Ok(OrderType::Market),
        "StopMarket" | "stopMarket" | "Stop" | "stop" => Ok(OrderType::StopMarket),
        other => Err(anyhow!("unsupported orderType '{other}'")),
    }
}

fn parse_time_in_force(value: &str) -> Result<TimeInForce> {
    match value {
        "GTC" => Ok(TimeInForce::GoodTilCanceled),
        "IOC" => Ok(TimeInForce::ImmediateOrCancel),
        "FOK" => Ok(TimeInForce::FillOrKill),
        other => Err(anyhow!("unsupported timeInForce '{other}'")),
    }
}

fn map_side(side: Side) -> &'static str {
    match side {
        Side::Buy => "Buy",
        Side::Sell => "Sell",
    }
}

fn side_label(side: Option<Side>) -> &'static str {
    side.map(map_side).unwrap_or("None")
}

fn map_order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "Market",
        OrderType::Limit => "Limit",
        OrderType::StopMarket => "Market",
    }
}

fn map_order_status(status: OrderStatus) -> &'static str {
    match status {
        OrderStatus::PendingNew => "New",
        OrderStatus::Accepted => "Accepted",
        OrderStatus::PartiallyFilled => "PartiallyFilled",
        OrderStatus::Filled => "Filled",
        OrderStatus::Canceled => "Cancelled",
        OrderStatus::Rejected => "Rejected",
    }
}

fn order_message(order: &Order) -> PrivateMessage {
    let leaves_qty = (order.request.quantity - order.filled_quantity).max(Decimal::ZERO);
    json!({
        "topic": "order",
        "type": "snapshot",
        "data": [{
            "orderId": order.id,
            "symbol": order.request.symbol,
            "side": map_side(order.request.side),
            "orderStatus": map_order_status(order.status),
            "orderType": map_order_type(order.request.order_type),
            "qty": decimal_to_string(order.request.quantity),
            "cumExecQty": decimal_to_string(order.filled_quantity),
            "leavesQty": decimal_to_string(leaves_qty),
            "price": order.request.price.map(decimal_to_string),
            "avgPrice": order.avg_fill_price.map(decimal_to_string).unwrap_or_else(|| "0".into()),
            "orderLinkId": order.request.client_order_id.clone().unwrap_or_default(),
            "updatedTime": order.updated_at.timestamp_millis().to_string(),
        }]
    })
}

fn execution_message(fill: &Fill) -> PrivateMessage {
    json!({
        "topic": "execution",
        "type": "snapshot",
        "data": [{
            "execId": Uuid::new_v4().to_string(),
            "orderId": fill.order_id,
            "symbol": fill.symbol,
            "side": map_side(fill.side),
            "execPrice": decimal_to_string(fill.fill_price),
            "execQty": decimal_to_string(fill.fill_quantity),
            "execFee": fill.fee.map(decimal_to_string).unwrap_or_else(|| "0".into()),
            "execTime": fill.timestamp.timestamp_millis().to_string(),
            "cumExecQty": decimal_to_string(fill.fill_quantity),
            "avgPrice": decimal_to_string(fill.fill_price),
        }]
    })
}

#[derive(Deserialize)]
struct CreateOrderPayload {
    #[allow(dead_code)]
    category: Option<String>,
    symbol: String,
    side: String,
    #[serde(rename = "orderType")]
    order_type: String,
    qty: String,
    price: Option<String>,
    #[serde(rename = "orderLinkId")]
    order_link_id: Option<String>,
    #[serde(rename = "timeInForce")]
    time_in_force: Option<String>,
    #[serde(rename = "triggerPrice")]
    trigger_price: Option<String>,
}

#[derive(Deserialize)]
struct CancelOrderPayload {
    #[allow(dead_code)]
    category: Option<String>,
    #[allow(dead_code)]
    symbol: Option<String>,
    #[serde(rename = "orderId")]
    order_id: Option<String>,
    #[serde(rename = "orderLinkId")]
    order_link_id: Option<String>,
}
