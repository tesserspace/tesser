use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use futures::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{self, json, Value};
use sha2::Sha256;
use tokio::net::TcpStream;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, MissedTickBehavior};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, error, info, warn};

use crate::{millis_to_datetime as parse_millis, BybitCredentials};

type HmacSha256 = Hmac<Sha256>;

use tesser_broker::{BrokerError, BrokerErrorKind, BrokerInfo, BrokerResult, MarketStream};
use tesser_core::{Candle, Fill, Interval, Order, OrderRequest, OrderType, Side, Tick};

#[derive(Clone, Copy, Debug)]
pub enum PublicChannel {
    Linear,
    Inverse,
    Spot,
    Option,
    Spread,
}

impl PublicChannel {
    pub fn as_path(&self) -> &'static str {
        match self {
            Self::Linear => "linear",
            Self::Inverse => "inverse",
            Self::Spot => "spot",
            Self::Option => "option",
            Self::Spread => "spread",
        }
    }
}

impl FromStr for PublicChannel {
    type Err = BrokerError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_lowercase().as_str() {
            "linear" => Ok(Self::Linear),
            "inverse" => Ok(Self::Inverse),
            "spot" => Ok(Self::Spot),
            "option" => Ok(Self::Option),
            "spread" => Ok(Self::Spread),
            other => Err(BrokerError::InvalidRequest(format!(
                "unsupported Bybit public channel '{other}'"
            ))),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum BybitSubscription {
    Trades { symbol: String },
    Kline { symbol: String, interval: Interval },
}

impl BybitSubscription {
    fn topic(&self) -> String {
        match self {
            Self::Trades { symbol } => format!("publicTrade.{symbol}"),
            Self::Kline { symbol, interval } => {
                format!("kline.{}.{}", interval.to_bybit(), symbol)
            }
        }
    }
}

enum WsCommand {
    Subscribe(String),
    Shutdown,
}

pub struct BybitMarketStream {
    info: BrokerInfo,
    command_tx: mpsc::UnboundedSender<WsCommand>,
    tick_rx: Mutex<mpsc::Receiver<Tick>>,
    candle_rx: Mutex<mpsc::Receiver<Candle>>,
}

impl BybitMarketStream {
    pub async fn connect_public(base_url: &str, channel: PublicChannel) -> BrokerResult<Self> {
        let endpoint = format!(
            "{}/v5/public/{}",
            base_url.trim_end_matches('/'),
            channel.as_path()
        );
        let (ws, _) = connect_async(&endpoint)
            .await
            .map_err(|err| BrokerError::from_display(err, BrokerErrorKind::Transport))?;
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (tick_tx, tick_rx) = mpsc::channel(2048);
        let (candle_tx, candle_rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            if let Err(err) = run_ws_loop(ws, command_rx, tick_tx, candle_tx).await {
                error!(error = %err, "bybit ws loop exited unexpectedly");
            }
        });
        Ok(Self {
            info: BrokerInfo {
                name: format!("bybit-{}", channel.as_path()),
                markets: vec![channel.as_path().to_string()],
                supports_testnet: endpoint.contains("testnet"),
            },
            command_tx,
            tick_rx: Mutex::new(tick_rx),
            candle_rx: Mutex::new(candle_rx),
        })
    }
}

pub async fn connect_private(
    base_url: &str,
    creds: &BybitCredentials,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, BrokerError> {
    let endpoint = format!("{}/v5/private", base_url.trim_end_matches('/'));
    let (mut socket, _) = connect_async(&endpoint)
        .await
        .map_err(|e| BrokerError::Transport(e.to_string()))?;

    let expires = (Utc::now() + chrono::Duration::seconds(10)).timestamp_millis();
    let payload = format!("GET/realtime{expires}");
    let mut mac = HmacSha256::new_from_slice(creds.api_secret.as_bytes())
        .map_err(|e| BrokerError::Other(format!("failed to init signer: {e}")))?;
    mac.update(payload.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    let auth_payload = json!({
        "op": "auth",
        "args": [creds.api_key.clone(), expires, signature],
    });

    socket
        .send(Message::Text(auth_payload.to_string()))
        .await
        .map_err(|e| BrokerError::Transport(e.to_string()))?;

    if let Some(Ok(Message::Text(text))) = socket.next().await {
        if let Ok(value) = serde_json::from_str::<Value>(&text) {
            if value
                .get("success")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                info!("Private websocket authenticated");
            } else {
                warn!(payload = text, "Private websocket auth failed");
                return Err(BrokerError::Authentication(
                    "private websocket auth failed".into(),
                ));
            }
        }
    }

    let sub_payload = json!({
        "op": "subscribe",
        "args": ["order", "execution"],
    });
    socket
        .send(Message::Text(sub_payload.to_string()))
        .await
        .map_err(|e| BrokerError::Transport(e.to_string()))?;

    info!("Subscribed to private order/execution channels");

    Ok(socket)
}

#[async_trait::async_trait]
impl MarketStream for BybitMarketStream {
    type Subscription = BybitSubscription;

    fn name(&self) -> &str {
        &self.info.name
    }

    fn info(&self) -> Option<&BrokerInfo> {
        Some(&self.info)
    }

    async fn subscribe(&mut self, subscription: Self::Subscription) -> BrokerResult<()> {
        let topic = subscription.topic();
        self.command_tx
            .send(WsCommand::Subscribe(topic.clone()))
            .map_err(|err| BrokerError::from_display(err, BrokerErrorKind::Transport))?;
        info!(topic, "subscribed to Bybit stream");
        Ok(())
    }

    async fn next_tick(&mut self) -> BrokerResult<Option<Tick>> {
        let mut rx = self.tick_rx.lock().await;
        match rx.try_recv() {
            Ok(tick) => Ok(Some(tick)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Ok(None),
        }
    }

    async fn next_candle(&mut self) -> BrokerResult<Option<Candle>> {
        let mut rx = self.candle_rx.lock().await;
        match rx.try_recv() {
            Ok(candle) => Ok(Some(candle)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Ok(None),
        }
    }
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn run_ws_loop(
    mut socket: WsStream,
    mut commands: mpsc::UnboundedReceiver<WsCommand>,
    tick_tx: mpsc::Sender<Tick>,
    candle_tx: mpsc::Sender<Candle>,
) -> BrokerResult<()> {
    let mut heartbeat = interval(Duration::from_secs(20));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            cmd = commands.recv() => {
                match cmd {
                    Some(WsCommand::Subscribe(topic)) => send_subscribe(&mut socket, &topic).await?,
                    Some(WsCommand::Shutdown) => {
                        let _ = socket.send(Message::Close(None)).await;
                        break;
                    }
                    None => break,
                }
            }
            msg = socket.next() => {
                match msg {
                    Some(Ok(Message::Ping(payload))) => {
                        socket
                            .send(Message::Pong(payload))
                            .await
                            .map_err(|err| BrokerError::from_display(err, BrokerErrorKind::Transport))?;
                    }
                    Some(Ok(message)) => handle_message(message, &tick_tx, &candle_tx).await?,
                    Some(Err(err)) => return Err(BrokerError::from_display(err, BrokerErrorKind::Transport)),
                    None => break,
                }
            }
            _ = heartbeat.tick() => {
                send_ping(&mut socket).await?;
            }
        }
    }

    Ok(())
}

async fn send_subscribe(socket: &mut WsStream, topic: &str) -> BrokerResult<()> {
    let payload = json!({
        "op": "subscribe",
        "args": [topic],
    });
    socket
        .send(Message::Text(payload.to_string()))
        .await
        .map_err(|err| BrokerError::from_display(err, BrokerErrorKind::Transport))
}

async fn send_ping(socket: &mut WsStream) -> BrokerResult<()> {
    let payload = json!({ "op": "ping" });
    socket
        .send(Message::Text(payload.to_string()))
        .await
        .map_err(|err| BrokerError::from_display(err, BrokerErrorKind::Transport))
}

async fn handle_message(
    message: Message,
    tick_tx: &mpsc::Sender<Tick>,
    candle_tx: &mpsc::Sender<Candle>,
) -> BrokerResult<()> {
    match message {
        Message::Text(text) => {
            process_text_message(&text, tick_tx, candle_tx).await;
        }
        Message::Binary(bytes) => {
            if let Ok(text) = String::from_utf8(bytes) {
                process_text_message(&text, tick_tx, candle_tx).await;
            } else {
                warn!("received non UTF-8 binary payload from Bybit ws");
            }
        }
        Message::Ping(payload) => {
            debug!(size = payload.len(), "received ping from Bybit");
        }
        Message::Pong(_) => {
            debug!("received pong from Bybit");
        }
        Message::Close(frame) => {
            debug!(?frame, "bybit stream closed");
            return Ok(());
        }
        Message::Frame(_) => {}
    }
    Ok(())
}

async fn process_text_message(
    text: &str,
    tick_tx: &mpsc::Sender<Tick>,
    candle_tx: &mpsc::Sender<Candle>,
) {
    if let Ok(value) = serde_json::from_str::<Value>(text) {
        if let Some(topic) = value.get("topic").and_then(|t| t.as_str()) {
            if topic.starts_with("publicTrade") {
                if let Ok(payload) = serde_json::from_value::<TradeMessage>(value.clone()) {
                    forward_trades(payload, tick_tx).await;
                }
            } else if topic.starts_with("kline") {
                if let Ok(payload) = serde_json::from_value::<KlineMessage>(value.clone()) {
                    forward_klines(payload, candle_tx).await;
                }
            } else if topic == "order" {
                if let Ok(payload) = serde_json::from_value::<PrivateMessage<BybitWsOrder>>(value) {
                    for order in payload.data {
                        debug!(
                            order_id = %order.order_id,
                            status = %order.order_status,
                            "received ws order update"
                        );
                    }
                }
            } else if topic == "execution" {
                if let Ok(payload) =
                    serde_json::from_value::<PrivateMessage<BybitWsExecution>>(value)
                {
                    for exec in payload.data {
                        debug!(exec_id = %exec.exec_id, "received ws execution");
                    }
                }
            } else {
                debug!(topic, "ignoring unsupported topic from Bybit");
            }
            return;
        }

        if let Some(op) = value.get("op").and_then(|v| v.as_str()) {
            match op {
                "subscribe" => {
                    let success = value
                        .get("success")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);
                    if success {
                        debug!("subscription acknowledged by Bybit");
                    } else {
                        let msg = value
                            .get("ret_msg")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown error");
                        warn!(message = msg, "Bybit rejected subscription request");
                    }
                }
                "ping" | "pong" => {
                    debug!(payload = ?value, "heartbeat ack from Bybit");
                }
                _ => {
                    debug!(payload = ?value, "command response from Bybit");
                }
            }
        }
    } else {
        warn!(payload = text, "failed to parse Bybit ws payload");
    }
}

#[derive(Deserialize, Debug)]
struct TradeMessage {
    _topic: String,
    data: Vec<TradeEntry>,
}

#[derive(Deserialize, Debug)]
struct TradeEntry {
    #[serde(rename = "T")]
    timestamp: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "v")]
    size: String,
    #[serde(rename = "p")]
    price: String,
}

#[derive(Deserialize, Debug)]
struct KlineMessage {
    topic: String,
    data: Vec<KlineEntry>,
}

#[derive(Deserialize, Debug)]
struct KlineEntry {
    _start: i64,
    _end: i64,
    interval: String,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    confirm: bool,
    timestamp: i64,
}

#[derive(Deserialize, Debug)]
pub struct PrivateMessage<T> {
    pub topic: String,
    pub data: Vec<T>,
}

#[derive(Deserialize, Debug)]
pub struct BybitWsOrder {
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "side")]
    pub side: String,
    #[serde(rename = "orderStatus")]
    pub order_status: String,
}

async fn forward_trades(payload: TradeMessage, tick_tx: &mpsc::Sender<Tick>) {
    for trade in payload.data {
        if let Some(tick) = build_tick(&trade) {
            if tick_tx.send(tick).await.is_err() {
                warn!("dropping trade tick; downstream receiver closed");
                break;
            }
        }
    }
}

fn build_tick(entry: &TradeEntry) -> Option<Tick> {
    let price = entry.price.parse().ok()?;
    let size = entry.size.parse().ok()?;
    let side = match entry.side.as_str() {
        "Buy" => Side::Buy,
        "Sell" => Side::Sell,
        _ => return None,
    };
    let exchange_timestamp = millis_to_datetime(entry.timestamp)?;
    Some(Tick {
        symbol: entry.symbol.clone(),
        price,
        size,
        side,
        exchange_timestamp,
        received_at: Utc::now(),
    })
}

async fn forward_klines(payload: KlineMessage, candle_tx: &mpsc::Sender<Candle>) {
    for kline in payload.data {
        if !kline.confirm {
            continue;
        }
        if let Some(candle) = build_candle(&payload.topic, &kline) {
            if candle_tx.send(candle).await.is_err() {
                warn!("dropping kline; downstream receiver closed");
                break;
            }
        }
    }
}

fn build_candle(topic: &str, entry: &KlineEntry) -> Option<Candle> {
    let interval = parse_interval(&entry.interval)?;
    let symbol = topic.split('.').next_back()?.to_string();
    Some(Candle {
        symbol,
        interval,
        open: entry.open.parse().ok()?,
        high: entry.high.parse().ok()?,
        low: entry.low.parse().ok()?,
        close: entry.close.parse().ok()?,
        volume: entry.volume.parse().ok()?,
        timestamp: millis_to_datetime(entry.timestamp)?,
    })
}

fn parse_interval(value: &str) -> Option<Interval> {
    match value {
        "1" => Some(Interval::OneMinute),
        "5" => Some(Interval::FiveMinutes),
        "15" => Some(Interval::FifteenMinutes),
        "60" => Some(Interval::OneHour),
        "240" => Some(Interval::FourHours),
        "D" | "d" => Some(Interval::OneDay),
        _ => None,
    }
}

fn millis_to_datetime(value: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(value).single()
}

impl Drop for BybitMarketStream {
    fn drop(&mut self) {
        let _ = self.command_tx.send(WsCommand::Shutdown);
    }
}

#[derive(Deserialize, Debug)]
pub struct BybitWsExecution {
    #[serde(rename = "execId")]
    pub exec_id: String,
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "execPrice")]
    pub exec_price: String,
    #[serde(rename = "execQty")]
    pub exec_qty: String,
    #[serde(rename = "side")]
    pub side: String,
    #[serde(rename = "execFee")]
    pub exec_fee: String,
    #[serde(rename = "execTime")]
    pub exec_time: String,
    #[serde(rename = "cumExecQty")]
    pub cum_exec_qty: String,
    #[serde(rename = "avgPrice")]
    pub avg_price: String,
}

impl BybitWsOrder {
    pub fn to_tesser_order(&self, existing: Option<&Order>) -> Result<Order, BrokerError> {
        Ok(Order {
            id: self.order_id.clone(),
            request: existing
                .map(|o| o.request.clone())
                .unwrap_or_else(|| OrderRequest {
                    symbol: self.symbol.clone(),
                    side: if self.side == "Buy" {
                        Side::Buy
                    } else {
                        Side::Sell
                    },
                    order_type: OrderType::Market,
                    quantity: 0.0,
                    price: None,
                    trigger_price: None,
                    time_in_force: None,
                    client_order_id: None,
                }),
            status: crate::BybitClient::map_order_status(&self.order_status),
            filled_quantity: existing.map(|o| o.filled_quantity).unwrap_or(0.0),
            avg_fill_price: existing.and_then(|o| o.avg_fill_price),
            created_at: existing.map(|o| o.created_at).unwrap_or_else(Utc::now),
            updated_at: Utc::now(),
        })
    }
}

impl BybitWsExecution {
    pub fn to_tesser_fill(&self) -> Result<Fill, BrokerError> {
        let fill_price = self.exec_price.parse().map_err(|e| {
            BrokerError::Serialization(format!(
                "failed to parse exec price {}: {e}",
                self.exec_price
            ))
        })?;
        let fill_quantity = self.exec_qty.parse().map_err(|e| {
            BrokerError::Serialization(format!("failed to parse exec qty {}: {e}", self.exec_qty))
        })?;
        let fee = self.exec_fee.parse().ok();
        let timestamp = parse_millis(&self.exec_time);
        let side = match self.side.as_str() {
            "Buy" => Side::Buy,
            "Sell" => Side::Sell,
            other => {
                return Err(BrokerError::Serialization(format!(
                    "unhandled execution side: {other}"
                )))
            }
        };

        Ok(Fill {
            order_id: self.order_id.clone(),
            symbol: self.symbol.clone(),
            side,
            fill_price,
            fill_quantity,
            fee,
            timestamp,
        })
    }
}
