use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use futures::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
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
use tesser_core::{
    Candle, ExchangeId, Fill, Interval, LocalOrderBook, Order, OrderBook, OrderBookLevel,
    OrderRequest, OrderType, Side, Symbol, Tick,
};

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
    OrderBook { symbol: String, depth: usize },
}

impl BybitSubscription {
    fn topic(&self) -> String {
        match self {
            Self::Kline { symbol, interval } => {
                format!("kline.{}.{}", interval.to_bybit(), symbol)
            }
            Self::Trades { symbol } => format!("publicTrade.{symbol}"),
            Self::OrderBook { symbol, depth } => {
                format!("orderbook.{depth}.{symbol}")
            }
        }
    }
}

#[derive(Clone, Debug)]
enum WsCommand {
    Subscribe(String),
    Shutdown,
}

pub struct BybitMarketStream {
    info: BrokerInfo,
    command_tx: mpsc::UnboundedSender<WsCommand>,
    tick_rx: Mutex<mpsc::Receiver<Tick>>,
    candle_rx: Mutex<mpsc::Receiver<Candle>>,
    order_book_rx: Mutex<mpsc::Receiver<tesser_core::OrderBook>>,
    connection_status: Option<Arc<AtomicBool>>,
}

impl BybitMarketStream {
    pub async fn connect_public(
        base_url: &str,
        channel: PublicChannel,
        connection_status: Option<Arc<AtomicBool>>,
        exchange: ExchangeId,
    ) -> BrokerResult<Self> {
        let endpoint = format!(
            "{}/v5/public/{}",
            base_url.trim_end_matches('/'),
            channel.as_path()
        );
        let (ws, _) = connect_async(&endpoint)
            .await
            .map_err(|err| BrokerError::from_display(err, BrokerErrorKind::Transport))?;
        if let Some(flag) = &connection_status {
            flag.store(true, Ordering::SeqCst);
        }
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let command_loop = command_tx.clone();
        let (tick_tx, tick_rx) = mpsc::channel(2048);
        let (candle_tx, candle_rx) = mpsc::channel(1024);
        let (order_book_tx, order_book_rx) = mpsc::channel(256);
        let status_for_loop = connection_status.clone();
        let exchange_id = exchange;
        tokio::spawn(async move {
            if let Err(err) = run_ws_loop(
                ws,
                command_rx,
                command_loop,
                tick_tx,
                candle_tx,
                order_book_tx,
                status_for_loop,
                exchange_id,
            )
            .await
            {
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
            order_book_rx: Mutex::new(order_book_rx),
            connection_status,
        })
    }

    pub fn connection_status(&self) -> Option<Arc<AtomicBool>> {
        self.connection_status.clone()
    }
}

pub async fn connect_private(
    base_url: &str,
    creds: &BybitCredentials,
    connection_status: Option<Arc<AtomicBool>>,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, BrokerError> {
    let endpoint = format!("{}/v5/private", base_url.trim_end_matches('/'));
    let (mut socket, _) = match connect_async(&endpoint).await {
        Ok(value) => {
            if let Some(flag) = &connection_status {
                flag.store(true, Ordering::SeqCst);
            }
            value
        }
        Err(err) => {
            if let Some(flag) = &connection_status {
                flag.store(false, Ordering::SeqCst);
            }
            return Err(BrokerError::Transport(err.to_string()));
        }
    };

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

    async fn next_order_book(&mut self) -> BrokerResult<Option<tesser_core::OrderBook>> {
        let mut rx = self.order_book_rx.lock().await;
        match rx.try_recv() {
            Ok(book) => Ok(Some(book)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Ok(None),
        }
    }
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn run_ws_loop(
    mut socket: WsStream,
    mut commands: mpsc::UnboundedReceiver<WsCommand>,
    command_tx: mpsc::UnboundedSender<WsCommand>,
    tick_tx: mpsc::Sender<Tick>,
    candle_tx: mpsc::Sender<Candle>,
    order_book_tx: mpsc::Sender<OrderBook>,
    connection_status: Option<Arc<AtomicBool>>,
    exchange: ExchangeId,
) -> BrokerResult<()> {
    let mut heartbeat = interval(Duration::from_secs(20));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

    if let Some(flag) = &connection_status {
        flag.store(true, Ordering::SeqCst);
    }

    let mut book_manager = BookManager::new(exchange, order_book_tx.clone(), command_tx);

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
                    Some(Ok(message)) => {
                        handle_message(
                            message,
                            &tick_tx,
                            &candle_tx,
                            &mut book_manager,
                            exchange,
                        )
                        .await?
                    }
                    Some(Err(err)) => return Err(BrokerError::from_display(err, BrokerErrorKind::Transport)),
                    None => break,
                }
            }
            _ = heartbeat.tick() => {
                send_ping(&mut socket).await?;
            }
        }
    }

    if let Some(flag) = connection_status {
        flag.store(false, Ordering::SeqCst);
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
    book_manager: &mut BookManager,
    exchange: ExchangeId,
) -> BrokerResult<()> {
    match message {
        Message::Text(text) => {
            process_text_message(&text, tick_tx, candle_tx, book_manager, exchange).await;
        }
        Message::Binary(bytes) => {
            if let Ok(text) = String::from_utf8(bytes) {
                process_text_message(&text, tick_tx, candle_tx, book_manager, exchange).await;
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
    book_manager: &mut BookManager,
    exchange: ExchangeId,
) {
    if let Ok(value) = serde_json::from_str::<Value>(text) {
        if let Some(topic) = value.get("topic").and_then(|t| t.as_str()) {
            if topic.starts_with("publicTrade") {
                if let Ok(payload) = serde_json::from_value::<TradeMessage>(value.clone()) {
                    forward_trades(exchange, payload, tick_tx).await;
                }
            } else if topic.starts_with("kline") {
                if let Ok(payload) = serde_json::from_value::<KlineMessage>(value.clone()) {
                    forward_klines(exchange, payload, candle_tx).await;
                }
            } else if topic.starts_with("orderbook") {
                if let Ok(payload) = serde_json::from_value::<OrderbookMessage>(value.clone()) {
                    book_manager.handle(payload).await;
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

async fn forward_trades(
    exchange: ExchangeId,
    payload: TradeMessage,
    tick_tx: &mpsc::Sender<Tick>,
) {
    for trade in payload.data {
        if let Some(tick) = build_tick(exchange, &trade) {
            if tick_tx.send(tick).await.is_err() {
                warn!("dropping trade tick; downstream receiver closed");
                break;
            }
        }
    }
}

fn build_tick(exchange: ExchangeId, entry: &TradeEntry) -> Option<Tick> {
    let price = entry.price.parse().ok()?;
    let size = entry.size.parse().ok()?;
    let side = match entry.side.as_str() {
        "Buy" => Side::Buy,
        "Sell" => Side::Sell,
        _ => return None,
    };
    let exchange_timestamp = millis_to_datetime(entry.timestamp)?;
    Some(Tick {
        symbol: Symbol::from_code(exchange, &entry.symbol),
        price,
        size,
        side,
        exchange_timestamp,
        received_at: Utc::now(),
    })
}

async fn forward_klines(
    exchange: ExchangeId,
    payload: KlineMessage,
    candle_tx: &mpsc::Sender<Candle>,
) {
    for kline in payload.data {
        if !kline.confirm {
            continue;
        }
        if let Some(candle) = build_candle(exchange, &payload.topic, &kline) {
            if candle_tx.send(candle).await.is_err() {
                warn!("dropping kline; downstream receiver closed");
                break;
            }
        }
    }
}

fn build_candle(exchange: ExchangeId, topic: &str, entry: &KlineEntry) -> Option<Candle> {
    let interval = parse_interval(&entry.interval)?;
    let symbol = topic.split('.').next_back()?.to_string();
    Some(Candle {
        symbol: Symbol::from_code(exchange, &symbol),
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

fn parse_levels(entries: &[[String; 2]]) -> Option<Vec<(Decimal, Decimal)>> {
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let price = entry.first()?.parse().ok()?;
        let qty = entry.get(1)?.parse().ok()?;
        out.push((price, qty));
    }
    Some(out)
}

fn parse_topic(topic: &str) -> Option<(usize, String)> {
    let mut parts = topic.split('.');
    let kind = parts.next()?;
    if kind != "orderbook" {
        return None;
    }
    let depth = parts.next()?.parse().ok()?;
    let symbol = parts.next()?.to_string();
    Some((depth, symbol))
}

#[derive(Deserialize, Debug)]
struct OrderbookMessage {
    topic: String,
    #[serde(rename = "type")]
    msg_type: String, // "snapshot" or "delta"
    ts: i64,
    data: Vec<OrderbookData>,
}

#[derive(Clone, Deserialize, Debug)]
struct OrderbookData {
    s: String,
    b: Vec<[String; 2]>, // Bids
    a: Vec<[String; 2]>, // Asks
    #[serde(rename = "u")]
    update_id: i64,
    #[serde(rename = "seq", default)]
    seq: Option<i64>,
    #[serde(rename = "prev_seq", default)]
    prev_seq: Option<i64>,
    #[serde(rename = "pu", default)]
    prev_update_id: Option<i64>,
    #[serde(rename = "checksum", default)]
    checksum: Option<u32>,
}

impl OrderbookData {
    fn sequence(&self) -> i64 {
        self.seq.unwrap_or(self.update_id)
    }

    fn previous_sequence(&self) -> Option<i64> {
        self.prev_seq.or(self.prev_update_id)
    }
}

struct BookManager {
    streams: HashMap<String, SymbolBook>,
    order_book_tx: mpsc::Sender<OrderBook>,
    command_tx: mpsc::UnboundedSender<WsCommand>,
    exchange: ExchangeId,
}

impl BookManager {
    fn new(
        exchange: ExchangeId,
        order_book_tx: mpsc::Sender<OrderBook>,
        command_tx: mpsc::UnboundedSender<WsCommand>,
    ) -> Self {
        Self {
            streams: HashMap::new(),
            order_book_tx,
            command_tx,
            exchange,
        }
    }

    async fn handle(&mut self, payload: OrderbookMessage) {
        let Some((depth, _)) = parse_topic(&payload.topic) else {
            return;
        };
        let Some(data) = payload.data.into_iter().next() else {
            return;
        };
        let symbol = data.s.clone();
        let stream = self
            .streams
            .entry(payload.topic.clone())
            .or_insert_with(|| SymbolBook::new(self.exchange, payload.topic.clone(), symbol, depth));

        match stream.ingest(payload.msg_type.as_str(), data, payload.ts) {
            BookUpdate::Pending => {}
            BookUpdate::OutOfSync => {
                warn!(topic = %payload.topic, "order book sequence gap detected; resubscribing");
                let _ = self.command_tx.send(WsCommand::Subscribe(payload.topic));
            }
            BookUpdate::Updates(mut books) => {
                for book in books.drain(..) {
                    if self.order_book_tx.send(book).await.is_err() {
                        warn!("dropping order book; downstream receiver closed");
                        break;
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct BookLevel {
    price: Decimal,
    quantity: Decimal,
}

struct PendingDelta {
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    seq: i64,
    prev_seq: Option<i64>,
    ts: i64,
}

impl PendingDelta {
    fn from_data(data: OrderbookData, ts: i64) -> Option<Self> {
        let bids = parse_levels(&data.b)?
            .into_iter()
            .map(|(price, quantity)| BookLevel { price, quantity })
            .collect();
        let asks = parse_levels(&data.a)?
            .into_iter()
            .map(|(price, quantity)| BookLevel { price, quantity })
            .collect();
        Some(Self {
            bids,
            asks,
            seq: data.sequence(),
            prev_seq: data.previous_sequence(),
            ts,
        })
    }
}

struct SymbolBook {
    exchange: ExchangeId,
    symbol: String,
    depth: usize,
    book: LocalOrderBook,
    last_seq: Option<i64>,
    synced: bool,
    pending: Vec<PendingDelta>,
    last_checksum: Option<u32>,
}

impl SymbolBook {
    fn new(exchange: ExchangeId, _topic: String, symbol: String, depth: usize) -> Self {
        Self {
            exchange,
            symbol,
            depth,
            book: LocalOrderBook::new(),
            last_seq: None,
            synced: false,
            pending: Vec::new(),
            last_checksum: None,
        }
    }

    fn ingest(&mut self, msg_type: &str, data: OrderbookData, ts: i64) -> BookUpdate {
        match msg_type {
            "snapshot" => self.apply_snapshot(data, ts),
            "delta" => self.apply_delta(data, ts),
            _ => BookUpdate::Pending,
        }
    }

    fn apply_snapshot(&mut self, data: OrderbookData, ts: i64) -> BookUpdate {
        self.last_checksum = data.checksum;
        let Some(snapshot_bids) = parse_levels(&data.b) else {
            return BookUpdate::Pending;
        };
        let Some(snapshot_asks) = parse_levels(&data.a) else {
            return BookUpdate::Pending;
        };
        self.book.load_snapshot(&snapshot_bids, &snapshot_asks);
        self.last_seq = Some(data.sequence());
        self.synced = true;
        let mut updates = Vec::new();
        if let Some(book) = self.snapshot(ts) {
            updates.push(book);
        }
        let pending = std::mem::take(&mut self.pending);
        for delta in pending {
            match self.apply_pending(delta) {
                ApplyOutcome::Gap => return BookUpdate::OutOfSync,
                ApplyOutcome::Updates(mut book_updates) => updates.append(&mut book_updates),
                ApplyOutcome::Pending => {}
            }
        }
        BookUpdate::Updates(updates)
    }

    fn apply_delta(&mut self, data: OrderbookData, ts: i64) -> BookUpdate {
        self.last_checksum = data.checksum;
        let Some(delta) = PendingDelta::from_data(data, ts) else {
            return BookUpdate::Pending;
        };
        if !self.synced {
            self.pending.push(delta);
            return BookUpdate::Pending;
        }
        match self.apply_pending(delta) {
            ApplyOutcome::Gap => BookUpdate::OutOfSync,
            ApplyOutcome::Pending => BookUpdate::Pending,
            ApplyOutcome::Updates(updates) => BookUpdate::Updates(updates),
        }
    }

    fn apply_pending(&mut self, delta: PendingDelta) -> ApplyOutcome {
        if let Some(last) = self.last_seq {
            if let Some(prev) = delta.prev_seq {
                if prev != last {
                    self.reset();
                    return ApplyOutcome::Gap;
                }
            } else if delta.seq - 1 != last {
                self.reset();
                return ApplyOutcome::Gap;
            }
        } else {
            self.pending.push(delta);
            return ApplyOutcome::Pending;
        }

        for level in &delta.bids {
            self.book
                .apply_delta(Side::Buy, level.price, level.quantity);
        }
        for level in &delta.asks {
            self.book
                .apply_delta(Side::Sell, level.price, level.quantity);
        }
        self.last_seq = Some(delta.seq);

        if let Some(book) = self.snapshot(delta.ts) {
            ApplyOutcome::Updates(vec![book])
        } else {
            ApplyOutcome::Updates(Vec::new())
        }
    }

    fn snapshot(&self, ts: i64) -> Option<OrderBook> {
        if self.book.is_empty() {
            return None;
        }
        let timestamp = millis_to_datetime(ts)?;
        let bids = self
            .book
            .bid_levels(self.depth)
            .into_iter()
            .map(|(price, size)| OrderBookLevel { price, size })
            .collect::<Vec<_>>();
        let asks = self
            .book
            .ask_levels(self.depth)
            .into_iter()
            .map(|(price, size)| OrderBookLevel { price, size })
            .collect::<Vec<_>>();
        Some(OrderBook {
            symbol: Symbol::from_code(self.exchange, &self.symbol),
            bids,
            asks,
            timestamp,
            exchange_checksum: self.last_checksum,
            local_checksum: Some(self.book.checksum(self.depth)),
        })
    }

    fn reset(&mut self) {
        self.synced = false;
        self.last_seq = None;
        self.pending.clear();
    }
}

enum ApplyOutcome {
    Updates(Vec<OrderBook>),
    Pending,
    Gap,
}

enum BookUpdate {
    Updates(Vec<OrderBook>),
    Pending,
    OutOfSync,
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
    pub fn to_tesser_order(
        &self,
        exchange: ExchangeId,
        existing: Option<&Order>,
    ) -> Result<Order, BrokerError> {
        Ok(Order {
            id: self.order_id.clone(),
            request: existing
                .map(|o| o.request.clone())
                .unwrap_or_else(|| OrderRequest {
                    symbol: Symbol::from_code(exchange, &self.symbol),
                    side: if self.side == "Buy" {
                        Side::Buy
                    } else {
                        Side::Sell
                    },
                    order_type: OrderType::Market,
                    quantity: Decimal::ZERO,
                    price: None,
                    trigger_price: None,
                    time_in_force: None,
                    client_order_id: None,
                    take_profit: None,
                    stop_loss: None,
                    display_quantity: None,
                }),
            status: crate::BybitClient::map_order_status(&self.order_status),
            filled_quantity: existing.map(|o| o.filled_quantity).unwrap_or(Decimal::ZERO),
            avg_fill_price: existing.and_then(|o| o.avg_fill_price),
            created_at: existing.map(|o| o.created_at).unwrap_or_else(Utc::now),
            updated_at: Utc::now(),
        })
    }
}

impl BybitWsExecution {
    pub fn to_tesser_fill(&self, exchange: ExchangeId) -> Result<Fill, BrokerError> {
        let fill_price = self.exec_price.parse::<Decimal>().map_err(|e| {
            BrokerError::Serialization(format!(
                "failed to parse exec price {}: {e}",
                self.exec_price
            ))
        })?;
        let fill_quantity = self.exec_qty.parse::<Decimal>().map_err(|e| {
            BrokerError::Serialization(format!("failed to parse exec qty {}: {e}", self.exec_qty))
        })?;
        let fee = self.exec_fee.parse::<Decimal>().ok();
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
            symbol: Symbol::from_code(exchange, &self.symbol),
            side,
            fill_price,
            fill_quantity,
            fee,
            timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_levels(levels: &[(&str, &str)]) -> Vec<[String; 2]> {
        levels
            .iter()
            .map(|(price, qty)| [price.to_string(), qty.to_string()])
            .collect()
    }

    fn sample_data(
        symbol: &str,
        bids: &[(&str, &str)],
        asks: &[(&str, &str)],
        seq: i64,
        prev_seq: Option<i64>,
    ) -> OrderbookData {
        OrderbookData {
            s: symbol.into(),
            b: sample_levels(bids),
            a: sample_levels(asks),
            update_id: seq,
            seq: Some(seq),
            prev_seq,
            prev_update_id: None,
            checksum: None,
        }
    }

    #[tokio::test]
    async fn book_manager_applies_snapshot_and_deltas() {
        let (book_tx, mut book_rx) = mpsc::channel(8);
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let mut manager = BookManager::new(book_tx, cmd_tx);

        let snapshot = OrderbookMessage {
            topic: "orderbook.2.BTCUSDT".into(),
            msg_type: "snapshot".into(),
            ts: 1,
            data: vec![sample_data(
                "BTCUSDT",
                &[("100", "1"), ("99", "2")],
                &[("101", "1"), ("102", "2")],
                10,
                Some(9),
            )],
        };
        manager.handle(snapshot).await;
        let first = book_rx.recv().await.expect("snapshot missing");
        assert_eq!(first.bids[0].price, Decimal::from(100));
        assert_eq!(first.asks[0].price, Decimal::from(101));

        let delta = OrderbookMessage {
            topic: "orderbook.2.BTCUSDT".into(),
            msg_type: "delta".into(),
            ts: 2,
            data: vec![sample_data(
                "BTCUSDT",
                &[("100", "0"), ("98", "1")],
                &[("101", "2")],
                11,
                Some(10),
            )],
        };
        manager.handle(delta).await;
        let update = book_rx.recv().await.expect("delta missing");
        assert_eq!(update.bids.len(), 2);
        assert_eq!(update.bids[1].price, Decimal::from(98));
        assert_eq!(update.asks[0].size, Decimal::from(2));
    }

    #[tokio::test]
    async fn book_manager_requests_resub_on_gap() {
        let (book_tx, mut book_rx) = mpsc::channel(8);
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        let mut manager = BookManager::new(book_tx, cmd_tx.clone());

        let snapshot = OrderbookMessage {
            topic: "orderbook.1.BTCUSDT".into(),
            msg_type: "snapshot".into(),
            ts: 1,
            data: vec![sample_data(
                "BTCUSDT",
                &[("100", "1")],
                &[("101", "1")],
                5,
                Some(4),
            )],
        };
        manager.handle(snapshot).await;
        book_rx.recv().await.expect("snapshot missing");

        let gap_delta = OrderbookMessage {
            topic: "orderbook.1.BTCUSDT".into(),
            msg_type: "delta".into(),
            ts: 2,
            data: vec![sample_data(
                "BTCUSDT",
                &[("100", "0")],
                &[("101", "2")],
                8,
                Some(6),
            )],
        };
        manager.handle(gap_delta).await;

        let resub = cmd_rx.recv().await.expect("resubscribe missing");
        match resub {
            WsCommand::Subscribe(topic) => assert_eq!(topic, "orderbook.1.BTCUSDT"),
            _ => panic!("unexpected command {:?}", resub),
        }
    }
}
