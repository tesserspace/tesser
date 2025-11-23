use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use binance_sdk::common::{
    config::ConfigurationWebsocketStreams,
    models::WebsocketEvent,
    websocket::{Subscription, WebsocketStream},
};
pub use binance_sdk::derivatives_trading_usds_futures::websocket_streams::UserDataStreamEventsResponse;
use binance_sdk::derivatives_trading_usds_futures::{
    self as binance_futures,
    websocket_streams::{
        self, AggregateTradeStreamsParams, AggregateTradeStreamsResponse,
        KlineCandlestickStreamsParams, KlineCandlestickStreamsResponse, OrderTradeUpdateO,
    },
};
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tesser_broker::{BrokerError, BrokerInfo, BrokerResult, MarketStream};
use tesser_core::{
    Candle, ExchangeId, Interval, LocalOrderBook, OrderBook, OrderBookLevel, Side, Symbol, Tick,
};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::warn;

use crate::{parse_decimal_opt, timestamp_from_ms};

#[derive(Clone, Debug, Serialize)]
pub enum BinanceSubscription {
    Trades { symbol: String },
    Kline { symbol: String, interval: Interval },
    OrderBook { symbol: String, depth: usize },
}

pub struct BinanceMarketStream {
    info: BrokerInfo,
    ws: Arc<websocket_streams::WebsocketStreams>,
    rest_url: String,
    ws_base_url: String,
    http: Client,
    tick_tx: mpsc::Sender<Tick>,
    candle_tx: mpsc::Sender<Candle>,
    book_tx: mpsc::Sender<OrderBook>,
    tick_rx: Mutex<mpsc::Receiver<Tick>>,
    candle_rx: Mutex<mpsc::Receiver<Candle>>,
    book_rx: Mutex<mpsc::Receiver<OrderBook>>,
    handles: Vec<StreamHandle>,
    _event_subscription: Option<Subscription>,
    exchange: ExchangeId,
}

#[allow(dead_code)]
enum StreamHandle {
    Trade(Arc<WebsocketStream<AggregateTradeStreamsResponse>>),
    Kline(Arc<WebsocketStream<KlineCandlestickStreamsResponse>>),
    Diff(tokio::task::JoinHandle<()>),
}

impl BinanceMarketStream {
    pub async fn connect(
        ws_url: &str,
        rest_url: &str,
        connection_status: Option<Arc<AtomicBool>>,
        exchange: ExchangeId,
    ) -> BrokerResult<Self> {
        let cfg = ConfigurationWebsocketStreams::builder()
            .ws_url(ws_url.to_string())
            .build()
            .map_err(|err| BrokerError::Transport(err.to_string()))?;
        let handle = binance_futures::DerivativesTradingUsdsFuturesWsStreams::from_config(cfg);
        let ws = handle
            .connect()
            .await
            .map_err(|err| BrokerError::Transport(err.to_string()))?;
        let ws = Arc::new(ws);
        if let Some(flag) = &connection_status {
            flag.store(true, Ordering::SeqCst);
        }
        let event_subscription = connection_status.clone().map(|flag| {
            ws.subscribe_on_ws_events(move |event| match event {
                WebsocketEvent::Open => flag.store(true, Ordering::SeqCst),
                WebsocketEvent::Close(_, _) | WebsocketEvent::Error(_) => {
                    flag.store(false, Ordering::SeqCst)
                }
                _ => {}
            })
        });
        let (tick_tx, tick_rx) = mpsc::channel(2048);
        let (candle_tx, candle_rx) = mpsc::channel(1024);
        let (book_tx, book_rx) = mpsc::channel(256);
        let http = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|err| BrokerError::Other(err.to_string()))?;
        let ws_base_url = ws_url.trim_end_matches("/stream").trim_end_matches('/');
        let ws_base = if ws_base_url.is_empty() {
            ws_url.to_string()
        } else {
            ws_base_url.to_string()
        };
        Ok(Self {
            info: BrokerInfo {
                name: "binance-market".into(),
                markets: vec!["usd_perp".into()],
                supports_testnet: ws_url.contains("testnet"),
            },
            ws,
            rest_url: rest_url.to_string(),
            ws_base_url: ws_base,
            http,
            tick_tx,
            candle_tx,
            book_tx,
            tick_rx: Mutex::new(tick_rx),
            candle_rx: Mutex::new(candle_rx),
            book_rx: Mutex::new(book_rx),
            handles: Vec::new(),
            _event_subscription: event_subscription,
            exchange,
        })
    }

    async fn subscribe_trades(&mut self, symbol: String) -> BrokerResult<()> {
        let params = AggregateTradeStreamsParams::builder(symbol.to_lowercase())
            .build()
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let stream = self
            .ws
            .aggregate_trade_streams(params)
            .await
            .map_err(|err| BrokerError::Transport(err.to_string()))?;
        let tx = self.tick_tx.clone();
        let exchange = self.exchange;
        stream.on_message(move |payload: AggregateTradeStreamsResponse| {
            if let Some(tick) = convert_trade(exchange, &payload) {
                let _ = tx.try_send(tick);
            }
        });
        self.handles.push(StreamHandle::Trade(stream));
        Ok(())
    }

    async fn subscribe_kline(&mut self, symbol: String, interval: Interval) -> BrokerResult<()> {
        let params =
            KlineCandlestickStreamsParams::builder(symbol.to_lowercase(), interval_label(interval))
                .build()
                .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let stream = self
            .ws
            .kline_candlestick_streams(params)
            .await
            .map_err(|err| BrokerError::Transport(err.to_string()))?;
        let tx = self.candle_tx.clone();
        let exchange = self.exchange;
        stream.on_message(move |payload: KlineCandlestickStreamsResponse| {
            if let Some(candle) = convert_kline(exchange, &payload) {
                let _ = tx.try_send(candle);
            }
        });
        self.handles.push(StreamHandle::Kline(stream));
        Ok(())
    }

    async fn subscribe_order_book(&mut self, symbol: String, depth: usize) -> BrokerResult<()> {
        let ws_base = self.ws_base_url.clone();
        let rest_url = self.rest_url.clone();
        let http = self.http.clone();
        let tx = self.book_tx.clone();
        let depth = depth.max(1);
        let exchange = self.exchange;
        let handle = tokio::spawn(async move {
            let mut task = DiffDepthTask::new(symbol, depth, rest_url, ws_base, http, tx, exchange);
            let symbol = task.symbol_upper().to_string();
            if let Err(err) = task.run().await {
                warn!(symbol = %symbol, error = %err, "binance depth stream terminated");
            }
        });
        self.handles.push(StreamHandle::Diff(handle));
        Ok(())
    }
}

#[async_trait::async_trait]
impl MarketStream for BinanceMarketStream {
    type Subscription = BinanceSubscription;

    fn name(&self) -> &str {
        &self.info.name
    }

    fn info(&self) -> Option<&BrokerInfo> {
        Some(&self.info)
    }

    async fn subscribe(&mut self, subscription: Self::Subscription) -> BrokerResult<()> {
        match subscription {
            BinanceSubscription::Trades { symbol } => self.subscribe_trades(symbol).await,
            BinanceSubscription::Kline { symbol, interval } => {
                self.subscribe_kline(symbol, interval).await
            }
            BinanceSubscription::OrderBook { symbol, depth } => {
                self.subscribe_order_book(symbol, depth).await
            }
        }
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

    async fn next_order_book(&mut self) -> BrokerResult<Option<OrderBook>> {
        let mut rx = self.book_rx.lock().await;
        match rx.try_recv() {
            Ok(book) => Ok(Some(book)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Ok(None),
        }
    }
}

fn convert_trade(exchange: ExchangeId, payload: &AggregateTradeStreamsResponse) -> Option<Tick> {
    let symbol = payload.s.clone()?;
    let price = parse_decimal_opt(payload.p.as_deref())?;
    let quantity = parse_decimal_opt(payload.q.as_deref())?;
    let side = match payload.m.unwrap_or(false) {
        true => Side::Sell,
        false => Side::Buy,
    };
    Some(Tick {
        symbol: Symbol::from_code(exchange, symbol),
        price,
        size: quantity,
        side,
        exchange_timestamp: timestamp_from_ms(payload.t_uppercase),
        received_at: Utc::now(),
    })
}

fn convert_kline(exchange: ExchangeId, payload: &KlineCandlestickStreamsResponse) -> Option<Candle> {
    let kline = payload.k.as_ref()?;
    let symbol = kline.s.clone()?;
    let interval = Interval::from_str(kline.i.as_deref().unwrap_or("1m")).ok()?;
    let open = parse_decimal_opt(kline.o.as_deref())?;
    let high = parse_decimal_opt(kline.h.as_deref())?;
    let low = parse_decimal_opt(kline.l.as_deref())?;
    let close = parse_decimal_opt(kline.c.as_deref())?;
    let volume = parse_decimal_opt(kline.v.as_deref()).unwrap_or(Decimal::ZERO);
    Some(Candle {
        symbol: Symbol::from_code(exchange, symbol),
        interval,
        open,
        high,
        low,
        close,
        volume,
        timestamp: timestamp_from_ms(kline.t),
    })
}

struct DiffDepthTask {
    symbol_upper: String,
    symbol_lower: String,
    depth: usize,
    snapshot_limit: usize,
    rest_url: String,
    ws_base_url: String,
    http: Client,
    tx: mpsc::Sender<OrderBook>,
    book: LocalOrderBook,
    last_update_id: u64,
    exchange: ExchangeId,
}

impl DiffDepthTask {
    fn new(
        symbol: String,
        depth: usize,
        rest_url: String,
        ws_base_url: String,
        http: Client,
        tx: mpsc::Sender<OrderBook>,
        exchange: ExchangeId,
    ) -> Self {
        let upper = symbol.to_uppercase();
        let lower = symbol.to_lowercase();
        Self {
            symbol_upper: upper,
            symbol_lower: lower,
            depth,
            snapshot_limit: snapshot_limit(depth),
            rest_url,
            ws_base_url,
            http,
            tx,
            book: LocalOrderBook::new(),
            last_update_id: 0,
            exchange,
        }
    }

    fn symbol_upper(&self) -> &str {
        &self.symbol_upper
    }

    async fn run(&mut self) -> BrokerResult<()> {
        loop {
            match self.stream_once().await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    warn!(
                        symbol = %self.symbol_upper,
                        error = %err,
                        "binance diff depth stream restarting"
                    );
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn stream_once(&mut self) -> BrokerResult<()> {
        self.sync_snapshot().await?;
        let url = format!(
            "{}/ws/{}@depth@100ms",
            self.ws_base_url.trim_end_matches('/'),
            self.symbol_lower
        );
        let (mut ws, _) = connect_async(&url)
            .await
            .map_err(|err| BrokerError::Transport(err.to_string()))?;
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    self.process_message(&text).await?;
                }
                Ok(Message::Binary(bytes)) => {
                    if let Ok(text) = String::from_utf8(bytes) {
                        self.process_message(&text).await?;
                    }
                }
                Ok(Message::Ping(payload)) => {
                    ws.send(Message::Pong(payload))
                        .await
                        .map_err(|err| BrokerError::Transport(err.to_string()))?;
                }
                Ok(Message::Pong(_)) => {}
                Ok(Message::Close(_)) => {
                    return Err(BrokerError::Other("binance depth stream closed".into()));
                }
                Ok(Message::Frame(_)) => {}
                Err(err) => return Err(BrokerError::Transport(err.to_string())),
            }
        }
        Err(BrokerError::Other("binance depth stream terminated".into()))
    }

    async fn sync_snapshot(&mut self) -> BrokerResult<()> {
        let snapshot = self.fetch_snapshot().await?;
        let bids = convert_price_levels(&snapshot.bids)
            .ok_or_else(|| BrokerError::Serialization("invalid snapshot bids".into()))?;
        let asks = convert_price_levels(&snapshot.asks)
            .ok_or_else(|| BrokerError::Serialization("invalid snapshot asks".into()))?;
        self.book.load_snapshot(&bids, &asks);
        self.last_update_id = snapshot.last_update_id;
        self.emit_snapshot(Utc::now()).await;
        Ok(())
    }

    async fn fetch_snapshot(&self) -> BrokerResult<DepthSnapshot> {
        let url = format!(
            "{}/fapi/v1/depth?symbol={}&limit={}",
            self.rest_url.trim_end_matches('/'),
            self.symbol_upper,
            self.snapshot_limit
        );
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|err| BrokerError::Transport(err.to_string()))?
            .error_for_status()
            .map_err(|err| BrokerError::Transport(err.to_string()))?
            .json::<DepthSnapshot>()
            .await
            .map_err(|err| BrokerError::Serialization(err.to_string()))?;
        Ok(resp)
    }

    async fn process_message(&mut self, text: &str) -> BrokerResult<()> {
        let event: DiffDepthEvent = serde_json::from_str(text)
            .map_err(|err| BrokerError::Serialization(err.to_string()))?;
        if event.final_update_id <= self.last_update_id {
            return Ok(());
        }
        let expected = self
            .last_update_id
            .checked_add(1)
            .ok_or_else(|| BrokerError::Other("sequence overflow".into()))?;
        if event.first_update_id > expected {
            return Err(BrokerError::Other(format!(
                "diff depth gap detected: expected {}, got {}",
                expected, event.first_update_id
            )));
        }
        if event.final_update_id < expected {
            return Ok(());
        }
        self.apply_levels(&event.bids, Side::Buy)
            .map_err(BrokerError::Serialization)?;
        self.apply_levels(&event.asks, Side::Sell)
            .map_err(BrokerError::Serialization)?;
        self.last_update_id = event.final_update_id;
        self.emit_book(event.event_time).await;
        Ok(())
    }

    fn apply_levels(&mut self, levels: &[[String; 2]], side: Side) -> Result<(), String> {
        for entry in levels {
            let price = entry
                .first()
                .ok_or_else(|| "missing price".to_string())
                .and_then(|v| Decimal::from_str(v).map_err(|err| err.to_string()))?;
            let qty = entry
                .get(1)
                .ok_or_else(|| "missing quantity".to_string())
                .and_then(|v| Decimal::from_str(v).map_err(|err| err.to_string()))?;
            self.book.apply_delta(side, price, qty);
        }
        Ok(())
    }

    async fn emit_snapshot(&self, ts: DateTime<Utc>) {
        if let Some(book) = self.build_book(ts) {
            let _ = self.tx.send(book).await;
        }
    }

    async fn emit_book(&self, event_time: i64) {
        let timestamp = timestamp_from_ms(Some(event_time));
        if let Some(book) = self.build_book(timestamp) {
            let _ = self.tx.send(book).await;
        }
    }

    fn build_book(&self, timestamp: DateTime<Utc>) -> Option<OrderBook> {
        if self.book.is_empty() {
            return None;
        }
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
            symbol: Symbol::from_code(self.exchange, &self.symbol_upper),
            bids,
            asks,
            timestamp,
            exchange_checksum: None,
            local_checksum: Some(self.book.checksum(self.depth)),
        })
    }
}

#[derive(Debug, Deserialize)]
struct DepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct DiffDepthEvent {
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
    #[serde(rename = "E")]
    event_time: i64,
}

fn snapshot_limit(depth: usize) -> usize {
    match depth {
        d if d <= 5 => 5,
        d if d <= 10 => 10,
        d if d <= 20 => 20,
        d if d <= 50 => 50,
        d if d <= 100 => 100,
        d if d <= 500 => 500,
        d if d <= 1000 => 1000,
        _ => 1000,
    }
}

fn convert_price_levels(levels: &[[String; 2]]) -> Option<Vec<(Decimal, Decimal)>> {
    let mut out = Vec::with_capacity(levels.len());
    for entry in levels {
        let price = Decimal::from_str(entry.first()?).ok()?;
        let size = Decimal::from_str(entry.get(1)?).ok()?;
        out.push((price, size));
    }
    Some(out)
}

pub struct BinanceUserDataStream {
    ws: Arc<websocket_streams::WebsocketStreams>,
    stream: Arc<WebsocketStream<UserDataStreamEventsResponse>>,
}

impl BinanceUserDataStream {
    pub async fn connect(ws_url: &str, listen_key: &str) -> BrokerResult<Self> {
        let cfg = ConfigurationWebsocketStreams::builder()
            .ws_url(ws_url.to_string())
            .build()
            .map_err(|err| BrokerError::Transport(err.to_string()))?;
        let handle = binance_futures::DerivativesTradingUsdsFuturesWsStreams::from_config(cfg);
        let ws = handle
            .connect()
            .await
            .map_err(|err| BrokerError::Transport(err.to_string()))?;
        let ws = Arc::new(ws);
        let stream = ws
            .user_data(listen_key.to_string(), None)
            .await
            .map_err(|err| BrokerError::Transport(err.to_string()))?;
        Ok(Self { ws, stream })
    }

    pub fn on_event<F>(&self, callback: F)
    where
        F: Fn(UserDataStreamEventsResponse) + Send + Sync + 'static,
    {
        self.stream.on_message(callback);
    }

    pub async fn unsubscribe(&self) {
        self.stream.unsubscribe().await;
        let _ = self.ws.disconnect().await;
    }
}

pub fn extract_order_update(event: &UserDataStreamEventsResponse) -> Option<&OrderTradeUpdateO> {
    match event {
        UserDataStreamEventsResponse::OrderTradeUpdate(payload) => payload.o.as_deref(),
        _ => None,
    }
}

fn interval_label(interval: Interval) -> String {
    interval.to_binance().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::sync::mpsc;

    fn test_task(depth: usize) -> (DiffDepthTask, mpsc::Receiver<OrderBook>) {
        let client = Client::builder()
            .build()
            .expect("failed to build http client");
        let (tx, rx) = mpsc::channel(8);
        let task = DiffDepthTask::new(
            "BTCUSDT".into(),
            depth,
            "https://example.com".into(),
            "wss://example.com".into(),
            client,
            tx,
        );
        (task, rx)
    }

    #[tokio::test]
    async fn diff_depth_task_applies_updates() {
        let (mut task, mut rx) = test_task(5);
        task.book.load_snapshot(
            &[(Decimal::from(10), Decimal::from(1))],
            &[(Decimal::from(11), Decimal::from(2))],
        );
        task.last_update_id = 1;
        let message = json!({
            "U": 2,
            "u": 2,
            "b": [["10","0"], ["9","1"]],
            "a": [["12","3"]],
            "E": 1_680_000_000_000i64
        })
        .to_string();
        task.process_message(&message).await.expect("diff applied");
        let book = rx.recv().await.expect("order book emitted");
        assert_eq!(book.symbol, "BTCUSDT");
        assert_eq!(book.bids[0].price, Decimal::from(9));
        assert_eq!(book.asks[0].price, Decimal::from(11));
    }

    #[tokio::test]
    async fn diff_depth_task_detects_sequence_gap() {
        let (mut task, mut rx) = test_task(5);
        task.book.load_snapshot(
            &[(Decimal::from(100), Decimal::from(1))],
            &[(Decimal::from(101), Decimal::from(1))],
        );
        task.last_update_id = 1;
        let ok = json!({
            "U": 2,
            "u": 2,
            "b": [["100","0"]],
            "a": [],
            "E": 1_680_000_000_100i64
        })
        .to_string();
        task.process_message(&ok).await.expect("first diff ok");
        let _ = rx.recv().await;
        let gap = json!({
            "U": 5,
            "u": 5,
            "b": [],
            "a": [],
            "E": 1_680_000_000_200i64
        })
        .to_string();
        let err = task.process_message(&gap).await;
        assert!(err.is_err());
    }

    #[test]
    fn snapshot_limit_matches_expectations() {
        assert_eq!(snapshot_limit(1), 5);
        assert_eq!(snapshot_limit(7), 10);
        assert_eq!(snapshot_limit(15), 20);
        assert_eq!(snapshot_limit(40), 50);
        assert_eq!(snapshot_limit(80), 100);
        assert_eq!(snapshot_limit(200), 500);
        assert_eq!(snapshot_limit(800), 1000);
        assert_eq!(snapshot_limit(5_000), 1000);
    }
}
