use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
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
        PartialBookDepthStreamsParams, PartialBookDepthStreamsResponse,
    },
};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Serialize;
use tesser_broker::{BrokerError, BrokerInfo, BrokerResult, MarketStream};
use tesser_core::{Candle, Interval, OrderBook, OrderBookLevel, Side, Tick};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, Mutex};

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
    tick_tx: mpsc::Sender<Tick>,
    candle_tx: mpsc::Sender<Candle>,
    book_tx: mpsc::Sender<OrderBook>,
    tick_rx: Mutex<mpsc::Receiver<Tick>>,
    candle_rx: Mutex<mpsc::Receiver<Candle>>,
    book_rx: Mutex<mpsc::Receiver<OrderBook>>,
    handles: Vec<StreamHandle>,
    _event_subscription: Option<Subscription>,
}

#[allow(dead_code)]
enum StreamHandle {
    Trade(Arc<WebsocketStream<AggregateTradeStreamsResponse>>),
    Kline(Arc<WebsocketStream<KlineCandlestickStreamsResponse>>),
    Book(Arc<WebsocketStream<PartialBookDepthStreamsResponse>>),
}

impl BinanceMarketStream {
    pub async fn connect(
        ws_url: &str,
        connection_status: Option<Arc<AtomicBool>>,
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
        Ok(Self {
            info: BrokerInfo {
                name: "binance-market".into(),
                markets: vec!["usd_perp".into()],
                supports_testnet: ws_url.contains("testnet"),
            },
            ws,
            tick_tx,
            candle_tx,
            book_tx,
            tick_rx: Mutex::new(tick_rx),
            candle_rx: Mutex::new(candle_rx),
            book_rx: Mutex::new(book_rx),
            handles: Vec::new(),
            _event_subscription: event_subscription,
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
        stream.on_message(move |payload: AggregateTradeStreamsResponse| {
            if let Some(tick) = convert_trade(&payload) {
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
        stream.on_message(move |payload: KlineCandlestickStreamsResponse| {
            if let Some(candle) = convert_kline(&payload) {
                let _ = tx.try_send(candle);
            }
        });
        self.handles.push(StreamHandle::Kline(stream));
        Ok(())
    }

    async fn subscribe_order_book(&mut self, symbol: String, depth: usize) -> BrokerResult<()> {
        let clamped_depth = match depth {
            d if d <= 5 => 5,
            d if d <= 10 => 10,
            _ => 20,
        } as i64;
        let params = PartialBookDepthStreamsParams::builder(symbol.to_lowercase(), clamped_depth)
            .update_speed(Some("100ms".to_string()))
            .build()
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let stream = self
            .ws
            .partial_book_depth_streams(params)
            .await
            .map_err(|err| BrokerError::Transport(err.to_string()))?;
        let tx = self.book_tx.clone();
        stream.on_message(move |payload: PartialBookDepthStreamsResponse| {
            if let Some(book) = convert_book(&payload) {
                let _ = tx.try_send(book);
            }
        });
        self.handles.push(StreamHandle::Book(stream));
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

fn convert_trade(payload: &AggregateTradeStreamsResponse) -> Option<Tick> {
    let symbol = payload.s.clone()?;
    let price = parse_decimal_opt(payload.p.as_deref())?;
    let quantity = parse_decimal_opt(payload.q.as_deref())?;
    let side = match payload.m.unwrap_or(false) {
        true => Side::Sell,
        false => Side::Buy,
    };
    Some(Tick {
        symbol,
        price,
        size: quantity,
        side,
        exchange_timestamp: timestamp_from_ms(payload.t_uppercase),
        received_at: Utc::now(),
    })
}

fn convert_kline(payload: &KlineCandlestickStreamsResponse) -> Option<Candle> {
    let kline = payload.k.as_ref()?;
    let symbol = kline.s.clone()?;
    let interval = Interval::from_str(kline.i.as_deref().unwrap_or("1m")).ok()?;
    let open = parse_decimal_opt(kline.o.as_deref())?;
    let high = parse_decimal_opt(kline.h.as_deref())?;
    let low = parse_decimal_opt(kline.l.as_deref())?;
    let close = parse_decimal_opt(kline.c.as_deref())?;
    let volume = parse_decimal_opt(kline.v.as_deref()).unwrap_or_else(|| Decimal::ZERO);
    Some(Candle {
        symbol,
        interval,
        open,
        high,
        low,
        close,
        volume,
        timestamp: timestamp_from_ms(kline.t),
    })
}

fn convert_book(payload: &PartialBookDepthStreamsResponse) -> Option<OrderBook> {
    let symbol = payload.s.clone()?;
    let bids = parse_levels(payload.b.as_ref()?);
    let asks = parse_levels(payload.a.as_ref()?);
    Some(OrderBook {
        symbol,
        bids,
        asks,
        timestamp: timestamp_from_ms(payload.t_uppercase),
    })
}

fn parse_levels(levels: &Vec<Vec<String>>) -> Vec<OrderBookLevel> {
    levels
        .iter()
        .filter_map(|level| {
            let price = level.get(0)?.parse::<Decimal>().ok()?;
            let size = level.get(1)?.parse::<Decimal>().ok()?;
            Some(OrderBookLevel { price, size })
        })
        .collect()
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

pub fn extract_order_update<'a>(
    event: &'a UserDataStreamEventsResponse,
) -> Option<&'a OrderTradeUpdateO> {
    match event {
        UserDataStreamEventsResponse::OrderTradeUpdate(payload) => payload.o.as_deref(),
        _ => None,
    }
}

fn interval_label(interval: Interval) -> String {
    interval.to_binance().to_string()
}
