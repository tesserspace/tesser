use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};

use anyhow::Result;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tracing::warn;

use tesser_core::Side;

use crate::state::{MockExchangeState, PrivateMessage};

pub struct MockWebSocketServer {
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
}

impl MockWebSocketServer {
    pub async fn spawn(state: MockExchangeState) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        break;
                    }
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, peer)) => {
                                let state = state.clone();
                                tokio::spawn(async move {
                                    if let Err(err) = handle_socket(state, stream, peer).await {
                                        tracing::warn!(error = %err, "websocket connection ended with error");
                                    }
                                });
                            }
                            Err(err) => {
                                tracing::error!(error = %err, "failed to accept websocket connection");
                                break;
                            }
                        }
                    }
                }
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
        format!("ws://{}", self.addr)
    }

    pub async fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.handle.abort();
    }
}

impl Drop for MockWebSocketServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.handle.abort();
    }
}

async fn handle_socket(
    state: MockExchangeState,
    stream: TcpStream,
    _peer: SocketAddr,
) -> Result<()> {
    let captured_path = Arc::new(StdMutex::new(String::new()));
    let path_clone = captured_path.clone();
    let ws_stream = accept_hdr_async(stream, move |req: &Request, resp: Response| {
        if let Ok(mut path) = path_clone.lock() {
            *path = req.uri().path().to_string();
        }
        Ok(resp)
    })
    .await?;
    let path = captured_path
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_else(|_| "/".to_string());
    if path.starts_with("/v5/public/") {
        handle_public_stream(state, ws_stream, path).await
    } else if path == "/v5/private" {
        handle_private_stream(state, ws_stream).await
    } else {
        tracing::warn!(path = %path, "received websocket connection for unknown path");
        Ok(())
    }
}

async fn handle_public_stream(
    state: MockExchangeState,
    stream: WebSocketStream<TcpStream>,
    _topic_path: String,
) -> Result<()> {
    let (mut sink, mut source) = stream.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sink.send(msg).await.is_err() {
                break;
            }
        }
    });
    while let Some(msg) = source.next().await {
        match msg? {
            Message::Text(text) => {
                if let Err(err) = handle_public_command(&state, &tx, text).await {
                    warn!(error = %err, "public ws command handling failed");
                }
            }
            Message::Binary(bytes) => {
                if let Ok(text) = String::from_utf8(bytes) {
                    if let Err(err) = handle_public_command(&state, &tx, text).await {
                        warn!(error = %err, "public ws binary command failed");
                    }
                }
            }
            Message::Ping(payload) => {
                let _ = tx.send(Message::Pong(payload));
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    drop(tx);
    writer.abort();
    Ok(())
}

async fn handle_public_command(
    state: &MockExchangeState,
    tx: &mpsc::UnboundedSender<Message>,
    text: String,
) -> Result<()> {
    let value: Value = serde_json::from_str(&text)?;
    match value.get("op").and_then(|op| op.as_str()) {
        Some("ping") => {
            let _ = tx.send(Message::Text(json!({"op": "pong"}).to_string()));
        }
        Some("subscribe") => {
            if let Some(args) = value.get("args").and_then(|v| v.as_array()) {
                for topic in args.iter().filter_map(|entry| entry.as_str()) {
                    let ack = json!({
                        "success": true,
                        "conn_id": 0,
                        "topic": topic,
                        "req_id": value.get("req_id"),
                    });
                    let _ = tx.send(Message::Text(ack.to_string()));
                    spawn_public_topic_task(state.clone(), topic.to_string(), tx.clone());
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn spawn_public_topic_task(
    state: MockExchangeState,
    topic: String,
    tx: mpsc::UnboundedSender<Message>,
) {
    tokio::spawn(async move {
        match parse_public_topic(&topic) {
            Some(PublicTopic::Trades { symbol }) => {
                stream_trades(state, topic, symbol, tx).await;
            }
            Some(PublicTopic::Kline { symbol, interval }) => {
                stream_klines(state, topic, symbol, interval, tx).await;
            }
            Some(PublicTopic::OrderBook { symbol, depth }) => {
                send_orderbook_snapshot(topic, symbol, depth, tx).await;
            }
            None => warn!(topic = %topic, "unrecognized public subscription"),
        }
    });
}

enum PublicTopic {
    Trades { symbol: String },
    Kline { symbol: String, interval: String },
    OrderBook { symbol: String, depth: usize },
}

fn parse_public_topic(topic: &str) -> Option<PublicTopic> {
    if let Some(symbol) = topic.strip_prefix("publicTrade.") {
        Some(PublicTopic::Trades {
            symbol: symbol.to_string(),
        })
    } else if let Some(rest) = topic.strip_prefix("kline.") {
        let mut parts = rest.splitn(2, '.');
        let interval = parts.next()?.to_string();
        let symbol = parts.next()?.to_string();
        Some(PublicTopic::Kline { symbol, interval })
    } else if let Some(rest) = topic.strip_prefix("orderbook.") {
        let mut parts = rest.splitn(2, '.');
        let depth = parts.next()?.parse().ok()?;
        let symbol = parts.next()?.to_string();
        Some(PublicTopic::OrderBook { symbol, depth })
    } else {
        None
    }
}

async fn stream_trades(
    state: MockExchangeState,
    topic: String,
    symbol: String,
    tx: mpsc::UnboundedSender<Message>,
) {
    while let Some(tick) = state.next_tick().await {
        if tick.symbol != symbol {
            continue;
        }
        let payload = json!({
            "topic": topic,
            "_topic": topic,
            "type": "snapshot",
            "ts": tick.exchange_timestamp.timestamp_millis(),
            "data": [{
                "T": tick.exchange_timestamp.timestamp_millis(),
                "s": tick.symbol,
                "S": match tick.side { Side::Buy => "Buy", Side::Sell => "Sell" },
                "v": decimal_to_string(tick.size),
                "p": decimal_to_string(tick.price),
            }]
        });
        if tx.send(Message::Text(payload.to_string())).is_err() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn stream_klines(
    state: MockExchangeState,
    topic: String,
    symbol: String,
    interval: String,
    tx: mpsc::UnboundedSender<Message>,
) {
    while let Some(candle) = state.next_candle().await {
        if candle.symbol != symbol {
            continue;
        }
        let payload = json!({
            "topic": topic,
            "_topic": topic,
            "type": "snapshot",
            "ts": candle.timestamp.timestamp_millis(),
            "data": [{
                "_start": candle.timestamp.timestamp_millis(),
                "_end": candle.timestamp.timestamp_millis(),
                "interval": interval,
                "open": decimal_to_string(candle.open),
                "high": decimal_to_string(candle.high),
                "low": decimal_to_string(candle.low),
                "close": decimal_to_string(candle.close),
                "volume": decimal_to_string(candle.volume),
                "confirm": true,
                "timestamp": candle.timestamp.timestamp_millis(),
            }]
        });
        if tx.send(Message::Text(payload.to_string())).is_err() {
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn send_orderbook_snapshot(
    topic: String,
    symbol: String,
    depth: usize,
    tx: mpsc::UnboundedSender<Message>,
) {
    let bids = vec![["9999".to_string(), "1".to_string()]];
    let asks = vec![["10001".to_string(), "1".to_string()]];
    let payload = json!({
        "topic": topic,
        "_topic": topic,
        "type": "snapshot",
        "ts": Utc::now().timestamp_millis(),
        "data": {
            "s": symbol,
            "b": bids,
            "a": asks,
            "u": depth as i64,
        }
    });
    let _ = tx.send(Message::Text(payload.to_string()));
}

fn decimal_to_string(value: impl Into<rust_decimal::Decimal>) -> String {
    let decimal: rust_decimal::Decimal = value.into();
    decimal.normalize().to_string()
}

async fn handle_private_stream(
    state: MockExchangeState,
    stream: WebSocketStream<TcpStream>,
) -> Result<()> {
    let (mut sink, mut source) = stream.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<PrivateMessage>();
    state.set_private_ws_sender(tx.clone()).await;
    let forward = tokio::spawn(async move {
        while let Some(payload) = rx.recv().await {
            if sink.send(Message::Text(payload.to_string())).await.is_err() {
                break;
            }
        }
    });
    while let Some(msg) = source.next().await {
        match msg? {
            Message::Text(text) => {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                    match value.get("op").and_then(|v| v.as_str()) {
                        Some("ping") => {
                            let _ = tx.send(json!({"op": "pong"}));
                        }
                        Some("auth") => {
                            let _ = tx.send(json!({"op":"auth","success":true}));
                        }
                        _ => {}
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    state.clear_private_ws_sender().await;
    forward.abort();
    Ok(())
}
