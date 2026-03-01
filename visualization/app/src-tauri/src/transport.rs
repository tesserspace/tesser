use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tauri::State;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex};
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{info, warn};
use uuid::Uuid;

use crate::limits;
use crate::stream_ref::{StreamRef, StreamTransport};

#[derive(Debug, thiserror::Error)]
enum TransportError {
    #[error("transport not started")]
    NotStarted,
    #[error("unauthorized")]
    Unauthorized,
}

#[derive(Debug, Clone)]
pub enum TransportErrorPublic {
    Unavailable,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransportInfo {
    pub ws_url: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DemoStreamRef {
    pub stream_id: String,
    pub ws_url: String,
    pub auth_token: String,
    pub token_in: String,
    pub expires_at_ms: u64,
}

pub struct TransportState {
    inner: Mutex<TransportInner>,
}

struct TransportInner {
    ws_url: Option<String>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    registry: Arc<std::sync::Mutex<StreamRegistry>>,
    active_connections: Arc<AtomicUsize>,
    server_running: Arc<AtomicBool>,
}

impl Default for TransportState {
    fn default() -> Self {
        Self {
            inner: Mutex::new(TransportInner {
                ws_url: None,
                shutdown_tx: None,
                registry: Arc::new(std::sync::Mutex::new(StreamRegistry::default())),
                active_connections: Arc::new(AtomicUsize::new(0)),
                server_running: Arc::new(AtomicBool::new(false)),
            }),
        }
    }
}

impl TransportState {
    async fn start(&self) -> Result<TransportInfo, TransportError> {
        let ws_url = ensure_transport_started(self).await?;
        Ok(TransportInfo { ws_url })
    }

    pub async fn open_pending_arrow_stream(
        &self,
        chunks: Vec<Vec<u8>>,
        schema_id: Option<String>,
    ) -> Result<StreamRef, TransportErrorPublic> {
        let ws_url = ensure_transport_started(self)
            .await
            .map_err(|_| TransportErrorPublic::Unavailable)?;

        let stream_id = Uuid::new_v4();
        let auth_token = new_auth_token_hex();
        let expires_at_ms = now_ms() + 60_000;

        let stream = DemoStream {
            stream_id,
            expected_next_seq: 1,
            chunks,
            terminal_sent: false,
        };

        let inner = self.inner.lock().await;
        {
            let mut reg = inner
                .registry
                .lock()
                .map_err(|_| TransportErrorPublic::Unavailable)?;
            reg.pending_by_token.insert(
                auth_token.clone(),
                PendingStream {
                    stream,
                    expires_at_ms,
                },
            );
        }

        Ok(StreamRef {
            stream_id: stream_id.to_string(),
            format: "arrow_ipc_stream".to_string(),
            transport: StreamTransport::LoopbackWs {
                url: ws_url,
                auth_token,
                token_in: "sec-websocket-protocol".to_string(),
                expires_at_ms,
            },
            schema_id,
        })
    }

    async fn open_demo_stream(&self) -> Result<DemoStreamRef, TransportError> {
        let ws_url = ensure_transport_started(self).await?;

        let stream_id = Uuid::new_v4();
        let auth_token = new_auth_token_hex();

        let demo = DemoStream {
            stream_id,
            expected_next_seq: 1,
            chunks: vec![
                b"DEMO:chunk-1\n".to_vec(),
                b"DEMO:chunk-2\n".to_vec(),
                b"DEMO:chunk-3\n".to_vec(),
            ],
            terminal_sent: false,
        };

        let expires_at_ms = now_ms() + 60_000;

        let inner = self.inner.lock().await;
        {
            let mut reg = inner
                .registry
                .lock()
                .map_err(|_| TransportError::Unauthorized)?;
            reg.pending_by_token.insert(
                auth_token.clone(),
                PendingStream {
                    stream: demo,
                    expires_at_ms,
                },
            );
        }

        Ok(DemoStreamRef {
            stream_id: stream_id.to_string(),
            ws_url,
            auth_token,
            token_in: "sec-websocket-protocol".to_string(),
            expires_at_ms,
        })
    }

    #[cfg(test)]
    async fn shutdown(&self) {
        let mut inner = self.inner.lock().await;
        if let Some(tx) = inner.shutdown_tx.take() {
            let _ = tx.send(());
        }
        inner.ws_url = None;
    }
}

#[derive(Default)]
struct StreamRegistry {
    pending_by_token: HashMap<String, PendingStream>,
}

struct PendingStream {
    stream: DemoStream,
    expires_at_ms: u64,
}

struct DemoStream {
    stream_id: Uuid,
    expected_next_seq: u64,
    chunks: Vec<Vec<u8>>,
    terminal_sent: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum ClientMessage {
    #[serde(rename = "streams.pull")]
    Pull {
        stream_id: String,
        next_seq: u64,
        max_bytes: u32,
        correlation_id: Option<String>,
        request_id: Option<String>,
    },
    #[serde(rename = "streams.cancel")]
    Cancel {
        stream_id: String,
        correlation_id: Option<String>,
        request_id: Option<String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum ServerMessage {
    #[serde(rename = "streams.error")]
    Error {
        stream_id: String,
        code: String,
        message: String,
        terminal: bool,
        seq: u64,
        correlation_id: Option<String>,
        request_id: Option<String>,
    },
    #[serde(rename = "streams.closed")]
    Closed {
        stream_id: String,
        reason_code: String,
        error_code: Option<String>,
        seq: u64,
        correlation_id: Option<String>,
        request_id: Option<String>,
    },
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as u64
}

fn new_auth_token_hex() -> String {
    let mut token_bytes = [0u8; 32];
    token_bytes[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    token_bytes[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    hex::encode(token_bytes)
}

fn build_binary_frame(kind: u8, seq: u64, stream_id: Uuid, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32 + payload.len());
    buf.extend_from_slice(b"TSR1");
    buf.push(kind);
    buf.push(0);
    buf.extend_from_slice(&(32u16.to_be_bytes()));
    buf.extend_from_slice(&seq.to_be_bytes());
    buf.extend_from_slice(stream_id.as_bytes());
    buf.extend_from_slice(payload);
    buf
}

fn parse_subprotocols(header_value: &str) -> Vec<String> {
    header_value
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

async fn ensure_transport_started(state: &TransportState) -> Result<String, TransportError> {
    let mut inner = state.inner.lock().await;
    if let Some(url) = inner.ws_url.clone() {
        if inner.server_running.load(Ordering::SeqCst) {
            return Ok(url);
        }
        inner.ws_url = None;
        inner.shutdown_tx = None;
    }

    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|_| TransportError::NotStarted)?;
    let local_addr = listener
        .local_addr()
        .map_err(|_| TransportError::NotStarted)?;
    let ws_url = format!("ws://{}", local_addr);
    let ws_url_for_task = ws_url.clone();

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let registry = inner.registry.clone();
    let active_connections = inner.active_connections.clone();
    let server_running = inner.server_running.clone();

    server_running.store(true, Ordering::SeqCst);

    tokio::spawn(async move {
        info!(ws_url=%ws_url_for_task, "loopback_ws transport started");
        let mut tick = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_millis(500),
            Duration::from_millis(500),
        );
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("loopback_ws transport shutdown");
                    break;
                }
                _ = tick.tick() => {
                    let now = now_ms();
                    if let Ok(mut reg) = registry.lock() {
                        reg.pending_by_token.retain(|_, pending| pending.expires_at_ms > now);
                        if reg.pending_by_token.is_empty()
                            && active_connections.load(Ordering::SeqCst) == 0
                        {
                            info!("loopback_ws transport auto-shutdown (no active streams)");
                            break;
                        }
                    }
                }
                accept_res = listener.accept() => {
                    let (tcp_stream, peer) = match accept_res {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(error=%e, "accept failed");
                            continue;
                        }
                    };

                    let registry = registry.clone();
                    let active_connections = active_connections.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_ws_connection(registry, active_connections, tcp_stream).await {
                            warn!(peer=%peer, error=%e, "ws connection ended");
                        }
                    });
                }
            }
        }
        server_running.store(false, Ordering::SeqCst);
    });

    inner.ws_url = Some(ws_url.clone());
    inner.shutdown_tx = Some(shutdown_tx);
    Ok(ws_url)
}

async fn handle_ws_connection(
    registry: Arc<std::sync::Mutex<StreamRegistry>>,
    active_connections: Arc<AtomicUsize>,
    tcp_stream: tokio::net::TcpStream,
) -> Result<(), TransportError> {
    let selected_token: Arc<std::sync::Mutex<Option<String>>> =
        Arc::new(std::sync::Mutex::new(None));
    let selected_token_cb = selected_token.clone();
    let selected_stream: Arc<std::sync::Mutex<Option<DemoStream>>> =
        Arc::new(std::sync::Mutex::new(None));
    let selected_stream_cb = selected_stream.clone();
    let reserved_slot = Arc::new(AtomicBool::new(false));
    let reserved_slot_cb = reserved_slot.clone();
    let registry_cb = registry.clone();
    let active_cb = active_connections.clone();

    use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
    use tokio_tungstenite::tungstenite::http::Response as HttpResponse;
    use tokio_tungstenite::tungstenite::http::StatusCode;
    use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

    fn error_response(status: StatusCode) -> ErrorResponse {
        HttpResponse::builder()
            .status(status)
            .body(None)
            .unwrap_or_else(|_| {
                HttpResponse::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(None)
                    .unwrap()
            })
    }

    let ws_config = WebSocketConfig {
        max_message_size: Some(64 * 1024),
        max_frame_size: Some(64 * 1024),
        ..WebSocketConfig::default()
    };

    let ws_stream = tokio_tungstenite::accept_hdr_async_with_config(
        tcp_stream,
        move |req: &Request, mut resp: Response| {
            let Some(header_val) = req.headers().get("sec-websocket-protocol") else {
                return Err(error_response(StatusCode::UNAUTHORIZED));
            };
            let Ok(header_str) = header_val.to_str() else {
                return Err(error_response(StatusCode::UNAUTHORIZED));
            };
            let candidates = parse_subprotocols(header_str);

            let now = now_ms();
            let mut registry_guard = registry_cb
                .lock()
                .map_err(|_| error_response(StatusCode::UNAUTHORIZED))?;
            registry_guard
                .pending_by_token
                .retain(|_, pending| pending.expires_at_ms > now);

            for token in candidates {
                let Some(pending) = registry_guard.pending_by_token.remove(&token) else {
                    continue;
                };
                if pending.expires_at_ms <= now {
                    continue;
                }

                let prev = active_cb.fetch_add(1, Ordering::SeqCst);
                if prev >= limits::MAX_ACTIVE_STREAMS {
                    active_cb.fetch_sub(1, Ordering::SeqCst);
                    registry_guard.pending_by_token.insert(token, pending);
                    return Err(error_response(StatusCode::TOO_MANY_REQUESTS));
                }
                reserved_slot_cb.store(true, Ordering::SeqCst);

                resp.headers_mut().insert(
                    "sec-websocket-protocol",
                    tokio_tungstenite::tungstenite::http::HeaderValue::from_str(&token)
                        .map_err(|_| error_response(StatusCode::BAD_REQUEST))?,
                );
                *selected_token_cb
                    .lock()
                    .map_err(|_| error_response(StatusCode::UNAUTHORIZED))? = Some(token);
                *selected_stream_cb
                    .lock()
                    .map_err(|_| error_response(StatusCode::UNAUTHORIZED))? = Some(pending.stream);
                return Ok(resp);
            }

            Err(error_response(StatusCode::UNAUTHORIZED))
        },
        Some(ws_config),
    )
    .await
    .map_err(|_| {
        if reserved_slot.load(Ordering::SeqCst) {
            active_connections.fetch_sub(1, Ordering::SeqCst);
        }
        TransportError::Unauthorized
    })?;

    struct ActiveConnGuard {
        active: Arc<AtomicUsize>,
    }
    impl Drop for ActiveConnGuard {
        fn drop(&mut self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let _conn_guard = ActiveConnGuard {
        active: active_connections.clone(),
    };

    let _token = selected_token
        .lock()
        .map_err(|_| TransportError::Unauthorized)?
        .take()
        .ok_or(TransportError::Unauthorized)?;
    let mut stream = selected_stream
        .lock()
        .map_err(|_| TransportError::Unauthorized)?
        .take()
        .ok_or(TransportError::Unauthorized)?;

    let (mut write, mut read) = ws_stream.split();
    let mut bad_msg_count: u32 = 0;

    loop {
        let msg = match tokio::time::timeout(
            Duration::from_millis(limits::STREAM_IDLE_TIMEOUT_MS),
            read.next(),
        )
        .await
        {
            Ok(v) => v,
            Err(_) => {
                if stream.terminal_sent {
                    break;
                }
                let closed = ServerMessage::Closed {
                    stream_id: stream.stream_id.to_string(),
                    reason_code: "idle_timeout".to_string(),
                    error_code: None,
                    seq: stream.expected_next_seq,
                    correlation_id: None,
                    request_id: None,
                };
                if let Ok(text) = serde_json::to_string(&closed) {
                    let _ = write.send(Message::Text(text)).await;
                }
                stream.terminal_sent = true;
                break;
            }
        };

        let Some(msg) = msg else {
            break;
        };
        let msg = msg.map_err(|_| TransportError::Unauthorized)?;

        match msg {
            Message::Text(text) => {
                let Ok(client_msg) = serde_json::from_str::<ClientMessage>(&text) else {
                    bad_msg_count = bad_msg_count.saturating_add(1);
                    if bad_msg_count >= 8 {
                        let closed = ServerMessage::Closed {
                            stream_id: stream.stream_id.to_string(),
                            reason_code: "error".to_string(),
                            error_code: Some("STREAM.BAD_REQUEST".to_string()),
                            seq: stream.expected_next_seq,
                            correlation_id: None,
                            request_id: None,
                        };
                        if let Ok(text) = serde_json::to_string(&closed) {
                            let _ = write.send(Message::Text(text)).await;
                        }
                        stream.terminal_sent = true;
                        break;
                    }
                    continue;
                };
                match client_msg {
                    ClientMessage::Pull {
                        stream_id,
                        next_seq,
                        max_bytes,
                        correlation_id,
                        request_id,
                    } => {
                        if stream.terminal_sent {
                            continue;
                        }

                        if stream_id != stream.stream_id.to_string() {
                            let err = ServerMessage::Error {
                                stream_id: stream.stream_id.to_string(),
                                code: "STREAM.STREAM_ID_MISMATCH".to_string(),
                                message: "stream_id does not match connection".to_string(),
                                terminal: true,
                                seq: next_seq,
                                correlation_id: correlation_id.clone(),
                                request_id: request_id.clone(),
                            };
                            if let Ok(text) = serde_json::to_string(&err) {
                                let _ = write.send(Message::Text(text)).await;
                            }
                            let closed = ServerMessage::Closed {
                                stream_id: stream.stream_id.to_string(),
                                reason_code: "error".to_string(),
                                error_code: Some("STREAM.STREAM_ID_MISMATCH".to_string()),
                                seq: stream.expected_next_seq,
                                correlation_id,
                                request_id,
                            };
                            if let Ok(text) = serde_json::to_string(&closed) {
                                let _ = write.send(Message::Text(text)).await;
                            }
                            stream.terminal_sent = true;
                            break;
                        }

                        if next_seq != stream.expected_next_seq {
                            let err = ServerMessage::Error {
                                stream_id: stream.stream_id.to_string(),
                                code: "STREAM.SEQ_INVALID".to_string(),
                                message: "next_seq must equal expected_next_seq".to_string(),
                                terminal: false,
                                seq: next_seq,
                                correlation_id,
                                request_id,
                            };
                            if let Ok(text) = serde_json::to_string(&err) {
                                let _ = write.send(Message::Text(text)).await;
                            }
                            continue;
                        }

                        if max_bytes == 0 {
                            let err = ServerMessage::Error {
                                stream_id: stream.stream_id.to_string(),
                                code: "STREAM.MAX_BYTES_INVALID".to_string(),
                                message: "max_bytes must be > 0".to_string(),
                                terminal: false,
                                seq: next_seq,
                                correlation_id,
                                request_id,
                            };
                            if let Ok(text) = serde_json::to_string(&err) {
                                let _ = write.send(Message::Text(text)).await;
                            }
                            continue;
                        }

                        let next_index = (stream.expected_next_seq - 1) as usize;
                        if next_index < stream.chunks.len() {
                            let payload = &stream.chunks[next_index];
                            let effective_max = max_bytes.min(limits::MAX_BYTES_PER_PULL);
                            if payload.len() > effective_max as usize {
                                let err = ServerMessage::Error {
                                    stream_id: stream.stream_id.to_string(),
                                    code: "STREAM.MAX_BYTES_TOO_SMALL".to_string(),
                                    message: "max_bytes too small for one message".to_string(),
                                    terminal: false,
                                    seq: next_seq,
                                    correlation_id,
                                    request_id,
                                };
                                if let Ok(text) = serde_json::to_string(&err) {
                                    let _ = write.send(Message::Text(text)).await;
                                }
                                continue;
                            }

                            let frame = build_binary_frame(
                                1,
                                stream.expected_next_seq,
                                stream.stream_id,
                                payload,
                            );
                            let _ = write.send(Message::Binary(frame)).await;
                            stream.expected_next_seq += 1;
                        } else {
                            let eof_frame = build_binary_frame(
                                2,
                                stream.expected_next_seq,
                                stream.stream_id,
                                &[],
                            );
                            let _ = write.send(Message::Binary(eof_frame)).await;
                            let closed = ServerMessage::Closed {
                                stream_id: stream.stream_id.to_string(),
                                reason_code: "eof".to_string(),
                                error_code: None,
                                seq: stream.expected_next_seq,
                                correlation_id,
                                request_id,
                            };
                            if let Ok(text) = serde_json::to_string(&closed) {
                                let _ = write.send(Message::Text(text)).await;
                            }
                            stream.terminal_sent = true;
                            break;
                        }
                    }
                    ClientMessage::Cancel {
                        stream_id,
                        correlation_id,
                        request_id,
                    } => {
                        if stream_id != stream.stream_id.to_string() {
                            continue;
                        }
                        if stream.terminal_sent {
                            break;
                        }
                        let closed = ServerMessage::Closed {
                            stream_id: stream.stream_id.to_string(),
                            reason_code: "canceled".to_string(),
                            error_code: None,
                            seq: stream.expected_next_seq,
                            correlation_id,
                            request_id,
                        };
                        if let Ok(text) = serde_json::to_string(&closed) {
                            let _ = write.send(Message::Text(text)).await;
                        }
                        stream.terminal_sent = true;
                        break;
                    }
                }
            }
            Message::Binary(_) => {
                bad_msg_count = bad_msg_count.saturating_add(1);
                if bad_msg_count >= 8 {
                    let closed = ServerMessage::Closed {
                        stream_id: stream.stream_id.to_string(),
                        reason_code: "error".to_string(),
                        error_code: Some("STREAM.BAD_REQUEST".to_string()),
                        seq: stream.expected_next_seq,
                        correlation_id: None,
                        request_id: None,
                    };
                    if let Ok(text) = serde_json::to_string(&closed) {
                        let _ = write.send(Message::Text(text)).await;
                    }
                    stream.terminal_sent = true;
                    break;
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    Ok(())
}

#[tauri::command]
pub async fn transport_start(state: State<'_, TransportState>) -> Result<TransportInfo, String> {
    state.start().await.map_err(|e| e.to_string())
}

#[cfg(debug_assertions)]
#[tauri::command]
pub async fn debug_open_demo_stream(
    state: State<'_, TransportState>,
) -> Result<DemoStreamRef, String> {
    state.open_demo_stream().await.map_err(|e| e.to_string())
}

#[cfg(not(debug_assertions))]
#[tauri::command]
pub async fn debug_open_demo_stream(
    _state: State<'_, TransportState>,
) -> Result<DemoStreamRef, String> {
    Err("debug_open_demo_stream is disabled in release builds".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::HeaderValue;

    type Ws = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    fn parse_frame(frame: &[u8]) -> (u8, u64, Uuid, Vec<u8>) {
        assert!(frame.len() >= 32);
        assert_eq!(&frame[0..4], b"TSR1");
        let kind = frame[4];
        let header_len = u16::from_be_bytes([frame[6], frame[7]]);
        assert_eq!(header_len, 32);
        let seq = u64::from_be_bytes(frame[8..16].try_into().expect("seq bytes"));
        let stream_id = Uuid::from_slice(&frame[16..32]).expect("uuid");
        let payload = frame[32..].to_vec();
        (kind, seq, stream_id, payload)
    }

    async fn connect_with_token(ws_url: &str, token: &str) -> Ws {
        connect_with_token_result(ws_url, token)
            .await
            .expect("ws connect")
    }

    async fn connect_with_token_result(ws_url: &str, token: &str) -> Result<Ws, String> {
        let mut req = ws_url.into_client_request().expect("client request");
        req.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(token).expect("header value"),
        );
        let (ws, _) = tokio_tungstenite::connect_async(req)
            .await
            .map_err(|e| e.to_string())?;
        Ok(ws)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn demo_stream_happy_path_chunks_then_eof_then_closed() {
        let state = TransportState::default();
        let transport = state.start().await.expect("transport start");
        let demo = state.open_demo_stream().await.expect("demo stream");

        assert_eq!(demo.ws_url, transport.ws_url);
        let expected_stream_id = Uuid::parse_str(&demo.stream_id).expect("uuid");

        let mut ws = connect_with_token(&demo.ws_url, &demo.auth_token).await;

        for (i, expected_payload) in ["DEMO:chunk-1\n", "DEMO:chunk-2\n", "DEMO:chunk-3\n"]
            .into_iter()
            .enumerate()
        {
            let next_seq = (i as u64) + 1;
            ws.send(Message::Text(
                serde_json::to_string(&ClientMessage::Pull {
                    stream_id: demo.stream_id.clone(),
                    next_seq,
                    max_bytes: 256 * 1024,
                    correlation_id: Some("test".to_string()),
                    request_id: Some(Uuid::new_v4().to_string()),
                })
                .expect("json"),
            ))
            .await
            .expect("send pull");

            let msg = timeout(Duration::from_secs(2), ws.next())
                .await
                .expect("timeout")
                .expect("stream ended")
                .expect("ws msg");
            let Message::Binary(frame) = msg else {
                panic!("expected binary frame");
            };
            let (kind, seq, stream_id, payload) = parse_frame(&frame);
            assert_eq!(kind, 1);
            assert_eq!(seq, next_seq);
            assert_eq!(stream_id, expected_stream_id);
            assert_eq!(payload, expected_payload.as_bytes());
        }

        ws.send(Message::Text(
            serde_json::to_string(&ClientMessage::Pull {
                stream_id: demo.stream_id.clone(),
                next_seq: 4,
                max_bytes: 256 * 1024,
                correlation_id: Some("test".to_string()),
                request_id: Some(Uuid::new_v4().to_string()),
            })
            .expect("json"),
        ))
        .await
        .expect("send pull");

        let msg = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("ws msg");
        let Message::Binary(frame) = msg else {
            panic!("expected binary eof frame");
        };
        let (kind, seq, stream_id, payload) = parse_frame(&frame);
        assert_eq!(kind, 2);
        assert_eq!(seq, 4);
        assert_eq!(stream_id, expected_stream_id);
        assert!(payload.is_empty());

        let msg = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("ws msg");
        let Message::Text(text) = msg else {
            panic!("expected closed message");
        };
        let closed: ServerMessage = serde_json::from_str(&text).expect("closed json");
        match closed {
            ServerMessage::Closed {
                stream_id,
                reason_code,
                error_code,
                seq,
                ..
            } => {
                assert_eq!(stream_id, demo.stream_id);
                assert_eq!(reason_code, "eof");
                assert_eq!(error_code, None);
                assert_eq!(seq, 4);
            }
            _ => panic!("expected streams.closed"),
        }

        state.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn demo_stream_cancel_yields_closed_canceled() {
        let state = TransportState::default();
        let demo = state.open_demo_stream().await.expect("demo stream");
        let mut ws = connect_with_token(&demo.ws_url, &demo.auth_token).await;

        ws.send(Message::Text(
            serde_json::to_string(&ClientMessage::Cancel {
                stream_id: demo.stream_id.clone(),
                correlation_id: Some("test".to_string()),
                request_id: Some(Uuid::new_v4().to_string()),
            })
            .expect("json"),
        ))
        .await
        .expect("send cancel");

        let msg = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("ws msg");
        let Message::Text(text) = msg else {
            panic!("expected closed message");
        };
        let closed: ServerMessage = serde_json::from_str(&text).expect("closed json");
        match closed {
            ServerMessage::Closed {
                stream_id,
                reason_code,
                ..
            } => {
                assert_eq!(stream_id, demo.stream_id);
                assert_eq!(reason_code, "canceled");
            }
            _ => panic!("expected streams.closed"),
        }

        state.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wrong_seq_returns_nonterminal_error_and_allows_retry() {
        let state = TransportState::default();
        let demo = state.open_demo_stream().await.expect("demo stream");
        let mut ws = connect_with_token(&demo.ws_url, &demo.auth_token).await;

        ws.send(Message::Text(
            serde_json::to_string(&ClientMessage::Pull {
                stream_id: demo.stream_id.clone(),
                next_seq: 2,
                max_bytes: 256 * 1024,
                correlation_id: Some("test".to_string()),
                request_id: Some(Uuid::new_v4().to_string()),
            })
            .expect("json"),
        ))
        .await
        .expect("send pull");

        let msg = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("ws msg");
        let Message::Text(text) = msg else {
            panic!("expected streams.error");
        };
        let err: ServerMessage = serde_json::from_str(&text).expect("error json");
        match err {
            ServerMessage::Error {
                stream_id,
                code,
                terminal,
                seq,
                ..
            } => {
                assert_eq!(stream_id, demo.stream_id);
                assert_eq!(code, "STREAM.SEQ_INVALID");
                assert!(!terminal);
                assert_eq!(seq, 2);
            }
            _ => panic!("expected streams.error"),
        }

        ws.send(Message::Text(
            serde_json::to_string(&ClientMessage::Pull {
                stream_id: demo.stream_id.clone(),
                next_seq: 1,
                max_bytes: 256 * 1024,
                correlation_id: Some("test".to_string()),
                request_id: Some(Uuid::new_v4().to_string()),
            })
            .expect("json"),
        ))
        .await
        .expect("send pull");

        let msg = timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("ws msg");
        let Message::Binary(frame) = msg else {
            panic!("expected binary frame");
        };
        let (kind, seq, stream_id, payload) = parse_frame(&frame);
        assert_eq!(kind, 1);
        assert_eq!(seq, 1);
        assert_eq!(stream_id, Uuid::parse_str(&demo.stream_id).expect("uuid"));
        assert_eq!(payload, b"DEMO:chunk-1\n");

        state.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_token_is_rejected() {
        let state = TransportState::default();
        let transport = state.start().await.expect("transport start");

        let mut req = transport
            .ws_url
            .into_client_request()
            .expect("client request");
        req.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static("not-a-real-token"),
        );
        let res = tokio_tungstenite::connect_async(req).await;
        assert!(res.is_err());

        state.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn token_is_one_time() {
        let state = TransportState::default();
        let demo = state.open_demo_stream().await.expect("demo stream");

        let _ws = connect_with_token(&demo.ws_url, &demo.auth_token).await;
        let second = connect_with_token_result(&demo.ws_url, &demo.auth_token).await;
        assert!(second.is_err());

        state.shutdown().await;
    }
}
