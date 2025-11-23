use anyhow::Error;
use async_trait::async_trait;
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tesser_core::{Candle, Fill, OrderBook, Signal, Symbol, Tick};
use tesser_strategy::{
    register_strategy, Strategy, StrategyContext, StrategyError, StrategyResult,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{error, info, warn};
use url::Url;

use crate::client::RemoteStrategyClient;
use crate::proto::{CandleRequest, FillRequest, InitRequest, OrderBookRequest, TickRequest};
use crate::transport::grpc::GrpcAdapter;

#[derive(Clone, Deserialize, Debug)]
#[serde(tag = "transport")]
enum TransportConfig {
    #[serde(rename = "grpc")]
    Grpc {
        endpoint: String,
        #[serde(default = "default_timeout_ms")]
        timeout_ms: u64,
    },
    // Future expansion: ZMQ, SHM, etc.
}

fn default_timeout_ms() -> u64 {
    500
}

fn default_heartbeat_ms() -> u64 {
    5_000
}

#[derive(Clone, Deserialize, Debug)]
struct RpcStrategyConfig {
    #[serde(flatten)]
    transport: TransportConfig,
    #[serde(default)]
    symbols: Vec<String>,
    #[serde(default = "default_heartbeat_ms")]
    heartbeat_interval_ms: u64,
}

type SharedClient = Arc<AsyncMutex<Box<dyn RemoteStrategyClient>>>;

const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(5_000);
const MAX_HEARTBEAT_FAILURES: u32 = 3;

/// A strategy adapter that delegates decision making to an external service via a pluggable transport.
pub struct RpcStrategy {
    client: Option<SharedClient>,
    config: Option<RpcStrategyConfig>,
    config_payload: String,
    subscriptions: Vec<Symbol>,
    pending_signals: Vec<Signal>,
    symbol: Symbol, // Primary symbol fallback
    health: Arc<AtomicBool>,
    heartbeat_handle: Option<JoinHandle<()>>,
    heartbeat_interval: Duration,
    max_heartbeat_failures: u32,
}

impl Default for RpcStrategy {
    fn default() -> Self {
        Self {
            client: None,
            config: None,
            config_payload: "{}".to_string(),
            subscriptions: vec![],
            pending_signals: vec![],
            symbol: Symbol::from("UNKNOWN"),
            health: Arc::new(AtomicBool::new(true)),
            heartbeat_handle: None,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            max_heartbeat_failures: MAX_HEARTBEAT_FAILURES,
        }
    }
}

impl RpcStrategy {
    fn build_client(config: &TransportConfig) -> Box<dyn RemoteStrategyClient> {
        match config {
            TransportConfig::Grpc {
                endpoint,
                timeout_ms,
            } => {
                info!(target: "rpc", endpoint, "configured gRPC transport");
                Box::new(GrpcAdapter::new(endpoint.clone(), *timeout_ms))
            }
        }
    }

    fn teardown_client(&mut self) {
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }
        self.client = None;
        self.health.store(false, Ordering::Relaxed);
    }

    fn spawn_heartbeat(&mut self, client: SharedClient) {
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }
        let interval_duration = self.heartbeat_interval;
        let max_failures = self.max_heartbeat_failures;
        let health = self.health.clone();
        self.heartbeat_handle = Some(tokio::spawn(async move {
            let mut ticker = interval(interval_duration);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut failures = 0u32;
            loop {
                ticker.tick().await;
                let mut guard = client.lock().await;
                match guard.heartbeat().await {
                    Ok(resp) if resp.healthy => {
                        health.store(true, Ordering::Relaxed);
                        failures = 0;
                    }
                    Ok(resp) => {
                        warn!(
                            target: "rpc",
                            status = %resp.status_msg,
                            "heartbeat reported unhealthy"
                        );
                        failures += 1;
                        health.store(false, Ordering::Relaxed);
                    }
                    Err(err) => {
                        warn!(target: "rpc", %err, "heartbeat failure");
                        failures += 1;
                        health.store(false, Ordering::Relaxed);
                    }
                }

                if failures >= max_failures {
                    error!(
                        target: "rpc",
                        failures,
                        "heartbeat exceeded failure threshold"
                    );
                    break;
                }
            }
        }));
    }

    async fn ensure_client(&mut self) -> StrategyResult<SharedClient> {
        if let Some(handle) = &self.client {
            if self.health.load(Ordering::Relaxed) {
                return Ok(handle.clone());
            }
            self.teardown_client();
        }

        if self.client.is_none() {
            let config = self
                .config
                .clone()
                .ok_or_else(|| StrategyError::InvalidConfig("rpc config missing".into()))?;

            let mut client = Self::build_client(&config.transport);

            client
                .connect()
                .await
                .map_err(|e| StrategyError::Internal(format!("RPC connect failed: {e}")))?;

            let init_request = InitRequest {
                config_json: self.config_payload.clone(),
            };

            let response = client.initialize(init_request).await.map_err(|e| {
                StrategyError::Internal(format!("remote strategy init failed: {e}"))
            })?;

            if !response.success {
                return Err(StrategyError::Internal(format!(
                    "remote strategy rejected init: {}",
                    response.error_message
                )));
            }

            let symbols = if !config.symbols.is_empty() {
                config.symbols
            } else {
                response.symbols
            };
            self.apply_remote_metadata(symbols);
            info!(target: "rpc", symbols = ?self.subscriptions, "RPC strategy initialized");
            self.health.store(true, Ordering::Relaxed);
            let shared = Arc::new(AsyncMutex::new(client));
            self.spawn_heartbeat(shared.clone());
            self.client = Some(shared.clone());
            return Ok(shared);
        }

        self.client
            .as_ref()
            .cloned()
            .ok_or_else(|| StrategyError::Internal("RPC client not initialized".into()))
    }

    fn apply_remote_metadata(&mut self, mut symbols: Vec<String>) {
        if symbols.is_empty() {
            symbols.push(self.symbol.to_string());
        }
        let parsed: Vec<Symbol> = symbols.into_iter().map(Symbol::from).collect();
        if let Some(primary) = parsed.first().copied() {
            self.symbol = primary;
        }
        self.subscriptions = parsed;
    }

    fn handle_signals(&mut self, signals: Vec<crate::proto::Signal>) {
        for proto_sig in signals {
            self.pending_signals.push(proto_sig.into());
        }
    }

    fn handle_rpc_error(&mut self, err: Error, context: &str) {
        error!(target: "rpc", %context, error = %err, "RPC call failed; dropping client");
        self.teardown_client();
    }

    fn is_symbol_allowed(&self, symbol: &Symbol) -> bool {
        self.subscriptions.is_empty() || self.subscriptions.iter().any(|s| s == symbol)
    }
}

#[async_trait]
impl Strategy for RpcStrategy {
    fn name(&self) -> &str {
        "rpc-strategy"
    }

    fn symbol(&self) -> Symbol {
        self.symbol
    }

    fn subscriptions(&self) -> Vec<Symbol> {
        if self.subscriptions.is_empty() {
            vec![self.symbol]
        } else {
            self.subscriptions.clone()
        }
    }

    fn configure(&mut self, params: toml::Value) -> StrategyResult<()> {
        let config: RpcStrategyConfig = params.clone().try_into().map_err(|e| {
            StrategyError::InvalidConfig(format!("failed to parse RPC config: {}", e))
        })?;

        match &config.transport {
            TransportConfig::Grpc { endpoint, .. } => Url::parse(endpoint).map_err(|e| {
                StrategyError::InvalidConfig(format!(
                    "invalid gRPC endpoint URL '{}': {}",
                    endpoint, e
                ))
            })?,
        };

        self.heartbeat_interval = Duration::from_millis(config.heartbeat_interval_ms.max(1));
        self.config = Some(config.clone());
        self.teardown_client();
        self.pending_signals.clear();
        self.subscriptions = config
            .symbols
            .iter()
            .map(|symbol| Symbol::from(symbol.as_str()))
            .collect();
        self.symbol = self
            .subscriptions
            .first()
            .copied()
            .unwrap_or_else(|| Symbol::from("UNKNOWN"));
        self.config_payload = serde_json::to_string(&params).unwrap_or_else(|_| "{}".to_string());
        Ok(())
    }

    async fn on_tick(&mut self, ctx: &StrategyContext, tick: &Tick) -> StrategyResult<()> {
        if !self.is_symbol_allowed(&tick.symbol) {
            return Ok(());
        }
        let request = TickRequest {
            tick: Some(tick.clone().into()),
            context: Some(ctx.into()),
        };

        let client = match self.ensure_client().await {
            Ok(client) => client,
            Err(e) => {
                warn!(target: "rpc", "Skipping OnTick, client unavailable: {}", e);
                return Ok(());
            }
        };
        let mut transport = client.lock().await;
        let result = transport.on_tick(request).await;
        drop(transport);
        match result {
            Ok(response) => self.handle_signals(response.signals),
            Err(e) => self.handle_rpc_error(e, "OnTick"),
        }
        Ok(())
    }

    async fn on_candle(&mut self, ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        if !self.is_symbol_allowed(&candle.symbol) {
            return Ok(());
        }
        let request = CandleRequest {
            candle: Some(candle.clone().into()),
            context: Some(ctx.into()),
        };

        let client = match self.ensure_client().await {
            Ok(client) => client,
            Err(e) => {
                warn!(target: "rpc", "Skipping OnCandle, client unavailable: {}", e);
                return Ok(());
            }
        };
        let mut transport = client.lock().await;
        let result = transport.on_candle(request).await;
        drop(transport);
        match result {
            Ok(response) => self.handle_signals(response.signals),
            Err(e) => self.handle_rpc_error(e, "OnCandle"),
        }
        Ok(())
    }

    async fn on_fill(&mut self, ctx: &StrategyContext, fill: &Fill) -> StrategyResult<()> {
        let request = FillRequest {
            fill: Some(fill.clone().into()),
            context: Some(ctx.into()),
        };

        let client = match self.ensure_client().await {
            Ok(client) => client,
            Err(e) => {
                warn!(target: "rpc", "Skipping OnFill, client unavailable: {}", e);
                return Ok(());
            }
        };
        let mut transport = client.lock().await;
        let result = transport.on_fill(request).await;
        drop(transport);
        match result {
            Ok(response) => self.handle_signals(response.signals),
            Err(e) => self.handle_rpc_error(e, "OnFill"),
        }
        Ok(())
    }

    async fn on_order_book(
        &mut self,
        ctx: &StrategyContext,
        book: &OrderBook,
    ) -> StrategyResult<()> {
        if !self.is_symbol_allowed(&book.symbol) {
            return Ok(());
        }
        let request = OrderBookRequest {
            order_book: Some(book.clone().into()),
            context: Some(ctx.into()),
        };

        let client = match self.ensure_client().await {
            Ok(client) => client,
            Err(e) => {
                warn!(target: "rpc", "Skipping OnOrderBook, client unavailable: {}", e);
                return Ok(());
            }
        };
        let mut transport = client.lock().await;
        let result = transport.on_order_book(request).await;
        drop(transport);
        match result {
            Ok(response) => self.handle_signals(response.signals),
            Err(e) => self.handle_rpc_error(e, "OnOrderBook"),
        }
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.pending_signals)
    }
}

register_strategy!(RpcStrategy, "RpcStrategy");

impl Drop for RpcStrategy {
    fn drop(&mut self) {
        self.teardown_client();
    }
}
