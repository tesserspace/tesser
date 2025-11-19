use anyhow::Context;
use async_trait::async_trait;
use serde::Deserialize;
use tesser_core::{Candle, Fill, OrderBook, Signal, Symbol, Tick};
use tesser_strategy::{
    register_strategy, Strategy, StrategyContext, StrategyError, StrategyResult,
};
use tracing::{error, info, warn};

use crate::client::RemoteStrategyClient;
use crate::proto::{
    CandleRequest, FillRequest, InitRequest, OrderBookRequest, TickRequest,
};
use crate::transport::grpc::GrpcAdapter;

#[derive(Deserialize)]
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

/// A strategy adapter that delegates decision making to an external service via a pluggable transport.
pub struct RpcStrategy {
    client: Option<Box<dyn RemoteStrategyClient>>,
    subscriptions: Vec<String>,
    pending_signals: Vec<Signal>,
    symbol: String, // Primary symbol fallback
}

impl Default for RpcStrategy {
    fn default() -> Self {
        Self {
            client: None,
            subscriptions: vec![],
            pending_signals: vec![],
            symbol: "UNKNOWN".to_string(),
        }
    }
}

impl RpcStrategy {
    fn client(&self) -> StrategyResult<&dyn RemoteStrategyClient> {
        self.client
            .as_deref()
            .ok_or_else(|| StrategyError::InvalidConfig("RPC client not initialized".into()))
    }

    fn handle_signals(&mut self, signals: Vec<crate::proto::Signal>) {
        for proto_sig in signals {
            self.pending_signals.push(proto_sig.into());
        }
    }
}

#[async_trait]
impl Strategy for RpcStrategy {
    fn name(&self) -> &str {
        "rpc-strategy"
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn subscriptions(&self) -> Vec<Symbol> {
        if self.subscriptions.is_empty() {
            vec![self.symbol.clone()]
        } else {
            self.subscriptions.clone()
        }
    }

    fn configure(&mut self, params: toml::Value) -> StrategyResult<()> {
        let config: TransportConfig = params.try_into().map_err(|e| {
            StrategyError::InvalidConfig(format!("failed to parse RPC config: {}", e))
        })?;

        let client: Box<dyn RemoteStrategyClient> = match config {
            TransportConfig::Grpc { endpoint, timeout_ms } => {
                info!("configured gRPC transport targeting {}", endpoint);
                Box::new(GrpcAdapter::new(endpoint, timeout_ms))
            }
        };
        
        self.client = Some(client);
        Ok(())
    }

    async fn initialize(&mut self, _ctx: &mut StrategyContext) -> StrategyResult<()> {
        let client = self.client.as_mut()
            .ok_or_else(|| StrategyError::InvalidConfig("Client not ready".into()))?;
            
        client.connect().await.map_err(|e| StrategyError::Internal(format!("Connect failed: {}", e)))?;

        let req = InitRequest {
            config_json: "{}".to_string(), 
        };

        let response = client.initialize(req).await
            .map_err(|e| StrategyError::Internal(format!("Remote strategy init failed: {}", e)))?;

        if !response.success {
            return Err(StrategyError::Internal(format!("Remote strategy rejected init: {}", response.error_message)));
        }

        self.subscriptions = response.symbols;
        if let Some(first) = self.subscriptions.first() {
            self.symbol = first.clone();
        }
        
        info!("RPC strategy initialized via transport, subscribed to: {:?}", self.subscriptions);
        Ok(())
    }

    async fn on_tick(&mut self, ctx: &StrategyContext, tick: &Tick) -> StrategyResult<()> {
        let request = TickRequest {
            tick: Some(tick.clone().into()),
            context: Some(ctx.into()),
        };

        match self.client()?.on_tick(request).await {
            Ok(response) => self.handle_signals(response.signals),
            Err(e) => error!("RPC OnTick error: {}", e),
        }
        Ok(())
    }

    async fn on_candle(&mut self, ctx: &StrategyContext, candle: &Candle) -> StrategyResult<()> {
        let request = CandleRequest {
            candle: Some(candle.clone().into()),
            context: Some(ctx.into()),
        };

        match self.client()?.on_candle(request).await {
            Ok(response) => self.handle_signals(response.signals),
            Err(e) => error!("RPC OnCandle error: {}", e),
        }
        Ok(())
    }

    async fn on_fill(&mut self, ctx: &StrategyContext, fill: &Fill) -> StrategyResult<()> {
        let request = FillRequest {
            fill: Some(fill.clone().into()),
            context: Some(ctx.into()),
        };

        match self.client()?.on_fill(request).await {
            Ok(response) => self.handle_signals(response.signals),
            Err(e) => error!("RPC OnFill error: {}", e),
        }
        Ok(())
    }

    async fn on_order_book(&mut self, ctx: &StrategyContext, book: &OrderBook) -> StrategyResult<()> {
        let request = OrderBookRequest {
            order_book: Some(book.clone().into()),
            context: Some(ctx.into()),
        };

        match self.client()?.on_order_book(request).await {
            Ok(response) => self.handle_signals(response.signals),
            Err(e) => error!("RPC OnOrderBook error: {}", e),
        }
        Ok(())
    }

    fn drain_signals(&mut self) -> Vec<Signal> {
        std::mem::take(&mut self.pending_signals)
    }
}

register_strategy!(RpcStrategy, "RpcStrategy");
