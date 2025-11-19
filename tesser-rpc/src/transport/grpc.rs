use anyhow::Result;
use async_trait::async_trait;
use tonic::transport::Channel;
use tracing::debug;

use crate::client::RemoteStrategyClient;
use crate::proto::strategy_service_client::StrategyServiceClient;
use crate::proto::{
    CandleRequest, FillRequest, InitRequest, InitResponse, OrderBookRequest, SignalList, TickRequest,
};

/// A gRPC-based implementation of the strategy client.
pub struct GrpcAdapter {
    endpoint: String,
    client: Option<StrategyServiceClient<Channel>>,
    timeout_ms: u64,
}

impl GrpcAdapter {
    pub fn new(endpoint: String, timeout_ms: u64) -> Self {
        Self {
            endpoint,
            client: None,
            timeout_ms,
        }
    }

    fn client(&self) -> Result<StrategyServiceClient<Channel>> {
        self.client.clone().ok_or_else(|| anyhow::anyhow!("gRPC client not connected"))
    }
    
    async fn request<T, R, F>(
        &self,
        req: T,
        f: F
    ) -> Result<R>
    where
        F: FnOnce(StrategyServiceClient<Channel>, tonic::Request<T>) -> tonic::client::GrpcFuture<R>,
    {
        let client = self.client()?;
        let mut request = tonic::Request::new(req);
        request.set_timeout(std::time::Duration::from_millis(self.timeout_ms));
        
        let response = f(client, request).await?;
        Ok(response.into_inner())
    }
}

#[async_trait]
impl RemoteStrategyClient for GrpcAdapter {
    async fn connect(&mut self) -> Result<()> {
        debug!("connecting to gRPC strategy at {}", self.endpoint);
        let channel = Channel::from_shared(self.endpoint.clone())?
            .connect_lazy();
        self.client = Some(StrategyServiceClient::new(channel));
        Ok(())
    }

    async fn initialize(&mut self, req: InitRequest) -> Result<InitResponse> {
        // Initialization might take longer than event processing
        let client = self.client()?;
        let response = client.clone().initialize(tonic::Request::new(req)).await?;
        Ok(response.into_inner())
    }

    async fn on_tick(&self, req: TickRequest) -> Result<SignalList> {
        self.request(req, |mut c, r| c.on_tick(r)).await
    }

    async fn on_candle(&self, req: CandleRequest) -> Result<SignalList> {
        self.request(req, |mut c, r| c.on_candle(r)).await
    }

    async fn on_order_book(&self, req: OrderBookRequest) -> Result<SignalList> {
        self.request(req, |mut c, r| c.on_order_book(r)).await
    }

    async fn on_fill(&self, req: FillRequest) -> Result<SignalList> {
        self.request(req, |mut c, r| c.on_fill(r)).await
    }
}
