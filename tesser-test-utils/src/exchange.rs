use anyhow::Result;

use crate::rest::MockRestApi;
use crate::state::{MockExchangeConfig, MockExchangeState};
use crate::websocket::MockWebSocketServer;

/// High-level interface for controlling the mock exchange servers.
pub struct MockExchange {
    state: MockExchangeState,
    rest: MockRestApi,
    websocket: MockWebSocketServer,
}

impl MockExchange {
    /// Spawns REST and WebSocket servers backed by the provided configuration.
    pub async fn start(config: MockExchangeConfig) -> Result<Self> {
        let state = MockExchangeState::new(config);
        let rest = MockRestApi::spawn(state.clone()).await?;
        let websocket = MockWebSocketServer::spawn(state.clone()).await?;
        Ok(Self {
            state,
            rest,
            websocket,
        })
    }

    #[must_use]
    pub fn rest_url(&self) -> String {
        self.rest.base_url()
    }

    #[must_use]
    pub fn ws_url(&self) -> String {
        self.websocket.base_url()
    }

    #[must_use]
    pub fn state(&self) -> MockExchangeState {
        self.state.clone()
    }

    pub async fn shutdown(&mut self) {
        self.rest.shutdown().await;
        self.websocket.shutdown().await;
    }
}
