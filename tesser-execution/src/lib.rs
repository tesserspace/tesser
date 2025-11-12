//! Order management and signal execution helpers.

use std::sync::Arc;

use anyhow::Context;
use tesser_broker::{BrokerError, BrokerResult, ExecutionClient};
use tesser_bybit::{BybitClient, BybitCredentials};
use tesser_core::{Order, OrderRequest, OrderType, Quantity, Side, Signal, SignalKind, Symbol};
use thiserror::Error;
use tracing::{info, warn};

/// Determine how large an order should be for a given signal.
pub trait OrderSizer: Send + Sync {
    /// Calculate the desired base asset quantity.
    fn size(&self, signal: &Signal) -> anyhow::Result<Quantity>;
}

/// Simplest possible sizer that always returns a fixed size.
pub struct FixedOrderSizer {
    pub quantity: Quantity,
}

impl OrderSizer for FixedOrderSizer {
    fn size(&self, _signal: &Signal) -> anyhow::Result<Quantity> {
        Ok(self.quantity)
    }
}

/// Context passed to risk checks describing current exposure state.
#[derive(Clone, Copy, Debug, Default)]
pub struct RiskContext {
    /// Signed quantity of the current open position (long positive, short negative).
    pub signed_position_qty: f64,
    /// When true, only exposure-reducing orders are allowed.
    pub liquidate_only: bool,
}

/// Validates an order before it reaches the broker.
pub trait PreTradeRiskChecker: Send + Sync {
    /// Return `Ok(())` if the order passes risk checks.
    fn check(&self, request: &OrderRequest, ctx: &RiskContext) -> Result<(), RiskError>;
}

/// No-op risk checker used by tests/backtests.
pub struct NoopRiskChecker;

impl PreTradeRiskChecker for NoopRiskChecker {
    fn check(&self, _request: &OrderRequest, _ctx: &RiskContext) -> Result<(), RiskError> {
        Ok(())
    }
}

/// Upper bounds enforced by the [`BasicRiskChecker`].
#[derive(Clone, Copy, Debug)]
pub struct RiskLimits {
    pub max_order_quantity: f64,
    pub max_position_quantity: f64,
}

impl RiskLimits {
    /// Ensure limits are non-negative and default to zero (disabled) when NaN.
    pub fn sanitized(self) -> Self {
        Self {
            max_order_quantity: self.max_order_quantity.max(0.0),
            max_position_quantity: self.max_position_quantity.max(0.0),
        }
    }
}

/// Simple risk checker enforcing fat-finger order size limits plus position caps.
pub struct BasicRiskChecker {
    limits: RiskLimits,
}

impl BasicRiskChecker {
    /// Build a new checker with the provided limits.
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits: limits.sanitized(),
        }
    }
}

impl PreTradeRiskChecker for BasicRiskChecker {
    fn check(&self, request: &OrderRequest, ctx: &RiskContext) -> Result<(), RiskError> {
        let qty = request.quantity.abs();
        if self.limits.max_order_quantity > 0.0 && qty > self.limits.max_order_quantity {
            return Err(RiskError::MaxOrderSize {
                quantity: qty,
                limit: self.limits.max_order_quantity,
            });
        }

        let projected_position = match request.side {
            Side::Buy => ctx.signed_position_qty + qty,
            Side::Sell => ctx.signed_position_qty - qty,
        };

        if self.limits.max_position_quantity > 0.0
            && projected_position.abs() > self.limits.max_position_quantity
        {
            return Err(RiskError::MaxPositionExposure {
                projected: projected_position,
                limit: self.limits.max_position_quantity,
            });
        }

        if ctx.liquidate_only {
            let position = ctx.signed_position_qty;
            if position.abs() < f64::EPSILON {
                return Err(RiskError::LiquidateOnly);
            }
            let reduces = (position > 0.0 && request.side == Side::Sell)
                || (position < 0.0 && request.side == Side::Buy);
            if !reduces {
                return Err(RiskError::LiquidateOnly);
            }
            if qty > position.abs() + f64::EPSILON {
                return Err(RiskError::LiquidateOnly);
            }
        }

        Ok(())
    }
}

/// Errors surfaced by pre-trade risk checks.
#[derive(Debug, Error)]
pub enum RiskError {
    #[error("order quantity {quantity:.4} exceeds limit {limit:.4}")]
    MaxOrderSize { quantity: f64, limit: f64 },
    #[error("projected position {projected:.4} exceeds limit {limit:.4}")]
    MaxPositionExposure { projected: f64, limit: f64 },
    #[error("liquidate-only mode active")]
    LiquidateOnly,
}

/// Translates signals into orders using a provided [`ExecutionClient`].
pub struct ExecutionEngine {
    client: Arc<dyn ExecutionClient>,
    sizer: Box<dyn OrderSizer>,
    risk: Arc<dyn PreTradeRiskChecker>,
}

impl ExecutionEngine {
    /// Instantiate the engine with its dependencies.
    pub fn new(
        client: Arc<dyn ExecutionClient>,
        sizer: Box<dyn OrderSizer>,
        risk: Arc<dyn PreTradeRiskChecker>,
    ) -> Self {
        Self {
            client,
            sizer,
            risk,
        }
    }

    /// Consume a signal and forward it to the broker.
    pub async fn handle_signal(
        &self,
        signal: Signal,
        ctx: RiskContext,
    ) -> BrokerResult<Option<Order>> {
        let qty = self
            .sizer
            .size(&signal)
            .context("failed to determine order size")
            .map_err(|err| BrokerError::Other(err.to_string()))?;

        if qty <= 0.0 {
            warn!(signal = ?signal.id, "order size is zero, skipping");
            return Ok(None);
        }

        let request = match signal.kind {
            SignalKind::EnterLong => self.build_request(signal.symbol.clone(), Side::Buy, qty),
            SignalKind::ExitLong | SignalKind::Flatten => {
                self.build_request(signal.symbol.clone(), Side::Sell, qty)
            }
            SignalKind::EnterShort => self.build_request(signal.symbol.clone(), Side::Sell, qty),
            SignalKind::ExitShort => self.build_request(signal.symbol.clone(), Side::Buy, qty),
        };

        let order = self.send_order(request, &ctx).await?;

        let stop_side = match signal.kind {
            SignalKind::EnterLong | SignalKind::ExitShort => Side::Sell,
            SignalKind::EnterShort | SignalKind::ExitLong => Side::Buy,
            SignalKind::Flatten => return Ok(Some(order)),
        };

        if let Some(sl_price) = signal.stop_loss {
            let sl_request = OrderRequest {
                symbol: signal.symbol.clone(),
                side: stop_side,
                order_type: OrderType::StopMarket,
                quantity: qty,
                price: None,
                trigger_price: Some(sl_price),
                time_in_force: None,
                client_order_id: None,
            };
            if let Err(e) = self.send_order(sl_request, &ctx).await {
                warn!(error = %e, "failed to place stop-loss order");
            }
        }

        if let Some(tp_price) = signal.take_profit {
            let tp_request = OrderRequest {
                symbol: signal.symbol.clone(),
                side: stop_side,
                order_type: OrderType::StopMarket,
                quantity: qty,
                price: None,
                trigger_price: Some(tp_price),
                time_in_force: None,
                client_order_id: None,
            };
            if let Err(e) = self.send_order(tp_request, &ctx).await {
                warn!(error = %e, "failed to place take-profit order");
            }
        }

        Ok(Some(order))
    }

    fn build_request(&self, symbol: Symbol, side: Side, qty: Quantity) -> OrderRequest {
        OrderRequest {
            symbol,
            side,
            order_type: OrderType::Market,
            quantity: qty,
            price: None,
            trigger_price: None,
            time_in_force: None,
            client_order_id: None,
        }
    }

    async fn send_order(&self, request: OrderRequest, ctx: &RiskContext) -> BrokerResult<Order> {
        self.risk
            .check(&request, ctx)
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let order = self.client.place_order(request).await?;
        info!(order_id = %order.id, qty = order.request.quantity, "order sent to broker");
        Ok(order)
    }

    pub fn client(&self) -> Arc<dyn ExecutionClient> {
        Arc::clone(&self.client)
    }

    pub fn credentials(&self) -> Option<BybitCredentials> {
        self.client
            .as_any()
            .downcast_ref::<BybitClient>()
            .and_then(|client| client.get_credentials())
    }

    pub fn ws_url(&self) -> String {
        self.client
            .as_any()
            .downcast_ref::<BybitClient>()
            .map(|client| client.get_ws_url())
            .unwrap_or_default()
    }
}
