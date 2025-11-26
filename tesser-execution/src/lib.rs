//! Order management and signal execution helpers.

pub mod algorithm;
pub mod orchestrator;
pub mod repository;

// Re-export key types for convenience
pub use algorithm::{AlgoStatus, ChildOrderRequest, ExecutionAlgorithm};
pub use orchestrator::OrderOrchestrator;
pub use repository::{AlgoStateRepository, SqliteAlgoStateRepository, StoredAlgoState};

use anyhow::{bail, Context};
use rust_decimal::Decimal;
use std::sync::Arc;
use tesser_broker::{BrokerError, BrokerResult, ExecutionClient};
use tesser_core::{
    AssetId, ExchangeId, InstrumentKind, Order, OrderRequest, OrderType, OrderUpdateRequest, Price,
    Quantity, Side, Signal, SignalKind, Symbol,
};
use thiserror::Error;
use tracing::{info, warn};
use uuid::Uuid;

/// Determines how the orchestrator unwinds partially filled execution groups.
#[derive(Clone, Copy, Debug, Default)]
pub enum PanicCloseMode {
    #[default]
    Market,
    AggressiveLimit,
}

/// Configuration describing how panic-close orders should be sent.
#[derive(Clone, Copy, Debug)]
pub struct PanicCloseConfig {
    pub mode: PanicCloseMode,
    /// Offset applied to the observed mid price when using [`PanicCloseMode::AggressiveLimit`] (basis points).
    pub limit_offset_bps: Decimal,
}

impl Default for PanicCloseConfig {
    fn default() -> Self {
        Self {
            mode: PanicCloseMode::Market,
            limit_offset_bps: Decimal::from(50u32),
        }
    }
}

/// Observes panic-close events so callers can emit alerts or metrics.
pub trait PanicObserver: Send + Sync {
    fn on_group_event(&self, group_id: Uuid, symbol: Symbol, quantity: Quantity, reason: &str);
}

/// Determine how large an order should be for a given signal.
pub trait OrderSizer: Send + Sync {
    /// Calculate the desired base asset quantity.
    fn size(
        &self,
        signal: &Signal,
        portfolio_equity: Price,
        last_price: Price,
    ) -> anyhow::Result<Quantity>;
}

/// Simplest possible sizer that always returns a fixed size.
pub struct FixedOrderSizer {
    pub quantity: Quantity,
}

impl OrderSizer for FixedOrderSizer {
    fn size(
        &self,
        _signal: &Signal,
        _portfolio_equity: Price,
        _last_price: Price,
    ) -> anyhow::Result<Quantity> {
        Ok(self.quantity)
    }
}

/// Sizes orders based on a fixed percentage of portfolio equity.
pub struct PortfolioPercentSizer {
    /// The fraction of equity to allocate per trade (e.g., 0.02 for 2%).
    pub percent: Decimal,
}

impl OrderSizer for PortfolioPercentSizer {
    fn size(
        &self,
        _signal: &Signal,
        portfolio_equity: Price,
        last_price: Price,
    ) -> anyhow::Result<Quantity> {
        if last_price <= Decimal::ZERO {
            bail!("cannot size order with zero or negative price");
        }
        if self.percent <= Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }
        let notional = portfolio_equity * self.percent;
        Ok(notional / last_price)
    }
}

/// Sizes orders based on position volatility. (Placeholder)
#[derive(Default)]
pub struct RiskAdjustedSizer {
    /// Target risk contribution per trade, as a fraction of equity (e.g., 0.002 for 0.2%).
    pub risk_fraction: Decimal,
}

impl OrderSizer for RiskAdjustedSizer {
    fn size(
        &self,
        _signal: &Signal,
        portfolio_equity: Price,
        last_price: Price,
    ) -> anyhow::Result<Quantity> {
        if last_price <= Decimal::ZERO {
            bail!("cannot size order with zero or negative price");
        }
        if self.risk_fraction <= Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }
        // Placeholder volatility; replace with instrument-specific estimator.
        let volatility = Decimal::new(2, 2); // 0.02
        let denom = last_price * volatility;
        if denom <= Decimal::ZERO {
            bail!("volatility multiplier produced an invalid denominator");
        }
        let dollars_at_risk = portfolio_equity * self.risk_fraction;
        Ok(dollars_at_risk / denom)
    }
}

/// Context passed to risk checks describing current exposure state.
#[derive(Clone, Copy, Debug, Default)]
pub struct RiskContext {
    /// Symbol used to construct risk metadata.
    pub symbol: Symbol,
    /// Venue that will carry the exposure.
    pub exchange: ExchangeId,
    /// Signed quantity of the current open position (long positive, short negative).
    pub signed_position_qty: Quantity,
    /// Total current portfolio equity.
    pub portfolio_equity: Price,
    /// Equity scoped to the symbol's exchange.
    pub exchange_equity: Price,
    /// Last known price for the signal's symbol.
    pub last_price: Price,
    /// When true, only exposure-reducing orders are allowed.
    pub liquidate_only: bool,
    /// Instrument kind, if metadata is available.
    pub instrument_kind: Option<InstrumentKind>,
    /// Base asset tracked for solvency checks.
    pub base_asset: AssetId,
    /// Quote asset tracked for solvency checks.
    pub quote_asset: AssetId,
    /// Settlement asset for derivatives.
    pub settlement_asset: AssetId,
    /// Available base asset quantity on the venue.
    pub base_available: Quantity,
    /// Available quote asset quantity on the venue.
    pub quote_available: Quantity,
    /// Available settlement asset quantity on the venue.
    pub settlement_available: Quantity,
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
    pub max_order_quantity: Quantity,
    pub max_position_quantity: Quantity,
    pub max_order_notional: Option<Price>,
}

impl RiskLimits {
    /// Ensure limits are non-negative and default to zero (disabled) when NaN.
    pub fn sanitized(self) -> Self {
        Self {
            max_order_quantity: self.max_order_quantity.max(Decimal::ZERO),
            max_position_quantity: self.max_position_quantity.max(Decimal::ZERO),
            max_order_notional: self
                .max_order_notional
                .and_then(|limit| (limit > Decimal::ZERO).then_some(limit)),
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
        let max_order = self.limits.max_order_quantity;
        if max_order > Decimal::ZERO && qty > max_order {
            return Err(RiskError::MaxOrderSize {
                quantity: qty,
                limit: max_order,
            });
        }

        let positive_last_price = || {
            if ctx.last_price > Decimal::ZERO {
                Some(ctx.last_price)
            } else {
                None
            }
        };

        let reference_price = match request.order_type {
            OrderType::Limit => request
                .price
                .filter(|price| *price > Decimal::ZERO)
                .or_else(positive_last_price),
            _ => positive_last_price(),
        };

        if let Some(limit) = self.limits.max_order_notional {
            if let Some(price) = reference_price {
                let notional = qty * price;
                if notional > limit {
                    return Err(RiskError::MaxOrderNotional { notional, limit });
                }
            }
        }

        let projected_position = match request.side {
            Side::Buy => ctx.signed_position_qty + qty,
            Side::Sell => ctx.signed_position_qty - qty,
        };

        let max_position = self.limits.max_position_quantity;
        if max_position > Decimal::ZERO && projected_position.abs() > max_position {
            return Err(RiskError::MaxPositionExposure {
                projected: projected_position,
                limit: max_position,
            });
        }

        if ctx.liquidate_only {
            let position = ctx.signed_position_qty;
            if position.is_zero() {
                return Err(RiskError::LiquidateOnly);
            }
            let reduces = (position > Decimal::ZERO && request.side == Side::Sell)
                || (position < Decimal::ZERO && request.side == Side::Buy);
            if !reduces {
                return Err(RiskError::LiquidateOnly);
            }
            if qty > position.abs() {
                return Err(RiskError::LiquidateOnly);
            }
        }

        match ctx.instrument_kind {
            Some(InstrumentKind::Spot) => match request.side {
                Side::Buy => {
                    if let Some(price) = reference_price {
                        let notional = qty * price;
                        if ctx.quote_available < notional {
                            return Err(RiskError::InsufficientBalance {
                                asset: ctx.quote_asset,
                                needed: notional,
                                available: ctx.quote_available,
                            });
                        }
                    }
                }
                Side::Sell => {
                    if ctx.base_available < qty {
                        return Err(RiskError::InsufficientBalance {
                            asset: ctx.base_asset,
                            needed: qty,
                            available: ctx.base_available,
                        });
                    }
                }
            },
            Some(InstrumentKind::LinearPerpetual) => {
                if let Some(price) = reference_price {
                    let margin = qty * price;
                    if ctx.settlement_available < margin {
                        return Err(RiskError::InsufficientBalance {
                            asset: ctx.settlement_asset,
                            needed: margin,
                            available: ctx.settlement_available,
                        });
                    }
                }
            }
            Some(InstrumentKind::InversePerpetual) => {
                if let Some(price) = reference_price {
                    if price > Decimal::ZERO {
                        let margin = qty / price;
                        if ctx.settlement_available < margin {
                            return Err(RiskError::InsufficientBalance {
                                asset: ctx.settlement_asset,
                                needed: margin,
                                available: ctx.settlement_available,
                            });
                        }
                    }
                }
            }
            None => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tesser_core::SignalKind;

    fn dummy_signal() -> Signal {
        Signal::new("BTCUSDT", SignalKind::EnterLong, 1.0)
    }

    #[test]
    fn portfolio_percent_sizer_matches_decimal_math() {
        let signal = dummy_signal();
        let sizer = PortfolioPercentSizer {
            percent: Decimal::new(5, 2),
        };
        let qty = sizer
            .size(&signal, Decimal::from(25_000), Decimal::from(50_000))
            .unwrap();
        assert_eq!(qty, Decimal::new(25, 3)); // 0.025
    }

    #[test]
    fn risk_adjusted_sizer_respects_zero_price_guard() {
        let signal = dummy_signal();
        let sizer = RiskAdjustedSizer {
            risk_fraction: Decimal::new(1, 2),
        };
        let err = sizer
            .size(&signal, Decimal::from(10_000), Decimal::ZERO)
            .unwrap_err();
        assert!(
            err.to_string().contains("zero or negative price"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn liquidate_only_blocks_new_exposure() {
        let checker = BasicRiskChecker::new(RiskLimits {
            max_order_quantity: Decimal::ZERO,
            max_position_quantity: Decimal::ZERO,
            max_order_notional: None,
        });
        let ctx = RiskContext {
            signed_position_qty: Decimal::from(2),
            portfolio_equity: Decimal::from(10_000),
            last_price: Decimal::from(25_000),
            liquidate_only: true,
            ..RiskContext::default()
        };
        let order = OrderRequest {
            symbol: "BTCUSDT".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: Decimal::ONE,
            price: None,
            trigger_price: None,
            time_in_force: None,
            client_order_id: None,
            take_profit: None,
            stop_loss: None,
            display_quantity: None,
        };
        let result = checker.check(&order, &ctx);
        assert!(matches!(result, Err(RiskError::LiquidateOnly)));
    }

    #[test]
    fn liquidate_only_allows_position_reduction() {
        let checker = BasicRiskChecker::new(RiskLimits {
            max_order_quantity: Decimal::ZERO,
            max_position_quantity: Decimal::ZERO,
            max_order_notional: None,
        });
        let ctx = RiskContext {
            signed_position_qty: Decimal::from(2),
            portfolio_equity: Decimal::from(10_000),
            last_price: Decimal::from(25_000),
            liquidate_only: true,
            ..RiskContext::default()
        };
        let reduce = OrderRequest {
            symbol: "BTCUSDT".into(),
            side: Side::Sell,
            order_type: OrderType::Market,
            quantity: Decimal::ONE,
            price: None,
            trigger_price: None,
            time_in_force: None,
            client_order_id: None,
            take_profit: None,
            stop_loss: None,
            display_quantity: None,
        };
        assert!(checker.check(&reduce, &ctx).is_ok());
    }

    #[test]
    fn limit_order_notional_limit_triggers_rejection() {
        let checker = BasicRiskChecker::new(RiskLimits {
            max_order_quantity: Decimal::ZERO,
            max_position_quantity: Decimal::ZERO,
            max_order_notional: Some(Decimal::from(10_000u32)),
        });
        let ctx = RiskContext {
            signed_position_qty: Decimal::ZERO,
            portfolio_equity: Decimal::from(25_000u32),
            last_price: Decimal::from(20_000u32),
            liquidate_only: false,
            ..RiskContext::default()
        };
        let order = OrderRequest {
            symbol: "BTCUSDT".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: Decimal::ONE,
            price: Some(Decimal::from(20_000u32)),
            trigger_price: None,
            time_in_force: None,
            client_order_id: None,
            take_profit: None,
            stop_loss: None,
            display_quantity: None,
        };
        match checker.check(&order, &ctx) {
            Err(RiskError::MaxOrderNotional { notional, limit }) => {
                assert_eq!(limit, Decimal::from(10_000u32));
                assert!(notional > limit, "expected {notional} > {limit}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn market_order_notional_limit_uses_last_price() {
        let checker = BasicRiskChecker::new(RiskLimits {
            max_order_quantity: Decimal::ZERO,
            max_position_quantity: Decimal::ZERO,
            max_order_notional: Some(Decimal::from(5_000u32)),
        });
        let ctx = RiskContext {
            signed_position_qty: Decimal::ZERO,
            portfolio_equity: Decimal::from(50_000u32),
            last_price: Decimal::from(25_000u32),
            liquidate_only: false,
            ..RiskContext::default()
        };
        let order = OrderRequest {
            symbol: "BTCUSDT".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: Decimal::ONE,
            price: None,
            trigger_price: None,
            time_in_force: None,
            client_order_id: None,
            take_profit: None,
            stop_loss: None,
            display_quantity: None,
        };
        assert!(matches!(
            checker.check(&order, &ctx),
            Err(RiskError::MaxOrderNotional { .. })
        ));
    }
}

/// Errors surfaced by pre-trade risk checks.
#[derive(Debug, Error)]
pub enum RiskError {
    #[error("order quantity {quantity} exceeds limit {limit}")]
    MaxOrderSize { quantity: Quantity, limit: Quantity },
    #[error("order notional {notional} exceeds limit {limit}")]
    MaxOrderNotional { notional: Price, limit: Price },
    #[error("projected position {projected} exceeds limit {limit}")]
    MaxPositionExposure {
        projected: Quantity,
        limit: Quantity,
    },
    #[error("liquidate-only mode active")]
    LiquidateOnly,
    #[error("insufficient {asset} balance: need {needed}, have {available}")]
    InsufficientBalance {
        asset: AssetId,
        needed: Quantity,
        available: Quantity,
    },
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

    /// Determine the quantity that should be used for a signal, honoring overrides when present.
    pub fn determine_quantity(
        &self,
        signal: &Signal,
        ctx: &RiskContext,
    ) -> anyhow::Result<Quantity> {
        if let Some(qty) = signal.quantity {
            return Ok(qty.max(Decimal::ZERO));
        }
        self.sizer.size(signal, ctx.exchange_equity, ctx.last_price)
    }

    /// Consume a signal and forward it to the broker.
    pub async fn handle_signal(
        &self,
        signal: Signal,
        ctx: RiskContext,
    ) -> BrokerResult<Option<Order>> {
        let qty = self
            .determine_quantity(&signal, &ctx)
            .context("failed to determine order size")
            .map_err(|err| BrokerError::Other(err.to_string()))?;

        if qty <= Decimal::ZERO {
            warn!(signal = ?signal.id, "order size is zero, skipping");
            return Ok(None);
        }

        let client_order_id = if let Some(group) = signal.group_id {
            format!("{}|grp:{}", signal.id, group)
        } else {
            signal.id.to_string()
        };
        let request = match signal.kind {
            SignalKind::EnterLong => {
                self.build_request(signal.symbol, Side::Buy, qty, Some(client_order_id.clone()))
            }
            SignalKind::ExitLong | SignalKind::Flatten => self.build_request(
                signal.symbol,
                Side::Sell,
                qty,
                Some(client_order_id.clone()),
            ),
            SignalKind::EnterShort => self.build_request(
                signal.symbol,
                Side::Sell,
                qty,
                Some(client_order_id.clone()),
            ),
            SignalKind::ExitShort => {
                self.build_request(signal.symbol, Side::Buy, qty, Some(client_order_id.clone()))
            }
        };

        let order = self.send_order(request, &ctx).await?;

        let stop_side = match signal.kind {
            SignalKind::EnterLong | SignalKind::ExitShort => Side::Sell,
            SignalKind::EnterShort | SignalKind::ExitLong => Side::Buy,
            SignalKind::Flatten => return Ok(Some(order)),
        };

        if let Some(sl_price) = signal.stop_loss {
            let sl_request = OrderRequest {
                symbol: signal.symbol,
                side: stop_side,
                order_type: OrderType::StopMarket,
                quantity: qty,
                price: None,
                trigger_price: Some(sl_price),
                time_in_force: None,
                client_order_id: Some(format!("{}-sl", signal.id)),
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            };
            if let Err(e) = self.send_order(sl_request, &ctx).await {
                warn!(error = %e, "failed to place stop-loss order");
            }
        }

        if let Some(tp_price) = signal.take_profit {
            let tp_request = OrderRequest {
                symbol: signal.symbol,
                side: stop_side,
                order_type: OrderType::StopMarket,
                quantity: qty,
                price: None,
                trigger_price: Some(tp_price),
                time_in_force: None,
                client_order_id: Some(format!("{}-tp", signal.id)),
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            };
            if let Err(e) = self.send_order(tp_request, &ctx).await {
                warn!(error = %e, "failed to place take-profit order");
            }
        }

        Ok(Some(order))
    }

    fn build_request(
        &self,
        symbol: Symbol,
        side: Side,
        qty: Quantity,
        client_order_id: Option<String>,
    ) -> OrderRequest {
        OrderRequest {
            symbol,
            side,
            order_type: OrderType::Market,
            quantity: qty,
            price: None,
            trigger_price: None,
            time_in_force: None,
            client_order_id,
            take_profit: None,
            stop_loss: None,
            display_quantity: None,
        }
    }

    async fn send_order(&self, request: OrderRequest, ctx: &RiskContext) -> BrokerResult<Order> {
        self.risk
            .check(&request, ctx)
            .map_err(|err| BrokerError::InvalidRequest(err.to_string()))?;
        let order = self.client.place_order(request).await?;
        info!(
            order_id = %order.id,
            qty = %order.request.quantity,
            "order sent to broker"
        );
        Ok(order)
    }

    pub async fn amend_order(&self, request: OrderUpdateRequest) -> BrokerResult<Order> {
        let order = self.client.amend_order(request).await?;
        info!(
            order_id = %order.id,
            qty = %order.request.quantity,
            "order amended via broker"
        );
        Ok(order)
    }

    pub fn client(&self) -> Arc<dyn ExecutionClient> {
        Arc::clone(&self.client)
    }

    pub fn sizer(&self) -> &dyn OrderSizer {
        self.sizer.as_ref()
    }
}
