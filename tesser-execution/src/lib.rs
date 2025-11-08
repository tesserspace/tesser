//! Order management and signal execution helpers.

use anyhow::Context;
use tesser_broker::{BrokerError, BrokerResult, ExecutionClient};
use tesser_core::{Order, OrderRequest, OrderType, Quantity, Side, Signal, SignalKind};
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

/// Translates signals into orders using a provided [`ExecutionClient`].
pub struct ExecutionEngine<C> {
    client: C,
    sizer: Box<dyn OrderSizer>,
}

impl<C> ExecutionEngine<C>
where
    C: ExecutionClient,
{
    /// Instantiate the engine with its dependencies.
    pub fn new(client: C, sizer: Box<dyn OrderSizer>) -> Self {
        Self { client, sizer }
    }

    /// Consume a signal and forward it to the broker.
    pub async fn handle_signal(&self, signal: Signal) -> BrokerResult<Option<Order>> {
        let qty = self
            .sizer
            .size(&signal)
            .context("failed to determine order size")
            .map_err(|err| BrokerError::Other(err.to_string()))?;

        if qty <= 0.0 {
            warn!(signal = ?signal.id, "order size is zero, skipping");
            return Ok(None);
        }

        let order = match signal.kind {
            SignalKind::EnterLong => {
                self.send_order(signal.symbol.clone(), Side::Buy, qty)
                    .await?
            }
            SignalKind::ExitLong | SignalKind::Flatten => {
                self.send_order(signal.symbol.clone(), Side::Sell, qty)
                    .await?
            }
            SignalKind::EnterShort => {
                self.send_order(signal.symbol.clone(), Side::Sell, qty)
                    .await?
            }
            SignalKind::ExitShort => {
                self.send_order(signal.symbol.clone(), Side::Buy, qty)
                    .await?
            }
        };
        Ok(Some(order))
    }

    async fn send_order(&self, symbol: String, side: Side, qty: Quantity) -> BrokerResult<Order> {
        let request = OrderRequest {
            symbol,
            side,
            order_type: OrderType::Market,
            quantity: qty,
            price: None,
            time_in_force: None,
            client_order_id: None,
        };
        let order = self.client.place_order(request).await?;
        info!(order_id = %order.id, %qty, "order sent to broker");
        Ok(order)
    }
}
