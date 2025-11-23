//! Time-Weighted Average Price (TWAP) execution algorithm.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{AlgoStatus, ChildOrderAction, ChildOrderRequest, ExecutionAlgorithm};
use tesser_core::{Fill, Order, OrderRequest, OrderStatus, OrderType, Quantity, Signal, Tick};

/// Persistent state for the TWAP algorithm.
#[derive(Debug, Deserialize, Serialize)]
struct TwapState {
    id: Uuid,
    parent_signal: Signal,
    status: String,

    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,

    total_quantity: Quantity,
    filled_quantity: Quantity,

    num_slices: u32,
    executed_slices: u32,

    next_slice_time: DateTime<Utc>,
    slice_interval: Duration,
}

/// TWAP (Time-Weighted Average Price) execution algorithm.
///
/// This algorithm spreads an order over a specified time duration by breaking it
/// into smaller slices executed at regular intervals. This helps minimize market
/// impact and achieve a more representative average price.
pub struct TwapAlgorithm {
    state: TwapState,
}

impl TwapAlgorithm {
    /// Create a new TWAP algorithm instance.
    ///
    /// # Arguments
    /// * `signal` - The parent signal that triggered this algorithm
    /// * `total_quantity` - Total quantity to be executed
    /// * `duration` - Time period over which to spread the execution
    /// * `num_slices` - Number of smaller orders to break the total into
    pub fn new(
        signal: Signal,
        total_quantity: Quantity,
        duration: Duration,
        num_slices: u32,
    ) -> Result<Self> {
        if duration <= Duration::zero() || num_slices == 0 {
            return Err(anyhow!("TWAP duration and slices must be positive"));
        }

        if total_quantity <= Decimal::ZERO {
            return Err(anyhow!("TWAP total quantity must be positive"));
        }

        let now = Utc::now();
        let slice_interval =
            Duration::seconds((duration.num_seconds() as f64 / num_slices as f64).ceil() as i64);

        Ok(Self {
            state: TwapState {
                id: Uuid::new_v4(),
                parent_signal: signal,
                status: "Working".to_string(),
                start_time: now,
                end_time: now + duration,
                total_quantity,
                filled_quantity: Decimal::ZERO,
                num_slices,
                executed_slices: 0,
                next_slice_time: now,
                slice_interval,
            },
        })
    }

    /// Check if we should execute the next slice based on current time.
    fn should_execute_slice(&self) -> bool {
        let now = Utc::now();
        now >= self.state.next_slice_time
            && self.state.executed_slices < self.state.num_slices
            && now < self.state.end_time
    }

    /// Calculate the quantity for the next slice.
    fn calculate_slice_quantity(&self) -> Quantity {
        let remaining_qty = self.state.total_quantity - self.state.filled_quantity;
        if remaining_qty <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        let remaining_slices = self.state.num_slices - self.state.executed_slices;
        if remaining_slices == 0 {
            return Decimal::ZERO;
        }

        let divisor = Decimal::from_u32(remaining_slices).unwrap_or(Decimal::ONE);
        if divisor.is_zero() {
            Decimal::ZERO
        } else {
            remaining_qty / divisor
        }
    }

    /// Generate a child order request for the next slice.
    fn create_slice_order(&self, slice_qty: Quantity) -> ChildOrderRequest {
        ChildOrderRequest {
            parent_algo_id: self.state.id,
            action: ChildOrderAction::Place(OrderRequest {
                symbol: self.state.parent_signal.symbol.clone(),
                side: self.state.parent_signal.kind.side(),
                order_type: OrderType::Market,
                quantity: slice_qty,
                price: None,
                trigger_price: None,
                time_in_force: None,
                client_order_id: Some(format!(
                    "twap-{}-slice-{}",
                    self.state.id,
                    self.state.executed_slices + 1
                )),
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            }),
        }
    }

    /// Check if the algorithm should complete based on time or quantity.
    fn check_completion(&mut self) {
        let now = Utc::now();

        if now >= self.state.end_time {
            self.state.status = "Completed".to_string();
            tracing::info!(
                id = %self.state.id,
                "TWAP completed due to reaching end time"
            );
        } else if self.state.filled_quantity >= self.state.total_quantity {
            self.state.status = "Completed".to_string();
            tracing::info!(
                id = %self.state.id,
                filled = %self.state.filled_quantity,
                total = %self.state.total_quantity,
                "TWAP completed due to reaching total quantity"
            );
        }
    }

    fn slice_from_client_id(&self, client_id: &str) -> Option<u32> {
        let rest = client_id.strip_prefix("twap-")?;
        let (id_part, slice_part) = rest.split_once("-slice-")?;
        if id_part != self.state.id.to_string() {
            return None;
        }
        slice_part.parse().ok()
    }

    fn is_active_status(status: OrderStatus) -> bool {
        matches!(
            status,
            OrderStatus::PendingNew | OrderStatus::Accepted | OrderStatus::PartiallyFilled
        )
    }
}

impl ExecutionAlgorithm for TwapAlgorithm {
    fn kind(&self) -> &'static str {
        "TWAP"
    }

    fn id(&self) -> &Uuid {
        &self.state.id
    }

    fn status(&self) -> AlgoStatus {
        match self.state.status.as_str() {
            "Working" => AlgoStatus::Working,
            "Completed" => AlgoStatus::Completed,
            "Cancelled" => AlgoStatus::Cancelled,
            "Failed" => AlgoStatus::Failed("Generic failure".to_string()),
            _ => AlgoStatus::Failed("Unknown state".to_string()),
        }
    }

    fn start(&mut self) -> Result<Vec<ChildOrderRequest>> {
        // TWAP starts by waiting for the first timer tick, so no initial orders
        tracing::info!(
            id = %self.state.id,
            duration_secs = self.state.end_time.signed_duration_since(self.state.start_time).num_seconds(),
            slices = self.state.num_slices,
            total_qty = %self.state.total_quantity,
            "TWAP algorithm started"
        );
        Ok(vec![])
    }

    fn on_child_order_placed(&mut self, order: &Order) {
        tracing::debug!(
            id = %self.state.id,
            order_id = %order.id,
            qty = %order.request.quantity,
            "TWAP child order placed"
        );
        // For a simple TWAP, we don't need special handling when orders are placed
        // A more sophisticated version could track in-flight orders
    }

    fn on_fill(&mut self, fill: &Fill) -> Result<Vec<ChildOrderRequest>> {
        tracing::debug!(
            id = %self.state.id,
            fill_qty = %fill.fill_quantity,
            fill_price = %fill.fill_price,
            "TWAP received fill"
        );

        self.state.filled_quantity += fill.fill_quantity;
        self.check_completion();

        Ok(vec![])
    }

    fn on_tick(&mut self, _tick: &Tick) -> Result<Vec<ChildOrderRequest>> {
        // TWAP is time-driven, not tick-driven
        Ok(vec![])
    }

    fn on_timer(&mut self) -> Result<Vec<ChildOrderRequest>> {
        // Check if algorithm should complete
        self.check_completion();

        if !matches!(self.status(), AlgoStatus::Working) {
            return Ok(vec![]);
        }

        // Check if we should execute a slice
        if !self.should_execute_slice() {
            return Ok(vec![]);
        }

        let slice_qty = self.calculate_slice_quantity();
        if slice_qty <= Decimal::ZERO {
            return Ok(vec![]);
        }

        // Update state for the new slice
        self.state.executed_slices += 1;
        self.state.next_slice_time = Utc::now() + self.state.slice_interval;

        tracing::debug!(
            id = %self.state.id,
            slice = self.state.executed_slices,
            qty = %slice_qty,
            next_slice_time = %self.state.next_slice_time,
            "Executing TWAP slice"
        );

        let request = self.create_slice_order(slice_qty);
        Ok(vec![request])
    }

    fn cancel(&mut self) -> Result<()> {
        self.state.status = "Cancelled".to_string();
        tracing::info!(id = %self.state.id, "TWAP algorithm cancelled");
        Ok(())
    }

    fn bind_child_order(&mut self, order: Order) -> Result<()> {
        if !Self::is_active_status(order.status) {
            return Ok(());
        }
        let Some(client_id) = order.request.client_order_id.as_deref() else {
            return Ok(());
        };
        let Some(slice_number) = self.slice_from_client_id(client_id) else {
            return Ok(());
        };

        let previous = self.state.executed_slices;
        if slice_number > previous {
            self.state.executed_slices = slice_number;
        }
        self.state.next_slice_time = Utc::now() + self.state.slice_interval;
        if !matches!(self.status(), AlgoStatus::Working) {
            self.state.status = "Working".to_string();
        }

        tracing::info!(
            id = %self.state.id,
            slice = slice_number,
            order_id = %order.id,
            "bound TWAP child order after recovery"
        );
        Ok(())
    }

    fn state(&self) -> serde_json::Value {
        serde_json::to_value(&self.state).expect("Failed to serialize TWAP state")
    }

    fn from_state(state_val: serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        let state: TwapState = serde_json::from_value(state_val)?;
        Ok(Self { state })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tesser_core::SignalKind;

    #[test]
    fn test_twap_creation() {
        let signal = Signal::new("BTCUSDT", SignalKind::EnterLong, 0.8);
        let duration = Duration::minutes(30);
        let twap = TwapAlgorithm::new(signal, Decimal::ONE, duration, 10).unwrap();

        assert_eq!(twap.state.total_quantity, Decimal::ONE);
        assert_eq!(twap.state.num_slices, 10);
        assert_eq!(twap.status(), AlgoStatus::Working);
    }

    #[test]
    fn test_twap_invalid_parameters() {
        let signal = Signal::new("BTCUSDT", SignalKind::EnterLong, 0.8);

        // Zero duration should fail
        let result = TwapAlgorithm::new(signal.clone(), Decimal::ONE, Duration::zero(), 10);
        assert!(result.is_err());

        // Zero slices should fail
        let result = TwapAlgorithm::new(signal, Decimal::ONE, Duration::minutes(30), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_slice_quantity_calculation() {
        let signal = Signal::new("BTCUSDT", SignalKind::EnterLong, 0.8);
        let twap = TwapAlgorithm::new(
            signal,
            Decimal::from_i32(10).unwrap(),
            Duration::minutes(30),
            5,
        )
        .unwrap();

        let slice_qty = twap.calculate_slice_quantity();
        assert_eq!(slice_qty, Decimal::from_i32(2).unwrap());
    }
}
