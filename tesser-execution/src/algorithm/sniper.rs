use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tesser_core::{Order, OrderRequest, OrderType, Price, Quantity, Signal, SignalKind, Tick};
use uuid::Uuid;

use super::{AlgoStatus, ChildOrderAction, ChildOrderRequest, ExecutionAlgorithm};

#[derive(Debug, Deserialize, Serialize)]
struct SniperState {
    id: Uuid,
    parent_signal: Signal,
    status: String,
    total_quantity: Quantity,
    filled_quantity: Quantity,
    trigger_price: Price,
    timeout: Option<Duration>,
    triggered: bool,
    started_at: chrono::DateTime<Utc>,
}

/// Fires a single aggressive order once a target price is touched.
pub struct SniperAlgorithm {
    state: SniperState,
}

impl SniperAlgorithm {
    pub fn new(
        signal: Signal,
        total_quantity: Quantity,
        trigger_price: Price,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        if total_quantity <= Decimal::ZERO {
            return Err(anyhow!("sniper quantity must be positive"));
        }
        if trigger_price <= Decimal::ZERO {
            return Err(anyhow!("trigger price must be positive"));
        }
        Ok(Self {
            state: SniperState {
                id: Uuid::new_v4(),
                parent_signal: signal,
                status: "Working".into(),
                total_quantity,
                filled_quantity: Decimal::ZERO,
                trigger_price,
                timeout,
                triggered: false,
                started_at: Utc::now(),
            },
        })
    }

    fn remaining(&self) -> Quantity {
        (self.state.total_quantity - self.state.filled_quantity).max(Decimal::ZERO)
    }

    fn ready_to_fire(&self, price: Price) -> bool {
        match self.state.parent_signal.kind {
            SignalKind::EnterLong | SignalKind::ExitShort => price <= self.state.trigger_price,
            SignalKind::EnterShort | SignalKind::ExitLong => price >= self.state.trigger_price,
            SignalKind::Flatten => false,
        }
    }

    fn build_child(&mut self, qty: Quantity) -> ChildOrderRequest {
        ChildOrderRequest {
            parent_algo_id: self.state.id,
            action: ChildOrderAction::Place(OrderRequest {
                symbol: self.state.parent_signal.symbol.clone(),
                side: self.state.parent_signal.kind.side(),
                order_type: OrderType::Market,
                quantity: qty,
                price: None,
                trigger_price: None,
                time_in_force: None,
                client_order_id: Some(format!("sniper-{}", self.state.id)),
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            }),
        }
    }

    fn check_expired(&mut self) {
        if let Some(timeout) = self.state.timeout {
            if Utc::now() - self.state.started_at >= timeout {
                self.state.status = "Cancelled".into();
            }
        }
    }
}

impl ExecutionAlgorithm for SniperAlgorithm {
    fn kind(&self) -> &'static str {
        "SNIPER"
    }

    fn id(&self) -> &Uuid {
        &self.state.id
    }

    fn status(&self) -> AlgoStatus {
        match self.state.status.as_str() {
            "Working" => AlgoStatus::Working,
            "Completed" => AlgoStatus::Completed,
            "Cancelled" => AlgoStatus::Cancelled,
            other => AlgoStatus::Failed(other.to_string()),
        }
    }

    fn start(&mut self) -> Result<Vec<ChildOrderRequest>> {
        Ok(Vec::new())
    }

    fn on_child_order_placed(&mut self, _order: &Order) {}

    fn on_fill(&mut self, fill: &tesser_core::Fill) -> Result<Vec<ChildOrderRequest>> {
        self.state.filled_quantity += fill.fill_quantity;
        if self.remaining() <= Decimal::ZERO {
            self.state.status = "Completed".into();
        }
        Ok(Vec::new())
    }

    fn on_tick(&mut self, tick: &Tick) -> Result<Vec<ChildOrderRequest>> {
        if !matches!(self.status(), AlgoStatus::Working) {
            return Ok(Vec::new());
        }
        if self.state.triggered {
            return Ok(Vec::new());
        }
        if self.ready_to_fire(tick.price) {
            self.state.triggered = true;
            let qty = self.remaining();
            if qty > Decimal::ZERO {
                return Ok(vec![self.build_child(qty)]);
            }
        }
        Ok(Vec::new())
    }

    fn on_timer(&mut self) -> Result<Vec<ChildOrderRequest>> {
        self.check_expired();
        Ok(Vec::new())
    }

    fn cancel(&mut self) -> Result<()> {
        self.state.status = "Cancelled".into();
        Ok(())
    }

    fn state(&self) -> serde_json::Value {
        serde_json::to_value(&self.state).expect("sniper state serialization failed")
    }

    fn from_state(state: serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        let state: SniperState = serde_json::from_value(state)?;
        Ok(Self { state })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tesser_core::{Signal, SignalKind, Tick};

    #[test]
    fn sniper_triggers_when_price_crosses() {
        let signal = Signal::new("BTCUSDT", SignalKind::EnterLong, 0.5);
        let mut algo =
            SniperAlgorithm::new(signal, Decimal::from(2), Decimal::from(100), None).unwrap();
        let tick = Tick {
            symbol: "BTCUSDT".into(),
            price: Decimal::from(95),
            size: Decimal::ONE,
            side: tesser_core::Side::Sell,
            exchange_timestamp: Utc::now(),
            received_at: Utc::now(),
        };
        let orders = algo.on_tick(&tick).unwrap();
        assert_eq!(orders.len(), 1);
        match &orders[0].action {
            ChildOrderAction::Place(request) => {
                assert_eq!(request.order_type, OrderType::Market);
            }
            other => panic!("unexpected action: {other:?}"),
        }
    }
}
