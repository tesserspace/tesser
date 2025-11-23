use anyhow::{bail, Result};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use serde::{Deserialize, Serialize};
use tesser_core::{Order, OrderRequest, OrderType, Quantity, Signal, Tick};
use uuid::Uuid;

use super::{AlgoStatus, ChildOrderAction, ChildOrderRequest, ExecutionAlgorithm};

#[derive(Debug, Deserialize, Serialize)]
struct VwapState {
    id: Uuid,
    parent_signal: Signal,
    status: String,
    total_quantity: Quantity,
    filled_quantity: Quantity,
    participation_rate: Option<Decimal>,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    observed_volume: Quantity,
    min_slice: Quantity,
    next_child_seq: u32,
}

pub struct VwapAlgorithm {
    state: VwapState,
}

impl VwapAlgorithm {
    pub fn new(
        signal: Signal,
        total_quantity: Quantity,
        duration: Duration,
        participation_rate: Option<Decimal>,
    ) -> Result<Self> {
        if total_quantity <= Decimal::ZERO {
            bail!("VWAP total quantity must be positive");
        }
        if duration <= Duration::zero() {
            bail!("VWAP duration must be positive");
        }

        let now = Utc::now();
        let min_slice = (total_quantity * Decimal::new(5, 2)).max(Decimal::new(1, 3));
        Ok(Self {
            state: VwapState {
                id: Uuid::new_v4(),
                parent_signal: signal,
                status: "Working".into(),
                total_quantity,
                filled_quantity: Decimal::ZERO,
                participation_rate,
                start_time: now,
                end_time: now + duration,
                observed_volume: Decimal::ZERO,
                min_slice,
                next_child_seq: 0,
            },
        })
    }

    fn schedule_target(&self, now: DateTime<Utc>) -> Quantity {
        let total_window = self
            .state
            .end_time
            .signed_duration_since(self.state.start_time);
        let elapsed = now.signed_duration_since(self.state.start_time);
        if total_window.num_milliseconds() <= 0 {
            return self.state.total_quantity;
        }
        let total_ms = Decimal::from_i64(total_window.num_milliseconds()).unwrap_or(Decimal::ONE);
        let elapsed_ms = Decimal::from_i64(elapsed.num_milliseconds()).unwrap_or(Decimal::ZERO);
        let mut progress = if total_ms.is_zero() {
            Decimal::ONE
        } else {
            elapsed_ms / total_ms
        };
        if progress < Decimal::ZERO {
            progress = Decimal::ZERO;
        } else if progress > Decimal::ONE {
            progress = Decimal::ONE;
        }
        let mut target = self.state.total_quantity * progress;
        if let Some(rate) = self.state.participation_rate {
            let clamped = rate.max(Decimal::ZERO);
            target = target.min(self.state.observed_volume * clamped);
        }
        target.min(self.state.total_quantity)
    }

    fn build_market_child(&mut self, quantity: Quantity) -> ChildOrderRequest {
        self.state.next_child_seq += 1;
        ChildOrderRequest {
            parent_algo_id: self.state.id,
            action: ChildOrderAction::Place(OrderRequest {
                symbol: self.state.parent_signal.symbol.clone(),
                side: self.state.parent_signal.kind.side(),
                order_type: OrderType::Market,
                quantity,
                price: None,
                trigger_price: None,
                time_in_force: None,
                client_order_id: Some(format!(
                    "vwap-{}-{}",
                    self.state.id, self.state.next_child_seq
                )),
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            }),
        }
    }

    fn check_completion(&mut self) {
        if self.state.filled_quantity >= self.state.total_quantity {
            self.state.status = "Completed".into();
            return;
        }
        if Utc::now() >= self.state.end_time {
            self.state.status = "Completed".into();
        }
    }
}

impl ExecutionAlgorithm for VwapAlgorithm {
    fn kind(&self) -> &'static str {
        "VWAP"
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
        self.check_completion();
        Ok(Vec::new())
    }

    fn on_tick(&mut self, tick: &Tick) -> Result<Vec<ChildOrderRequest>> {
        self.state.observed_volume += tick.size.max(Decimal::ZERO);
        if !matches!(self.status(), AlgoStatus::Working) {
            return Ok(Vec::new());
        }
        let now = Utc::now();
        let target = self.schedule_target(now);
        let deficit = target - self.state.filled_quantity;
        if deficit <= self.state.min_slice {
            return Ok(Vec::new());
        }
        let qty = deficit.min(self.state.total_quantity - self.state.filled_quantity);
        if qty <= Decimal::ZERO {
            return Ok(Vec::new());
        }
        Ok(vec![self.build_market_child(qty)])
    }

    fn on_timer(&mut self) -> Result<Vec<ChildOrderRequest>> {
        self.check_completion();
        Ok(Vec::new())
    }

    fn cancel(&mut self) -> Result<()> {
        self.state.status = "Cancelled".into();
        Ok(())
    }

    fn state(&self) -> serde_json::Value {
        serde_json::to_value(&self.state).expect("vwap state serialization failed")
    }

    fn from_state(state: serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        let state: VwapState = serde_json::from_value(state)?;
        Ok(Self { state })
    }
}
