use anyhow::{bail, Result};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tesser_core::{Order, OrderRequest, OrderType, Signal, Tick};
use uuid::Uuid;

use super::{AlgoStatus, ChildOrderRequest, ExecutionAlgorithm};

const EPS: f64 = 1e-9;

#[derive(Debug, Deserialize, Serialize)]
struct VwapState {
    id: Uuid,
    parent_signal: Signal,
    status: String,
    total_quantity: f64,
    filled_quantity: f64,
    participation_rate: Option<f64>,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    observed_volume: f64,
    min_slice: f64,
    next_child_seq: u32,
}

pub struct VwapAlgorithm {
    state: VwapState,
}

impl VwapAlgorithm {
    pub fn new(
        signal: Signal,
        total_quantity: f64,
        duration: Duration,
        participation_rate: Option<f64>,
    ) -> Result<Self> {
        if total_quantity <= 0.0 {
            bail!("VWAP total quantity must be positive");
        }
        if duration <= Duration::zero() {
            bail!("VWAP duration must be positive");
        }

        let now = Utc::now();
        Ok(Self {
            state: VwapState {
                id: Uuid::new_v4(),
                parent_signal: signal,
                status: "Working".into(),
                total_quantity,
                filled_quantity: 0.0,
                participation_rate,
                start_time: now,
                end_time: now + duration,
                observed_volume: 0.0,
                min_slice: (total_quantity * 0.05).max(0.001),
                next_child_seq: 0,
            },
        })
    }

    fn schedule_target(&self, now: DateTime<Utc>) -> f64 {
        let total_window = self
            .state
            .end_time
            .signed_duration_since(self.state.start_time);
        let elapsed = now.signed_duration_since(self.state.start_time);
        if total_window.num_milliseconds() <= 0 {
            return self.state.total_quantity;
        }
        let progress = (elapsed.num_milliseconds() as f64 / total_window.num_milliseconds() as f64)
            .clamp(0.0, 1.0);
        let mut target = self.state.total_quantity * progress;
        if let Some(rate) = self.state.participation_rate {
            target = target.min(self.state.observed_volume * rate.max(0.0));
        }
        target.min(self.state.total_quantity)
    }

    fn build_market_child(&mut self, quantity: f64) -> ChildOrderRequest {
        self.state.next_child_seq += 1;
        ChildOrderRequest {
            parent_algo_id: self.state.id,
            order_request: OrderRequest {
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
            },
        }
    }

    fn check_completion(&mut self) {
        if self.state.filled_quantity + EPS >= self.state.total_quantity {
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
        self.state.observed_volume += tick.size.max(0.0);
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
        if qty <= EPS {
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
