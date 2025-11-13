use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use tesser_core::{Order, OrderRequest, OrderType, Signal, Side, Tick, TimeInForce};
use uuid::Uuid;

use super::{AlgoStatus, ChildOrderRequest, ExecutionAlgorithm};

const EPS: f64 = 1e-9;

#[derive(Debug, Deserialize, Serialize)]
struct ActiveChild {
    order_id: String,
    remaining: f64,
}

#[derive(Debug, Deserialize, Serialize)]
struct IcebergState {
    id: Uuid,
    parent_signal: Signal,
    status: String,
    total_quantity: f64,
    filled_quantity: f64,
    display_quantity: f64,
    limit_price: f64,
    limit_offset_bps: Option<f64>,
    next_child_seq: u32,
    active_child: Option<ActiveChild>,
}

pub struct IcebergAlgorithm {
    state: IcebergState,
}

impl IcebergAlgorithm {
    pub fn new(
        signal: Signal,
        total_quantity: f64,
        display_quantity: f64,
        limit_price: f64,
        limit_offset_bps: Option<f64>,
    ) -> Result<Self> {
        if total_quantity <= 0.0 {
            bail!("iceberg total quantity must be positive");
        }
        if display_quantity <= 0.0 {
            bail!("iceberg display quantity must be positive");
        }
        if limit_price <= 0.0 {
            bail!("iceberg limit price must be positive");
        }
        Ok(Self {
            state: IcebergState {
                id: Uuid::new_v4(),
                parent_signal: signal,
                status: "Working".into(),
                total_quantity,
                filled_quantity: 0.0,
                display_quantity,
                limit_price,
                limit_offset_bps,
                next_child_seq: 0,
                active_child: None,
            },
        })
    }

    fn remaining_parent(&self) -> f64 {
        (self.state.total_quantity - self.state.filled_quantity).max(0.0)
    }

    fn build_limit_child(&mut self, quantity: f64) -> ChildOrderRequest {
        self.state.next_child_seq += 1;
        let price = self.adjusted_limit_price();
        ChildOrderRequest {
            parent_algo_id: self.state.id,
            order_request: OrderRequest {
                symbol: self.state.parent_signal.symbol.clone(),
                side: self.state.parent_signal.kind.side(),
                order_type: OrderType::Limit,
                quantity,
                price: Some(price),
                trigger_price: None,
                time_in_force: Some(TimeInForce::GoodTilCanceled),
                client_order_id: Some(format!(
                    "iceberg-{}-{}",
                    self.state.id, self.state.next_child_seq
                )),
                take_profit: None,
                stop_loss: None,
                display_quantity: Some(self.state.display_quantity.min(quantity)),
            },
        }
    }

    fn adjusted_limit_price(&self) -> f64 {
        let base = self.state.limit_price;
        let offset = self.state.limit_offset_bps.unwrap_or(0.0).max(0.0) / 10_000.0;
        match self.state.parent_signal.kind.side() {
            Side::Buy => base * (1.0 + offset),
            Side::Sell => base * (1.0 - offset),
        }
    }

    fn maybe_spawn_slice(&mut self) -> Vec<ChildOrderRequest> {
        if !matches!(self.status(), AlgoStatus::Working) {
            return Vec::new();
        }
        if self.remaining_parent() <= EPS || self.state.active_child.is_some() {
            return Vec::new();
        }
        let qty = self
            .remaining_parent()
            .min(self.state.display_quantity)
            .max(0.0);
        if qty <= EPS {
            return Vec::new();
        }
        vec![self.build_limit_child(qty)]
    }

    fn complete_if_needed(&mut self) {
        if self.remaining_parent() <= EPS {
            self.state.status = "Completed".into();
        }
    }
}

impl ExecutionAlgorithm for IcebergAlgorithm {
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
        Ok(self.maybe_spawn_slice())
    }

    fn on_child_order_placed(&mut self, order: &Order) {
        self.state.active_child = Some(ActiveChild {
            order_id: order.id.clone(),
            remaining: order.request.quantity.abs(),
        });
    }

    fn on_fill(&mut self, fill: &tesser_core::Fill) -> Result<Vec<ChildOrderRequest>> {
        self.state.filled_quantity += fill.fill_quantity;
        if let Some(active) = self.state.active_child.as_mut() {
            if active.order_id == fill.order_id {
                active.remaining -= fill.fill_quantity;
            }
            if active.remaining <= EPS {
                self.state.active_child = None;
            }
        }
        self.complete_if_needed();
        Ok(self.maybe_spawn_slice())
    }

    fn on_tick(&mut self, _tick: &Tick) -> Result<Vec<ChildOrderRequest>> {
        Ok(Vec::new())
    }

    fn on_timer(&mut self) -> Result<Vec<ChildOrderRequest>> {
        self.complete_if_needed();
        Ok(Vec::new())
    }

    fn cancel(&mut self) -> Result<()> {
        self.state.status = "Cancelled".into();
        Ok(())
    }

    fn state(&self) -> serde_json::Value {
        serde_json::to_value(&self.state).expect("iceberg state serialization failed")
    }

    fn from_state(state: serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        let state: IcebergState = serde_json::from_value(state)?;
        Ok(Self { state })
    }
}
