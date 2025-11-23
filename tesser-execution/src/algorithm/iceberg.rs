use anyhow::{bail, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tesser_core::{
    Order, OrderRequest, OrderStatus, OrderType, Price, Quantity, Side, Signal, Tick, TimeInForce,
};
use tracing::info;
use uuid::Uuid;

use super::{AlgoStatus, ChildOrderAction, ChildOrderRequest, ExecutionAlgorithm};

#[derive(Debug, Deserialize, Serialize)]
struct ActiveChild {
    order_id: String,
    remaining: Quantity,
}

#[derive(Debug, Deserialize, Serialize)]
struct IcebergState {
    id: Uuid,
    parent_signal: Signal,
    status: String,
    total_quantity: Quantity,
    filled_quantity: Quantity,
    display_quantity: Quantity,
    limit_price: Price,
    limit_offset_bps: Option<Decimal>,
    next_child_seq: u32,
    active_child: Option<ActiveChild>,
}

pub struct IcebergAlgorithm {
    state: IcebergState,
}

impl IcebergAlgorithm {
    pub fn new(
        signal: Signal,
        total_quantity: Quantity,
        display_quantity: Quantity,
        limit_price: Price,
        limit_offset_bps: Option<Decimal>,
    ) -> Result<Self> {
        if total_quantity <= Decimal::ZERO {
            bail!("iceberg total quantity must be positive");
        }
        if display_quantity <= Decimal::ZERO {
            bail!("iceberg display quantity must be positive");
        }
        if limit_price <= Decimal::ZERO {
            bail!("iceberg limit price must be positive");
        }
        Ok(Self {
            state: IcebergState {
                id: Uuid::new_v4(),
                parent_signal: signal,
                status: "Working".into(),
                total_quantity,
                filled_quantity: Decimal::ZERO,
                display_quantity,
                limit_price,
                limit_offset_bps,
                next_child_seq: 0,
                active_child: None,
            },
        })
    }

    fn remaining_parent(&self) -> Quantity {
        (self.state.total_quantity - self.state.filled_quantity).max(Decimal::ZERO)
    }

    fn build_limit_child(&mut self, quantity: Quantity) -> ChildOrderRequest {
        self.state.next_child_seq += 1;
        let price = self.adjusted_limit_price();
        ChildOrderRequest {
            parent_algo_id: self.state.id,
            action: ChildOrderAction::Place(OrderRequest {
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
            }),
        }
    }

    fn adjusted_limit_price(&self) -> Price {
        let base = self.state.limit_price;
        let offset = self
            .state
            .limit_offset_bps
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::ZERO)
            / Decimal::from(10_000);
        match self.state.parent_signal.kind.side() {
            Side::Buy => base * (Decimal::ONE + offset),
            Side::Sell => base * (Decimal::ONE - offset),
        }
    }

    fn maybe_spawn_slice(&mut self) -> Vec<ChildOrderRequest> {
        if !matches!(self.status(), AlgoStatus::Working) {
            return Vec::new();
        }
        if self.remaining_parent() <= Decimal::ZERO || self.state.active_child.is_some() {
            return Vec::new();
        }
        let qty = self
            .remaining_parent()
            .min(self.state.display_quantity)
            .max(Decimal::ZERO);
        if qty <= Decimal::ZERO {
            return Vec::new();
        }
        vec![self.build_limit_child(qty)]
    }

    fn complete_if_needed(&mut self) {
        if self.remaining_parent() <= Decimal::ZERO {
            self.state.status = "Completed".into();
        }
    }

    fn sequence_from_client_id(&self, client_id: &str) -> Option<u32> {
        let rest = client_id.strip_prefix("iceberg-")?;
        let (id_part, seq_part) = rest.rsplit_once('-')?;
        if id_part != self.state.id.to_string() {
            return None;
        }
        seq_part.parse().ok()
    }

    fn is_active_status(status: OrderStatus) -> bool {
        matches!(
            status,
            OrderStatus::PendingNew | OrderStatus::Accepted | OrderStatus::PartiallyFilled
        )
    }
}

impl ExecutionAlgorithm for IcebergAlgorithm {
    fn kind(&self) -> &'static str {
        "ICEBERG"
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
            if active.remaining <= Decimal::ZERO {
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

    fn bind_child_order(&mut self, order: Order) -> Result<()> {
        if !Self::is_active_status(order.status) {
            return Ok(());
        }
        let Some(client_id) = order.request.client_order_id.as_deref() else {
            return Ok(());
        };
        let Some(seq) = self.sequence_from_client_id(client_id) else {
            return Ok(());
        };

        self.state.next_child_seq = self.state.next_child_seq.max(seq);
        let remaining = (order.request.quantity.abs() - order.filled_quantity).max(Decimal::ZERO);
        self.state.active_child = Some(ActiveChild {
            order_id: order.id.clone(),
            remaining,
        });
        if !matches!(self.status(), AlgoStatus::Working) {
            self.state.status = "Working".into();
        }

        info!(
            id = %self.state.id,
            order_id = %order.id,
            seq = seq,
            remaining = %remaining,
            "bound Iceberg child order after recovery"
        );
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
