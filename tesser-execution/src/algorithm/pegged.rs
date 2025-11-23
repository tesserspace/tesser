use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tesser_core::{
    Order, OrderId, OrderRequest, OrderStatus, OrderType, OrderUpdateRequest, Quantity, Signal,
    Tick, TimeInForce,
};
use uuid::Uuid;

use super::{AlgoStatus, ChildOrderAction, ChildOrderRequest, ExecutionAlgorithm};

#[derive(Debug, Deserialize, Serialize)]
struct PeggedState {
    id: Uuid,
    parent_signal: Signal,
    status: String,
    total_quantity: Quantity,
    filled_quantity: Quantity,
    clip_size: Quantity,
    offset_bps: Decimal,
    refresh: Duration,
    last_order_time: Option<DateTime<Utc>>,
    last_peg_price: Option<Decimal>,
    next_child_seq: u32,
    #[serde(default)]
    min_chase_distance: Decimal,
    #[serde(default)]
    active_child: Option<ActiveChildOrder>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ActiveChildOrder {
    order_id: OrderId,
    client_order_id: Option<String>,
    total_quantity: Quantity,
    filled_quantity: Quantity,
    working_price: Option<Decimal>,
}

impl ActiveChildOrder {
    fn remaining(&self) -> Quantity {
        (self.total_quantity - self.filled_quantity).max(Decimal::ZERO)
    }
}

/// Algorithm that repeatedly submits IOC limit orders pegged to the top of book.
pub struct PeggedBestAlgorithm {
    state: PeggedState,
}

impl PeggedBestAlgorithm {
    pub fn new(
        signal: Signal,
        total_quantity: Quantity,
        offset_bps: Decimal,
        clip_size: Option<Quantity>,
        refresh: Duration,
        min_chase_distance: Option<Decimal>,
    ) -> Result<Self> {
        if total_quantity <= Decimal::ZERO {
            return Err(anyhow!("pegged algorithm requires positive quantity"));
        }
        if offset_bps < Decimal::ZERO {
            return Err(anyhow!("offset must be non-negative"));
        }
        if refresh <= Duration::zero() {
            return Err(anyhow!("refresh interval must be positive"));
        }
        let clip = clip_size
            .unwrap_or(total_quantity)
            .max(Decimal::ZERO)
            .min(total_quantity);
        Ok(Self {
            state: PeggedState {
                id: Uuid::new_v4(),
                parent_signal: signal,
                status: "Working".into(),
                total_quantity,
                filled_quantity: Decimal::ZERO,
                clip_size: if clip <= Decimal::ZERO {
                    total_quantity
                } else {
                    clip
                },
                offset_bps,
                refresh,
                last_order_time: None,
                last_peg_price: None,
                next_child_seq: 0,
                min_chase_distance: min_chase_distance
                    .unwrap_or(Decimal::ZERO)
                    .max(Decimal::ZERO),
                active_child: None,
            },
        })
    }

    fn remaining(&self) -> Quantity {
        (self.state.total_quantity - self.state.filled_quantity).max(Decimal::ZERO)
    }

    fn peg_price(&self, tick_price: Decimal) -> Decimal {
        let offset = self.state.offset_bps / Decimal::from(10_000);
        match self.state.parent_signal.kind.side() {
            tesser_core::Side::Buy => tick_price * (Decimal::ONE - offset),
            tesser_core::Side::Sell => tick_price * (Decimal::ONE + offset),
        }
    }

    fn should_refresh(&self, price: Decimal, now: DateTime<Utc>) -> bool {
        if self.last_emit_elapsed(now) >= self.state.refresh {
            return true;
        }
        self.state
            .last_peg_price
            .map(|prev| (prev - price).abs() > self.state.min_chase_distance)
            .unwrap_or(true)
    }

    fn should_chase(&self, price: Decimal, now: DateTime<Utc>) -> bool {
        if self.last_emit_elapsed(now) < self.state.refresh {
            return false;
        }
        if let Some(prev) = self.state.last_peg_price {
            if (prev - price).abs() < self.state.min_chase_distance {
                return false;
            }
        }
        true
    }

    fn last_emit_elapsed(&self, now: DateTime<Utc>) -> Duration {
        self.state
            .last_order_time
            .map(|ts| now.signed_duration_since(ts))
            .unwrap_or(self.state.refresh)
    }

    fn build_child(&mut self, price: Decimal, qty: Quantity) -> ChildOrderRequest {
        self.state.next_child_seq += 1;
        ChildOrderRequest {
            parent_algo_id: self.state.id,
            action: ChildOrderAction::Place(OrderRequest {
                symbol: self.state.parent_signal.symbol.clone(),
                side: self.state.parent_signal.kind.side(),
                order_type: OrderType::Limit,
                quantity: qty,
                price: Some(price),
                trigger_price: None,
                time_in_force: Some(TimeInForce::GoodTilCanceled),
                client_order_id: Some(format!(
                    "peg-{}-{}",
                    self.state.id, self.state.next_child_seq
                )),
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            }),
        }
    }

    fn mark_completed(&mut self) {
        if self.remaining() <= Decimal::ZERO {
            self.state.status = "Completed".into();
        }
    }

    fn sequence_from_client_id(&self, client_id: &str) -> Option<u32> {
        let rest = client_id.strip_prefix("peg-")?;
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

impl ExecutionAlgorithm for PeggedBestAlgorithm {
    fn kind(&self) -> &'static str {
        "PEGGED_BEST"
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

    fn on_child_order_placed(&mut self, order: &Order) {
        self.state.last_order_time = Some(Utc::now());
        if let Some(price) = order.request.price {
            self.state.last_peg_price = Some(price);
        }
        self.state.active_child = Some(ActiveChildOrder {
            order_id: order.id.clone(),
            client_order_id: order.request.client_order_id.clone(),
            total_quantity: order.request.quantity,
            filled_quantity: order.filled_quantity,
            working_price: order.request.price,
        });
    }

    fn on_fill(&mut self, fill: &tesser_core::Fill) -> Result<Vec<ChildOrderRequest>> {
        self.state.filled_quantity += fill.fill_quantity;
        if let Some(active) = self.state.active_child.as_mut() {
            if active.order_id == fill.order_id {
                active.filled_quantity += fill.fill_quantity;
                if active.remaining() <= Decimal::ZERO {
                    self.state.active_child = None;
                }
            }
        }
        self.mark_completed();
        Ok(Vec::new())
    }

    fn on_tick(&mut self, tick: &Tick) -> Result<Vec<ChildOrderRequest>> {
        if !matches!(self.status(), AlgoStatus::Working) {
            return Ok(Vec::new());
        }
        let remaining = self.remaining();
        if remaining <= Decimal::ZERO {
            self.state.status = "Completed".into();
            return Ok(Vec::new());
        }
        let now = Utc::now();
        let price = self.peg_price(tick.price.max(Decimal::ZERO));
        if let Some(active) = &self.state.active_child {
            if !self.should_chase(price, now) {
                return Ok(Vec::new());
            }
            if active.remaining() <= Decimal::ZERO {
                return Ok(Vec::new());
            }
            self.state.last_peg_price = Some(price);
            self.state.last_order_time = Some(now);
            let request = OrderUpdateRequest {
                order_id: active.order_id.clone(),
                symbol: self.state.parent_signal.symbol.clone(),
                side: self.state.parent_signal.kind.side(),
                new_price: Some(price),
                new_quantity: Some(active.total_quantity),
            };
            return Ok(vec![ChildOrderRequest {
                parent_algo_id: self.state.id,
                action: ChildOrderAction::Amend(request),
            }]);
        }
        if !self.should_refresh(price, now) {
            return Ok(Vec::new());
        }
        self.state.last_peg_price = Some(price);
        let qty = remaining.min(self.state.clip_size);
        if qty <= Decimal::ZERO {
            return Ok(Vec::new());
        }
        Ok(vec![self.build_child(price, qty)])
    }

    fn on_timer(&mut self) -> Result<Vec<ChildOrderRequest>> {
        self.mark_completed();
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
        if let Some(seq) = self.sequence_from_client_id(client_id) {
            self.state.next_child_seq = self.state.next_child_seq.max(seq);
        }
        self.state.last_order_time = Some(order.updated_at);
        if let Some(price) = order.request.price {
            self.state.last_peg_price = Some(price);
        }
        self.state.active_child = Some(ActiveChildOrder {
            order_id: order.id.clone(),
            client_order_id: order.request.client_order_id.clone(),
            total_quantity: order.request.quantity,
            filled_quantity: order.filled_quantity,
            working_price: order.request.price,
        });
        if !matches!(self.status(), AlgoStatus::Working) {
            self.state.status = "Working".into();
        }
        Ok(())
    }

    fn state(&self) -> serde_json::Value {
        serde_json::to_value(&self.state).expect("pegged state serialization failed")
    }

    fn from_state(state: serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        let state: PeggedState = serde_json::from_value(state)?;
        Ok(Self { state })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tesser_core::{Order, OrderStatus, Signal, SignalKind, Tick};

    #[test]
    fn emits_child_after_tick() {
        let signal = Signal::new("BTCUSDT", SignalKind::EnterLong, 0.9);
        let mut algo = PeggedBestAlgorithm::new(
            signal,
            Decimal::from(5),
            Decimal::new(5, 1),
            None,
            Duration::seconds(1),
            Some(Decimal::new(1, 2)),
        )
        .unwrap();
        let tick = Tick {
            symbol: "BTCUSDT".into(),
            price: Decimal::from(100),
            size: Decimal::ONE,
            side: tesser_core::Side::Buy,
            exchange_timestamp: Utc::now(),
            received_at: Utc::now(),
        };
        let orders = algo.on_tick(&tick).unwrap();
        assert_eq!(orders.len(), 1);
        match &orders[0].action {
            ChildOrderAction::Place(request) => assert!(request.quantity > Decimal::ZERO),
            other => panic!("unexpected action: {other:?}"),
        }
    }

    #[test]
    fn chases_active_order_with_amend() {
        let signal = Signal::new("BTCUSDT", SignalKind::EnterLong, 0.9);
        let mut algo = PeggedBestAlgorithm::new(
            signal,
            Decimal::from(2),
            Decimal::new(1, 1),
            None,
            Duration::seconds(1),
            Some(Decimal::from(5)),
        )
        .unwrap();
        let first_tick = Tick {
            symbol: "BTCUSDT".into(),
            price: Decimal::from(30_000),
            size: Decimal::ONE,
            side: tesser_core::Side::Buy,
            exchange_timestamp: Utc::now(),
            received_at: Utc::now(),
        };
        let child = algo.on_tick(&first_tick).unwrap();
        let request = match &child[0].action {
            ChildOrderAction::Place(req) => req.clone(),
            other => panic!("expected placement, got {other:?}"),
        };
        let order = Order {
            id: "child-order".into(),
            request: request.clone(),
            status: OrderStatus::Accepted,
            filled_quantity: Decimal::ZERO,
            avg_fill_price: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        algo.on_child_order_placed(&order);
        algo.state.last_order_time = Some(Utc::now() - Duration::seconds(5));

        let chase_tick = Tick {
            price: Decimal::from(30_020),
            ..first_tick
        };
        let chase = algo.on_tick(&chase_tick).unwrap();
        assert_eq!(chase.len(), 1);
        match &chase[0].action {
            ChildOrderAction::Amend(update) => {
                assert_eq!(update.order_id, order.id);
                assert_eq!(update.symbol, order.request.symbol);
                assert_eq!(update.new_quantity, Some(order.request.quantity));
                assert!(update.new_price.unwrap() > request.price.unwrap());
            }
            other => panic!("expected amend action, got {other:?}"),
        }
    }
}
