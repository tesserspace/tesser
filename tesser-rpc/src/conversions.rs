use crate::proto;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::{json, Map};
use std::str::FromStr;
use tesser_core::{
    Candle, Cash, ExecutionHint, Fill, Interval, Order, OrderBook, OrderBookLevel, OrderStatus,
    OrderType, Position, Side, Signal, SignalKind, Tick,
};
use tesser_portfolio::{Portfolio, PortfolioState};
use tesser_strategy::StrategyContext;
use uuid::Uuid;

// --- Helpers ---

pub fn to_decimal_proto(d: Decimal) -> proto::Decimal {
    proto::Decimal {
        value: d.to_string(),
    }
}

pub fn from_decimal_proto(d: proto::Decimal) -> Decimal {
    Decimal::from_str(&d.value).unwrap_or(Decimal::ZERO)
}

pub fn to_timestamp_proto(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

pub fn from_timestamp_proto(ts: prost_types::Timestamp) -> DateTime<Utc> {
    let secs = ts.seconds;
    let nanos = ts.nanos.clamp(0, 999_999_999);
    DateTime::<Utc>::from_timestamp(secs, nanos as u32)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap())
}

// --- Enums ---

fn side_to_proto(s: Side) -> proto::Side {
    match s {
        Side::Buy => proto::Side::Buy,
        Side::Sell => proto::Side::Sell,
    }
}

fn interval_to_proto(i: Interval) -> proto::Interval {
    match i {
        Interval::OneSecond => proto::Interval::Interval1s,
        Interval::OneMinute => proto::Interval::Interval1m,
        Interval::FiveMinutes => proto::Interval::Interval5m,
        Interval::FifteenMinutes => proto::Interval::Interval15m,
        Interval::OneHour => proto::Interval::Interval1h,
        Interval::FourHours => proto::Interval::Interval4h,
        Interval::OneDay => proto::Interval::Interval1d,
    }
}

fn order_type_to_proto(order_type: OrderType) -> proto::OrderType {
    match order_type {
        OrderType::Market => proto::OrderType::Market,
        OrderType::Limit => proto::OrderType::Limit,
        OrderType::StopMarket => proto::OrderType::StopMarket,
    }
}

fn order_status_to_proto(status: OrderStatus) -> proto::OrderStatus {
    match status {
        OrderStatus::PendingNew => proto::OrderStatus::PendingNew,
        OrderStatus::Accepted => proto::OrderStatus::Accepted,
        OrderStatus::PartiallyFilled => proto::OrderStatus::PartiallyFilled,
        OrderStatus::Filled => proto::OrderStatus::Filled,
        OrderStatus::Canceled => proto::OrderStatus::Canceled,
        OrderStatus::Rejected => proto::OrderStatus::Rejected,
    }
}

// --- Structs to Proto ---

impl From<Tick> for proto::Tick {
    fn from(t: Tick) -> Self {
        Self {
            symbol: t.symbol,
            price: Some(to_decimal_proto(t.price)),
            size: Some(to_decimal_proto(t.size)),
            side: side_to_proto(t.side) as i32,
            exchange_timestamp: Some(to_timestamp_proto(t.exchange_timestamp)),
            received_at: Some(to_timestamp_proto(t.received_at)),
        }
    }
}

impl From<Candle> for proto::Candle {
    fn from(c: Candle) -> Self {
        Self {
            symbol: c.symbol,
            interval: interval_to_proto(c.interval) as i32,
            open: Some(to_decimal_proto(c.open)),
            high: Some(to_decimal_proto(c.high)),
            low: Some(to_decimal_proto(c.low)),
            close: Some(to_decimal_proto(c.close)),
            volume: Some(to_decimal_proto(c.volume)),
            timestamp: Some(to_timestamp_proto(c.timestamp)),
        }
    }
}

impl From<OrderBook> for proto::OrderBook {
    fn from(b: OrderBook) -> Self {
        Self {
            symbol: b.symbol,
            bids: b.bids.into_iter().map(Into::into).collect(),
            asks: b.asks.into_iter().map(Into::into).collect(),
            timestamp: Some(to_timestamp_proto(b.timestamp)),
            exchange_checksum: b.exchange_checksum,
            local_checksum: b.local_checksum,
        }
    }
}

impl From<OrderBookLevel> for proto::OrderBookLevel {
    fn from(l: OrderBookLevel) -> Self {
        Self {
            price: Some(to_decimal_proto(l.price)),
            size: Some(to_decimal_proto(l.size)),
        }
    }
}

impl From<Fill> for proto::Fill {
    fn from(f: Fill) -> Self {
        Self {
            order_id: f.order_id,
            symbol: f.symbol,
            side: side_to_proto(f.side) as i32,
            fill_price: Some(to_decimal_proto(f.fill_price)),
            fill_quantity: Some(to_decimal_proto(f.fill_quantity)),
            fee: Some(
                f.fee
                    .map(to_decimal_proto)
                    .unwrap_or_else(|| to_decimal_proto(Decimal::ZERO)),
            ),
            timestamp: Some(to_timestamp_proto(f.timestamp)),
        }
    }
}

impl From<&Order> for proto::OrderSnapshot {
    fn from(order: &Order) -> Self {
        Self {
            id: order.id.clone(),
            symbol: order.request.symbol.clone(),
            side: side_to_proto(order.request.side) as i32,
            order_type: order_type_to_proto(order.request.order_type) as i32,
            quantity: Some(to_decimal_proto(order.request.quantity)),
            filled_quantity: Some(to_decimal_proto(order.filled_quantity)),
            avg_fill_price: order.avg_fill_price.map(to_decimal_proto),
            status: order_status_to_proto(order.status) as i32,
            created_at: Some(to_timestamp_proto(order.created_at)),
            updated_at: Some(to_timestamp_proto(order.updated_at)),
        }
    }
}

impl From<Order> for proto::OrderSnapshot {
    fn from(order: Order) -> Self {
        (&order).into()
    }
}

impl From<&Portfolio> for proto::PortfolioSnapshot {
    fn from(portfolio: &Portfolio) -> Self {
        let snapshot = portfolio.snapshot();
        let equity = portfolio.equity();
        let realized = portfolio.realized_pnl();
        let mut proto: proto::PortfolioSnapshot = snapshot.into();
        proto.equity = Some(to_decimal_proto(equity));
        proto.realized_pnl = Some(to_decimal_proto(realized));
        proto
    }
}

impl From<PortfolioState> for proto::PortfolioSnapshot {
    fn from(state: PortfolioState) -> Self {
        portfolio_state_to_proto(&state)
    }
}

impl From<&PortfolioState> for proto::PortfolioSnapshot {
    fn from(state: &PortfolioState) -> Self {
        portfolio_state_to_proto(state)
    }
}

impl From<Signal> for proto::Signal {
    fn from(signal: Signal) -> Self {
        let metadata = signal_metadata(&signal).unwrap_or_default();
        Self {
            symbol: signal.symbol,
            kind: signal_kind_to_proto(signal.kind) as i32,
            confidence: signal.confidence,
            stop_loss: signal.stop_loss.map(to_decimal_proto),
            take_profit: signal.take_profit.map(to_decimal_proto),
            execution_hint: None,
            note: signal.note.unwrap_or_default(),
            id: signal.id.to_string(),
            generated_at: Some(to_timestamp_proto(signal.generated_at)),
            metadata,
        }
    }
}

fn signal_kind_to_proto(kind: SignalKind) -> proto::signal::Kind {
    match kind {
        SignalKind::EnterLong => proto::signal::Kind::EnterLong,
        SignalKind::ExitLong => proto::signal::Kind::ExitLong,
        SignalKind::EnterShort => proto::signal::Kind::EnterShort,
        SignalKind::ExitShort => proto::signal::Kind::ExitShort,
        SignalKind::Flatten => proto::signal::Kind::Flatten,
    }
}

fn portfolio_state_to_proto(state: &PortfolioState) -> proto::PortfolioSnapshot {
    let unrealized: Decimal = state.positions.values().map(|pos| pos.unrealized_pnl).sum();
    let cash_value = state.balances.total_value();
    let equity = cash_value + unrealized;
    let realized = equity - state.initial_equity - unrealized;

    proto::PortfolioSnapshot {
        balances: state
            .balances
            .iter()
            .map(|(currency, cash)| cash_to_proto(currency, cash))
            .collect(),
        positions: state.positions.values().cloned().map(Into::into).collect(),
        equity: Some(to_decimal_proto(equity)),
        initial_equity: Some(to_decimal_proto(state.initial_equity)),
        realized_pnl: Some(to_decimal_proto(realized)),
        reporting_currency: state.reporting_currency.clone(),
        liquidate_only: state.liquidate_only,
    }
}

fn cash_to_proto(currency: &str, cash: &Cash) -> proto::CashBalance {
    proto::CashBalance {
        currency: currency.to_string(),
        quantity: Some(to_decimal_proto(cash.quantity)),
        conversion_rate: Some(to_decimal_proto(cash.conversion_rate)),
    }
}

impl From<Position> for proto::Position {
    fn from(p: Position) -> Self {
        Self {
            symbol: p.symbol,
            side: match p.side {
                Some(Side::Buy) => proto::Side::Buy as i32,
                Some(Side::Sell) => proto::Side::Sell as i32,
                None => proto::Side::Unspecified as i32,
            },
            quantity: Some(to_decimal_proto(p.quantity)),
            entry_price: Some(
                p.entry_price
                    .map(to_decimal_proto)
                    .unwrap_or_else(|| to_decimal_proto(Decimal::ZERO)),
            ),
            unrealized_pnl: Some(to_decimal_proto(p.unrealized_pnl)),
            updated_at: Some(to_timestamp_proto(p.updated_at)),
        }
    }
}

impl<'a> From<&'a StrategyContext> for proto::StrategyContext {
    fn from(ctx: &'a StrategyContext) -> Self {
        Self {
            positions: ctx.positions().iter().cloned().map(Into::into).collect(),
        }
    }
}

// --- Proto to Structs ---

impl From<proto::Signal> for Signal {
    fn from(p: proto::Signal) -> Self {
        let kind = match proto::signal::Kind::try_from(p.kind)
            .unwrap_or(proto::signal::Kind::Unspecified)
        {
            proto::signal::Kind::EnterLong => SignalKind::EnterLong,
            proto::signal::Kind::ExitLong => SignalKind::ExitLong,
            proto::signal::Kind::EnterShort => SignalKind::EnterShort,
            proto::signal::Kind::ExitShort => SignalKind::ExitShort,
            proto::signal::Kind::Flatten => SignalKind::Flatten,
            _ => SignalKind::EnterLong, // Default fallback
        };

        let mut signal = Signal::new(p.symbol, kind, p.confidence);

        if let Some(sl) = p.stop_loss {
            signal.stop_loss = Some(from_decimal_proto(sl));
        }
        if let Some(tp) = p.take_profit {
            signal.take_profit = Some(from_decimal_proto(tp));
        }
        if let Some(note) = if p.note.is_empty() {
            None
        } else {
            Some(p.note)
        } {
            signal.note = Some(note);
        }

        if let Some(ts) = p.generated_at {
            signal.generated_at = from_timestamp_proto(ts);
        }
        if !p.id.is_empty() {
            if let Ok(uuid) = Uuid::parse_str(&p.id) {
                signal.id = uuid;
            }
        }

        signal
    }
}

fn signal_metadata(signal: &Signal) -> Option<String> {
    let note = signal.note.as_deref();
    let hint = signal.execution_hint.as_ref().map(execution_hint_metadata);
    if note.is_none() && hint.is_none() {
        return None;
    }
    let mut payload = Map::new();
    if let Some(note) = note {
        payload.insert("note".into(), json!(note));
    }
    if let Some(hint) = hint {
        payload.insert("execution_hint".into(), hint);
    }
    serde_json::to_string(&serde_json::Value::Object(payload)).ok()
}

fn execution_hint_metadata(hint: &ExecutionHint) -> serde_json::Value {
    match hint {
        ExecutionHint::Twap { duration } => json!({
            "type": "twap",
            "duration_ms": duration.num_milliseconds(),
        }),
        ExecutionHint::Vwap {
            duration,
            participation_rate,
        } => json!({
            "type": "vwap",
            "duration_ms": duration.num_milliseconds(),
            "participation_rate": participation_rate.as_ref().map(|d| d.to_string()),
        }),
        ExecutionHint::IcebergSimulated {
            display_size,
            limit_offset_bps,
        } => json!({
            "type": "iceberg",
            "display_size": display_size.to_string(),
            "limit_offset_bps": limit_offset_bps.as_ref().map(|d| d.to_string()),
        }),
        ExecutionHint::PeggedBest {
            offset_bps,
            clip_size,
            refresh_secs,
        } => json!({
            "type": "pegged_best",
            "offset_bps": offset_bps.to_string(),
            "clip_size": clip_size.as_ref().map(|d| d.to_string()),
            "refresh_secs": refresh_secs,
        }),
        ExecutionHint::Sniper {
            trigger_price,
            timeout,
        } => json!({
            "type": "sniper",
            "trigger_price": trigger_price.to_string(),
            "timeout_ms": timeout.map(|d| d.num_milliseconds()),
        }),
    }
}
