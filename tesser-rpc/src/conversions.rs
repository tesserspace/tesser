use crate::proto;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::str::FromStr;
use tesser_core::{
    Candle, ExecutionHint, Fill, Interval, OrderBook, OrderBookLevel, Position, Side, Signal,
    SignalKind, StrategyContext, Tick,
};

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

// --- Enums ---

impl From<Side> for i32 {
    fn from(s: Side) -> Self {
        match s {
            Side::Buy => proto::Side::SideBuy as i32,
            Side::Sell => proto::Side::SideSell as i32,
        }
    }
}

impl From<Interval> for i32 {
    fn from(i: Interval) -> Self {
        match i {
            Interval::OneSecond => proto::Interval::Interval1s as i32,
            Interval::OneMinute => proto::Interval::Interval1m as i32,
            Interval::FiveMinutes => proto::Interval::Interval5m as i32,
            Interval::FifteenMinutes => proto::Interval::Interval15m as i32,
            Interval::OneHour => proto::Interval::Interval1h as i32,
            Interval::FourHours => proto::Interval::Interval4h as i32,
            Interval::OneDay => proto::Interval::Interval1d as i32,
        }
    }
}

// --- Structs to Proto ---

impl From<Tick> for proto::Tick {
    fn from(t: Tick) -> Self {
        Self {
            symbol: t.symbol,
            price: Some(to_decimal_proto(t.price)),
            size: Some(to_decimal_proto(t.size)),
            side: Into::<i32>::into(t.side),
            exchange_timestamp: Some(to_timestamp_proto(t.exchange_timestamp)),
            received_at: Some(to_timestamp_proto(t.received_at)),
        }
    }
}

impl From<Candle> for proto::Candle {
    fn from(c: Candle) -> Self {
        Self {
            symbol: c.symbol,
            interval: Into::<i32>::into(c.interval),
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
            side: Into::<i32>::into(f.side),
            fill_price: Some(to_decimal_proto(f.fill_price)),
            fill_quantity: Some(to_decimal_proto(f.fill_quantity)),
            fee: f.fee.map(to_decimal_proto).unwrap_or_else(|| to_decimal_proto(Decimal::ZERO)),
            timestamp: Some(to_timestamp_proto(f.timestamp)),
        }
    }
}

impl From<Position> for proto::Position {
    fn from(p: Position) -> Self {
        Self {
            symbol: p.symbol,
            side: match p.side {
                Some(Side::Buy) => proto::Side::SideBuy as i32,
                Some(Side::Sell) => proto::Side::SideSell as i32,
                None => proto::Side::SideUnspecified as i32,
            },
            quantity: Some(to_decimal_proto(p.quantity)),
            entry_price: p.entry_price.map(to_decimal_proto).unwrap_or_else(|| to_decimal_proto(Decimal::ZERO)),
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
        let kind = match proto::Signal::Kind::try_from(p.kind).unwrap_or(proto::Signal::Kind::KindUnspecified) {
            proto::Signal::Kind::KindEnterLong => SignalKind::EnterLong,
            proto::Signal::Kind::KindExitLong => SignalKind::ExitLong,
            proto::Signal::Kind::KindEnterShort => SignalKind::EnterShort,
            proto::Signal::Kind::KindExitShort => SignalKind::ExitShort,
            proto::Signal::Kind::KindFlatten => SignalKind::Flatten,
            _ => SignalKind::EnterLong, // Default fallback
        };

        let mut signal = Signal::new(p.symbol, kind, p.confidence);
        
        if let Some(sl) = p.stop_loss {
            signal.stop_loss = Some(from_decimal_proto(sl));
        }
        if let Some(tp) = p.take_profit {
            signal.take_profit = Some(from_decimal_proto(tp));
        }
        if let Some(note) = if p.note.is_empty() { None } else { Some(p.note) } {
            signal.note = Some(note);
        }
        
        // TODO: Future expansion for execution hints
        
        signal
    }
}
