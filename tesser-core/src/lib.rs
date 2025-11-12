//! Fundamental data types shared across the entire workspace.

use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Alias for price precision.
pub type Price = f64;
/// Alias for quantity precision.
pub type Quantity = f64;
/// Alias used for human-readable market symbols (e.g., `BTCUSDT`).
pub type Symbol = String;

/// Unique identifier assigned to orders (exchange or client provided).
pub type OrderId = String;

/// The side of an order or position.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Side {
    /// Buy the instrument.
    Buy,
    /// Sell the instrument.
    Sell,
}

impl Side {
    /// Returns the opposite side (buy <-> sell).
    #[must_use]
    pub fn inverse(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }

    /// Convert to `i8` representation used by certain exchanges.
    #[must_use]
    pub fn as_i8(self) -> i8 {
        match self {
            Self::Buy => 1,
            Self::Sell => -1,
        }
    }
}

/// Order execution style.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum OrderType {
    /// Execute immediately at best available price.
    Market,
    /// Execute at the provided limit price.
    Limit,
    /// A conditional market order triggered by a price movement.
    StopMarket,
}

/// Optional time-in-force constraints.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TimeInForce {
    GoodTilCanceled,
    ImmediateOrCancel,
    FillOrKill,
}

/// Interval granularity used when aggregating ticks into candles.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Interval {
    OneSecond,
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
    FourHours,
    OneDay,
}

impl Interval {
    /// Convert the interval into a chrono `Duration`.
    #[must_use]
    pub fn as_duration(self) -> Duration {
        match self {
            Self::OneSecond => Duration::seconds(1),
            Self::OneMinute => Duration::minutes(1),
            Self::FiveMinutes => Duration::minutes(5),
            Self::FifteenMinutes => Duration::minutes(15),
            Self::OneHour => Duration::hours(1),
            Self::FourHours => Duration::hours(4),
            Self::OneDay => Duration::days(1),
        }
    }

    /// Convert to Bybit interval identifiers.
    #[must_use]
    pub fn to_bybit(self) -> &'static str {
        match self {
            Self::OneSecond => "1",
            Self::OneMinute => "1",
            Self::FiveMinutes => "5",
            Self::FifteenMinutes => "15",
            Self::OneHour => "60",
            Self::FourHours => "240",
            Self::OneDay => "D",
        }
    }
}

impl FromStr for Interval {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_lowercase().as_str() {
            "1s" | "1sec" | "1second" | "1" => Ok(Self::OneSecond),
            "1m" | "1min" | "1minute" => Ok(Self::OneMinute),
            "5m" | "5min" | "5minutes" => Ok(Self::FiveMinutes),
            "15m" | "15min" | "15minutes" => Ok(Self::FifteenMinutes),
            "1h" | "60m" | "1hour" | "60" => Ok(Self::OneHour),
            "4h" | "240m" | "4hours" | "240" => Ok(Self::FourHours),
            "1d" | "day" | "d" => Ok(Self::OneDay),
            other => Err(format!("unsupported interval '{other}'")),
        }
    }
}

/// Base market data structure representing the smallest, most recent trade.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Tick {
    pub symbol: Symbol,
    pub price: Price,
    pub size: Quantity,
    pub side: Side,
    pub exchange_timestamp: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
}

/// Aggregated OHLCV bar data.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Candle {
    pub symbol: Symbol,
    pub interval: Interval,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Quantity,
    pub timestamp: DateTime<Utc>,
}

/// Represents a single level in the order book.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OrderBookLevel {
    pub price: Price,
    pub size: Quantity,
}

/// Snapshot of the order book depth.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OrderBook {
    pub symbol: Symbol,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub timestamp: DateTime<Utc>,
}

impl OrderBook {
    /// Returns the best bid if available.
    #[must_use]
    pub fn best_bid(&self) -> Option<&OrderBookLevel> {
        self.bids.first()
    }

    /// Returns the best ask if available.
    #[must_use]
    pub fn best_ask(&self) -> Option<&OrderBookLevel> {
        self.asks.first()
    }

    /// Calculates bid/ask imbalance for the top `depth` levels.
    #[must_use]
    pub fn imbalance(&self, depth: usize) -> Option<f64> {
        let depth = depth.max(1);
        let bid_vol: f64 = self.bids.iter().take(depth).map(|level| level.size).sum();
        let ask_vol: f64 = self.asks.iter().take(depth).map(|level| level.size).sum();
        let denom = bid_vol + ask_vol;
        if denom.abs() < f64::EPSILON {
            None
        } else {
            Some((bid_vol - ask_vol) / denom)
        }
    }
}

/// Incremental order book update.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DepthUpdate {
    pub symbol: Symbol,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub timestamp: DateTime<Utc>,
}

/// Desired order placement parameters.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OrderRequest {
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Quantity,
    pub price: Option<Price>,
    pub trigger_price: Option<Price>,
    pub time_in_force: Option<TimeInForce>,
    pub client_order_id: Option<String>,
}

/// High-level order status maintained inside the framework.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum OrderStatus {
    PendingNew,
    Accepted,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

/// Order representation that aggregates exchange state.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Order {
    pub id: OrderId,
    pub request: OrderRequest,
    pub status: OrderStatus,
    pub filled_quantity: Quantity,
    pub avg_fill_price: Option<Price>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Execution information emitted whenever an order is filled.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Fill {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub side: Side,
    pub fill_price: Price,
    pub fill_quantity: Quantity,
    pub fee: Option<Price>,
    pub timestamp: DateTime<Utc>,
}

/// Trade is an immutable record derived from a fill.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Trade {
    pub id: Uuid,
    pub fill: Fill,
    pub realized_pnl: Price,
}

/// Snapshot of a portfolio position.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Position {
    pub symbol: Symbol,
    pub side: Option<Side>,
    pub quantity: Quantity,
    pub entry_price: Option<Price>,
    pub unrealized_pnl: Price,
    pub updated_at: DateTime<Utc>,
}

impl Position {
    /// Update the mark price to refresh unrealized PnL.
    pub fn mark_price(&mut self, price: Price) {
        if let (Some(entry), Some(side)) = (self.entry_price, self.side) {
            let delta = match side {
                Side::Buy => price - entry,
                Side::Sell => entry - price,
            };
            self.unrealized_pnl = delta * self.quantity;
        }
        self.updated_at = Utc::now();
    }
}

/// Simple representation of an account balance by currency.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AccountBalance {
    pub currency: String,
    pub total: Price,
    pub available: Price,
    pub updated_at: DateTime<Utc>,
}

/// High-level intent generated by strategies.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Signal {
    pub id: Uuid,
    pub symbol: Symbol,
    pub kind: SignalKind,
    pub confidence: f64,
    pub generated_at: DateTime<Utc>,
    pub note: Option<String>,
    pub stop_loss: Option<Price>,
    pub take_profit: Option<Price>,
}

/// The type of action a signal instructs the execution layer to take.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SignalKind {
    EnterLong,
    ExitLong,
    EnterShort,
    ExitShort,
    Flatten,
}

impl Signal {
    /// Convenience constructor to build a signal with a random identifier.
    #[must_use]
    pub fn new(symbol: impl Into<Symbol>, kind: SignalKind, confidence: f64) -> Self {
        Self {
            id: Uuid::new_v4(),
            symbol: symbol.into(),
            kind,
            confidence,
            generated_at: Utc::now(),
            note: None,
            stop_loss: None,
            take_profit: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_duration_matches_definition() {
        assert_eq!(Interval::OneMinute.as_duration(), Duration::minutes(1));
        assert_eq!(Interval::FourHours.as_duration(), Duration::hours(4));
    }

    #[test]
    fn position_mark_price_updates_unrealized_pnl() {
        let mut position = Position {
            symbol: "BTCUSDT".to_string(),
            side: Some(Side::Buy),
            quantity: 0.5,
            entry_price: Some(60_000.0),
            unrealized_pnl: 0.0,
            updated_at: Utc::now(),
        };
        position.mark_price(60_500.0);
        assert!((position.unrealized_pnl - 250.0).abs() < f64::EPSILON);
    }
}
