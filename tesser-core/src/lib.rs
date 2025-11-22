//! Fundamental data types shared across the entire workspace.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use crc32fast::Hasher;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Alias for price precision.
pub type Price = Decimal;
/// Alias for quantity precision.
pub type Quantity = Decimal;
/// Alias used for human-readable market symbols (e.g., `BTCUSDT`).
pub type Symbol = String;

/// Unique identifier assigned to orders (exchange or client provided).
pub type OrderId = String;

/// Enumerates the supported financial instrument families.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind {
    Spot,
    LinearPerpetual,
    InversePerpetual,
}

/// Immutable metadata describing a tradable market.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Instrument {
    pub symbol: Symbol,
    pub base: Symbol,
    pub quote: Symbol,
    pub kind: InstrumentKind,
    pub settlement_currency: Symbol,
    pub tick_size: Price,
    pub lot_size: Quantity,
}

/// Represents a currency balance and its current conversion rate to the reporting currency.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Cash {
    pub currency: Symbol,
    pub quantity: Quantity,
    pub conversion_rate: Price,
}

impl Cash {
    /// Convert this balance into the reporting currency using the latest conversion rate.
    #[must_use]
    pub fn value(&self) -> Price {
        self.quantity * self.conversion_rate
    }
}

/// Multi-currency ledger keyed by currency symbol.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CashBook(pub HashMap<Symbol, Cash>);

impl CashBook {
    #[must_use]
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn upsert(&mut self, cash: Cash) {
        self.0.insert(cash.currency.clone(), cash);
    }

    pub fn adjust(&mut self, currency: &str, delta: Quantity) -> Quantity {
        let entry = self.0.entry(currency.to_string()).or_insert(Cash {
            currency: currency.to_string(),
            quantity: Decimal::ZERO,
            conversion_rate: Decimal::ZERO,
        });
        entry.quantity += delta;
        entry.quantity
    }

    pub fn update_conversion_rate(&mut self, currency: &str, rate: Price) {
        let entry = self.0.entry(currency.to_string()).or_insert(Cash {
            currency: currency.to_string(),
            quantity: Decimal::ZERO,
            conversion_rate: Decimal::ZERO,
        });
        entry.conversion_rate = rate;
    }

    #[must_use]
    pub fn total_value(&self) -> Price {
        self.0.values().map(Cash::value).sum()
    }

    #[must_use]
    pub fn get(&self, currency: &str) -> Option<&Cash> {
        self.0.get(currency)
    }

    #[must_use]
    pub fn get_mut(&mut self, currency: &str) -> Option<&mut Cash> {
        self.0.get_mut(currency)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Symbol, &Cash)> {
        self.0.iter()
    }
}

/// Execution hints for algorithmic order placement.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ExecutionHint {
    /// Time-Weighted Average Price execution over specified duration.
    Twap { duration: Duration },
    /// Volume-Weighted Average Price execution.
    Vwap {
        duration: Duration,
        #[serde(default)]
        participation_rate: Option<Decimal>,
    },
    /// Iceberg order (simulated in software).
    IcebergSimulated {
        display_size: Quantity,
        #[serde(default)]
        limit_offset_bps: Option<Decimal>,
    },
    /// Pegged-to-best style trading that refreshes a passive order at the top of book.
    PeggedBest {
        offset_bps: Decimal,
        #[serde(default)]
        clip_size: Option<Quantity>,
        #[serde(default)]
        refresh_secs: Option<u64>,
    },
    /// Sits on the sidelines until a target price is reached, then fires aggressively.
    Sniper {
        trigger_price: Price,
        #[serde(default)]
        timeout: Option<Duration>,
    },
}

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

    /// Convert the interval to Binance-compatible identifiers.
    pub fn to_binance(self) -> &'static str {
        match self {
            Self::OneSecond => "1s",
            Self::OneMinute => "1m",
            Self::FiveMinutes => "5m",
            Self::FifteenMinutes => "15m",
            Self::OneHour => "1h",
            Self::FourHours => "4h",
            Self::OneDay => "1d",
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
    #[serde(default)]
    pub exchange_checksum: Option<u32>,
    #[serde(default)]
    pub local_checksum: Option<u32>,
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
    pub fn imbalance(&self, depth: usize) -> Option<Decimal> {
        let depth = depth.max(1);
        let bid_vol: Decimal = self.bids.iter().take(depth).map(|level| level.size).sum();
        let ask_vol: Decimal = self.asks.iter().take(depth).map(|level| level.size).sum();
        let denom = bid_vol + ask_vol;
        if denom.is_zero() {
            None
        } else {
            Some((bid_vol - ask_vol) / denom)
        }
    }

    /// Compute a checksum for the current order book using up to `depth` levels (or full depth when `None`).
    #[must_use]
    pub fn computed_checksum(&self, depth: Option<usize>) -> u32 {
        let mut lob = LocalOrderBook::new();
        let bids = self
            .bids
            .iter()
            .map(|level| (level.price, level.size))
            .collect::<Vec<_>>();
        let asks = self
            .asks
            .iter()
            .map(|level| (level.price, level.size))
            .collect::<Vec<_>>();
        lob.load_snapshot(&bids, &asks);
        let depth = depth.unwrap_or_else(|| bids.len().max(asks.len()).max(1));
        lob.checksum(depth)
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

/// Local view of an order book backed by sorted price levels.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LocalOrderBook {
    bids: BTreeMap<Reverse<Price>, Quantity>,
    asks: BTreeMap<Price, Quantity>,
}

impl LocalOrderBook {
    /// Create an empty order book.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset the book with explicit bid/ask snapshots.
    pub fn load_snapshot(&mut self, bids: &[(Price, Quantity)], asks: &[(Price, Quantity)]) {
        self.bids.clear();
        self.asks.clear();
        for &(price, qty) in bids {
            self.add_order(Side::Buy, price, qty);
        }
        for &(price, qty) in asks {
            self.add_order(Side::Sell, price, qty);
        }
    }

    /// Insert or update a price level by accumulating quantity.
    pub fn add_order(&mut self, side: Side, price: Price, quantity: Quantity) {
        if quantity <= Decimal::ZERO {
            return;
        }
        match side {
            Side::Buy => {
                let key = Reverse(price);
                let entry = self.bids.entry(key).or_insert(Decimal::ZERO);
                *entry += quantity;
            }
            Side::Sell => {
                let entry = self.asks.entry(price).or_insert(Decimal::ZERO);
                *entry += quantity;
            }
        }
    }

    /// Overwrite a price level with the provided absolute quantity (removing when zero).
    pub fn apply_delta(&mut self, side: Side, price: Price, quantity: Quantity) {
        if quantity <= Decimal::ZERO {
            self.clear_level(side, price);
            return;
        }
        match side {
            Side::Buy => {
                self.bids.insert(Reverse(price), quantity);
            }
            Side::Sell => {
                self.asks.insert(price, quantity);
            }
        }
    }

    /// Apply a batch of bid/ask deltas atomically.
    pub fn apply_deltas(&mut self, bids: &[(Price, Quantity)], asks: &[(Price, Quantity)]) {
        for &(price, qty) in bids {
            self.apply_delta(Side::Buy, price, qty);
        }
        for &(price, qty) in asks {
            self.apply_delta(Side::Sell, price, qty);
        }
    }

    /// Remove quantity from a price level, deleting the level when depleted.
    pub fn remove_order(&mut self, side: Side, price: Price, quantity: Quantity) {
        if quantity <= Decimal::ZERO {
            return;
        }
        match side {
            Side::Buy => {
                let key = Reverse(price);
                if let Some(level) = self.bids.get_mut(&key) {
                    *level -= quantity;
                    if *level <= Decimal::ZERO {
                        self.bids.remove(&key);
                    }
                }
            }
            Side::Sell => {
                if let Some(level) = self.asks.get_mut(&price) {
                    *level -= quantity;
                    if *level <= Decimal::ZERO {
                        self.asks.remove(&price);
                    }
                }
            }
        }
    }

    /// Remove an entire price level regardless of resting quantity.
    pub fn clear_level(&mut self, side: Side, price: Price) {
        match side {
            Side::Buy => {
                self.bids.remove(&Reverse(price));
            }
            Side::Sell => {
                self.asks.remove(&price);
            }
        }
    }

    /// Best bid price/quantity currently stored.
    #[must_use]
    pub fn best_bid(&self) -> Option<(Price, Quantity)> {
        self.bids.iter().next().map(|(price, qty)| (price.0, *qty))
    }

    /// Best ask price/quantity currently stored.
    #[must_use]
    pub fn best_ask(&self) -> Option<(Price, Quantity)> {
        self.asks.iter().next().map(|(price, qty)| (*price, *qty))
    }

    /// Iterate bids in descending price order.
    pub fn bids(&self) -> impl Iterator<Item = (Price, Quantity)> + '_ {
        self.bids.iter().map(|(price, qty)| (price.0, *qty))
    }

    /// Iterate asks in ascending price order.
    pub fn asks(&self) -> impl Iterator<Item = (Price, Quantity)> + '_ {
        self.asks.iter().map(|(price, qty)| (*price, *qty))
    }

    /// Consume liquidity from the opposite side of an aggressive order.
    pub fn take_liquidity(
        &mut self,
        aggressive_side: Side,
        mut quantity: Quantity,
    ) -> Vec<(Price, Quantity)> {
        let mut fills = Vec::new();
        while quantity > Decimal::ZERO {
            let (price, available) = match aggressive_side {
                Side::Buy => match self.best_ask() {
                    Some(level) => level,
                    None => break,
                },
                Side::Sell => match self.best_bid() {
                    Some(level) => level,
                    None => break,
                },
            };
            let traded = quantity.min(available);
            let contra_side = aggressive_side.inverse();
            self.remove_order(contra_side, price, traded);
            fills.push((price, traded));
            quantity -= traded;
        }
        fills
    }

    /// Returns true when either side of the book currently holds levels.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    /// Compute a CRC32 checksum for the top N levels (Bybit/Binance compatibility).
    #[must_use]
    pub fn checksum(&self, depth: usize) -> u32 {
        if depth == 0 {
            return 0;
        }
        let mut buffer = String::new();
        let mut first = true;
        for (price, size) in self.bids().take(depth) {
            if !first {
                buffer.push(':');
            }
            first = false;
            write!(buffer, "{}:{}", price.normalize(), size.normalize()).ok();
        }
        for (price, size) in self.asks().take(depth) {
            if !first {
                buffer.push(':');
            }
            first = false;
            write!(buffer, "{}:{}", price.normalize(), size.normalize()).ok();
        }
        let mut hasher = Hasher::new();
        hasher.update(buffer.as_bytes());
        hasher.finalize()
    }

    /// Helper for generating owned bid levels up to the desired depth.
    pub fn bid_levels(&self, depth: usize) -> Vec<(Price, Quantity)> {
        self.bids().take(depth).collect()
    }

    /// Helper for generating owned ask levels up to the desired depth.
    pub fn ask_levels(&self, depth: usize) -> Vec<(Price, Quantity)> {
        self.asks().take(depth).collect()
    }
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
    pub take_profit: Option<Price>,
    pub stop_loss: Option<Price>,
    pub display_quantity: Option<Quantity>,
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
    pub execution_hint: Option<ExecutionHint>,
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

impl SignalKind {
    /// Returns the Side for this signal kind.
    #[must_use]
    pub fn side(self) -> Side {
        match self {
            Self::EnterLong | Self::ExitShort => Side::Buy,
            Self::EnterShort | Self::ExitLong => Side::Sell,
            Self::Flatten => {
                // For flatten, we need position context to determine side
                // This is a simplification - in practice, the execution engine
                // would determine the correct side based on current position
                Side::Sell
            }
        }
    }
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
            execution_hint: None,
        }
    }

    /// Add an execution hint to the signal.
    #[must_use]
    pub fn with_hint(mut self, hint: ExecutionHint) -> Self {
        self.execution_hint = Some(hint);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::FromPrimitive;

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
            quantity: Decimal::from_f64(0.5).unwrap(),
            entry_price: Some(Decimal::from(60_000)),
            unrealized_pnl: Decimal::ZERO,
            updated_at: Utc::now(),
        };
        position.mark_price(Decimal::from(60_500));
        assert_eq!(position.unrealized_pnl, Decimal::from(250));
    }

    #[test]
    fn local_order_book_tracks_best_levels() {
        let mut lob = LocalOrderBook::new();
        lob.add_order(Side::Buy, Decimal::from(10), Decimal::from(2));
        lob.add_order(Side::Buy, Decimal::from(11), Decimal::from(1));
        lob.add_order(
            Side::Sell,
            Decimal::from(12),
            Decimal::from_f64(1.5).unwrap(),
        );
        lob.add_order(Side::Sell, Decimal::from(13), Decimal::from(3));

        assert_eq!(lob.best_bid(), Some((Decimal::from(11), Decimal::from(1))));
        assert_eq!(
            lob.best_ask(),
            Some((Decimal::from(12), Decimal::from_f64(1.5).unwrap()))
        );

        let fills = lob.take_liquidity(Side::Buy, Decimal::from(2));
        assert_eq!(
            fills,
            vec![
                (Decimal::from(12), Decimal::from_f64(1.5).unwrap()),
                (Decimal::from(13), Decimal::from_f64(0.5).unwrap())
            ]
        );
        assert_eq!(
            lob.best_ask(),
            Some((Decimal::from(13), Decimal::from_f64(2.5).unwrap()))
        );
    }

    #[test]
    fn local_order_book_apply_delta_overwrites_level() {
        let mut lob = LocalOrderBook::new();
        lob.apply_delta(Side::Buy, Decimal::from(100), Decimal::from(1));
        lob.apply_delta(Side::Buy, Decimal::from(100), Decimal::from(3));
        assert_eq!(lob.best_bid(), Some((Decimal::from(100), Decimal::from(3))));

        lob.apply_delta(Side::Buy, Decimal::from(100), Decimal::ZERO);
        assert!(lob.best_bid().is_none());
    }

    #[test]
    fn local_order_book_checksum_reflects_depth() {
        let mut lob = LocalOrderBook::new();
        lob.apply_delta(Side::Buy, Decimal::from(10), Decimal::from(1));
        lob.apply_delta(Side::Buy, Decimal::from(9), Decimal::from(2));
        lob.apply_delta(Side::Sell, Decimal::from(11), Decimal::from(1));
        lob.apply_delta(Side::Sell, Decimal::from(12), Decimal::from(2));

        let checksum_full = lob.checksum(2);
        let checksum_partial = lob.checksum(1);
        assert_ne!(checksum_full, checksum_partial);
    }
}
