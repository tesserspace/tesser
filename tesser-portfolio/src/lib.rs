//! Portfolio accounting primitives.

use std::cmp;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tesser_core::{
    AccountBalance, Cash, CashBook, Fill, Instrument, InstrumentKind, Order, Position, Price,
    Quantity, Side, Symbol,
};
use tesser_markets::MarketRegistry;
use thiserror::Error;

/// Result alias for portfolio operations.
pub type PortfolioResult<T> = Result<T, PortfolioError>;

/// Portfolio-specific error type.
#[derive(Debug, Error)]
pub enum PortfolioError {
    /// Raised when a fill references a symbol that is not being tracked yet.
    #[error("unknown symbol: {0}")]
    UnknownSymbol(Symbol),
    /// Wraps any other issues surfaced by dependencies.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Configuration used when instantiating a portfolio.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PortfolioConfig {
    pub initial_balances: HashMap<Symbol, Price>,
    pub reporting_currency: Symbol,
    pub max_drawdown: Option<Decimal>,
}

impl Default for PortfolioConfig {
    fn default() -> Self {
        let mut balances = HashMap::new();
        balances.insert("USDT".to_string(), Decimal::from(10_000));
        Self {
            initial_balances: balances,
            reporting_currency: "USDT".into(),
            max_drawdown: None,
        }
    }
}

/// Stores aggregate positions keyed by symbol.
pub struct Portfolio {
    positions: HashMap<Symbol, Position>,
    balances: CashBook,
    reporting_currency: Symbol,
    initial_equity: Price,
    drawdown_limit: Option<Decimal>,
    peak_equity: Price,
    liquidate_only: bool,
    market_registry: Arc<MarketRegistry>,
}

impl Portfolio {
    /// Instantiate a new portfolio with default configuration.
    pub fn new(config: PortfolioConfig, registry: Arc<MarketRegistry>) -> Self {
        let limit = config.max_drawdown.filter(|value| *value > Decimal::ZERO);
        let mut balances = CashBook::new();
        for (currency, amount) in &config.initial_balances {
            balances.upsert(Cash {
                currency: currency.clone(),
                quantity: *amount,
                conversion_rate: if currency == &config.reporting_currency {
                    Decimal::ONE
                } else {
                    Decimal::ZERO
                },
            });
        }
        balances
            .0
            .entry(config.reporting_currency.clone())
            .or_insert(Cash {
                currency: config.reporting_currency.clone(),
                quantity: Decimal::ZERO,
                conversion_rate: Decimal::ONE,
            });
        let initial_equity = balances.total_value();
        Self {
            positions: HashMap::new(),
            balances,
            reporting_currency: config.reporting_currency,
            initial_equity,
            drawdown_limit: limit,
            peak_equity: initial_equity,
            liquidate_only: false,
            market_registry: registry,
        }
    }

    /// Build a portfolio snapshot from live exchange balances and positions.
    pub fn from_exchange_state(
        positions: Vec<Position>,
        balances: Vec<AccountBalance>,
        config: PortfolioConfig,
        registry: Arc<MarketRegistry>,
    ) -> Self {
        let mut portfolio = Self::new(config, registry);
        let mut position_map = HashMap::new();
        for position in positions.into_iter() {
            if position.quantity.is_zero() {
                continue;
            }
            position_map.insert(position.symbol.clone(), position);
        }
        let mut cash_book = CashBook::new();
        for balance in balances {
            cash_book.upsert(Cash {
                currency: balance.currency.clone(),
                quantity: balance.available,
                conversion_rate: if balance.currency == portfolio.reporting_currency {
                    Decimal::ONE
                } else {
                    Decimal::ZERO
                },
            });
        }
        cash_book
            .0
            .entry(portfolio.reporting_currency.clone())
            .or_insert(Cash {
                currency: portfolio.reporting_currency.clone(),
                quantity: Decimal::ZERO,
                conversion_rate: Decimal::ONE,
            });
        portfolio.positions = position_map;
        portfolio.balances = cash_book;
        portfolio.initial_equity = portfolio.balances.total_value();
        portfolio.peak_equity = portfolio.initial_equity;
        portfolio.update_drawdown_state();
        portfolio
    }

    /// Apply a trade fill to the internal bookkeeping.
    pub fn apply_fill(&mut self, fill: &Fill) -> PortfolioResult<()> {
        let instrument = self
            .market_registry
            .get(&fill.symbol)
            .ok_or_else(|| PortfolioError::UnknownSymbol(fill.symbol.clone()))?;
        self.ensure_currency(&instrument.settlement_currency);
        self.ensure_currency(&instrument.base);
        self.ensure_currency(&instrument.quote);
        let mut realized_delta = Decimal::ZERO;
        {
            let entry = self
                .positions
                .entry(fill.symbol.clone())
                .or_insert(Position {
                    symbol: fill.symbol.clone(),
                    side: Some(fill.side),
                    quantity: Decimal::ZERO,
                    entry_price: Some(fill.fill_price),
                    unrealized_pnl: Decimal::ZERO,
                    updated_at: fill.timestamp,
                });

            if entry.side.is_none() {
                entry.side = Some(fill.side);
            }

            match entry.side {
                Some(side) if side == fill.side => {
                    let total_qty = entry.quantity + fill.fill_quantity;
                    let prev_cost = entry
                        .entry_price
                        .map(|price| price * entry.quantity)
                        .unwrap_or_default();
                    let new_cost = fill.fill_price * fill.fill_quantity;
                    entry.entry_price = if total_qty.is_zero() {
                        Some(fill.fill_price)
                    } else {
                        Some((prev_cost + new_cost) / total_qty)
                    };
                    entry.quantity = total_qty;
                }
                Some(_) => {
                    if let Some(entry_price) = entry.entry_price {
                        let closing_qty = entry.quantity.min(fill.fill_quantity);
                        realized_delta = calculate_realized_pnl(
                            &instrument.kind,
                            entry_price,
                            fill.fill_price,
                            closing_qty,
                            fill.side,
                        );
                    }
                    let remaining = entry.quantity - fill.fill_quantity;
                    if remaining > Decimal::ZERO {
                        entry.quantity = remaining;
                    } else if remaining < Decimal::ZERO {
                        entry.quantity = remaining.abs();
                        entry.side = Some(fill.side);
                        entry.entry_price = Some(fill.fill_price);
                    } else {
                        entry.quantity = Decimal::ZERO;
                        entry.side = None;
                        entry.entry_price = None;
                    }
                }
                None => {
                    entry.side = Some(fill.side);
                    entry.quantity = fill.fill_quantity;
                    entry.entry_price = Some(fill.fill_price);
                }
            }

            entry.updated_at = fill.timestamp;
        }

        self.apply_cash_flow(&instrument, fill, realized_delta);
        self.update_drawdown_state();
        Ok(())
    }

    /// Retrieve a position snapshot for a symbol.
    #[must_use]
    pub fn position(&self, symbol: &str) -> Option<&Position> {
        self.positions.get(symbol)
    }

    /// Total net asset value (cash + unrealized PnL).
    #[must_use]
    pub fn equity(&self) -> Price {
        let unrealized: Price = self.positions.values().map(|p| p.unrealized_pnl).sum();
        self.balances.total_value() + unrealized
    }

    /// Cash on hand that is not locked in positions.
    #[must_use]
    pub fn cash(&self) -> Price {
        self.balances
            .get(&self.reporting_currency)
            .map(|cash| cash.quantity)
            .unwrap_or_default()
    }

    /// Realized profit and loss across all closed positions.
    #[must_use]
    pub fn realized_pnl(&self) -> Price {
        let unrealized: Price = self.positions.values().map(|p| p.unrealized_pnl).sum();
        self.equity() - self.initial_equity - unrealized
    }

    /// Initial capital provided to the portfolio.
    #[must_use]
    pub fn initial_equity(&self) -> Price {
        self.initial_equity
    }

    /// Clone all tracked positions for external consumers (e.g., strategies).
    #[must_use]
    pub fn positions(&self) -> Vec<Position> {
        self.positions.values().cloned().collect()
    }

    /// Signed position quantity helper (long positive, short negative).
    #[must_use]
    pub fn signed_position_qty(&self, symbol: &str) -> Quantity {
        self.positions
            .get(symbol)
            .map(|position| match position.side {
                Some(Side::Buy) => position.quantity,
                Some(Side::Sell) => -position.quantity,
                None => Decimal::ZERO,
            })
            .unwrap_or(Decimal::ZERO)
    }

    /// Whether the portfolio currently allows only exposure-reducing orders.
    #[must_use]
    pub fn liquidate_only(&self) -> bool {
        self.liquidate_only
    }

    /// Forcefully toggle liquidate-only mode. Returns true if the flag changed.
    pub fn set_liquidate_only(&mut self, enabled: bool) -> bool {
        if self.liquidate_only == enabled {
            false
        } else {
            self.liquidate_only = enabled;
            true
        }
    }

    /// Snapshot the current state for persistence.
    #[must_use]
    pub fn snapshot(&self) -> PortfolioState {
        PortfolioState {
            positions: self.positions.clone(),
            balances: self.balances.clone(),
            reporting_currency: self.reporting_currency.clone(),
            initial_equity: self.initial_equity,
            drawdown_limit: self.drawdown_limit,
            peak_equity: self.peak_equity,
            liquidate_only: self.liquidate_only,
        }
    }

    /// Rehydrate a portfolio from a persisted snapshot.
    pub fn from_state(
        state: PortfolioState,
        config: PortfolioConfig,
        registry: Arc<MarketRegistry>,
    ) -> Self {
        let drawdown_limit = config
            .max_drawdown
            .filter(|value| *value > Decimal::ZERO)
            .or(state.drawdown_limit);
        let peak_against_state = cmp::max(state.peak_equity, state.initial_equity);
        let mut portfolio = Self {
            positions: state.positions,
            balances: state.balances,
            reporting_currency: state.reporting_currency,
            initial_equity: state.initial_equity,
            drawdown_limit,
            peak_equity: cmp::max(peak_against_state, state.initial_equity),
            liquidate_only: state.liquidate_only,
            market_registry: registry,
        };
        let reporting = portfolio.reporting_currency.clone();
        portfolio.ensure_currency(&reporting);
        portfolio
            .balances
            .update_conversion_rate(&reporting, Decimal::ONE);
        portfolio.update_drawdown_state();
        portfolio
    }

    /// Refresh mark-to-market pricing and conversion rates for a symbol.
    pub fn update_market_data(&mut self, symbol: &str, price: Price) -> PortfolioResult<bool> {
        let instrument = self
            .market_registry
            .get(symbol)
            .ok_or_else(|| PortfolioError::UnknownSymbol(symbol.to_string()))?;
        let mut updated = false;
        if let Some(position) = self.positions.get_mut(symbol) {
            update_unrealized(position, &instrument, price);
            updated = true;
        }
        self.update_conversion_rates(&instrument, price);
        self.update_drawdown_state();
        Ok(updated)
    }

    fn update_drawdown_state(&mut self) {
        let equity = self.equity();
        if equity > self.peak_equity {
            self.peak_equity = equity;
        }
        if let Some(limit) = self.drawdown_limit {
            if self.peak_equity > Decimal::ZERO {
                let drawdown = (self.peak_equity - equity) / self.peak_equity;
                if drawdown >= limit {
                    self.liquidate_only = true;
                }
            }
        }
    }

    fn apply_cash_flow(&mut self, instrument: &Instrument, fill: &Fill, realized: Price) {
        match instrument.kind {
            InstrumentKind::Spot => self.apply_spot_flow(instrument, fill),
            InstrumentKind::LinearPerpetual | InstrumentKind::InversePerpetual => {
                self.apply_derivative_flow(instrument, fill, realized)
            }
        }
        if let Some(fee) = fill.fee {
            let fee_currency = match instrument.kind {
                InstrumentKind::Spot => &instrument.quote,
                _ => &instrument.settlement_currency,
            };
            self.ensure_currency(fee_currency);
            self.balances.adjust(fee_currency, -fee);
        }
    }

    fn apply_spot_flow(&mut self, instrument: &Instrument, fill: &Fill) {
        let notional = fill.fill_price * fill.fill_quantity;
        match fill.side {
            Side::Buy => {
                self.ensure_currency(&instrument.base);
                self.ensure_currency(&instrument.quote);
                self.balances.adjust(&instrument.base, fill.fill_quantity);
                self.balances.adjust(&instrument.quote, -notional);
            }
            Side::Sell => {
                self.ensure_currency(&instrument.base);
                self.ensure_currency(&instrument.quote);
                self.balances.adjust(&instrument.base, -fill.fill_quantity);
                self.balances.adjust(&instrument.quote, notional);
            }
        }
    }

    fn apply_derivative_flow(&mut self, instrument: &Instrument, fill: &Fill, realized: Price) {
        let notional = fill.fill_price * fill.fill_quantity;
        let direction = Decimal::from(fill.side.as_i8());
        let settlement = &instrument.settlement_currency;
        self.ensure_currency(settlement);
        self.balances.adjust(settlement, -(notional * direction));
        if !realized.is_zero() {
            self.balances.adjust(settlement, realized);
        }
    }

    fn ensure_currency(&mut self, currency: &str) {
        self.balances.0.entry(currency.to_string()).or_insert(Cash {
            currency: currency.to_string(),
            quantity: Decimal::ZERO,
            conversion_rate: if currency == self.reporting_currency {
                Decimal::ONE
            } else {
                Decimal::ZERO
            },
        });
    }

    fn update_conversion_rates(&mut self, instrument: &Instrument, price: Price) {
        if instrument.quote == self.reporting_currency {
            self.ensure_currency(&instrument.base);
            self.ensure_currency(&instrument.quote);
            self.balances
                .update_conversion_rate(&instrument.base, price);
            self.balances
                .update_conversion_rate(&instrument.quote, Decimal::ONE);
            return;
        }
        if instrument.base == self.reporting_currency && !price.is_zero() {
            self.ensure_currency(&instrument.quote);
            self.ensure_currency(&instrument.base);
            self.balances
                .update_conversion_rate(&instrument.base, Decimal::ONE);
            self.balances
                .update_conversion_rate(&instrument.quote, Decimal::ONE / price);
            return;
        }
        let quote_rate = self
            .balances
            .get(&instrument.quote)
            .map(|cash| cash.conversion_rate)
            .unwrap_or(Decimal::ZERO);
        if quote_rate > Decimal::ZERO {
            self.ensure_currency(&instrument.base);
            self.balances
                .update_conversion_rate(&instrument.base, price * quote_rate);
        }
        let base_rate = self
            .balances
            .get(&instrument.base)
            .map(|cash| cash.conversion_rate)
            .unwrap_or(Decimal::ZERO);
        if base_rate > Decimal::ZERO && !price.is_zero() {
            self.ensure_currency(&instrument.quote);
            self.balances
                .update_conversion_rate(&instrument.quote, base_rate / price);
        }
    }
}

fn calculate_realized_pnl(
    kind: &InstrumentKind,
    entry_price: Price,
    exit_price: Price,
    quantity: Quantity,
    exit_side: Side,
) -> Price {
    match kind {
        InstrumentKind::Spot => Decimal::ZERO,
        InstrumentKind::LinearPerpetual => {
            let delta = match exit_side {
                Side::Buy => entry_price - exit_price,
                Side::Sell => exit_price - entry_price,
            };
            delta * quantity
        }
        InstrumentKind::InversePerpetual => {
            if entry_price.is_zero() || exit_price.is_zero() {
                return Decimal::ZERO;
            }
            let inv_entry = Decimal::ONE / entry_price;
            let inv_exit = Decimal::ONE / exit_price;
            let delta = match exit_side {
                Side::Buy => inv_entry - inv_exit,
                Side::Sell => inv_exit - inv_entry,
            };
            delta * quantity
        }
    }
}

fn update_unrealized(position: &mut Position, instrument: &Instrument, price: Price) {
    position.unrealized_pnl = match (instrument.kind, position.entry_price, position.side) {
        (InstrumentKind::Spot, _, _) => Decimal::ZERO,
        (InstrumentKind::LinearPerpetual, Some(entry), Some(side)) => {
            let delta = match side {
                Side::Buy => price - entry,
                Side::Sell => entry - price,
            };
            delta * position.quantity
        }
        (InstrumentKind::InversePerpetual, Some(entry), Some(side)) => {
            if price.is_zero() || entry.is_zero() {
                Decimal::ZERO
            } else {
                let inv_entry = Decimal::ONE / entry;
                let inv_price = Decimal::ONE / price;
                let delta = match side {
                    Side::Buy => inv_price - inv_entry,
                    Side::Sell => inv_entry - inv_price,
                };
                delta * position.quantity
            }
        }
        _ => Decimal::ZERO,
    };
    position.updated_at = Utc::now();
}

/// Serializable representation of a portfolio used for persistence.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PortfolioState {
    pub positions: HashMap<Symbol, Position>,
    pub balances: CashBook,
    pub reporting_currency: Symbol,
    pub initial_equity: Price,
    pub drawdown_limit: Option<Decimal>,
    pub peak_equity: Price,
    pub liquidate_only: bool,
}

/// Durable snapshot of the live trading runtime persisted on disk.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LiveState {
    pub portfolio: Option<PortfolioState>,
    pub open_orders: Vec<Order>,
    pub last_prices: HashMap<String, Price>,
    pub last_candle_ts: Option<DateTime<Utc>>,
    pub strategy_state: Option<serde_json::Value>,
}

/// Abstraction over state persistence backends.
pub trait StateRepository: Send + Sync + 'static {
    /// Load the most recent state from durable storage or defaults if none exists.
    fn load(&self) -> PortfolioResult<LiveState>;
    /// Atomically save the provided state snapshot.
    fn save(&self, state: &LiveState) -> PortfolioResult<()>;
}

const STATE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    payload TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"#;

/// [`StateRepository`] implementation backed by a SQLite database file.
#[derive(Clone)]
pub struct SqliteStateRepository {
    path: PathBuf,
}

impl SqliteStateRepository {
    /// Create a new repository that stores state inside the provided file path.
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn connect(&self) -> PortfolioResult<Connection> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                PortfolioError::Internal(format!(
                    "failed to create state directory {}: {err}",
                    parent.display()
                ))
            })?;
        }
        let conn = Connection::open(&self.path).map_err(|err| {
            PortfolioError::Internal(format!(
                "failed to open state database {}: {err}",
                self.path.display()
            ))
        })?;
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")
            .map_err(|err| {
                PortfolioError::Internal(format!("failed to configure sqlite: {err}"))
            })?;
        conn.execute_batch(STATE_SCHEMA)
            .map_err(|err| PortfolioError::Internal(format!("failed to apply schema: {err}")))?;
        Ok(conn)
    }
}

impl StateRepository for SqliteStateRepository {
    fn load(&self) -> PortfolioResult<LiveState> {
        let conn = self.connect()?;
        let payload: Option<String> = conn
            .query_row("SELECT payload FROM state WHERE id = 1", [], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|err| PortfolioError::Internal(format!("failed to read state: {err}")))?;
        if let Some(json) = payload {
            serde_json::from_str(&json).map_err(|err| {
                PortfolioError::Internal(format!("failed to decode persisted state: {err}"))
            })
        } else {
            Ok(LiveState::default())
        }
    }

    fn save(&self, state: &LiveState) -> PortfolioResult<()> {
        let mut conn = self.connect()?;
        let payload = serde_json::to_string(state).map_err(|err| {
            PortfolioError::Internal(format!("failed to serialize live state: {err}"))
        })?;
        let tx = conn.transaction().map_err(|err| {
            PortfolioError::Internal(format!("failed to begin transaction: {err}"))
        })?;
        tx.execute(
            "INSERT INTO state (id, payload, updated_at)
             VALUES (1, ?, CURRENT_TIMESTAMP)
             ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_at=CURRENT_TIMESTAMP",
            params![payload],
        )
        .map_err(|err| PortfolioError::Internal(format!("failed to upsert state row: {err}")))?;
        tx.commit()
            .map_err(|err| PortfolioError::Internal(format!("failed to commit state: {err}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::sync::Arc;
    use tesser_core::{Instrument, InstrumentKind, Side, Symbol};

    fn sample_fill(side: Side, price: Price, qty: Quantity) -> Fill {
        Fill {
            order_id: uuid::Uuid::new_v4().to_string(),
            symbol: Symbol::from("BTCUSDT"),
            side,
            fill_price: price,
            fill_quantity: qty,
            fee: None,
            timestamp: Utc::now(),
        }
    }

    fn sample_registry() -> Arc<MarketRegistry> {
        let instrument = Instrument {
            symbol: "BTCUSDT".into(),
            base: "BTC".into(),
            quote: "USDT".into(),
            kind: InstrumentKind::LinearPerpetual,
            settlement_currency: "USDT".into(),
            tick_size: Decimal::new(1, 0),
            lot_size: Decimal::new(1, 0),
        };
        Arc::new(MarketRegistry::from_instruments(vec![instrument]).unwrap())
    }

    #[test]
    fn portfolio_updates_equity() {
        let mut portfolio = Portfolio::new(PortfolioConfig::default(), sample_registry());
        let buy = sample_fill(Side::Buy, Decimal::from(50_000), Decimal::new(1, 1));
        portfolio.apply_fill(&buy).unwrap();
        assert!(portfolio.cash() < Decimal::from(10_000));
    }

    #[test]
    fn triggers_liquidate_only_on_drawdown() {
        let registry = sample_registry();
        let config = PortfolioConfig {
            max_drawdown: Some(Decimal::new(2, 2)), // 2%
            ..PortfolioConfig::default()
        };
        let mut portfolio = Portfolio::new(config, registry);
        let buy = sample_fill(Side::Buy, Decimal::from(10), Decimal::from(10));
        portfolio.apply_fill(&buy).unwrap();
        assert!(!portfolio.liquidate_only());
        // Price crash reduces equity by more than 2%
        portfolio
            .update_market_data(&buy.symbol, Decimal::ZERO)
            .unwrap();
        assert!(portfolio.liquidate_only());
    }
}
