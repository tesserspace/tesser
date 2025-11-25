//! Portfolio accounting primitives.

use std::cmp;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tesser_core::{
    AccountBalance, AssetId, Cash, CashBook, ExchangeId, Fill, Instrument, InstrumentKind, Order,
    Position, Price, Quantity, Side, Symbol,
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
    pub initial_balances: HashMap<AssetId, Price>,
    pub reporting_currency: AssetId,
    pub max_drawdown: Option<Decimal>,
}

impl Default for PortfolioConfig {
    fn default() -> Self {
        let mut balances = HashMap::new();
        balances.insert(AssetId::from("USDT"), Decimal::from(10_000));
        Self {
            initial_balances: balances,
            reporting_currency: AssetId::from("USDT"),
            max_drawdown: None,
        }
    }
}

/// Aggregated ledger and position data for a specific venue.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct SubAccount {
    pub exchange: ExchangeId,
    pub balances: CashBook,
    pub positions: HashMap<Symbol, Position>,
}

impl SubAccount {
    fn new(exchange: ExchangeId) -> Self {
        Self {
            exchange,
            balances: CashBook::new(),
            positions: HashMap::new(),
        }
    }

    fn ensure_currency(&mut self, reporting_currency: AssetId, currency: AssetId) {
        self.balances.0.entry(currency).or_insert(Cash {
            currency,
            quantity: Decimal::ZERO,
            conversion_rate: conversion_rate_for_currency(currency, reporting_currency),
        });
    }
}

/// Stores aggregate positions keyed by symbol.
pub struct Portfolio {
    sub_accounts: HashMap<ExchangeId, SubAccount>,
    reporting_currency: AssetId,
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
        let mut sub_accounts: HashMap<ExchangeId, SubAccount> = HashMap::new();
        for (currency, amount) in &config.initial_balances {
            let exchange = currency.exchange;
            let account = sub_accounts
                .entry(exchange)
                .or_insert_with(|| SubAccount::new(exchange));
            account.ensure_currency(config.reporting_currency, *currency);
            account.balances.upsert(Cash {
                currency: *currency,
                quantity: *amount,
                conversion_rate: conversion_rate_for_currency(*currency, config.reporting_currency),
            });
        }
        let mut portfolio = Self {
            sub_accounts,
            reporting_currency: config.reporting_currency,
            initial_equity: Decimal::ZERO,
            drawdown_limit: limit,
            peak_equity: Decimal::ZERO,
            liquidate_only: false,
            market_registry: registry,
        };
        portfolio.ensure_reporting_currency_entries();
        portfolio.initial_equity = portfolio.cash_value();
        portfolio.peak_equity = portfolio.initial_equity;
        portfolio
    }

    /// Build a portfolio snapshot from live exchange balances and positions.
    pub fn from_exchange_state(
        positions: Vec<Position>,
        balances: Vec<AccountBalance>,
        config: PortfolioConfig,
        registry: Arc<MarketRegistry>,
    ) -> Self {
        let mut portfolio = Self::new(config, registry);
        portfolio.sub_accounts.clear();
        for position in positions.into_iter() {
            if position.quantity.is_zero() {
                continue;
            }
            let exchange = position.symbol.exchange;
            let account = portfolio.account_mut(exchange);
            account.positions.insert(position.symbol, position);
        }
        for balance in balances {
            let exchange = if balance.exchange.is_specified() {
                balance.exchange
            } else {
                balance.asset.exchange
            };
            let account = portfolio.account_mut(exchange);
            account.ensure_currency(portfolio.reporting_currency, balance.asset);
            account.balances.upsert(Cash {
                currency: balance.asset,
                quantity: balance.available,
                conversion_rate: conversion_rate_for_currency(
                    balance.asset,
                    portfolio.reporting_currency,
                ),
            });
        }
        portfolio.ensure_reporting_currency_entries();
        portfolio.initial_equity = portfolio.cash_value();
        portfolio.peak_equity = portfolio.initial_equity;
        portfolio.update_drawdown_state();
        portfolio
    }

    /// Apply a trade fill to the internal bookkeeping.
    pub fn apply_fill(&mut self, fill: &Fill) -> PortfolioResult<()> {
        let instrument = self
            .market_registry
            .get(fill.symbol)
            .ok_or(PortfolioError::UnknownSymbol(fill.symbol))?;
        let account = self.account_mut(fill.symbol.exchange);
        account.ensure_currency(self.reporting_currency, instrument.settlement_currency);
        account.ensure_currency(self.reporting_currency, instrument.base);
        account.ensure_currency(self.reporting_currency, instrument.quote);
        let mut realized_delta = Decimal::ZERO;
        {
            let entry = account.positions.entry(fill.symbol).or_insert(Position {
                symbol: fill.symbol,
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

        self.apply_cash_flow(account, &instrument, fill, realized_delta);
        self.update_drawdown_state();
        Ok(())
    }

    /// Retrieve a position snapshot for a symbol.
    #[must_use]
    pub fn position(&self, symbol: impl Into<Symbol>) -> Option<&Position> {
        let symbol = symbol.into();
        self.sub_accounts
            .get(&symbol.exchange)
            .and_then(|account| account.positions.get(&symbol))
    }

    /// Total net asset value (cash + unrealized PnL).
    #[must_use]
    pub fn equity(&self) -> Price {
        let unrealized: Price = self
            .sub_accounts
            .values()
            .flat_map(|account| account.positions.values())
            .map(|p| p.unrealized_pnl)
            .sum();
        self.cash_value() + unrealized
    }

    /// Cash on hand that is not locked in positions.
    #[must_use]
    pub fn cash(&self) -> Price {
        let mut total = Decimal::ZERO;
        for account in self.sub_accounts.values() {
            for (currency, cash) in account.balances.iter() {
                if matches_reporting_currency(*currency, self.reporting_currency) {
                    total += cash.quantity;
                }
            }
        }
        total
    }

    /// Realized profit and loss across all closed positions.
    #[must_use]
    pub fn realized_pnl(&self) -> Price {
        self.equity() - self.initial_equity - self.total_unrealized()
    }

    /// Initial capital provided to the portfolio.
    #[must_use]
    pub fn initial_equity(&self) -> Price {
        self.initial_equity
    }

    /// Clone all tracked positions for external consumers (e.g., strategies).
    #[must_use]
    pub fn positions(&self) -> Vec<Position> {
        self.sub_accounts
            .values()
            .flat_map(|account| account.positions.values().cloned())
            .collect()
    }

    /// Return the available cash balance for a specific asset, if tracked.
    #[must_use]
    pub fn balance(&self, currency: impl Into<AssetId>) -> Option<Cash> {
        let currency = currency.into();
        self.sub_accounts
            .get(&currency.exchange)
            .and_then(|account| account.balances.get(currency).cloned())
    }

    /// Compute the aggregate equity for a single exchange.
    #[must_use]
    pub fn exchange_equity(&self, exchange: ExchangeId) -> Price {
        self.sub_accounts
            .get(&exchange)
            .map(|account| {
                let unrealized: Price = account
                    .positions
                    .values()
                    .map(|position| position.unrealized_pnl)
                    .sum();
                account.balances.total_value() + unrealized
            })
            .unwrap_or_default()
    }

    /// Signed position quantity helper (long positive, short negative).
    #[must_use]
    pub fn signed_position_qty(&self, symbol: impl Into<Symbol>) -> Quantity {
        let symbol = symbol.into();
        self.sub_accounts
            .get(&symbol.exchange)
            .and_then(|account| account.positions.get(&symbol))
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
            positions: self.aggregate_positions_map(),
            balances: self.aggregate_balances(),
            reporting_currency: self.reporting_currency,
            initial_equity: self.initial_equity,
            drawdown_limit: self.drawdown_limit,
            peak_equity: self.peak_equity,
            liquidate_only: self.liquidate_only,
            sub_accounts: self
                .sub_accounts
                .iter()
                .map(|(exchange, account)| {
                    (
                        *exchange,
                        SubAccountState {
                            exchange: *exchange,
                            balances: account.balances.clone(),
                            positions: account.positions.clone(),
                        },
                    )
                })
                .collect(),
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
        let mut portfolio = Self {
            sub_accounts: HashMap::new(),
            reporting_currency: state.reporting_currency,
            initial_equity: state.initial_equity,
            drawdown_limit,
            peak_equity: cmp::max(state.peak_equity, state.initial_equity),
            liquidate_only: state.liquidate_only,
            market_registry: registry,
        };
        if state.sub_accounts.is_empty() {
            for (symbol, position) in state.positions {
                let account = portfolio.account_mut(symbol.exchange);
                account.positions.insert(symbol, position);
            }
            for (asset, cash) in state.balances.iter() {
                let account = portfolio.account_mut(asset.exchange);
                account.balances.upsert(cash.clone());
            }
        } else {
            for (exchange, snapshot) in state.sub_accounts {
                let mut account = SubAccount::new(exchange);
                account.positions = snapshot.positions;
                account.balances = snapshot.balances;
                portfolio.sub_accounts.insert(exchange, account);
            }
        }
        portfolio.ensure_reporting_currency_entries();
        portfolio.update_drawdown_state();
        portfolio
    }

    /// Refresh mark-to-market pricing and conversion rates for a symbol.
    pub fn update_market_data(
        &mut self,
        symbol: impl Into<Symbol>,
        price: Price,
    ) -> PortfolioResult<bool> {
        let symbol = symbol.into();
        let instrument = self
            .market_registry
            .get(symbol)
            .ok_or(PortfolioError::UnknownSymbol(symbol))?;
        let account = self.account_mut(symbol.exchange);
        let mut updated = false;
        if let Some(position) = account.positions.get_mut(&symbol) {
            update_unrealized(position, &instrument, price);
            updated = true;
        }
        self.update_conversion_rates(account, &instrument, price);
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

    fn apply_cash_flow(
        &mut self,
        account: &mut SubAccount,
        instrument: &Instrument,
        fill: &Fill,
        realized: Price,
    ) {
        match instrument.kind {
            InstrumentKind::Spot => self.apply_spot_flow(account, instrument, fill),
            InstrumentKind::LinearPerpetual | InstrumentKind::InversePerpetual => {
                self.apply_derivative_flow(account, instrument, fill, realized)
            }
        }
        if let Some(fee) = fill.fee {
            let fee_currency = fill.fee_asset.unwrap_or_else(|| match instrument.kind {
                InstrumentKind::Spot => instrument.quote,
                _ => instrument.settlement_currency,
            });
            account.ensure_currency(self.reporting_currency, fee_currency);
            account.balances.adjust(fee_currency, -fee);
        }
    }

    fn apply_spot_flow(&mut self, account: &mut SubAccount, instrument: &Instrument, fill: &Fill) {
        let notional = fill.fill_price * fill.fill_quantity;
        match fill.side {
            Side::Buy => {
                account.ensure_currency(self.reporting_currency, instrument.base);
                account.ensure_currency(self.reporting_currency, instrument.quote);
                account.balances.adjust(instrument.base, fill.fill_quantity);
                account.balances.adjust(instrument.quote, -notional);
            }
            Side::Sell => {
                account.ensure_currency(self.reporting_currency, instrument.base);
                account.ensure_currency(self.reporting_currency, instrument.quote);
                account
                    .balances
                    .adjust(instrument.base, -fill.fill_quantity);
                account.balances.adjust(instrument.quote, notional);
            }
        }
    }

    fn apply_derivative_flow(
        &mut self,
        account: &mut SubAccount,
        instrument: &Instrument,
        fill: &Fill,
        realized: Price,
    ) {
        let notional = fill.fill_price * fill.fill_quantity;
        let direction = Decimal::from(fill.side.as_i8());
        let settlement = &instrument.settlement_currency;
        account.ensure_currency(self.reporting_currency, *settlement);
        account
            .balances
            .adjust(*settlement, -(notional * direction));
        if !realized.is_zero() {
            account.balances.adjust(*settlement, realized);
        }
    }

    fn update_conversion_rates(
        &mut self,
        account: &mut SubAccount,
        instrument: &Instrument,
        price: Price,
    ) {
        if instrument.quote == self.reporting_currency {
            account.ensure_currency(self.reporting_currency, instrument.base);
            account.ensure_currency(self.reporting_currency, instrument.quote);
            account
                .balances
                .update_conversion_rate(instrument.base, price);
            account
                .balances
                .update_conversion_rate(instrument.quote, Decimal::ONE);
            return;
        }
        if instrument.base == self.reporting_currency && !price.is_zero() {
            account.ensure_currency(self.reporting_currency, instrument.quote);
            account.ensure_currency(self.reporting_currency, instrument.base);
            account
                .balances
                .update_conversion_rate(instrument.base, Decimal::ONE);
            account
                .balances
                .update_conversion_rate(instrument.quote, Decimal::ONE / price);
            return;
        }
        let quote_rate = account
            .balances
            .get(instrument.quote)
            .map(|cash| cash.conversion_rate)
            .unwrap_or(Decimal::ZERO);
        if quote_rate > Decimal::ZERO {
            account.ensure_currency(self.reporting_currency, instrument.base);
            account
                .balances
                .update_conversion_rate(instrument.base, price * quote_rate);
        }
        let base_rate = account
            .balances
            .get(instrument.base)
            .map(|cash| cash.conversion_rate)
            .unwrap_or(Decimal::ZERO);
        if base_rate > Decimal::ZERO && !price.is_zero() {
            account.ensure_currency(self.reporting_currency, instrument.quote);
            account
                .balances
                .update_conversion_rate(instrument.quote, base_rate / price);
        }
    }

    fn account_mut(&mut self, exchange: ExchangeId) -> &mut SubAccount {
        self.sub_accounts.entry(exchange).or_insert_with(|| {
            let mut account = SubAccount::new(exchange);
            account.ensure_currency(self.reporting_currency, self.reporting_currency);
            account
        })
    }

    fn account(&self, exchange: ExchangeId) -> Option<&SubAccount> {
        self.sub_accounts.get(&exchange)
    }

    fn ensure_reporting_currency_entries(&mut self) {
        let reporting = self.reporting_currency;
        for account in self.sub_accounts.values_mut() {
            account.ensure_currency(reporting, reporting);
        }
    }

    fn aggregate_positions_map(&self) -> HashMap<Symbol, Position> {
        self.sub_accounts
            .values()
            .flat_map(|account| account.positions.iter())
            .map(|(symbol, position)| (*symbol, position.clone()))
            .collect()
    }

    fn aggregate_balances(&self) -> CashBook {
        let mut combined = CashBook::new();
        for account in self.sub_accounts.values() {
            for (asset, cash) in account.balances.iter() {
                let entry = combined.0.entry(*asset).or_insert(Cash {
                    currency: *asset,
                    quantity: Decimal::ZERO,
                    conversion_rate: cash.conversion_rate,
                });
                entry.quantity += cash.quantity;
                if cash.conversion_rate > Decimal::ZERO {
                    entry.conversion_rate = cash.conversion_rate;
                }
            }
        }
        combined
    }

    fn total_unrealized(&self) -> Price {
        self.sub_accounts
            .values()
            .flat_map(|account| account.positions.values())
            .map(|position| position.unrealized_pnl)
            .sum()
    }

    fn cash_value(&self) -> Price {
        self.sub_accounts
            .values()
            .map(|account| account.balances.total_value())
            .sum()
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

fn matches_reporting_currency(currency: AssetId, reporting: AssetId) -> bool {
    if currency == reporting {
        return true;
    }
    reporting.exchange == ExchangeId::UNSPECIFIED && currency.code() == reporting.code()
}

fn conversion_rate_for_currency(currency: AssetId, reporting: AssetId) -> Price {
    if matches_reporting_currency(currency, reporting) {
        Decimal::ONE
    } else {
        Decimal::ZERO
    }
}

/// Snapshot of a single venue's ledger/positions for persistence.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct SubAccountState {
    pub exchange: ExchangeId,
    pub positions: HashMap<Symbol, Position>,
    pub balances: CashBook,
}

/// Serializable representation of a portfolio used for persistence.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PortfolioState {
    pub positions: HashMap<Symbol, Position>,
    pub balances: CashBook,
    pub reporting_currency: AssetId,
    pub initial_equity: Price,
    pub drawdown_limit: Option<Decimal>,
    pub peak_equity: Price,
    pub liquidate_only: bool,
    #[serde(default)]
    pub sub_accounts: HashMap<ExchangeId, SubAccountState>,
}

/// Durable snapshot of the live trading runtime persisted on disk.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LiveState {
    pub portfolio: Option<PortfolioState>,
    pub open_orders: Vec<Order>,
    pub last_prices: HashMap<Symbol, Price>,
    pub last_candle_ts: Option<DateTime<Utc>>,
    pub strategy_state: Option<serde_json::Value>,
}

/// Abstraction over state persistence backends.
pub trait StateRepository: Send + Sync + 'static {
    /// Structured snapshot stored in durable persistence.
    type Snapshot: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Load the most recent state from durable storage or defaults if none exists.
    fn load(&self) -> PortfolioResult<Self::Snapshot>;
    /// Atomically save the provided state snapshot.
    fn save(&self, state: &Self::Snapshot) -> PortfolioResult<()>;
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
    type Snapshot = LiveState;

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
            fee_asset: None,
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
            .update_market_data(buy.symbol, Decimal::ZERO)
            .unwrap();
        assert!(portfolio.liquidate_only());
    }
}
