//! Portfolio accounting primitives.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tesser_core::{Fill, Position, Price, Side, Symbol};
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
    pub initial_equity: Price,
    pub max_drawdown: Option<f64>,
}

impl Default for PortfolioConfig {
    fn default() -> Self {
        Self {
            initial_equity: 10_000.0,
            max_drawdown: None,
        }
    }
}

/// Stores aggregate positions keyed by symbol.
pub struct Portfolio {
    positions: HashMap<Symbol, Position>,
    cash: Price,
    realized_pnl: Price,
    initial_equity: Price,
    drawdown_limit: Option<f64>,
    peak_equity: Price,
    liquidate_only: bool,
}

impl Portfolio {
    /// Instantiate a new portfolio with default configuration.
    pub fn new(config: PortfolioConfig) -> Self {
        let limit = config
            .max_drawdown
            .filter(|value| value.is_finite() && *value > 0.0);
        Self {
            cash: config.initial_equity,
            positions: HashMap::new(),
            realized_pnl: 0.0,
            initial_equity: config.initial_equity,
            drawdown_limit: limit,
            peak_equity: config.initial_equity,
            liquidate_only: false,
        }
    }

    /// Apply a trade fill to the internal bookkeeping.
    pub fn apply_fill(&mut self, fill: &Fill) -> PortfolioResult<()> {
        let entry = self
            .positions
            .entry(fill.symbol.clone())
            .or_insert(Position {
                symbol: fill.symbol.clone(),
                side: Some(fill.side),
                quantity: 0.0,
                entry_price: Some(fill.fill_price),
                unrealized_pnl: 0.0,
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
                entry.entry_price = Some((prev_cost + new_cost) / total_qty.max(f64::EPSILON));
                entry.quantity = total_qty;
            }
            Some(_) => {
                // Closing or flipping the position.
                let remaining = entry.quantity - fill.fill_quantity;
                if let Some(entry_price) = entry.entry_price {
                    let delta = match fill.side {
                        Side::Buy => entry_price - fill.fill_price,
                        Side::Sell => fill.fill_price - entry_price,
                    };
                    self.realized_pnl += delta * fill.fill_quantity;
                }
                entry.quantity = remaining.abs();
                entry.side = if remaining > 0.0 {
                    entry.side
                } else if remaining < 0.0 {
                    Some(fill.side)
                } else {
                    None
                };
                entry.entry_price = if entry.quantity > 0.0 {
                    Some(fill.fill_price)
                } else {
                    None
                };
            }
            None => {
                entry.side = Some(fill.side);
                entry.quantity = fill.fill_quantity;
                entry.entry_price = Some(fill.fill_price);
            }
        }

        self.cash -= fill.fill_price * fill.fill_quantity * fill.side.as_i8() as f64;
        if let Some(fee) = fill.fee {
            self.cash -= fee;
        }

        entry.updated_at = fill.timestamp;
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
        self.cash + self.realized_pnl + unrealized
    }

    /// Cash on hand that is not locked in positions.
    #[must_use]
    pub fn cash(&self) -> Price {
        self.cash
    }

    /// Realized profit and loss across all closed positions.
    #[must_use]
    pub fn realized_pnl(&self) -> Price {
        self.realized_pnl
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
    pub fn signed_position_qty(&self, symbol: &str) -> f64 {
        self.positions
            .get(symbol)
            .map(|position| match position.side {
                Some(Side::Buy) => position.quantity,
                Some(Side::Sell) => -position.quantity,
                None => 0.0,
            })
            .unwrap_or(0.0)
    }

    /// Whether the portfolio currently allows only exposure-reducing orders.
    #[must_use]
    pub fn liquidate_only(&self) -> bool {
        self.liquidate_only
    }

    /// Snapshot the current state for persistence.
    #[must_use]
    pub fn snapshot(&self) -> PortfolioState {
        PortfolioState {
            positions: self.positions.clone(),
            cash: self.cash,
            realized_pnl: self.realized_pnl,
            initial_equity: self.initial_equity,
            drawdown_limit: self.drawdown_limit,
            peak_equity: self.peak_equity,
            liquidate_only: self.liquidate_only,
        }
    }

    /// Rehydrate a portfolio from a persisted snapshot.
    pub fn from_state(state: PortfolioState, config: PortfolioConfig) -> Self {
        let drawdown_limit = config
            .max_drawdown
            .or(state.drawdown_limit)
            .filter(|value| value.is_finite() && *value > 0.0);
        let mut portfolio = Self {
            positions: state.positions,
            cash: state.cash,
            realized_pnl: state.realized_pnl,
            initial_equity: state.initial_equity,
            drawdown_limit,
            peak_equity: state
                .peak_equity
                .max(state.initial_equity)
                .max(config.initial_equity),
            liquidate_only: state.liquidate_only,
        };
        portfolio.update_drawdown_state();
        portfolio
    }

    /// Refresh mark-to-market pricing for a symbol when new data arrives.
    pub fn mark_price(&mut self, symbol: &str, price: Price) {
        if let Some(position) = self.positions.get_mut(symbol) {
            position.mark_price(price);
        }
        self.update_drawdown_state();
    }

    fn update_drawdown_state(&mut self) {
        let equity = self.equity();
        if equity > self.peak_equity {
            self.peak_equity = equity;
        }
        if let Some(limit) = self.drawdown_limit {
            if self.peak_equity > 0.0 {
                let drawdown = (self.peak_equity - equity) / self.peak_equity;
                if drawdown >= limit {
                    self.liquidate_only = true;
                }
            }
        }
    }
}

/// Serializable representation of a portfolio used for persistence.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PortfolioState {
    pub positions: HashMap<Symbol, Position>,
    pub cash: Price,
    pub realized_pnl: Price,
    pub initial_equity: Price,
    pub drawdown_limit: Option<f64>,
    pub peak_equity: Price,
    pub liquidate_only: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tesser_core::{Side, Symbol};

    fn sample_fill(side: Side, price: Price, qty: f64) -> Fill {
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

    #[test]
    fn portfolio_updates_equity() {
        let mut portfolio = Portfolio::new(PortfolioConfig::default());
        let buy = sample_fill(Side::Buy, 50_000.0, 0.1);
        portfolio.apply_fill(&buy).unwrap();
        assert!(portfolio.cash() < 10_000.0);
    }
}
