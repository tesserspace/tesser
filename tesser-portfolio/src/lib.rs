//! Portfolio accounting primitives.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tesser_core::{Fill, Position, Price, Symbol};
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
}

impl Default for PortfolioConfig {
    fn default() -> Self {
        Self {
            initial_equity: 10_000.0,
        }
    }
}

/// Stores aggregate positions keyed by symbol.
pub struct Portfolio {
    positions: HashMap<Symbol, Position>,
    cash: Price,
    realized_pnl: Price,
    initial_equity: Price,
}

impl Portfolio {
    /// Instantiate a new portfolio with default configuration.
    pub fn new(config: PortfolioConfig) -> Self {
        Self {
            cash: config.initial_equity,
            positions: HashMap::new(),
            realized_pnl: 0.0,
            initial_equity: config.initial_equity,
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
                        tesser_core::Side::Buy => entry_price - fill.fill_price,
                        tesser_core::Side::Sell => fill.fill_price - entry_price,
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
