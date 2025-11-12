use std::fmt;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use tesser_core::{Fill, Side};

const TRADING_DAYS_PER_YEAR: f64 = 252.0;
const RISK_FREE_RATE: f64 = 0.0;

#[derive(Debug)]
struct Trade {
    pnl: f64,
    is_win: bool,
}

#[derive(Debug, Default)]
pub struct PerformanceReport {
    pub total_return_pct: f64,
    pub annualized_return_pct: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub max_drawdown_pct: f64,
    pub calmar_ratio: f64,
    pub total_trades: usize,
    pub win_rate_pct: f64,
    pub avg_win_pct: f64,
    pub avg_loss_pct: f64,
    pub profit_loss_ratio: f64,
    pub ending_equity: f64,
}

impl fmt::Display for PerformanceReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Backtest Performance Report")?;
        writeln!(f, "------------------------------------")?;
        writeln!(f, "{:<25} {:.2}%", "Total Return", self.total_return_pct)?;
        writeln!(
            f,
            "{:<25} {:.2}%",
            "Annualized Return", self.annualized_return_pct
        )?;
        writeln!(f, "{:<25} {:.2}", "Sharpe Ratio", self.sharpe_ratio)?;
        writeln!(f, "{:<25} {:.2}", "Sortino Ratio", self.sortino_ratio)?;
        writeln!(f, "{:<25} {:.2}%", "Max Drawdown", self.max_drawdown_pct)?;
        writeln!(f, "{:<25} {:.2}", "Calmar Ratio", self.calmar_ratio)?;
        writeln!(f, "------------------------------------")?;
        writeln!(f, "{:<25} {}", "Total Trades", self.total_trades)?;
        writeln!(f, "{:<25} {:.2}%", "Win Rate", self.win_rate_pct)?;
        writeln!(
            f,
            "{:<25} {:.4}",
            "Profit/Loss Ratio", self.profit_loss_ratio
        )?;
        writeln!(f, "{:<25} ${:.2}", "Ending Equity", self.ending_equity)?;
        writeln!(f, "------------------------------------")
    }
}

pub struct Reporter {
    initial_equity: f64,
    equity_curve: Vec<(DateTime<Utc>, f64)>,
    fills: Vec<Fill>,
}

impl Reporter {
    pub fn new(
        initial_equity: f64,
        equity_curve: Vec<(DateTime<Utc>, f64)>,
        fills: Vec<Fill>,
    ) -> Self {
        Self {
            initial_equity,
            equity_curve,
            fills,
        }
    }

    pub fn calculate(&self) -> Result<PerformanceReport> {
        if self.equity_curve.len() < 2 {
            return Err(anyhow!("not enough data points to generate a report"));
        }

        let daily_returns = self.calculate_daily_returns();
        let years = self.duration_years();

        let total_return_pct =
            (self.equity_curve.last().unwrap().1 / self.initial_equity - 1.0) * 100.0;
        let annualized_return_pct = if years > 0.0 {
            ((1.0 + total_return_pct / 100.0).powf(1.0 / years) - 1.0) * 100.0
        } else {
            0.0
        };

        let (sharpe_ratio, sortino_ratio) = self.calculate_risk_ratios(&daily_returns);
        let max_drawdown_pct = self.calculate_max_drawdown() * 100.0;
        let calmar_ratio = if max_drawdown_pct.abs() > 0.01 {
            annualized_return_pct / max_drawdown_pct.abs()
        } else {
            0.0
        };

        let trades = self.process_fills_into_trades();
        let total_trades = trades.len();
        let wins = trades.iter().filter(|t| t.is_win).count();
        let win_rate_pct = if total_trades > 0 {
            (wins as f64 / total_trades as f64) * 100.0
        } else {
            0.0
        };

        let total_win_pnl: f64 = trades.iter().filter(|t| t.is_win).map(|t| t.pnl).sum();
        let total_loss_pnl: f64 = trades.iter().filter(|t| !t.is_win).map(|t| t.pnl).sum();
        let losses = total_trades - wins;

        let avg_win_pct = if wins > 0 {
            total_win_pnl / wins as f64
        } else {
            0.0
        };
        let avg_loss_pct = if losses > 0 {
            total_loss_pnl / losses as f64
        } else {
            0.0
        };

        let profit_loss_ratio = if avg_loss_pct.abs() > 1e-9 {
            -avg_win_pct / avg_loss_pct
        } else {
            f64::INFINITY
        };

        Ok(PerformanceReport {
            total_return_pct,
            annualized_return_pct,
            sharpe_ratio,
            sortino_ratio,
            max_drawdown_pct,
            calmar_ratio,
            total_trades,
            win_rate_pct,
            avg_win_pct: avg_win_pct * 100.0,
            avg_loss_pct: avg_loss_pct * 100.0,
            profit_loss_ratio,
            ending_equity: self.equity_curve.last().unwrap().1,
        })
    }

    fn calculate_daily_returns(&self) -> Vec<f64> {
        self.equity_curve
            .iter()
            .chunk_by(|(ts, _)| ts.date_naive())
            .into_iter()
            .filter_map(|(_, group)| group.last())
            .map(|(_, equity)| *equity)
            .collect::<Vec<_>>()
            .windows(2)
            .map(|w| (w[1] / w[0]) - 1.0)
            .collect()
    }

    fn duration_years(&self) -> f64 {
        let start = self.equity_curve.first().unwrap().0;
        let end = self.equity_curve.last().unwrap().0;
        (end - start).num_days() as f64 / 365.25
    }

    fn calculate_risk_ratios(&self, daily_returns: &[f64]) -> (f64, f64) {
        let n = daily_returns.len() as f64;
        if n < 2.0 {
            return (0.0, 0.0);
        }
        let mean_return = daily_returns.iter().sum::<f64>() / n;
        let excess_return = mean_return - (RISK_FREE_RATE / TRADING_DAYS_PER_YEAR);

        let std_dev = (daily_returns
            .iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>()
            / (n - 1.0))
            .sqrt();

        let downside_returns: Vec<_> = daily_returns.iter().filter(|&&r| r < 0.0).collect();
        let downside_dev = if downside_returns.len() > 1 {
            (downside_returns.iter().map(|r| r.powi(2)).sum::<f64>()
                / downside_returns.len() as f64)
                .sqrt()
        } else {
            0.0
        };

        let sharpe = if std_dev > 1e-9 {
            (excess_return * TRADING_DAYS_PER_YEAR.sqrt()) / std_dev
        } else {
            0.0
        };

        let sortino = if downside_dev > 1e-9 {
            (excess_return * TRADING_DAYS_PER_YEAR.sqrt()) / downside_dev
        } else {
            0.0
        };

        (sharpe, sortino)
    }

    fn calculate_max_drawdown(&self) -> f64 {
        let mut peak = self.initial_equity;
        let mut max_drawdown = 0.0;
        for &(_, equity) in &self.equity_curve {
            if equity > peak {
                peak = equity;
            }
            let drawdown = (peak - equity) / peak;
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }
        }
        max_drawdown
    }

    fn process_fills_into_trades(&self) -> Vec<Trade> {
        // This is a simplified implementation assuming single symbol and no partial exits.
        // A robust implementation would need to track position entries more carefully.
        self.fills
            .windows(2)
            .filter_map(|w| {
                let (open, close) = (&w[0], &w[1]);
                if open.side != close.side {
                    let pnl = match open.side {
                        Side::Buy => close.fill_price - open.fill_price,
                        Side::Sell => open.fill_price - close.fill_price,
                    };
                    Some(Trade {
                        pnl: pnl / open.fill_price,
                        is_win: pnl > 0.0,
                    })
                } else {
                    None
                }
            })
            .collect()
    }
}
