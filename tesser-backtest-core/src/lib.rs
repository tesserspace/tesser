//! Deterministic backtest core for the browser playground.
//!
//! Industry-practice default semantics:
//! - Strategy decides on bar close `i`.
//! - Rebalance executes at next bar open `i+1` (no lookahead).

use anyhow::{anyhow, Result};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestOptions {
    pub initial_equity: f64,
    pub fee_bps: f64,
    pub slippage_bps: f64,
    pub allow_short: bool,
}

impl Default for BacktestOptions {
    fn default() -> Self {
        Self {
            initial_equity: 10_000.0,
            fee_bps: 0.0,
            slippage_bps: 0.0,
            allow_short: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Candle {
    /// Unix seconds (UTC), bar open timestamp.
    pub t: i64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub v: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub time: i64,
    pub side: TradeSide,
    pub price: f64,
    pub quantity: f64,
    pub fee: f64,
    pub target_weight_after: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub realized_pnl: Option<f64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum TradeSide {
    #[serde(rename = "BUY")]
    Buy,
    #[serde(rename = "SELL")]
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EquityPoint {
    pub time: i64,
    pub equity: f64,
    pub target_weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestMetrics {
    pub bars: usize,
    pub trades: usize,
    pub total_return_pct: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cagr_pct: Option<f64>,
    pub max_drawdown_pct: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sharpe: Option<f64>,
    pub fees_paid: f64,
    pub realized_pnl: f64,
    pub start_time: i64,
    pub end_time: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
    pub equity: Vec<EquityPoint>,
    pub trades: Vec<Trade>,
    pub metrics: BacktestMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestInput {
    pub candles: Vec<Candle>,
    pub options: BacktestOptions,
}

fn clamp_target(value: f64, allow_short: bool) -> f64 {
    if !value.is_finite() {
        return 0.0;
    }
    if allow_short {
        value.clamp(-1.0, 1.0)
    } else {
        value.clamp(0.0, 1.0)
    }
}

fn dec(v: f64) -> Result<Decimal> {
    Decimal::from_f64(v).ok_or_else(|| anyhow!("invalid number: {v}"))
}

fn compute_max_drawdown_pct(equity: &[f64]) -> f64 {
    let mut peak = f64::NEG_INFINITY;
    let mut max_dd = 0.0;
    for &value in equity {
        if value > peak {
            peak = value;
        }
        if peak.is_finite() && peak > 0.0 {
            let dd = (peak - value) / peak;
            if dd > max_dd {
                max_dd = dd;
            }
        }
    }
    max_dd * 100.0
}

fn compute_sharpe(daily_returns: &[f64]) -> Option<f64> {
    if daily_returns.len() < 2 {
        return None;
    }
    let mean = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;
    let mut var = 0.0;
    for &r in daily_returns {
        var += (r - mean) * (r - mean);
    }
    var /= (daily_returns.len() - 1) as f64;
    let std = var.sqrt();
    if std == 0.0 {
        None
    } else {
        Some((252.0_f64.sqrt() * mean) / std)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyContext {
    pub i: usize,
    pub bar: Candle,
    pub equity: f64,
    pub position_weight: f64,
    pub target_weight: f64,
}

/// Run a candle backtest using next-open fills.
///
/// The provided strategy is called on **bar close** `i`, and any returned target is executed at
/// **next bar open** `i+1`.
pub fn run_backtest<F>(input: BacktestInput, mut strategy: F) -> Result<BacktestResult>
where
    F: FnMut(StrategyContext) -> Option<f64>,
{
    let candles = input.candles;
    let options = input.options;

    if candles.is_empty() {
        return Ok(BacktestResult {
            equity: Vec::new(),
            trades: Vec::new(),
            metrics: BacktestMetrics {
                bars: 0,
                trades: 0,
                total_return_pct: 0.0,
                cagr_pct: None,
                max_drawdown_pct: 0.0,
                sharpe: None,
                fees_paid: 0.0,
                realized_pnl: 0.0,
                start_time: 0,
                end_time: 0,
            },
        });
    }
    let initial_equity = dec(options.initial_equity)?;
    let fee_rate = dec(options.fee_bps)? / dec(10_000.0)?;
    let slip_rate = dec(options.slippage_bps)? / dec(10_000.0)?;

    let mut cash = initial_equity;
    let mut position_units = Decimal::ZERO;

    let mut avg_entry_price = Decimal::ZERO;

    let mut fees_paid = Decimal::ZERO;
    let mut realized_pnl = Decimal::ZERO;

    let mut target_weight = Decimal::ZERO;
    let mut pending_target: Option<Decimal> = None;

    let mut trades: Vec<Trade> = Vec::new();
    let mut equity: Vec<EquityPoint> = Vec::with_capacity(candles.len());

    for i in 0..candles.len() {
        let bar = candles[i];

        // Execute pending rebalance at bar open (except for bar 0).
        if i > 0 {
            if let Some(next_target) = pending_target.take() {
                let open = dec(bar.o)?;
                if open > Decimal::ZERO {
                    let is_buy = next_target > target_weight;
                    let exec_price = if is_buy {
                        open * (Decimal::ONE + slip_rate)
                    } else {
                        open * (Decimal::ONE - slip_rate)
                    };

                    let mark_price_prev_close = dec(candles[i - 1].c)?;
                    let equity_at_signal = cash + position_units * mark_price_prev_close;

                    let desired_notional = next_target * equity_at_signal;
                    let desired_units = desired_notional / exec_price;
                    let delta_units = desired_units - position_units;

                    // Skip tiny adjustments.
                    if delta_units != Decimal::ZERO {
                        let side = if delta_units > Decimal::ZERO {
                            TradeSide::Buy
                        } else {
                            TradeSide::Sell
                        };
                        let quantity = delta_units.abs();
                        let notional = quantity * exec_price;
                        let fee = notional * fee_rate;

                        // Update cash/position.
                        cash -= delta_units * exec_price;
                        cash -= fee;
                        position_units += delta_units;
                        fees_paid += fee;

                        let trade_realized = apply_realized_pnl(
                            position_units - delta_units,
                            position_units,
                            &mut avg_entry_price,
                            exec_price,
                        );
                        realized_pnl += trade_realized.unwrap_or(Decimal::ZERO);

                        trades.push(Trade {
                            time: bar.t,
                            side,
                            price: exec_price.to_f64().unwrap_or(bar.o),
                            quantity: quantity.to_f64().unwrap_or(0.0),
                            fee: fee.to_f64().unwrap_or(0.0),
                            target_weight_after: next_target.to_f64().unwrap_or(0.0),
                            realized_pnl: trade_realized.and_then(|v| v.to_f64()),
                        });
                    }

                    target_weight = next_target;
                }
            }
        }

        // Mark-to-market at bar close.
        let close = dec(bar.c)?;
        let equity_now = cash + position_units * close;
        let equity_f64 = equity_now.to_f64().unwrap_or(0.0);
        equity.push(EquityPoint {
            time: bar.t,
            equity: equity_f64,
            target_weight: target_weight.to_f64().unwrap_or(0.0),
        });

        // Strategy decision at bar close, schedules execution at next open.
        // Skip scheduling on last bar (no next open).
        if i + 1 < candles.len() {
            let position_value = (position_units * close).to_f64().unwrap_or(0.0);
            let position_weight = if equity_f64 == 0.0 {
                0.0
            } else {
                position_value / equity_f64
            };
            let ctx = StrategyContext {
                i,
                bar,
                equity: equity_f64,
                position_weight,
                target_weight: target_weight.to_f64().unwrap_or(0.0),
            };
            if let Some(target) = strategy(ctx) {
                let clamped = clamp_target(target, options.allow_short);
                let next = dec(clamped)?;
                if next != target_weight {
                    pending_target = Some(next);
                } else {
                    pending_target = None;
                }
            }
        }
    }

    let equity_values: Vec<f64> = equity.iter().map(|p| p.equity).collect();
    let mut daily_returns = Vec::with_capacity(equity_values.len().saturating_sub(1));
    for i in 1..equity_values.len() {
        let prev = equity_values[i - 1];
        let next = equity_values[i];
        daily_returns.push(if prev == 0.0 { 0.0 } else { next / prev - 1.0 });
    }

    let end_equity = equity_values
        .last()
        .copied()
        .unwrap_or(options.initial_equity);
    let total_return_pct = if options.initial_equity == 0.0 {
        0.0
    } else {
        (end_equity / options.initial_equity - 1.0) * 100.0
    };

    let start_time = candles.first().map(|c| c.t).unwrap_or(0);
    let end_time = candles.last().map(|c| c.t).unwrap_or(0);
    let years = ((end_time - start_time) as f64 / (365.25 * 24.0 * 60.0 * 60.0)).max(0.0);
    let cagr_pct = if years > 0.0 && options.initial_equity > 0.0 {
        Some(((end_equity / options.initial_equity).powf(1.0 / years) - 1.0) * 100.0)
    } else {
        None
    };

    let trade_count = trades.len();
    Ok(BacktestResult {
        equity,
        trades,
        metrics: BacktestMetrics {
            bars: candles.len(),
            trades: trade_count,
            total_return_pct,
            cagr_pct,
            max_drawdown_pct: compute_max_drawdown_pct(&equity_values),
            sharpe: compute_sharpe(&daily_returns),
            fees_paid: fees_paid.to_f64().unwrap_or(0.0),
            realized_pnl: realized_pnl.to_f64().unwrap_or(0.0),
            start_time,
            end_time,
        },
    })
}

fn apply_realized_pnl(
    units_before: Decimal,
    units_after: Decimal,
    avg_entry: &mut Decimal,
    exec_price: Decimal,
) -> Option<Decimal> {
    if units_before == units_after {
        return None;
    }

    let was_long = units_before > Decimal::ZERO;
    let was_short = units_before < Decimal::ZERO;
    let is_long = units_after > Decimal::ZERO;
    let is_short = units_after < Decimal::ZERO;

    let qty_before = units_before.abs();
    let qty_after = units_after.abs();
    let qty_delta = (units_after - units_before).abs();

    // Realized PnL is only generated when we reduce/close an existing position.
    let realized = if was_long && units_after < units_before {
        // Selling (reducing long).
        let qty_closed = qty_delta.min(qty_before);
        Some((exec_price - *avg_entry) * qty_closed)
    } else if was_short && units_after > units_before {
        // Buying (reducing short).
        let qty_closed = qty_delta.min(qty_before);
        Some((*avg_entry - exec_price) * qty_closed)
    } else {
        None
    };

    // Update cost basis.
    if units_after == Decimal::ZERO {
        *avg_entry = Decimal::ZERO;
    } else if (was_long && is_long) || (was_short && is_short) {
        // Same direction. Update avg entry only if increasing exposure.
        if qty_after > qty_before {
            let qty_added = qty_after - qty_before;
            if qty_after > Decimal::ZERO {
                *avg_entry = (*avg_entry * qty_before + exec_price * qty_added) / qty_after;
            }
        }
    } else {
        // Reversal: new position opens at exec price.
        *avg_entry = exec_price;
    }

    realized
}
