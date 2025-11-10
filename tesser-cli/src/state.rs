use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use chrono::SecondsFormat;
use serde_json::to_string_pretty;
use tesser_core::Side;
use tesser_portfolio::{LiveState, PortfolioState, SqliteStateRepository, StateRepository};

const MAX_ORDER_ROWS: usize = 5;
const MAX_PRICE_ROWS: usize = 8;

pub async fn inspect_state(path: PathBuf, raw: bool) -> Result<()> {
    let repo = SqliteStateRepository::new(path.clone());
    let state = tokio::task::spawn_blocking(move || repo.load())
        .await
        .map_err(|err| anyhow!("state inspection task failed: {err}"))?
        .map_err(|err| anyhow!(err.to_string()))?;
    if raw {
        println!("{}", to_string_pretty(&state)?);
    } else {
        print_summary(&path, &state);
    }
    Ok(())
}

fn print_summary(path: &Path, state: &LiveState) {
    println!("State database: {}", path.display());
    if let Some(ts) = state.last_candle_ts {
        println!(
            "Last candle timestamp: {}",
            ts.to_rfc3339_opts(SecondsFormat::Secs, true)
        );
    } else {
        println!("Last candle timestamp: <none>");
    }
    if let Some(portfolio) = &state.portfolio {
        print_portfolio(portfolio);
    } else {
        println!("Portfolio snapshot: <empty>");
    }

    println!("Open orders ({} total):", state.open_orders.len());
    if state.open_orders.is_empty() {
        println!("  none");
    } else {
        for order in state.open_orders.iter().take(MAX_ORDER_ROWS) {
            println!(
                "  {} {} {} @ {:?} status={:?} filled={:.4}",
                order.id,
                order.request.symbol,
                format_side(Some(order.request.side)),
                order.request.price,
                order.status,
                order.filled_quantity
            );
        }
        if state.open_orders.len() > MAX_ORDER_ROWS {
            println!(
                "  ... {} additional order(s) omitted",
                state.open_orders.len() - MAX_ORDER_ROWS
            );
        }
    }

    println!("Last price cache ({} symbol(s)):", state.last_prices.len());
    if state.last_prices.is_empty() {
        println!("  none");
    } else {
        let mut entries: Vec<_> = state.last_prices.iter().collect();
        entries.sort_by(|(lhs, _), (rhs, _)| lhs.cmp(rhs));
        for (symbol, price) in entries.into_iter().take(MAX_PRICE_ROWS) {
            println!("  {symbol}: {price:.6}");
        }
        if state.last_prices.len() > MAX_PRICE_ROWS {
            println!(
                "  ... {} additional symbol(s) omitted",
                state.last_prices.len() - MAX_PRICE_ROWS
            );
        }
    }
}

fn print_portfolio(portfolio: &PortfolioState) {
    let equity = portfolio.cash
        + portfolio.realized_pnl
        + portfolio
            .positions
            .values()
            .map(|pos| pos.unrealized_pnl)
            .sum::<f64>();
    println!("Portfolio snapshot:");
    println!("  Cash: {:.2}", portfolio.cash);
    println!("  Realized PnL: {:.2}", portfolio.realized_pnl);
    println!("  Equity: {:.2}", equity);
    println!(
        "  Peak equity: {:.2} (liquidate_only={})",
        portfolio.peak_equity, portfolio.liquidate_only
    );
    if let Some(limit) = portfolio.drawdown_limit {
        println!("  Drawdown limit: {:.2}%", limit * 100.0);
    }

    if portfolio.positions.is_empty() {
        println!("  Positions: none");
        return;
    }
    println!("  Positions:");
    let mut positions: Vec<_> = portfolio.positions.iter().collect();
    positions.sort_by(|(lhs, _), (rhs, _)| lhs.cmp(rhs));
    for (symbol, position) in positions {
        println!(
            "    {:<12} side={} qty={:.4} entry={:.4?} unrealized={:.2} updated={}",
            symbol,
            format_side(position.side),
            position.quantity,
            position.entry_price,
            position.unrealized_pnl,
            position
                .updated_at
                .to_rfc3339_opts(SecondsFormat::Secs, true)
        );
    }
}

fn format_side(side: Option<Side>) -> &'static str {
    match side {
        Some(Side::Buy) => "Buy",
        Some(Side::Sell) => "Sell",
        None => "Flat",
    }
}
