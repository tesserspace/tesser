use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use chrono::SecondsFormat;
use rust_decimal::Decimal;
use serde_json::to_string_pretty;
use tesser_config::PersistenceEngine;
use tesser_core::{CashBook, Position, Side, Symbol};
use tesser_journal::LmdbJournal;
use tesser_portfolio::{LiveState, PortfolioState, SqliteStateRepository, StateRepository};

const MAX_ORDER_ROWS: usize = 5;
const MAX_PRICE_ROWS: usize = 8;

pub async fn inspect_state(path: PathBuf, engine: PersistenceEngine, raw: bool) -> Result<()> {
    let (state, resolved_path) = match engine {
        PersistenceEngine::Sqlite => {
            let repo = SqliteStateRepository::new(path.clone());
            let state = tokio::task::spawn_blocking(move || repo.load())
                .await
                .map_err(|err| anyhow!("state inspection task failed: {err}"))?
                .map_err(|err| anyhow!(err.to_string()))?;
            (state, path)
        }
        PersistenceEngine::Lmdb => {
            let journal = LmdbJournal::open(&path)
                .map_err(|err| anyhow!("failed to open LMDB journal: {err}"))?;
            let repo = journal.state_repo();
            let state = tokio::task::spawn_blocking(move || repo.load())
                .await
                .map_err(|err| anyhow!("state inspection task failed: {err}"))?
                .map_err(|err| anyhow!(err.to_string()))?;
            (state, journal.path().to_path_buf())
        }
    };
    if raw {
        println!("{}", to_string_pretty(&state)?);
    } else {
        print_summary(&resolved_path, &state);
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
        entries.sort_by_key(|(symbol, _)| (symbol.exchange.as_raw(), symbol.market_id));
        for (symbol, price) in entries.into_iter().take(MAX_PRICE_ROWS) {
            println!("  {symbol}: {price}");
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
    let unrealized: Decimal = portfolio
        .positions
        .values()
        .map(|pos| pos.unrealized_pnl)
        .sum();
    let cash_value = portfolio.balances.total_value();
    let equity = cash_value + unrealized;
    let realized = equity - portfolio.initial_equity - unrealized;
    let reporting_cash = portfolio
        .balances
        .get(portfolio.reporting_currency)
        .map(|cash| cash.quantity)
        .unwrap_or_default();
    println!("Portfolio snapshot:");
    println!(
        "  Cash ({}): {:.2}",
        portfolio.reporting_currency, reporting_cash
    );
    println!("  Realized PnL: {:.2}", realized);
    println!("  Equity: {:.2}", equity);
    println!(
        "  Peak equity: {:.2} (liquidate_only={})",
        portfolio.peak_equity, portfolio.liquidate_only
    );
    if let Some(limit) = portfolio.drawdown_limit {
        println!("  Drawdown limit: {:.2}%", limit * Decimal::from(100));
    }

    if !portfolio.sub_accounts.is_empty() {
        println!("  Venue breakdown:");
        let mut venues: Vec<_> = portfolio.sub_accounts.values().collect();
        venues.sort_by_key(|acct| acct.exchange.as_raw());
        for account in venues {
            let unrealized: Decimal = account
                .positions
                .values()
                .map(|pos| pos.unrealized_pnl)
                .sum();
            let equity = account.balances.total_value() + unrealized;
            println!(
                "    {:<12} equity={:.2} balances={} positions={}",
                account.exchange,
                equity,
                account.balances.iter().count(),
                account.positions.len()
            );
            print_balances("      Balances", &account.balances);
            print_positions("      Positions", &account.positions);
        }
    }

    print_balances("Balances", &portfolio.balances);
    print_positions("Positions", &portfolio.positions);
}

fn format_side(side: Option<Side>) -> &'static str {
    match side {
        Some(Side::Buy) => "Buy",
        Some(Side::Sell) => "Sell",
        None => "Flat",
    }
}

fn print_balances(label: &str, balances: &CashBook) {
    if balances.iter().any(|(_, cash)| !cash.quantity.is_zero()) {
        println!("  {label}:");
        let mut entries: Vec<_> = balances.iter().collect();
        entries.sort_by_key(|(asset, _)| (asset.exchange.as_raw(), asset.asset_id));
        for (currency, cash) in entries {
            println!(
                "    {:<8} qty={:.6} rate={:.6}",
                currency, cash.quantity, cash.conversion_rate
            );
        }
    }
}

fn print_positions(label: &str, positions: &std::collections::HashMap<Symbol, Position>) {
    if positions.is_empty() {
        println!("  {label}: none");
        return;
    }
    println!("  {label}:");
    let mut entries: Vec<_> = positions.iter().collect();
    entries.sort_by_key(|(symbol, _)| (symbol.exchange.as_raw(), symbol.market_id));
    for (symbol, position) in entries {
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
