use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use tesser_core::{AssetId, Cash, CashBook, ExchangeId, Position, Side, Symbol};
use tesser_portfolio::{PortfolioState, SubAccountState};
use tesser_rpc::proto::PortfolioSnapshot;

#[test]
fn portfolio_snapshot_includes_sub_accounts() {
    let binance = ExchangeId::from("binance_perp");
    let bybit = ExchangeId::from("bybit_linear");
    let mut state = PortfolioState {
        reporting_currency: AssetId::from("USDT"),
        initial_equity: Decimal::from(10_000),
        peak_equity: Decimal::from(10_500),
        ..PortfolioState::default()
    };
    state.sub_accounts.insert(
        binance,
        build_sub_account(binance, "BTCUSDT", "USDT", Decimal::from(6_000)),
    );
    state.sub_accounts.insert(
        bybit,
        build_sub_account(bybit, "ETHUSDT", "USDT", Decimal::from(4_000)),
    );

    let snapshot: PortfolioSnapshot = (&state).into();

    assert_eq!(
        snapshot.sub_accounts.len(),
        2,
        "sub_accounts should mirror each exchange"
    );
    let exchanges: HashSet<_> = snapshot
        .sub_accounts
        .iter()
        .map(|acct| acct.exchange.clone())
        .collect();
    assert!(exchanges.contains("binance_perp"));
    assert!(exchanges.contains("bybit_linear"));
    assert!(
        snapshot
            .sub_accounts
            .iter()
            .all(|acct| !acct.balances.is_empty() && !acct.positions.is_empty()),
        "each sub_account should carry balances and positions"
    );
    assert_eq!(
        snapshot.positions.len(),
        2,
        "global positions aggregate per-exchange entries"
    );
    assert_eq!(
        snapshot.balances.len(),
        2,
        "global balances aggregate per-exchange ledgers"
    );
    assert!(
        snapshot.equity.is_some(),
        "equity should be derived from the venue snapshots"
    );
}

fn build_sub_account(
    exchange: ExchangeId,
    symbol_code: &str,
    asset_code: &str,
    cash_qty: Decimal,
) -> SubAccountState {
    let mut balances = CashBook::new();
    let asset = AssetId::from_code(exchange, asset_code);
    balances.upsert(Cash {
        currency: asset,
        quantity: cash_qty,
        conversion_rate: Decimal::ONE,
    });

    let mut positions = HashMap::new();
    let symbol = Symbol::from_code(exchange, symbol_code);
    positions.insert(
        symbol,
        Position {
            symbol,
            side: Some(Side::Buy),
            quantity: Decimal::new(1, 0),
            entry_price: Some(Decimal::from(25_000)),
            unrealized_pnl: Decimal::from(150),
            updated_at: Utc::now(),
        },
    );

    SubAccountState {
        exchange,
        positions,
        balances,
    }
}
