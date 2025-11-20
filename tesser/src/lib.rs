#![allow(ambiguous_glob_reexports)]

//! Tesser aggregate crate that re-exports the main components for downstream users.

pub use tesser_backtester as backtester;
pub use tesser_binance as binance;
pub use tesser_broker as broker;
pub use tesser_bybit as bybit;
pub use tesser_cli;
pub use tesser_config as config;
pub use tesser_core as core;
pub use tesser_data as data;
pub use tesser_events as events;
pub use tesser_execution as execution;
pub use tesser_indicators as indicators;
pub use tesser_markets as markets;
pub use tesser_paper as paper;
pub use tesser_portfolio as portfolio;
pub use tesser_strategy as strategy;
pub use tesser_strategy_macros as strategy_macros;

/// Convenience entrypoint to run the CLI directly from the facade crate.
pub async fn run_cli() -> anyhow::Result<()> {
    tesser_cli::run_app().await
}

/// Convenience prelude to pull commonly used items into scope.
pub mod prelude {
    pub use tesser_backtester::{BacktestConfig, BacktestMode, BacktestReport, Backtester};
    pub use tesser_binance::*;
    pub use tesser_broker::*;
    pub use tesser_bybit::*;
    pub use tesser_config::*;
    pub use tesser_core::*;
    pub use tesser_data::*;
    pub use tesser_events::*;
    pub use tesser_execution::*;
    pub use tesser_indicators::*;
    pub use tesser_markets::*;
    pub use tesser_paper::*;
    pub use tesser_portfolio::*;
    pub use tesser_strategy::{register_strategy, Strategy, *};
}
