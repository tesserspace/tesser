use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use assert_cmd::prelude::*;
use chrono::{Duration, Utc};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rust_decimal::{prelude::RoundingStrategy, Decimal};
use tempfile::tempdir;

use arrow::array::{ArrayRef, Decimal128Builder, Int64Builder, StringBuilder};
use arrow::record_batch::RecordBatch;
use tesser_core::{Candle, Interval, Symbol};
use tesser_data::schema::{
    canonical_candle_schema, canonical_decimal_type, CANONICAL_DECIMAL_SCALE_U32,
};

const STRATEGY_CONFIG: &str = r#"
strategy_name = "SmaCross"

[params]
symbol = "BTCUSDT"
fast_period = 3
slow_period = 5
min_samples = 5
"#;

const MULTI_STRATEGY_CONFIG: &str = r#"
strategy_name = "CrossExchangeArb"

[params]
symbol_a = "bybit_linear:BTCUSDT"
symbol_b = "binance_perp:BTCUSDT"
spread_bps = 0.10
exit_bps = 0.05
ichimoku_conversion = 2
ichimoku_base = 4
ichimoku_span_b = 4
"#;

#[test]
fn backtest_runs_with_parquet_inputs() -> Result<()> {
    let temp = tempdir()?;
    let strategy_path = temp.path().join("strategy.toml");
    fs::write(&strategy_path, STRATEGY_CONFIG)?;

    let candles = sample_candles();

    let parquet_path = temp.path().join("bars.parquet");
    write_canonical_parquet(&parquet_path, &candles)?;
    run_backtest(&strategy_path, &parquet_path)?;
    Ok(())
}

#[test]
fn backtest_routes_multi_exchange_strategies() -> Result<()> {
    let temp = tempdir()?;
    let strategy_path = temp.path().join("multi.toml");
    fs::write(&strategy_path, MULTI_STRATEGY_CONFIG)?;

    let parquet_path = temp.path().join("spreads.parquet");
    write_canonical_parquet(&parquet_path, &dual_exchange_candles())?;
    run_backtest(&strategy_path, &parquet_path)?;
    Ok(())
}

fn run_backtest(strategy: &Path, data: &Path) -> Result<()> {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let markets_file = workspace_root.join("config/markets.toml");
    let binary = assert_cmd::cargo::cargo_bin!("tesser-cli");
    let mut cmd = Command::new(binary);
    cmd.current_dir(&workspace_root);
    cmd.args([
        "--env",
        "default",
        "backtest",
        "run",
        "--strategy-config",
        strategy.to_str().unwrap(),
        "--data",
        data.to_str().unwrap(),
        "--markets-file",
        markets_file.to_str().unwrap(),
        "--quantity",
        "0.01",
        "--candles",
        "64",
    ]);
    cmd.assert().success();
    Ok(())
}

fn write_canonical_parquet(path: &Path, candles: &[Candle]) -> Result<()> {
    let schema = canonical_candle_schema();
    let decimal_type = canonical_decimal_type();
    let mut timestamps = Int64Builder::new();
    let mut symbols = StringBuilder::new();
    let mut intervals = StringBuilder::new();
    let mut open = Decimal128Builder::new().with_data_type(decimal_type.clone());
    let mut high = Decimal128Builder::new().with_data_type(decimal_type.clone());
    let mut low = Decimal128Builder::new().with_data_type(decimal_type.clone());
    let mut close = Decimal128Builder::new().with_data_type(decimal_type.clone());
    let mut volume = Decimal128Builder::new().with_data_type(decimal_type.clone());
    for candle in candles {
        let nanos = candle
            .timestamp
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow::anyhow!("timestamp overflow"))?;
        timestamps.append_value(nanos);
        symbols.append_value(candle.symbol.as_ref());
        intervals.append_value(interval_label(candle.interval));
        open.append_value(decimal_to_i128(candle.open)?);
        high.append_value(decimal_to_i128(candle.high)?);
        low.append_value(decimal_to_i128(candle.low)?);
        close.append_value(decimal_to_i128(candle.close)?);
        volume.append_value(decimal_to_i128(candle.volume)?);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(timestamps.finish()),
        Arc::new(symbols.finish()),
        Arc::new(intervals.finish()),
        Arc::new(open.finish()),
        Arc::new(high.finish()),
        Arc::new(low.finish()),
        Arc::new(close.finish()),
        Arc::new(volume.finish()),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn sample_candles() -> Vec<Candle> {
    let base = Utc::now() - Duration::minutes(10);
    (0..8)
        .map(|idx| Candle {
            symbol: "BTCUSDT".into(),
            interval: Interval::OneMinute,
            open: Decimal::new(20_000 + idx as i64, 0),
            high: Decimal::new(20_010 + idx as i64, 0),
            low: Decimal::new(19_990 + idx as i64, 0),
            close: Decimal::new(20_005 + idx as i64, 0),
            volume: Decimal::new(1, 0),
            timestamp: base + Duration::minutes(idx as i64),
        })
        .collect()
}

fn dual_exchange_candles() -> Vec<Candle> {
    let base = Utc::now() - Duration::minutes(10);
    let symbol_a = Symbol::from("bybit_linear:BTCUSDT");
    let symbol_b = Symbol::from("binance_perp:BTCUSDT");
    let mut candles = Vec::new();
    for idx in 0..8 {
        let ts = base + Duration::minutes(idx as i64);
        let price_a = Decimal::new(20_000 + idx as i64 * 5, 0);
        let price_b = Decimal::new(19_900 - idx as i64 * 3, 0);
        candles.push(Candle {
            symbol: symbol_a,
            interval: Interval::OneMinute,
            open: price_a,
            high: price_a + Decimal::ONE,
            low: price_a - Decimal::ONE,
            close: price_a,
            volume: Decimal::ONE,
            timestamp: ts,
        });
        candles.push(Candle {
            symbol: symbol_b,
            interval: Interval::OneMinute,
            open: price_b,
            high: price_b + Decimal::ONE,
            low: price_b - Decimal::ONE,
            close: price_b,
            volume: Decimal::ONE,
            timestamp: ts,
        });
    }
    candles
}

fn interval_label(interval: Interval) -> &'static str {
    match interval {
        Interval::OneSecond => "1s",
        Interval::OneMinute => "1m",
        Interval::FiveMinutes => "5m",
        Interval::FifteenMinutes => "15m",
        Interval::OneHour => "1h",
        Interval::FourHours => "4h",
        Interval::OneDay => "1d",
    }
}

fn decimal_to_i128(value: Decimal) -> Result<i128> {
    let mut normalized = value;
    if normalized.scale() > CANONICAL_DECIMAL_SCALE_U32 {
        normalized = normalized.round_dp_with_strategy(
            CANONICAL_DECIMAL_SCALE_U32,
            RoundingStrategy::MidpointNearestEven,
        );
    }
    let scale = normalized.scale();
    if scale > CANONICAL_DECIMAL_SCALE_U32 {
        return Err(anyhow!(
            "value scale {} exceeds canonical precision {CANONICAL_DECIMAL_SCALE_U32}",
            scale
        ));
    }
    let diff = CANONICAL_DECIMAL_SCALE_U32 - scale;
    let factor = 10i128
        .checked_pow(diff)
        .ok_or_else(|| anyhow!("decimal scaling factor overflow"))?;
    normalized
        .mantissa()
        .checked_mul(factor)
        .ok_or_else(|| anyhow!("decimal mantissa overflow"))
}
