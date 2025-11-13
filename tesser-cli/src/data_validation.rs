use std::collections::HashMap;

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use tesser_core::{Candle, Interval, Symbol};

/// Configuration flags used when validating a dataset.
#[derive(Clone, Copy, Debug)]
pub struct ValidationConfig {
    pub price_jump_threshold: f64,
    pub reference_tolerance: f64,
    pub repair_missing: bool,
}

/// Aggregated statistics describing the validation results.
#[derive(Clone, Debug)]
pub struct ValidationSummary {
    pub symbol: Symbol,
    pub interval: Interval,
    pub rows: usize,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub missing_candles: usize,
    pub duplicate_candles: usize,
    pub zero_volume_candles: usize,
    pub price_spike_count: usize,
    pub cross_mismatch_count: usize,
    pub repaired_candles: usize,
}

/// Describes a gap in the time-series.
#[derive(Clone, Debug)]
pub struct GapRecord {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub missing: usize,
}

/// Describes a suspicious price jump.
#[derive(Clone, Debug)]
pub struct SpikeRecord {
    pub timestamp: DateTime<Utc>,
    pub change_fraction: f64,
}

/// Describes a mismatch between the primary data source and a reference feed.
#[derive(Clone, Debug)]
pub struct CrossMismatch {
    pub timestamp: DateTime<Utc>,
    pub primary_close: f64,
    pub reference_close: f64,
    pub delta_fraction: f64,
}

/// Result of a validation run.
#[derive(Clone, Debug)]
pub struct ValidationOutcome {
    pub summary: ValidationSummary,
    pub gaps: Vec<GapRecord>,
    pub price_spikes: Vec<SpikeRecord>,
    pub cross_mismatches: Vec<CrossMismatch>,
    pub repaired: Vec<Candle>,
}

/// Validate a group of candles, optionally repairing missing entries.
pub fn validate_dataset(
    mut candles: Vec<Candle>,
    reference: Option<Vec<Candle>>,
    config: ValidationConfig,
) -> Result<ValidationOutcome> {
    if candles.is_empty() {
        bail!("no candles supplied for validation");
    }
    candles.sort_by_key(|c| c.timestamp);

    let symbol = ensure_single_symbol(&candles)?;
    let interval = candles.first().expect("checked len").interval;
    let interval_duration = interval.as_duration();
    let expected_ms = interval_duration.num_milliseconds().max(1); // guard against zero-duration intervals

    let mut gaps = Vec::new();
    let mut price_spikes = Vec::new();
    let mut cross_mismatches = Vec::new();
    let mut duplicate_candles = 0usize;
    let mut missing_candles = 0usize;
    let mut zero_volume_candles = 0usize;

    let mut prev_close: Option<f64> = None;
    let mut prev_timestamp: Option<DateTime<Utc>> = None;
    for candle in &candles {
        let volume = candle.volume.to_f64().unwrap_or(0.0);
        if volume <= 0.0 {
            zero_volume_candles += 1;
        }
        let close = candle.close.to_f64().unwrap_or(0.0);
        if let (Some(last_close), Some(last_ts)) = (prev_close, prev_timestamp) {
            let delta: Duration = candle.timestamp - last_ts;
            let delta_ms = delta.num_milliseconds();
            if delta_ms < expected_ms {
                duplicate_candles += 1;
            } else if delta_ms > expected_ms {
                let missing = ((delta_ms / expected_ms) as usize).saturating_sub(1);
                if missing > 0 {
                    missing_candles += missing;
                    gaps.push(GapRecord {
                        start: last_ts,
                        end: candle.timestamp,
                        missing,
                    });
                }
            }

            let denom = last_close.abs().max(f64::EPSILON);
            let price_change = (close - last_close).abs() / denom;
            if price_change >= config.price_jump_threshold {
                price_spikes.push(SpikeRecord {
                    timestamp: candle.timestamp,
                    change_fraction: price_change,
                });
            }
        }

        prev_close = Some(close);
        prev_timestamp = Some(candle.timestamp);
    }

    if let Some(reference_data) = reference.as_ref() {
        let mut map = HashMap::with_capacity(reference_data.len());
        for candle in reference_data {
            map.insert(
                candle.timestamp.timestamp_millis(),
                candle.close.to_f64().unwrap_or(0.0),
            );
        }
        for candle in &candles {
            if let Some(reference_close) = map.get(&candle.timestamp.timestamp_millis()) {
                let close = candle.close.to_f64().unwrap_or(0.0);
                let denom = reference_close.abs().max(f64::EPSILON);
                let diff = (close - reference_close).abs() / denom;
                if diff >= config.reference_tolerance {
                    cross_mismatches.push(CrossMismatch {
                        timestamp: candle.timestamp,
                        primary_close: close,
                        reference_close: *reference_close,
                        delta_fraction: diff,
                    });
                }
            }
        }
    }

    let mut repaired = candles.clone();
    let mut repaired_candles = 0usize;
    if config.repair_missing {
        let mut idx = 0usize;
        while idx + 1 < repaired.len() {
            let current_ts = repaired[idx].timestamp;
            let next_ts = repaired[idx + 1].timestamp;
            let delta: Duration = next_ts - current_ts;
            let delta_ms = delta.num_milliseconds();
            if delta_ms > expected_ms {
                let missing = ((delta_ms / expected_ms) as usize).saturating_sub(1);
                if missing > 0 {
                    for step in 1..=missing {
                        let ts = current_ts + Duration::milliseconds(expected_ms * step as i64);
                        let fill_price = repaired[idx].close;
                        let fill = Candle {
                            symbol: repaired[idx].symbol.clone(),
                            interval,
                            open: fill_price,
                            high: fill_price,
                            low: fill_price,
                            close: fill_price,
                            volume: Decimal::ZERO,
                            timestamp: ts,
                        };
                        repaired.insert(idx + step, fill);
                        repaired_candles += 1;
                    }
                    idx += missing;
                }
            }
            idx += 1;
        }
    }

    let summary = ValidationSummary {
        symbol,
        interval,
        rows: candles.len(),
        start: candles.first().expect("len checked").timestamp,
        end: candles.last().expect("len checked").timestamp,
        missing_candles,
        duplicate_candles,
        zero_volume_candles,
        price_spike_count: price_spikes.len(),
        cross_mismatch_count: cross_mismatches.len(),
        repaired_candles,
    };

    Ok(ValidationOutcome {
        summary,
        gaps,
        price_spikes,
        cross_mismatches,
        repaired,
    })
}

fn ensure_single_symbol(candles: &[Candle]) -> Result<Symbol> {
    let mut iter = candles.iter();
    let first = iter
        .next()
        .ok_or_else(|| anyhow!("no candles to inspect for symbol"))?;
    let symbol = first.symbol.clone();
    if iter.any(|c| c.symbol != symbol) {
        bail!("validation currently supports a single symbol per run");
    }
    Ok(symbol)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candle_at(minute: i64, close: f64, volume: f64) -> Candle {
        Candle {
            symbol: "BTCUSDT".to_string(),
            interval: Interval::OneMinute,
            open: close,
            high: close,
            low: close,
            close,
            volume,
            timestamp: Utc::now() + Duration::minutes(minute),
        }
    }

    #[test]
    fn detects_gaps_and_repairs() {
        let candles = vec![candle_at(0, 100.0, 10.0), candle_at(2, 101.0, 11.0)];
        let cfg = ValidationConfig {
            price_jump_threshold: 0.05,
            reference_tolerance: 0.01,
            repair_missing: true,
        };
        let outcome = validate_dataset(candles, None, cfg).expect("ok");
        assert_eq!(outcome.summary.missing_candles, 1);
        assert_eq!(outcome.summary.repaired_candles, 1);
        assert_eq!(outcome.repaired.len(), 3);
    }

    #[test]
    fn detects_zero_volume_and_spikes() {
        let candles = vec![
            candle_at(0, 100.0, 10.0),
            candle_at(1, 150.0, 0.0),
            candle_at(2, 160.0, 12.0),
        ];
        let cfg = ValidationConfig {
            price_jump_threshold: 0.2,
            reference_tolerance: 0.01,
            repair_missing: false,
        };
        let outcome = validate_dataset(candles, None, cfg).expect("ok");
        assert_eq!(outcome.summary.zero_volume_candles, 1);
        assert_eq!(outcome.summary.price_spike_count, 1);
    }

    #[test]
    fn detects_cross_source_mismatches() {
        let primary = vec![candle_at(0, 100.0, 1.0), candle_at(1, 102.0, 1.0)];
        let reference = vec![candle_at(0, 100.0, 1.0), candle_at(1, 100.0, 1.0)];
        let cfg = ValidationConfig {
            price_jump_threshold: 0.2,
            reference_tolerance: 0.01,
            repair_missing: false,
        };
        let outcome = validate_dataset(primary, Some(reference), cfg).expect("ok");
        assert_eq!(outcome.summary.cross_mismatch_count, 1);
    }
}
