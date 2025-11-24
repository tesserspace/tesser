use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::mem;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use rust_decimal::Decimal;

use tesser_core::{Candle, Interval, Symbol};

/// Resamples a chronologically ordered candle stream into coarser intervals.
///
/// Typical usage collects candles from either an in-memory vector or an async stream:
/// ```
/// # use tesser_core::{Candle, Interval};
/// # use tesser_data::transform::Resampler;
/// # fn demo(candles: Vec<Candle>) {
/// let resampled = Resampler::resample(candles, Interval::OneHour);
/// # let _ = resampled;
/// # }
/// ```
pub struct Resampler {
    interval: Interval,
    interval_nanos: i64,
    active: HashMap<Symbol, Bucket>,
    output: Vec<Candle>,
}

impl Resampler {
    /// Create a new resampler targeting the provided [`Interval`].
    pub fn new(interval: Interval) -> Self {
        let interval_nanos = interval
            .as_duration()
            .num_nanoseconds()
            .expect("interval nanoseconds fit into i64");
        Self {
            interval,
            interval_nanos,
            active: HashMap::new(),
            output: Vec::new(),
        }
    }

    /// Convenience helper that processes the entire slice and returns the resampled candles.
    pub fn resample(candles: Vec<Candle>, interval: Interval) -> Vec<Candle> {
        let mut sorted = candles;
        sorted.sort_by_key(|c| c.timestamp);
        let mut resampler = Self::new(interval);
        for candle in sorted {
            resampler.push(candle);
        }
        resampler.finish()
    }

    /// Resample candles coming from an async stream.
    pub async fn resample_stream<S>(interval: Interval, stream: &mut S) -> Vec<Candle>
    where
        S: futures::Stream<Item = Candle> + Unpin,
    {
        let mut resampler = Self::new(interval);
        while let Some(candle) = stream.next().await {
            resampler.push(candle);
        }
        resampler.finish()
    }

    /// Ingest one candle into the resampler. Completed buckets are pushed into the output buffer.
    pub fn push(&mut self, candle: Candle) {
        let bucket_start = align_timestamp(candle.timestamp, self.interval_nanos);
        let symbol = candle.symbol;
        match self.active.entry(symbol) {
            Entry::Vacant(slot) => {
                slot.insert(Bucket::from_candle(
                    symbol,
                    bucket_start,
                    self.interval,
                    &candle,
                ));
            }
            Entry::Occupied(mut slot) => {
                let entry = slot.get_mut();
                match bucket_start.cmp(&entry.start) {
                    Ordering::Less => {
                        // Out-of-order candle; flush the existing bucket before rewinding.
                        let finished = mem::replace(
                            entry,
                            Bucket::from_candle(symbol, bucket_start, self.interval, &candle),
                        );
                        self.output.push(finished.into_candle());
                    }
                    Ordering::Equal => {
                        entry.update(&candle);
                    }
                    Ordering::Greater => {
                        // Finalize the previous bucket and start a new one.
                        let finished = mem::replace(
                            entry,
                            Bucket::from_candle(symbol, bucket_start, self.interval, &candle),
                        );
                        self.output.push(finished.into_candle());
                    }
                }
            }
        }
    }

    /// Finalize the current buckets and return all resampled candles sorted by timestamp.
    pub fn finish(mut self) -> Vec<Candle> {
        for bucket in self.active.into_values() {
            self.output.push(bucket.into_candle());
        }
        self.output.sort_by(|a, b| {
            let ts = a.timestamp.cmp(&b.timestamp);
            if ts == Ordering::Equal {
                (a.symbol.exchange.as_raw(), a.symbol.market_id)
                    .cmp(&(b.symbol.exchange.as_raw(), b.symbol.market_id))
            } else {
                ts
            }
        });
        self.output
    }
}

struct Bucket {
    symbol: Symbol,
    interval: Interval,
    start: DateTime<Utc>,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
}

impl Bucket {
    fn from_candle(
        symbol: Symbol,
        start: DateTime<Utc>,
        interval: Interval,
        candle: &Candle,
    ) -> Self {
        Self {
            symbol,
            interval,
            start,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume,
        }
    }

    fn update(&mut self, candle: &Candle) {
        if candle.high > self.high {
            self.high = candle.high;
        }
        if candle.low < self.low {
            self.low = candle.low;
        }
        self.close = candle.close;
        self.volume += candle.volume;
    }

    fn into_candle(self) -> Candle {
        Candle {
            symbol: self.symbol,
            interval: self.interval,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            timestamp: self.start,
        }
    }
}

fn align_timestamp(ts: DateTime<Utc>, step_nanos: i64) -> DateTime<Utc> {
    let timestamp = ts
        .timestamp_nanos_opt()
        .expect("timestamp fits into i64 nanoseconds");
    let aligned = timestamp - timestamp.rem_euclid(step_nanos);
    let secs = aligned.div_euclid(1_000_000_000);
    let nanos = aligned.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos)
        .expect("aligned timestamp within chrono supported range")
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Timelike, Utc};
    use rust_decimal::Decimal;
    use tesser_core::Interval;

    use super::*;

    fn candle_at(minute: i64, close: i64) -> Candle {
        Candle {
            symbol: "BTCUSDT".into(),
            interval: Interval::OneMinute,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::new(close, 0),
            volume: Decimal::new(10, 0),
            timestamp: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()
                + Duration::minutes(minute),
        }
    }

    #[tokio::test]
    async fn resamples_stream() {
        let candles: Vec<_> = (0..10).map(|idx| candle_at(idx, idx)).collect();
        let mut stream = futures::stream::iter(candles.clone());
        let resampled = Resampler::resample_stream(Interval::FiveMinutes, &mut stream).await;
        assert_eq!(resampled.len(), 2);
        assert_eq!(resampled[0].close, candles[4].close);
        assert_eq!(resampled[1].close, candles[9].close);
    }

    #[test]
    fn resamples_vec() {
        let candles: Vec<_> = (0..10).map(|idx| candle_at(idx, idx)).collect();
        let resampled = Resampler::resample(candles, Interval::FiveMinutes);
        assert_eq!(resampled.len(), 2);
        assert_eq!(resampled[0].timestamp.minute(), 0);
        assert_eq!(resampled[1].timestamp.minute(), 5);
    }
}
