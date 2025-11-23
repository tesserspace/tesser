use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, Decimal128Array, StringArray, TimestampNanosecondArray};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use csv::{ReaderBuilder, WriterBuilder};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rust_decimal::Decimal;

use tesser_core::{Candle, Interval, Symbol, Tick};

use crate::encoding::{candles_to_batch, ticks_to_batch};

/// Canonical formats supported by `read_dataset`/`write_dataset`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatasetFormat {
    Csv,
    Parquet,
}

impl DatasetFormat {
    /// Attempt to infer the format based on the file extension.
    #[must_use]
    pub fn from_path(path: &Path) -> Self {
        match path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase())
            .as_deref()
        {
            Some("parquet") => Self::Parquet,
            _ => Self::Csv,
        }
    }
}

/// Fully materialized dataset containing normalized candles.
pub struct CandleDataset {
    pub format: DatasetFormat,
    pub candles: Vec<Candle>,
}

/// Load a dataset from disk using either CSV or Parquet encodings.
pub fn read_dataset(path: &Path) -> Result<CandleDataset> {
    let format = DatasetFormat::from_path(path);
    let candles = match format {
        DatasetFormat::Csv => read_csv(path)
            .with_context(|| format!("failed to load CSV dataset {}", path.display()))?,
        DatasetFormat::Parquet => read_parquet(path)
            .with_context(|| format!("failed to load parquet dataset {}", path.display()))?,
    };
    Ok(CandleDataset { format, candles })
}

/// Persist the provided candles to disk using the requested format.
pub fn write_dataset(path: &Path, format: DatasetFormat, candles: &[Candle]) -> Result<()> {
    match format {
        DatasetFormat::Csv => write_csv(path, candles),
        DatasetFormat::Parquet => write_parquet(path, candles),
    }
}

/// Helper for normalizing and persisting tick data to parquet files.
pub struct TicksWriter {
    path: PathBuf,
    rows: Vec<Tick>,
    seen_ids: HashSet<String>,
}

impl TicksWriter {
    /// Build a writer bound to the provided destination path.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            rows: Vec::new(),
            seen_ids: HashSet::new(),
        }
    }

    /// Current number of buffered ticks (before final dedupe).
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns true when no ticks have been appended.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Append a single tick, skipping duplicates that reuse the same trade identifier.
    pub fn push(&mut self, trade_id: Option<String>, tick: Tick) {
        if let Some(id) = trade_id.filter(|value| !value.is_empty()) {
            if !self.seen_ids.insert(id) {
                return;
            }
        }
        self.rows.push(tick);
    }

    /// Extend the writer with one or more `(trade_id, tick)` tuples.
    pub fn extend<I>(&mut self, trades: I)
    where
        I: IntoIterator<Item = (Option<String>, Tick)>,
    {
        for (trade_id, tick) in trades {
            self.push(trade_id, tick);
        }
    }

    /// Finalize the parquet file once all ticks have been queued.
    pub fn finish(mut self) -> Result<()> {
        self.rows
            .sort_by_key(|tick| tick.exchange_timestamp.timestamp_millis());
        self.rows.dedup_by(|a, b| {
            a.exchange_timestamp == b.exchange_timestamp
                && a.price == b.price
                && a.size == b.size
                && a.side == b.side
        });
        write_ticks_parquet(&self.path, &self.rows)
    }
}

fn read_csv(path: &Path) -> Result<Vec<Candle>> {
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    let mut candles = Vec::new();
    for row in reader.records() {
        let record = row.with_context(|| format!("invalid row in {}", path.display()))?;
        let timestamp = parse_timestamp(
            record
                .get(1)
                .ok_or_else(|| anyhow!("missing timestamp column in {}", path.display()))?,
        )?;
        let symbol = match record.get(0) {
            Some(value) if !value.trim().is_empty() => value.to_string(),
            _ => infer_symbol(path).ok_or_else(|| {
                anyhow!(
                    "missing symbol column and unable to infer from {}",
                    path.display()
                )
            })?,
        };
        let candle = Candle {
            symbol: Symbol::from(symbol.as_str()),
            interval: infer_interval(path).unwrap_or(Interval::OneMinute),
            open: parse_decimal(record.get(2), "open", path)?,
            high: parse_decimal(record.get(3), "high", path)?,
            low: parse_decimal(record.get(4), "low", path)?,
            close: parse_decimal(record.get(5), "close", path)?,
            volume: parse_decimal(record.get(6), "volume", path)?,
            timestamp,
        };
        candles.push(candle);
    }
    candles.sort_by_key(|c| c.timestamp);
    Ok(candles)
}

fn read_parquet(path: &Path) -> Result<Vec<Candle>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open parquet file {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(1024)
        .build()?;
    let mut columns: Option<CandleColumns> = None;
    let mut candles = Vec::new();
    for batch in reader {
        let batch = batch?;
        if columns.is_none() {
            columns = Some(CandleColumns::from_batch(&batch)?);
        }
        let column_mapping = columns.as_ref().unwrap();
        for row in 0..batch.num_rows() {
            candles.push(column_mapping.decode(&batch, row)?);
        }
    }
    candles.sort_by_key(|c| c.timestamp);
    Ok(candles)
}

fn write_csv(path: &Path, candles: &[Candle]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let mut writer = WriterBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    writer.write_record([
        "symbol",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ])?;
    for candle in candles {
        writer.write_record([
            &candle.symbol.to_string(),
            &candle.timestamp.to_rfc3339(),
            &candle.open.to_string(),
            &candle.high.to_string(),
            &candle.low.to_string(),
            &candle.close.to_string(),
            &candle.volume.to_string(),
        ])?;
    }
    writer.flush()?;
    Ok(())
}

fn write_parquet(path: &Path, candles: &[Candle]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let batch = candles_to_batch(candles)?;
    let file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn write_ticks_parquet(path: &Path, ticks: &[Tick]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let batch = ticks_to_batch(ticks)?;
    let file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn parse_decimal(value: Option<&str>, column: &str, path: &Path) -> Result<Decimal> {
    let text = value.ok_or_else(|| anyhow!("missing {column} column in {}", path.display()))?;
    Decimal::from_str(text)
        .with_context(|| format!("invalid {column} value '{text}' in {}", path.display()))
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>> {
    if let Ok(ts) = DateTime::parse_from_rfc3339(value) {
        return Ok(ts.with_timezone(&Utc));
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow!("invalid date '{value}'"))?;
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
    }
    Err(anyhow!("unable to parse timestamp '{value}'"))
}

fn infer_symbol(path: &Path) -> Option<String> {
    path.parent()
        .and_then(|parent| parent.file_name())
        .map(|os| os.to_string_lossy().to_string())
}

fn infer_interval(path: &Path) -> Option<Interval> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.split('_').next())
        .and_then(|token| Interval::from_str(token).ok())
}

struct CandleColumns {
    symbol: usize,
    interval: usize,
    open: usize,
    high: usize,
    low: usize,
    close: usize,
    volume: usize,
    timestamp: usize,
}

impl CandleColumns {
    fn from_batch(batch: &RecordBatch) -> Result<Self> {
        Ok(Self {
            symbol: column_index(batch, "symbol")?,
            interval: column_index(batch, "interval")?,
            open: column_index(batch, "open")?,
            high: column_index(batch, "high")?,
            low: column_index(batch, "low")?,
            close: column_index(batch, "close")?,
            volume: column_index(batch, "volume")?,
            timestamp: column_index(batch, "timestamp")?,
        })
    }

    fn decode(&self, batch: &RecordBatch, row: usize) -> Result<Candle> {
        let symbol = string_value(batch, self.symbol, row)?;
        let interval_raw = string_value(batch, self.interval, row)?;
        let interval =
            Interval::from_str(&interval_raw).map_err(|err| anyhow!("{interval_raw}: {err}"))?;
        Ok(Candle {
            symbol: Symbol::from(symbol.as_str()),
            interval,
            open: decimal_value(batch, self.open, row)?,
            high: decimal_value(batch, self.high, row)?,
            low: decimal_value(batch, self.low, row)?,
            close: decimal_value(batch, self.close, row)?,
            volume: decimal_value(batch, self.volume, row)?,
            timestamp: timestamp_value(batch, self.timestamp, row)?,
        })
    }
}

fn column_index(batch: &RecordBatch, name: &str) -> Result<usize> {
    batch
        .schema()
        .column_with_name(name)
        .map(|(idx, _)| idx)
        .ok_or_else(|| anyhow!("column '{name}' missing from schema"))
}

fn string_value(batch: &RecordBatch, column: usize, row: usize) -> Result<String> {
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("column {column} is not Utf8"))?;
    if array.is_null(row) {
        return Err(anyhow!("column {column} contains null string"));
    }
    Ok(array.value(row).to_string())
}

fn decimal_value(batch: &RecordBatch, column: usize, row: usize) -> Result<Decimal> {
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| anyhow!("column {column} is not decimal"))?;
    if array.is_null(row) {
        return Err(anyhow!("column {column} contains null decimal"));
    }
    Ok(Decimal::from_i128_with_scale(
        array.value(row),
        array.scale() as u32,
    ))
}

fn timestamp_value(batch: &RecordBatch, column: usize, row: usize) -> Result<DateTime<Utc>> {
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| anyhow!("column {column} is not timestamp"))?;
    if array.is_null(row) {
        return Err(anyhow!("column {column} contains null timestamp"));
    }
    let nanos = array.value(row);
    let secs = nanos.div_euclid(1_000_000_000);
    let sub = nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, sub)
        .ok_or_else(|| anyhow!("timestamp overflow for value {nanos}"))
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal::{prelude::FromPrimitive, Decimal};
    use tempfile::tempdir;
    use tesser_core::{Side, Symbol};

    use super::*;

    fn sample_candles() -> Vec<Candle> {
        let base = Utc::now() - Duration::minutes(10);
        (0..4)
            .map(|idx| Candle {
                symbol: Symbol::from("BTCUSDT"),
                interval: Interval::OneMinute,
                open: Decimal::new(10 + idx as i64, 0),
                high: Decimal::new(11 + idx as i64, 0),
                low: Decimal::new(9 + idx as i64, 0),
                close: Decimal::new(10 + idx as i64, 0),
                volume: Decimal::new(1, 0),
                timestamp: base + Duration::minutes(idx as i64),
            })
            .collect()
    }

    #[test]
    fn round_trip_csv() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("1m_BTCUSDT.csv");
        let candles = sample_candles();
        write_dataset(&path, DatasetFormat::Csv, &candles)?;
        let dataset = read_dataset(&path)?;
        assert_eq!(dataset.candles.len(), candles.len());
        Ok(())
    }

    #[test]
    fn round_trip_parquet() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("1m_BTCUSDT.parquet");
        let candles = sample_candles();
        write_dataset(&path, DatasetFormat::Parquet, &candles)?;
        let dataset = read_dataset(&path)?;
        assert_eq!(dataset.candles.len(), candles.len());
        Ok(())
    }

    #[test]
    fn ticks_writer_dedupes_trade_ids_and_payloads() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("ticks.parquet");
        let mut writer = TicksWriter::new(&path);
        writer.push(
            Some("trade-1".to_string()),
            sample_tick(1_000, 100.0, 1.0, Side::Buy),
        );
        // Duplicate ID should be skipped.
        writer.push(
            Some("trade-1".to_string()),
            sample_tick(1_000, 100.0, 1.0, Side::Buy),
        );
        // Same payload without ID should dedupe during finish.
        writer.extend([
            (None, sample_tick(2_000, 101.0, 2.0, Side::Sell)),
            (None, sample_tick(2_000, 101.0, 2.0, Side::Sell)),
        ]);
        writer.finish()?;

        let file = File::open(&path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(8)
            .build()?;
        let mut rows = 0;
        for batch in reader {
            rows += batch?.num_rows();
        }
        assert_eq!(rows, 2);
        Ok(())
    }

    fn sample_tick(ts_ms: i64, price: f64, size: f64, side: Side) -> Tick {
        let price = Decimal::from_f64(price).expect("valid price");
        let size = Decimal::from_f64(size).expect("valid size");
        let timestamp = Utc
            .timestamp_millis_opt(ts_ms)
            .single()
            .expect("valid timestamp");
        Tick {
            symbol: Symbol::from("BTCUSDT"),
            price,
            size,
            side,
            exchange_timestamp: timestamp,
            received_at: timestamp,
        }
    }
}
