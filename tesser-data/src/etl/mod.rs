use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{ArrayRef, Decimal128Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, Utc};
use csv::StringRecord;
use glob::glob;
use parquet::arrow::ArrowWriter;
use rust_decimal::prelude::RoundingStrategy;
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::info;

use crate::schema::{
    canonical_candle_schema, CANONICAL_DECIMAL_PRECISION, CANONICAL_DECIMAL_SCALE,
    CANONICAL_DECIMAL_SCALE_U32,
};

/// Strategy that controls how normalized candles are partitioned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Partitioning {
    Daily,
    Monthly,
}

/// Declarative mapping describing how a raw CSV file should be parsed.
#[derive(Debug, Clone, Deserialize)]
pub struct MappingConfig {
    pub csv: CsvConfig,
    pub fields: FieldMapping,
    #[serde(default = "MappingConfig::default_interval")]
    pub interval: String,
}

impl MappingConfig {
    fn default_interval() -> String {
        "1m".to_string()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CsvConfig {
    #[serde(default)]
    delimiter: Option<String>,
    #[serde(default = "CsvConfig::default_has_header")]
    has_header: bool,
}

impl CsvConfig {
    fn delimiter(&self) -> u8 {
        self.delimiter
            .as_deref()
            .and_then(|value| value.as_bytes().first().copied())
            .unwrap_or(b',')
    }

    fn default_has_header() -> bool {
        true
    }

    fn has_header(&self) -> bool {
        self.has_header
    }
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            delimiter: None,
            has_header: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FieldMapping {
    pub timestamp: TimestampField,
    pub open: ValueField,
    pub high: ValueField,
    pub low: ValueField,
    pub close: ValueField,
    #[serde(default)]
    pub volume: Option<ValueField>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimestampField {
    pub col: usize,
    #[serde(default)]
    pub unit: TimestampUnit,
    #[serde(default)]
    pub format: TimestampFormat,
}

impl TimestampField {
    fn parse(&self, record: &StringRecord) -> Result<i64> {
        let raw = record
            .get(self.col)
            .ok_or_else(|| anyhow!("row missing timestamp column {}", self.col))?
            .trim();
        if raw.is_empty() {
            bail!("timestamp column {} is empty", self.col);
        }
        match self.format {
            TimestampFormat::Unix => self.parse_unix(raw),
            TimestampFormat::Rfc3339 => Self::parse_rfc3339(raw),
        }
    }

    fn parse_unix(&self, raw: &str) -> Result<i64> {
        let value: i64 = raw
            .parse()
            .with_context(|| format!("invalid timestamp '{raw}'"))?;
        let nanos = value
            .checked_mul(self.unit.multiplier())
            .ok_or_else(|| anyhow!("timestamp overflow for value {value}"))?;
        Ok(nanos)
    }

    fn parse_rfc3339(raw: &str) -> Result<i64> {
        let dt = DateTime::parse_from_rfc3339(raw)
            .with_context(|| format!("invalid RFC3339 timestamp '{raw}'"))?
            .with_timezone(&Utc);
        dt.timestamp_nanos_opt()
            .ok_or_else(|| anyhow!("timestamp overflow for value {raw}"))
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum TimestampUnit {
    Seconds,
    #[default]
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl TimestampUnit {
    fn multiplier(self) -> i64 {
        match self {
            TimestampUnit::Seconds => 1_000_000_000,
            TimestampUnit::Milliseconds => 1_000_000,
            TimestampUnit::Microseconds => 1_000,
            TimestampUnit::Nanoseconds => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum TimestampFormat {
    #[default]
    Unix,
    Rfc3339,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ValueField {
    pub col: usize,
}

impl ValueField {
    fn parse_decimal(&self, record: &StringRecord, label: &str) -> Result<Decimal> {
        let raw = record
            .get(self.col)
            .ok_or_else(|| anyhow!("row missing {label} column {}", self.col))?
            .trim();
        if raw.is_empty() {
            bail!("{label} column {} is empty", self.col);
        }
        Decimal::from_str(raw).map_err(|err| anyhow!("invalid {} value '{}': {err}", label, raw))
    }
}

/// ETL pipeline that converts arbitrary CSVs into the canonical Arrow schema.
pub struct Pipeline {
    mapping: MappingConfig,
}

impl Pipeline {
    pub fn new(mapping: MappingConfig) -> Self {
        Self { mapping }
    }

    pub fn run(
        &self,
        pattern: &str,
        output: &Path,
        symbol: &str,
        partitioning: Partitioning,
    ) -> Result<usize> {
        let mut total_rows = 0usize;
        let mut matched = false;
        for entry in glob(pattern).with_context(|| format!("invalid source glob {pattern}"))? {
            let path = entry?;
            matched = true;
            let rows = self.load_rows(&path, symbol)?;
            if rows.is_empty() {
                continue;
            }
            let count = rows.len();
            self.write_partitions(rows, output, partitioning)?;
            total_rows += count;
            info!(path = %path.display(), rows = count, "normalized source file");
        }
        if !matched {
            bail!("no files matched pattern {pattern}");
        }
        Ok(total_rows)
    }

    fn load_rows(&self, path: &Path, symbol: &str) -> Result<Vec<CanonicalCandle>> {
        let interval_label = self.mapping.interval.clone();
        let file = File::open(path)
            .with_context(|| format!("failed to open source file {}", path.display()))?;
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(self.mapping.csv.delimiter())
            .has_headers(self.mapping.csv.has_header())
            .from_reader(BufReader::new(file));
        let mut rows = Vec::new();
        for (idx, record) in reader.records().enumerate() {
            let record = record.with_context(|| format!("failed to read record {}", idx + 1))?;
            let timestamp = self
                .mapping
                .fields
                .timestamp
                .parse(&record)
                .with_context(|| format!("invalid timestamp in {}", path.display()))?;
            let open = self
                .mapping
                .fields
                .open
                .parse_decimal(&record, "open")
                .with_context(|| format!("invalid open price in {}", path.display()))?;
            let high = self
                .mapping
                .fields
                .high
                .parse_decimal(&record, "high")
                .with_context(|| format!("invalid high price in {}", path.display()))?;
            let low = self
                .mapping
                .fields
                .low
                .parse_decimal(&record, "low")
                .with_context(|| format!("invalid low price in {}", path.display()))?;
            let close = self
                .mapping
                .fields
                .close
                .parse_decimal(&record, "close")
                .with_context(|| format!("invalid close price in {}", path.display()))?;
            if high < low {
                bail!(
                    "row {} failed validation: high {} < low {}",
                    idx + 1,
                    high,
                    low
                );
            }
            let volume = if let Some(field) = &self.mapping.fields.volume {
                let parsed = field
                    .parse_decimal(&record, "volume")
                    .with_context(|| format!("invalid volume in {}", path.display()))?;
                if parsed < Decimal::ZERO {
                    bail!(
                        "row {} failed validation: negative volume {}",
                        idx + 1,
                        parsed
                    );
                }
                Some(parsed)
            } else {
                None
            };
            rows.push(CanonicalCandle {
                timestamp,
                symbol: symbol.to_string(),
                interval: interval_label.clone(),
                open,
                high,
                low,
                close,
                volume,
            });
        }
        Ok(rows)
    }

    fn write_partitions(
        &self,
        rows: Vec<CanonicalCandle>,
        output: &Path,
        partitioning: Partitioning,
    ) -> Result<()> {
        let schema = canonical_candle_schema();
        let mut partitions: BTreeMap<String, Vec<CanonicalCandle>> = BTreeMap::new();
        for row in rows {
            let key = partition_path(&row.symbol, &row.interval, row.timestamp, partitioning)?;
            partitions.entry(key).or_default().push(row);
        }
        for (counter, (relative, records)) in partitions.into_iter().enumerate() {
            let dir = output.join(&relative);
            fs::create_dir_all(&dir)
                .with_context(|| format!("failed to create {}", dir.display()))?;
            let file_path = dir.join(format!("part-{counter:05}.parquet"));
            let batch = rows_to_batch(&records, &schema)?;
            let file = File::create(&file_path)
                .with_context(|| format!("failed to create {}", file_path.display()))?;
            let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;
            writer.write(&batch)?;
            writer.close()?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct CanonicalCandle {
    timestamp: i64,
    symbol: String,
    interval: String,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Option<Decimal>,
}

fn rows_to_batch(rows: &[CanonicalCandle], schema: &SchemaRef) -> Result<RecordBatch> {
    let decimal_type = DataType::Decimal128(CANONICAL_DECIMAL_PRECISION, CANONICAL_DECIMAL_SCALE);
    let mut timestamps = Int64Builder::new();
    let mut symbols = StringBuilder::new();
    let mut intervals = StringBuilder::new();
    let mut open_builder = Decimal128Builder::new().with_data_type(decimal_type.clone());
    let mut high_builder = Decimal128Builder::new().with_data_type(decimal_type.clone());
    let mut low_builder = Decimal128Builder::new().with_data_type(decimal_type.clone());
    let mut close_builder = Decimal128Builder::new().with_data_type(decimal_type.clone());
    let mut volume_builder = Decimal128Builder::new().with_data_type(decimal_type.clone());

    for row in rows {
        timestamps.append_value(row.timestamp);
        symbols.append_value(&row.symbol);
        intervals.append_value(&row.interval);
        open_builder.append_value(decimal_to_i128(row.open)?);
        high_builder.append_value(decimal_to_i128(row.high)?);
        low_builder.append_value(decimal_to_i128(row.low)?);
        close_builder.append_value(decimal_to_i128(row.close)?);
        if let Some(volume) = row.volume {
            volume_builder.append_value(decimal_to_i128(volume)?);
        } else {
            volume_builder.append_null();
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(timestamps.finish()),
        Arc::new(symbols.finish()),
        Arc::new(intervals.finish()),
        Arc::new(open_builder.finish()),
        Arc::new(high_builder.finish()),
        Arc::new(low_builder.finish()),
        Arc::new(close_builder.finish()),
        Arc::new(volume_builder.finish()),
    ];

    RecordBatch::try_new(schema.clone(), columns).map_err(Into::into)
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
        bail!(
            "value scale {} exceeds canonical precision {CANONICAL_DECIMAL_SCALE_U32}",
            scale
        );
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

fn partition_path(
    symbol: &str,
    interval: &str,
    timestamp: i64,
    partitioning: Partitioning,
) -> Result<String> {
    let dt = datetime_from_ns(timestamp)?;
    let mut segments = vec![
        format!("symbol={}", sanitize_symbol(symbol)),
        format!("interval={}", sanitize_interval(interval)),
    ];
    segments.push(format!("year={:04}", dt.year()));
    segments.push(format!("month={:02}", dt.month()));
    if matches!(partitioning, Partitioning::Daily) {
        segments.push(format!("day={:02}", dt.day()));
    }
    Ok(segments.join("/"))
}

fn sanitize_symbol(symbol: &str) -> String {
    symbol.replace(':', "_")
}

fn sanitize_interval(interval: &str) -> String {
    interval
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

fn datetime_from_ns(timestamp: i64) -> Result<DateTime<Utc>> {
    let seconds = timestamp / 1_000_000_000;
    let nanos = (timestamp % 1_000_000_000).unsigned_abs() as u32;
    DateTime::from_timestamp(seconds, nanos)
        .ok_or_else(|| anyhow!("failed to convert timestamp {} to datetime", timestamp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn pipeline_normalizes_csv() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("candles.csv");
        fs::write(
            &src,
            "ts,open,high,low,close,vol
1700000000000,100,110,90,105,12
1700000060000,105,115,95,100,15
",
        )
        .unwrap();
        let mapping = MappingConfig {
            csv: CsvConfig::default(),
            fields: FieldMapping {
                timestamp: TimestampField {
                    col: 0,
                    unit: TimestampUnit::Milliseconds,
                    format: TimestampFormat::Unix,
                },
                open: ValueField { col: 1 },
                high: ValueField { col: 2 },
                low: ValueField { col: 3 },
                close: ValueField { col: 4 },
                volume: Some(ValueField { col: 5 }),
            },
            interval: "1m".into(),
        };
        let pipeline = Pipeline::new(mapping);
        let output = dir.path().join("lake");
        let rows = pipeline
            .run(
                src.to_str().unwrap(),
                &output,
                "binance:BTCUSDT",
                Partitioning::Daily,
            )
            .unwrap();
        assert_eq!(rows, 2);
        assert!(count_files(&output) > 0, "no parquet files written");
    }

    #[test]
    fn pipeline_parses_rfc3339_timestamps() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("candles.csv");
        fs::write(
            &src,
            "ts,open,high,low,close,vol\n2024-01-01T00:00:00Z,100,110,90,105,12\n",
        )
        .unwrap();
        let mapping = MappingConfig {
            csv: CsvConfig::default(),
            fields: FieldMapping {
                timestamp: TimestampField {
                    col: 0,
                    unit: TimestampUnit::Milliseconds,
                    format: TimestampFormat::Rfc3339,
                },
                open: ValueField { col: 1 },
                high: ValueField { col: 2 },
                low: ValueField { col: 3 },
                close: ValueField { col: 4 },
                volume: Some(ValueField { col: 5 }),
            },
            interval: "1m".into(),
        };
        let pipeline = Pipeline::new(mapping);
        let output = dir.path().join("lake");
        let rows = pipeline
            .run(
                src.to_str().unwrap(),
                &output,
                "binance:BTCUSDT",
                Partitioning::Daily,
            )
            .unwrap();
        assert_eq!(rows, 1);
        assert!(count_files(&output) > 0);
    }

    fn count_files(root: &Path) -> usize {
        fn visit(dir: &Path, total: &mut usize) {
            if let Ok(entries) = fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        visit(&path, total);
                    } else {
                        *total += 1;
                    }
                }
            }
        }
        let mut total = 0;
        if root.exists() {
            visit(root, &mut total);
        }
        total
    }
}
