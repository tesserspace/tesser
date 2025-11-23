use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, Decimal128Array, Int8Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;
use tesser_core::{Side, Symbol};

/// Request payload for execution analytics.
#[derive(Debug, Clone)]
pub struct ExecutionAnalysisRequest {
    /// Directory that contains `orders/`, `fills/` and `ticks/` sub-directories.
    pub data_dir: PathBuf,
    /// Optional inclusive lower bound for the analyzed window.
    pub start: Option<DateTime<Utc>>,
    /// Optional inclusive upper bound for the analyzed window.
    pub end: Option<DateTime<Utc>>,
}

/// High-level metrics computed for either the entire data set or one algo bucket.
#[derive(Debug, Clone)]
pub struct ExecutionStats {
    pub label: String,
    pub order_count: usize,
    pub fill_count: usize,
    pub orders_with_arrival: usize,
    pub filled_quantity: Decimal,
    pub notional: Decimal,
    pub total_fees: Decimal,
    pub implementation_shortfall: Decimal,
    pub avg_slippage_bps: Option<Decimal>,
}

impl ExecutionStats {
    fn empty(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            order_count: 0,
            fill_count: 0,
            orders_with_arrival: 0,
            filled_quantity: Decimal::ZERO,
            notional: Decimal::ZERO,
            total_fees: Decimal::ZERO,
            implementation_shortfall: Decimal::ZERO,
            avg_slippage_bps: None,
        }
    }
}

/// Execution analysis output, containing totals and the algo breakdown.
#[derive(Debug, Clone)]
pub struct ExecutionReport {
    pub period_start: Option<DateTime<Utc>>,
    pub period_end: Option<DateTime<Utc>>,
    pub totals: ExecutionStats,
    pub per_algo: Vec<ExecutionStats>,
    pub skipped_orders: usize,
}

fn bps_factor() -> Decimal {
    Decimal::new(10_000, 0)
}

/// Compute a [`ExecutionReport`] by scanning flight-recorder parquet files.
pub fn analyze_execution(request: &ExecutionAnalysisRequest) -> Result<ExecutionReport> {
    let range = TimeRange::new(request.start, request.end)?;
    let orders_dir = request.data_dir.join("orders");
    let fills_dir = request.data_dir.join("fills");
    if !orders_dir.exists() {
        bail!(
            "orders directory missing at {}",
            orders_dir.to_string_lossy()
        );
    }
    if !fills_dir.exists() {
        bail!("fills directory missing at {}", fills_dir.to_string_lossy());
    }
    let ticks_dir = request.data_dir.join("ticks");

    let order_paths = collect_parquet_files(&orders_dir)?;
    if order_paths.is_empty() {
        bail!(
            "no parquet files found under {}",
            orders_dir.to_string_lossy()
        );
    }
    let fill_paths = collect_parquet_files(&fills_dir)?;
    if fill_paths.is_empty() {
        bail!(
            "no parquet files found under {}",
            fills_dir.to_string_lossy()
        );
    }
    let tick_paths = collect_parquet_files(&ticks_dir)?;

    let orders = load_orders(&order_paths, &range)?;
    if orders.is_empty() {
        return Ok(ExecutionReport {
            period_start: request.start,
            period_end: request.end,
            totals: ExecutionStats::empty("ALL"),
            per_algo: Vec::new(),
            skipped_orders: 0,
        });
    }
    let fills = load_fills(&fill_paths)?;
    let ticks = load_ticks(&tick_paths)?;

    let mut fills_by_order: HashMap<String, Vec<FillRow>> = HashMap::new();
    for fill in fills {
        fills_by_order
            .entry(fill.order_id.clone())
            .or_default()
            .push(fill);
    }

    let arrival_lookup = ArrivalLookup::new(ticks);
    let mut aggregator = StatsAggregator::new();
    let mut skipped = 0usize;

    for order in orders {
        let Some(fill_rows) = fills_by_order.get(&order.id) else {
            skipped += 1;
            continue;
        };
        if fill_rows.is_empty() {
            skipped += 1;
            continue;
        }
        match summarize_order(&order, fill_rows, &arrival_lookup) {
            Some(summary) => aggregator.record(&order.algo_label, &summary),
            None => skipped += 1,
        }
    }

    let (totals, mut per_algo) = aggregator.finish();
    per_algo.sort_by(|a, b| b.notional.cmp(&a.notional));

    Ok(ExecutionReport {
        period_start: request.start,
        period_end: request.end,
        totals,
        per_algo,
        skipped_orders: skipped,
    })
}

struct StatsAggregator {
    totals: StatsAccumulator,
    groups: HashMap<String, StatsAccumulator>,
}

impl StatsAggregator {
    fn new() -> Self {
        Self {
            totals: StatsAccumulator::new("ALL"),
            groups: HashMap::new(),
        }
    }

    fn record(&mut self, label: &str, summary: &OrderSummary) {
        self.totals.ingest(summary);
        self.groups
            .entry(label.to_string())
            .or_insert_with(|| StatsAccumulator::new(label))
            .ingest(summary);
    }

    fn finish(self) -> (ExecutionStats, Vec<ExecutionStats>) {
        let totals = self.totals.into_stats();
        let groups = self
            .groups
            .into_values()
            .map(|acc| acc.into_stats())
            .collect();
        (totals, groups)
    }
}

struct StatsAccumulator {
    stats: ExecutionStats,
    slippage_weighted_sum: Decimal,
    slippage_weight: Decimal,
}

impl StatsAccumulator {
    fn new(label: &str) -> Self {
        Self {
            stats: ExecutionStats::empty(label),
            slippage_weighted_sum: Decimal::ZERO,
            slippage_weight: Decimal::ZERO,
        }
    }

    fn ingest(&mut self, summary: &OrderSummary) {
        self.stats.order_count += 1;
        self.stats.fill_count += summary.fill_count;
        if summary.has_arrival {
            self.stats.orders_with_arrival += 1;
        }
        self.stats.filled_quantity += summary.filled_quantity;
        self.stats.notional += summary.notional;
        self.stats.total_fees += summary.total_fees;
        self.stats.implementation_shortfall += summary.shortfall_value;
        if let Some(bps) = summary.slippage_bps {
            self.slippage_weighted_sum += bps * summary.filled_quantity;
            self.slippage_weight += summary.filled_quantity;
        }
    }

    fn into_stats(mut self) -> ExecutionStats {
        self.stats.avg_slippage_bps = if self.slippage_weight > Decimal::ZERO {
            Some(self.slippage_weighted_sum / self.slippage_weight)
        } else {
            None
        };
        self.stats
    }
}

#[derive(Clone)]
struct OrderRow {
    id: String,
    symbol: Symbol,
    side: Side,
    created_at: DateTime<Utc>,
    algo_label: String,
}

#[derive(Clone)]
struct FillRow {
    order_id: String,
    price: Decimal,
    quantity: Decimal,
    fee: Decimal,
}

struct OrderSummary {
    fill_count: usize,
    filled_quantity: Decimal,
    notional: Decimal,
    total_fees: Decimal,
    slippage_bps: Option<Decimal>,
    shortfall_value: Decimal,
    has_arrival: bool,
}

struct ArrivalLookup {
    ticks: HashMap<Symbol, Vec<TickPoint>>,
}

impl ArrivalLookup {
    fn new(rows: Vec<TickPoint>) -> Self {
        let mut ticks: HashMap<Symbol, Vec<TickPoint>> = HashMap::new();
        for row in rows {
            ticks.entry(row.symbol.clone()).or_default().push(row);
        }
        for series in ticks.values_mut() {
            series.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        }
        Self { ticks }
    }

    fn price_at(&self, symbol: &Symbol, timestamp: DateTime<Utc>) -> Option<Decimal> {
        let series = self.ticks.get(symbol)?;
        if series.is_empty() {
            return None;
        }
        let idx = series.partition_point(|point| point.timestamp <= timestamp);
        if idx == 0 {
            Some(series[0].price)
        } else {
            Some(series[idx - 1].price)
        }
    }
}

#[derive(Clone)]
struct TickPoint {
    symbol: Symbol,
    price: Decimal,
    timestamp: DateTime<Utc>,
}

#[derive(Clone)]
struct TimeRange {
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
}

impl TimeRange {
    fn new(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> Result<Self> {
        if let (Some(s), Some(e)) = (start, end) {
            if e < s {
                bail!("end time must be after start time");
            }
        }
        Ok(Self { start, end })
    }

    fn contains(&self, ts: DateTime<Utc>) -> bool {
        if let Some(start) = self.start {
            if ts < start {
                return false;
            }
        }
        if let Some(end) = self.end {
            if ts > end {
                return false;
            }
        }
        true
    }
}

fn summarize_order(
    order: &OrderRow,
    fills: &[FillRow],
    arrival_lookup: &ArrivalLookup,
) -> Option<OrderSummary> {
    if fills.is_empty() {
        return None;
    }

    let mut filled_quantity = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    let mut total_fees = Decimal::ZERO;
    for fill in fills {
        filled_quantity += fill.quantity;
        notional += fill.price * fill.quantity;
        total_fees += fill.fee;
    }
    if filled_quantity <= Decimal::ZERO {
        return None;
    }
    let avg_fill_price = notional / filled_quantity;
    let arrival = arrival_lookup.price_at(&order.symbol, order.created_at);
    let mut slippage_bps = None;
    let mut shortfall_value = Decimal::ZERO;
    if let Some(arrival_price) = arrival {
        if arrival_price > Decimal::ZERO {
            let price_delta = (avg_fill_price - arrival_price) * side_sign(order.side);
            shortfall_value = price_delta * filled_quantity;
            let ratio = price_delta / arrival_price;
            slippage_bps = Some(ratio * bps_factor());
        }
    }
    Some(OrderSummary {
        fill_count: fills.len(),
        filled_quantity,
        notional,
        total_fees,
        slippage_bps,
        shortfall_value,
        has_arrival: arrival.is_some(),
    })
}

fn side_sign(side: Side) -> Decimal {
    match side {
        Side::Buy => Decimal::ONE,
        Side::Sell => -Decimal::ONE,
    }
}

pub fn collect_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut stack = vec![dir.to_path_buf()];
    let mut files = Vec::new();
    while let Some(path) = stack.pop() {
        let metadata = fs::metadata(&path)
            .with_context(|| format!("failed to inspect {}", path.to_string_lossy()))?;
        if metadata.is_dir() {
            for entry in fs::read_dir(&path)
                .with_context(|| format!("failed to list {}", path.to_string_lossy()))?
            {
                let entry = entry?;
                stack.push(entry.path());
            }
        } else if path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("parquet"))
            .unwrap_or(false)
        {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn load_orders(paths: &[PathBuf], range: &TimeRange) -> Result<Vec<OrderRow>> {
    let mut rows = Vec::new();
    for path in paths {
        let file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(4096)
            .build()?;
        let mut columns: Option<OrderColumns> = None;
        for batch in reader {
            let batch = batch?;
            if columns.is_none() {
                columns = Some(OrderColumns::from_schema(&batch.schema())?);
            }
            let columns = columns.as_ref().expect("order columns should be set");
            for row in 0..batch.num_rows() {
                let created_at = timestamp_value(&batch, columns.created_at, row)?;
                if !range.contains(created_at) {
                    continue;
                }
                let id = string_value(&batch, columns.id, row)?;
                let symbol = string_value(&batch, columns.symbol, row)?;
                let side = side_value(&batch, columns.side, row)?;
                let client_order_id = string_option(&batch, columns.client_order_id, row)?;
                let algo_label = infer_algo_label(client_order_id.as_deref());
                rows.push(OrderRow {
                    id,
                    symbol: Symbol::from(symbol.as_str()),
                    side,
                    created_at,
                    algo_label,
                });
            }
        }
    }
    Ok(rows)
}

fn load_fills(paths: &[PathBuf]) -> Result<Vec<FillRow>> {
    let mut rows = Vec::new();
    for path in paths {
        let file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(4096)
            .build()?;
        let mut columns: Option<FillColumns> = None;
        for batch in reader {
            let batch = batch?;
            if columns.is_none() {
                columns = Some(FillColumns::from_schema(&batch.schema())?);
            }
            let columns = columns.as_ref().expect("fill columns set");
            for row in 0..batch.num_rows() {
                rows.push(FillRow {
                    order_id: string_value(&batch, columns.order_id, row)?,
                    price: decimal_value(&batch, columns.price, row)?,
                    quantity: decimal_value(&batch, columns.quantity, row)?,
                    fee: decimal_option(&batch, columns.fee, row)?.unwrap_or(Decimal::ZERO),
                });
            }
        }
    }
    Ok(rows)
}

fn load_ticks(paths: &[PathBuf]) -> Result<Vec<TickPoint>> {
    let mut rows = Vec::new();
    for path in paths {
        let file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(4096)
            .build()?;
        let mut columns: Option<TickColumns> = None;
        for batch in reader {
            let batch = batch?;
            if columns.is_none() {
                columns = Some(TickColumns::from_schema(&batch.schema())?);
            }
            let columns = columns.as_ref().expect("tick columns set");
            for row in 0..batch.num_rows() {
                let symbol = string_value(&batch, columns.symbol, row)?;
                rows.push(TickPoint {
                    symbol: Symbol::from(symbol.as_str()),
                    price: decimal_value(&batch, columns.price, row)?,
                    timestamp: timestamp_value(&batch, columns.exchange_ts, row)?,
                });
            }
        }
    }
    Ok(rows)
}

fn infer_algo_label(client_order_id: Option<&str>) -> String {
    let value = client_order_id.unwrap_or("unlabeled");
    let normalized = value.to_ascii_lowercase();
    if normalized.starts_with("twap") {
        "TWAP".to_string()
    } else if normalized.starts_with("vwap") {
        "VWAP".to_string()
    } else if normalized.starts_with("iceberg") {
        "ICEBERG".to_string()
    } else if normalized.starts_with("pegged") {
        "PEGGED".to_string()
    } else if normalized.starts_with("sniper") {
        "SNIPER".to_string()
    } else if normalized.ends_with("-sl") {
        "STOP_LOSS".to_string()
    } else if normalized.ends_with("-tp") {
        "TAKE_PROFIT".to_string()
    } else {
        "SIGNAL".to_string()
    }
}

struct OrderColumns {
    id: usize,
    symbol: usize,
    side: usize,
    client_order_id: usize,
    created_at: usize,
}

impl OrderColumns {
    fn from_schema(schema: &SchemaRef) -> Result<Self> {
        Ok(Self {
            id: column_index(schema, "id")?,
            symbol: column_index(schema, "symbol")?,
            side: column_index(schema, "side")?,
            client_order_id: column_index(schema, "client_order_id")?,
            created_at: column_index(schema, "created_at")?,
        })
    }
}

struct FillColumns {
    order_id: usize,
    price: usize,
    quantity: usize,
    fee: usize,
}

impl FillColumns {
    fn from_schema(schema: &SchemaRef) -> Result<Self> {
        Ok(Self {
            order_id: column_index(schema, "order_id")?,
            price: column_index(schema, "fill_price")?,
            quantity: column_index(schema, "fill_quantity")?,
            fee: column_index(schema, "fee")?,
        })
    }
}

struct TickColumns {
    symbol: usize,
    price: usize,
    exchange_ts: usize,
}

impl TickColumns {
    fn from_schema(schema: &SchemaRef) -> Result<Self> {
        Ok(Self {
            symbol: column_index(schema, "symbol")?,
            price: column_index(schema, "price")?,
            exchange_ts: column_index(schema, "exchange_timestamp")?,
        })
    }
}

fn column_index(schema: &SchemaRef, name: &str) -> Result<usize> {
    schema
        .column_with_name(name)
        .map(|(idx, _)| idx)
        .ok_or_else(|| anyhow!("column '{name}' missing from parquet schema"))
}

fn as_array<T: Array + 'static>(batch: &RecordBatch, column: usize) -> Result<&T> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow!("column {column} type mismatch"))
}

fn string_value(batch: &RecordBatch, column: usize, row: usize) -> Result<String> {
    let array = as_array::<StringArray>(batch, column)?;
    if array.is_null(row) {
        return Err(anyhow!("column {column} contains null string"));
    }
    Ok(array.value(row).to_string())
}

fn string_option(batch: &RecordBatch, column: usize, row: usize) -> Result<Option<String>> {
    let array = as_array::<StringArray>(batch, column)?;
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(array.value(row).to_string()))
}

fn decimal_value(batch: &RecordBatch, column: usize, row: usize) -> Result<Decimal> {
    let array = as_array::<Decimal128Array>(batch, column)?;
    if array.is_null(row) {
        return Err(anyhow!("column {column} contains null decimal"));
    }
    Ok(Decimal::from_i128_with_scale(
        array.value(row),
        array.scale() as u32,
    ))
}

fn decimal_option(batch: &RecordBatch, column: usize, row: usize) -> Result<Option<Decimal>> {
    let array = as_array::<Decimal128Array>(batch, column)?;
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(Decimal::from_i128_with_scale(
        array.value(row),
        array.scale() as u32,
    )))
}

fn timestamp_value(batch: &RecordBatch, column: usize, row: usize) -> Result<DateTime<Utc>> {
    let array = as_array::<TimestampNanosecondArray>(batch, column)?;
    if array.is_null(row) {
        return Err(anyhow!("column {column} contains null timestamp"));
    }
    let nanos = array.value(row);
    let secs = nanos.div_euclid(1_000_000_000);
    let sub = nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, sub)
        .ok_or_else(|| anyhow!("timestamp overflow for value {nanos}"))
}

fn side_value(batch: &RecordBatch, column: usize, row: usize) -> Result<Side> {
    let array = as_array::<Int8Array>(batch, column)?;
    if array.is_null(row) {
        return Err(anyhow!("column {column} contains null side"));
    }
    Ok(if array.value(row) >= 0 {
        Side::Buy
    } else {
        Side::Sell
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::record_batch::RecordBatch;
    use chrono::TimeZone;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use rust_decimal::prelude::FromPrimitive;
    use tempfile::tempdir;
    use tesser_core::{Fill, Order, OrderRequest, OrderStatus, OrderType, Symbol, Tick, TimeInForce};

    use crate::encoding::{fills_to_batch, orders_to_batch, ticks_to_batch};

    #[test]
    fn computes_slippage_from_mock_data() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path();
        let order_id = "order-1".to_string();
        let created_at = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let order = Order {
            id: order_id.clone(),
            request: OrderRequest {
                symbol: Symbol::from("BTCUSDT"),
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: Decimal::from_i64(2).unwrap(),
                price: None,
                trigger_price: None,
                time_in_force: Some(TimeInForce::GoodTilCanceled),
                client_order_id: Some("twap-demo-1".to_string()),
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            },
            status: OrderStatus::Filled,
            filled_quantity: Decimal::from_i64(2).unwrap(),
            avg_fill_price: None,
            created_at,
            updated_at: created_at,
        };
        let orders_batch = orders_to_batch(std::slice::from_ref(&order))?;
        write_partition(root, "orders", created_at, &orders_batch)?;

        let fill_one = Fill {
            order_id: order_id.clone(),
            symbol: order.request.symbol.clone(),
            side: order.request.side,
            fill_price: Decimal::from_f64(101.0).unwrap(),
            fill_quantity: Decimal::ONE,
            fee: Some(Decimal::new(1, 2)),
            timestamp: created_at,
        };
        let fill_two = Fill {
            order_id: order_id.clone(),
            symbol: order.request.symbol.clone(),
            side: order.request.side,
            fill_price: Decimal::from_f64(102.0).unwrap(),
            fill_quantity: Decimal::ONE,
            fee: Some(Decimal::new(1, 2)),
            timestamp: created_at,
        };
        let fills_batch = fills_to_batch(&[fill_one, fill_two])?;
        write_partition(root, "fills", created_at, &fills_batch)?;

        let tick = Tick {
            symbol: order.request.symbol.clone(),
            price: Decimal::from_f64(100.0).unwrap(),
            size: Decimal::ONE,
            side: Side::Buy,
            exchange_timestamp: created_at,
            received_at: created_at,
        };
        let ticks_batch = ticks_to_batch(std::slice::from_ref(&tick))?;
        write_partition(root, "ticks", created_at, &ticks_batch)?;

        let report = analyze_execution(&ExecutionAnalysisRequest {
            data_dir: root.into(),
            start: None,
            end: None,
        })?;

        assert_eq!(report.totals.order_count, 1);
        assert_eq!(report.totals.fill_count, 2);
        assert_eq!(report.totals.filled_quantity, Decimal::from_i64(2).unwrap());
        assert_eq!(report.totals.orders_with_arrival, 1);
        assert_eq!(report.totals.total_fees, Decimal::from_f64(0.02).unwrap());
        assert_eq!(
            report.totals.implementation_shortfall,
            Decimal::from_f64(3.0).unwrap()
        );
        let bps = report.totals.avg_slippage_bps.expect("slippage available");
        assert_eq!(bps, Decimal::from_i64(150).unwrap());
        let algo = report
            .per_algo
            .iter()
            .find(|entry| entry.label == "TWAP")
            .expect("twap bucket exists");
        assert_eq!(algo.order_count, 1);
        Ok(())
    }

    #[test]
    fn handles_missing_orders_in_window() -> Result<()> {
        let dir = tempdir()?;
        let root = dir.path();
        let created_at = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let order = Order {
            id: "order-1".to_string(),
            request: OrderRequest {
                symbol: Symbol::from("BTCUSDT"),
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: Decimal::ONE,
                price: None,
                trigger_price: None,
                time_in_force: Some(TimeInForce::FillOrKill),
                client_order_id: Some("sniper-1".to_string()),
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            },
            status: OrderStatus::Canceled,
            filled_quantity: Decimal::ZERO,
            avg_fill_price: None,
            created_at,
            updated_at: created_at,
        };
        let orders_batch = orders_to_batch(std::slice::from_ref(&order))?;
        write_partition(root, "orders", created_at, &orders_batch)?;

        let fills_batch = fills_to_batch(&[])?;
        write_partition(root, "fills", created_at, &fills_batch)?;

        let ticks_batch = ticks_to_batch(&[])?;
        write_partition(root, "ticks", created_at, &ticks_batch)?;

        let start = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let report = analyze_execution(&ExecutionAnalysisRequest {
            data_dir: root.into(),
            start: Some(start),
            end: None,
        })?;
        assert_eq!(report.totals.order_count, 0);
        assert_eq!(report.skipped_orders, 0);
        Ok(())
    }

    fn write_partition(
        root: &Path,
        kind: &str,
        timestamp: DateTime<Utc>,
        batch: &RecordBatch,
    ) -> Result<()> {
        let day = timestamp.date_naive().to_string();
        let dir = root.join(kind).join(day);
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
        let path = dir.join("part-000.parquet");
        let file =
            File::create(&path).with_context(|| format!("failed to create {}", path.display()))?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close().map(|_| ()).map_err(Into::into)
    }
}
