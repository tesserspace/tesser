use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use arrow::array::{
    Array, Decimal128Array, Int8Array, ListArray, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use rust_decimal::Decimal;
use tokio::fs::File;

use tesser_broker::{BrokerError, BrokerInfo, BrokerResult, MarketStream};
use tesser_core::{
    Candle, DepthUpdate, Interval, LocalOrderBook, OrderBook, OrderBookLevel, Side, Symbol, Tick,
};

const DEFAULT_BATCH_SIZE: usize = 4_096;

/// Market stream backed by on-disk parquet files (flight recorder output).
pub struct ParquetMarketStream {
    info: BrokerInfo,
    ticks: Option<TickCursor>,
    candles: Option<CandleCursor>,
    order_books: Option<OrderBookCursor>,
    depth_updates: Option<DepthCursor>,
    book_state: HashMap<Symbol, LocalOrderBook>,
}

unsafe impl Sync for ParquetMarketStream {}

impl ParquetMarketStream {
    /// Build a stream configured with the provided parquet partitions.
    pub fn new(
        symbols: Vec<Symbol>,
        tick_paths: Vec<PathBuf>,
        candle_paths: Vec<PathBuf>,
        order_book_paths: Vec<PathBuf>,
        depth_paths: Vec<PathBuf>,
    ) -> Self {
        let info = BrokerInfo {
            name: "parquet-replay".into(),
            markets: symbols,
            supports_testnet: true,
        };
        Self {
            info,
            ticks: if tick_paths.is_empty() {
                None
            } else {
                Some(TickCursor::new(tick_paths))
            },
            candles: if candle_paths.is_empty() {
                None
            } else {
                Some(CandleCursor::new(candle_paths))
            },
            order_books: if order_book_paths.is_empty() {
                None
            } else {
                Some(OrderBookCursor::new(order_book_paths))
            },
            depth_updates: if depth_paths.is_empty() {
                None
            } else {
                Some(DepthCursor::new(depth_paths))
            },
            book_state: HashMap::new(),
        }
    }

    /// Convenience helper when only candles are being replayed.
    pub fn with_candles(symbols: Vec<Symbol>, candle_paths: Vec<PathBuf>) -> Self {
        Self::new(symbols, Vec::new(), candle_paths, Vec::new(), Vec::new())
    }

    /// Convenience helper when only order book snapshots are being replayed.
    pub fn with_order_books(symbols: Vec<Symbol>, order_book_paths: Vec<PathBuf>) -> Self {
        Self::new(
            symbols,
            Vec::new(),
            Vec::new(),
            order_book_paths,
            Vec::new(),
        )
    }

    /// Convenience helper when only depth deltas are needed.
    pub fn with_depth_updates(symbols: Vec<Symbol>, depth_paths: Vec<PathBuf>) -> Self {
        Self::new(symbols, Vec::new(), Vec::new(), Vec::new(), depth_paths)
    }
}

#[async_trait]
impl MarketStream for ParquetMarketStream {
    type Subscription = ();

    fn name(&self) -> &str {
        &self.info.name
    }

    fn info(&self) -> Option<&BrokerInfo> {
        Some(&self.info)
    }

    async fn subscribe(&mut self, _subscription: Self::Subscription) -> BrokerResult<()> {
        Ok(())
    }

    async fn next_tick(&mut self) -> BrokerResult<Option<Tick>> {
        match &mut self.ticks {
            Some(cursor) => cursor.next().await.map_err(map_err),
            None => Ok(None),
        }
    }

    async fn next_candle(&mut self) -> BrokerResult<Option<Candle>> {
        match &mut self.candles {
            Some(cursor) => cursor.next().await.map_err(map_err),
            None => Ok(None),
        }
    }

    async fn next_order_book(&mut self) -> BrokerResult<Option<OrderBook>> {
        if let Some(cursor) = self.order_books.as_mut() {
            if let Some(book) = cursor.next().await.map_err(map_err)? {
                return Ok(Some(book));
            }
        }

        if let Some(cursor) = self.depth_updates.as_mut() {
            while let Some(update) = cursor.next().await.map_err(map_err)? {
                let state = self.book_state.entry(update.symbol.clone()).or_default();
                apply_depth_delta(state, &update);
                if let Some(book) = snapshot_from_state(&update.symbol, state, update.timestamp) {
                    return Ok(Some(book));
                }
            }
        }

        Ok(None)
    }
}

fn map_err(err: anyhow::Error) -> BrokerError {
    BrokerError::Other(err.to_string())
}

struct TickCursor {
    loader: BatchLoader,
    columns: Option<TickColumns>,
}

unsafe impl Sync for TickCursor {}

impl TickCursor {
    fn new(paths: Vec<PathBuf>) -> Self {
        Self {
            loader: BatchLoader::new(paths),
            columns: None,
        }
    }

    async fn next(&mut self) -> Result<Option<Tick>> {
        loop {
            if !self.loader.ensure_batch().await? {
                return Ok(None);
            }
            if let Some(schema) = self.loader.take_schema_update() {
                self.columns = Some(TickColumns::from_schema(&schema)?);
            }
            if let Some((batch, row)) = self.loader.next_row() {
                let columns = self
                    .columns
                    .as_ref()
                    .ok_or_else(|| anyhow!("tick schema not initialized"))?;
                return decode_tick(&batch, row, columns).map(Some);
            }
        }
    }
}

struct CandleCursor {
    loader: BatchLoader,
    columns: Option<CandleColumns>,
}

unsafe impl Sync for CandleCursor {}

impl CandleCursor {
    fn new(paths: Vec<PathBuf>) -> Self {
        Self {
            loader: BatchLoader::new(paths),
            columns: None,
        }
    }

    async fn next(&mut self) -> Result<Option<Candle>> {
        loop {
            if !self.loader.ensure_batch().await? {
                return Ok(None);
            }
            if let Some(schema) = self.loader.take_schema_update() {
                self.columns = Some(CandleColumns::from_schema(&schema)?);
            }
            if let Some((batch, row)) = self.loader.next_row() {
                let columns = self
                    .columns
                    .as_ref()
                    .ok_or_else(|| anyhow!("candle schema not initialized"))?;
                return decode_candle(&batch, row, columns).map(Some);
            }
        }
    }
}

struct OrderBookCursor {
    loader: BatchLoader,
    columns: Option<OrderBookColumns>,
}

unsafe impl Sync for OrderBookCursor {}

impl OrderBookCursor {
    fn new(paths: Vec<PathBuf>) -> Self {
        Self {
            loader: BatchLoader::new(paths),
            columns: None,
        }
    }

    async fn next(&mut self) -> Result<Option<OrderBook>> {
        loop {
            if !self.loader.ensure_batch().await? {
                return Ok(None);
            }
            if let Some(schema) = self.loader.take_schema_update() {
                self.columns = Some(OrderBookColumns::from_schema(&schema)?);
            }
            if let Some((batch, row)) = self.loader.next_row() {
                let columns = self
                    .columns
                    .as_ref()
                    .ok_or_else(|| anyhow!("order book schema not initialized"))?;
                return decode_order_book(&batch, row, columns).map(Some);
            }
        }
    }
}

struct DepthCursor {
    loader: BatchLoader,
    columns: Option<DepthColumns>,
}

unsafe impl Sync for DepthCursor {}

impl DepthCursor {
    fn new(paths: Vec<PathBuf>) -> Self {
        Self {
            loader: BatchLoader::new(paths),
            columns: None,
        }
    }

    async fn next(&mut self) -> Result<Option<DepthUpdate>> {
        loop {
            if !self.loader.ensure_batch().await? {
                return Ok(None);
            }
            if let Some(schema) = self.loader.take_schema_update() {
                self.columns = Some(DepthColumns::from_schema(&schema)?);
            }
            if let Some((batch, row)) = self.loader.next_row() {
                let columns = self
                    .columns
                    .as_ref()
                    .ok_or_else(|| anyhow!("depth schema not initialized"))?;
                return decode_depth_update(&batch, row, columns).map(Some);
            }
        }
    }
}

struct BatchLoader {
    files: VecDeque<PathBuf>,
    stream: Option<Pin<Box<ParquetRecordBatchStream<File>>>>,
    batch: Option<RecordBatch>,
    row_index: usize,
    schema_update: Option<SchemaRef>,
    batch_size: usize,
}

unsafe impl Sync for BatchLoader {}

impl BatchLoader {
    fn new(mut paths: Vec<PathBuf>) -> Self {
        paths.sort();
        Self {
            files: paths.into(),
            stream: None,
            batch: None,
            row_index: 0,
            schema_update: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    async fn ensure_batch(&mut self) -> Result<bool> {
        loop {
            if let Some(batch) = &self.batch {
                if self.row_index < batch.num_rows() {
                    return Ok(true);
                }
                self.batch = None;
            }

            if let Some(stream) = self.stream.as_mut() {
                match stream.next().await {
                    Some(Ok(batch)) => {
                        self.row_index = 0;
                        self.batch = Some(batch);
                        continue;
                    }
                    Some(Err(err)) => return Err(err.into()),
                    None => {
                        self.stream = None;
                    }
                }
            }

            if !self.open_next_stream().await? {
                return Ok(false);
            }
        }
    }

    fn next_row(&mut self) -> Option<(RecordBatch, usize)> {
        let batch = self.batch.as_ref()?.clone();
        let row = self.row_index;
        self.row_index += 1;
        Some((batch, row))
    }

    fn take_schema_update(&mut self) -> Option<SchemaRef> {
        self.schema_update.take()
    }

    async fn open_next_stream(&mut self) -> Result<bool> {
        let Some(path) = self.files.pop_front() else {
            return Ok(false);
        };
        let file = File::open(&path)
            .await
            .with_context(|| format!("failed to open {}", path.display()))?;
        let mut builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .with_context(|| format!("failed to read parquet metadata from {}", path.display()))?;
        builder = builder.with_batch_size(self.batch_size);
        let schema = builder.schema().clone();
        let stream = builder
            .build()
            .with_context(|| format!("failed to build parquet stream for {}", path.display()))?;
        self.stream = Some(Box::pin(stream));
        self.schema_update = Some(schema);
        Ok(true)
    }
}

#[derive(Clone, Copy)]
struct TickColumns {
    symbol: usize,
    price: usize,
    size: usize,
    side: usize,
    exchange_ts: usize,
    received_ts: usize,
}

impl TickColumns {
    fn from_schema(schema: &SchemaRef) -> Result<Self> {
        Ok(Self {
            symbol: column_index(schema, "symbol")?,
            price: column_index(schema, "price")?,
            size: column_index(schema, "size")?,
            side: column_index(schema, "side")?,
            exchange_ts: column_index(schema, "exchange_timestamp")?,
            received_ts: column_index(schema, "received_at")?,
        })
    }
}

#[derive(Clone, Copy)]
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
    fn from_schema(schema: &SchemaRef) -> Result<Self> {
        Ok(Self {
            symbol: column_index(schema, "symbol")?,
            interval: column_index(schema, "interval")?,
            open: column_index(schema, "open")?,
            high: column_index(schema, "high")?,
            low: column_index(schema, "low")?,
            close: column_index(schema, "close")?,
            volume: column_index(schema, "volume")?,
            timestamp: column_index(schema, "timestamp")?,
        })
    }
}

struct OrderBookColumns {
    symbol: usize,
    bids_price: usize,
    bids_size: usize,
    asks_price: usize,
    asks_size: usize,
    timestamp: usize,
}

impl OrderBookColumns {
    fn from_schema(schema: &SchemaRef) -> Result<Self> {
        Ok(Self {
            symbol: column_index(schema, "symbol")?,
            bids_price: column_index(schema, "bids_price")?,
            bids_size: column_index(schema, "bids_size")?,
            asks_price: column_index(schema, "asks_price")?,
            asks_size: column_index(schema, "asks_size")?,
            timestamp: column_index(schema, "timestamp")?,
        })
    }
}

struct DepthColumns {
    symbol: usize,
    bids_price: usize,
    bids_size: usize,
    asks_price: usize,
    asks_size: usize,
    timestamp: usize,
}

impl DepthColumns {
    fn from_schema(schema: &SchemaRef) -> Result<Self> {
        Ok(Self {
            symbol: column_index(schema, "symbol")?,
            bids_price: column_index(schema, "bids_price")?,
            bids_size: column_index(schema, "bids_size")?,
            asks_price: column_index(schema, "asks_price")?,
            asks_size: column_index(schema, "asks_size")?,
            timestamp: column_index(schema, "timestamp")?,
        })
    }
}

fn column_index(schema: &SchemaRef, name: &str) -> Result<usize> {
    schema
        .column_with_name(name)
        .map(|(idx, _)| idx)
        .ok_or_else(|| anyhow!("column '{name}' missing from parquet schema"))
}

fn decode_tick(batch: &RecordBatch, row: usize, columns: &TickColumns) -> Result<Tick> {
    let symbol = string_value(batch, columns.symbol, row)?;
    let price = decimal_value(batch, columns.price, row)?;
    let size = decimal_value(batch, columns.size, row)?;
    let side = side_value(batch, columns.side, row)?;
    let exchange_timestamp = timestamp_value(batch, columns.exchange_ts, row)?;
    let received_at = timestamp_value(batch, columns.received_ts, row)?;
    Ok(Tick {
        symbol,
        price,
        size,
        side,
        exchange_timestamp,
        received_at,
    })
}

fn decode_candle(batch: &RecordBatch, row: usize, columns: &CandleColumns) -> Result<Candle> {
    let symbol = string_value(batch, columns.symbol, row)?;
    let interval_raw = string_value(batch, columns.interval, row)?;
    let interval = Interval::from_str(&interval_raw)
        .map_err(|err| anyhow!("invalid interval '{interval_raw}': {err}"))?;
    let open = decimal_value(batch, columns.open, row)?;
    let high = decimal_value(batch, columns.high, row)?;
    let low = decimal_value(batch, columns.low, row)?;
    let close = decimal_value(batch, columns.close, row)?;
    let volume = decimal_value(batch, columns.volume, row)?;
    let timestamp = timestamp_value(batch, columns.timestamp, row)?;
    Ok(Candle {
        symbol,
        interval,
        open,
        high,
        low,
        close,
        volume,
        timestamp,
    })
}

fn decode_order_book(
    batch: &RecordBatch,
    row: usize,
    columns: &OrderBookColumns,
) -> Result<OrderBook> {
    let symbol = string_value(batch, columns.symbol, row)?;
    let bids = level_columns(batch, columns.bids_price, columns.bids_size, row)?;
    let asks = level_columns(batch, columns.asks_price, columns.asks_size, row)?;
    let timestamp = timestamp_value(batch, columns.timestamp, row)?;
    Ok(OrderBook {
        symbol,
        bids,
        asks,
        timestamp,
    })
}

fn decode_depth_update(
    batch: &RecordBatch,
    row: usize,
    columns: &DepthColumns,
) -> Result<DepthUpdate> {
    Ok(DepthUpdate {
        symbol: string_value(batch, columns.symbol, row)?,
        bids: level_columns(batch, columns.bids_price, columns.bids_size, row)?,
        asks: level_columns(batch, columns.asks_price, columns.asks_size, row)?,
        timestamp: timestamp_value(batch, columns.timestamp, row)?,
    })
}

fn string_value(batch: &RecordBatch, column: usize, row: usize) -> Result<Symbol> {
    let array = as_array::<StringArray>(batch, column)?;
    if array.is_null(row) {
        return Err(anyhow!("column {column} contains null string"));
    }
    Ok(array.value(row).to_string())
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

fn level_columns(
    batch: &RecordBatch,
    price_column: usize,
    size_column: usize,
    row: usize,
) -> Result<Vec<OrderBookLevel>> {
    let prices = decimal_list(batch, price_column, row)?;
    let sizes = decimal_list(batch, size_column, row)?;
    if prices.len() != sizes.len() {
        return Err(anyhow!(
            "mismatched price/size list lengths for row {row}: {} vs {}",
            prices.len(),
            sizes.len()
        ));
    }
    Ok(prices
        .into_iter()
        .zip(sizes)
        .map(|(price, size)| OrderBookLevel { price, size })
        .collect())
}

fn decimal_list(batch: &RecordBatch, column: usize, row: usize) -> Result<Vec<Decimal>> {
    let list = as_array::<ListArray>(batch, column)?;
    if list.is_null(row) {
        return Ok(Vec::new());
    }
    let values = list.value(row);
    let decimals = values
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| anyhow!("column {column} is not a decimal list"))?;
    let mut out = Vec::with_capacity(decimals.len());
    for idx in 0..decimals.len() {
        out.push(Decimal::from_i128_with_scale(
            decimals.value(idx),
            decimals.scale() as u32,
        ));
    }
    Ok(out)
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

fn as_array<T: Array + 'static>(batch: &RecordBatch, column: usize) -> Result<&T> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow!("column {column} type mismatch"))
}

fn apply_depth_delta(book: &mut LocalOrderBook, update: &DepthUpdate) {
    for level in &update.bids {
        upsert_level(book, Side::Buy, level);
    }
    for level in &update.asks {
        upsert_level(book, Side::Sell, level);
    }
}

fn upsert_level(book: &mut LocalOrderBook, side: Side, level: &OrderBookLevel) {
    if level.size <= Decimal::ZERO {
        book.clear_level(side, level.price);
    } else {
        book.clear_level(side, level.price);
        book.add_order(side, level.price, level.size);
    }
}

fn snapshot_from_state(
    symbol: &str,
    book: &LocalOrderBook,
    timestamp: DateTime<Utc>,
) -> Option<OrderBook> {
    if book.is_empty() {
        return None;
    }
    let bids = book
        .bids()
        .map(|(price, size)| OrderBookLevel { price, size })
        .collect::<Vec<_>>();
    let asks = book
        .asks()
        .map(|(price, size)| OrderBookLevel { price, size })
        .collect::<Vec<_>>();
    if bids.is_empty() && asks.is_empty() {
        return None;
    }
    Some(OrderBook {
        symbol: symbol.to_string(),
        bids,
        asks,
        timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Builder, ListBuilder, StringBuilder, TimestampNanosecondBuilder};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use rust_decimal::Decimal;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tesser_core::{Interval, Side};

    use crate::encoding::{candles_to_batch, ticks_to_batch};

    const TEST_DECIMAL_PRECISION: u8 = 38;
    const TEST_DECIMAL_SCALE: u32 = 18;

    fn write_parquet_file(path: &PathBuf, batch: &RecordBatch) -> Result<()> {
        let file = std::fs::File::create(path)
            .with_context(|| format!("failed to create {}", path.display()))?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close().map(|_| ()).map_err(Into::into)
    }

    struct BookRow {
        symbol: &'static str,
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
    }

    fn order_book_batch(rows: &[BookRow]) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            list_field("bids_price"),
            list_field("bids_size"),
            list_field("asks_price"),
            list_field("asks_size"),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let mut symbols = StringBuilder::new();
        let mut bids_price = ListBuilder::new(decimal_builder());
        let mut bids_size = ListBuilder::new(decimal_builder());
        let mut asks_price = ListBuilder::new(decimal_builder());
        let mut asks_size = ListBuilder::new(decimal_builder());
        let mut timestamps = TimestampNanosecondBuilder::new();

        for row in rows {
            symbols.append_value(row.symbol);
            append_levels(&mut bids_price, &mut bids_size, &row.bids)?;
            append_levels(&mut asks_price, &mut asks_size, &row.asks)?;
            let ts = Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000);
            timestamps.append_value(ts);
        }

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(symbols.finish()),
                Arc::new(bids_price.finish()),
                Arc::new(bids_size.finish()),
                Arc::new(asks_price.finish()),
                Arc::new(asks_size.finish()),
                Arc::new(timestamps.finish()),
            ],
        )
        .context("failed to build order book batch")
    }

    fn list_field(name: &str) -> Field {
        Field::new(
            name,
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Decimal128(TEST_DECIMAL_PRECISION, TEST_DECIMAL_SCALE as i8),
                true,
            ))),
            true,
        )
    }

    fn append_levels(
        prices: &mut ListBuilder<Decimal128Builder>,
        sizes: &mut ListBuilder<Decimal128Builder>,
        levels: &[(Decimal, Decimal)],
    ) -> Result<()> {
        for &(price, size) in levels {
            prices.values().append_value(rescale_decimal(price));
            sizes.values().append_value(rescale_decimal(size));
        }
        prices.append(true);
        sizes.append(true);
        Ok(())
    }

    fn rescale_decimal(mut value: Decimal) -> i128 {
        value.rescale(TEST_DECIMAL_SCALE);
        value.mantissa()
    }

    fn decimal_builder() -> Decimal128Builder {
        Decimal128Builder::new().with_data_type(DataType::Decimal128(
            TEST_DECIMAL_PRECISION,
            TEST_DECIMAL_SCALE as i8,
        ))
    }

    fn sample_candles() -> Vec<Candle> {
        vec![Candle {
            symbol: "BTCUSDT".into(),
            interval: Interval::OneMinute,
            open: Decimal::ONE,
            high: Decimal::new(2, 0),
            low: Decimal::ZERO,
            close: Decimal::new(15, 1),
            volume: Decimal::new(5, 0),
            timestamp: Utc::now(),
        }]
    }

    fn sample_ticks() -> Vec<Tick> {
        vec![Tick {
            symbol: "BTCUSDT".into(),
            price: Decimal::new(20_000, 0),
            size: Decimal::new(1, 0),
            side: Side::Buy,
            exchange_timestamp: Utc::now(),
            received_at: Utc::now(),
        }]
    }

    #[tokio::test]
    async fn replays_candles_from_parquet() -> Result<()> {
        let tmp = tempdir()?;
        let path = tmp.path().join("candles.parquet");
        let candles = sample_candles();
        let batch = candles_to_batch(&candles)?;
        write_parquet_file(&path, &batch)?;

        let mut stream = ParquetMarketStream::with_candles(vec!["BTCUSDT".into()], vec![path]);
        let first = stream
            .next_candle()
            .await
            .context("expected candle")?
            .expect("candle available");
        assert_eq!(first.symbol, candles[0].symbol);
        Ok(())
    }

    #[tokio::test]
    async fn replays_ticks_from_parquet() -> Result<()> {
        let tmp = tempdir()?;
        let path = tmp.path().join("ticks.parquet");
        let ticks = sample_ticks();
        let batch = ticks_to_batch(&ticks)?;
        write_parquet_file(&path, &batch)?;

        let mut stream = ParquetMarketStream::new(
            vec!["BTCUSDT".into()],
            vec![path],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        let first = stream
            .next_tick()
            .await
            .context("expected tick")?
            .expect("tick available");
        assert_eq!(first.price, ticks[0].price);
        Ok(())
    }

    #[tokio::test]
    async fn replays_order_books_from_parquet() -> Result<()> {
        let tmp = tempdir()?;
        let path = tmp.path().join("books.parquet");
        let rows = vec![BookRow {
            symbol: "BTCUSDT",
            bids: vec![(Decimal::new(20_000, 0), Decimal::new(2, 0))],
            asks: vec![(Decimal::new(20_010, 0), Decimal::new(3, 0))],
        }];
        let batch = order_book_batch(&rows)?;
        write_parquet_file(&path, &batch)?;

        let mut stream = ParquetMarketStream::with_order_books(vec!["BTCUSDT".into()], vec![path]);
        let book = stream
            .next_order_book()
            .await?
            .expect("order book snapshot");
        assert_eq!(book.bids[0].price, Decimal::new(20_000, 0));
        assert_eq!(book.asks[0].price, Decimal::new(20_010, 0));
        Ok(())
    }

    #[tokio::test]
    async fn replays_depth_updates_as_snapshots() -> Result<()> {
        let tmp = tempdir()?;
        let path = tmp.path().join("depth.parquet");
        let rows = vec![BookRow {
            symbol: "BTCUSDT",
            bids: vec![(Decimal::new(19_900, 0), Decimal::new(1, 0))],
            asks: vec![(Decimal::new(19_910, 0), Decimal::new(1, 0))],
        }];
        let batch = order_book_batch(&rows)?;
        write_parquet_file(&path, &batch)?;

        let mut stream =
            ParquetMarketStream::with_depth_updates(vec!["BTCUSDT".into()], vec![path]);
        let book = stream
            .next_order_book()
            .await?
            .expect("depth-derived snapshot");
        assert_eq!(book.bids[0].price, Decimal::new(19_900, 0));
        assert_eq!(book.asks[0].price, Decimal::new(19_910, 0));
        Ok(())
    }
}
