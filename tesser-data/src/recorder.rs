use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDate, Utc};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tokio::fs::{self, File};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::task::JoinHandle;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, warn};

use tesser_core::{Candle, Fill, Order, Signal, Tick};

use crate::encoding::{
    candle_schema, candles_to_batch, fill_schema, fills_to_batch, order_schema, orders_to_batch,
    signal_schema, signals_to_batch, tick_schema, ticks_to_batch,
};

/// Configuration used when spawning a [`ParquetRecorder`].
#[derive(Clone, Debug)]
pub struct RecorderConfig {
    /// Directory where all flight recorder data will be stored.
    pub root: PathBuf,
    /// Maximum number of rows buffered in-memory before forcing a flush.
    pub max_buffered_rows: usize,
    /// How often to flush buffers when traffic is low.
    pub flush_interval: Duration,
    /// Maximum number of rows per parquet file before starting a new file.
    pub max_rows_per_file: usize,
    /// Capacity of the asynchronous channel accepting recorder events.
    pub channel_capacity: usize,
}

impl Default for RecorderConfig {
    fn default() -> Self {
        Self {
            root: PathBuf::from("data/flight_recorder"),
            max_buffered_rows: 1024,
            flush_interval: Duration::from_secs(5),
            max_rows_per_file: 250_000,
            channel_capacity: 4096,
        }
    }
}

/// Background component responsible for writing parquet batches to disk.
pub struct ParquetRecorder {
    handle: RecorderHandle,
    task: Option<JoinHandle<Result<()>>>,
}

impl ParquetRecorder {
    /// Starts a new recorder task with the provided configuration.
    pub async fn spawn(config: RecorderConfig) -> Result<Self> {
        fs::create_dir_all(&config.root)
            .await
            .with_context(|| format!("failed to create {}", config.root.display()))?;

        let (tx, rx) = mpsc::channel(config.channel_capacity.max(1));
        let worker = FlightRecorderWorker::new(config, rx).await?;
        let task = tokio::spawn(async move { worker.run().await });
        Ok(Self {
            handle: RecorderHandle { sender: tx },
            task: Some(task),
        })
    }

    /// Returns a handle that can be cloned and used across tasks to enqueue records.
    pub fn handle(&self) -> RecorderHandle {
        self.handle.clone()
    }

    /// Signals the recorder to stop and waits for buffers to flush.
    pub async fn shutdown(mut self) -> Result<()> {
        drop(self.handle);
        if let Some(task) = self.task.take() {
            match task.await {
                Ok(result) => result?,
                Err(err) => {
                    return Err(anyhow!("flight recorder task aborted: {err}"));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct RecorderHandle {
    sender: mpsc::Sender<RecorderMessage>,
}

impl RecorderHandle {
    /// Enqueues a tick event for recording.
    pub fn record_tick(&self, tick: Tick) {
        self.enqueue(RecorderMessage::Tick(tick), "tick", &TICK_SATURATION);
    }

    /// Enqueues a candle event for recording.
    pub fn record_candle(&self, candle: Candle) {
        self.enqueue(
            RecorderMessage::Candle(candle),
            "candle",
            &CANDLE_SATURATION,
        );
    }

    /// Enqueues a fill event for recording.
    pub fn record_fill(&self, fill: Fill) {
        self.enqueue(RecorderMessage::Fill(fill), "fill", &FILL_SATURATION);
    }

    /// Enqueues an order update for recording.
    pub fn record_order(&self, order: Order) {
        self.enqueue(RecorderMessage::Order(order), "order", &ORDER_SATURATION);
    }

    /// Enqueues a signal for recording.
    pub fn record_signal(&self, signal: Signal) {
        self.enqueue(
            RecorderMessage::Signal(signal),
            "signal",
            &SIGNAL_SATURATION,
        );
    }

    fn enqueue(&self, message: RecorderMessage, label: &'static str, flag: &'static AtomicBool) {
        match self.sender.try_send(message) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                if !flag.swap(true, Ordering::Relaxed) {
                    warn!("flight recorder channel saturated; dropping {label} events");
                }
            }
            Err(TrySendError::Closed(_)) => {
                warn!("flight recorder channel closed; ignoring {label} event");
            }
        }
    }
}

enum RecorderMessage {
    Tick(Tick),
    Candle(Candle),
    Fill(Fill),
    Order(Order),
    Signal(Signal),
}

static TICK_SATURATION: AtomicBool = AtomicBool::new(false);
static CANDLE_SATURATION: AtomicBool = AtomicBool::new(false);
static FILL_SATURATION: AtomicBool = AtomicBool::new(false);
static ORDER_SATURATION: AtomicBool = AtomicBool::new(false);
static SIGNAL_SATURATION: AtomicBool = AtomicBool::new(false);

struct FlightRecorderWorker {
    rx: mpsc::Receiver<RecorderMessage>,
    tick_sink: DataSink<TickEncoder>,
    candle_sink: DataSink<CandleEncoder>,
    fill_sink: DataSink<FillEncoder>,
    order_sink: DataSink<OrderEncoder>,
    signal_sink: DataSink<SignalEncoder>,
    flush_interval: Duration,
}

impl FlightRecorderWorker {
    async fn new(config: RecorderConfig, rx: mpsc::Receiver<RecorderMessage>) -> Result<Self> {
        let writer_props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .build(),
        );

        let tick_dir = ensure_subdir(&config.root, TickEncoder::KIND).await?;
        let candle_dir = ensure_subdir(&config.root, CandleEncoder::KIND).await?;
        let fill_dir = ensure_subdir(&config.root, FillEncoder::KIND).await?;
        let order_dir = ensure_subdir(&config.root, OrderEncoder::KIND).await?;
        let signal_dir = ensure_subdir(&config.root, SignalEncoder::KIND).await?;

        Ok(Self {
            rx,
            tick_sink: DataSink::new(
                tick_dir,
                config.max_buffered_rows,
                config.max_rows_per_file,
                config.flush_interval,
                writer_props.clone(),
            ),
            candle_sink: DataSink::new(
                candle_dir,
                config.max_buffered_rows,
                config.max_rows_per_file,
                config.flush_interval,
                writer_props.clone(),
            ),
            fill_sink: DataSink::new(
                fill_dir,
                config.max_buffered_rows,
                config.max_rows_per_file,
                config.flush_interval,
                writer_props.clone(),
            ),
            order_sink: DataSink::new(
                order_dir,
                config.max_buffered_rows,
                config.max_rows_per_file,
                config.flush_interval,
                writer_props.clone(),
            ),
            signal_sink: DataSink::new(
                signal_dir,
                config.max_buffered_rows,
                config.max_rows_per_file,
                config.flush_interval,
                writer_props,
            ),
            flush_interval: config.flush_interval,
        })
    }

    async fn run(mut self) -> Result<()> {
        let mut timer = interval(self.flush_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                message = self.rx.recv() => {
                    match message {
                        Some(msg) => self.handle_message(msg).await?,
                        None => break,
                    }
                }
                _ = timer.tick() => {
                    self.tick_sink.maybe_flush_due_time().await?;
                    self.candle_sink.maybe_flush_due_time().await?;
                    self.fill_sink.maybe_flush_due_time().await?;
                    self.order_sink.maybe_flush_due_time().await?;
                    self.signal_sink.maybe_flush_due_time().await?;
                }
            }
        }

        self.tick_sink.shutdown().await?;
        self.candle_sink.shutdown().await?;
        self.fill_sink.shutdown().await?;
        self.order_sink.shutdown().await?;
        self.signal_sink.shutdown().await?;
        Ok(())
    }

    async fn handle_message(&mut self, msg: RecorderMessage) -> Result<()> {
        match msg {
            RecorderMessage::Tick(tick) => self.tick_sink.push(tick).await?,
            RecorderMessage::Candle(candle) => self.candle_sink.push(candle).await?,
            RecorderMessage::Fill(fill) => self.fill_sink.push(fill).await?,
            RecorderMessage::Order(order) => self.order_sink.push(order).await?,
            RecorderMessage::Signal(signal) => self.signal_sink.push(signal).await?,
        }
        Ok(())
    }
}

async fn ensure_subdir(root: &Path, kind: &str) -> Result<PathBuf> {
    let path = root.join(kind);
    fs::create_dir_all(&path)
        .await
        .with_context(|| format!("failed to create {}", path.display()))?;
    Ok(path)
}

trait SinkEncoder {
    type Record: Send + 'static;
    const KIND: &'static str;

    fn schema() -> Arc<arrow::datatypes::Schema>;
    fn encode(records: &[Self::Record]) -> Result<arrow::record_batch::RecordBatch>;
    fn partition_for(record: &Self::Record) -> NaiveDate;
}

struct TickEncoder;
struct CandleEncoder;
struct FillEncoder;
struct OrderEncoder;
struct SignalEncoder;

impl SinkEncoder for TickEncoder {
    type Record = Tick;
    const KIND: &'static str = "ticks";

    fn schema() -> Arc<arrow::datatypes::Schema> {
        tick_schema()
    }

    fn encode(records: &[Self::Record]) -> Result<arrow::record_batch::RecordBatch> {
        ticks_to_batch(records)
    }

    fn partition_for(record: &Self::Record) -> NaiveDate {
        record.exchange_timestamp.date_naive()
    }
}

impl SinkEncoder for CandleEncoder {
    type Record = Candle;
    const KIND: &'static str = "candles";

    fn schema() -> Arc<arrow::datatypes::Schema> {
        candle_schema()
    }

    fn encode(records: &[Self::Record]) -> Result<arrow::record_batch::RecordBatch> {
        candles_to_batch(records)
    }

    fn partition_for(record: &Self::Record) -> NaiveDate {
        record.timestamp.date_naive()
    }
}

impl SinkEncoder for FillEncoder {
    type Record = Fill;
    const KIND: &'static str = "fills";

    fn schema() -> Arc<arrow::datatypes::Schema> {
        fill_schema()
    }

    fn encode(records: &[Self::Record]) -> Result<arrow::record_batch::RecordBatch> {
        fills_to_batch(records)
    }

    fn partition_for(record: &Self::Record) -> NaiveDate {
        record.timestamp.date_naive()
    }
}

impl SinkEncoder for OrderEncoder {
    type Record = Order;
    const KIND: &'static str = "orders";

    fn schema() -> Arc<arrow::datatypes::Schema> {
        order_schema()
    }

    fn encode(records: &[Self::Record]) -> Result<arrow::record_batch::RecordBatch> {
        orders_to_batch(records)
    }

    fn partition_for(record: &Self::Record) -> NaiveDate {
        record.updated_at.date_naive()
    }
}

impl SinkEncoder for SignalEncoder {
    type Record = Signal;
    const KIND: &'static str = "signals";

    fn schema() -> Arc<arrow::datatypes::Schema> {
        signal_schema()
    }

    fn encode(records: &[Self::Record]) -> Result<arrow::record_batch::RecordBatch> {
        signals_to_batch(records)
    }

    fn partition_for(record: &Self::Record) -> NaiveDate {
        record.generated_at.date_naive()
    }
}

struct DataSink<E: SinkEncoder> {
    dir: PathBuf,
    buffer: Vec<E::Record>,
    writer: Option<ActiveWriter>,
    partition: Option<NaiveDate>,
    max_buffered_rows: usize,
    max_rows_per_file: usize,
    flush_interval: Duration,
    last_flush: Instant,
    properties: Arc<WriterProperties>,
    file_seq: u64,
    schema: Arc<arrow::datatypes::Schema>,
    _marker: PhantomData<E>,
}

impl<E: SinkEncoder> DataSink<E> {
    fn new(
        dir: PathBuf,
        max_buffered_rows: usize,
        max_rows_per_file: usize,
        flush_interval: Duration,
        properties: Arc<WriterProperties>,
    ) -> Self {
        Self {
            dir,
            buffer: Vec::with_capacity(max_buffered_rows.max(1)),
            writer: None,
            partition: None,
            max_buffered_rows: max_buffered_rows.max(1),
            max_rows_per_file: max_rows_per_file.max(1),
            flush_interval,
            last_flush: Instant::now(),
            properties,
            file_seq: 0,
            schema: E::schema(),
            _marker: PhantomData,
        }
    }

    async fn push(&mut self, record: E::Record) -> Result<()> {
        let partition = E::partition_for(&record);
        if self.partition != Some(partition) {
            self.flush().await?;
            self.close_writer().await?;
            self.partition = Some(partition);
        }
        self.buffer.push(record);
        if self.buffer.len() >= self.max_buffered_rows {
            self.flush().await?;
        }
        Ok(())
    }

    async fn maybe_flush_due_time(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        if self.last_flush.elapsed() >= self.flush_interval {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        if self.partition.is_none() {
            if let Some(first) = self.buffer.first() {
                self.partition = Some(E::partition_for(first));
            }
        }
        let partition = self
            .partition
            .ok_or_else(|| anyhow!("buffer partition undefined"))?;

        self.ensure_writer(partition).await?;
        let rows = std::mem::take(&mut self.buffer);
        if rows.is_empty() {
            return Ok(());
        }
        let row_count = rows.len();
        let batch = match E::encode(&rows) {
            Ok(batch) => batch,
            Err(err) => {
                warn!(
                    kind = E::KIND,
                    error = %err,
                    dropped = row_count,
                    "failed to encode flight recorder batch"
                );
                self.last_flush = Instant::now();
                return Ok(());
            }
        };

        if batch.num_rows() == 0 {
            return Ok(());
        }

        if let Some(writer) = &mut self.writer {
            writer.write(&batch).await?;
            if writer.rows_written >= self.max_rows_per_file {
                if let Some(writer) = self.writer.take() {
                    writer.finish().await?;
                }
            }
        }
        self.last_flush = Instant::now();
        Ok(())
    }

    async fn ensure_writer(&mut self, partition: NaiveDate) -> Result<()> {
        if self.writer.is_some() {
            return Ok(());
        }
        let date_dir = self.dir.join(partition.format("%Y-%m-%d").to_string());
        fs::create_dir_all(&date_dir)
            .await
            .with_context(|| format!("failed to create {}", date_dir.display()))?;
        let timestamp = Utc::now().format("%Y%m%dT%H%M%S");
        let file_name = format!("{}-{}-{:04}.parquet", E::KIND, timestamp, self.file_seq);
        self.file_seq = self.file_seq.wrapping_add(1);
        let path = date_dir.join(file_name);
        let file = File::create(&path)
            .await
            .with_context(|| format!("failed to create {}", path.display()))?;
        let writer = AsyncArrowWriter::try_new(
            file,
            self.schema.clone(),
            Some(self.properties.as_ref().clone()),
        )?;
        debug!(kind = E::KIND, path = %path.display(), "opened flight recorder file");
        self.writer = Some(ActiveWriter {
            writer,
            rows_written: 0,
            path,
        });
        Ok(())
    }

    async fn close_writer(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.finish().await?;
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.flush().await?;
        self.close_writer().await?;
        Ok(())
    }
}

struct ActiveWriter {
    writer: AsyncArrowWriter<File>,
    rows_written: usize,
    path: PathBuf,
}

impl ActiveWriter {
    async fn write(&mut self, batch: &arrow::record_batch::RecordBatch) -> Result<()> {
        self.writer.write(batch).await?;
        self.rows_written += batch.num_rows();
        Ok(())
    }

    async fn finish(mut self) -> Result<()> {
        self.writer.finish().await?;
        debug!(path = %self.path.display(), "closed flight recorder file");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, Decimal128Array, Float64Array, StringArray, TimestampNanosecondArray,
    };
    use arrow::datatypes::DataType;
    use arrow::record_batch::RecordBatch;
    use chrono::Duration as ChronoDuration;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use rust_decimal::Decimal;
    use tempfile::tempdir;
    use tesser_core::{ExecutionHint, Signal, SignalKind};

    fn build_signal() -> Signal {
        let mut signal = Signal::new("BTCUSDT", SignalKind::EnterLong, 0.82);
        signal.stop_loss = Some(Decimal::new(99_500, 2));
        signal.take_profit = Some(Decimal::new(101_500, 2));
        signal.generated_at = chrono::Utc::now();
        signal.note = Some("trend-follow".into());
        signal.execution_hint = Some(ExecutionHint::Twap {
            duration: ChronoDuration::minutes(5),
        });
        signal
    }

    fn read_signal_batch(path: &Path) -> RecordBatch {
        let file = std::fs::File::open(path).expect("open parquet file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("build batch reader");
        let mut reader = builder.build().expect("build reader");
        reader.next().expect("batch result").expect("batch")
    }

    fn find_signal_file(root: &Path) -> PathBuf {
        let signals_dir = root.join(SignalEncoder::KIND);
        for day in std::fs::read_dir(&signals_dir).expect("read signal dir") {
            let day = day.expect("entry");
            if day.path().is_dir() {
                for file in std::fs::read_dir(day.path()).expect("read partition dir") {
                    let file = file.expect("file entry");
                    if file
                        .path()
                        .extension()
                        .map(|ext| ext == "parquet")
                        .unwrap_or(false)
                    {
                        return file.path();
                    }
                }
            }
        }
        panic!("no signal parquet files found in {}", signals_dir.display());
    }

    fn decimal_from_array(arr: &Decimal128Array, idx: usize) -> Decimal {
        let scale = match arr.data_type() {
            DataType::Decimal128(_, scale) => (*scale) as u32,
            _ => panic!("unexpected data type"),
        };
        Decimal::from_i128_with_scale(arr.value(idx), scale)
    }

    #[tokio::test]
    async fn writes_signal_batches() {
        let signal = build_signal();
        let temp = tempdir().expect("tempdir");
        let config = RecorderConfig {
            root: temp.path().to_path_buf(),
            max_buffered_rows: 1,
            flush_interval: Duration::from_millis(20),
            max_rows_per_file: 16,
            channel_capacity: 4,
        };
        let recorder = ParquetRecorder::spawn(config)
            .await
            .expect("spawn recorder");
        let handle = recorder.handle();
        handle.record_signal(signal.clone());
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(handle);
        recorder.shutdown().await.expect("shutdown recorder");

        let file_path = find_signal_file(temp.path());
        let batch = read_signal_batch(&file_path);
        assert_eq!(batch.num_rows(), 1);

        let ids = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.value(0), signal.id.to_string());

        let kinds = batch
            .column(batch.schema().index_of("kind").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(kinds.value(0), "enter_long");

        let confidences = batch
            .column(batch.schema().index_of("confidence").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((confidences.value(0) - signal.confidence).abs() < f64::EPSILON);

        let stop_losses = batch
            .column(batch.schema().index_of("stop_loss").unwrap())
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(
            decimal_from_array(stop_losses, 0),
            signal.stop_loss.unwrap()
        );

        let generated = batch
            .column(batch.schema().index_of("generated_at").unwrap())
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let nanos = generated.value(0);
        let secs = nanos / 1_000_000_000;
        let rem = (nanos % 1_000_000_000) as u32;
        let recorded_ts = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, rem).unwrap();
        assert_eq!(recorded_ts.timestamp(), signal.generated_at.timestamp());

        let metadata = batch
            .column(batch.schema().index_of("metadata").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let meta_value: serde_json::Value =
            serde_json::from_str(metadata.value(0)).expect("metadata json");
        assert_eq!(meta_value["note"], signal.note.as_ref().unwrap().as_str());
    }
}
