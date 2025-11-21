use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow::array::{
    ArrayRef, Decimal128Builder, Int8Builder, StringBuilder, TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use rust_decimal::prelude::RoundingStrategy;
use rust_decimal::Decimal;
use tracing::warn;

use tesser_core::{Candle, Fill, Interval, Order, OrderStatus, OrderType, Tick, TimeInForce};

const DECIMAL_PRECISION: u8 = 38;
const DECIMAL_SCALE: i8 = 18;

static DECIMAL_ROUND_WARNED: AtomicBool = AtomicBool::new(false);

fn decimal_builder(capacity: usize) -> Decimal128Builder {
    Decimal128Builder::with_capacity(capacity)
        .with_data_type(DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE))
}

fn timestamp_builder(capacity: usize) -> TimestampNanosecondBuilder {
    TimestampNanosecondBuilder::with_capacity(capacity)
        .with_data_type(DataType::Timestamp(TimeUnit::Nanosecond, None))
}

fn string_builder(capacity: usize) -> StringBuilder {
    // Allocate a reasonable byte backing assuming ~16 bytes per value.
    StringBuilder::with_capacity(capacity, capacity.saturating_mul(16))
}

fn decimal_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
        nullable,
    )
}

fn timestamp_field(name: &str) -> Field {
    Field::new(name, DataType::Timestamp(TimeUnit::Nanosecond, None), false)
}

static TICK_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        decimal_field("price", false),
        decimal_field("size", false),
        Field::new("side", DataType::Int8, false),
        timestamp_field("exchange_timestamp"),
        timestamp_field("received_at"),
    ]))
});

static CANDLE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("interval", DataType::Utf8, false),
        decimal_field("open", false),
        decimal_field("high", false),
        decimal_field("low", false),
        decimal_field("close", false),
        decimal_field("volume", false),
        timestamp_field("timestamp"),
    ]))
});

static FILL_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("side", DataType::Int8, false),
        decimal_field("fill_price", false),
        decimal_field("fill_quantity", false),
        decimal_field("fee", true),
        timestamp_field("timestamp"),
    ]))
});

static ORDER_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("side", DataType::Int8, false),
        Field::new("order_type", DataType::Utf8, false),
        Field::new("status", DataType::Int8, false),
        Field::new("time_in_force", DataType::Utf8, true),
        decimal_field("quantity", false),
        decimal_field("price", true),
        decimal_field("trigger_price", true),
        Field::new("client_order_id", DataType::Utf8, true),
        decimal_field("take_profit", true),
        decimal_field("stop_loss", true),
        decimal_field("display_quantity", true),
        decimal_field("filled_quantity", false),
        decimal_field("avg_fill_price", true),
        timestamp_field("created_at"),
        timestamp_field("updated_at"),
    ]))
});

/// Returns the schema used when encoding ticks.
pub fn tick_schema() -> SchemaRef {
    TICK_SCHEMA.clone()
}

/// Returns the schema used when encoding candles.
pub fn candle_schema() -> SchemaRef {
    CANDLE_SCHEMA.clone()
}

/// Returns the schema used when encoding fills.
pub fn fill_schema() -> SchemaRef {
    FILL_SCHEMA.clone()
}

/// Returns the schema used when encoding orders.
pub fn order_schema() -> SchemaRef {
    ORDER_SCHEMA.clone()
}

/// Converts a slice of ticks into a [`RecordBatch`].
pub fn ticks_to_batch(rows: &[Tick]) -> Result<RecordBatch> {
    let capacity = rows.len();
    let mut symbols = string_builder(capacity);
    let mut prices = decimal_builder(capacity);
    let mut sizes = decimal_builder(capacity);
    let mut sides = Int8Builder::with_capacity(capacity);
    let mut exchange_ts = timestamp_builder(capacity);
    let mut received_ts = timestamp_builder(capacity);

    for tick in rows {
        symbols.append_value(&tick.symbol);
        let price = decimal_to_i128(tick.price)?;
        prices.append_value(price);
        let size = decimal_to_i128(tick.size)?;
        sizes.append_value(size);
        sides.append_value(tick.side.as_i8());
        exchange_ts.append_value(timestamp_to_nanos(&tick.exchange_timestamp));
        received_ts.append_value(timestamp_to_nanos(&tick.received_at));
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(symbols.finish()),
        Arc::new(prices.finish()),
        Arc::new(sizes.finish()),
        Arc::new(sides.finish()),
        Arc::new(exchange_ts.finish()),
        Arc::new(received_ts.finish()),
    ];

    RecordBatch::try_new(tick_schema(), columns).context("failed to build tick batch")
}

/// Converts a slice of candles into a [`RecordBatch`].
pub fn candles_to_batch(rows: &[Candle]) -> Result<RecordBatch> {
    let capacity = rows.len();
    let mut symbols = string_builder(capacity);
    let mut intervals = string_builder(capacity);
    let mut opens = decimal_builder(capacity);
    let mut highs = decimal_builder(capacity);
    let mut lows = decimal_builder(capacity);
    let mut closes = decimal_builder(capacity);
    let mut volumes = decimal_builder(capacity);
    let mut timestamps = timestamp_builder(capacity);

    for candle in rows {
        symbols.append_value(&candle.symbol);
        intervals.append_value(interval_label(candle.interval));
        let open = decimal_to_i128(candle.open)?;
        opens.append_value(open);
        let high = decimal_to_i128(candle.high)?;
        highs.append_value(high);
        let low = decimal_to_i128(candle.low)?;
        lows.append_value(low);
        let close = decimal_to_i128(candle.close)?;
        closes.append_value(close);
        let volume = decimal_to_i128(candle.volume)?;
        volumes.append_value(volume);
        timestamps.append_value(timestamp_to_nanos(&candle.timestamp));
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(symbols.finish()),
        Arc::new(intervals.finish()),
        Arc::new(opens.finish()),
        Arc::new(highs.finish()),
        Arc::new(lows.finish()),
        Arc::new(closes.finish()),
        Arc::new(volumes.finish()),
        Arc::new(timestamps.finish()),
    ];

    RecordBatch::try_new(candle_schema(), columns).context("failed to build candle batch")
}

/// Converts a slice of fills into a [`RecordBatch`].
pub fn fills_to_batch(rows: &[Fill]) -> Result<RecordBatch> {
    let capacity = rows.len();
    let mut order_ids = string_builder(capacity);
    let mut symbols = string_builder(capacity);
    let mut sides = Int8Builder::with_capacity(capacity);
    let mut prices = decimal_builder(capacity);
    let mut quantities = decimal_builder(capacity);
    let mut fees = decimal_builder(capacity);
    let mut timestamps = timestamp_builder(capacity);

    for fill in rows {
        order_ids.append_value(&fill.order_id);
        symbols.append_value(&fill.symbol);
        sides.append_value(fill.side.as_i8());
        let price = decimal_to_i128(fill.fill_price)?;
        prices.append_value(price);
        let qty = decimal_to_i128(fill.fill_quantity)?;
        quantities.append_value(qty);
        append_decimal_option(&mut fees, fill.fee)?;
        timestamps.append_value(timestamp_to_nanos(&fill.timestamp));
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(order_ids.finish()),
        Arc::new(symbols.finish()),
        Arc::new(sides.finish()),
        Arc::new(prices.finish()),
        Arc::new(quantities.finish()),
        Arc::new(fees.finish()),
        Arc::new(timestamps.finish()),
    ];

    RecordBatch::try_new(fill_schema(), columns).context("failed to build fill batch")
}

/// Converts a slice of orders into a [`RecordBatch`].
pub fn orders_to_batch(rows: &[Order]) -> Result<RecordBatch> {
    let capacity = rows.len();
    let mut ids = string_builder(capacity);
    let mut symbols = string_builder(capacity);
    let mut sides = Int8Builder::with_capacity(capacity);
    let mut types = string_builder(capacity);
    let mut statuses = Int8Builder::with_capacity(capacity);
    let mut time_in_force = string_builder(capacity);
    let mut quantities = decimal_builder(capacity);
    let mut prices = decimal_builder(capacity);
    let mut triggers = decimal_builder(capacity);
    let mut client_order_ids = string_builder(capacity);
    let mut take_profit = decimal_builder(capacity);
    let mut stop_loss = decimal_builder(capacity);
    let mut display_qty = decimal_builder(capacity);
    let mut filled_qty = decimal_builder(capacity);
    let mut avg_fill_price = decimal_builder(capacity);
    let mut created = timestamp_builder(capacity);
    let mut updated = timestamp_builder(capacity);

    for order in rows {
        let req = &order.request;
        ids.append_value(&order.id);
        symbols.append_value(&req.symbol);
        sides.append_value(req.side.as_i8());
        types.append_value(order_type_label(req.order_type));
        statuses.append_value(status_code(order.status));
        if let Some(tif) = req.time_in_force {
            time_in_force.append_value(time_in_force_label(tif));
        } else {
            time_in_force.append_null();
        }
        let request_qty = decimal_to_i128(req.quantity)?;
        quantities.append_value(request_qty);
        append_decimal_option(&mut prices, req.price)?;
        append_decimal_option(&mut triggers, req.trigger_price)?;
        append_option_str(&mut client_order_ids, req.client_order_id.as_deref())?;
        append_decimal_option(&mut take_profit, req.take_profit)?;
        append_decimal_option(&mut stop_loss, req.stop_loss)?;
        append_decimal_option(&mut display_qty, req.display_quantity)?;
        let filled = decimal_to_i128(order.filled_quantity)?;
        filled_qty.append_value(filled);
        append_decimal_option(&mut avg_fill_price, order.avg_fill_price)?;
        created.append_value(timestamp_to_nanos(&order.created_at));
        updated.append_value(timestamp_to_nanos(&order.updated_at));
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(ids.finish()),
        Arc::new(symbols.finish()),
        Arc::new(sides.finish()),
        Arc::new(types.finish()),
        Arc::new(statuses.finish()),
        Arc::new(time_in_force.finish()),
        Arc::new(quantities.finish()),
        Arc::new(prices.finish()),
        Arc::new(triggers.finish()),
        Arc::new(client_order_ids.finish()),
        Arc::new(take_profit.finish()),
        Arc::new(stop_loss.finish()),
        Arc::new(display_qty.finish()),
        Arc::new(filled_qty.finish()),
        Arc::new(avg_fill_price.finish()),
        Arc::new(created.finish()),
        Arc::new(updated.finish()),
    ];

    RecordBatch::try_new(order_schema(), columns).context("failed to build order batch")
}

fn decimal_to_i128(value: Decimal) -> Result<i128> {
    let scale_limit = DECIMAL_SCALE as i32;
    let mut normalized = value;
    if normalized.scale() as i32 > scale_limit {
        if !DECIMAL_ROUND_WARNED.swap(true, Ordering::Relaxed) {
            warn!(
                original_scale = normalized.scale(),
                target_scale = scale_limit,
                "value scale exceeded flight recorder precision and will be rounded"
            );
        }
        normalized = normalized
            .round_dp_with_strategy(DECIMAL_SCALE as u32, RoundingStrategy::MidpointNearestEven);
    }
    let scale = normalized.scale() as i32;
    if scale > scale_limit {
        return Err(anyhow!(
            "unable to normalize decimal with scale {} for flight recorder",
            scale
        ));
    }
    let diff = scale_limit - scale;
    let factor = 10i128
        .checked_pow(diff as u32)
        .ok_or_else(|| anyhow!("decimal scaling factor overflow"))?;
    normalized
        .mantissa()
        .checked_mul(factor)
        .ok_or_else(|| anyhow!("decimal mantissa overflow"))
}

fn append_decimal_option(builder: &mut Decimal128Builder, value: Option<Decimal>) -> Result<()> {
    if let Some(v) = value {
        let scaled = decimal_to_i128(v)?;
        builder.append_value(scaled);
    } else {
        builder.append_null();
    }
    Ok(())
}

fn append_option_str(builder: &mut StringBuilder, value: Option<&str>) -> Result<()> {
    if let Some(text) = value {
        builder.append_value(text);
    } else {
        builder.append_null();
    }
    Ok(())
}

fn timestamp_to_nanos(ts: &DateTime<Utc>) -> i64 {
    ts.timestamp_nanos_opt()
        .unwrap_or_else(|| ts.timestamp_micros() * 1_000)
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

fn order_type_label(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Market => "market",
        OrderType::Limit => "limit",
        OrderType::StopMarket => "stop_market",
    }
}

fn time_in_force_label(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GoodTilCanceled => "gtc",
        TimeInForce::ImmediateOrCancel => "ioc",
        TimeInForce::FillOrKill => "fok",
    }
}

fn status_code(status: OrderStatus) -> i8 {
    match status {
        OrderStatus::PendingNew => 0,
        OrderStatus::Accepted => 1,
        OrderStatus::PartiallyFilled => 2,
        OrderStatus::Filled => 3,
        OrderStatus::Canceled => 4,
        OrderStatus::Rejected => 5,
    }
}
