use chrono::{DateTime, Utc};

/// Canonical time type across the workspace (nanosecond precision).
pub type NanoTime = DateTime<Utc>;

/// Returns the current UTC time with nanosecond precision.
#[inline]
pub fn now() -> NanoTime {
    Utc::now()
}

/// Convert a UTC timestamp into unix nanoseconds.
#[inline]
pub fn to_nanos(ts: &NanoTime) -> i64 {
    ts.timestamp_nanos_opt()
        .unwrap_or_else(|| ts.timestamp_micros() * 1_000)
}

/// Convert a UTC timestamp into unix milliseconds (lossy).
#[inline]
pub fn to_millis(ts: &NanoTime) -> i64 {
    to_nanos(ts) / 1_000_000
}

/// Build a UTC timestamp from unix nanoseconds.
#[inline]
pub fn from_nanos(nanos: i64) -> Option<NanoTime> {
    let secs = nanos.div_euclid(1_000_000_000);
    let sub = nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, sub)
}

/// Build a UTC timestamp from unix milliseconds.
#[inline]
pub fn from_millis(millis: i64) -> Option<NanoTime> {
    let nanos = millis.saturating_mul(1_000_000);
    from_nanos(nanos)
}
