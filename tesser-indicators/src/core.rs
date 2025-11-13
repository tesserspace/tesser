//! Core traits shared by all indicators.

use rust_decimal::{prelude::FromPrimitive, Decimal};
use tesser_core::Candle;
use thiserror::Error;

use crate::combinators::PipedIndicator;

/// Provides a uniform way to extract a `Decimal` value from different input types.
pub trait Input {
    /// Returns the canonical decimal representation for the input.
    fn value(&self) -> Decimal;
}

impl Input for Decimal {
    fn value(&self) -> Decimal {
        *self
    }
}

impl Input for f64 {
    fn value(&self) -> Decimal {
        decimal_from_f64(*self)
    }
}

impl Input for Candle {
    fn value(&self) -> Decimal {
        decimal_from_f64(self.close)
    }
}

impl<T> Input for &T
where
    T: Input + ?Sized,
{
    fn value(&self) -> Decimal {
        T::value(self)
    }
}

/// Common configuration errors emitted by indicators.
#[derive(Debug, Error, PartialEq)]
pub enum IndicatorError {
    /// Returned when a period of zero is provided.
    #[error("{name} requires period > 0 (got {period})")]
    InvalidPeriod {
        /// Human-readable indicator name.
        name: &'static str,
        /// User-provided period value.
        period: usize,
    },
    /// Returned when a parameter must be non-negative.
    #[error("{name} parameter '{parameter}' must be non-negative (got {value})")]
    InvalidParameter {
        /// Human-readable indicator name.
        name: &'static str,
        /// Name of the invalid parameter (e.g., `std_multiplier`).
        parameter: &'static str,
        /// Provided parameter value.
        value: Decimal,
    },
}

impl IndicatorError {
    /// Helper constructor for invalid period errors.
    pub fn invalid_period(name: &'static str, period: usize) -> Self {
        Self::InvalidPeriod { name, period }
    }

    /// Helper constructor for invalid parameter errors.
    pub fn invalid_parameter(name: &'static str, parameter: &'static str, value: Decimal) -> Self {
        Self::InvalidParameter {
            name,
            parameter,
            value,
        }
    }
}

/// Core abstraction implemented by every indicator in the library.
pub trait Indicator {
    /// Input type accepted by the indicator.
    type Input: Input;
    /// Value produced after each update.
    type Output;

    /// Consumes a new data point and returns the most recent value, if available.
    fn next(&mut self, input: Self::Input) -> Option<Self::Output>;

    /// Resets the indicator to its initial state.
    fn reset(&mut self);

    /// Chains the current indicator with another indicator, feeding this output into the next.
    fn pipe<Next>(self, next: Next) -> PipedIndicator<Self, Next>
    where
        Self: Sized,
        Next: Indicator<Input = Self::Output>,
    {
        PipedIndicator::new(self, next)
    }
}

fn decimal_from_f64(value: f64) -> Decimal {
    Decimal::from_f64(value)
        .or_else(|| Decimal::from_f64_retain(value))
        .expect("failed to convert f64 into Decimal")
}

pub(crate) fn decimal_from_usize(value: usize) -> Decimal {
    Decimal::from_usize(value).expect("usize should convert into Decimal")
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::Utc;
    use rust_decimal::Decimal;
    use tesser_core::{Candle, Interval, Symbol};

    use super::{decimal_from_f64, Input};

    #[test]
    fn converts_f64_to_decimal() {
        let value = 42.1234_f64;
        let decimal = decimal_from_f64(value);
        assert_eq!(decimal.round_dp(4), Decimal::from_str("42.1234").unwrap());
    }

    #[test]
    fn candle_input_uses_close_price() {
        let candle = Candle {
            symbol: Symbol::from("BTCUSDT"),
            interval: Interval::OneMinute,
            open: 10.0,
            high: 15.0,
            low: 9.5,
            close: 12.5,
            volume: 100.0,
            timestamp: Utc::now(),
        };

        let decimal = candle.value();
        assert_eq!(decimal.round_dp(1), Decimal::from_str("12.5").unwrap());
    }

    #[test]
    fn reference_input_delegates_to_inner_type() {
        let price = Decimal::from(100);
        assert_eq!((price).value(), Decimal::from(100));
    }
}
