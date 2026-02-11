//! Exponential Moving Average (EMA).

use std::marker::PhantomData;

use rust_decimal::Decimal;

use crate::core::{decimal_from_usize, Indicator, IndicatorError, Input};

/// Computes an exponentially-weighted moving average using Wilder's smoothing.
#[derive(Debug, Clone)]
pub struct Ema<I = Decimal> {
    period: usize,
    alpha: Decimal,
    divisor: Decimal,
    state: Option<Decimal>,
    warmup_sum: Decimal,
    warmup_count: usize,
    marker: PhantomData<I>,
}

impl<I> Ema<I>
where
    I: Input,
{
    /// Creates a new EMA with the provided period.
    pub fn new(period: usize) -> Result<Self, IndicatorError> {
        if period == 0 {
            return Err(IndicatorError::invalid_period("EMA", period));
        }

        let alpha = Decimal::from(2) / decimal_from_usize(period + 1);

        Ok(Self {
            period,
            alpha,
            divisor: decimal_from_usize(period),
            state: None,
            warmup_sum: Decimal::ZERO,
            warmup_count: 0,
            marker: PhantomData,
        })
    }

    /// Returns the current EMA value, if the indicator has produced one.
    pub fn value(&self) -> Option<Decimal> {
        self.state
    }
}

impl<I> Indicator for Ema<I>
where
    I: Input,
{
    type Input = I;
    type Output = Decimal;

    fn next(&mut self, input: Self::Input) -> Option<Self::Output> {
        let value = input.value();

        if let Some(current) = self.state {
            let next = (value - current) * self.alpha + current;
            self.state = Some(next);
            Some(next)
        } else {
            self.warmup_sum += value;
            self.warmup_count += 1;

            if self.warmup_count == self.period {
                let average = self.warmup_sum / self.divisor;
                self.state = Some(average);
                self.warmup_sum = Decimal::ZERO;
                self.warmup_count = 0;
                Some(average)
            } else {
                None
            }
        }
    }

    fn reset(&mut self) {
        self.state = None;
        self.warmup_sum = Decimal::ZERO;
        self.warmup_count = 0;
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use super::Ema;
    use crate::Indicator;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    #[test]
    fn produces_average_after_warmup() {
        let mut ema = Ema::new(3).unwrap();
        assert_eq!(ema.next(dec("1")), None);
        assert_eq!(ema.next(dec("2")), None);
        assert_eq!(ema.next(dec("3")), Some(dec("2")));
    }

    #[test]
    fn updates_in_constant_time() {
        let mut ema = Ema::new(3).unwrap();
        ema.next(dec("1"));
        ema.next(dec("2"));
        ema.next(dec("3"));
        // Alpha = 0.5 for period 3, so next value is 0.5*(4-2)+2 = 3
        assert_eq!(ema.next(dec("4")).unwrap().round_dp(4), dec("3"));
    }

    #[test]
    fn reset_clears_state() {
        let mut ema = Ema::new(2).unwrap();
        ema.next(dec("1"));
        ema.next(dec("2"));
        assert!(ema.next(dec("3")).is_some());
        ema.reset();
        assert_eq!(ema.next(dec("4")), None);
    }
}
