//! Data utilities including streaming and historical downloads.

pub mod download;
pub mod encoding;
pub mod recorder;

/// Data distribution utilities that consume [`tesser_broker::MarketStream`].
use anyhow::Context;
use async_trait::async_trait;
#[cfg(test)]
use rust_decimal::Decimal;
use tokio::time::{sleep, Duration};
use tracing::instrument;

use tesser_broker::{BrokerResult, MarketStream};
use tesser_core::{Candle, OrderBook, Tick};

/// Abstract handler for tick events.
#[async_trait]
pub trait TickHandler: Send {
    /// Called for each incoming tick.
    async fn on_tick(&mut self, tick: Tick) -> anyhow::Result<()>;
}

/// Abstract handler for candle events.
#[async_trait]
pub trait CandleHandler: Send {
    /// Called for each incoming candle.
    async fn on_candle(&mut self, candle: Candle) -> anyhow::Result<()>;
}

/// Abstract handler for order book events.
#[async_trait]
pub trait OrderBookHandler: Send {
    /// Called for each incoming order book snapshot.
    async fn on_order_book(&mut self, book: OrderBook) -> anyhow::Result<()>;
}

/// Pulls data from a [`MarketStream`] and forwards it to registered handlers.
pub struct DataDistributor<S> {
    stream: S,
    backoff: Duration,
}

impl<S> DataDistributor<S>
where
    S: MarketStream,
{
    /// Build a new distributor with the provided stream.
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            backoff: Duration::from_millis(200),
        }
    }

    /// Run the event loop until the stream ends.
    #[instrument(skip_all)]
    pub async fn run(
        &mut self,
        tick_handler: &mut dyn TickHandler,
        candle_handler: &mut dyn CandleHandler,
        order_book_handler: &mut dyn OrderBookHandler,
    ) -> BrokerResult<()> {
        loop {
            let mut progressed = false;

            if let Some(tick) = self.stream.next_tick().await? {
                tick_handler
                    .on_tick(tick)
                    .await
                    .context("tick handler failed")
                    .map_err(|err| tesser_broker::BrokerError::Other(err.to_string()))?;
                progressed = true;
            }

            if let Some(candle) = self.stream.next_candle().await? {
                candle_handler
                    .on_candle(candle)
                    .await
                    .context("candle handler failed")
                    .map_err(|err| tesser_broker::BrokerError::Other(err.to_string()))?;
                progressed = true;
            }

            if let Some(book) = self.stream.next_order_book().await? {
                order_book_handler
                    .on_order_book(book)
                    .await
                    .context("order book handler failed")
                    .map_err(|err| tesser_broker::BrokerError::Other(err.to_string()))?;
                progressed = true;
            }

            if !progressed {
                // Avoid busy looping when the upstream stream is momentarily idle.
                sleep(self.backoff).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tesser_broker::BrokerInfo;
    use tesser_core::{Interval, Side, Symbol};

    struct TestStream {
        ticks: Vec<Tick>,
        candles: Vec<Candle>,
    }

    #[async_trait]
    impl MarketStream for TestStream {
        type Subscription = ();

        fn name(&self) -> &str {
            "test"
        }

        fn info(&self) -> Option<&BrokerInfo> {
            None
        }

        async fn subscribe(&mut self, _subscription: Self::Subscription) -> BrokerResult<()> {
            Ok(())
        }

        async fn next_tick(&mut self) -> BrokerResult<Option<Tick>> {
            Ok(self.ticks.pop())
        }

        async fn next_candle(&mut self) -> BrokerResult<Option<Candle>> {
            Ok(self.candles.pop())
        }

        async fn next_order_book(&mut self) -> BrokerResult<Option<tesser_core::OrderBook>> {
            Ok(None)
        }
    }

    struct CountHandler {
        ticks: Arc<AtomicUsize>,
        candles: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TickHandler for CountHandler {
        async fn on_tick(&mut self, _tick: Tick) -> anyhow::Result<()> {
            self.ticks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl CandleHandler for CountHandler {
        async fn on_candle(&mut self, _candle: Candle) -> anyhow::Result<()> {
            self.candles.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl OrderBookHandler for CountHandler {
        async fn on_order_book(&mut self, _book: OrderBook) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn distributor_pumps_events() {
        let ticks = vec![Tick {
            symbol: Symbol::from("BTCUSDT"),
            price: Decimal::ONE,
            size: Decimal::ONE,
            side: Side::Buy,
            exchange_timestamp: chrono::Utc::now(),
            received_at: chrono::Utc::now(),
        }];
        let candles = vec![Candle {
            symbol: Symbol::from("BTCUSDT"),
            interval: Interval::OneMinute,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: Decimal::ONE,
            timestamp: chrono::Utc::now(),
        }];
        let mut distributor = DataDistributor::new(TestStream { ticks, candles });
        let ticks_counter = Arc::new(AtomicUsize::new(0));
        let candles_counter = Arc::new(AtomicUsize::new(0));
        let mut tick_handler = CountHandler {
            ticks: ticks_counter.clone(),
            candles: candles_counter.clone(),
        };
        let mut candle_handler = CountHandler {
            ticks: ticks_counter.clone(),
            candles: candles_counter.clone(),
        };
        let mut order_book_handler = CountHandler {
            ticks: ticks_counter.clone(),
            candles: candles_counter.clone(),
        };
        let _ = tokio::time::timeout(Duration::from_millis(50), async {
            distributor
                .run(
                    &mut tick_handler,
                    &mut candle_handler,
                    &mut order_book_handler,
                )
                .await
        })
        .await;
        assert!(ticks_counter.load(Ordering::SeqCst) >= 1);
        assert!(candles_counter.load(Ordering::SeqCst) >= 1);
    }
}
