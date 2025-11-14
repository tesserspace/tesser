use std::convert::Infallible;
use std::fs::{self, OpenOptions};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::OnceLock;

use anyhow::{Context, Result};
use hyper::body::Body;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Request, Response, StatusCode};
use prometheus::{Encoder, Gauge, GaugeVec, IntCounter, Registry, TextEncoder};
use tracing::{error, info};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

static FILE_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

/// Install the global tracing subscriber with optional JSON file logging.
pub fn init_tracing(filter: &str, log_path: Option<&Path>) -> Result<()> {
    if let Some(path) = log_path {
        let stdout_layer = fmt::layer()
            .with_target(false)
            .with_filter(EnvFilter::new(filter));
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)
                .with_context(|| format!("failed to create log directory {dir:?}"))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("failed to open log file {}", path.display()))?;
        let (writer, guard) = tracing_appender::non_blocking(file);
        let _ = FILE_GUARD.set(guard);
        let file_layer = fmt::layer()
            .json()
            .with_ansi(false)
            .with_target(true)
            .with_writer(writer)
            .with_filter(EnvFilter::new(filter));
        tracing_subscriber::registry()
            .with(stdout_layer)
            .with(file_layer)
            .try_init()?;
    } else {
        let stdout_layer = fmt::layer()
            .with_target(false)
            .with_filter(EnvFilter::new(filter));
        tracing_subscriber::registry()
            .with(stdout_layer)
            .try_init()?;
    }

    Ok(())
}

/// Prometheus metrics collected during live trading.
pub struct LiveMetrics {
    registry: Registry,
    ticks_total: IntCounter,
    candles_total: IntCounter,
    signals_total: IntCounter,
    orders_total: IntCounter,
    order_failures: IntCounter,
    equity_gauge: Gauge,
    price_gauge: GaugeVec,
    data_gap_gauge: Gauge,
}

impl LiveMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let ticks_total = IntCounter::new("ticks_total", "Number of ticks processed").unwrap();
        let candles_total =
            IntCounter::new("candles_total", "Number of candles processed").unwrap();
        let signals_total =
            IntCounter::new("signals_total", "Signals emitted by strategies").unwrap();
        let orders_total =
            IntCounter::new("orders_total", "Orders submitted to execution").unwrap();
        let order_failures = IntCounter::new("order_failures_total", "Execution failures").unwrap();
        let equity_gauge = Gauge::new("portfolio_equity", "Current portfolio equity").unwrap();
        let price_gauge = GaugeVec::new(
            prometheus::Opts::new("symbol_price", "Latest observed price per symbol"),
            &["symbol"],
        )
        .unwrap();
        let data_gap_gauge = Gauge::new(
            "market_data_gap_seconds",
            "Seconds since last market data heartbeat",
        )
        .unwrap();

        registry.register(Box::new(ticks_total.clone())).unwrap();
        registry.register(Box::new(candles_total.clone())).unwrap();
        registry.register(Box::new(signals_total.clone())).unwrap();
        registry.register(Box::new(orders_total.clone())).unwrap();
        registry.register(Box::new(order_failures.clone())).unwrap();
        registry.register(Box::new(equity_gauge.clone())).unwrap();
        registry.register(Box::new(price_gauge.clone())).unwrap();
        registry.register(Box::new(data_gap_gauge.clone())).unwrap();

        Self {
            registry,
            ticks_total,
            candles_total,
            signals_total,
            orders_total,
            order_failures,
            equity_gauge,
            price_gauge,
            data_gap_gauge,
        }
    }

    pub fn registry(&self) -> Registry {
        self.registry.clone()
    }

    pub fn inc_tick(&self) {
        self.ticks_total.inc();
    }

    pub fn inc_candle(&self) {
        self.candles_total.inc();
    }

    pub fn inc_signals(&self, count: usize) {
        self.signals_total.inc_by(count as u64);
    }

    pub fn inc_order(&self) {
        self.orders_total.inc();
    }

    pub fn inc_order_failure(&self) {
        self.order_failures.inc();
    }

    pub fn update_equity(&self, equity: f64) {
        self.equity_gauge.set(equity);
    }

    pub fn update_price(&self, symbol: &str, price: f64) {
        self.price_gauge.with_label_values(&[symbol]).set(price);
    }

    pub fn update_staleness(&self, seconds: f64) {
        self.data_gap_gauge.set(seconds);
    }
}

impl Default for LiveMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Launch a lightweight HTTP server that exposes Prometheus metrics.
pub fn spawn_metrics_server(registry: Registry, addr: SocketAddr) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let make_svc = make_service_fn(move |_| {
            let registry = registry.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |_req: Request<Body>| {
                    let registry = registry.clone();
                    async move {
                        let encoder = TextEncoder::new();
                        let metric_families = registry.gather();
                        let mut buffer = Vec::new();
                        if let Err(err) = encoder.encode(&metric_families, &mut buffer) {
                            error!(error = %err, "failed to encode Prometheus metrics");
                            return Ok::<_, Infallible>(
                                Response::builder()
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(Body::from("failed to encode metrics"))
                                    .unwrap(),
                            );
                        }
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("Content-Type", encoder.format_type())
                                .body(Body::from(buffer))
                                .unwrap(),
                        )
                    }
                }))
            }
        });

        if let Err(err) = hyper::Server::bind(&addr).serve(make_svc).await {
            error!(error = %err, %addr, "metrics server terminated");
        } else {
            info!(%addr, "metrics server shutdown");
        }
    })
}
