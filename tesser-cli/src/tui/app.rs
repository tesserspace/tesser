use std::collections::VecDeque;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tesser_rpc::conversions::from_decimal_proto;
use tesser_rpc::proto::{
    self, CancelAllResponse, Event, GetStatusResponse, OrderSnapshot, PortfolioSnapshot,
};

const LOG_CAPACITY: usize = 200;

#[derive(Clone)]
pub struct MonitorConfig {
    pub control_addr: String,
    pub tick_rate: Duration,
}

impl MonitorConfig {
    pub fn new(control_addr: String, tick_rate: Duration) -> Self {
        Self {
            control_addr,
            tick_rate,
        }
    }
}

pub struct MonitorApp {
    config: MonitorConfig,
    status: Option<GetStatusResponse>,
    portfolio: Option<PortfolioSnapshot>,
    orders: Vec<OrderSnapshot>,
    logs: VecDeque<LogEntry>,
    last_error: Option<String>,
    last_snapshot_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,
    stream_connected: bool,
    cancel_in_progress: bool,
    should_quit: bool,
    overlay: CommandOverlay,
    overlay_error: Option<String>,
}

impl MonitorApp {
    pub fn new(config: MonitorConfig) -> Self {
        Self {
            config,
            status: None,
            portfolio: None,
            orders: Vec::new(),
            logs: VecDeque::with_capacity(LOG_CAPACITY),
            last_error: None,
            last_snapshot_at: None,
            last_event_at: None,
            stream_connected: false,
            cancel_in_progress: false,
            should_quit: false,
            overlay: CommandOverlay::Hidden,
            overlay_error: None,
        }
    }

    pub fn control_addr(&self) -> &str {
        &self.config.control_addr
    }

    pub fn tick_rate(&self) -> Duration {
        self.config.tick_rate
    }

    pub fn should_quit(&self) -> bool {
        self.should_quit
    }

    pub fn request_quit(&mut self) {
        self.should_quit = true;
    }

    pub fn on_status(&mut self, status: GetStatusResponse) {
        self.status = Some(status);
        self.last_snapshot_at = Some(Utc::now());
        self.clear_error();
    }

    pub fn on_portfolio(&mut self, snapshot: PortfolioSnapshot) {
        self.portfolio = Some(snapshot);
        self.last_snapshot_at = Some(Utc::now());
        self.clear_error();
    }

    pub fn on_orders(&mut self, orders: Vec<OrderSnapshot>) {
        self.orders = orders;
        self.last_snapshot_at = Some(Utc::now());
        self.clear_error();
    }

    pub fn on_stream_event(&mut self, event: Event) {
        self.last_event_at = Some(Utc::now());
        if let Some(entry) = LogEntry::from_event(event) {
            self.push_log(entry);
        }
    }

    pub fn log(&self) -> impl DoubleEndedIterator<Item = &LogEntry> {
        self.logs.iter()
    }

    pub fn orders(&self) -> &[OrderSnapshot] {
        &self.orders
    }

    pub fn status(&self) -> Option<&GetStatusResponse> {
        self.status.as_ref()
    }

    pub fn portfolio(&self) -> Option<&PortfolioSnapshot> {
        self.portfolio.as_ref()
    }

    pub fn sub_accounts(&self) -> Option<&[proto::SubAccountSnapshot]> {
        self.portfolio.as_ref().map(|p| p.sub_accounts.as_slice())
    }

    pub fn balances(&self) -> Option<&[proto::CashBalance]> {
        self.portfolio.as_ref().map(|p| p.balances.as_slice())
    }

    pub fn positions(&self) -> Option<&[proto::Position]> {
        self.portfolio.as_ref().map(|p| p.positions.as_slice())
    }

    pub fn equity(&self) -> Option<Decimal> {
        self.portfolio
            .as_ref()
            .and_then(|p| decimal_from_option(p.equity.as_ref()))
    }

    pub fn realized_pnl(&self) -> Option<Decimal> {
        self.portfolio
            .as_ref()
            .and_then(|p| decimal_from_option(p.realized_pnl.as_ref()))
    }

    pub fn initial_equity(&self) -> Option<Decimal> {
        self.portfolio
            .as_ref()
            .and_then(|p| decimal_from_option(p.initial_equity.as_ref()))
    }

    pub fn reporting_currency(&self) -> Option<&str> {
        self.portfolio
            .as_ref()
            .map(|p| p.reporting_currency.as_str())
    }

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    pub fn set_error(&mut self, msg: impl Into<String>) {
        let message = msg.into();
        self.last_error = Some(message.clone());
        self.push_log(LogEntry {
            timestamp: Utc::now(),
            category: LogCategory::Error,
            message,
        });
    }

    pub fn clear_error(&mut self) {
        self.last_error = None;
    }

    pub fn last_snapshot_at(&self) -> Option<DateTime<Utc>> {
        self.last_snapshot_at
    }

    pub fn last_event_at(&self) -> Option<DateTime<Utc>> {
        self.last_event_at
    }

    pub fn set_stream_connected(&mut self, connected: bool) {
        self.stream_connected = connected;
    }

    pub fn stream_connected(&self) -> bool {
        self.stream_connected
    }

    pub fn set_cancel_in_progress(&mut self, active: bool) {
        self.cancel_in_progress = active;
    }

    pub fn cancel_in_progress(&self) -> bool {
        self.cancel_in_progress
    }

    pub fn overlay(&self) -> &CommandOverlay {
        &self.overlay
    }

    pub fn overlay_error(&self) -> Option<&str> {
        self.overlay_error.as_deref()
    }

    pub fn overlay_visible(&self) -> bool {
        !matches!(self.overlay, CommandOverlay::Hidden)
    }

    pub fn open_command_palette(&mut self) {
        self.overlay = CommandOverlay::Palette;
        self.overlay_error = None;
    }

    pub fn close_overlay(&mut self) {
        self.overlay = CommandOverlay::Hidden;
        self.overlay_error = None;
    }

    pub fn begin_cancel_confirmation(&mut self) {
        self.overlay = CommandOverlay::Confirm {
            buffer: String::new(),
        };
        self.overlay_error = None;
    }

    pub fn append_confirmation_char(&mut self, ch: char) {
        if let CommandOverlay::Confirm { buffer } = &mut self.overlay {
            buffer.push(ch);
        }
    }

    pub fn backspace_confirmation(&mut self) {
        if let CommandOverlay::Confirm { buffer } = &mut self.overlay {
            buffer.pop();
        }
    }

    pub fn confirmation_buffer(&self) -> Option<&str> {
        match &self.overlay {
            CommandOverlay::Confirm { buffer } => Some(buffer.as_str()),
            _ => None,
        }
    }

    pub fn confirmation_matches(&self) -> bool {
        self.confirmation_buffer()
            .map(|buf| buf.trim().eq_ignore_ascii_case("cancel all"))
            .unwrap_or(false)
    }

    pub fn set_overlay_error(&mut self, msg: impl Into<String>) {
        self.overlay_error = Some(msg.into());
    }

    pub fn toggle_command_palette(&mut self) {
        if matches!(self.overlay, CommandOverlay::Palette) {
            self.close_overlay();
        } else {
            self.open_command_palette();
        }
    }

    pub fn record_info(&mut self, msg: impl Into<String>) {
        self.push_log(LogEntry::info(msg));
    }

    pub fn record_cancel_result(&mut self, response: CancelAllResponse) {
        self.cancel_in_progress = false;
        let message = format!(
            "CancelAll completed: {} orders, {} algos",
            response.cancelled_orders, response.cancelled_algorithms
        );
        self.push_log(LogEntry {
            timestamp: Utc::now(),
            category: LogCategory::Info,
            message,
        });
    }

    fn push_log(&mut self, entry: LogEntry) {
        if self.logs.len() == LOG_CAPACITY {
            self.logs.pop_front();
        }
        self.logs.push_back(entry);
    }
}

#[derive(Clone, Copy, Debug)]
pub enum LogCategory {
    Signal,
    Fill,
    Order,
    Info,
    Error,
}

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub category: LogCategory,
    pub message: String,
}

impl LogEntry {
    pub fn info(msg: impl Into<String>) -> Self {
        Self {
            timestamp: Utc::now(),
            category: LogCategory::Info,
            message: msg.into(),
        }
    }

    pub fn timestamp_short(&self) -> String {
        self.timestamp.format("%H:%M:%S").to_string()
    }

    pub fn from_event(event: Event) -> Option<Self> {
        use tesser_rpc::proto::event::Payload;

        let payload = event.payload?;
        let timestamp = Utc::now();

        match payload {
            Payload::Signal(signal) => Some(Self {
                timestamp,
                category: LogCategory::Signal,
                message: format!(
                    "Signal {} {} (conf {:.2})",
                    signal.symbol,
                    signal_kind(signal.kind),
                    signal.confidence
                ),
            }),
            Payload::Fill(fill) => Some(Self {
                timestamp,
                category: LogCategory::Fill,
                message: format!(
                    "Fill {} {} {} @ {} (fee {})",
                    fill.symbol,
                    side_label(fill.side),
                    decimal_to_string(fill.fill_quantity.as_ref()),
                    decimal_to_string(fill.fill_price.as_ref()),
                    decimal_to_string(fill.fee.as_ref())
                ),
            }),
            Payload::Order(order) => Some(Self {
                timestamp,
                category: LogCategory::Order,
                message: format!(
                    "Order {} {} {} {}/{} @ {}",
                    order.id,
                    order.symbol,
                    order_status(order.status),
                    decimal_to_string(order.filled_quantity.as_ref()),
                    decimal_to_string(order.quantity.as_ref()),
                    decimal_to_string(order.avg_fill_price.as_ref())
                ),
            }),
            _ => None,
        }
    }
}

fn decimal_from_option(proto: Option<&proto::Decimal>) -> Option<Decimal> {
    proto.map(|inner| from_decimal_proto(inner.clone()))
}

fn decimal_to_string(proto: Option<&proto::Decimal>) -> String {
    decimal_from_option(proto)
        .map(|d| d.normalize().to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn signal_kind(kind: i32) -> &'static str {
    match proto::signal::Kind::try_from(kind).unwrap_or(proto::signal::Kind::Unspecified) {
        proto::signal::Kind::EnterLong => "ENTER_LONG",
        proto::signal::Kind::ExitLong => "EXIT_LONG",
        proto::signal::Kind::EnterShort => "ENTER_SHORT",
        proto::signal::Kind::ExitShort => "EXIT_SHORT",
        proto::signal::Kind::Flatten => "FLATTEN",
        proto::signal::Kind::Unspecified => "UNKNOWN",
    }
}

fn side_label(side: i32) -> &'static str {
    match proto::Side::try_from(side).unwrap_or(proto::Side::Unspecified) {
        proto::Side::Buy => "BUY",
        proto::Side::Sell => "SELL",
        proto::Side::Unspecified => "NA",
    }
}

fn order_status(status: i32) -> &'static str {
    match proto::OrderStatus::try_from(status).unwrap_or(proto::OrderStatus::Unspecified) {
        proto::OrderStatus::PendingNew => "PENDING",
        proto::OrderStatus::Accepted => "ACCEPTED",
        proto::OrderStatus::PartiallyFilled => "PARTIAL",
        proto::OrderStatus::Filled => "FILLED",
        proto::OrderStatus::Canceled => "CANCELLED",
        proto::OrderStatus::Rejected => "REJECTED",
        proto::OrderStatus::Unspecified => "UNKNOWN",
    }
}

#[derive(Clone)]
pub enum CommandOverlay {
    Hidden,
    Palette,
    Confirm { buffer: String },
}
