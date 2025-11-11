use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::Client;
use serde_json::json;
use tesser_config::AlertingConfig;
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{error, warn};

#[derive(Clone)]
pub struct AlertDispatcher {
    client: Client,
    webhook: Option<String>,
}

impl AlertDispatcher {
    pub fn new(webhook: Option<String>) -> Self {
        Self {
            client: Client::builder().build().expect("reqwest client"),
            webhook,
        }
    }

    pub async fn notify(&self, title: &str, message: &str) {
        warn!(%title, %message, "alert raised");
        let Some(url) = self.webhook.as_ref() else {
            return;
        };
        let payload = json!({ "title": title, "message": message });
        if let Err(err) = self.client.post(url).json(&payload).send().await {
            error!(error = %err, "failed to send alert webhook");
        }
    }
}

struct AlertState {
    last_data: Instant,
    consecutive_failures: u32,
    peak_equity: f64,
    drawdown_triggered: bool,
    data_gap_triggered: bool,
}

pub struct AlertManager {
    config: AlertingConfig,
    dispatcher: AlertDispatcher,
    state: Arc<Mutex<AlertState>>,
}

impl AlertManager {
    pub fn new(config: AlertingConfig, dispatcher: AlertDispatcher) -> Self {
        let state = AlertState {
            last_data: Instant::now(),
            consecutive_failures: 0,
            peak_equity: 0.0,
            drawdown_triggered: false,
            data_gap_triggered: false,
        };
        Self {
            config,
            dispatcher,
            state: Arc::new(Mutex::new(state)),
        }
    }

    pub async fn heartbeat(&self) {
        let mut state = self.state.lock().await;
        state.last_data = Instant::now();
        state.data_gap_triggered = false;
    }

    pub async fn order_failure(&self, reason: &str) {
        let mut state = self.state.lock().await;
        state.consecutive_failures += 1;
        let max_failures = self.config.max_order_failures.max(1);
        if state.consecutive_failures >= max_failures {
            drop(state);
            self.dispatcher
                .notify(
                    "Execution failures",
                    &format!("{} consecutive order failures ({reason})", max_failures),
                )
                .await;
            let mut state = self.state.lock().await;
            state.consecutive_failures = 0;
        }
    }

    pub async fn reset_order_failures(&self) {
        let mut state = self.state.lock().await;
        state.consecutive_failures = 0;
    }

    pub async fn notify(&self, title: &str, message: &str) {
        self.dispatcher.notify(title, message).await;
    }

    pub async fn update_equity(&self, equity: f64) {
        if equity <= 0.0 {
            return;
        }
        let mut state = self.state.lock().await;
        if equity > state.peak_equity {
            state.peak_equity = equity;
            state.drawdown_triggered = false;
            return;
        }
        if state.peak_equity <= 0.0 {
            state.peak_equity = equity;
            return;
        }
        let drawdown = (state.peak_equity - equity) / state.peak_equity;
        if drawdown >= self.config.max_drawdown && !state.drawdown_triggered {
            state.drawdown_triggered = true;
            let peak = state.peak_equity;
            drop(state);
            self.dispatcher
                .notify(
                    "Drawdown limit breached",
                    &format!(
                        "Current equity {:.2} vs peak {:.2} (drawdown {:.2}%)",
                        equity,
                        peak,
                        drawdown * 100.0
                    ),
                )
                .await;
        }
    }

    pub fn spawn_watchdog(&self) -> Option<tokio::task::JoinHandle<()>> {
        let threshold = self.config.max_data_gap_secs;
        if threshold == 0 {
            return None;
        }
        let dispatcher = self.dispatcher.clone();
        let state = self.state.clone();
        let period = Duration::from_secs(threshold);
        Some(tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(30));
            loop {
                ticker.tick().await;
                let mut guard = state.lock().await;
                if guard.last_data.elapsed() >= period && !guard.data_gap_triggered {
                    guard.data_gap_triggered = true;
                    drop(guard);
                    dispatcher
                        .notify("Market data stalled", "No heartbeat in configured window")
                        .await;
                }
            }
        }))
    }
}

pub fn sanitize_webhook(input: Option<String>) -> Option<String> {
    input.and_then(|value| {
        let trimmed = value.trim().to_string();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}
