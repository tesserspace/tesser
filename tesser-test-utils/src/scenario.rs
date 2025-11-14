use std::sync::Arc;
use std::time::Duration;

use hyper::StatusCode;
use rust_decimal::Decimal;
use serde_json::Value;
use tokio::sync::Mutex;

/// Identifies the lifecycle event that can trigger a scripted behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScenarioTrigger {
    OrderCreate,
    OrderCancel,
    PrivateDisconnect,
    PublicStream,
}

/// Scripted behavior that mutates the mock exchange interaction.
#[derive(Clone, Debug)]
pub enum ScenarioAction {
    Delay(Duration),
    Fail { status: StatusCode, reason: String },
    InjectPrivateEvent(Value),
    FillPlan { steps: Vec<OrderFillStep> },
}

/// Declarative instruction for staged fills.
#[derive(Clone, Debug)]
pub struct OrderFillStep {
    pub after: Duration,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
}

/// Declarative scenario scheduled for execution.
#[derive(Clone, Debug)]
pub struct Scenario {
    pub name: String,
    pub trigger: ScenarioTrigger,
    pub action: ScenarioAction,
}

impl Scenario {
    /// Utility constructor for failure scenarios.
    pub fn fail(
        name: impl Into<String>,
        trigger: ScenarioTrigger,
        status: StatusCode,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            trigger,
            action: ScenarioAction::Fail {
                status,
                reason: reason.into(),
            },
        }
    }
}

/// Thread-safe store for scripted scenarios.
#[derive(Clone, Default)]
pub struct ScenarioManager {
    inner: Arc<Mutex<Vec<Scenario>>>,
}

impl ScenarioManager {
    /// Creates an empty scenario manager.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Pushes a scenario to the back of the queue.
    pub async fn push(&self, scenario: Scenario) {
        let mut guard = self.inner.lock().await;
        guard.push(scenario);
    }

    /// Drains and returns the first scenario matching the trigger.
    pub async fn take_for(&self, trigger: ScenarioTrigger) -> Option<ScenarioAction> {
        let mut guard = self.inner.lock().await;
        guard
            .iter()
            .position(|scenario| scenario.trigger == trigger)
            .map(|idx| guard.remove(idx).action)
    }

    /// Clears all registered scenarios.
    pub async fn clear(&self) {
        let mut guard = self.inner.lock().await;
        guard.clear();
    }
}
