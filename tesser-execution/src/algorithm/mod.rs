//! Execution algorithms for algorithmic order placement.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tesser_core::{Fill, Order, OrderRequest, Tick};
use uuid::Uuid;

/// Represents a child order request from an execution algorithm.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChildOrderRequest {
    /// ID of the parent algorithm that generated this request.
    pub parent_algo_id: Uuid,
    /// The actual order request to be submitted.
    pub order_request: OrderRequest,
}

/// Current status of an execution algorithm.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum AlgoStatus {
    /// Algorithm is actively working.
    Working,
    /// Algorithm has completed successfully.
    Completed,
    /// Algorithm has been cancelled.
    Cancelled,
    /// Algorithm failed with an error message.
    Failed(String),
}

/// Trait defining the behavior of an execution algorithm.
///
/// Each execution algorithm is a stateful entity that responds to various events
/// and generates child orders as needed. The algorithm maintains its own internal
/// state and can be persisted and restored.
pub trait ExecutionAlgorithm: Send + Sync {
    /// Returns the unique ID of this algorithm instance.
    fn id(&self) -> &Uuid;

    /// Returns the current status of the algorithm.
    fn status(&self) -> AlgoStatus;

    /// Start the algorithm and return any initial child orders.
    fn start(&mut self) -> Result<Vec<ChildOrderRequest>>;

    /// Called when a child order has been successfully placed.
    fn on_child_order_placed(&mut self, order: &Order);

    /// Called when a fill is received for one of this algorithm's child orders.
    fn on_fill(&mut self, fill: &Fill) -> Result<Vec<ChildOrderRequest>>;

    /// Called when market tick data is received (mainly for VWAP algorithms).
    fn on_tick(&mut self, tick: &Tick) -> Result<Vec<ChildOrderRequest>>;

    /// Called when a timer event occurs (mainly for TWAP algorithms).
    fn on_timer(&mut self) -> Result<Vec<ChildOrderRequest>>;

    /// Request cancellation of the algorithm.
    fn cancel(&mut self) -> Result<()>;

    /// Return the current state for persistence.
    fn state(&self) -> serde_json::Value;

    /// Restore algorithm from persisted state.
    fn from_state(state: serde_json::Value) -> Result<Self>
    where
        Self: Sized;
}

pub mod twap;
pub use twap::TwapAlgorithm;
