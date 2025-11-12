//! Order orchestrator for managing algorithmic execution.

use anyhow::{anyhow, Result};
use chrono::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use crate::algorithm::twap::TwapAlgorithm;
use crate::algorithm::{AlgoStatus, ChildOrderRequest, ExecutionAlgorithm};
use crate::repository::AlgoStateRepository;
use crate::{ExecutionEngine, RiskContext};
use tesser_core::{ExecutionHint, Fill, Order, Signal, Tick};

/// Maps order IDs to their parent algorithm IDs for routing fills.
type OrderToAlgoMap = HashMap<String, Uuid>;

/// Core orchestrator for managing algorithmic order execution.
///
/// The orchestrator is responsible for:
/// - Creating and managing algorithm instances
/// - Routing events (fills, ticks, timers) to appropriate algorithms
/// - Persisting algorithm state for crash recovery
/// - Handling the lifecycle of algorithmic orders
pub struct OrderOrchestrator {
    /// Active algorithm instances.
    algorithms: Arc<Mutex<HashMap<Uuid, Box<dyn ExecutionAlgorithm>>>>,

    /// Maps order IDs to their parent algorithm IDs.
    order_mapping: Arc<Mutex<OrderToAlgoMap>>,

    /// Underlying execution engine for placing child orders.
    execution_engine: Arc<ExecutionEngine>,

    /// State persistence backend.
    state_repo: Arc<dyn AlgoStateRepository>,
}

impl OrderOrchestrator {
    /// Create a new orchestrator and restore any persisted algorithms.
    pub async fn new(
        execution_engine: Arc<ExecutionEngine>,
        state_repo: Arc<dyn AlgoStateRepository>,
    ) -> Result<Self> {
        let algorithms = Arc::new(Mutex::new(HashMap::new()));
        let order_mapping = Arc::new(Mutex::new(HashMap::new()));

        let orchestrator = Self {
            algorithms,
            order_mapping,
            execution_engine,
            state_repo,
        };

        // Restore algorithms from persistent state
        orchestrator.restore_algorithms().await?;

        Ok(orchestrator)
    }

    /// Restore algorithms from persistent state.
    async fn restore_algorithms(&self) -> Result<()> {
        let states = self.state_repo.load_all()?;

        let mut algorithms = self.algorithms.lock().unwrap();

        for (id, state) in states {
            // For now, we only support TWAP. In the future, we'd need to detect
            // the algorithm type from the state and dispatch accordingly.
            match TwapAlgorithm::from_state(state) {
                Ok(algo) => {
                    tracing::info!(id = %id, "Restored TWAP algorithm from state");
                    algorithms.insert(id, Box::new(algo));
                }
                Err(e) => {
                    tracing::warn!(
                        id = %id,
                        error = %e,
                        "Failed to restore algorithm, deleting state"
                    );
                    if let Err(delete_err) = self.state_repo.delete(&id) {
                        tracing::error!(
                            id = %id,
                            error = %delete_err,
                            "Failed to delete corrupted algorithm state"
                        );
                    }
                }
            }
        }

        tracing::info!(
            count = algorithms.len(),
            "Restored algorithms from persistent state"
        );

        Ok(())
    }

    /// Handle a signal from a strategy.
    pub async fn on_signal(&self, signal: &Signal, ctx: &RiskContext) -> Result<()> {
        match &signal.execution_hint {
            Some(ExecutionHint::Twap { duration }) => {
                self.handle_twap_signal(signal.clone(), *duration, ctx)
                    .await
            }
            Some(ExecutionHint::Vwap { .. }) => {
                // TODO: Implement VWAP
                Err(anyhow!("VWAP algorithm not yet implemented"))
            }
            Some(ExecutionHint::IcebergSimulated { .. }) => {
                // TODO: Implement Iceberg
                Err(anyhow!("Iceberg algorithm not yet implemented"))
            }
            None => {
                // Handle normal, non-algorithmic orders
                self.execution_engine
                    .handle_signal(signal.clone(), *ctx)
                    .await?;
                Ok(())
            }
        }
    }

    /// Handle a TWAP signal by creating a new TWAP algorithm instance.
    async fn handle_twap_signal(
        &self,
        signal: Signal,
        duration: Duration,
        ctx: &RiskContext,
    ) -> Result<()> {
        // Calculate total quantity using the execution engine's sizer
        let total_quantity =
            self.execution_engine
                .sizer()
                .size(&signal, ctx.portfolio_equity, ctx.last_price)?;

        if total_quantity <= 0.0 {
            tracing::warn!("TWAP order size is zero, skipping");
            return Ok(());
        }

        // Use a sensible default for number of slices
        // TODO: Make this configurable
        let num_slices = std::cmp::min(30, duration.num_minutes() as u32).max(1);

        // Create and start the algorithm
        let mut algo = TwapAlgorithm::new(signal, total_quantity, duration, num_slices)?;
        let algo_id = *algo.id();

        tracing::info!(
            id = %algo_id,
            total_qty = total_quantity,
            duration_mins = duration.num_minutes(),
            slices = num_slices,
            "Starting new TWAP algorithm"
        );

        // Start the algorithm and get any initial orders
        let initial_orders = algo.start()?;

        // Add to active algorithms
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            algorithms.insert(algo_id, Box::new(algo));
        }

        // Persist initial state
        self.persist_algo_state(&algo_id).await?;

        // Send initial orders (if any)
        for child_req in initial_orders {
            self.send_child_order(child_req, ctx).await?;
        }

        Ok(())
    }

    /// Send a child order and track the mapping.
    async fn send_child_order(
        &self,
        child_req: ChildOrderRequest,
        ctx: &RiskContext,
    ) -> Result<Order> {
        let order = self
            .execution_engine
            .send_order(child_req.order_request, ctx)
            .await?;

        // Track the mapping from order ID to parent algorithm
        {
            let mut mapping = self.order_mapping.lock().unwrap();
            mapping.insert(order.id.clone(), child_req.parent_algo_id);
        }

        // Notify the algorithm that the order was placed
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            if let Some(algo) = algorithms.get_mut(&child_req.parent_algo_id) {
                algo.on_child_order_placed(&order);
                // Note: We don't persist state here as it's not critical
            }
        }

        Ok(order)
    }

    /// Handle a fill from the execution engine.
    pub async fn on_fill(&self, fill: &Fill) -> Result<()> {
        // Find the parent algorithm for this fill
        let parent_algo_id = {
            let mapping = self.order_mapping.lock().unwrap();
            mapping.get(&fill.order_id).copied()
        };

        let Some(algo_id) = parent_algo_id else {
            // This fill doesn't belong to any algorithm, ignore it
            return Ok(());
        };

        tracing::debug!(
            algo_id = %algo_id,
            order_id = %fill.order_id,
            fill_qty = fill.fill_quantity,
            "Routing fill to algorithm"
        );

        let mut new_child_orders = Vec::new();
        let mut algo_completed = false;

        // Process the fill with the algorithm
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            if let Some(algo) = algorithms.get_mut(&algo_id) {
                match algo.on_fill(fill) {
                    Ok(orders) => {
                        new_child_orders = orders;
                        if !matches!(algo.status(), AlgoStatus::Working) {
                            algo_completed = true;
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            algo_id = %algo_id,
                            error = %e,
                            "Algorithm failed to process fill"
                        );
                        // Mark the algorithm as failed
                        let _ = algo.cancel();
                        algo_completed = true;
                    }
                }
            }
        }

        // Send new child orders if any
        // TODO: We need a way to get RiskContext here
        let dummy_ctx = RiskContext::default();
        for child_req in &new_child_orders {
            if let Err(e) = self.send_child_order(child_req.clone(), &dummy_ctx).await {
                tracing::error!(
                    algo_id = %algo_id,
                    error = %e,
                    "Failed to send child order"
                );
            }
        }

        // Persist updated state
        self.persist_algo_state(&algo_id).await?;

        // Clean up completed algorithm
        if algo_completed {
            self.cleanup_algo(&algo_id).await?;
        }

        Ok(())
    }

    /// Handle market tick data.
    pub async fn on_tick(&self, tick: &Tick) -> Result<()> {
        let mut algorithms_to_process = Vec::new();

        // Collect algorithms that might be interested in this tick
        {
            let algorithms = self.algorithms.lock().unwrap();
            for (id, algo) in algorithms.iter() {
                if matches!(algo.status(), AlgoStatus::Working) {
                    algorithms_to_process.push(*id);
                }
            }
        }

        // Process tick with each algorithm
        for algo_id in algorithms_to_process {
            let mut new_child_orders = Vec::new();
            let mut algo_completed = false;

            {
                let mut algorithms = self.algorithms.lock().unwrap();
                if let Some(algo) = algorithms.get_mut(&algo_id) {
                    match algo.on_tick(tick) {
                        Ok(orders) => {
                            new_child_orders = orders;
                            if !matches!(algo.status(), AlgoStatus::Working) {
                                algo_completed = true;
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                algo_id = %algo_id,
                                error = %e,
                                "Algorithm failed to process tick"
                            );
                        }
                    }
                }
            }

            // Send new child orders
            let has_orders = !new_child_orders.is_empty();
            let dummy_ctx = RiskContext::default();
            for child_req in new_child_orders {
                if let Err(e) = self.send_child_order(child_req, &dummy_ctx).await {
                    tracing::error!(
                        algo_id = %algo_id,
                        error = %e,
                        "Failed to send child order from tick"
                    );
                }
            }

            // Handle state persistence and cleanup
            if has_orders || algo_completed {
                self.persist_algo_state(&algo_id).await?;
            }

            if algo_completed {
                self.cleanup_algo(&algo_id).await?;
            }
        }

        Ok(())
    }

    /// Handle timer events (mainly for TWAP algorithms).
    pub async fn on_timer_tick(&self) -> Result<()> {
        let mut completed_ids = Vec::new();
        let mut new_child_orders: HashMap<Uuid, Vec<ChildOrderRequest>> = HashMap::new();

        // Process timer event for all working algorithms
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            for (id, algo) in algorithms.iter_mut() {
                if matches!(algo.status(), AlgoStatus::Working) {
                    match algo.on_timer() {
                        Ok(requests) if !requests.is_empty() => {
                            new_child_orders.insert(*id, requests);
                        }
                        Ok(_) => {
                            // No new orders, but check if algorithm completed
                            if !matches!(algo.status(), AlgoStatus::Working) {
                                completed_ids.push(*id);
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                id = %id,
                                error = %e,
                                "Algorithm on_timer failed"
                            );
                            // Mark as failed
                            let _ = algo.cancel();
                            completed_ids.push(*id);
                        }
                    }
                } else {
                    // Algorithm is no longer working
                    completed_ids.push(*id);
                }
            }
        }

        // Send new child orders (outside the lock)
        for (id, requests) in new_child_orders {
            for req in requests {
                // TODO: We need a way to get the current RiskContext here
                let dummy_ctx = RiskContext::default();
                if let Err(e) = self.send_child_order(req, &dummy_ctx).await {
                    tracing::error!(id = %id, error = %e, "Failed to send child order");
                }
            }
            self.persist_algo_state(&id).await?;
        }

        // Clean up completed algorithms
        for id in completed_ids {
            self.cleanup_algo(&id).await?;
        }

        Ok(())
    }

    /// Cancel an algorithmic order.
    pub async fn cancel_algo(&self, algo_id: &Uuid) -> Result<()> {
        let mut algo_completed = false;

        // Cancel the algorithm
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            if let Some(algo) = algorithms.get_mut(algo_id) {
                algo.cancel()?;
                algo_completed = true;
            }
        }

        if algo_completed {
            self.persist_algo_state(algo_id).await?;
            self.cleanup_algo(algo_id).await?;
        }

        Ok(())
    }

    /// Persist the state of a specific algorithm.
    async fn persist_algo_state(&self, id: &Uuid) -> Result<()> {
        let state_json = {
            let algorithms = self.algorithms.lock().unwrap();
            let algo = algorithms
                .get(id)
                .ok_or_else(|| anyhow!("Algorithm not found for persistence: {}", id))?;
            algo.state()
        };

        self.state_repo.save(id, state_json)?;
        Ok(())
    }

    /// Clean up a completed algorithm.
    async fn cleanup_algo(&self, id: &Uuid) -> Result<()> {
        let status = {
            let mut algorithms = self.algorithms.lock().unwrap();
            let status = algorithms.get(id).map(|algo| algo.status());
            algorithms.remove(id);
            status
        };

        // Clean up order mappings
        {
            let mut mapping = self.order_mapping.lock().unwrap();
            mapping.retain(|_, algo_id| algo_id != id);
        }

        // Delete persistent state
        self.state_repo.delete(id)?;

        tracing::info!(
            id = %id,
            status = ?status,
            "Algorithm cleaned up"
        );

        Ok(())
    }

    /// Get the number of active algorithms.
    pub fn active_algorithms_count(&self) -> usize {
        let algorithms = self.algorithms.lock().unwrap();
        algorithms.len()
    }

    /// Get a snapshot of algorithm statuses for monitoring.
    pub fn algorithm_statuses(&self) -> HashMap<Uuid, AlgoStatus> {
        let algorithms = self.algorithms.lock().unwrap();
        algorithms
            .iter()
            .map(|(id, algo)| (*id, algo.status()))
            .collect()
    }

    /// Access to the underlying execution engine.
    pub fn execution_engine(&self) -> Arc<ExecutionEngine> {
        Arc::clone(&self.execution_engine)
    }
}
