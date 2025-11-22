//! Order orchestrator for managing algorithmic execution.

use anyhow::{anyhow, bail, Result};
use chrono::Duration;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration as StdDuration, Instant};
use uuid::Uuid;

use crate::algorithm::{
    AlgoStatus, ChildOrderRequest, ExecutionAlgorithm, IcebergAlgorithm, PeggedBestAlgorithm,
    SniperAlgorithm, TwapAlgorithm, VwapAlgorithm,
};
use crate::repository::AlgoStateRepository;
use crate::{ExecutionEngine, RiskContext};
use tesser_core::{
    ExecutionHint, Fill, Order, OrderRequest, OrderStatus, Price, Quantity, Signal, Tick,
};

/// Maps order IDs to their parent algorithm IDs for routing fills.
type OrderToAlgoMap = HashMap<String, Uuid>;

#[derive(Clone)]
struct PendingOrder {
    request: OrderRequest,
    placed_at: Instant,
}

pub const ORDER_TIMEOUT: StdDuration = StdDuration::from_secs(60);
pub const ORDER_POLL_INTERVAL: StdDuration = StdDuration::from_secs(15);

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredAlgoState {
    algo_type: String,
    state: serde_json::Value,
}

/// Core orchestrator for managing algorithmic order execution.
///
/// The orchestrator is responsible for:
/// - Creating and managing algorithm instances
/// - Routing events (fills, ticks, timers) to appropriate algorithms
/// - Persisting algorithm state for crash recovery
/// - Handling the lifecycle of algorithmic orders
#[derive(Clone)]
pub struct OrderOrchestrator {
    /// Active algorithm instances.
    algorithms: Arc<Mutex<HashMap<Uuid, Box<dyn ExecutionAlgorithm>>>>,

    /// Maps order IDs to their parent algorithm IDs.
    order_mapping: Arc<Mutex<OrderToAlgoMap>>,
    /// Tracks pending orders for timeout handling.
    pending_orders: Arc<Mutex<HashMap<String, PendingOrder>>>,

    /// Cached risk context per symbol supplied by the portfolio.
    risk_contexts: Arc<Mutex<HashMap<String, RiskContext>>>,

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
        open_orders: Vec<Order>,
    ) -> Result<Self> {
        let algorithms = Arc::new(Mutex::new(HashMap::new()));
        let order_mapping = Arc::new(Mutex::new(HashMap::new()));
        let pending_orders = Arc::new(Mutex::new(HashMap::new()));
        let risk_contexts = Arc::new(Mutex::new(HashMap::new()));

        let orchestrator = Self {
            algorithms,
            order_mapping,
            pending_orders,
            risk_contexts,
            execution_engine,
            state_repo,
        };

        // Restore algorithms from persistent state
        orchestrator.restore_algorithms().await?;
        orchestrator.adopt_open_orders(open_orders).await?;

        Ok(orchestrator)
    }

    /// Restore algorithms from persistent state.
    async fn restore_algorithms(&self) -> Result<()> {
        let states = self.state_repo.load_all()?;

        let mut algorithms = self.algorithms.lock().unwrap();

        let mut restored = 0usize;

        for (id, raw_state) in states {
            let decoded = Self::decode_stored_state(raw_state);
            match Self::instantiate_algorithm(&decoded.algo_type, decoded.state) {
                Ok(algo) => {
                    tracing::info!(
                        id = %id,
                        algo_type = algo.kind(),
                        "Restored algorithm from state"
                    );
                    algorithms.insert(id, algo);
                    restored += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        id = %id,
                        algo_type = decoded.algo_type,
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
            count = restored,
            "Restored algorithms from persistent state"
        );

        Ok(())
    }

    async fn adopt_open_orders(&self, open_orders: Vec<Order>) -> Result<()> {
        if open_orders.is_empty() {
            return Ok(());
        }

        let mut touched_algorithms = HashSet::new();

        for order in open_orders {
            if !Self::is_active_order_status(order.status) {
                continue;
            }
            let Some(client_id) = order.request.client_order_id.clone() else {
                continue;
            };
            let Some(algo_id) = Self::extract_algo_id(&client_id) else {
                continue;
            };

            let mut should_register = false;
            {
                let mut algorithms = self.algorithms.lock().unwrap();
                if let Some(algo) = algorithms.get_mut(&algo_id) {
                    if let Err(err) = algo.bind_child_order(order.clone()) {
                        tracing::warn!(
                            algo_id = %algo_id,
                            order_id = %order.id,
                            error = %err,
                            "failed to bind recovered child order"
                        );
                    } else {
                        should_register = true;
                        touched_algorithms.insert(algo_id);
                    }
                } else {
                    tracing::debug!(
                        algo_id = %algo_id,
                        order_id = %order.id,
                        "no matching algorithm found for recovered order"
                    );
                }
            }

            if !should_register {
                continue;
            }

            {
                let mut mapping = self.order_mapping.lock().unwrap();
                mapping.insert(order.id.clone(), algo_id);
            }
            self.register_pending(&order);
            tracing::info!(
                algo_id = %algo_id,
                order_id = %order.id,
                symbol = %order.request.symbol,
                status = ?order.status,
                "adopted in-flight child order"
            );
        }

        for algo_id in touched_algorithms {
            if let Err(err) = self.persist_algo_state(&algo_id).await {
                tracing::error!(
                    algo_id = %algo_id,
                    error = %err,
                    "failed to persist algorithm state after adoption"
                );
            }
        }

        Ok(())
    }

    fn decode_stored_state(value: serde_json::Value) -> StoredAlgoState {
        serde_json::from_value::<StoredAlgoState>(value.clone()).unwrap_or(StoredAlgoState {
            algo_type: "TWAP".to_string(),
            state: value,
        })
    }

    fn instantiate_algorithm(
        algo_type: &str,
        state: serde_json::Value,
    ) -> Result<Box<dyn ExecutionAlgorithm>> {
        match algo_type {
            "TWAP" => Ok(Box::new(TwapAlgorithm::from_state(state)?)),
            "VWAP" => Ok(Box::new(VwapAlgorithm::from_state(state)?)),
            "ICEBERG" => Ok(Box::new(IcebergAlgorithm::from_state(state)?)),
            "PEGGED_BEST" => Ok(Box::new(PeggedBestAlgorithm::from_state(state)?)),
            "SNIPER" => Ok(Box::new(SniperAlgorithm::from_state(state)?)),
            other => bail!("unsupported algorithm type '{other}'"),
        }
    }

    /// Update the latest risk context for a symbol.
    pub fn update_risk_context(&self, symbol: impl Into<String>, ctx: RiskContext) {
        let mut contexts = self.risk_contexts.lock().unwrap();
        contexts.insert(symbol.into(), ctx);
    }

    fn cached_risk_context(&self, symbol: &str) -> Option<RiskContext> {
        let contexts = self.risk_contexts.lock().unwrap();
        contexts.get(symbol).copied()
    }

    fn is_active_order_status(status: OrderStatus) -> bool {
        matches!(
            status,
            OrderStatus::PendingNew | OrderStatus::Accepted | OrderStatus::PartiallyFilled
        )
    }

    fn extract_algo_id(client_id: &str) -> Option<Uuid> {
        if let Some(rest) = client_id.strip_prefix("twap-") {
            let (id_part, _) = rest.split_once("-slice-")?;
            return Uuid::parse_str(id_part).ok();
        }
        if let Some(rest) = client_id.strip_prefix("iceberg-") {
            let (id_part, _) = rest.rsplit_once('-')?;
            return Uuid::parse_str(id_part).ok();
        }
        if let Some(rest) = client_id.strip_prefix("peg-") {
            let (id_part, _) = rest.rsplit_once('-')?;
            return Uuid::parse_str(id_part).ok();
        }
        if let Some(rest) = client_id.strip_prefix("vwap-") {
            let (id_part, _) = rest.rsplit_once('-')?;
            return Uuid::parse_str(id_part).ok();
        }
        if let Some(rest) = client_id.strip_prefix("sniper-") {
            return Uuid::parse_str(rest).ok();
        }
        None
    }

    /// Handle a signal from a strategy.
    pub async fn on_signal(&self, signal: &Signal, ctx: &RiskContext) -> Result<()> {
        match &signal.execution_hint {
            Some(ExecutionHint::Twap { duration }) => {
                self.handle_twap_signal(signal.clone(), *duration, ctx)
                    .await
            }
            Some(ExecutionHint::Vwap {
                duration,
                participation_rate,
            }) => {
                self.handle_vwap_signal(signal.clone(), *duration, *participation_rate, ctx)
                    .await
            }
            Some(ExecutionHint::IcebergSimulated {
                display_size,
                limit_offset_bps,
            }) => {
                self.handle_iceberg_signal(signal.clone(), *display_size, *limit_offset_bps, ctx)
                    .await
            }
            Some(ExecutionHint::PeggedBest {
                offset_bps,
                clip_size,
                refresh_secs,
            }) => {
                self.handle_pegged_signal(
                    signal.clone(),
                    *offset_bps,
                    *clip_size,
                    *refresh_secs,
                    ctx,
                )
                .await
            }
            Some(ExecutionHint::Sniper {
                trigger_price,
                timeout,
            }) => {
                self.handle_sniper_signal(signal.clone(), *trigger_price, *timeout, ctx)
                    .await
            }
            None => {
                // Handle normal, non-algorithmic orders
                if let Some(order) = self
                    .execution_engine
                    .handle_signal(signal.clone(), *ctx)
                    .await?
                {
                    self.register_pending(&order);
                }
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
        self.update_risk_context(signal.symbol.clone(), *ctx);
        // Calculate total quantity using the execution engine's sizer
        let total_quantity =
            self.execution_engine
                .sizer()
                .size(&signal, ctx.portfolio_equity, ctx.last_price)?;

        if total_quantity <= Decimal::ZERO {
            tracing::warn!("TWAP order size is zero, skipping");
            return Ok(());
        }

        // Use a sensible default for number of slices (seconds granularity for shorter runs)
        // TODO: Make this configurable
        let mut slice_guess = duration.num_minutes() as u32;
        if slice_guess == 0 {
            let seconds = duration.num_seconds().max(1);
            slice_guess = seconds as u32;
        }
        let num_slices = slice_guess.clamp(1, 30);

        // Create and start the algorithm
        let mut algo = TwapAlgorithm::new(signal, total_quantity, duration, num_slices)?;
        let algo_id = *algo.id();

        tracing::info!(
            id = %algo_id,
            total_qty = %total_quantity,
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
            self.send_child_order(child_req, Some(*ctx)).await?;
        }

        Ok(())
    }

    async fn handle_vwap_signal(
        &self,
        signal: Signal,
        duration: Duration,
        participation_rate: Option<Decimal>,
        ctx: &RiskContext,
    ) -> Result<()> {
        self.update_risk_context(signal.symbol.clone(), *ctx);
        let total_quantity =
            self.execution_engine
                .sizer()
                .size(&signal, ctx.portfolio_equity, ctx.last_price)?;
        if total_quantity <= Decimal::ZERO {
            tracing::warn!("VWAP order size is zero, skipping");
            return Ok(());
        }
        let mut algo = VwapAlgorithm::new(signal, total_quantity, duration, participation_rate)?;
        let algo_id = *algo.id();
        tracing::info!(
            id = %algo_id,
            qty = %total_quantity,
            duration_mins = duration.num_minutes(),
            participation = ?participation_rate,
            "Starting new VWAP algorithm"
        );

        let initial_orders = algo.start()?;
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            algorithms.insert(algo_id, Box::new(algo));
        }
        self.persist_algo_state(&algo_id).await?;
        for child in initial_orders {
            self.send_child_order(child, Some(*ctx)).await?;
        }
        Ok(())
    }

    async fn handle_iceberg_signal(
        &self,
        signal: Signal,
        display_size: Quantity,
        limit_offset_bps: Option<Decimal>,
        ctx: &RiskContext,
    ) -> Result<()> {
        self.update_risk_context(signal.symbol.clone(), *ctx);
        let total_quantity =
            self.execution_engine
                .sizer()
                .size(&signal, ctx.portfolio_equity, ctx.last_price)?;
        if total_quantity <= Decimal::ZERO {
            tracing::warn!("Iceberg order size is zero, skipping");
            return Ok(());
        }
        let limit_price = if ctx.last_price > Decimal::ZERO {
            ctx.last_price
        } else {
            tracing::warn!(
                "last price unavailable for iceberg order; defaulting to 1.0 for {}",
                signal.symbol
            );
            Decimal::ONE
        };

        let mut algo = IcebergAlgorithm::new(
            signal,
            total_quantity,
            display_size,
            limit_price,
            limit_offset_bps,
        )?;
        let algo_id = *algo.id();
        tracing::info!(
            id = %algo_id,
            qty = %total_quantity,
            display = %display_size,
            "Starting new Iceberg algorithm"
        );

        let initial_orders = algo.start()?;
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            algorithms.insert(algo_id, Box::new(algo));
        }
        self.persist_algo_state(&algo_id).await?;
        for child in initial_orders {
            self.send_child_order(child, Some(*ctx)).await?;
        }
        Ok(())
    }

    async fn handle_pegged_signal(
        &self,
        signal: Signal,
        offset_bps: Decimal,
        clip_size: Option<Quantity>,
        refresh_secs: Option<u64>,
        ctx: &RiskContext,
    ) -> Result<()> {
        self.update_risk_context(signal.symbol.clone(), *ctx);
        let total_quantity =
            self.execution_engine
                .sizer()
                .size(&signal, ctx.portfolio_equity, ctx.last_price)?;
        if total_quantity <= Decimal::ZERO {
            tracing::warn!("Pegged order size is zero, skipping");
            return Ok(());
        }
        let secs = refresh_secs.unwrap_or(1).max(1) as i64;
        let refresh = Duration::seconds(secs);
        let mut algo =
            PeggedBestAlgorithm::new(signal, total_quantity, offset_bps, clip_size, refresh)?;
        let algo_id = *algo.id();
        tracing::info!(
            id = %algo_id,
            qty = %total_quantity,
            offset = %offset_bps,
            "Starting new PeggedBest algorithm"
        );
        let initial_orders = algo.start()?;
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            algorithms.insert(algo_id, Box::new(algo));
        }
        self.persist_algo_state(&algo_id).await?;
        for child in initial_orders {
            self.send_child_order(child, Some(*ctx)).await?;
        }
        Ok(())
    }

    async fn handle_sniper_signal(
        &self,
        signal: Signal,
        trigger_price: Price,
        timeout: Option<Duration>,
        ctx: &RiskContext,
    ) -> Result<()> {
        self.update_risk_context(signal.symbol.clone(), *ctx);
        let total_quantity =
            self.execution_engine
                .sizer()
                .size(&signal, ctx.portfolio_equity, ctx.last_price)?;
        if total_quantity <= Decimal::ZERO {
            tracing::warn!("Sniper order size is zero, skipping");
            return Ok(());
        }
        let mut algo = SniperAlgorithm::new(signal, total_quantity, trigger_price, timeout)?;
        let algo_id = *algo.id();
        tracing::info!(id = %algo_id, qty = %total_quantity, "Starting new Sniper algorithm");
        let initial_orders = algo.start()?;
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            algorithms.insert(algo_id, Box::new(algo));
        }
        self.persist_algo_state(&algo_id).await?;
        for child in initial_orders {
            self.send_child_order(child, Some(*ctx)).await?;
        }
        Ok(())
    }

    /// Send a child order and track the mapping.
    async fn send_child_order(
        &self,
        child_req: ChildOrderRequest,
        ctx: Option<RiskContext>,
    ) -> Result<Order> {
        let ChildOrderRequest {
            parent_algo_id,
            order_request,
        } = child_req;
        let symbol = order_request.symbol.clone();
        let resolved_ctx = ctx
            .or_else(|| self.cached_risk_context(&symbol))
            .ok_or_else(|| anyhow!("missing risk context for symbol {}", symbol))?;
        // Keep cache warm with the latest context.
        self.update_risk_context(symbol.clone(), resolved_ctx);

        let order = self
            .execution_engine
            .send_order(order_request, &resolved_ctx)
            .await?;

        // Track the mapping from order ID to parent algorithm
        {
            let mut mapping = self.order_mapping.lock().unwrap();
            mapping.insert(order.id.clone(), parent_algo_id);
        }
        self.register_pending(&order);

        // Notify the algorithm that the order was placed
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            if let Some(algo) = algorithms.get_mut(&parent_algo_id) {
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
        self.clear_pending(&fill.order_id);

        let Some(algo_id) = parent_algo_id else {
            // This fill doesn't belong to any algorithm, ignore it
            return Ok(());
        };

        tracing::debug!(
            algo_id = %algo_id,
            order_id = %fill.order_id,
            fill_qty = %fill.fill_quantity,
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
        for child_req in &new_child_orders {
            if let Err(e) = self.send_child_order(child_req.clone(), None).await {
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
            for child_req in new_child_orders {
                if let Err(e) = self.send_child_order(child_req, None).await {
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
                if let Err(e) = self.send_child_order(req, None).await {
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
        let payload = {
            let algorithms = self.algorithms.lock().unwrap();
            let algo = algorithms
                .get(id)
                .ok_or_else(|| anyhow!("Algorithm not found for persistence: {}", id))?;
            StoredAlgoState {
                algo_type: algo.kind().to_string(),
                state: algo.state(),
            }
        };

        self.state_repo.save(id, serde_json::to_value(payload)?)?;
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
            mapping.retain(|order_id, algo_id| {
                let retain = algo_id != id;
                if !retain {
                    self.clear_pending(order_id);
                }
                retain
            });
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

    /// Remove a pending order when an update arrives.
    pub fn on_order_update(&self, order: &Order) {
        if matches!(
            order.status,
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
        ) {
            self.clear_pending(&order.id);
        }
    }

    /// Poll the exchange for any pending orders that exceeded the timeout.
    /// Returns synthesized order updates for downstream handling.
    pub async fn poll_stale_orders(&self) -> Result<Vec<Order>> {
        let stale: Vec<(String, PendingOrder)> = {
            let mut pending = self.pending_orders.lock().unwrap();
            let now = Instant::now();
            let expired: Vec<String> = pending
                .iter()
                .filter_map(|(id, entry)| {
                    if now.duration_since(entry.placed_at) >= ORDER_TIMEOUT {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect();
            expired
                .into_iter()
                .filter_map(|id| pending.remove(&id).map(|entry| (id, entry)))
                .collect()
        };

        let mut updates = Vec::new();
        for (order_id, entry) in stale {
            let client = self.execution_engine.client();
            let synthesized = match client.list_open_orders(&entry.request.symbol).await {
                Ok(remote_orders) => {
                    if let Some(existing) =
                        remote_orders.into_iter().find(|order| order.id == order_id)
                    {
                        // If the order is still working, cancel it proactively.
                        if matches!(
                            existing.status,
                            OrderStatus::PendingNew
                                | OrderStatus::Accepted
                                | OrderStatus::PartiallyFilled
                        ) {
                            let _ = client
                                .cancel_order(order_id.clone(), &entry.request.symbol)
                                .await;
                            let mut canceled = existing.clone();
                            canceled.status = OrderStatus::Canceled;
                            canceled.updated_at = chrono::Utc::now();
                            Some(canceled)
                        } else {
                            Some(existing)
                        }
                    } else {
                        Some(build_timeout_order(
                            order_id.clone(),
                            entry.request.clone(),
                            OrderStatus::Rejected,
                        ))
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        order_id = %order_id,
                        symbol = %entry.request.symbol,
                        error = %err,
                        "failed to query stale order; canceling defensively"
                    );
                    let _ = client
                        .cancel_order(order_id.clone(), &entry.request.symbol)
                        .await;
                    Some(build_timeout_order(
                        order_id.clone(),
                        entry.request.clone(),
                        OrderStatus::Canceled,
                    ))
                }
            };

            if let Some(order) = synthesized {
                self.on_order_update(&order);
                self.clear_order_mapping(&order_id);
                updates.push(order);
            }
        }

        Ok(updates)
    }

    fn register_pending(&self, order: &Order) {
        let mut pending = self.pending_orders.lock().unwrap();
        pending.insert(
            order.id.clone(),
            PendingOrder {
                request: order.request.clone(),
                placed_at: Instant::now(),
            },
        );
    }

    fn clear_pending(&self, order_id: &str) {
        let mut pending = self.pending_orders.lock().unwrap();
        pending.remove(order_id);
    }

    fn clear_order_mapping(&self, order_id: &str) {
        let mut mapping = self.order_mapping.lock().unwrap();
        mapping.remove(order_id);
    }
}

fn build_timeout_order(id: String, request: OrderRequest, status: OrderStatus) -> Order {
    Order {
        id,
        request,
        status,
        filled_quantity: Decimal::ZERO,
        avg_fill_price: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}
